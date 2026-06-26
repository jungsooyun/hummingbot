"""Shared base for cross-venue *hedged* executors (JEP-143).

Two executors in this tree implement the same finite-state machine:

  * ``LadderMakerExecutor`` — post-only maker ladder on a perp (Hyperliquid HIP-3),
    each fill hedged on KIS spot.
  * ``XEMMExecutor`` — upstream-style cross-exchange market making: a maker quote on
    venue A, hedged with a taker order on venue B.

Both share roughly the same bookkeeping machine — *maker fill on venue A -> hedge on
venue B* — with ~90% duplicated accounting. This module factors that shared machine
into ``CrossVenueHedgedExecutorBase`` so a future rewire (JEP-147) can let both
executors inherit it without re-deriving inventory / PnL / fee / retry plumbing.

Design note — what is SHARED vs what is a HOOK
==============================================

SHARED (lives here, identical behavior for every subclass):

  * Connector + pair holding for the maker leg and the hedge leg
    (``maker_connector`` / ``maker_trading_pair`` / ``hedge_connector`` /
    ``hedge_trading_pair``), plus ``entry_side`` / ``hedge_side`` derivation.
  * Order books: ``maker_orders`` / ``hedge_orders`` dicts of ``TrackedOrder``.
  * Fill accounting: ``_maker_executed_base`` / ``_maker_executed_quote`` /
    ``_hedge_executed_base`` / ``_hedge_executed_quote`` and the fee accumulators
    ``_maker_fees_quote`` / ``_hedge_fees_quote``.
  * Hedge queue: signed ``_pending_hedge_signed`` with single-in-flight double-hedge
    prevention (``_hedge_in_flight``) — a maker fill enqueues base to hedge, and at
    most one hedge order is live at a time.
  * Inventory accounting: ``_unhedged_base()`` (always >= 0) and
    ``_unhedged_base_signed()`` (long positive / short negative).
  * PnL / fees: ``get_net_pnl_quote()`` (matched-quantity maker-vs-hedge spread minus
    fees), ``get_net_pnl_pct()``, ``get_cum_fees_quote()``.
  * ``max_retries`` -> ``CloseType.FAILED`` handling on repeated hedge failures.
  * Order-event plumbing: ``process_order_created_event`` /
    ``process_order_filled_event`` / ``process_order_completed_event`` /
    ``process_order_canceled_event`` / ``process_order_failed_event`` updating the
    accounting above.
  * Open-order helpers: ``_open_maker_orders`` / ``_open_hedge_orders`` /
    ``_has_open_orders`` / ``_update_tracked``.
  * ``get_custom_info()`` base fields and a ``validate_sufficient_balance`` skeleton
    that defers the candidate construction to a hook.

HOOK (abstract / overridable — the per-strategy difference):

  * ``_compute_targets()`` — derive the maker order(s) to quote this tick.
  * ``_should_reprice(targets)`` — reprice guard policy.
  * ``_place_targets(targets)`` — translate targets into ``place_order`` calls.
  * ``_gates_open()`` — pre-quote kill-switch / data-readiness gate.
  * ``_size_hedge(pending_base)`` — convert pending base into a concrete hedge
    (side / price / amount), or ``None`` to skip this tick.
  * ``_maker_balance_candidate()`` — the ``OrderCandidate`` used by the shared
    ``validate_sufficient_balance`` skeleton.
  * ``_pnl_gross_quote(matched, maker_avg, hedge_avg)`` — sign convention for gross
    PnL (perp-maker vs spot-hedge differs from buy-maker vs sell-taker).

This class is ABSTRACT. It is intentionally NOT registered in
``executor_orchestrator._executor_mapping`` — only concrete executor types
(``ladder_maker_executor`` / ``xemm_executor``) belong there. The rewire that makes
``LadderMakerExecutor`` / ``XEMMExecutor`` inherit from this base is JEP-147; this
file is additive scaffolding only.
"""

import asyncio
import inspect
import logging
import time
from abc import abstractmethod
from collections import OrderedDict
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.core.data_type.common import PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.hedge_defer_policy import decide_hedge_defer
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import (
    RestingOrder,
    RungTarget,
    Side,
    blocked_targets,
    diff_ladder_targets,
)
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_halt_source import (
    FORCE_ELIGIBLE_HALT_REASONS,
    SessionHaltState,
)
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

ZERO = Decimal("0")
ONE = Decimal("1")


class CrossVenueHedgedExecutorBase(ExecutorBase):
    """Shared machine for *maker fill on venue A -> hedge on venue B* executors.

    Subclasses supply the per-strategy pricing / sizing / reprice policy via the
    abstract hooks below; the inventory, hedge-queue, fee, PnL and retry plumbing
    is implemented once here. See the module docstring for the full shared-vs-hook
    breakdown and the JEP-147 rewire plan.
    """

    _logger = None

    # Past this multiple of max_retries consecutive hedge failures, the hedge kill-switch
    # escalates from "hold + keep retrying" to a hard FAILED stop (dead-venue backstop).
    _HEDGE_HARD_STOP_FACTOR = 3
    _HEDGE_TERMINAL_ID_CAP = 128
    # Same cap, but for MAKER orders that were popped (cancel/fail) and may still emit a
    # late cross fill (cancel/fill race). See _maker_terminal_ids below.
    _MAKER_TERMINAL_ID_CAP = 128

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        strategy: StrategyV2Base,
        config: ExecutorConfigBase,
        maker_market: ConnectorPair,
        hedge_market: ConnectorPair,
        entry_side: TradeType,
        connectors: Optional[List[str]] = None,
        update_interval: float = 1.0,
        max_retries: int = 10,
    ):
        """
        :param maker_market: connector+pair of the maker (passive) leg.
        :param hedge_market: connector+pair of the hedge (aggressive) leg.
        :param entry_side: side of the maker leg; the hedge leg trades the opposite side.
        :param connectors: extra connectors to subscribe (e.g. an FX feed); maker and
            hedge connectors are always included.
        """
        self.config = config
        self.maker_connector = maker_market.connector_name
        self.maker_trading_pair = maker_market.trading_pair
        self.hedge_connector = hedge_market.connector_name
        self.hedge_trading_pair = hedge_market.trading_pair
        self.entry_side = entry_side
        self.hedge_side = TradeType.BUY if entry_side == TradeType.SELL else TradeType.SELL

        # Order books keyed by client order id.
        self.maker_orders: Dict[str, TrackedOrder] = {}
        self.hedge_orders: Dict[str, TrackedOrder] = {}
        self._flatten_on_stop = False
        self._flatten_started_ts = None

        # Fill accounting.
        self._maker_executed_base = ZERO
        self._maker_executed_quote = ZERO
        self._hedge_executed_base = ZERO
        self._hedge_executed_quote = ZERO
        self._maker_fees_quote = ZERO
        self._hedge_fees_quote = ZERO
        self._perp_cash = ZERO
        self._spot_cash = ZERO

        # Hedge queue: signed base awaiting a hedge order. Single-in-flight prevents
        # double hedging the same fill. ``_pending_hedge_base`` is a read-only
        # magnitude alias for legacy callers and characterization snapshots.
        self._pending_hedge_signed = ZERO

        # JEP-219: bounded naked-exposure on a parked hedge. A marketable hedge that does not
        # fill (thin/empty book, no contra) rests OPEN; _hedge_in_flight() then blocks every
        # re-hedge and pending only decrements on a real fill, so the perp leg stays naked for
        # the resting order's whole lifetime. _reconcile_open_hedges cancels a hedge open longer
        # than this so the next tick re-prices it. 0 (default) disables -> behavior-neutral.
        self._hedge_fill_timeout_s = float(getattr(config, "hedge_fill_timeout_s", 0.0) or 0.0)
        # oid -> last cancel-request timestamp. A dict (not a set) so a LOST/FAILED cancel REST
        # call does not permanently suppress re-cancel: we re-issue every _hedge_fill_timeout_s
        # while the order stays open (else one dropped cancel re-opens the naked window forever).
        self._hedge_cancel_requested: Dict[str, float] = {}

        # JEP-226: session/auction-aware hedge gate. Cached per-tick folded halt state (set by
        # _evaluate_session_state; None on non-session venues -> never blocks). The defer timer
        # measures CONTINUOUS same-side naked age; cap from config (<=0 disables the gate).
        self._session_halt_state: Optional[SessionHaltState] = None
        self._hedge_defer_since_ts: Optional[float] = None
        self._hedge_defer_side: Optional[TradeType] = None
        self._hedge_defer_logged_kind: Optional[str] = None
        self._hedge_session_defer_cap_s = float(getattr(config, "hedge_session_defer_cap_s", 30.0) or 0.0)

        # Direction-aware inventory ledgers. Legacy executed-base/quote totals above
        # stay as magnitudes because get_net_pnl_quote intentionally remains unchanged.
        self._maker_buy_base = ZERO
        self._maker_sell_base = ZERO
        self._hedge_buy_base = ZERO
        self._hedge_sell_base = ZERO
        self._hedge_order_side: Dict[str, Union[TradeType, tuple[TradeType, Decimal]]] = {}
        self._hedge_credited_base: Dict[str, Decimal] = {}
        # JEP-186: per-order cumulative NATIVE (pre-FX) quote credited so far, used to
        # reconstruct the marginal fill price when the lost-event reconcile path supplies a
        # cumulative-AVERAGE price for a delta that follows an earlier, differently-priced
        # partial. Keyed identically to _hedge_credited_base and evicted alongside it.
        self._hedge_credited_native_quote: Dict[str, Decimal] = {}
        self._hedge_terminal_ids = OrderedDict()
        # MONEY-CRITICAL (cancel/fill race): a maker order popped on cancel/fail can still
        # emit a late cross fill. _maker_terminal_ids records those popped ids so the fill
        # is ACCOUNTED (not dropped as "unknown") -> the hedge fires. _maker_credited_base
        # watermarks the cumulative base already credited (via maker_orders OR terminal
        # branch) so a fill is never double-counted across the popped transition.
        self._maker_terminal_ids = OrderedDict()
        self._maker_credited_base: Dict[str, Decimal] = {}
        self._maker_placed_edge_bps: Dict[str, Decimal] = {}

        # JEP-221 #2: HARD per-boot order-rate circuit breaker. A live-confirmed runaway
        # (2026-06-25 NXT) churned ~6 maker placements/s for ~72s because close fills were
        # dropped as "unknown" (Q never converged) and _should_reprice bypasses the reprice
        # rate limit when no maker order rests. This latch is a backstop INDEPENDENT of that
        # root cause: more than max_placements_per_window real maker placements within
        # placement_rate_window_s latches a per-boot HALT (cancel all makers + suppress all
        # further placement until restart). Defaults are generous (won't trip normal reprice,
        # which is itself rate-limited while orders rest) but catch sustained churn. The
        # JEP-218 clock watchdog cannot catch this (the clock keeps advancing).
        self._maker_placement_ts: list = []
        self._rate_halt_latched = False
        self._open_edge_base = ZERO
        self._open_edge_notional_bps = ZERO
        self._open_edge_vwap = ZERO

        self._current_retries = 0
        self._max_retries = max_retries
        self._seed_adopted = False
        self._seed_fail_closed = False
        self._seed_adopting = False
        self._seed_perp_basis_quote = ZERO
        self._seed_readiness_timeout = 20.0  # HL position populate takes 5-12s (spec decision 2)
        self._seed_readiness_interval = 0.1

        # Hedge kill-switch: tripped after max_retries CONSECUTIVE hedge failures (e.g. a
        # hedge-venue health failure / lost orders). When tripped, the maker gate closes
        # (stop quoting + cancel resting makers every tick + hold position, no liquidation)
        # while hedging of the remaining pending continues, so it recovers in place when the
        # venue returns. Reversible (an operator can clear it) -> a false trip is conservative.
        self._hedge_kill_switch = False
        self._init_ws_staleness_state()

        # JEP-184: read-only per-tick latency profiling (off unless config opts in).
        self._latency_recorder = None
        if getattr(config, "latency_profiling", False):
            try:
                from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.latency_recorder import (
                    LatencyRecorder,
                )
                self._latency_recorder = LatencyRecorder(symbol=maker_market.trading_pair)
            except Exception:
                self._latency_recorder = None

        subscribed = [self.maker_connector, self.hedge_connector]
        for extra in connectors or []:
            if extra and extra not in subscribed:
                subscribed.append(extra)

        super().__init__(
            strategy=strategy,
            connectors=subscribed,
            config=config,
            update_interval=update_interval,
        )

    def on_stop(self):
        super().on_stop()
        rec = getattr(self, "_latency_recorder", None)
        if rec is not None:
            rec.close()

    # ============================================================ abstract hooks

    def _evaluate_session_state(self) -> None:
        """JEP-226: compute + cache ``self._session_halt_state`` for this tick. Base = no-op
        (no session venue); subclasses with a SessionCalendar/HaltSource override. Called from
        ``control_task`` before every ``_process_hedges`` so the hedge gate sees fresh state."""
        return

    @abstractmethod
    def _gates_open(self) -> bool:
        """Return ``True`` when it is safe to quote (kill-switch off, data ready)."""
        raise NotImplementedError

    @abstractmethod
    def _compute_targets(self) -> List:
        """Compute the maker order target(s) to quote this tick.

        Returns a strategy-defined list (e.g. ladder rungs, or a single maker quote).
        Returned objects are only consumed by ``_should_reprice`` / ``_place_targets``,
        which the same subclass implements, so their shape is private to the subclass.
        """
        raise NotImplementedError

    @abstractmethod
    def _should_reprice(self, targets: List) -> bool:
        """Reprice guard: return ``True`` to cancel-and-replace maker orders."""
        raise NotImplementedError

    @abstractmethod
    def _place_targets(self, targets: List) -> None:
        """Translate ``targets`` into ``place_order`` calls + ``maker_orders`` entries."""
        raise NotImplementedError

    @abstractmethod
    def _size_hedge(self, pending_base: Decimal) -> Optional[Dict]:
        """Convert pending base into a concrete hedge order spec, or ``None`` to skip.

        Expected return shape (consumed by :meth:`_process_hedges`)::

            {"amount": Decimal, "price": Decimal, "order_type": OrderType,
             "metadata": Optional[Dict]}

        ``amount`` must already be quantized; return ``None`` (or amount <= 0) to skip
        hedging this tick (e.g. missing book, sub-minimum size).
        """
        raise NotImplementedError

    @abstractmethod
    def _maker_balance_candidate(self) -> Optional[OrderCandidate]:
        """Return the maker ``OrderCandidate`` for ``validate_sufficient_balance``.

        Return ``None`` to skip the balance check this tick (e.g. no price yet).
        """
        raise NotImplementedError

    def _pnl_gross_quote(self, matched: Decimal, maker_avg: Decimal, hedge_avg: Decimal) -> Decimal:
        """Gross PnL on ``matched`` base, before fees.

        Default convention matches both current executors: when the maker leg SELLs
        (and hedge BUYs) profit is ``(maker_avg - hedge_avg) * matched``; otherwise the
        signs flip. Subclasses may override for venue-specific sign conventions.
        """
        if self.entry_side == TradeType.SELL:
            return (maker_avg - hedge_avg) * matched
        return (hedge_avg - maker_avg) * matched

    def _init_ws_staleness_state(self) -> None:
        # JEP-134 WS-staleness kill switch (off unless config enables it).
        self._staleness_kill_switch = False
        self._staleness_since_ts: Optional[float] = None
        self._maker_ws_stale = False
        self._hedge_ws_stale = False
        self._maker_ws_age_s: Optional[float] = None
        self._hedge_ws_age_s: Optional[float] = None
        self._hedge_suppress_logged = False

    def _ws_freshness_sec(self, connector: str, pair: str) -> Optional[float]:
        try:
            provider = getattr(self._strategy, "market_data_provider", None)
            if provider is None:
                return None
            freshness = getattr(provider, "get_ws_freshness_sec", None)
            if freshness is None:
                return None
            return freshness(connector, pair)
        except Exception:
            return None

    @staticmethod
    def _leg_ws_stale(age: Optional[float], max_age: Optional[float]) -> bool:
        if max_age is None:
            return False
        if age is None:
            return True
        return age > max_age

    def _evaluate_ws_staleness(self) -> None:
        """JEP-134: per-leg WS-orderbook staleness flags and Stage-2 latch."""
        if not getattr(self.config, "ws_staleness_kill_switch_enabled", False):
            return

        maker_age = self._ws_freshness_sec(self.maker_connector, self.maker_trading_pair)
        hedge_age = self._ws_freshness_sec(self.hedge_connector, self.hedge_trading_pair)
        self._maker_ws_age_s = maker_age
        self._hedge_ws_age_s = hedge_age
        self._maker_ws_stale = self._leg_ws_stale(maker_age, getattr(self.config, "max_hl_ws_age_s", None))
        self._hedge_ws_stale = self._leg_ws_stale(hedge_age, getattr(self.config, "max_kis_ws_age_s", None))

        any_stale = self._maker_ws_stale or self._hedge_ws_stale
        now = self._strategy.current_timestamp
        grace = float(getattr(self.config, "ws_staleness_grace_s", 90.0))
        if any_stale:
            if self._staleness_since_ts is None:
                self._staleness_since_ts = now
                self.logger().warning(
                    "JEP-134 WS staleness: maker_stale=%s (age=%s) hedge_stale=%s (age=%s); "
                    "pausing quoting + suppressing stale-priced hedges, grace=%.0fs.",
                    self._maker_ws_stale,
                    maker_age,
                    self._hedge_ws_stale,
                    hedge_age,
                    grace,
                )
            elif (now - self._staleness_since_ts) >= grace and not self._staleness_kill_switch:
                self._staleness_kill_switch = True
                self.logger().error(
                    "JEP-134 WS continuously stale >= %.0fs: tripping the staleness kill-switch "
                    "(HOLD: stop quoting, cancel makers, hold position, suppress stale hedges). "
                    "Manual restart required once WS recovers.",
                    grace,
                )
        else:
            if self._staleness_since_ts is not None:
                self.logger().info("JEP-134 WS feeds recovered; clearing staleness timer.")
            self._staleness_since_ts = None
            self._hedge_suppress_logged = False

    # ============================================================ lifecycle

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            await self._seed_inventory_from_connector()
            # JEP-226: refresh the cached session-halt state BEFORE any _process_hedges call.
            # The seed-pending branch below calls _process_hedges before _evaluate_ws_staleness,
            # so this must run at the very top to keep the hedge gate fresh on every path.
            self._evaluate_session_state()
            if self._seed_pending():
                # JEP-210: an adopt:true seed is still in progress (not yet adopted, not yet
                # fail-closed). Do not quote -- otherwise the executor would place opens before
                # recognizing the held inventory, over-exposing during the cold-boot retry grace.
                # Lives here (base) so every subclass -- ladder AND A&S -- is covered.
                self._cancel_all_maker()
                self._process_hedges()
                return
            self._evaluate_ws_staleness()
            if not self._gates_open():
                self._cancel_all_maker()
                self._process_hedges()
                return
            self._reconcile_maker()
            self._process_hedges()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            self._evaluate_ws_staleness()
            self._evaluate_session_state()  # JEP-226: _control_shutdown -> _process_hedges needs fresh state
            await self._control_shutdown()

    def early_stop(self, keep_position: bool = False, flatten: bool = False):
        if flatten:
            self._flatten_on_stop = True
            self._flatten_started_ts = self._strategy.current_timestamp
            self.close_type = CloseType.EARLY_STOP
            self._status = RunnableStatus.SHUTTING_DOWN
            return
        if keep_position:
            self.close_type = CloseType.POSITION_HOLD
            self.stop()
            return
        self.close_type = CloseType.EARLY_STOP
        self._status = RunnableStatus.SHUTTING_DOWN

    async def _control_shutdown(self):
        if getattr(self, "_flatten_on_stop", False) and self._flatten_unwind_step():
            return
        self._cancel_all_maker()
        # Best-effort: hedge any remaining unhedged fills before terminating.
        self._process_hedges()
        if not self._has_open_orders() and self._unhedged_base() <= ZERO:
            self.stop()

    def _flatten_unwind_step(self) -> bool:
        """Override to actively unwind held inventory during a flatten shutdown.

        Return True while still unwinding (skip the cancel+stop), False when there is
        nothing left to flatten (let the base finish).
        """
        return False

    # ============================================================ maker reconcile

    def _is_two_sided(self) -> bool:
        return getattr(self.config, "two_sided", False) is True

    def _reconcile_maker(self) -> None:
        """Selective cancel/replace: reprice only the rungs that actually changed.

        Generic partial-diff shared by every hedge executor (JEP-145). Builds the live
        resting ladder (incl. just-placed inflight orders via ``_maker_placed_rung`` so
        they are not double-placed), diffs it against this tick's targets, cancels only
        the unmatched LIVE orders (never an inflight one — its create task may still be
        racing to the exchange), and places only the unmatched targets. A side with an
        unmatched inflight order is withheld this tick (that inflight order may still be
        the side's rung). ``observe`` short-circuits to a no-submit full-ladder snapshot.

        Cancel-all is retained on ``_cancel_all_maker`` for the gates-closed (kill-switch)
        and wind_down paths, which deliberately do NOT partial-diff.
        """
        if self._rate_halted:
            # JEP-221 breaker latched: makers already cancelled on trip; place nothing.
            return
        rec = getattr(self, "_latency_recorder", None)
        if rec is not None:
            try:
                rec.tick_start(
                    maker_freshness_ms=self._book_freshness_ms(self.maker_connector, self.maker_trading_pair),
                    fair_freshness_ms=self._book_freshness_ms(self.hedge_connector, self.hedge_trading_pair),
                    two_sided=self._is_two_sided(),
                    ts_wall=self._strategy.current_timestamp,
                )
            except Exception:
                rec = None
        try:
            # JEP-177 Fix #2: adopt any maker stuck at order is None (lost created event) BEFORE
            # this tick's diff, so the adopted order is visible and the side unfreezes. No-op when
            # reconcile_stuck_makers_enabled is off (default) -> behavior-neutral.
            self._reconcile_stuck_makers()
            targets = self._compute_targets()
            if rec is not None:
                rec.mark("compute")
            reprice = self._should_reprice(targets)
            if rec is not None:
                rec.mark("decision")
            if not reprice:
                return
            if getattr(self.config, "observe", False):
                self._place_targets(targets)
                if rec is not None:
                    rec.mark("submit")
                return
            if hasattr(self, "_maker_placed_rung"):
                # Bound to ACTIVE makers only (inflight: order is None, or open). A filled maker
                # stays in maker_orders (pre-existing retention), but its rung is never read again
                # — only inflight rungs are injected into the diff — so dropping it here caps
                # _maker_placed_rung at the live+inflight order count instead of tracking the
                # unbounded maker_orders retention (JEP-145 adversarial review, MEDIUM).
                active_ids = {oid for oid, o in self.maker_orders.items() if o.order is None or o.order.is_open}
                self._maker_placed_rung = {
                    oid: rung for oid, rung in self._maker_placed_rung.items() if oid in active_ids
                }
            inflight_ids = {oid for oid, o in self.maker_orders.items() if o.order is None}
            resting = self._resting_maker_orders()
            for oid in inflight_ids:
                rung = getattr(self, "_maker_placed_rung", {}).get(oid)
                if rung is None:
                    continue
                side, price, size = rung
                resting.append(RestingOrder(order_id=oid, side=side, price=price, size=size))
            diff = diff_ladder_targets(
                resting,
                targets,
                self.config.maker_tick,
                self.config.min_reprice_delta_ticks,
            )
            unmatched_inflight = [oid for oid in diff.to_cancel if oid in inflight_ids]
            placed_rung = getattr(self, "_maker_placed_rung", {})
            # Each unmatched in-flight rung as (side, quantized_price). JEP-177 Fix #5:
            # block_targets defaults to the round-2 whole-side block (granular=False), and
            # only suppresses by (side, price-bucket) when maker_inflight_block_rung_granular
            # is opted in — so a genuinely-new far same-side rung is no longer deferred.
            inflight_conflicts = [
                (placed_rung[oid][0], placed_rung[oid][1])
                for oid in unmatched_inflight
                if oid in placed_rung
            ]
            granular = getattr(self.config, "maker_inflight_block_rung_granular", False)
            radius_ticks = getattr(self.config, "maker_inflight_block_radius_ticks", Decimal("4"))
            radius = Decimal(str(radius_ticks)) * self.config.maker_tick
            to_place = blocked_targets(
                diff.to_place,
                inflight_conflicts,
                radius=radius,
                granular=bool(granular),
            )
            for oid in diff.to_cancel:
                if oid in inflight_ids:
                    continue
                self._strategy.cancel(self.maker_connector, self.maker_trading_pair, oid)
            self._place_targets_subset(to_place)
            if rec is not None:
                rec.mark("submit")
            self._last_reprice_ts = self._strategy.current_timestamp
        finally:
            if rec is not None:
                rec.tick_end()

    def _book_freshness_ms(self, connector_name: str, trading_pair: str):
        """Monotonic book age in ms (JEP-184); None when unavailable. Read-only, never raises."""
        try:
            mdp = getattr(self._strategy, "market_data_provider", None)
            if mdp is None:
                return None
            sec = mdp.get_order_book_freshness_sec(connector_name, trading_pair)
            return None if sec is None else float(sec) * 1000.0
        except Exception:
            return None

    def _resting_maker_orders(self) -> List[RestingOrder]:
        conn = self.connectors[self.maker_connector]
        pair = self.maker_trading_pair
        out: List[RestingOrder] = []
        for o in self._open_maker_orders():
            if o.order is None:
                continue
            side = Side.BUY if o.order.trade_type == TradeType.BUY else Side.SELL
            out.append(
                RestingOrder(
                    order_id=o.order_id,
                    side=side,
                    price=conn.quantize_order_price(pair, o.order.price),
                    size=conn.quantize_order_amount(pair, o.order.amount),
                )
            )
        return out

    def _place_maker(
        self,
        price: Decimal,
        amount: Decimal,
        edge_bps: Decimal,
        side: Optional[TradeType] = None,
        position_action: PositionAction = PositionAction.OPEN,
    ) -> Optional[str]:
        """Placement hook: submit one maker order, track it in ``maker_orders``, return its id.

        Soft hook (raises ``NotImplementedError`` if unimplemented) rather than
        ``@abstractmethod`` so the accounting-only test harnesses that subclass this base but
        never quote do not have to implement it. Concrete executors override it (the ladder
        records ``_maker_placed_edge_bps`` and honors observe-mode no-submit here).

        Contract: return the placed order's client id, or ``None`` if no order was submitted
        (observe-mode no-submit, sub-minimum size, etc.). The BASE — not the subclass —
        records ``_maker_placed_rung`` from this id (see ``_place_target_one`` /
        ``_record_placed_rung``), so the inflight double-place guard holds for ANY subclass
        that merely returns its placed id.
        """
        raise NotImplementedError

    def _place_target_one(self, target: RungTarget) -> None:
        if self._rate_halted:
            # JEP-221 breaker latched -> never place again this boot.
            return
        if self._is_two_sided():
            side = TradeType.SELL if target.side == Side.SELL else TradeType.BUY
            position_action = PositionAction.OPEN if side == self.entry_side else PositionAction.CLOSE
            order_id = self._place_maker(
                target.price,
                target.size,
                target.edge_bps,
                side=side,
                position_action=position_action,
            )
        else:
            order_id = self._place_maker(target.price, target.size, target.edge_bps)
        self._record_placed_rung(order_id, target)
        if order_id is not None:
            # JEP-221: count only REAL placements (observe / sub-min / refused return None).
            self._note_maker_placement()

    def _record_placed_rung(self, order_id: Optional[str], target: RungTarget) -> None:
        """Record a just-placed rung so the next reconcile's partial-diff sees it while inflight.

        Recorded in the BASE (not the subclass ``_place_maker``) so EVERY hedge executor's
        partial-diff is double-place-safe by construction — a subclass only has to return the
        order id it placed. Quantized with the maker connector so the recorded rung matches the
        resting-order representation built by ``_resting_maker_orders``. ``order_id is None``
        (no submit / observe / sub-min) records nothing.
        """
        if order_id is None:
            return
        conn = self.connectors[self.maker_connector]
        pair = self.maker_trading_pair
        if not hasattr(self, "_maker_placed_rung"):
            self._maker_placed_rung = {}
        self._maker_placed_rung[order_id] = (
            target.side,
            conn.quantize_order_price(pair, target.price),
            conn.quantize_order_amount(pair, target.size),
        )

    def _place_targets_subset(self, targets: List[RungTarget]) -> None:
        for target in targets:
            self._place_target_one(target)
        self._last_reprice_ts = self._strategy.current_timestamp

    def _cancel_all_maker(self) -> None:
        for order in self._open_maker_orders():
            self._strategy.cancel(self.maker_connector, self.maker_trading_pair, order.order_id)

    # ============================================ JEP-221 #2 order-rate circuit breaker

    def _placement_rate_config(self) -> tuple:
        """(enabled, window_s, cap). Read lazily via getattr so configs/harnesses missing
        the keys default to ON with generous bounds (safety backstop, default-on)."""
        cfg = getattr(self, "config", None)
        enabled = getattr(cfg, "placement_rate_breaker_enabled", True)
        window = float(getattr(cfg, "placement_rate_window_s", 30.0) or 30.0)
        cap = int(getattr(cfg, "max_placements_per_window", 80) or 80)
        return enabled, window, cap

    @property
    def _rate_halted(self) -> bool:
        return getattr(self, "_rate_halt_latched", False)

    def _note_maker_placement(self) -> None:
        """Record one REAL maker placement and latch the breaker if the rolling rate within
        ``placement_rate_window_s`` exceeds ``max_placements_per_window``. Call only for a
        submitted order (order_id is not None); observe/no-submit must not count."""
        enabled, window, cap = self._placement_rate_config()
        if not enabled:
            return
        now = self._strategy.current_timestamp
        ts = getattr(self, "_maker_placement_ts", None)
        if ts is None:
            ts = self._maker_placement_ts = []
        ts.append(now)
        cutoff = now - window
        if ts and ts[0] < cutoff:
            ts = self._maker_placement_ts = [t for t in ts if t >= cutoff]
        if len(ts) > cap:
            self._trip_rate_halt(len(ts), window)

    def _trip_rate_halt(self, count: int, window: float) -> None:
        # INTENTIONALLY a terminal per-boot halt: cancel resting makers + suppress all further
        # placement, leave the process ALIVE, and require an operator restart. Do NOT wire this
        # to os._exit / self-heal (cf. the JEP-218 clock watchdog) — a placement STORM that
        # survived an auto-restart would just re-trip and crash-loop the live book. Halting in
        # place is the correct fail-safe: it stops the bleeding and forces human diagnosis.
        if getattr(self, "_rate_halt_latched", False):
            return
        self._rate_halt_latched = True
        self.logger().critical(
            "JEP-221 order-rate circuit breaker TRIPPED for %s: %s maker placements within "
            "%.0fs exceeded max_placements_per_window — latching per-boot HALT (cancelling all "
            "resting makers, suppressing all further maker/hedge placement until restart).",
            getattr(self, "maker_trading_pair", "?"), count, window,
        )
        try:
            self._cancel_all_maker()
        except Exception:
            self.logger().exception("JEP-221 breaker: _cancel_all_maker raised during halt.")

    # ============================================================ hedge queue

    def _hedge_in_flight(self) -> bool:
        """At most one hedge order may be live at a time (double-hedge prevention).

        A just-placed order has ``order is None`` until its created event arrives;
        that window counts as in-flight too — otherwise, since pending is no longer
        zeroed at placement, the next tick would re-submit the same residual.
        """
        return any(o.order is None or o.order.is_open for o in self.hedge_orders.values())

    @property
    def _pending_hedge_base(self) -> Decimal:
        return abs(self._pending_hedge_signed)

    def _ensure_direction_accounting(self) -> None:
        for attr in (
            "_maker_buy_base",
            "_maker_sell_base",
            "_hedge_buy_base",
            "_hedge_sell_base",
            "_perp_cash",
            "_spot_cash",
            "_open_edge_base",
            "_open_edge_notional_bps",
            "_open_edge_vwap",
        ):
            if not hasattr(self, attr):
                setattr(self, attr, ZERO)
        if not hasattr(self, "_pending_hedge_signed"):
            self._pending_hedge_signed = ZERO
        if not hasattr(self, "_hedge_order_side"):
            self._hedge_order_side = {}
        if not hasattr(self, "_hedge_credited_base"):
            self._hedge_credited_base = {}
        if not hasattr(self, "_hedge_credited_native_quote"):
            self._hedge_credited_native_quote = {}
        if not hasattr(self, "_hedge_terminal_ids"):
            self._hedge_terminal_ids = OrderedDict()
        if not hasattr(self, "_maker_terminal_ids"):
            self._maker_terminal_ids = OrderedDict()
        if not hasattr(self, "_maker_credited_base"):
            self._maker_credited_base = {}
        if not hasattr(self, "_maker_placed_edge_bps"):
            self._maker_placed_edge_bps = {}

    def _remember_terminal_hedge_order(self, order_id: str, allow_event_delta_fallback: bool = False) -> None:
        self._ensure_direction_accounting()
        self._hedge_terminal_ids.pop(order_id, None)
        self._hedge_terminal_ids[order_id] = allow_event_delta_fallback
        while len(self._hedge_terminal_ids) > self._HEDGE_TERMINAL_ID_CAP:
            oldest_order_id, _ = self._hedge_terminal_ids.popitem(last=False)
            self._hedge_order_side.pop(oldest_order_id, None)
            self._hedge_credited_base.pop(oldest_order_id, None)
            self._hedge_credited_native_quote.pop(oldest_order_id, None)

    def _remember_terminal_maker_order(self, order_id: str) -> None:
        """Record a popped MAKER order so a late cross fill (cancel/fill race) is accounted.

        Mirrors _remember_terminal_hedge_order: record the id, cap-evict the oldest along
        with its credited-base watermark. Without this a maker order popped on cancel/fail
        whose fill arrives afterwards falls through to the "unknown order" else-branch and
        is DROPPED, leaving the perp position change unhedged.
        """
        self._ensure_direction_accounting()
        self._maker_terminal_ids.pop(order_id, None)
        self._maker_terminal_ids[order_id] = True
        while len(self._maker_terminal_ids) > self._MAKER_TERMINAL_ID_CAP:
            oldest_order_id, _ = self._maker_terminal_ids.popitem(last=False)
            self._maker_credited_base.pop(oldest_order_id, None)

    @staticmethod
    def _signed_base(side: TradeType, amount: Decimal) -> Decimal:
        return amount if side == TradeType.BUY else -amount

    def _hedge_price_to_maker_quote(self, price: Decimal, side: TradeType) -> Decimal:
        """Convert a hedge-leg fill price into the maker-leg quote unit.

        Default identity: the base is a generic two-venue machine, and tests inject
        pre-converted prices. ``LadderMakerExecutor`` overrides this to divide a KRW
        spot fill by the fill-time, side-aware FX rate so ``_spot_cash`` accrues in USD
        (the maker leg's quote), keeping the round-trip PnL single-currency. ``side`` is
        the hedge fill side, threaded so the override can mirror ``compute_fair_price``'s
        side-aware FX pairing (hedge BUY <-> maker SELL fx_bid; hedge SELL <-> maker BUY
        fx_ask).
        """
        return price

    def _residual_mark_price(self) -> Decimal:
        """Mark price for the naked (unhedged) residual in two-sided cash-flow PnL.

        Default = the maker execution VWAP. Marking the open leg at its OWN entry price
        contributes zero unrealized PnL on the unhedged portion, which makes the
        cash-flow PnL a *strict generalization* of the legacy matched-quantity PnL: on
        any single-direction sequence ``_perp_cash + _spot_cash + naked*maker_avg - fees``
        reduces to the legacy matched spread (verified full / partial / reconcile, both
        sides). The executor layer may later override this to a live fair mark so an
        open two-sided inventory is marked to market instead of to entry.
        """
        if self._maker_executed_base > ZERO:
            return self._maker_executed_quote / self._maker_executed_base
        return ZERO

    def _hedge_base_to_maker_base(self, amount: Decimal) -> Decimal:
        """Convert a hedge-leg base amount (e.g. KIS shares) into maker-leg base units.

        Default identity (1 hedge unit == 1 maker unit). ``LadderMakerExecutor`` overrides
        to divide by ``share_per_unit`` so the signed inventory ledgers (``_spot_net`` /
        ``_pending_hedge_signed``) net against ``_perp_net`` in the SAME unit. Without this
        seam a ``share_per_unit != 1`` hedge fill would credit shares into a units-denominated
        signed pending queue and flip its sign (the old non-negative ``_pending_hedge_base``
        masked this by clamping at 0). The notional accumulators (``_hedge_executed_quote``
        / ``_spot_cash``) keep the RAW hedge BASE amount but pass the PRICE through
        ``_hedge_price_to_maker_quote`` (identity in the base; KRW->USD side-aware FX in the
        ladder, JEP-185) — so the generic / fx=1 legacy matched PnL stays byte-identical
        while a KRW hedge leg accrues quote in the maker leg's currency.
        """
        return amount

    def _maker_fill_side(self, event: OrderFilledEvent) -> TradeType:
        return getattr(event, "trade_type", None) or self.entry_side

    def _record_maker_fill_side(self, side: TradeType, amount: Decimal) -> None:
        if side == TradeType.BUY:
            self._maker_buy_base += amount
        else:
            self._maker_sell_base += amount

    def _record_hedge_fill_side(self, side: TradeType, amount: Decimal) -> None:
        if side == TradeType.BUY:
            self._hedge_buy_base += amount
        else:
            self._hedge_sell_base += amount

    def _record_open_edge(self, order_id: str, side: TradeType, amount: Decimal) -> None:
        # Opening = a maker fill in the entry direction (accumulate basis); closing = the
        # opposite side (consume basis at average cost). Keyed off entry_side, not a
        # hardcoded SELL, so a BUY-open (long-biased) two-sided executor is correct too.
        if side == self.entry_side:
            placed_edge = self._maker_placed_edge_bps.get(order_id, ZERO)
            self._open_edge_base += amount
            self._open_edge_notional_bps += placed_edge * amount
        elif self._open_edge_base > ZERO:
            close_amount = min(amount, self._open_edge_base)
            self._open_edge_notional_bps -= self._open_edge_vwap * close_amount
            self._open_edge_base -= close_amount

        if self._open_edge_base <= ZERO:
            self._open_edge_base = ZERO
            self._open_edge_notional_bps = ZERO
            self._open_edge_vwap = ZERO
        else:
            self._open_edge_vwap = self._open_edge_notional_bps / self._open_edge_base

    def _hedge_fill_side(self, order_id: str) -> TradeType:
        recorded = self._hedge_order_side.get(order_id)
        if isinstance(recorded, tuple):
            return recorded[0]
        return recorded or self.hedge_side

    def _credit_hedge_fill(
        self,
        order_id: str,
        observed_cumulative: Decimal,
        price: Optional[Decimal],
        price_is_cumulative_avg: bool = False,
    ) -> None:
        """Credit a hedge fill's marginal base/quote once, watermarked by cumulative base.

        ``price`` is normally the per-fill MARGINAL price (the event path). The
        lost-lifecycle-event reconcile path can only read the order's cumulative AVERAGE
        price (``average_executed_price``); it passes ``price_is_cumulative_avg=True`` so
        this method reconstructs the marginal price for the new ``delta``. Without that, a
        ``delta`` that follows an earlier, differently-priced partial would be credited at
        the blended average, mis-attributing quote/PnL (JEP-186). The reconstruction backs
        out the marginal native quote = ``avg * observed_cumulative - already_native``, then
        ``marginal_px = marginal_native / delta``; with no prior credit it reduces to the
        average exactly, so the clean lost-then-filled case is byte-identical.
        """
        already = self._hedge_credited_base.get(order_id, ZERO)
        if order_id not in self.hedge_orders and observed_cumulative <= already:
            observed_cumulative = already + observed_cumulative
        delta = observed_cumulative - already
        if delta <= ZERO:
            return
        self._hedge_credited_base[order_id] = observed_cumulative
        side = self._hedge_fill_side(order_id)
        # Notional/legacy accumulators use the RAW hedge amount (money + byte-identical
        # legacy matched PnL); the signed delta ledgers use maker-unit-converted base so
        # _spot_net nets against _perp_net in one unit (see _hedge_base_to_maker_base).
        self._hedge_executed_base += delta
        if price is not None and not price.is_nan():
            already_native = self._hedge_credited_native_quote.get(order_id, ZERO)
            marginal_price = price
            if price_is_cumulative_avg and already > ZERO:
                # Reconstruct the marginal native price for this delta from the cumulative
                # average and what was already credited natively. Guard the rare case where
                # a prior delta was credited base-only (NaN price, no native baseline): if
                # the back-out yields a non-positive marginal price, fall back to the
                # average (current behavior) rather than crediting a negative/zero price.
                marginal_native = price * observed_cumulative - already_native
                reconstructed = marginal_native / delta
                if reconstructed > ZERO:
                    marginal_price = reconstructed
            # JEP-185: convert the hedge fill price to the maker quote unit ONCE and use it
            # for BOTH notional accumulators, so neither carries the hedge's native
            # currency. Base default is identity (generic two-venue / fx=1); the ladder
            # override divides KRW by the side-aware FX. This keeps the matched
            # (single-sided) ``hedge_avg`` AND the two-sided ``_spot_cash`` single-currency.
            quote_px = self._hedge_price_to_maker_quote(marginal_price, side)
            self._hedge_executed_quote += delta * quote_px
            self._spot_cash += quote_px * delta * (ONE if side == TradeType.SELL else -ONE)
            # Track the native (pre-FX) quote credited so a later reconcile can back out the
            # next marginal price correctly.
            self._hedge_credited_native_quote[order_id] = already_native + marginal_price * delta
        maker_base = self._hedge_base_to_maker_base(delta)
        self._record_hedge_fill_side(side, maker_base)
        self._pending_hedge_signed += self._signed_base(side, maker_base)

    def _reconcile_stuck_hedges(self) -> None:
        """Resolve hedge orders stuck with ``order is None`` (lost-lifecycle-event guard).

        Runs every tick. ``_hedge_in_flight`` counts an ``order is None`` hedge as
        in-flight (the just-placed window before its created event). If a hedge's
        created/failed/canceled event is ever LOST, the order would otherwise stay
        ``None`` forever and freeze ALL hedging. We consult the connector's order
        tracker — ``get_in_flight_order`` reads ``active_orders + cached_orders`` (the
        30s TTL cache retains the terminal state and fill amount). For each ``order is
        None`` hedge:

          * **connector tracks it (``in_flight is not None``)** -> adopt the
            ``InFlightOrder`` (clears the freeze; the created event being lost no longer
            matters because we read the tracker, not the event). ``order is None`` ⟺ no
            created/fill event was ever processed for it ⟺ exactly 0 was counted, so
            credit any fill that landed before adoption *once*:

              - fully filled (``is_filled``) -> credit the full order ``amount``. The
                connector completes a FILLED order after only a short fill-wait and may
                cache it with ``executed_amount_base`` still lagging behind ``amount``;
                trusting the FILLED *state* over the lagging amount prevents a re-hedge
                that would double up with the late fill.
              - otherwise -> credit ``executed_amount_base`` (a partial on a still-open
                or canceled/failed order). The unfilled remainder stays in
                ``_pending_hedge_base`` and re-hedges.

            A terminal order is then dropped (no further events will come; its fees are
            credited here since no completion event will). A still-open order is kept
            adopted so its remaining fills flow through the normal event path.

          * **connector has no record (``in_flight is None``)** -> do NOT reap/re-hedge.
            ``buy()``/``sell()`` schedule ``_create_order`` via ``safe_ensure_future``
            and return the id *before* ``start_tracking_order`` runs, so a just-placed
            hedge is briefly absent from the tracker; reaping it would pop an order whose
            create task still runs and places on the exchange -> double hedge (the exact
            failure of the reverted tick-watchdog f85b628d8). The next tick will see the
            now-tracked order and adopt it. The only case this leaves stuck is a create
            task cancelled before it ever tracked the order (≈ teardown) -> a visible,
            conservative freeze that never over-hedges and is restart-recoverable.

        Net invariant: this only ever ADOPTS orders the connector is actually tracking
        and credits each fill once; it never re-submits a hedge -> no double hedge.
        """
        self._ensure_direction_accounting()
        for order_id, tracked in list(self.hedge_orders.items()):
            if tracked.order is not None:
                continue
            in_flight = self.get_in_flight_order(self.hedge_connector, order_id)
            if in_flight is None:
                # Just-placed (tracker not yet populated) or create task cancelled.
                # Never reap here — see docstring. Wait for the next tick to adopt.
                continue
            # Adopt the connector's truth. order was None => 0 counted for this order.
            tracked.order = in_flight
            filled_base = in_flight.amount if in_flight.is_filled else (in_flight.executed_amount_base or ZERO)
            if filled_base > ZERO:
                price = in_flight.average_executed_price
                # JEP-186: average_executed_price is a CUMULATIVE-AVERAGE over all fills of
                # this order. If an earlier partial was already credited at a different
                # (marginal) price, crediting the recovered delta at this blended average
                # mis-attributes quote/PnL. Flag it so _credit_hedge_fill reconstructs the
                # marginal price for the delta. The in_flight.price fallback is the order's
                # limit price (a per-order constant), so it is NOT a cumulative average.
                price_is_cumulative_avg = price is not None and not price.is_nan()
                if not price_is_cumulative_avg:
                    price = in_flight.price
                # Guard the maker hedge's NaN market price from poisoning quote/PnL;
                # crediting base-only slightly under-reports quote for this rare order.
                self._credit_hedge_fill(
                    order_id, filled_base, price, price_is_cumulative_avg=price_is_cumulative_avg
                )
            if in_flight.is_done:
                # No completion event will arrive (its events were lost): credit fees here
                # (for a still-open order the normal completion event credits them instead).
                self._hedge_fees_quote += in_flight.cumulative_fee_paid(in_flight.quote_asset)
                self.hedge_orders.pop(order_id, None)
                self._remember_terminal_hedge_order(order_id)
                if not in_flight.is_filled:
                    # Terminated without completing the hedge (lost cancel/failure): count
                    # it toward max_retries so a persistently failing hedge cannot loop.
                    self._current_retries += 1
                    self.evaluate_max_retries()

    def _reconcile_open_hedges(self) -> None:
        """JEP-219: cancel a hedge order that has rested OPEN past ``_hedge_fill_timeout_s``.

        ``_reconcile_stuck_hedges`` only resolves ``order is None`` (lost-created-event) hedges;
        a hedge that is TRACKED and OPEN but not filling (a marketable limit parked on a thin or
        momentarily-empty book) is invisible to it, yet ``_hedge_in_flight`` keeps blocking every
        new hedge while it rests and ``_pending_hedge`` only decrements on a real fill -> the perp
        leg stays naked for the resting order's whole lifetime. Cancelling it frees the in-flight
        slot; pending is untouched (it only drops on fills) so the next tick re-prices the hedge at
        the current book. No double-hedge: a fill that races the cancel still decrements pending via
        its fill event, and the canceled-unfilled case re-hedges the same pending. The guard set
        suppresses re-issuing cancel every tick while the cancel is in flight.
        ``_hedge_fill_timeout_s`` <= 0 (the default) disables this entirely (behavior-neutral)."""
        timeout = getattr(self, "_hedge_fill_timeout_s", 0.0) or 0.0
        if timeout <= 0:
            return
        requested = getattr(self, "_hedge_cancel_requested", None)
        if not isinstance(requested, dict):
            requested = {}
            self._hedge_cancel_requested = requested
        # Drop ids no longer tracked so the map cannot grow unbounded.
        for oid in [o for o in requested if o not in self.hedge_orders]:
            requested.pop(oid, None)
        now = self._strategy.current_timestamp
        for oid, tracked in list(self.hedge_orders.items()):
            order = getattr(tracked, "order", None)
            if order is None or getattr(order, "is_done", False):
                continue
            created = getattr(order, "creation_timestamp", None)
            if created is None:
                continue
            age = now - float(created)
            if age <= timeout:
                continue
            # RETRY the cancel every ``timeout`` seconds while the order stays open: a lost or
            # failed cancel REST call must NOT permanently suppress re-hedging (that would re-open
            # the exact naked-exposure window this guards). last-cancel-ts only rate-limits the
            # retry so we do not spam cancel every tick.
            last = requested.get(oid)
            if last is not None and (now - last) < timeout:
                continue
            requested[oid] = now
            self.logger().warning(
                "JEP-219: hedge %s open %.1fs > timeout %.1fs without filling - cancelling to "
                "re-hedge (retry every %.1fs until terminal; pending held).", oid, age, timeout, timeout,
            )
            self._strategy.cancel(self.hedge_connector, self.hedge_trading_pair, oid)

    def _reconcile_stuck_makers(self) -> None:
        """Resolve maker orders stuck with ``order is None`` (lost-created-event guard).

        The maker analogue of ``_reconcile_stuck_hedges`` (JEP-177 Fix #2). A maker
        ``order is None`` resolves only on a created/filled/cancelled event; if the *created*
        event is PERMANENTLY lost on a genuinely-resting order, the order stays ``None`` forever
        and freezes that side (the reprice diff never sees it, ``_open_maker_orders`` excludes it)
        until a fill or restart — an under-quote-only gap, restart-recoverable. We consult the
        connector's order tracker (``get_in_flight_order`` reads ``active_orders + cached_orders``)
        and, for each ``order is None`` maker:

          * **connector tracks it (``in_flight is not None``)** -> ADOPT it
            (``tracked.order = in_flight``). The order becomes visible to the reprice diff and the
            side unfreezes; the created event being lost no longer matters because we read the
            tracker, not the event. Adopt-only: we do NOT credit fills here — maker fill accounting
            stays exclusively in ``process_order_filled_event`` (so an already-filled-but-event-lost
            order is made visible and its still-pending fill events flow normally; crediting here
            would risk a double-count against the FX-bridged money path, JEP-185).

          * **connector has no record (``in_flight is None``)** -> do NOT reap/cancel. ``buy()`` /
            ``sell()`` schedule ``_create_order`` via ``safe_ensure_future`` and return the id
            *before* ``start_tracking_order`` runs, so a just-placed maker is briefly absent from the
            tracker; reaping or cancelling it could pop/cancel an order whose create task still runs
            and places on the exchange -> orphan/naked order (the live-money "never reap an untracked
            order" invariant). The next tick re-checks; the only case this leaves stuck is a create
            task cancelled before it ever tracked the order (≈ teardown) -> a visible, conservative
            freeze that is restart-recoverable and never over-places.

        Gated behind ``reconcile_stuck_makers_enabled`` (default False = current behavior).

        Net invariant: this only ever ADOPTS orders the connector is actually tracking; it never
        places, re-places, or cancels a maker -> no over-quote, no orphan, no naked exposure.
        """
        if not getattr(self.config, "reconcile_stuck_makers_enabled", False):
            return
        for order_id, tracked in list(self.maker_orders.items()):
            if tracked.order is not None:
                continue
            in_flight = self.get_in_flight_order(self.maker_connector, order_id)
            if in_flight is None:
                # Just-placed (tracker not yet populated) or create task cancelled.
                # Never reap/cancel here — see docstring. Wait for the next tick.
                continue
            tracked.order = in_flight

    def _process_hedges(self) -> None:
        self._ensure_direction_accounting()
        self._reconcile_stuck_hedges()
        self._reconcile_open_hedges()
        if self._rate_halted:
            # JEP-221 breaker latched: existing-hedge reconcile/cancel above is fine, but never
            # PLACE a new hedge (placement is the runaway surface). Freeze until restart.
            return
        if self.status == RunnableStatus.TERMINATED:
            # _reconcile_stuck_hedges may trip evaluate_max_retries() -> stop() (e.g. a
            # terminal stuck hedge exceeding max_retries). Never place a hedge after the
            # executor has been terminated, including during _control_shutdown.
            return
        if self._pending_hedge_signed == ZERO:
            needed_side = None
        else:
            needed_side = TradeType.BUY if self._pending_hedge_signed < ZERO else TradeType.SELL
        for oid, rec in list(self._hedge_order_side.items()):
            tracked = self.hedge_orders.get(oid)
            side = rec[0] if isinstance(rec, tuple) else rec
            if (
                tracked is not None
                and tracked.order is not None
                and not getattr(tracked.order, "is_done", False)
                and side != needed_side
            ):
                self._strategy.cancel(self.hedge_connector, self.hedge_trading_pair, oid)
        if self._pending_hedge_signed == ZERO:
            # JEP-226: naked exposure cleared -> reset the defer timer so a fresh pending
            # never inherits a stale elapsed naked age.
            self._hedge_defer_since_ts = None
            self._hedge_defer_side = None
            self._hedge_defer_logged_kind = None
            return
        if self._hedge_in_flight():
            return
        if getattr(self, "_hedge_ws_stale", False):
            if not self._hedge_suppress_logged:
                self.logger().error(
                    "JEP-134: KIS hedge book WS-stale (age=%s) - suppressing new hedge "
                    "submissions; holding pending=%s until WS recovers.",
                    self._hedge_ws_age_s,
                    self._pending_hedge_base,
                )
                self._hedge_suppress_logged = True
            return
        # JEP-226: session/auction-aware gate. needed_side is non-None here (pending != ZERO).
        # Defer a hedge that cannot fill at a continuous price; force only on a clock-scheduled
        # auction past the cap; hold (never force) on a genuine halt. cap<=0 disables the gate.
        dec = decide_hedge_defer(
            cap=self._hedge_session_defer_cap_s,
            halted=(self._session_halt_state is not None and self._session_halt_state.halted),
            reason=(self._session_halt_state.reason if self._session_halt_state is not None else ""),
            defer_since=self._hedge_defer_since_ts,
            defer_side=self._hedge_defer_side,
            needed_side=needed_side,
            now=self._strategy.current_timestamp,
            force_eligible_reasons=FORCE_ELIGIBLE_HALT_REASONS,
        )
        self._hedge_defer_since_ts = dec.since
        self._hedge_defer_side = dec.side
        if dec.since is None:
            self._hedge_defer_logged_kind = None
        elif dec.kind != self._hedge_defer_logged_kind:
            self.logger().warning(
                "JEP-226 hedge %s: reason=%s pending=%s cap=%.0fs (place=%s).",
                dec.kind,
                self._session_halt_state.reason if self._session_halt_state is not None else "",
                self._pending_hedge_base,
                self._hedge_session_defer_cap_s,
                dec.place,
            )
            self._hedge_defer_logged_kind = dec.kind
        if not dec.place:
            return
        pending_base = self._pending_hedge_base
        spec = self._size_hedge(pending_base)
        if spec is None:
            return
        amount = spec.get("amount", ZERO)
        if amount is None or amount <= ZERO:
            return
        hedge_side = TradeType.BUY if self._pending_hedge_signed < ZERO else TradeType.SELL
        order_id = self.place_order(
            connector_name=self.hedge_connector,
            trading_pair=self.hedge_trading_pair,
            order_type=spec["order_type"],
            side=hedge_side,
            amount=amount,
            price=spec.get("price", Decimal("NaN")),
            metadata=spec.get("metadata") or {"order_role": "hedge"},
        )
        self.hedge_orders[order_id] = TrackedOrder(order_id=order_id)
        self._hedge_order_side[order_id] = (hedge_side, amount)
        # Do NOT zero pending here: single-in-flight prevents double placement, and
        # pending now decrements on hedge FILL events so any unfilled/failed base is
        # re-hedged next tick (no strand on partial fill or order failure).

    # ============================================================ inventory helpers

    def _unhedged_base(self) -> Decimal:
        """Absolute unhedged base (>= 0): maker fills not yet matched by hedge fills."""
        return abs(self._unhedged_base_signed())

    def _unhedged_base_signed(self) -> Decimal:
        """Signed net inventory: positive long, negative short (drives skew / gate)."""
        self._ensure_direction_accounting()
        return self._perp_net() + self._spot_net()

    def _perp_net(self) -> Decimal:
        self._ensure_direction_accounting()
        return self._maker_buy_base - self._maker_sell_base

    def _spot_net(self) -> Decimal:
        self._ensure_direction_accounting()
        return self._hedge_buy_base - self._hedge_sell_base

    def _paired_oi(self) -> Decimal:
        perp_net = self._perp_net()
        spot_net = self._spot_net()
        if perp_net == ZERO or spot_net == ZERO or (perp_net > ZERO) == (spot_net > ZERO):
            return ZERO
        return min(abs(perp_net), abs(spot_net))

    def _open_maker_orders(self) -> List[TrackedOrder]:
        return [o for o in self.maker_orders.values() if o.order is not None and o.order.is_open]

    def _open_hedge_orders(self) -> List[TrackedOrder]:
        return [o for o in self.hedge_orders.values() if o.order is not None and o.order.is_open]

    def _has_open_orders(self) -> bool:
        return any(
            o.order is not None and o.order.is_open
            for o in list(self.maker_orders.values()) + list(self.hedge_orders.values())
        )

    # ============================================================ events

    def _update_tracked(self, connector_name: str, order_id: str) -> None:
        for book in (self.maker_orders, self.hedge_orders):
            tracked = book.get(order_id)
            if tracked is not None:
                in_flight = self.get_in_flight_order(connector_name, order_id)
                if in_flight is not None:
                    tracked.order = in_flight

    def process_order_created_event(
        self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]
    ):
        if event.order_id in self.maker_orders:
            self._update_tracked(self.maker_connector, event.order_id)
        elif event.order_id in self.hedge_orders:
            self._update_tracked(self.hedge_connector, event.order_id)

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        self._ensure_direction_accounting()
        amount = Decimal(event.amount)
        if event.order_id in self.maker_orders:
            self._update_tracked(self.maker_connector, event.order_id)
            maker_side = self._maker_fill_side(event)
            self._maker_executed_base += amount
            self._maker_executed_quote += amount * Decimal(event.price)
            self._perp_cash += amount * Decimal(event.price) * (ONE if maker_side == TradeType.SELL else -ONE)
            self._record_maker_fill_side(maker_side, amount)
            self._record_open_edge(event.order_id, maker_side, amount)
            self._pending_hedge_signed += self._signed_base(maker_side, amount)
            # Advance the credited watermark so that if this order is later popped
            # (cancel/fail) the terminal-maker branch credits only NEW base beyond what
            # was already counted here — no double-count across the popped transition.
            self._maker_credited_base[event.order_id] = (
                self._maker_credited_base.get(event.order_id, ZERO) + amount
            )
        elif event.order_id in self.hedge_orders:
            self._update_tracked(self.hedge_connector, event.order_id)
            # Decrement the hedge queue by what ACTUALLY hedged (fill), not what was
            # placed, so a partial-fill-then-fail re-hedges the remainder instead of
            # stranding it. pending then tracks the fill-truth unhedged need.
            tracked = self.hedge_orders[event.order_id]
            observed = getattr(getattr(tracked, "order", None), "executed_amount_base", None)
            if observed is None:
                # Lagging/absent connector cumulative (or a test double without the
                # attribute): fall back to cumulative-delta = prior watermark + this fill.
                observed = self._hedge_credited_base.get(event.order_id, ZERO) + amount
            self._credit_hedge_fill(event.order_id, Decimal(observed), Decimal(event.price))
            # A successful hedge fill proves the venue is responding: reset the consecutive
            # failure counter so the kill-switch only trips on a *persistent* streak.
            self._current_retries = 0
        elif event.order_id in self._hedge_terminal_ids:
            in_flight = self.get_in_flight_order(self.hedge_connector, event.order_id)
            observed = getattr(in_flight, "executed_amount_base", None)
            if observed is None:
                already = self._hedge_credited_base.get(event.order_id, ZERO)
                if self._hedge_terminal_ids[event.order_id]:
                    observed = already + amount
                else:
                    observed = already
            observed = Decimal(observed)
            already = self._hedge_credited_base.get(event.order_id, ZERO)
            if observed > already:
                self.logger().warning(
                    "Crediting post-optimistic-cancel cross fill on terminal hedge order %s: "
                    "observed cumulative %s, prior credited %s.",
                    event.order_id,
                    observed,
                    already,
                )
                self._credit_hedge_fill(event.order_id, observed, Decimal(event.price))
                self._current_retries = 0
        elif event.order_id in self._maker_terminal_ids:
            # MONEY-CRITICAL: a MAKER order popped on cancel/fail (cancel/fill race) still
            # filled on the venue. Account the cross fill IDENTICALLY to the in-maker_orders
            # branch so the perp position change is reflected and the hedge fires — but
            # credit only the NEW delta beyond what was already counted (watermark) so the
            # fill is never double-counted across the maker_orders -> terminal transition.
            in_flight = self.get_in_flight_order(self.maker_connector, event.order_id)
            observed = getattr(in_flight, "executed_amount_base", None)
            already = self._maker_credited_base.get(event.order_id, ZERO)
            if observed is None:
                # Lagging/absent connector cumulative: fall back to cumulative-delta =
                # prior watermark + this fill (matches the hedge-terminal fallback).
                observed = already + amount
            observed = Decimal(observed)
            delta = observed - already
            if delta > ZERO:
                self.logger().warning(
                    "Crediting post-cancel cross fill on terminal maker order %s: "
                    "observed cumulative %s, prior credited %s.",
                    event.order_id,
                    observed,
                    already,
                )
                maker_side = self._maker_fill_side(event)
                self._maker_credited_base[event.order_id] = observed
                self._maker_executed_base += delta
                self._maker_executed_quote += delta * Decimal(event.price)
                self._perp_cash += delta * Decimal(event.price) * (
                    ONE if maker_side == TradeType.SELL else -ONE
                )
                self._record_maker_fill_side(maker_side, delta)
                self._record_open_edge(event.order_id, maker_side, delta)
                self._pending_hedge_signed += self._signed_base(maker_side, delta)
        else:
            self.logger().warning(
                "Ignoring fill for unknown order %s: not tracked as maker, active hedge, "
                "terminal hedge, or terminal maker.",
                event.order_id,
            )

    def process_order_completed_event(
        self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]
    ):
        self._ensure_direction_accounting()
        if event.order_id in self.maker_orders:
            self._update_tracked(self.maker_connector, event.order_id)
            self._maker_fees_quote += self.maker_orders[event.order_id].cum_fees_quote
        elif event.order_id in self.hedge_orders:
            self._update_tracked(self.hedge_connector, event.order_id)
            self._hedge_fees_quote += self.hedge_orders[event.order_id].cum_fees_quote
            # Drop the completed hedge from BOTH books together. Leaving it in hedge_orders
            # while popping its recorded side let a stray post-completion fill credit at the
            # default self.hedge_side (wrong after a two-sided sign flip) and leaked the dict.
            self.hedge_orders.pop(event.order_id, None)
            self._remember_terminal_hedge_order(event.order_id)

    def process_order_canceled_event(self, _, market, event: OrderCancelledEvent):
        self._ensure_direction_accounting()
        if event.order_id in self.maker_orders:
            self.maker_orders.pop(event.order_id, None)
            # Record the popped maker order: a cancel/fill race can still emit a late
            # cross fill that would otherwise be dropped as "unknown" (naked exposure).
            self._remember_terminal_maker_order(event.order_id)
        # Pop a cancelled hedge too (no leak / no stale in-flight block). Its unfilled
        # base stays in _pending_hedge_base and is re-hedged on the next tick.
        if event.order_id in self.hedge_orders:
            self.hedge_orders.pop(event.order_id, None)
            self._remember_terminal_hedge_order(event.order_id, allow_event_delta_fallback=True)

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        self._ensure_direction_accounting()
        if event.order_id in self.maker_orders:
            self.maker_orders.pop(event.order_id, None)
            # Same cancel/fill-race protection on the FAIL path: a maker order can fail its
            # cancel/replace yet still have filled on the venue first.
            self._remember_terminal_maker_order(event.order_id)
        elif event.order_id in self.hedge_orders:
            self.hedge_orders.pop(event.order_id, None)
            self._remember_terminal_hedge_order(event.order_id, allow_event_delta_fallback=True)
            self._current_retries += 1
            self.evaluate_max_retries()

    def evaluate_max_retries(self):
        """Trip the hedge kill-switch instead of terminating on repeated hedge failure.

        A hedge that fails more than ``max_retries`` CONSECUTIVE times signals a hedge-venue
        health failure (e.g. KIS lost orders / persistent REST errors — both surface as a
        ``MarketOrderFailureEvent`` and increment ``_current_retries``). Rather than the base
        ``CloseType.FAILED`` terminate (which abandons the unhedged position and stops hedging
        entirely), trip ``_hedge_kill_switch``: the maker gate closes so quoting halts and
        resting makers are cancelled every tick (capping the naked exposure the maker leg
        would otherwise keep accumulating), the position is HELD (no market-close), and
        hedging of the outstanding pending keeps retrying so it completes in place once the
        venue recovers. This avoids the orchestrator's POSITION_HOLD hand-off (which the
        ladder config shape does not support) and never terminates, so there is no
        place-after-stop window. Reversible (an operator clears the flag) -> a false trip is
        conservative, not destructive.
        """
        if self._current_retries > self._max_retries and not self._hedge_kill_switch:
            self._hedge_kill_switch = True
            self.logger().error(
                "Hedge failed %s consecutive times (> max_retries=%s): tripping the hedge "
                "kill-switch. Maker quoting halts and resting makers are cancelled; the "
                "position is held and hedging continues for the remaining pending (%s). "
                "Clear _hedge_kill_switch once the hedge venue recovers.",
                self._current_retries, self._max_retries, self._pending_hedge_base,
            )
        # Hard backstop: if the venue stays dead well past the trip, stop hammering it.
        # The kill-switch keeps re-submitting a hedge every tick to recover in place; that
        # is right for a TRANSIENT outage (a fill resets _current_retries long before this),
        # but a truly dead venue would otherwise be hammered forever (worsening rate limits /
        # lockouts) and _control_shutdown would hang on the same path. Past _HEDGE_HARD_STOP_
        # FACTOR x max_retries consecutive failures, terminate (FAILED) as a last resort.
        if self._current_retries > self._max_retries * self._HEDGE_HARD_STOP_FACTOR:
            self.logger().error(
                "Hedge failed %s consecutive times (> %sx max_retries): hedge venue appears "
                "dead; stopping the executor (FAILED) to halt repeated submissions.",
                self._current_retries, self._HEDGE_HARD_STOP_FACTOR,
            )
            self.close_type = CloseType.FAILED
            self.stop()

    # ============================================================ balance / pnl

    async def validate_sufficient_balance(self):
        candidate = self._maker_balance_candidate()
        if candidate is None:
            return
        adjusted = self.adjust_order_candidates(self.maker_connector, [candidate])[0]
        if adjusted.amount == ZERO:
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.stop()

    # JEP-210: grace window for cold-boot seed retries before permanently fail-closing adoption,
    # and a per-REST-op timeout so a hung connector call cannot wedge the seed (and the control loop).
    _SEED_GRACE_SECONDS = 180.0
    _SEED_OP_TIMEOUT = 10.0
    # JEP-210: min seconds between seed-driven hedge balance polls, so actively refreshing
    # the hedge balance (below) does not hammer a throttled endpoint (KIS EGW00215) every
    # 1s control tick across the up-to-180s seed window.
    _SEED_BALANCE_REFRESH_INTERVAL = 5.0

    def _seed_pending(self) -> bool:
        # JEP-210: True while an adopt:true seed has neither adopted nor permanently fail-closed.
        # control_task() uses this to suppress quoting during the cold-boot retry grace window.
        return (
            getattr(self.config, "adopt_existing_inventory", False)
            and not getattr(self, "_seed_adopted", False)
            and not getattr(self, "_seed_fail_closed", False)
        )

    def _note_seed_retry(self) -> None:
        # JEP-210: a transient seed precondition (connector snapshots not fresh yet, a balance
        # poll not landed, a get_open_orders blip, _update_positions error) must NOT permanently
        # fail-close adoption -- the cold-boot startup race resolves within seconds once the
        # control loop runs, and the seed is re-attempted on the next control tick. Only fail-close
        # after a grace window so a genuinely empty adopt:true position still stops quoting.
        now = time.monotonic()
        if getattr(self, "_seed_first_attempt_ts", None) is None:
            self._seed_first_attempt_ts = now
        grace = getattr(self, "_seed_grace_seconds", self._SEED_GRACE_SECONDS)
        if now - self._seed_first_attempt_ts >= grace:
            if not getattr(self, "_seed_fail_closed", False):
                self.logger().warning(
                    "JEP-210 adopt seed gave up after %.0fs grace for %s "
                    "(perp/spot snapshot never both landed); fail-closing -> executor will not quote. "
                    "last hedge-balance refresh error: %s",
                    grace, getattr(self, "maker_trading_pair", "?"),
                    getattr(self, "_seed_last_balance_error", None),
                )
            self._seed_fail_closed = True

    async def _seed_inventory_from_connector(self) -> None:
        if not getattr(self.config, "adopt_existing_inventory", False):
            return
        if not hasattr(self, "_seed_adopted"):
            self._seed_adopted = False
        if not hasattr(self, "_seed_fail_closed"):
            self._seed_fail_closed = False
        if not hasattr(self, "_seed_adopting"):
            self._seed_adopting = False
        if (
            getattr(self, "_seed_adopted", False)
            or getattr(self, "_seed_fail_closed", False)
            or getattr(self, "_seed_adopting", False)
        ):
            return
        maker = self.connectors[self.maker_connector]
        if getattr(maker, "position_mode", PositionMode.ONEWAY) == PositionMode.HEDGE:
            # JEP-210: HEDGE position mode is an unrecoverable config error -> fail-close
            # immediately, before the retry grace and regardless of snapshot freshness.
            self._seed_fail_closed = True
            return
        self._seed_adopting = True
        try:
            op_timeout = getattr(self, "_seed_op_timeout", self._SEED_OP_TIMEOUT)
            update_positions = getattr(maker, "_update_positions", None)
            if callable(update_positions):
                try:
                    positions = update_positions()
                    if inspect.isawaitable(positions):
                        # JEP-210: bound the REST call so a hang cannot wedge the seed (which would
                        # leave _seed_adopting stuck and block control_task forever).
                        await asyncio.wait_for(positions, timeout=op_timeout)
                except Exception:
                    self._note_seed_retry()
                    return

            if not await self._await_connector_readiness():
                self._note_seed_retry()
                return
            if await self._has_resting_orders():
                self._note_seed_retry()
                return

            perp_signed, perp_entry_price = self._read_perp_position_signed()
            if getattr(self, "_seed_fail_closed", False):
                # Unrecoverable (e.g. HEDGE position mode) -- permanent, set by the reader.
                return
            if perp_signed != ZERO:
                # JEP-210: there IS a perp leg to match -- drive a fresh hedge balance poll
                # (rate-limited) so the spot read reflects a successful fetch instead of a
                # cache the throttled background poll (KIS EGW00215) never populated.
                # Review F1: a FAILED/unconfirmed refresh must retry, never fall through to a
                # possibly-stale positive cache (that would over-adopt a wrong balance).
                if not await self._refresh_hedge_balance(op_timeout):
                    self._note_seed_retry()
                    return
                # Review F5: re-read the perp leg after the hedge-refresh await so the adopted
                # pair is a tight snapshot (a position update landing during the await is seen).
                perp_signed, perp_entry_price = self._read_perp_position_signed()
                if getattr(self, "_seed_fail_closed", False):
                    return
            spot_base = self._read_spot_balance_base()
            if perp_signed == ZERO or spot_base == ZERO:
                self._note_seed_retry()
                return

            self._apply_seed(perp_signed, spot_base, perp_entry_price)
            self._seed_adopted = True
            self.logger().info(
                "JEP-210 adopt seed complete for %s: perp signed=%s entry=%s, hedge spot_base=%s.",
                self.maker_trading_pair, perp_signed, perp_entry_price, spot_base,
            )
        finally:
            self._seed_adopting = False

    def _read_perp_position_signed(self) -> tuple[Decimal, Decimal]:
        connector = self.connectors[self.maker_connector]
        if getattr(connector, "position_mode", PositionMode.ONEWAY) == PositionMode.HEDGE:
            self._seed_fail_closed = True
            return ZERO, ZERO

        positions = getattr(connector, "account_positions", {}) or {}
        iterable = positions.values() if hasattr(positions, "values") else positions
        for position in iterable:
            if getattr(position, "trading_pair", None) != self.maker_trading_pair:
                continue
            amount = Decimal(str(getattr(position, "amount", ZERO)))
            if amount == ZERO:
                return ZERO, ZERO
            side = getattr(position, "position_side", None)
            if side == PositionSide.SHORT:
                amount = -abs(amount)
            elif side == PositionSide.LONG:
                amount = abs(amount)
            entry_price = Decimal(str(getattr(position, "entry_price", ZERO)))
            return amount, entry_price
        return ZERO, ZERO

    def _read_spot_balance_base(self) -> Decimal:
        connector = self.connectors[self.hedge_connector]
        base_asset = self.hedge_trading_pair.split("-", 1)[0]
        if hasattr(connector, "get_balance"):
            balance = connector.get_balance(base_asset)
        elif hasattr(connector, "get_available_balance"):
            balance = connector.get_available_balance(base_asset)
        else:
            balance = ZERO
        return max(Decimal(str(balance)), ZERO)

    async def _refresh_hedge_balance(self, op_timeout: float) -> bool:
        """JEP-210: actively drive a hedge balance poll during seeding (rate-limited).

        ``_read_spot_balance_base`` reads the hedge spot balance from the connector cache
        (``get_balance``), which is filled by the hedge connector's background balance poll.
        On KIS under multi-symbol load that poll is starved by per-second throttle
        (EGW00215), so the cache never lands within the grace window and the seed read 0 ->
        fail-closed a *real* held hedge. Mirroring the perp ``_update_positions()`` refresh,
        drive the balance poll ourselves so a real holding adopts once any throttle-free
        window lands.

        Returns ``True`` only when the cache may be trusted for adoption: either the
        connector has no balance hook (other venues / stubs -> passive read), or a poll just
        succeeded, or a poll succeeded within the rate-limit interval (possibly driven by a
        sibling executor sharing the same connector). Returns ``False`` when a poll is needed
        but has not confirmed -- the caller then retries instead of adopting a possibly-stale
        cache (review F1).

        Rate-limit + success state live on the *hedge connector* (shared singleton), not the
        executor, so multiple symbols sharing one KIS connector do not each burst the
        throttled endpoint and one successful poll serves all (review F2). A failed poll is
        not fatal here -- it just withholds trust for this attempt; the seed's grace/retry
        machinery eventually fail-closes a genuinely unreachable hedge.
        """
        hedge = self.connectors[self.hedge_connector]
        update_balances = getattr(hedge, "_update_balances", None)
        if not callable(update_balances):
            return True  # no driver (other venues / test stubs) -> passive cache read
        now = time.monotonic()
        interval = getattr(self, "_seed_balance_refresh_interval", self._SEED_BALANCE_REFRESH_INTERVAL)

        def _trust_recent_success() -> bool:
            ok_ts = getattr(hedge, "_seed_balance_ok_ts", None)
            return ok_ts is not None and (time.monotonic() - ok_ts) < interval

        # Single-flight (review rev2 F1): if a sibling executor's poll is already in flight on
        # this shared connector, do not start a second overlapping _update_balances() -- the
        # op timeout (10s) exceeds the rate-limit interval (5s), so a timestamp gate alone would
        # let a second call overlap a slow/hung one. Trust only a recent confirmed success.
        if getattr(hedge, "_seed_balance_inflight", False):
            return _trust_recent_success()
        last_attempt = getattr(hedge, "_seed_balance_refresh_ts", None)
        if last_attempt is not None and (now - last_attempt) < interval:
            # Not due (a recent attempt -- possibly a sibling executor -- already polled).
            # Trust the cache only if a poll actually SUCCEEDED within the interval.
            return _trust_recent_success()
        hedge._seed_balance_refresh_ts = now  # record the attempt (bounds persistent failure too)
        hedge._seed_balance_inflight = True
        try:
            result = update_balances()
            if inspect.isawaitable(result):
                await asyncio.wait_for(result, timeout=op_timeout)
        except Exception as e:
            # Transient (EGW00215 throttle) or a harder error (auth/account). Either way do
            # NOT adopt this tick; surface the latest cause for the fail-close log (review F3).
            self._seed_last_balance_error = repr(e)
            return False
        finally:
            hedge._seed_balance_inflight = False
        hedge._seed_balance_ok_ts = time.monotonic()
        return True

    def _apply_seed(self, perp_signed: Decimal, spot_base: Decimal, perp_entry_price: Decimal) -> None:
        self._ensure_direction_accounting()
        if perp_signed > ZERO:
            self._maker_buy_base += perp_signed
        elif perp_signed < ZERO:
            self._maker_sell_base += -perp_signed
        self._hedge_buy_base += self._hedge_base_to_maker_base(spot_base)
        self._seed_perp_basis_quote = abs(perp_signed) * perp_entry_price

    def _seed_snapshots_fresh(self) -> bool:
        maker = self.connectors[self.maker_connector]
        hedge = self.connectors[self.hedge_connector]
        if not hasattr(maker, "account_positions"):
            return False
        positions = getattr(maker, "account_positions", None)
        try:
            positions_fresh = len(positions or {}) > 0
        except TypeError:
            positions_fresh = any(positions)
        return positions_fresh and (hasattr(hedge, "get_balance") or hasattr(hedge, "get_available_balance"))

    async def _await_connector_readiness(
        self,
        timeout_s: Optional[float] = None,
        interval_s: Optional[float] = None,
    ) -> bool:
        timeout = getattr(self, "_seed_readiness_timeout", 20.0) if timeout_s is None else timeout_s
        interval = getattr(self, "_seed_readiness_interval", 0.1) if interval_s is None else interval_s
        deadline = time.monotonic() + timeout

        while True:
            maker_ready = bool(getattr(self.connectors[self.maker_connector], "ready", False))
            hedge_ready = bool(getattr(self.connectors[self.hedge_connector], "ready", False))
            if maker_ready and hedge_ready and self._seed_snapshots_fresh():
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(interval)

    async def _has_resting_orders(self) -> bool:
        for connector_name, trading_pair in (
            (self.maker_connector, self.maker_trading_pair),
            (self.hedge_connector, self.hedge_trading_pair),
        ):
            connector = self.connectors[connector_name]
            get_open_orders = getattr(connector, "get_open_orders", None)
            if get_open_orders is None:
                continue
            try:
                orders = get_open_orders(trading_pair)
                if inspect.isawaitable(orders):
                    # JEP-210: bound the REST call (a hang here would otherwise wedge the seed).
                    orders = await asyncio.wait_for(
                        orders, timeout=getattr(self, "_seed_op_timeout", self._SEED_OP_TIMEOUT)
                    )
            except Exception:
                return True
            if orders:
                return True
        return False

    def get_net_pnl_quote(self) -> Decimal:
        if getattr(self.config, "two_sided", False):
            return self._roundtrip_net_pnl_quote()

        matched = min(self._maker_executed_base, self._hedge_executed_base)
        if matched <= ZERO:
            return ZERO
        maker_avg = (
            self._maker_executed_quote / self._maker_executed_base
            if self._maker_executed_base > ZERO
            else ZERO
        )
        # hedge_avg divides the (maker-quote, JEP-185) hedge notional by the RAW hedge
        # base (e.g. KIS shares). This single-sided matched PnL therefore assumes one hedge
        # unit == one maker unit (share_per_unit == 1), which the HIP3-KIS controller
        # enforces via validator; share_per_unit != 1 would be unit-inconsistent here.
        hedge_avg = (
            self._hedge_executed_quote / self._hedge_executed_base
            if self._hedge_executed_base > ZERO
            else ZERO
        )
        if maker_avg <= ZERO or hedge_avg <= ZERO:
            return ZERO
        gross = self._pnl_gross_quote(matched, maker_avg, hedge_avg)
        return gross - self.get_cum_fees_quote()

    def _roundtrip_net_pnl_quote(self) -> Decimal:
        residual = self._unhedged_base_signed() * self._residual_mark_price()
        return self._perp_cash + self._spot_cash + residual - self.get_cum_fees_quote()

    def get_net_pnl_pct(self) -> Decimal:
        if self._maker_executed_quote <= ZERO:
            return ZERO
        return self.get_net_pnl_quote() / self._maker_executed_quote

    def get_cum_fees_quote(self) -> Decimal:
        return self._maker_fees_quote + self._hedge_fees_quote

    def get_custom_info(self) -> Dict:
        info = {
            "side": self.entry_side,
            "execution_purpose": self.get_execution_purpose(),
            "maker_connector": self.maker_connector,
            "maker_trading_pair": self.maker_trading_pair,
            "hedge_connector": self.hedge_connector,
            "hedge_trading_pair": self.hedge_trading_pair,
            "maker_executed_base": self._maker_executed_base,
            "hedge_executed_base": self._hedge_executed_base,
            "unhedged_base": self._unhedged_base(),
            "pending_hedge_base": self._pending_hedge_base,
            "open_maker_orders": len(self._open_maker_orders()),
            "open_hedge_orders": len(self._open_hedge_orders()),
            "hedge_kill_switch": self._hedge_kill_switch,
        }
        if getattr(self.config, "ws_staleness_kill_switch_enabled", False):
            info.update(
                {
                    "staleness_kill_switch": getattr(self, "_staleness_kill_switch", False),
                    "ws_maker_stale": getattr(self, "_maker_ws_stale", False),
                    "ws_hedge_stale": getattr(self, "_hedge_ws_stale", False),
                }
            )
        return info
