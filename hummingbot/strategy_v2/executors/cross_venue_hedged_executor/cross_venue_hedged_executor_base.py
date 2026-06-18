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

from hummingbot.core.data_type.common import PositionMode, PositionSide, TradeType
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

        # Direction-aware inventory ledgers. Legacy executed-base/quote totals above
        # stay as magnitudes because get_net_pnl_quote intentionally remains unchanged.
        self._maker_buy_base = ZERO
        self._maker_sell_base = ZERO
        self._hedge_buy_base = ZERO
        self._hedge_sell_base = ZERO
        self._hedge_order_side: Dict[str, Union[TradeType, tuple[TradeType, Decimal]]] = {}
        self._hedge_credited_base: Dict[str, Decimal] = {}
        self._hedge_terminal_ids = OrderedDict()
        self._maker_placed_edge_bps: Dict[str, Decimal] = {}
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

    # ============================================================ abstract hooks

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

    # ============================================================ lifecycle

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            await self._seed_inventory_from_connector()
            if not self._gates_open():
                self._cancel_all_maker()
                self._process_hedges()
                return
            self._reconcile_maker()
            self._process_hedges()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
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

    def _reconcile_maker(self) -> None:
        targets = self._compute_targets()
        if not self._should_reprice(targets):
            return
        self._cancel_all_maker()
        self._place_targets(targets)

    def _cancel_all_maker(self) -> None:
        for order in self._open_maker_orders():
            self._strategy.cancel(self.maker_connector, self.maker_trading_pair, order.order_id)

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
        if not hasattr(self, "_hedge_terminal_ids"):
            self._hedge_terminal_ids = OrderedDict()
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

    @staticmethod
    def _signed_base(side: TradeType, amount: Decimal) -> Decimal:
        return amount if side == TradeType.BUY else -amount

    def _hedge_price_to_maker_quote(self, price: Decimal) -> Decimal:
        """Convert a hedge-leg fill price into the maker-leg quote unit.

        Default identity: the base is a generic two-venue machine, and tests inject
        pre-converted prices. ``LadderMakerExecutor`` overrides this to divide a KRW
        spot fill by the fill-time FX rate so ``_spot_cash`` accrues in USD (the maker
        leg's quote), keeping the round-trip PnL single-currency.
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
        masked this by clamping at 0). The notional accumulators (``_hedge_executed_*`` /
        ``_spot_cash``) intentionally keep the RAW hedge amount — they are money, not delta
        units — so legacy matched PnL stays byte-identical.
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

    def _credit_hedge_fill(self, order_id: str, observed_cumulative: Decimal, price: Optional[Decimal]) -> None:
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
            self._hedge_executed_quote += delta * price
            self._spot_cash += (
                self._hedge_price_to_maker_quote(price)
                * delta
                * (ONE if side == TradeType.SELL else -ONE)
            )
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
                if price is None or price.is_nan():
                    price = in_flight.price
                # Guard the maker hedge's NaN market price from poisoning quote/PnL;
                # crediting base-only slightly under-reports quote for this rare order.
                self._credit_hedge_fill(order_id, filled_base, price)
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

    def _process_hedges(self) -> None:
        self._ensure_direction_accounting()
        self._reconcile_stuck_hedges()
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
            if tracked is not None and tracked.order is not None and side != needed_side:
                self._strategy.cancel(self.hedge_connector, self.hedge_trading_pair, oid)
        if self._pending_hedge_signed == ZERO:
            return
        if self._hedge_in_flight():
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
        else:
            self.logger().warning(
                "Ignoring fill for unknown order %s: not tracked as maker, active hedge, or terminal hedge.",
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
        self.maker_orders.pop(event.order_id, None)
        # Pop a cancelled hedge too (no leak / no stale in-flight block). Its unfilled
        # base stays in _pending_hedge_base and is re-hedged on the next tick.
        if event.order_id in self.hedge_orders:
            self.hedge_orders.pop(event.order_id, None)
            self._remember_terminal_hedge_order(event.order_id, allow_event_delta_fallback=True)

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        self._ensure_direction_accounting()
        if event.order_id in self.maker_orders:
            self.maker_orders.pop(event.order_id, None)
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
        self._seed_adopting = True
        try:
            update_positions = getattr(self.connectors[self.maker_connector], "_update_positions", None)
            if callable(update_positions):
                try:
                    positions = update_positions()
                    if inspect.isawaitable(positions):
                        await positions
                except Exception:
                    self._seed_fail_closed = True
                    return

            if not await self._await_connector_readiness():
                self._seed_fail_closed = True
                return
            if await self._has_resting_orders():
                self._seed_fail_closed = True
                return

            perp_signed, perp_entry_price = self._read_perp_position_signed()
            if getattr(self, "_seed_fail_closed", False):
                return
            spot_base = self._read_spot_balance_base()
            if perp_signed == ZERO or spot_base == ZERO:
                self._seed_fail_closed = True
                return

            self._apply_seed(perp_signed, spot_base, perp_entry_price)
            self._seed_adopted = True
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
                    orders = await orders
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
        return {
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
