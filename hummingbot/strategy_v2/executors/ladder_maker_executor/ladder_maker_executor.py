"""Ladder maker executor, rewired onto the shared cross-venue hedged machine (JEP-147).

``LadderMakerExecutor`` now inherits ``CrossVenueHedgedExecutorBase`` (JEP-143):
all inventory / hedge-queue / fill-accounting / PnL / fee / retry / event plumbing
lives in the base, identical to ``XEMMExecutor``. This subclass supplies only the
ladder-specific policy via the base's abstract hooks:

  * fair-price derivation (KIS spot KRW->USD via side-aware FX) + inventory skew,
  * ladder target construction + reprice guard,
  * marketable-limit hedge sizing on KIS spot,
  * the pre-quote gate.

Pure pricing math stays in ``ladder_policy`` (unit-tested without the runtime).

Gate wiring (JEP-147): the kill-switch now flows through a composable ``GateChain``
(JEP-142). The chain is the extension seam JEP-133 fills with the staleness /
trading-hours / order-cap gates (those need feed-age + session inputs that are not
plumbed yet); for now the chain holds ``KillSwitchGate`` and the fair-price
readiness check is kept as an explicit data gate to preserve current behavior.
"""
import logging
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import LadderMakerExecutorConfig
from hummingbot.strategy_v2.executors.ladder_maker_executor.fx_bridged_fair_source import FxBridgedFairSource
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import (
    RungSpec,
    Side,
    TwoSidedTargets,
    apply_inventory_skew,
    build_ladder_targets,
    build_two_sided_targets,
    compute_hedge_order,
)
from hummingbot.strategy_v2.executors.ladder_maker_executor.session_calendar import KrxSessionCalendar
from hummingbot.strategy_v2.gates.gate_chain import GateChain, GateContext, InventoryGate, KillSwitchGate
from hummingbot.strategy_v2.models.executors import TrackedOrder

ZERO = Decimal("0")


def _fmt_num(x) -> Optional[str]:
    """Format a price/size for observe output: trim binary-float noise to 4dp.

    Decimals built from floats (e.g. fair = spot * fx) carry long tails like
    ``1690.40000000000009``; quantizing to 4 decimals keeps logs and status
    readable. ``None`` (missing spot/fx) renders as ``--`` so a closed fair gate
    is obvious rather than crashing the formatter.
    """
    if x is None:
        return "--"
    d = x if isinstance(x, Decimal) else Decimal(str(x))
    # Fixed-point (never scientific, so 100 stays "100" not "1E+2"), trailing zeros trimmed.
    s = format(d.quantize(Decimal("0.0001")), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


class LadderMakerExecutor(CrossVenueHedgedExecutorBase):
    """Ladder market-making on a perp (maker, post-only) hedged on KIS spot.

    Each tick: derive a conservative fair price from the KIS spot orderbook (KRW->USD
    via side-aware FX), apply inventory skew, build a simultaneous-maker ladder, and
    keep the perp quotes in sync (subject to reprice guards). When a perp maker fills,
    the base enqueues a marketable-limit hedge on KIS spot sized by ``_size_hedge``.
    """

    _logger = None

    # In observe mode the executor reprices every tick (nothing is tracked, so the
    # reprice guard never holds), which would emit a quote log several times a second.
    # Throttle the human-facing OBSERVE summary to one line per this interval.
    _OBSERVE_LOG_INTERVAL_S = 5.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        strategy: StrategyV2Base,
        config: LadderMakerExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 10,
    ):
        self._last_reprice_ts = 0.0
        self._last_observe_log_ts = 0.0
        self._last_observe: Optional[Dict] = None
        # Kill-switch flows through the composable chain; JEP-133 appends the
        # staleness / trading-hours / order-cap gates to this same chain.
        # KillSwitchGate (manual + auto hedge kill-switch) AND a HARD inventory cap:
        # InventoryGate halts quoting once |unhedged| reaches max_inventory, so naked
        # exposure cannot grow past the config's "safety pin" (it previously only drove
        # price skew, never a hard stop). config.max_inventory <= 0 disables the hard cap.
        # NB: read the local ``config`` param, NOT ``self.config`` — the base class
        # sets ``self.config`` inside ``super().__init__`` (below), so ``self.config``
        # does not exist yet here. (Live regression: AttributeError on every action.)
        _max_inv = config.max_inventory if config.max_inventory and config.max_inventory > ZERO else None
        self._gate_chain = GateChain([KillSwitchGate(), InventoryGate(_max_inv)])
        super().__init__(
            strategy=strategy,
            config=config,
            maker_market=config.maker_market,
            hedge_market=config.hedge_market,
            entry_side=config.entry_side,
            connectors=[config.fx_connector] if config.fx_connector else None,
            update_interval=update_interval,
            max_retries=max_retries,
        )
        self._calendar = KrxSessionCalendar()
        self._fair = FxBridgedFairSource(
            getattr(self.config, "side_aware_fx", True),
            getattr(self.config, "static_fx_rate", None),
            self.logger(),
        )

    # ------------------------------------------------------------------ gates

    def _policy_side(self) -> Side:
        return Side.BUY if self.entry_side == TradeType.BUY else Side.SELL

    def _pending_maker_notional(self) -> Decimal:
        total = ZERO
        for o in self._open_maker_orders():
            order = o.order
            if order is not None and order.price is not None and order.amount is not None:
                total += Decimal(order.price) * Decimal(order.amount)
        return total

    def _gates_open(self) -> bool:
        if getattr(self, "_seed_fail_closed", False):
            return False
        ctx = GateContext(
            now_kst=self._calendar.now(self._strategy.current_timestamp),
            kis_age_s=0.0,  # feed-age gates land in JEP-133
            hl_age_s=0.0,
            fx_age_s=0.0,
            inventory=self._unhedged_base_signed(),
            open_order_count=len(self._open_maker_orders()),
            pending_notional=self._pending_maker_notional(),
            # config kill_switch OR the auto-tripped hedge kill-switch (persistent hedge-
            # venue failure): either closes the maker gate (halt quoting, cancel makers).
            kill_switch=bool(self.config.kill_switch) or self._hedge_kill_switch,
        )
        if not self._gate_chain.evaluate(ctx).open:
            return False
        # Data-readiness gate: a fair price must be computable. Promoting this to a
        # StalenessGate with real KIS/FX ages is JEP-133.
        return self._compute_fair(self._policy_side()) is not None

    # ------------------------------------------------------------------ fair price

    def _compute_fair(self, side: Side) -> Optional[Decimal]:
        kis = self.connectors[self.hedge_connector]
        bid = kis.get_price_by_type(self.hedge_trading_pair, PriceType.BestBid)
        ask = kis.get_price_by_type(self.hedge_trading_pair, PriceType.BestAsk)
        if not bid or not ask or bid <= ZERO or ask <= ZERO:
            return None
        fair = self._fair.fair_from_book(Decimal(str(bid)), Decimal(str(ask)), side)
        if fair is None:
            return None
        return apply_inventory_skew(
            fair,
            self._unhedged_base_signed(),
            self.config.target_inventory,
            self.config.inventory_skew_bps_per_unit,
        )

    # ------------------------------------------------------------------ ladder hooks

    # _is_two_sided / _resting_maker_orders / _place_target_one / _place_targets_subset are
    # inherited from CrossVenueHedgedExecutorBase (lifted in JEP-145). Only _place_maker (the
    # placement hook, with observe no-submit + rung recording) and _place_targets (the
    # observe-summary full placement) remain ladder-specific below.

    def _compute_eod_pressure(self) -> Decimal:
        wind = getattr(self.config, "eod_wind_minutes", 0)
        return self._calendar.eod_pressure(self._strategy.current_timestamp, wind)

    def _effective_wind_down(self) -> bool:
        return bool(getattr(self.config, "wind_down", False)) or getattr(self, "_flatten_on_stop", False)

    def _compute_targets(self) -> List:
        rec = getattr(self, "_latency_recorder", None)
        side = self._policy_side()
        fair = self._compute_fair(side)
        if fair is None:
            return []
        rungs = [
            RungSpec(edge_bps=r.edge_bps, size=r.size, min_edge_bps=r.min_edge_bps, enabled=r.enabled)
            for r in self.config.rungs
        ]
        if not self._is_two_sided():
            if rec is not None:
                rec.mark("fair")
            targets = build_ladder_targets(
                fair=fair,
                rungs=rungs,
                total_size_cap=self.config.total_size_cap,
                side=side,
                tick=self.config.maker_tick,
                buffer_ticks=self.config.buffer_ticks,
                inventory=self._unhedged_base_signed(),
                max_inventory=self.config.max_inventory,
                cost_bps=self.config.round_trip_cost_bps,
                current_position=self._maker_executed_base,
            )
            if rec is not None:
                rec.mark("targets")
            return targets
        state = self._two_sided_state()
        close_side = Side.BUY if side is Side.SELL else Side.SELL
        fair_close = self._compute_fair(close_side)
        if fair_close is None:
            return []
        if rec is not None:
            rec.mark("fair")
        tst: TwoSidedTargets = build_two_sided_targets(
            fair_open=fair,
            fair_close=fair_close,
            rungs=rungs,
            total_size_cap=self.config.total_size_cap,
            net_position=state["Q"],
            open_edge_vwap=self._open_edge_vwap,
            util=state["util"],
            eod_pressure=state["eod"],
            cost_bps=self.config.round_trip_cost_bps,
            k_open_skew_bps=self.config.k_open_skew_bps,
            k_close_skew_bps=self.config.k_close_skew_bps,
            eod_close_skew_bps=self.config.eod_close_skew_bps,
            max_close_cost_bps=self.config.max_close_cost_bps,
            tick=self.config.maker_tick,
            buffer_ticks=self.config.buffer_ticks,
            wind_down=self._effective_wind_down(),
        )
        if rec is not None:
            rec.mark("targets")
        return list(tst.open) + list(tst.close)

    def _two_sided_state(self) -> Dict[str, Decimal]:
        cap = self.config.total_size_cap
        q = self._paired_oi()
        util = (q / cap) if cap > ZERO else ZERO
        return {
            "Q": q,
            "U": abs(self._unhedged_base_signed()),
            "util": util,
            "eod": self._compute_eod_pressure(),
            "pending_signed": self._pending_hedge_signed,
        }

    def _should_reprice(self, targets: List) -> bool:
        open_makers = self._open_maker_orders()
        if not open_makers:
            return bool(targets)
        elapsed = self._strategy.current_timestamp - self._last_reprice_ts
        if elapsed < self.config.min_reprice_interval_s:
            return False
        if self._is_two_sided():
            return self._two_sided_should_reprice(targets, open_makers)
        # Reprice only if the best target moved beyond the tick threshold.
        target_prices = sorted(t.price for t in targets)
        current_prices = sorted(o.order.price for o in open_makers if o.order is not None)
        if not current_prices or not target_prices:
            return True
        delta = abs(target_prices[0] - current_prices[0])
        return delta >= self.config.min_reprice_delta_ticks * self.config.maker_tick

    def _two_sided_should_reprice(self, targets: List, open_makers: List) -> bool:
        conn = self.connectors[self.maker_connector]
        pair = self.maker_trading_pair
        tol = self.config.min_reprice_delta_ticks * self.config.maker_tick

        def is_open_side(side: TradeType) -> bool:
            return side == self.entry_side

        tgt = {True: [], False: []}
        for t in targets:
            side = TradeType.SELL if t.side == Side.SELL else TradeType.BUY
            qp = conn.quantize_order_price(pair, t.price)
            qa = conn.quantize_order_amount(pair, t.size)
            tgt[is_open_side(side)].append((qp, qa))

        rest = {True: [], False: []}
        for o in open_makers:
            if o.order is None:
                return True
            rest[is_open_side(o.order.trade_type)].append((o.order.price, o.order.amount))

        for side_key in (True, False):
            t_side = tgt[side_key]
            r_side = rest[side_key]
            if len(t_side) != len(r_side):
                return True
            t_prices = sorted(p for p, _ in t_side)
            r_prices = sorted(p for p, _ in r_side)
            if any(abs(tp - rp) >= tol for tp, rp in zip(t_prices, r_prices)):
                return True
            t_sizes = sorted(a for _, a in t_side)
            r_sizes = sorted(a for _, a in r_side)
            if t_sizes != r_sizes:
                return True
        return False

    def _reconcile_maker(self) -> None:
        # JEP-145 Phase 2: the partial-diff (selective cancel/replace) is the base generic for
        # BOTH the single- and two-sided paths. The two-sided body that used to live here was
        # behaviorally equivalent to CrossVenueHedgedExecutorBase._reconcile_maker for every
        # production config (same prune -> inflight-injection -> diff_ladder_targets ->
        # blocked_sides -> cancel/place), so it is deduped onto super(). The lone textual
        # difference was the observe guard — the old inline `if self.config.observe` vs the base's
        # more-defensive `getattr(self.config, "observe", False)`; these diverge ONLY for a config
        # missing the attribute, which LadderMakerExecutorConfig cannot be (it declares observe).
        # The ONLY ladder-specific divergence is two-sided wind_down, which must cancel ALL makers
        # then re-place close-only — deliberately NOT a partial-diff (a partial-diff would leave a
        # matching close order resting instead of the wind_down cancel-then-replace, stranding
        # open-side makers). Single-sided wind_down was never special-cased (it fell through to the
        # base generic) and still isn't.
        if self._is_two_sided() and getattr(self.config, "wind_down", False):
            targets = self._compute_targets()
            if not self._should_reprice(targets):
                return
            self._cancel_all_maker()
            self._place_targets(targets)
            return
        return super()._reconcile_maker()

    def _place_targets(self, targets: List) -> None:
        if self.config.observe:
            # Capture the full intended ladder (fair + spot + fx + rungs) for both the
            # throttled human log and get_custom_info (status/dashboard), then emit one
            # summary line per _OBSERVE_LOG_INTERVAL_S instead of one log per rung per
            # tick. Observe reprices every tick (nothing tracked), so unthrottled this
            # would flood several lines/second.
            self._last_observe = self._build_observe(targets)
            now = self._strategy.current_timestamp
            if now - self._last_observe_log_ts >= self._OBSERVE_LOG_INTERVAL_S:
                self._last_observe_log_ts = now
                self.logger().info(self._format_observe_line(self._last_observe))
        for target in targets:
            self._place_target_one(target)
        self._last_reprice_ts = self._strategy.current_timestamp

    def _flatten_unwind_step(self) -> bool:
        self._process_hedges()
        perp = self._perp_net()
        taker_live = self._flatten_taker_live()
        if perp == ZERO and not taker_live:
            return False
        if self._hedge_kill_switch:
            return True
        if taker_live:
            return True
        now = self._strategy.current_timestamp
        if self._flatten_started_ts is None:
            self._flatten_started_ts = now
        elapsed = now - self._flatten_started_ts
        if elapsed < self.config.flatten_timeout_s:
            self._reconcile_maker()
        elif self._open_maker_orders():
            self._cancel_all_maker()
        else:
            self._place_flatten_taker(abs(perp))
        return True

    def _flatten_taker_live(self) -> bool:
        """True while the flatten MARKET taker is pending or open."""
        oid = getattr(self, "_flatten_taker_oid", None)
        if oid is None:
            return False
        o = self.maker_orders.get(oid)
        if o is None:
            return False
        return o.order is None or o.order.is_open

    def _place_flatten_taker(self, amount: Decimal) -> None:
        conn = self.connectors[self.maker_connector]
        q_amount = conn.quantize_order_amount(self.maker_trading_pair, amount)
        if q_amount <= ZERO:
            return
        close_side = TradeType.BUY if self.entry_side == TradeType.SELL else TradeType.SELL
        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.MARKET,
            side=close_side,
            amount=q_amount,
            position_action=PositionAction.CLOSE,
            price=Decimal("NaN"),
            metadata={"order_role": "flatten_taker"},
        )
        self.maker_orders[order_id] = TrackedOrder(order_id=order_id)
        self._flatten_taker_oid = order_id

    def _place_maker(
        self,
        price: Decimal,
        amount: Decimal,
        edge_bps: Decimal,
        side: Optional[TradeType] = None,
        position_action: PositionAction = PositionAction.OPEN,
    ) -> Optional[str]:
        side = side if side is not None else self.entry_side
        connector = self.connectors[self.maker_connector]
        q_amount = connector.quantize_order_amount(self.maker_trading_pair, amount)
        q_price = connector.quantize_order_price(self.maker_trading_pair, price)
        if self.config.observe:
            # No-submit: nothing is tracked, so _open_maker_orders stays empty (cancel
            # path no-ops) and no fills occur (so the hedge path never fires). Zero real
            # orders. The intended quote is surfaced by the _place_targets summary line.
            # Return None so the base records no _maker_placed_rung for a phantom order.
            return None
        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT_MAKER,
            side=side,
            amount=q_amount,
            position_action=position_action,
            price=q_price,
            metadata={"order_role": "maker", "edge_bps": str(edge_bps)},
        )
        self.maker_orders[order_id] = TrackedOrder(order_id=order_id)
        # NB: _maker_placed_rung (the inflight double-place guard) is recorded by the BASE
        # from this returned id (CrossVenueHedgedExecutorBase._record_placed_rung), so it is
        # populated identically on both the single- and two-sided paths. This hook only owns
        # the order submission + the open-edge basis below.
        if position_action == PositionAction.OPEN:
            if not hasattr(self, "_maker_placed_edge_bps"):
                self._maker_placed_edge_bps = {}
            self._maker_placed_edge_bps[order_id] = Decimal(edge_bps)
        return order_id

    # ------------------------------------------------------------------ observe

    def _build_observe(self, targets: List) -> Dict:
        """Snapshot the inputs and intended ladder for observe-mode visibility.

        Surfaces the spot bid/ask the fair is derived from (so a frozen book is
        visible at a glance — KIS WS is unreliable and the book is REST-refreshed),
        the FX leg, the resulting fair, and each quantized rung price.
        """
        kis = self.connectors[self.hedge_connector]
        bid = kis.get_price_by_type(self.hedge_trading_pair, PriceType.BestBid)
        ask = kis.get_price_by_type(self.hedge_trading_pair, PriceType.BestAsk)
        fx_bid, fx_ask = self._fair.observe_fx_legs()
        fair = self._compute_fair(self._policy_side())
        conn = self.connectors[self.maker_connector]
        if self._is_two_sided():
            state = self._two_sided_state()

            def serialize(target) -> Dict:
                side = TradeType.SELL if target.side == Side.SELL else TradeType.BUY
                position_action = PositionAction.OPEN if side == self.entry_side else PositionAction.CLOSE
                return {
                    "side": side.name,
                    "position_action": position_action.name,
                    "edge_bps": _fmt_num(target.edge_bps),
                    "price": _fmt_num(conn.quantize_order_price(self.maker_trading_pair, target.price)),
                    "size": _fmt_num(target.size),
                }

            open_targets = []
            close_targets = []
            for target in targets:
                serialized = serialize(target)
                if serialized["position_action"] == PositionAction.OPEN.name:
                    open_targets.append(serialized)
                else:
                    close_targets.append(serialized)
            return {
                "two_sided": True,
                "side": self.entry_side.name,
                "fair": _fmt_num(fair),
                "spot_pair": self.hedge_trading_pair,
                "spot_bid": _fmt_num(bid),
                "spot_ask": _fmt_num(ask),
                "fx_bid": _fmt_num(fx_bid),
                "fx_ask": _fmt_num(fx_ask),
                "Q": _fmt_num(state["Q"]),
                "U": _fmt_num(state["U"]),
                "util": _fmt_num(state["util"]),
                "eod": _fmt_num(state["eod"]),
                "pending_signed": _fmt_num(state["pending_signed"]),
                "open": open_targets,
                "close": close_targets,
            }
        rungs = [
            {
                "edge_bps": _fmt_num(t.edge_bps),
                "price": _fmt_num(conn.quantize_order_price(self.maker_trading_pair, t.price)),
                "size": _fmt_num(t.size),
            }
            for t in targets
        ]
        return {
            "side": self.entry_side.name,
            "fair": _fmt_num(fair),
            "spot_pair": self.hedge_trading_pair,
            "spot_bid": _fmt_num(bid),
            "spot_ask": _fmt_num(ask),
            "fx_bid": _fmt_num(fx_bid),
            "fx_ask": _fmt_num(fx_ask),
            "rungs": rungs,
        }

    @staticmethod
    def _format_observe_line(obs: Dict) -> str:
        if obs.get("two_sided"):
            open_rungs = " ".join(f"{r['side']} {r['price']}@{r['edge_bps']}bps" for r in obs["open"])
            close_rungs = " ".join(f"{r['side']} {r['price']}@{r['edge_bps']}bps" for r in obs["close"])
            return (
                f"[OBSERVE] {obs['side']} two_sided fair={obs['fair']} "
                f"Q={obs['Q']} util={obs['util']} "
                f"open: {open_rungs} close: {close_rungs} -- no submit"
            )
        rungs = " ".join(f"{r['price']}@{r['edge_bps']}bps" for r in obs["rungs"])
        return (
            f"[OBSERVE] {obs['side']} fair={obs['fair']} "
            f"spot[{obs['spot_pair']}] bid/ask={obs['spot_bid']}/{obs['spot_ask']} "
            f"fx={obs['fx_bid']}/{obs['fx_ask']} -> rungs: {rungs} -- no submit"
        )

    def get_custom_info(self) -> Dict:
        info = super().get_custom_info()
        info["observe"] = bool(self.config.observe)
        if self._last_observe is not None:
            info["last_quote"] = self._last_observe
        return info

    def _residual_mark_price(self) -> Decimal:
        fair = self._compute_fair(self._policy_side())
        return fair if fair is not None else super()._residual_mark_price()

    # ------------------------------------------------------------------ hedge hook

    def _size_hedge(self, pending_base: Decimal) -> Optional[Dict]:
        kis = self.connectors[self.hedge_connector]
        hedge_side = Side.BUY if self._pending_hedge_signed < ZERO else Side.SELL
        price_type = PriceType.BestAsk if hedge_side is Side.BUY else PriceType.BestBid
        ref = kis.get_price_by_type(self.hedge_trading_pair, price_type)
        if not ref or ref <= ZERO:
            return None
        hedge = compute_hedge_order(
            fill_qty=pending_base,
            share_per_unit=self.config.share_per_unit,
            kis_price=Decimal(str(ref)),
            max_slippage_bps=self.config.hedge_max_slippage_bps,
            tick=self.config.hedge_tick,
            side=hedge_side,
        )
        amount = kis.quantize_order_amount(self.hedge_trading_pair, hedge.size)
        if amount <= ZERO:
            return None
        price = (
            Decimal("NaN")
            if self.config.hedge_order_type == OrderType.MARKET
            else kis.quantize_order_price(self.hedge_trading_pair, hedge.price)
        )
        return {
            "amount": amount,
            "price": price,
            "order_type": self.config.hedge_order_type,
            "metadata": {"order_role": "hedge"},
        }

    def _hedge_base_to_maker_base(self, amount: Decimal) -> Decimal:
        spu = self.config.share_per_unit
        if spu and spu != ZERO:
            return amount / spu
        return amount

    def _hedge_price_to_maker_quote(self, price: Decimal, side: TradeType) -> Decimal:
        """JEP-185 backward bridge -> FxBridgedFairSource (forward/backward pairing kept in one file)."""
        return self._fair.hedge_price_to_maker_quote(price, side)

    # ------------------------------------------------------------------ balance hook

    def _maker_balance_candidate(self) -> Optional[OrderCandidate]:
        fair = self._compute_fair(self._policy_side())
        if fair is None:
            return None
        # The maker leg is a perpetual (orders placed with PositionAction.OPEN), so
        # the balance candidate must be a PerpetualOrderCandidate -- the perp budget
        # checker reads .position_close/.leverage, which a plain OrderCandidate lacks
        # (AttributeError in validate_sufficient_balance -> executor never quotes).
        return PerpetualOrderCandidate(
            trading_pair=self.maker_trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT_MAKER,
            order_side=self.entry_side,
            amount=self.config.total_size_cap,
            price=fair,
            leverage=Decimal(str(self.config.leverage)),
        )

    async def validate_sufficient_balance(self):
        # Observe (no-submit) places ZERO real orders, so maker-leg balance is irrelevant.
        # The base check sizes a PerpetualOrderCandidate at the FULL total_size_cap and, on
        # an underfunded maker account, stops the executor (CloseType.INSUFFICIENT_BALANCE)
        # inside on_start() — before it ever quotes — so observe would silently never run.
        # (JEP-162 live regression: executor created then immediately TERMINATED every tick,
        # _gates_open never reached.)
        if self.config.observe:
            return
        await super().validate_sufficient_balance()
