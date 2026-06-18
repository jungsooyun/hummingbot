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
from datetime import datetime, timedelta, timezone
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
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import (
    RungSpec,
    Side,
    TwoSidedTargets,
    apply_inventory_skew,
    build_ladder_targets,
    build_two_sided_targets,
    compute_fair_price,
    compute_hedge_order,
)
from hummingbot.strategy_v2.gates.gate_chain import GateChain, GateContext, InventoryGate, KillSwitchGate
from hummingbot.strategy_v2.models.executors import TrackedOrder

ZERO = Decimal("0")
_KST = timezone(timedelta(hours=9))


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
        ctx = GateContext(
            now_kst=datetime.fromtimestamp(self._strategy.current_timestamp, tz=_KST),
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

    def _get_fx(self):
        """Live blended USD/KRW from the process-wide FairFxSource (JEP-148).

        Preserves the ``(Optional[Decimal], Optional[Decimal])`` contract that
        ``_compute_fair`` unpacks: the singleton's ``None`` (stale/never-fetched
        bank) maps to ``(None, None)`` so the fair gate closes. ``fx_connector`` /
        ``fx_trading_pair`` still register the USDT-KRW market for subscription
        (the source reads it via the script's getter). ``static_fx_rate`` remains a
        guarded offline/test fallback ONLY — never the live path.
        """
        from hummingbot.data_feed.fair_fx.fair_fx_source import FairFxSource

        quote = FairFxSource.get_instance().get_fx()
        if quote is not None:
            return quote
        if self.config.static_fx_rate and self.config.static_fx_rate > ZERO:
            rate = self.config.static_fx_rate
            return rate, rate
        return None, None

    def _compute_fair(self, side: Side) -> Optional[Decimal]:
        kis = self.connectors[self.hedge_connector]
        bid = kis.get_price_by_type(self.hedge_trading_pair, PriceType.BestBid)
        ask = kis.get_price_by_type(self.hedge_trading_pair, PriceType.BestAsk)
        if not bid or not ask or bid <= ZERO or ask <= ZERO:
            return None
        fx_bid, fx_ask = self._get_fx()
        if fx_bid is None or fx_ask is None:
            return None
        fair = compute_fair_price(
            Decimal(str(bid)), Decimal(str(ask)), fx_bid, fx_ask, side, self.config.side_aware_fx
        )
        return apply_inventory_skew(
            fair,
            self._unhedged_base_signed(),
            self.config.target_inventory,
            self.config.inventory_skew_bps_per_unit,
        )

    # ------------------------------------------------------------------ ladder hooks

    def _is_two_sided(self) -> bool:
        return getattr(self.config, "two_sided", False) is True

    def _compute_targets(self) -> List:
        side = self._policy_side()
        fair = self._compute_fair(side)
        if fair is None:
            return []
        rungs = [
            RungSpec(edge_bps=r.edge_bps, size=r.size, min_edge_bps=r.min_edge_bps, enabled=r.enabled)
            for r in self.config.rungs
        ]
        if not self._is_two_sided():
            return build_ladder_targets(
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
        state = self._two_sided_state()
        tst: TwoSidedTargets = build_two_sided_targets(
            fair=fair,
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
            wind_down=self.config.wind_down,
        )
        return list(tst.open) + list(tst.close)

    def _two_sided_state(self) -> Dict[str, Decimal]:
        cap = self.config.total_size_cap
        q = self._paired_oi()
        util = (q / cap) if cap > ZERO else ZERO
        return {
            "Q": q,
            "U": abs(self._unhedged_base_signed()),
            "util": util,
            "eod": ZERO,
            "pending_signed": self._pending_hedge_signed,
        }

    def _should_reprice(self, targets: List) -> bool:
        open_makers = self._open_maker_orders()
        if not open_makers:
            return bool(targets)
        elapsed = self._strategy.current_timestamp - self._last_reprice_ts
        if elapsed < self.config.min_reprice_interval_s:
            return False
        # Reprice only if the best target moved beyond the tick threshold.
        target_prices = sorted(t.price for t in targets)
        current_prices = sorted(o.order.price for o in open_makers if o.order is not None)
        if not current_prices or not target_prices:
            return True
        delta = abs(target_prices[0] - current_prices[0])
        return delta >= self.config.min_reprice_delta_ticks * self.config.maker_tick

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
            if self._is_two_sided():
                side = TradeType.SELL if target.side == Side.SELL else TradeType.BUY
                position_action = PositionAction.OPEN if side == self.entry_side else PositionAction.CLOSE
                self._place_maker(
                    target.price,
                    target.size,
                    target.edge_bps,
                    side=side,
                    position_action=position_action,
                )
            else:
                self._place_maker(target.price, target.size, target.edge_bps)
        self._last_reprice_ts = self._strategy.current_timestamp

    def _place_maker(
        self,
        price: Decimal,
        amount: Decimal,
        edge_bps: Decimal,
        side: Optional[TradeType] = None,
        position_action: PositionAction = PositionAction.OPEN,
    ) -> None:
        side = side if side is not None else self.entry_side
        connector = self.connectors[self.maker_connector]
        q_amount = connector.quantize_order_amount(self.maker_trading_pair, amount)
        q_price = connector.quantize_order_price(self.maker_trading_pair, price)
        if self.config.observe:
            # No-submit: nothing is tracked, so _open_maker_orders stays empty (cancel
            # path no-ops) and no fills occur (so the hedge path never fires). Zero real
            # orders. The intended quote is surfaced by the _place_targets summary line.
            return
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
        if position_action == PositionAction.OPEN:
            if not hasattr(self, "_maker_placed_edge_bps"):
                self._maker_placed_edge_bps = {}
            self._maker_placed_edge_bps[order_id] = Decimal(edge_bps)

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
        fx_bid, fx_ask = self._get_fx()
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
