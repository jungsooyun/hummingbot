"""Lane 2 Avellaneda-Stoikov two-sided maker executor (Phase 3b/3c).

Two venues: an A&S maker leg (HL) + a distinct perp hedge leg (OKX), wired in
JEP-205 3c. Sibling of LadderMakerExecutor under CrossVenueHedgedExecutorBase.
Verified in observe mode; production placement path present but live-uncertified
(spec §3.9). Real submission / hedge fills are 3d-gated.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate, PerpetualOrderCandidate
from hummingbot.strategy_v2.executors.as_maker_executor import as_policy
from hummingbot.strategy_v2.executors.as_maker_executor.data_types import AsMakerExecutorConfig
from hummingbot.strategy_v2.executors.as_maker_executor.volatility_source import (
    InstantVolatilitySource,
    VolatilitySource,
)
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.inventory_adapter import PerpHedgeInventory
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import RungTarget, Side
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import compute_hedge_order
from hummingbot.strategy_v2.gates.gate_chain import GateChain, GateContext, InventoryGate, KillSwitchGate
from hummingbot.strategy_v2.models.executors import TrackedOrder

ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
_EDGE = ZERO


class AsMakerExecutor(CrossVenueHedgedExecutorBase):
    _OBSERVE_LOG_INTERVAL_S = 5.0

    def __init__(
        self,
        strategy,
        config: AsMakerExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 10,
        volatility_source: Optional[VolatilitySource] = None,
    ):
        self._last_reprice_ts = 0.0
        self._last_observe_log_ts = 0.0
        self._last_observe: Optional[Dict] = None
        self._last_quote: Optional[Dict] = None
        _max_inv = config.max_inventory if config.max_inventory and config.max_inventory > ZERO else None
        self._gate_chain = GateChain([KillSwitchGate(), InventoryGate(_max_inv)])
        maker_pair = ConnectorPair(connector_name=config.connector_name, trading_pair=config.trading_pair)
        hedge_pair = ConnectorPair(connector_name=config.hedge_connector_name, trading_pair=config.hedge_trading_pair)
        super().__init__(
            strategy=strategy,
            config=config,
            maker_market=maker_pair,
            hedge_market=hedge_pair,
            entry_side=config.entry_side,
            update_interval=update_interval,
            max_retries=max_retries,
        )
        self._vol: VolatilitySource = volatility_source or InstantVolatilitySource(
            config.volatility_sampling_length, config.volatility_processing_length
        )
        # 3d hedge-queue-consumer injection seam: constructed now, no 3c caller (JEP-205 §3.2/§6).
        self._inventory = PerpHedgeInventory(self.connectors, self.maker_connector, self.maker_trading_pair,
                                             self.hedge_connector, self.hedge_trading_pair)

    def _mid_price(self) -> Optional[Decimal]:
        conn = self.connectors.get(self.maker_connector)
        if conn is None:
            return None
        bid = conn.get_price_by_type(self.maker_trading_pair, PriceType.BestBid)
        ask = conn.get_price_by_type(self.maker_trading_pair, PriceType.BestAsk)
        if bid is None or ask is None:
            return None
        bid, ask = Decimal(str(bid)), Decimal(str(ask))
        if not bid.is_finite() or not ask.is_finite() or bid <= ZERO or ask <= ZERO:
            return None
        return (bid + ask) / TWO

    def _pending_maker_notional(self) -> Decimal:
        total = ZERO
        for o in self._open_maker_orders():
            order = o.order
            if order is not None and order.price is not None and order.amount is not None:
                total += Decimal(order.price) * Decimal(order.amount)
        return total

    def _gates_open(self) -> bool:
        mid = self._mid_price()
        if mid is not None:
            self._vol.add_sample(float(mid))
        ctx = GateContext(
            now_kst=datetime.fromtimestamp(self._strategy.current_timestamp, tz=timezone.utc),
            kis_age_s=0.0,
            hl_age_s=0.0,
            fx_age_s=0.0,
            inventory=self._unhedged_base_signed(),
            open_order_count=len(self._open_maker_orders()),
            pending_notional=self._pending_maker_notional(),
            kill_switch=bool(self.config.kill_switch) or self._hedge_kill_switch,
        )
        if not self._gate_chain.evaluate(ctx).open:
            return False
        return mid is not None and self._vol.is_ready

    def _compute_targets(self) -> List[RungTarget]:
        try:
            mid = self._mid_price()
            if mid is None:
                return []
            sigma = Decimal(str(self._vol.current_value))
            q = as_policy.normalize_inventory(self._perp_net(), self.config.target_inventory, self.config.max_inventory)
            r = as_policy.reservation_price(mid, q, self.config.gamma, sigma, self.config.tau)
            delta = as_policy.optimal_spread(self.config.gamma, sigma, self.config.tau, self.config.kappa)
            bid_raw, ask_raw = as_policy.clamp_quotes(mid, r, delta, self.config.min_spread_pct)
            bsz_raw, asz_raw = as_policy.inventory_skew_sizes(self.config.order_amount, q, self.config.eta)

            conn = self.connectors[self.maker_connector]
            pair = self.maker_trading_pair
            bid_q = conn.quantize_order_price(pair, bid_raw)
            ask_q = conn.quantize_order_price(pair, ask_raw)
            bsz_q = conn.quantize_order_amount(pair, bsz_raw)
            asz_q = conn.quantize_order_amount(pair, asz_raw)

            self._last_quote = {
                "mid": mid,
                "sigma": sigma,
                "q": q,
                "r": r,
                "delta": delta,
                "bid": bid_q,
                "ask": ask_q,
                "bid_size": bsz_q,
                "ask_size": asz_q,
            }

            for name, v in (("bid", bid_q), ("ask", ask_q), ("bid_size", bsz_q), ("ask_size", asz_q)):
                if not v.is_finite():
                    self.logger().error(
                        f"AsMaker non-finite {name}={v} (mid={mid} sigma={sigma} q={q} "
                        f"r={r} delta={delta}); suppressing tick"
                    )
                    return []

            targets: List[RungTarget] = []
            if bid_q > ZERO and bsz_q > ZERO:
                targets.append(RungTarget(side=Side.BUY, price=bid_q, size=bsz_q, edge_bps=_EDGE))
            else:
                self.logger().warning(f"AsMaker dropping BUY: bid={bid_q} size={bsz_q}")
            if ask_q > ZERO and asz_q > ZERO:
                targets.append(RungTarget(side=Side.SELL, price=ask_q, size=asz_q, edge_bps=_EDGE))
            else:
                self.logger().warning(f"AsMaker dropping SELL: ask={ask_q} size={asz_q}")
            return targets
        except Exception as e:
            self.logger().error(f"AsMaker _compute_targets failed: {e}", exc_info=True)
            return []

    def _should_reprice(self, targets: List[RungTarget]) -> bool:
        resting = self._resting_maker_orders()
        if not resting:
            return bool(targets)
        if {t.side.name for t in targets} != {ro.side.name for ro in resting}:
            return True
        if (self._strategy.current_timestamp - self._last_reprice_ts) < self.config.min_reprice_interval_s:
            return False
        thresh = self.config.min_reprice_delta_ticks * self.config.maker_tick
        rest_by_side: Dict[str, List[Decimal]] = {}
        for ro in resting:
            rest_by_side.setdefault(ro.side.name, []).append(ro.price)
        for t in targets:
            prices = rest_by_side.get(t.side.name, [])
            if not prices or min(abs(t.price - p) for p in prices) >= thresh:
                return True
        return False

    def _infer_position_action(self, side: TradeType) -> PositionAction:
        """OPEN if the order grows |net|, CLOSE if it reduces it (ONEWAY perp).
        Observe-logged only; precise live mapping (HEDGE mode, open+close splits) is 3d."""
        net = self._perp_net()
        if side == TradeType.BUY:
            return PositionAction.CLOSE if net < ZERO else PositionAction.OPEN
        return PositionAction.CLOSE if net > ZERO else PositionAction.OPEN

    def _place_target_one(self, target: RungTarget) -> None:
        """Override the base (whose two-sided mapping keys position_action off a fixed
        entry_side — ladder semantics). A&S places both sides with net-sign action."""
        side = TradeType.BUY if target.side == Side.BUY else TradeType.SELL
        position_action = self._infer_position_action(side)
        order_id = self._place_maker(
            target.price,
            target.size,
            target.edge_bps,
            side=side,
            position_action=position_action,
        )
        self._record_placed_rung(order_id, target)

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
            return None
        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT_MAKER,
            side=side,
            amount=q_amount,
            position_action=position_action,
            price=q_price,
            metadata={"order_role": "maker"},
        )
        self.maker_orders[order_id] = TrackedOrder(order_id=order_id)
        return order_id

    def _place_targets(self, targets: List[RungTarget]) -> None:
        if self.config.observe:
            self._emit_observe_snapshot(targets)
        for target in targets:
            self._place_target_one(target)
        self._last_reprice_ts = self._strategy.current_timestamp

    def _emit_observe_snapshot(self, targets: List[RungTarget]) -> None:
        snap = dict(self._last_quote or {})
        snap["targets"] = [(t.side.name, t.price, t.size) for t in targets]
        snap["hedge_preview"] = [
            {"maker_side": t.side.name, "hedge_side": self._opp(t.side).name,
             "spec": self._hedge_spec_for(self._opp(t.side), t.size)}
            for t in targets
        ]
        self._last_observe = snap
        now = self._strategy.current_timestamp
        if (now - self._last_observe_log_ts) >= self._OBSERVE_LOG_INTERVAL_S:
            self._last_observe_log_ts = now
            self.logger().info(f"[AsMaker observe] {snap}")

    @staticmethod
    def _opp(side: Side) -> Side:
        return Side.SELL if side is Side.BUY else Side.BUY

    def _hedge_spec_for(self, hedge_side: Side, base_qty: Decimal) -> Optional[Dict]:
        okx = self.connectors.get(self.hedge_connector)
        pt = PriceType.BestAsk if hedge_side is Side.BUY else PriceType.BestBid
        ref = okx.get_price_by_type(self.hedge_trading_pair, pt) if okx else None
        if not ref or Decimal(str(ref)) <= ZERO:
            return None
        # compute_hedge_order's `kis_price` arg is a generic reference price (the param name is
        # historical from ladder_policy/KIS); here it is the OKX hedge best-ask/bid.
        h = compute_hedge_order(fill_qty=base_qty, share_per_unit=ONE, kis_price=Decimal(str(ref)),
                                max_slippage_bps=self.config.hedge_max_slippage_bps,
                                tick=self.config.hedge_tick, side=hedge_side)
        amount = okx.quantize_order_amount(self.hedge_trading_pair, h.size)
        if amount <= ZERO:
            return None
        price = (Decimal("NaN") if self.config.hedge_order_type == OrderType.MARKET
                 else okx.quantize_order_price(self.hedge_trading_pair, h.price))
        return {"amount": amount, "price": price, "order_type": self.config.hedge_order_type,
                "metadata": {"order_role": "hedge"}}

    def _size_hedge(self, pending_base: Decimal) -> Optional[Dict]:
        hedge_side = Side.BUY if self._pending_hedge_signed < ZERO else Side.SELL
        return self._hedge_spec_for(hedge_side, pending_base)

    def _maker_balance_candidate(self) -> Optional[OrderCandidate]:
        if self.config.observe:
            return None
        mid = self._mid_price()
        if mid is None:
            return None
        return PerpetualOrderCandidate(
            trading_pair=self.maker_trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT_MAKER,
            order_side=self.entry_side,
            amount=self.config.order_amount,
            price=mid,
            leverage=Decimal(str(self.config.leverage)),
        )

    def get_custom_info(self) -> Dict:
        info = super().get_custom_info()
        info["as_maker"] = self._last_observe or self._last_quote or {}
        return info
