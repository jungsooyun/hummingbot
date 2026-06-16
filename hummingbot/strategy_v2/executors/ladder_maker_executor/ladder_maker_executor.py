import logging
from decimal import Decimal
from typing import Dict, List, Optional, Union

from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
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
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import LadderMakerExecutorConfig
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import (
    RungSpec,
    Side,
    apply_inventory_skew,
    build_ladder_targets,
    compute_fair_price,
    compute_hedge_order,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

ZERO = Decimal("0")


class LadderMakerExecutor(ExecutorBase):
    """Ladder market-making on a perp (maker, post-only) hedged on KIS spot.

    Each tick: derive a conservative fair price from the KIS spot orderbook (KRW->USD
    via side-aware FX), apply inventory skew, build a simultaneous-maker ladder, and
    keep the perp quotes in sync (subject to reprice guards). When a perp maker fills,
    enqueue a marketable-limit hedge on KIS spot.

    Pure pricing math lives in ``ladder_policy`` (unit-tested without the runtime).
    """

    _logger = None

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
        self.config = config
        self.maker_connector = config.maker_market.connector_name
        self.maker_trading_pair = config.maker_market.trading_pair
        self.hedge_connector = config.hedge_market.connector_name
        self.hedge_trading_pair = config.hedge_market.trading_pair
        self.entry_side = config.entry_side
        self.hedge_side = TradeType.BUY if config.entry_side == TradeType.SELL else TradeType.SELL

        self.maker_orders: Dict[str, TrackedOrder] = {}
        self.hedge_orders: Dict[str, TrackedOrder] = {}

        self._maker_executed_base = ZERO
        self._maker_executed_quote = ZERO
        self._hedge_executed_base = ZERO
        self._hedge_executed_quote = ZERO
        self._pending_hedge_base = ZERO
        self._maker_fees_quote = ZERO
        self._hedge_fees_quote = ZERO

        self._last_reprice_ts = 0.0
        self._current_retries = 0
        self._max_retries = max_retries

        connectors = [self.maker_connector, self.hedge_connector]
        if config.fx_connector and config.fx_connector not in connectors:
            connectors.append(config.fx_connector)

        super().__init__(
            strategy=strategy,
            connectors=connectors,
            config=config,
            update_interval=update_interval,
        )

    # ------------------------------------------------------------------ lifecycle

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            if not self._gates_open():
                self._cancel_all_maker()
                self._process_hedges()
                return
            self._reconcile_ladder()
            self._process_hedges()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self._control_shutdown()

    def early_stop(self, keep_position: bool = False):
        if keep_position:
            self.close_type = CloseType.POSITION_HOLD
            self.stop()
            return
        self.close_type = CloseType.EARLY_STOP
        self._status = RunnableStatus.SHUTTING_DOWN

    async def _control_shutdown(self):
        self._cancel_all_maker()
        # Best-effort: hedge any remaining unhedged fills before terminating
        self._process_hedges()
        if not self._has_open_orders() and self._unhedged_base() <= ZERO:
            self.stop()

    # ------------------------------------------------------------------ gates

    def _gates_open(self) -> bool:
        if self.config.kill_switch:
            return False
        if self._compute_fair(self._policy_side()) is None:
            return False
        return True

    # ------------------------------------------------------------------ fair price

    def _policy_side(self) -> Side:
        return Side.BUY if self.entry_side == TradeType.BUY else Side.SELL

    def _get_fx(self):
        if self.config.fx_connector and self.config.fx_trading_pair:
            conn = self.connectors.get(self.config.fx_connector)
            if conn is not None:
                bid = conn.get_price_by_type(self.config.fx_trading_pair, PriceType.BestBid)
                ask = conn.get_price_by_type(self.config.fx_trading_pair, PriceType.BestAsk)
                if bid and bid > ZERO and ask and ask > ZERO:
                    return Decimal(str(bid)), Decimal(str(ask))
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

    # ------------------------------------------------------------------ ladder reconcile

    def _reconcile_ladder(self):
        side = self._policy_side()
        fair = self._compute_fair(side)
        if fair is None:
            return
        rungs = [
            RungSpec(edge_bps=r.edge_bps, size=r.size, min_edge_bps=r.min_edge_bps, enabled=r.enabled)
            for r in self.config.rungs
        ]
        targets = build_ladder_targets(
            fair=fair,
            rungs=rungs,
            total_size_cap=self.config.total_size_cap,
            side=side,
            tick=self.config.maker_tick,
            buffer_ticks=self.config.buffer_ticks,
            inventory=self._unhedged_base_signed(),
            max_inventory=self.config.max_inventory,
        )
        if not self._should_reprice(targets):
            return
        self._cancel_all_maker()
        for target in targets:
            self._place_maker(target.price, target.size, target.edge_bps)
        self._last_reprice_ts = self._strategy.current_timestamp

    def _should_reprice(self, targets: List) -> bool:
        open_makers = self._open_maker_orders()
        if not open_makers:
            return bool(targets)
        elapsed = self._strategy.current_timestamp - self._last_reprice_ts
        if elapsed < self.config.min_reprice_interval_s:
            return False
        # Reprice only if the best target moved beyond the tick threshold
        target_prices = sorted(t.price for t in targets)
        current_prices = sorted(o.order.price for o in open_makers if o.order is not None)
        if not current_prices or not target_prices:
            return True
        delta = abs(target_prices[0] - current_prices[0])
        return delta >= self.config.min_reprice_delta_ticks * self.config.maker_tick

    def _place_maker(self, price: Decimal, amount: Decimal, edge_bps: Decimal):
        connector = self.connectors[self.maker_connector]
        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT_MAKER,
            side=self.entry_side,
            amount=connector.quantize_order_amount(self.maker_trading_pair, amount),
            position_action=PositionAction.OPEN,
            price=connector.quantize_order_price(self.maker_trading_pair, price),
            metadata={"order_role": "maker", "edge_bps": str(edge_bps)},
        )
        self.maker_orders[order_id] = TrackedOrder(order_id=order_id)

    def _cancel_all_maker(self):
        for order in self._open_maker_orders():
            self._strategy.cancel(self.maker_connector, self.maker_trading_pair, order.order_id)

    # ------------------------------------------------------------------ hedge

    def _process_hedges(self):
        if self._pending_hedge_base <= ZERO:
            return
        # Keep at most one in-flight hedge order to avoid double-hedging
        if any(o.order is not None and o.order.is_open for o in self.hedge_orders.values()):
            return
        kis = self.connectors[self.hedge_connector]
        best_ask = kis.get_price_by_type(self.hedge_trading_pair, PriceType.BestAsk)
        if not best_ask or best_ask <= ZERO:
            return
        hedge = compute_hedge_order(
            fill_qty=self._pending_hedge_base,
            share_per_unit=self.config.share_per_unit,
            kis_best_ask=Decimal(str(best_ask)),
            max_slippage_bps=self.config.hedge_max_slippage_bps,
            tick=self.config.hedge_tick,
        )
        amount = kis.quantize_order_amount(self.hedge_trading_pair, hedge.size)
        if amount <= ZERO:
            return
        price = Decimal("NaN") if self.config.hedge_order_type == OrderType.MARKET else \
            kis.quantize_order_price(self.hedge_trading_pair, hedge.price)
        order_id = self.place_order(
            connector_name=self.hedge_connector,
            trading_pair=self.hedge_trading_pair,
            order_type=self.config.hedge_order_type,
            side=self.hedge_side,
            amount=amount,
            price=price,
            metadata={"order_role": "hedge"},
        )
        self.hedge_orders[order_id] = TrackedOrder(order_id=order_id)
        self._pending_hedge_base = ZERO

    # ------------------------------------------------------------------ inventory helpers

    def _unhedged_base(self) -> Decimal:
        return max(ZERO, self._maker_executed_base - self._hedge_executed_base)

    def _unhedged_base_signed(self) -> Decimal:
        """Signed net inventory: positive long, negative short (drives skew/gate)."""
        unhedged = self._unhedged_base()
        return unhedged if self.entry_side == TradeType.BUY else -unhedged

    def _open_maker_orders(self) -> List[TrackedOrder]:
        return [o for o in self.maker_orders.values() if o.order is not None and o.order.is_open]

    def _has_open_orders(self) -> bool:
        return any(o.order is not None and o.order.is_open for o in
                   list(self.maker_orders.values()) + list(self.hedge_orders.values()))

    # ------------------------------------------------------------------ events

    def _update_tracked(self, connector_name: str, order_id: str):
        for book in (self.maker_orders, self.hedge_orders):
            tracked = book.get(order_id)
            if tracked is not None:
                in_flight = self.get_in_flight_order(connector_name, order_id)
                if in_flight is not None:
                    tracked.order = in_flight

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if event.order_id in self.maker_orders:
            self._update_tracked(self.maker_connector, event.order_id)
        elif event.order_id in self.hedge_orders:
            self._update_tracked(self.hedge_connector, event.order_id)

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        if event.order_id in self.maker_orders:
            self._update_tracked(self.maker_connector, event.order_id)
            self._maker_executed_base += Decimal(event.amount)
            self._maker_executed_quote += Decimal(event.amount) * Decimal(event.price)
            self._pending_hedge_base += Decimal(event.amount)
        elif event.order_id in self.hedge_orders:
            self._update_tracked(self.hedge_connector, event.order_id)
            self._hedge_executed_base += Decimal(event.amount)
            self._hedge_executed_quote += Decimal(event.amount) * Decimal(event.price)

    def process_order_completed_event(self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        if event.order_id in self.maker_orders:
            self._update_tracked(self.maker_connector, event.order_id)
            tracked = self.maker_orders[event.order_id]
            self._maker_fees_quote += tracked.cum_fees_quote
        elif event.order_id in self.hedge_orders:
            self._update_tracked(self.hedge_connector, event.order_id)
            tracked = self.hedge_orders[event.order_id]
            self._hedge_fees_quote += tracked.cum_fees_quote

    def process_order_canceled_event(self, _, market, event: OrderCancelledEvent):
        self.maker_orders.pop(event.order_id, None)

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        if event.order_id in self.maker_orders:
            self.maker_orders.pop(event.order_id, None)
        elif event.order_id in self.hedge_orders:
            self.hedge_orders.pop(event.order_id, None)
            self._current_retries += 1
            if self._current_retries > self._max_retries:
                self.close_type = CloseType.FAILED
                self.stop()

    # ------------------------------------------------------------------ balance / pnl

    async def validate_sufficient_balance(self):
        side = self._policy_side()
        fair = self._compute_fair(side)
        if fair is None:
            return
        candidate = OrderCandidate(
            trading_pair=self.maker_trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT_MAKER,
            order_side=self.entry_side,
            amount=self.config.total_size_cap,
            price=fair,
        )
        adjusted = self.adjust_order_candidates(self.maker_connector, [candidate])[0]
        if adjusted.amount == ZERO:
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.stop()

    def get_net_pnl_quote(self) -> Decimal:
        matched = min(self._maker_executed_base, self._hedge_executed_base)
        if matched <= ZERO:
            return ZERO
        maker_avg = self._maker_executed_quote / self._maker_executed_base if self._maker_executed_base > ZERO else ZERO
        hedge_avg = self._hedge_executed_quote / self._hedge_executed_base if self._hedge_executed_base > ZERO else ZERO
        if maker_avg <= ZERO or hedge_avg <= ZERO:
            return ZERO
        # entry SELL on perp, hedge BUY on spot: profit = (sell - buy) * qty
        if self.entry_side == TradeType.SELL:
            gross = (maker_avg - hedge_avg) * matched
        else:
            gross = (hedge_avg - maker_avg) * matched
        return gross - self.get_cum_fees_quote()

    def get_net_pnl_pct(self) -> Decimal:
        if self._maker_executed_quote <= ZERO:
            return ZERO
        return self.get_net_pnl_quote() / self._maker_executed_quote

    def get_cum_fees_quote(self) -> Decimal:
        return self._maker_fees_quote + self._hedge_fees_quote

    def get_custom_info(self) -> Dict:
        return {
            "side": self.entry_side,
            "execution_purpose": self.config.execution_purpose,
            "maker_connector": self.maker_connector,
            "hedge_connector": self.hedge_connector,
            "maker_executed_base": self._maker_executed_base,
            "hedge_executed_base": self._hedge_executed_base,
            "unhedged_base": self._unhedged_base(),
            "pending_hedge_base": self._pending_hedge_base,
            "open_maker_orders": len(self._open_maker_orders()),
        }
