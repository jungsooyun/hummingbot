import asyncio
import logging
from decimal import Decimal
from typing import Dict, Optional, Union

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
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
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.inventory_rebalance_executor.data_types import InventoryRebalanceExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class InventoryRebalanceExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        strategy: ScriptStrategyBase,
        config: InventoryRebalanceExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 10,
    ):
        self.config = config
        self.entry_connector = config.entry_market.connector_name
        self.entry_trading_pair = config.entry_market.trading_pair
        self.entry_side = config.entry_side
        self.hedge_connector = config.hedge_market.connector_name
        self.hedge_trading_pair = config.hedge_market.trading_pair
        self.hedge_side = TradeType.SELL if config.entry_side == TradeType.BUY else TradeType.BUY
        self.entry_order: Optional[TrackedOrder] = None
        self.hedge_order: Optional[TrackedOrder] = None
        self._current_retries = 0
        self._max_retries = config.hedge_max_retries if config.hedge_max_retries is not None else max_retries
        self._hedged_base_submitted = Decimal("0")
        self._hedged_base_completed = Decimal("0")
        self._entry_filled_base_backup = Decimal("0")
        self._entry_filled_quote_backup = Decimal("0")
        self._hedge_filled_base_backup = Decimal("0")
        self._hedge_filled_quote_backup = Decimal("0")
        self._entry_fee_quote = Decimal("0")
        self._hedge_fee_quote = Decimal("0")
        super().__init__(
            strategy=strategy,
            connectors=[self.entry_connector, self.hedge_connector],
            config=config,
            update_interval=update_interval,
        )

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            await self._control_entry_order()
            await self._control_hedge_order()
            if self._ready_to_complete():
                self.close_type = CloseType.COMPLETED
                self.stop()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()

    async def _control_entry_order(self):
        if self.entry_order is None:
            self.place_entry_order()
            return
        if self.entry_order.order is None:
            return
        if self.entry_order.order.is_open and self.config.entry_style == "passive":
            if self._should_refresh_entry_order():
                self._strategy.cancel(self.entry_connector, self.entry_trading_pair, self.entry_order.order_id)
        if self.entry_order.order.is_done:
            self._entry_fee_quote = self.entry_order.cum_fees_quote

    async def _control_hedge_order(self):
        outstanding_to_hedge = self._unhedged_entry_base()
        if outstanding_to_hedge > Decimal("0") and self.hedge_order is None:
            self.place_hedge_order(outstanding_to_hedge)
            return

        if self.hedge_order is None or self.hedge_order.order is None:
            return
        if self.hedge_order.order.is_done:
            self._hedged_base_completed = max(
                self._hedged_base_completed,
                max(self.hedge_order.executed_amount_base, self._hedge_filled_base_backup),
            )
            self._hedged_base_submitted = self._hedged_base_completed
            self._hedge_fee_quote += self.hedge_order.cum_fees_quote
            self.hedge_order = None

    def _unhedged_entry_base(self) -> Decimal:
        executed = self._entry_executed_amount_base()
        return max(Decimal("0"), executed - self._hedged_base_submitted)

    def _entry_executed_amount_base(self) -> Decimal:
        if self.entry_order is None:
            return self._entry_filled_base_backup
        return max(self.entry_order.executed_amount_base, self._entry_filled_base_backup)

    def _entry_executed_amount_quote(self) -> Decimal:
        if self.entry_order is None:
            return self._entry_filled_quote_backup
        return max(self.entry_order.executed_amount_quote, self._entry_filled_quote_backup)

    def _hedge_executed_amount_base(self) -> Decimal:
        if self.hedge_order is None:
            return self._hedge_filled_base_backup
        return max(self.hedge_order.executed_amount_base, self._hedge_filled_base_backup)

    def _ready_to_complete(self) -> bool:
        if self.entry_order is None or self.entry_order.order is None:
            return False
        if self.entry_order.order.is_open:
            return False
        if self.hedge_order is not None and self.hedge_order.order is not None and self.hedge_order.order.is_open:
            return False
        return self._unhedged_entry_base() == Decimal("0")

    def _entry_order_type(self) -> OrderType:
        return OrderType.LIMIT

    def _entry_price(self) -> Decimal:
        connector = self.connectors[self.entry_connector]
        if self.config.entry_style == "passive":
            price_type = PriceType.BestBid if self.entry_side == TradeType.BUY else PriceType.BestAsk
            reference_price = connector.get_price_by_type(self.entry_trading_pair, price_type)
        else:
            price_type = PriceType.BestAsk if self.entry_side == TradeType.BUY else PriceType.BestBid
            reference_price = connector.get_price_by_type(self.entry_trading_pair, price_type)
            slippage = self.config.hedge_slippage_buffer_bps / Decimal("10000")
            if reference_price is not None and reference_price > Decimal("0"):
                if self.entry_side == TradeType.BUY:
                    reference_price = reference_price * (Decimal("1") + slippage)
                else:
                    reference_price = reference_price * (Decimal("1") - slippage)
        return connector.quantize_order_price(self.entry_trading_pair, reference_price)

    def _hedge_price(self, amount: Decimal) -> Decimal:
        if self.config.hedge_order_type == OrderType.MARKET:
            return Decimal("NaN")
        connector = self.connectors[self.hedge_connector]
        price_type = PriceType.BestAsk if self.hedge_side == TradeType.BUY else PriceType.BestBid
        reference_price = connector.get_price_by_type(self.hedge_trading_pair, price_type)
        slippage = self.config.hedge_slippage_buffer_bps / Decimal("10000")
        if self.hedge_side == TradeType.BUY:
            reference_price = reference_price * (Decimal("1") + slippage)
        else:
            reference_price = reference_price * (Decimal("1") - slippage)
        return connector.quantize_order_price(self.hedge_trading_pair, reference_price)

    def _should_refresh_entry_order(self) -> bool:
        if self.entry_order is None or self.entry_order.order is None or not self.entry_order.order.is_open:
            return False
        current_price = self.entry_order.order.price
        target_price = self._entry_price()
        if current_price is None or current_price <= Decimal("0") or target_price <= Decimal("0"):
            return False
        price_deviation = abs(target_price - current_price) / current_price
        if price_deviation >= self.config.entry_price_refresh_pct:
            return True
        order_age = self._strategy.current_timestamp - self.entry_order.order.creation_timestamp
        return order_age >= self.config.entry_order_max_age_seconds and price_deviation > Decimal("0")

    def place_entry_order(self):
        order_id = self.place_order(
            connector_name=self.entry_connector,
            trading_pair=self.entry_trading_pair,
            order_type=self._entry_order_type(),
            side=self.entry_side,
            amount=self.config.order_amount,
            price=self._entry_price(),
            metadata={"order_role": "entry"},
        )
        self.entry_order = TrackedOrder(order_id=order_id)

    def place_hedge_order(self, amount: Decimal):
        order_id = self.place_order(
            connector_name=self.hedge_connector,
            trading_pair=self.hedge_trading_pair,
            order_type=self.config.hedge_order_type,
            side=self.hedge_side,
            amount=amount,
            price=self._hedge_price(amount),
            metadata={"order_role": "hedge"},
        )
        self.hedge_order = TrackedOrder(order_id=order_id)
        self._hedged_base_submitted += amount

    def update_tracked_order(self, tracked_order: Optional[TrackedOrder], connector_name: str, order_id: str):
        if tracked_order is None or tracked_order.order_id != order_id:
            return
        in_flight_order = self.get_in_flight_order(connector_name, order_id)
        if in_flight_order is not None:
            tracked_order.order = in_flight_order

    def process_order_created_event(self, _, market, event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        self.update_tracked_order(self.entry_order, self.entry_connector, event.order_id)
        self.update_tracked_order(self.hedge_order, self.hedge_connector, event.order_id)

    def process_order_filled_event(self, _, market, event: OrderFilledEvent):
        self.update_tracked_order(self.entry_order, self.entry_connector, event.order_id)
        self.update_tracked_order(self.hedge_order, self.hedge_connector, event.order_id)
        if self.entry_order is not None and self.entry_order.order_id == event.order_id:
            self._entry_filled_base_backup += Decimal(event.amount)
            self._entry_filled_quote_backup += Decimal(event.amount) * Decimal(event.price)
        elif self.hedge_order is not None and self.hedge_order.order_id == event.order_id:
            self._hedge_filled_base_backup += Decimal(event.amount)
            self._hedge_filled_quote_backup += Decimal(event.amount) * Decimal(event.price)

    def process_order_completed_event(self, _, market, event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        self.update_tracked_order(self.entry_order, self.entry_connector, event.order_id)
        self.update_tracked_order(self.hedge_order, self.hedge_connector, event.order_id)
        if self.entry_order is not None and self.entry_order.order_id == event.order_id:
            self._entry_filled_base_backup = max(self._entry_filled_base_backup, Decimal(event.base_asset_amount))
            self._entry_filled_quote_backup = max(self._entry_filled_quote_backup, Decimal(event.quote_asset_amount))
        elif self.hedge_order is not None and self.hedge_order.order_id == event.order_id:
            self._hedge_filled_base_backup = max(self._hedge_filled_base_backup, Decimal(event.base_asset_amount))
            self._hedge_filled_quote_backup = max(self._hedge_filled_quote_backup, Decimal(event.quote_asset_amount))

    def process_order_canceled_event(self, _, market: ConnectorBase, event: OrderCancelledEvent):
        self.update_tracked_order(self.entry_order, self.entry_connector, event.order_id)
        self.update_tracked_order(self.hedge_order, self.hedge_connector, event.order_id)
        if self.hedge_order is not None and self.hedge_order.order_id == event.order_id:
            self._hedged_base_submitted = self._hedged_base_completed
            self.hedge_order = None
            self._current_retries += 1

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        if self.hedge_order is not None and self.hedge_order.order_id == event.order_id:
            self._hedged_base_submitted = self._hedged_base_completed
            self.hedge_order = None
            self._current_retries += 1
            if self._current_retries > self._max_retries:
                self.close_type = CloseType.FAILED
                self.stop()
        elif self.entry_order is not None and self.entry_order.order_id == event.order_id:
            self.close_type = CloseType.FAILED
            self.stop()

    async def control_shutdown_process(self):
        if self.entry_order is not None and self.entry_order.order is not None and self.entry_order.order.is_open:
            self._strategy.cancel(self.entry_connector, self.entry_trading_pair, self.entry_order.order_id)
        if self.hedge_order is not None and self.hedge_order.order is not None and self.hedge_order.order.is_open:
            self._strategy.cancel(self.hedge_connector, self.hedge_trading_pair, self.hedge_order.order_id)
        if self._ready_to_complete():
            self.stop()
        await asyncio.sleep(1.0)

    def early_stop(self, keep_position: bool = False):
        if keep_position:
            self.close_type = CloseType.POSITION_HOLD
            self.stop()
            return
        self.close_type = CloseType.EARLY_STOP
        self._status = RunnableStatus.SHUTTING_DOWN

    async def validate_sufficient_balance(self):
        order_candidate = OrderCandidate(
            trading_pair=self.entry_trading_pair,
            is_maker=self.config.entry_style == "passive",
            order_type=self._entry_order_type(),
            order_side=self.entry_side,
            amount=self.config.order_amount,
            price=self._entry_price(),
        )
        adjusted = self.adjust_order_candidates(self.entry_connector, [order_candidate])[0]
        if adjusted.amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.stop()

    def get_net_pnl_quote(self) -> Decimal:
        matched_qty = min(
            self._entry_executed_amount_base(),
            max(self._hedged_base_completed, self._hedge_executed_amount_base()),
        )
        if matched_qty <= Decimal("0") or self.entry_order is None:
            return Decimal("0")
        entry_price = self.entry_order.average_executed_price
        hedge_price = Decimal("0")
        if self.hedge_order is not None and self.hedge_order.executed_amount_base > Decimal("0"):
            hedge_price = self.hedge_order.average_executed_price
        if hedge_price == Decimal("0"):
            return Decimal("0")
        if self.entry_side == TradeType.SELL:
            gross = (entry_price - hedge_price) * matched_qty
        else:
            gross = (hedge_price - entry_price) * matched_qty
        return gross - self.get_cum_fees_quote()

    def get_net_pnl_pct(self) -> Decimal:
        if self.config.order_amount <= Decimal("0"):
            return Decimal("0")
        entry_price = self.entry_order.average_executed_price if self.entry_order is not None else Decimal("0")
        notional = self.config.order_amount * entry_price
        if notional <= Decimal("0"):
            return Decimal("0")
        return self.get_net_pnl_quote() / notional

    def get_cum_fees_quote(self) -> Decimal:
        current_entry_fees = self.entry_order.cum_fees_quote if self.entry_order is not None else Decimal("0")
        current_hedge_fees = self.hedge_order.cum_fees_quote if self.hedge_order is not None else Decimal("0")
        return max(self._entry_fee_quote, current_entry_fees) + self._hedge_fee_quote + current_hedge_fees

    def get_custom_info(self) -> Dict:
        return {
            "side": self.entry_side,
            "execution_purpose": self.config.execution_purpose,
            "inventory_rebalance_reduces_delta": True,
            "entry_style": self.config.entry_style,
            "entry_connector": self.entry_connector,
            "hedge_connector": self.hedge_connector,
            "entry_order_id": self.entry_order.order_id if self.entry_order else None,
            "hedge_order_id": self.hedge_order.order_id if self.hedge_order else None,
            "inventory_delta_before": self.config.inventory_delta_before,
            "target_inventory_delta_after": self.config.target_inventory_delta_after,
            "expected_pnl_quote": self.config.expected_pnl_quote,
            "hedged_base_completed": self._hedged_base_completed,
            "unhedged_base": self._unhedged_entry_base(),
            "loop_metrics": self.get_loop_metrics(),
        }
