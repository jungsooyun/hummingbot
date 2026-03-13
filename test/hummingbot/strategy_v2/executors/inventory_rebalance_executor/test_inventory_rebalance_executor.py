from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.inventory_rebalance_executor.data_types import InventoryRebalanceExecutorConfig
from hummingbot.strategy_v2.executors.inventory_rebalance_executor.inventory_rebalance_executor import (
    InventoryRebalanceExecutor,
)
from hummingbot.strategy_v2.models.base import RunnableStatus


class TestInventoryRebalanceExecutor(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.strategy = self.create_mock_strategy()
        self.config = InventoryRebalanceExecutorConfig(
            timestamp=1234,
            entry_market=ConnectorPair(connector_name="bithumb", trading_pair="IP-KRW"),
            hedge_market=ConnectorPair(connector_name="upbit", trading_pair="IP-KRW"),
            entry_side=TradeType.SELL,
            entry_style="passive",
            order_amount=Decimal("5"),
            inventory_delta_before=Decimal("12"),
            target_inventory_delta_after=Decimal("2"),
            expected_pnl_quote=Decimal("10"),
            hedge_order_type=OrderType.LIMIT,
            hedge_slippage_buffer_bps=Decimal("10"),
        )
        self.executor = InventoryRebalanceExecutor(self.strategy, self.config, update_interval=0.5)
        self.executor._status = RunnableStatus.RUNNING

    @staticmethod
    def create_mock_strategy():
        strategy = MagicMock(spec=ScriptStrategyBase)
        strategy.current_timestamp = 100.0
        strategy.buy.side_effect = ["ENTRY-BUY-1", "HEDGE-BUY-1", "HEDGE-BUY-2"]
        strategy.sell.side_effect = ["ENTRY-SELL-1", "HEDGE-SELL-1", "HEDGE-SELL-2"]

        bithumb = MagicMock(spec=ExchangePyBase)
        upbit = MagicMock(spec=ExchangePyBase)
        for connector in (bithumb, upbit):
            connector.supported_order_types.return_value = [OrderType.LIMIT, OrderType.MARKET]
            connector.get_price_by_type.side_effect = lambda trading_pair, price_type: Decimal("100")
            connector.quantize_order_amount.side_effect = lambda trading_pair, amount: amount
            connector.quantize_order_price.side_effect = lambda trading_pair, price: price
            connector.get_available_balance.return_value = Decimal("100")
            connector.get_fee.side_effect = Exception("not implemented in test")
            connector.in_flight_orders = {}
            connector._order_tracker = MagicMock()
            connector._order_tracker.fetch_order.return_value = None
        strategy.connectors = {"bithumb": bithumb, "upbit": upbit}
        return strategy

    async def test_control_task_places_entry_order(self):
        await self.executor.control_task()

        self.assertIsNotNone(self.executor.entry_order)
        self.strategy.sell.assert_called_once()
        self.assertEqual("ENTRY-SELL-1", self.executor.entry_order.order_id)

    async def test_entry_fill_places_hedge_order(self):
        await self.executor.control_task()
        self.executor.entry_order.order = InFlightOrder(
            client_order_id="ENTRY-SELL-1",
            trading_pair="IP-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal("5"),
            creation_timestamp=100.0,
            price=Decimal("101"),
            initial_state=OrderState.OPEN,
        )
        self.executor.entry_order.order.executed_amount_base = Decimal("2")
        self.executor.entry_order.order.executed_amount_quote = Decimal("202")

        created_event = SellOrderCreatedEvent(
            timestamp=100.1,
            type=OrderType.LIMIT,
            trading_pair="IP-KRW",
            amount=Decimal("2"),
            price=Decimal("101"),
            order_id="ENTRY-SELL-1",
            creation_timestamp=100.0,
            exchange_order_id="EOID-ENTRY-1",
        )
        self.executor.process_order_created_event(0, self.strategy.connectors["bithumb"], created_event)
        fill_event = OrderFilledEvent(
            timestamp=100.2,
            order_id="ENTRY-SELL-1",
            trading_pair="IP-KRW",
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("101"),
            amount=Decimal("2"),
            trade_fee=MagicMock(),
            exchange_trade_id="trade-1",
        )
        self.executor.process_order_filled_event(0, self.strategy.connectors["bithumb"], fill_event)

        await self.executor.control_task()

        self.assertIsNotNone(self.executor.hedge_order)
        self.strategy.buy.assert_called_once()
        self.assertEqual("ENTRY-BUY-1", self.executor.hedge_order.order_id)

    def test_get_custom_info_reports_rebalance_metadata(self):
        custom_info = self.executor.get_custom_info()

        self.assertEqual(TradeType.SELL, custom_info["side"])
        self.assertEqual("inventory_rebalance", custom_info["execution_purpose"])
        self.assertEqual("passive", custom_info["entry_style"])
        self.assertTrue(custom_info["inventory_rebalance_reduces_delta"])
