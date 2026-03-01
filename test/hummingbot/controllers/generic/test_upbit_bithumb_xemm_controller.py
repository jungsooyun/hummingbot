import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, PropertyMock

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from controllers.generic.upbit_bithumb_xemm_controller import (
    UpbitBithumbXemmController,
    UpbitBithumbXemmControllerConfig,
)
from hummingbot.core.data_type.common import OrderType, TradeType


class TestUpbitBithumbXemmController(IsolatedAsyncioWrapperTestCase):

    def setUp(self):
        super().setUp()
        self.config = UpbitBithumbXemmControllerConfig(
            id="test-controller",
            total_amount_quote=Decimal("1000"),
            maker_connector="bithumb",
            maker_trading_pair="BTC-KRW",
            taker_connector="upbit",
            taker_trading_pair="BTC-KRW",
            buy_levels_targets_amount="0.001,1-0.002,2",
            sell_levels_targets_amount="0.001,1-0.002,2",
            max_executors_per_side=2,
            taker_order_type=OrderType.LIMIT,
            taker_slippage_buffer_bps=Decimal("7"),
            taker_fallback_to_market=False,
        )

        self.mock_market_data_provider = MagicMock()
        type(self.mock_market_data_provider).ready = PropertyMock(return_value=True)
        self.mock_market_data_provider.time.return_value = 100.0
        self.mock_market_data_provider.get_price_by_type.return_value = Decimal("100")
        self.mock_market_data_provider.quantize_order_amount.side_effect = lambda c, p, a: a

        bithumb_connector = MagicMock()
        upbit_connector = MagicMock()
        bithumb_connector.get_available_balance.return_value = Decimal("1")
        upbit_connector.get_available_balance.return_value = Decimal("1")
        self.mock_market_data_provider.connectors = {
            "bithumb": bithumb_connector,
            "upbit": upbit_connector,
        }

        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)
        self.controller = UpbitBithumbXemmController(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

    def test_validate_levels_targets_amount_from_string(self):
        parsed = self.config.buy_levels_targets_amount
        self.assertEqual(parsed, [[Decimal("0.001"), Decimal("1")], [Decimal("0.002"), Decimal("2")]])

    def test_taker_order_type_accepts_string(self):
        config = UpbitBithumbXemmControllerConfig(
            id="string-order-type",
            maker_connector="bithumb",
            maker_trading_pair="BTC-KRW",
            taker_connector="upbit",
            taker_trading_pair="BTC-KRW",
            taker_order_type="LIMIT",
        )
        self.assertEqual(config.taker_order_type, OrderType.LIMIT)

    def test_determine_executor_actions_creates_both_sides(self):
        self.controller.executors_info = []
        actions = self.controller.determine_executor_actions()

        self.assertEqual(4, len(actions))
        buy_actions = [action for action in actions if action.executor_config.maker_side == TradeType.BUY]
        sell_actions = [action for action in actions if action.executor_config.maker_side == TradeType.SELL]

        self.assertEqual(2, len(buy_actions))
        self.assertEqual(2, len(sell_actions))
        self.assertTrue(all(action.executor_config.taker_order_type == OrderType.LIMIT for action in actions))
        self.assertTrue(all(action.executor_config.taker_slippage_buffer_bps == Decimal("7") for action in actions))
        self.assertTrue(all(action.executor_config.taker_fallback_to_market is False for action in actions))

        # 먼 호가(weight=2)가 가까운 호가(weight=1)보다 더 큰 주문량인지 확인
        buy_actions_by_target = sorted(buy_actions, key=lambda action: action.executor_config.target_profitability)
        self.assertLess(
            buy_actions_by_target[0].executor_config.order_amount,
            buy_actions_by_target[1].executor_config.order_amount,
        )

    def test_inventory_skew_blocks_buy_side(self):
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("5")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        self.controller.config.max_inventory_skew_base = Decimal("0.5")

        actions = self.controller.determine_executor_actions()
        self.assertGreater(len(actions), 0)
        self.assertTrue(all(action.executor_config.maker_side == TradeType.SELL for action in actions))

    def test_quote_mismatch_raises_validation_error(self):
        with self.assertRaises(ValueError):
            UpbitBithumbXemmControllerConfig(
                id="invalid",
                maker_connector="bithumb",
                maker_trading_pair="BTC-KRW",
                taker_connector="upbit",
                taker_trading_pair="BTC-USDT",
            )
