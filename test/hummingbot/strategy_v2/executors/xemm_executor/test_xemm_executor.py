from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, PropertyMock, patch

from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class TestXEMMExecutor(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self):
        super().setUp()
        self.strategy = self.create_mock_strategy()
        self.xemm_base_config = self.base_config_long
        self.update_interval = 0.5
        self.executor = XEMMExecutor(self.strategy, self.xemm_base_config, self.update_interval)
        self.set_loggers(loggers=[self.executor.logger()])

    @property
    def base_config_long(self) -> XEMMExecutorConfig:
        return XEMMExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name='binance', trading_pair='ETH-USDT'),
            selling_market=ConnectorPair(connector_name='kucoin', trading_pair='ETH-USDT'),
            maker_side=TradeType.BUY,
            order_amount=Decimal('100'),
            min_profitability=Decimal('0.01'),
            target_profitability=Decimal('0.015'),
            max_profitability=Decimal('0.02'),
        )

    @property
    def base_config_short(self) -> XEMMExecutorConfig:
        return XEMMExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name='binance', trading_pair='ETH-USDT'),
            selling_market=ConnectorPair(connector_name='kucoin', trading_pair='ETH-USDT'),
            maker_side=TradeType.SELL,
            order_amount=Decimal('100'),
            min_profitability=Decimal('0.01'),
            target_profitability=Decimal('0.015'),
            max_profitability=Decimal('0.02'),
        )

    @property
    def base_config_long_limit_taker(self) -> XEMMExecutorConfig:
        return XEMMExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name='binance', trading_pair='ETH-USDT'),
            selling_market=ConnectorPair(connector_name='kucoin', trading_pair='ETH-USDT'),
            maker_side=TradeType.BUY,
            order_amount=Decimal('100'),
            min_profitability=Decimal('0.01'),
            target_profitability=Decimal('0.015'),
            max_profitability=Decimal('0.02'),
            taker_order_type=OrderType.LIMIT,
            taker_slippage_buffer_bps=Decimal("10"),
        )

    @property
    def base_config_shadow(self) -> XEMMExecutorConfig:
        return XEMMExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name='bithumb', trading_pair='XRP-KRW'),
            selling_market=ConnectorPair(connector_name='upbit', trading_pair='XRP-KRW'),
            maker_side=TradeType.BUY,
            order_amount=Decimal('10'),
            min_profitability=Decimal('0.001'),
            target_profitability=Decimal('0.0015'),
            max_profitability=Decimal('0.01'),
            taker_order_type=OrderType.LIMIT,
            shadow_maker_enabled=True,
        )

    @staticmethod
    def create_mock_strategy():
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market

        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="ETH-USDT")
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2", "OID-BUY-3"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2", "OID-SELL-3"]
        strategy.cancel.return_value = None
        strategy.current_timestamp = 1300.0
        binance_connector = MagicMock(spec=ExchangePyBase)
        binance_connector.supported_order_types = MagicMock(return_value=[OrderType.LIMIT, OrderType.MARKET])
        kucoin_connector = MagicMock(spec=ExchangePyBase)
        kucoin_connector.supported_order_types = MagicMock(return_value=[OrderType.LIMIT, OrderType.MARKET])
        strategy.connectors = {
            "binance": binance_connector,
            "kucoin": kucoin_connector,
        }
        return strategy

    @staticmethod
    def create_shadow_mock_strategy(include_limit_maker: bool = True):
        strategy = TestXEMMExecutor.create_mock_strategy()
        bithumb_connector = MagicMock(spec=ExchangePyBase)
        upbit_connector = MagicMock(spec=ExchangePyBase)
        supported = [OrderType.LIMIT, OrderType.MARKET]
        if include_limit_maker:
            supported.append(OrderType.LIMIT_MAKER)
        bithumb_connector.supported_order_types = MagicMock(return_value=[OrderType.LIMIT, OrderType.MARKET])
        upbit_connector.supported_order_types = MagicMock(return_value=supported)
        for connector in [bithumb_connector, upbit_connector]:
            connector.get_price_by_type.return_value = Decimal("100")
            connector.quantize_order_amount.side_effect = lambda pair, amount: amount
            connector.quantize_order_price.side_effect = lambda pair, price: price
            connector.get_order_price_quantum.side_effect = lambda pair, price: Decimal("1")
            connector.get_price_for_volume.return_value = SimpleNamespace(result_price=Decimal("100"))
            connector.in_flight_orders = {}
        strategy.connectors = {
            "bithumb": bithumb_connector,
            "upbit": upbit_connector,
        }
        return strategy

    def test_is_arbitrage_valid(self):
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-USDT', 'ETH-USDT'))
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-BUSD', 'ETH-USDT'))
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-USDT', 'WETH-USDT'))
        self.assertFalse(self.executor.is_arbitrage_valid('ETH-USDT', 'BTC-USDT'))
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-USDT', 'ETH-BTC'))

    def test_shadow_maker_requires_limit_maker_support(self):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=False)
        with self.assertRaises(ValueError):
            XEMMExecutor(strategy, self.base_config_shadow, self.update_interval)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_control_task_creates_shadow_order_when_enabled(self, tx_cost_mock, resulting_price_mock):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        executor = XEMMExecutor(strategy, self.base_config_shadow, self.update_interval)
        tx_cost_mock.return_value = Decimal("0.01")
        resulting_price_mock.return_value = Decimal("100")
        executor._status = RunnableStatus.RUNNING

        await executor.control_task()

        self.assertIsNotNone(executor.maker_order)
        self.assertIsNotNone(executor.shadow_order)
        strategy.sell.assert_called()
        sell_args, _ = strategy.sell.call_args
        self.assertEqual(sell_args[0], "upbit")
        self.assertEqual(sell_args[3], OrderType.LIMIT_MAKER)

    def test_shadow_fill_enters_prefill_mode(self):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        executor = XEMMExecutor(strategy, self.base_config_shadow, self.update_interval)
        executor.shadow_order = TrackedOrder(order_id="OID-SHADOW-1")

        fill_event = OrderFilledEvent(
            timestamp=1234.0,
            order_id="OID-SHADOW-1",
            trading_pair="XRP-KRW",
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT_MAKER,
            price=Decimal("100"),
            amount=Decimal("2"),
            trade_fee=MagicMock(),
        )
        executor.process_order_filled_event(1, MagicMock(), fill_event)

        self.assertTrue(executor._shadow_prefill_mode)
        self.assertEqual("cancel_maker", executor._shadow_prefill_stage)
        self.assertEqual(Decimal("2"), executor._shadow_prefill_remaining_base)
        self.assertEqual(Decimal("200"), executor._shadow_prefill_remaining_quote)

    async def test_shadow_prefill_cross_blocked_by_profit_guard(self):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        config = self.base_config_shadow
        config.allow_loss_hedge = False
        config.min_profitability_guard = Decimal("0.001")
        executor = XEMMExecutor(strategy, config, self.update_interval)
        executor._shadow_prefill_remaining_base = Decimal("2")
        executor._shadow_prefill_remaining_quote = Decimal("200")

        with patch.object(executor, "_get_aggressive_limit_price", return_value=Decimal("101")):
            await executor._place_shadow_prefill_cross_order()

        self.assertIsNone(executor.maker_order)
        strategy.buy.assert_not_called()

    async def test_shadow_prefill_unwind_allowed_when_loss_hedge_enabled(self):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        config = self.base_config_shadow
        config.allow_loss_hedge = True
        config.min_profitability_guard = Decimal("0.01")
        config.taker_order_type = OrderType.LIMIT
        executor = XEMMExecutor(strategy, config, self.update_interval)
        executor._shadow_prefill_remaining_base = Decimal("2")
        executor._shadow_prefill_remaining_quote = Decimal("200")

        with patch.object(executor, "_get_aggressive_limit_price", return_value=Decimal("101")):
            await executor._place_shadow_prefill_unwind_order()

        self.assertIsNotNone(executor._shadow_prefill_unwind_order)
        strategy.buy.assert_called()

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_update_prices_best_source_uses_best_ask_for_maker_sell(self, tx_cost_mock, resulting_price_mock):
        strategy = self.create_mock_strategy()
        config = self.base_config_short
        config.maker_price_source = "best"
        executor = XEMMExecutor(strategy, config, self.update_interval)
        tx_cost_mock.return_value = Decimal("0")
        resulting_price_mock.return_value = Decimal("100")
        strategy.connectors[executor.maker_connector].get_price.return_value = Decimal("101")
        strategy.connectors[executor.maker_connector].quantize_order_price.side_effect = lambda pair, price: price

        await executor.update_prices_and_tx_costs()

        self.assertEqual(Decimal("101"), executor._maker_target_price)
        strategy.connectors[executor.maker_connector].get_price.assert_called_with(executor.maker_trading_pair, True)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_update_prices_best_source_uses_best_bid_for_maker_buy(self, tx_cost_mock, resulting_price_mock):
        strategy = self.create_mock_strategy()
        config = self.base_config_long
        config.maker_price_source = "best"
        executor = XEMMExecutor(strategy, config, self.update_interval)
        tx_cost_mock.return_value = Decimal("0")
        resulting_price_mock.return_value = Decimal("100")
        strategy.connectors[executor.maker_connector].get_price.return_value = Decimal("99")
        strategy.connectors[executor.maker_connector].quantize_order_price.side_effect = lambda pair, price: price

        await executor.update_prices_and_tx_costs()

        self.assertEqual(Decimal("99"), executor._maker_target_price)
        strategy.connectors[executor.maker_connector].get_price.assert_called_with(executor.maker_trading_pair, False)

    async def test_create_maker_order_skips_when_profitability_recheck_fails(self):
        strategy = self.create_mock_strategy()
        executor = XEMMExecutor(strategy, self.base_config_long, self.update_interval)
        executor._maker_target_price = Decimal("100")
        with (
            patch.object(executor, "_safe_maker_price_with_stp", return_value=Decimal("100")),
            patch.object(executor, "_estimate_profitability_for_maker_price", AsyncMock(return_value=Decimal("0"))),
        ):
            await executor.create_maker_order()

        self.assertIsNone(executor.maker_order)
        strategy.buy.assert_not_called()
        strategy.sell.assert_not_called()

    def test_safe_maker_price_with_stp_offsets_away_from_conflict(self):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        config = self.base_config_shadow
        config.bithumb_stp_prevention_enabled = True
        config.bithumb_stp_base_offset_ticks = 1
        config.bithumb_stp_max_offset_ticks = 3
        executor = XEMMExecutor(strategy, config, self.update_interval)

        blocker = InFlightOrder(
            client_order_id="OID-SELL-BLOCKER",
            exchange_order_id="EX-OID-SELL-BLOCKER",
            trading_pair="XRP-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1234,
            initial_state=OrderState.OPEN,
        )
        strategy.connectors["bithumb"].in_flight_orders = {blocker.client_order_id: blocker}

        safe_price = executor._safe_maker_price_with_stp(Decimal("100"))
        self.assertEqual(Decimal("99"), safe_price)
        self.assertEqual("OID-SELL-BLOCKER", executor._stp_last_blocker_order_id)

    def test_safe_maker_price_with_stp_ignores_pending_cancel_when_disabled(self):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        config = self.base_config_shadow
        config.bithumb_stp_prevention_enabled = True
        config.bithumb_stp_consider_pending_cancel_as_conflict = False
        executor = XEMMExecutor(strategy, config, self.update_interval)

        blocker = InFlightOrder(
            client_order_id="OID-SELL-PENDING-CANCEL",
            exchange_order_id="EX-OID-SELL-PENDING-CANCEL",
            trading_pair="XRP-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal("1"),
            price=Decimal("100"),
            creation_timestamp=1234,
            initial_state=OrderState.PENDING_CANCEL,
        )
        strategy.connectors["bithumb"].in_flight_orders = {blocker.client_order_id: blocker}

        safe_price = executor._safe_maker_price_with_stp(Decimal("100"))
        self.assertEqual(Decimal("100"), safe_price)

    async def test_create_maker_order_skips_when_no_safe_stp_price(self):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        config = self.base_config_shadow
        config.bithumb_stp_prevention_enabled = True
        config.bithumb_stp_base_offset_ticks = 1
        config.bithumb_stp_max_offset_ticks = 3
        executor = XEMMExecutor(strategy, config, self.update_interval)
        executor._maker_target_price = Decimal("100")

        blockers = {}
        for idx, price in enumerate([Decimal("100"), Decimal("99"), Decimal("98"), Decimal("97")], start=1):
            blocker = InFlightOrder(
                client_order_id=f"OID-SELL-BLOCKER-{idx}",
                exchange_order_id=f"EX-BLOCKER-{idx}",
                trading_pair="XRP-KRW",
                order_type=OrderType.LIMIT,
                trade_type=TradeType.SELL,
                amount=Decimal("1"),
                price=price,
                creation_timestamp=1234,
                initial_state=OrderState.OPEN,
            )
            blockers[blocker.client_order_id] = blocker
        strategy.connectors["bithumb"].in_flight_orders = blockers

        await executor.create_maker_order()

        self.assertIsNone(executor.maker_order)
        self.assertGreater(executor._stp_cooldown_until_ts, strategy.current_timestamp)

    def test_process_order_failed_event_stp_rejection_updates_state(self):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        config = self.base_config_shadow
        config.bithumb_stp_prevention_enabled = True
        config.bithumb_stp_pause_after_rejects = 3
        executor = XEMMExecutor(strategy, config, self.update_interval)
        executor.maker_order = TrackedOrder(order_id="OID-BUY-1")

        failure_event = MarketOrderFailureEvent(
            timestamp=1234,
            order_id="OID-BUY-1",
            order_type=OrderType.LIMIT,
            error_type="BithumbSelfTradePreventionError",
            error_message="cross_trading",
        )
        executor.process_order_failed_event(1, MagicMock(), failure_event)

        self.assertIsNone(executor.maker_order)
        self.assertEqual(1, executor._stp_reject_streak)
        self.assertEqual(2, executor._stp_dynamic_offset_ticks)
        self.assertGreater(executor._stp_cooldown_until_ts, strategy.current_timestamp)

    def test_process_order_failed_event_stp_rejection_triggers_pause(self):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        config = self.base_config_shadow
        config.bithumb_stp_prevention_enabled = True
        config.bithumb_stp_pause_after_rejects = 2
        config.bithumb_stp_pause_duration_sec = 20.0
        executor = XEMMExecutor(strategy, config, self.update_interval)

        executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        event_1 = MarketOrderFailureEvent(
            timestamp=1234,
            order_id="OID-BUY-1",
            order_type=OrderType.LIMIT,
            error_message="cross_trading",
        )
        executor.process_order_failed_event(1, MagicMock(), event_1)

        executor.maker_order = TrackedOrder(order_id="OID-BUY-2")
        event_2 = MarketOrderFailureEvent(
            timestamp=1235,
            order_id="OID-BUY-2",
            order_type=OrderType.LIMIT,
            error_message="cross_trading",
        )
        executor.process_order_failed_event(1, MagicMock(), event_2)

        self.assertGreater(executor._stp_pause_until_ts, strategy.current_timestamp)

    async def test_control_maker_order_skips_create_during_stp_pause(self):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        config = self.base_config_shadow
        executor = XEMMExecutor(strategy, config, self.update_interval)
        executor._stp_pause_until_ts = strategy.current_timestamp + 10
        executor.create_maker_order = AsyncMock()

        await executor.control_maker_order()

        executor.create_maker_order.assert_not_called()

    def test_net_pnl_long(self):
        self.executor._status = RunnableStatus.TERMINATED
        self.executor.maker_order = Mock(spec=TrackedOrder)
        self.executor.taker_order = Mock(spec=TrackedOrder)
        self.executor.maker_order.executed_amount_base = Decimal('1')
        self.executor.taker_order.executed_amount_base = Decimal('1')
        self.executor.maker_order.average_executed_price = Decimal('100')
        self.executor.taker_order.average_executed_price = Decimal('200')
        self.executor.maker_order.cum_fees_quote = Decimal('1')
        self.executor.taker_order.cum_fees_quote = Decimal('1')
        self.assertEqual(self.executor.net_pnl_quote, Decimal('98'))
        self.assertEqual(self.executor.net_pnl_pct, Decimal('0.98'))

    def test_net_pnl_short(self):
        executor = XEMMExecutor(self.strategy, self.base_config_short, self.update_interval)
        executor._status = RunnableStatus.TERMINATED
        executor.maker_order = Mock(spec=TrackedOrder)
        executor.taker_order = Mock(spec=TrackedOrder)
        executor.maker_order.executed_amount_base = Decimal('1')
        executor.taker_order.executed_amount_base = Decimal('1')
        executor.maker_order.average_executed_price = Decimal('100')
        executor.taker_order.average_executed_price = Decimal('200')
        executor.maker_order.cum_fees_quote = Decimal('1')
        executor.taker_order.cum_fees_quote = Decimal('1')
        self.assertEqual(executor.net_pnl_quote, Decimal('98'))
        self.assertEqual(executor.net_pnl_pct, Decimal('0.98'))

    @patch.object(XEMMExecutor, 'get_trading_rules')
    @patch.object(XEMMExecutor, 'adjust_order_candidates')
    async def test_validate_sufficient_balance(self, mock_adjust_order_candidates, mock_get_trading_rules):
        # Mock trading rules
        trading_rules = TradingRule(trading_pair="ETH-USDT", min_order_size=Decimal("0.1"),
                                    min_price_increment=Decimal("0.1"), min_base_amount_increment=Decimal("0.1"))
        mock_get_trading_rules.return_value = trading_rules
        order_candidate = OrderCandidate(
            trading_pair="ETH-USDT",
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100")
        )
        # Test for sufficient balance
        mock_adjust_order_candidates.return_value = [order_candidate]
        await self.executor.validate_sufficient_balance()
        self.assertNotEqual(self.executor.close_type, CloseType.INSUFFICIENT_BALANCE)

        # Test for insufficient balance
        order_candidate.amount = Decimal("0")
        mock_adjust_order_candidates.return_value = [order_candidate]
        await self.executor.validate_sufficient_balance()
        self.assertEqual(self.executor.close_type, CloseType.INSUFFICIENT_BALANCE)
        self.assertEqual(self.executor.status, RunnableStatus.TERMINATED)

    @patch.object(XEMMExecutor, 'adjust_order_candidates')
    async def test_validate_sufficient_balance_allows_one_sided_mode(self, mock_adjust_order_candidates):
        config = XEMMExecutorConfig(
            timestamp=1234,
            buying_market=ConnectorPair(connector_name='binance', trading_pair='ETH-USDT'),
            selling_market=ConnectorPair(connector_name='kucoin', trading_pair='ETH-USDT'),
            maker_side=TradeType.BUY,
            order_amount=Decimal('1'),
            min_profitability=Decimal('0.01'),
            target_profitability=Decimal('0.015'),
            max_profitability=Decimal('0.02'),
            allow_one_sided_inventory_mode=True,
        )
        executor = XEMMExecutor(self.strategy, config, self.update_interval)
        maker_candidate = OrderCandidate(
            trading_pair="ETH-USDT",
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("100"),
        )
        taker_candidate = OrderCandidate(
            trading_pair="ETH-USDT",
            is_maker=False,
            order_type=OrderType.MARKET,
            order_side=TradeType.SELL,
            amount=Decimal("0"),
            price=Decimal("100"),
        )
        mock_adjust_order_candidates.side_effect = [
            [maker_candidate],
            [taker_candidate],
        ]

        await executor.validate_sufficient_balance()

        self.assertNotEqual(executor.close_type, CloseType.INSUFFICIENT_BALANCE)
        self.assertNotEqual(executor.status, RunnableStatus.TERMINATED)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_control_task_running_order_not_placed(self, tx_cost_mock, resulting_price_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.return_value = Decimal("100")
        self.executor._status = RunnableStatus.RUNNING
        await self.executor.control_task()
        # Calculate expected maker target price using the new formula:
        # maker_price = taker_price / (1 + target_profitability + tx_cost_pct)
        # tx_cost_pct = (0.01 + 0.01) / 100 = 0.0002
        # maker_price = 100 / (1 + 0.015 + 0.0002) = 100 / 1.0152
        expected_price = Decimal("100") / (Decimal("1") + Decimal("0.015") + Decimal("0.02") / Decimal("100"))
        self.assertEqual(self.executor._status, RunnableStatus.RUNNING)
        self.assertEqual(self.executor.maker_order.order_id, "OID-BUY-1")
        self.assertEqual(self.executor._maker_target_price, expected_price)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_control_task_running_order_not_placed_sell_side(self, tx_cost_mock, resulting_price_mock):
        # Test maker SELL side (taker BUY) to cover line 155
        executor = XEMMExecutor(self.strategy, self.base_config_short, self.update_interval)
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.return_value = Decimal("100")
        executor._status = RunnableStatus.RUNNING
        await executor.control_task()
        # Calculate expected maker target price using the new formula for SELL side:
        # maker_price = taker_price / (1 - target_profitability - tx_cost_pct)
        # tx_cost_pct = (0.01 + 0.01) / 100 = 0.0002
        # maker_price = 100 / (1 - 0.015 - 0.0002) = 100 / 0.9848
        expected_price = Decimal("100") / (Decimal("1") - Decimal("0.015") - Decimal("0.02") / Decimal("100"))
        self.assertEqual(executor._status, RunnableStatus.RUNNING)
        self.assertEqual(executor.maker_order.order_id, "OID-SELL-1")
        self.assertEqual(executor._maker_target_price, expected_price)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_control_task_running_order_placed_refresh_condition_min_profitability(self, tx_cost_mock,
                                                                                         resulting_price_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.return_value = Decimal("100")
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = Mock(spec=TrackedOrder)
        self.executor.maker_order.order_id = "OID-BUY-1"
        self.executor.maker_order.order = InFlightOrder(
            creation_timestamp=1234,
            trading_pair="ETH-USDT",
            client_order_id="OID-BUY-1",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("100"),
            price=Decimal("99.5"),
            initial_state=OrderState.OPEN,
        )
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.RUNNING)
        self.assertIsNotNone(self.executor.maker_order)
        self.assertTrue(self.executor._maker_cancel_in_flight)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_control_task_running_order_placed_refresh_condition_max_profitability(self, tx_cost_mock,
                                                                                         resulting_price_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.return_value = Decimal("103")
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = Mock(spec=TrackedOrder)
        self.executor.maker_order.order_id = "OID-BUY-1"
        self.executor.maker_order.order = InFlightOrder(
            creation_timestamp=1234,
            trading_pair="ETH-USDT",
            client_order_id="OID-BUY-1",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("100"),
            price=Decimal("99.5"),
            initial_state=OrderState.OPEN,
        )
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.RUNNING)
        self.assertIsNotNone(self.executor.maker_order)
        self.assertTrue(self.executor._maker_cancel_in_flight)

    @patch.object(XEMMExecutor, "get_resulting_price_for_amount")
    @patch.object(XEMMExecutor, "get_tx_cost_in_asset")
    async def test_control_pending_maker_cancel_avoids_spamming_cancel(self, tx_cost_mock, resulting_price_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.return_value = Decimal("100")
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = Mock(spec=TrackedOrder)
        self.executor.maker_order.order_id = "OID-BUY-1"
        self.executor.maker_order.order = InFlightOrder(
            creation_timestamp=1234,
            trading_pair="ETH-USDT",
            client_order_id="OID-BUY-1",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("100"),
            price=Decimal("99.5"),
            initial_state=OrderState.OPEN,
        )
        self.executor._maker_cancel_in_flight = True
        self.executor._maker_cancel_requested_timestamp = self.strategy.current_timestamp

        await self.executor.control_task()
        self.strategy.cancel.assert_not_called()

    async def test_control_task_shut_down_process(self):
        self.executor.maker_order = Mock(spec=TrackedOrder)
        self.executor.maker_order.is_done = True
        self.executor.taker_order = Mock(spec=TrackedOrder)
        self.executor.taker_order.is_done = True
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.TERMINATED)

    async def test_control_shutdown_process_requests_taker_cancel_before_replace(self):
        executor = XEMMExecutor(self.strategy, self.base_config_long_limit_taker, self.update_interval)
        executor._status = RunnableStatus.SHUTTING_DOWN
        executor.maker_order = Mock(spec=TrackedOrder)
        executor.maker_order.is_done = True
        executor.taker_order = TrackedOrder(order_id="OID-SELL-0")
        executor.taker_order.order = InFlightOrder(
            creation_timestamp=1000,
            trading_pair="ETH-USDT",
            client_order_id="OID-SELL-0",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            amount=Decimal("100"),
            price=Decimal("100"),
            initial_state=OrderState.OPEN,
        )
        self.strategy.current_timestamp = 1300
        executor.config.taker_order_max_age_seconds = 10

        await executor.control_task()

        self.assertTrue(executor._taker_cancel_in_flight)
        self.assertEqual(executor.taker_order.order_id, "OID-SELL-0")
        self.strategy.cancel.assert_called_once_with("kucoin", "ETH-USDT", "OID-SELL-0")

    def test_process_order_canceled_event_taker_replaces_order(self):
        executor = XEMMExecutor(self.strategy, self.base_config_long_limit_taker, self.update_interval)
        executor._status = RunnableStatus.SHUTTING_DOWN
        executor.taker_order = TrackedOrder(order_id="OID-SELL-0")
        executor._taker_cancel_in_flight = True

        cancel_event = OrderCancelledEvent(
            timestamp=1234,
            order_id="OID-SELL-0",
            exchange_order_id="12345",
        )
        executor.process_order_canceled_event(1, MagicMock(), cancel_event)

        self.assertFalse(executor._taker_cancel_in_flight)
        self.assertIsNotNone(executor.taker_order)
        self.assertEqual(executor.taker_order.order_id, "OID-SELL-1")

    def test_process_order_completed_event_taker_clears_cancel_flag(self):
        executor = XEMMExecutor(self.strategy, self.base_config_long_limit_taker, self.update_interval)
        executor._status = RunnableStatus.SHUTTING_DOWN
        executor.taker_order = TrackedOrder(order_id="OID-SELL-1")
        executor._taker_cancel_in_flight = True

        sell_order_completed_event = SellOrderCompletedEvent(
            base_asset="ETH",
            quote_asset="USDT",
            base_asset_amount=Decimal("100"),
            quote_asset_amount=Decimal("100"),
            order_type=OrderType.LIMIT,
            timestamp=1234,
            order_id="OID-SELL-1",
        )
        executor.process_order_completed_event(1, MagicMock(), sell_order_completed_event)

        self.assertFalse(executor._taker_cancel_in_flight)

    @patch.object(XEMMExecutor, "get_in_flight_order")
    def test_process_order_created_event(self, in_flight_order_mock):
        self.executor._status = RunnableStatus.RUNNING
        in_flight_order_mock.side_effect = [
            InFlightOrder(
                client_order_id="OID-BUY-1",
                creation_timestamp=1234,
                trading_pair="ETH-USDT",
                order_type=OrderType.LIMIT,
                trade_type=TradeType.BUY,
                amount=Decimal("100"),
                price=Decimal("100"),
            ),
            InFlightOrder(
                client_order_id="OID-SELL-1",
                creation_timestamp=1234,
                trading_pair="ETH-USDT",
                order_type=OrderType.MARKET,
                trade_type=TradeType.SELL,
                amount=Decimal("100"),
                price=Decimal("100"),
            )
        ]

        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        self.executor.taker_order = TrackedOrder(order_id="OID-SELL-1")
        buy_order_created_event = BuyOrderCreatedEvent(
            timestamp=1234,
            type=OrderType.LIMIT,
            creation_timestamp=1233,
            order_id="OID-BUY-1",
            trading_pair="ETH-USDT",
            amount=Decimal("100"),
            price=Decimal("100"),
        )
        sell_order_created_event = BuyOrderCreatedEvent(
            timestamp=1234,
            type=OrderType.MARKET,
            creation_timestamp=1233,
            order_id="OID-SELL-1",
            trading_pair="ETH-USDT",
            amount=Decimal("100"),
            price=Decimal("100"),
        )
        self.assertEqual(self.executor.maker_order.order, None)
        self.assertEqual(self.executor.taker_order.order, None)
        self.executor.process_order_created_event(1, MagicMock(), buy_order_created_event)
        self.assertEqual(self.executor.maker_order.order.client_order_id, "OID-BUY-1")
        self.executor.process_order_created_event(1, MagicMock(), sell_order_created_event)
        self.assertEqual(self.executor.taker_order.order.client_order_id, "OID-SELL-1")

    @patch.object(XEMMExecutor, "get_in_flight_order")
    def test_process_order_created_event_resets_stp_state_for_maker(self, in_flight_order_mock):
        strategy = self.create_shadow_mock_strategy(include_limit_maker=True)
        config = self.base_config_shadow
        config.bithumb_stp_prevention_enabled = True
        config.bithumb_stp_base_offset_ticks = 1
        executor = XEMMExecutor(strategy, config, self.update_interval)
        executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        executor._stp_reject_streak = 2
        executor._stp_dynamic_offset_ticks = 3
        executor._stp_cooldown_until_ts = strategy.current_timestamp + 10
        executor._stp_pause_until_ts = strategy.current_timestamp + 20
        executor._stp_last_error_message = "cross_trading"

        in_flight_order_mock.return_value = InFlightOrder(
            client_order_id="OID-BUY-1",
            creation_timestamp=1234,
            trading_pair="XRP-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("10"),
            price=Decimal("100"),
        )
        created_event = BuyOrderCreatedEvent(
            timestamp=1234,
            type=OrderType.LIMIT,
            creation_timestamp=1233,
            order_id="OID-BUY-1",
            trading_pair="XRP-KRW",
            amount=Decimal("10"),
            price=Decimal("100"),
        )
        executor.process_order_created_event(1, MagicMock(), created_event)

        self.assertEqual(0, executor._stp_reject_streak)
        self.assertEqual(1, executor._stp_dynamic_offset_ticks)
        self.assertEqual(0.0, executor._stp_cooldown_until_ts)
        self.assertEqual(0.0, executor._stp_pause_until_ts)
        self.assertEqual("", executor._stp_last_error_message)

    def test_process_order_completed_event(self):
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        self.executor._maker_cancel_in_flight = True
        self.assertEqual(self.executor.taker_order, None)
        buy_order_created_event = BuyOrderCompletedEvent(
            base_asset="ETH",
            quote_asset="USDT",
            base_asset_amount=Decimal("100"),
            quote_asset_amount=Decimal("100"),
            order_type=OrderType.LIMIT,
            timestamp=1234,
            order_id="OID-BUY-1",
        )
        self.executor.process_order_completed_event(1, MagicMock(), buy_order_created_event)
        self.assertEqual(self.executor.status, RunnableStatus.SHUTTING_DOWN)
        self.assertIsNone(self.executor.taker_order)
        self.assertFalse(self.executor._maker_cancel_in_flight)

    def test_process_order_canceled_event_clears_maker_state(self):
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        self.executor._maker_cancel_in_flight = True

        cancel_event = OrderCancelledEvent(
            timestamp=1234,
            order_id="OID-BUY-1",
            exchange_order_id="12345",
        )
        self.executor.process_order_canceled_event(1, MagicMock(), cancel_event)

        self.assertIsNone(self.executor.maker_order)
        self.assertFalse(self.executor._maker_cancel_in_flight)

    def test_process_order_failed_event(self):
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        maker_failure_event = MarketOrderFailureEvent(
            timestamp=1234,
            order_id="OID-BUY-1",
            order_type=OrderType.LIMIT,
        )
        self.executor.process_order_failed_event(1, MagicMock(), maker_failure_event)
        self.assertEqual(self.executor.maker_order, None)

        self.executor.taker_order = TrackedOrder(order_id="OID-SELL-0")
        taker_failure_event = MarketOrderFailureEvent(
            timestamp=1234,
            order_id="OID-SELL-0",
            order_type=OrderType.MARKET,
        )
        self.executor.process_order_failed_event(1, MagicMock(), taker_failure_event)
        self.assertEqual(self.executor.taker_order.order_id, "OID-SELL-1")

    def test_taker_failure_stops_when_fallback_disabled(self):
        config = self.base_config_long_limit_taker
        config.taker_max_retries = 0
        config.taker_fallback_to_market = False
        executor = XEMMExecutor(self.strategy, config, self.update_interval)
        executor.taker_order = TrackedOrder(order_id="OID-SELL-0")

        taker_failure_event = MarketOrderFailureEvent(
            timestamp=1234,
            order_id="OID-SELL-0",
            order_type=OrderType.LIMIT,
        )
        executor.process_order_failed_event(1, MagicMock(), taker_failure_event)

        self.assertEqual(executor.close_type, CloseType.FAILED)
        self.assertEqual(executor.status, RunnableStatus.TERMINATED)

    @patch.object(XEMMExecutor, "_get_taker_limit_price")
    def test_place_taker_order_uses_limit_when_configured(self, get_limit_price_mock):
        get_limit_price_mock.return_value = Decimal("99")
        executor = XEMMExecutor(self.strategy, self.base_config_long_limit_taker, self.update_interval)
        executor.place_taker_order()
        self.assertEqual(executor.taker_order.order_id, "OID-SELL-1")
        self.strategy.sell.assert_called_with("kucoin", "ETH-USDT", Decimal("100"), OrderType.LIMIT, Decimal("99"), ANY)

    def test_get_custom_info(self):
        custom_info = self.executor.get_custom_info()
        self.assertEqual(custom_info["maker_connector"], "binance")
        self.assertEqual(custom_info["maker_trading_pair"], "ETH-USDT")
        self.assertEqual(custom_info["taker_connector"], "kucoin")
        self.assertEqual(custom_info["taker_trading_pair"], "ETH-USDT")
        self.assertEqual(custom_info["side"], TradeType.BUY)
        self.assertEqual(custom_info["min_profitability"], Decimal("0.01"))
        self.assertEqual(custom_info["target_profitability_pct"], Decimal("0.015"))
        self.assertEqual(custom_info["max_profitability"], Decimal("0.02"))
        self.assertEqual(custom_info["tx_cost"], Decimal("1"))
        self.assertEqual(custom_info["tx_cost_pct"], Decimal("1"))
        self.assertEqual(custom_info["taker_order_type"], OrderType.MARKET.name)
        self.assertEqual(custom_info["taker_slippage_buffer_bps"], Decimal("0"))
        self.assertEqual(custom_info["taker_fallback_to_market"], False)

    def test_to_format_status(self):
        self.assertIn("Maker Side: TradeType.BUY", self.executor.to_format_status())

    def test_early_stop(self):
        self.executor._status = RunnableStatus.RUNNING
        self.executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        self.executor.maker_order.order = InFlightOrder(
            creation_timestamp=1234,
            trading_pair="ETH-USDT",
            client_order_id="OID-BUY-1",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("100"),
            price=Decimal("100"),
            initial_state=OrderState.OPEN,
        )
        self.executor.early_stop()
        self.assertEqual(self.executor._status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(self.executor.close_type, CloseType.EARLY_STOP)
        self.strategy.cancel.assert_called_once_with("binance", "ETH-USDT", "OID-BUY-1")

    def test_process_order_canceled_event_taker_does_not_replace_on_early_stop(self):
        executor = XEMMExecutor(self.strategy, self.base_config_long_limit_taker, self.update_interval)
        executor._status = RunnableStatus.SHUTTING_DOWN
        executor.close_type = CloseType.EARLY_STOP
        executor.taker_order = TrackedOrder(order_id="OID-SELL-0")
        executor._taker_cancel_in_flight = True
        self.strategy.sell.reset_mock()

        cancel_event = OrderCancelledEvent(
            timestamp=1234,
            order_id="OID-SELL-0",
            exchange_order_id="12345",
        )
        executor.process_order_canceled_event(1, MagicMock(), cancel_event)

        self.assertFalse(executor._taker_cancel_in_flight)
        self.assertIsNone(executor.taker_order)
        self.strategy.sell.assert_not_called()

    def test_process_order_completed_event_maker_skips_taker_on_early_stop(self):
        executor = XEMMExecutor(self.strategy, self.base_config_long_limit_taker, self.update_interval)
        executor._status = RunnableStatus.SHUTTING_DOWN
        executor.close_type = CloseType.EARLY_STOP
        executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        executor._maker_cancel_in_flight = True
        self.strategy.sell.reset_mock()

        buy_order_completed_event = BuyOrderCompletedEvent(
            base_asset="ETH",
            quote_asset="USDT",
            base_asset_amount=Decimal("100"),
            quote_asset_amount=Decimal("100"),
            order_type=OrderType.LIMIT,
            timestamp=1234,
            order_id="OID-BUY-1",
        )
        executor.process_order_completed_event(1, MagicMock(), buy_order_completed_event)

        self.assertFalse(executor._maker_cancel_in_flight)
        self.assertIsNone(executor.taker_order)
        self.strategy.sell.assert_not_called()

    def test_process_order_completed_event_maker_places_market_hedge_on_early_stop_when_configured(self):
        config = self.base_config_long_limit_taker
        config.stale_fill_hedge_mode = "market"
        executor = XEMMExecutor(self.strategy, config, self.update_interval)
        executor._status = RunnableStatus.SHUTTING_DOWN
        executor.close_type = CloseType.EARLY_STOP
        executor.maker_order = TrackedOrder(order_id="OID-BUY-1")
        executor._maker_cancel_in_flight = True
        self.strategy.sell.reset_mock()

        buy_order_completed_event = BuyOrderCompletedEvent(
            base_asset="ETH",
            quote_asset="USDT",
            base_asset_amount=Decimal("2"),
            quote_asset_amount=Decimal("200"),
            order_type=OrderType.LIMIT,
            timestamp=1234,
            order_id="OID-BUY-1",
        )
        executor.process_order_completed_event(1, MagicMock(), buy_order_completed_event)

        self.assertFalse(executor._maker_cancel_in_flight)
        self.assertIsNotNone(executor.taker_order)
        self.assertEqual(OrderType.MARKET, executor._effective_taker_order_type())
        self.assertTrue(executor._early_stop_hedge_initiated)

    async def test_control_early_stop_shutdown_does_not_cancel_emergency_hedge_taker(self):
        config = self.base_config_long_limit_taker
        config.stale_fill_hedge_mode = "market"
        executor = XEMMExecutor(self.strategy, config, self.update_interval)
        executor._status = RunnableStatus.SHUTTING_DOWN
        executor.close_type = CloseType.EARLY_STOP
        executor._early_stop_hedge_initiated = True
        executor.maker_order = None
        executor.taker_order = TrackedOrder(order_id="OID-SELL-1")
        executor.taker_order.order = InFlightOrder(
            creation_timestamp=1234,
            trading_pair="ETH-USDT",
            client_order_id="OID-SELL-1",
            order_type=OrderType.MARKET,
            trade_type=TradeType.SELL,
            amount=Decimal("2"),
            price=Decimal("100"),
            initial_state=OrderState.OPEN,
        )

        await executor.control_early_stop_shutdown()

        self.strategy.cancel.assert_not_called()
        self.assertEqual(RunnableStatus.SHUTTING_DOWN, executor.status)

    def test_get_cum_fees_quote_not_executed(self):
        self.assertEqual(self.executor.get_cum_fees_quote(), Decimal('0'))

    @patch.object(XEMMExecutor, 'rate_oracle', create=True)
    async def test_get_quote_asset_conversion_rate_none(self, mock_rate_oracle):
        mock_rate_oracle.get_pair_rate.return_value = None
        self.executor.quote_conversion_pair = "USDC-USDT"
        with self.assertRaises(ValueError):
            await self.executor.get_quote_asset_conversion_rate()

    @patch.object(XEMMExecutor, 'rate_oracle', create=True)
    async def test_get_quote_asset_conversion_rate_exception(self, mock_rate_oracle):
        mock_rate_oracle.get_pair_rate.side_effect = Exception("Test exception")
        self.executor.quote_conversion_pair = "USDC-USDT"
        with self.assertRaises(Exception):
            await self.executor.get_quote_asset_conversion_rate()
