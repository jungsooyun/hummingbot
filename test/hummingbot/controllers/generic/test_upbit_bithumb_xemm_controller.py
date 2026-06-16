import asyncio
import tempfile
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import yaml

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from controllers.generic.upbit_bithumb_xemm_controller import (
    UpbitBithumbXemmController,
    UpbitBithumbXemmControllerConfig,
)
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


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
        self.freshness_by_connector = {"bithumb": 0.1, "upbit": 0.1}
        self.mock_market_data_provider.get_order_book_freshness_sec.side_effect = (
            lambda connector_name, trading_pair: self.freshness_by_connector[connector_name]
        )
        self.mock_market_data_provider.quantize_order_amount.side_effect = lambda c, p, a: a

        bithumb_connector = MagicMock()
        upbit_connector = MagicMock()
        bithumb_connector.get_available_balance.return_value = Decimal("1")
        upbit_connector.get_available_balance.return_value = Decimal("1")
        bithumb_connector.get_order_price_quantum.return_value = Decimal("1")
        upbit_connector.get_order_price_quantum.return_value = Decimal("1")
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
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()
        super().tearDown()

    @staticmethod
    def _top_level_root() -> Path:
        return Path(__file__).resolve().parents[5]

    def _make_xrp_v3_config(self) -> UpbitBithumbXemmControllerConfig:
        config_path = self._top_level_root() / "deploy/hummingbot-conf/controllers/upbit_bithumb_xemm_xrp_live_v3.yml"
        with open(config_path, "r", encoding="utf-8") as stream:
            raw = yaml.safe_load(stream)
        return UpbitBithumbXemmControllerConfig(**raw)

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
        self.assertTrue(
            all(
                action.executor_config.allow_one_sided_inventory_mode == self.controller.config.allow_one_sided_inventory_mode
                for action in actions
            )
        )

        # 먼 호가(weight=2)가 가까운 호가(weight=1)보다 더 큰 주문량인지 확인
        buy_actions_by_target = sorted(buy_actions, key=lambda action: action.executor_config.target_profitability)
        self.assertLess(
            buy_actions_by_target[0].executor_config.order_amount,
            buy_actions_by_target[1].executor_config.order_amount,
        )

    def test_latency_diagnostics_flag_propagates_to_executor_config(self):
        self.controller.config.latency_diagnostics_enabled = True
        self.controller.executors_info = []

        actions = self.controller.determine_executor_actions()

        self.assertTrue(all(action.executor_config.latency_diagnostics_enabled is True for action in actions))

    def test_inventory_skew_blocks_buy_side(self):
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("5")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        self.controller.config.max_inventory_skew_base = Decimal("0.5")

        actions = self.controller.determine_executor_actions()
        self.assertGreater(len(actions), 0)
        self.assertTrue(all(action.executor_config.maker_side == TradeType.SELL for action in actions))

    def test_inventory_skew_side_gating_can_be_disabled(self):
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("5")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        self.controller.config.max_inventory_skew_base = Decimal("0.5")
        self.controller.config.inventory_skew_side_gating_enabled = False

        actions = self.controller.determine_executor_actions()

        self.assertGreater(len(actions), 0)
        self.assertTrue(any(action.executor_config.maker_side == TradeType.BUY for action in actions))
        self.assertTrue(any(action.executor_config.maker_side == TradeType.SELL for action in actions))

    def test_inventory_rebalance_creates_best_positive_candidate_and_prefers_rebated_passive_path(self):
        self.controller.config.inventory_rebalance_enabled = True
        self.controller.config.inventory_rebalance_hard_band_base = Decimal("6")
        self.controller.config.inventory_rebalance_soft_band_base = Decimal("2")
        self.controller.config.inventory_rebalance_min_slice_base = Decimal("1")
        self.controller.config.inventory_rebalance_max_slice_base = Decimal("10")
        self.controller.config.inventory_rebalance_min_expected_pnl_quote = Decimal("0")
        self.controller.config.inventory_rebalance_bithumb_rebate_bps = Decimal("2.5")
        self.controller.config.max_executors_per_side = 1
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("12")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("0")
        self.controller.executors_info = [
            SimpleNamespace(id="buy-xemm", is_done=False, side=TradeType.BUY, config=SimpleNamespace(target_profitability=Decimal("0.001"), type="xemm_executor")),
            SimpleNamespace(id="sell-xemm", is_done=False, side=TradeType.SELL, config=SimpleNamespace(target_profitability=Decimal("0.001"), type="xemm_executor")),
        ]

        prices = {
            ("bithumb", PriceType.BestBid): Decimal("100"),
            ("bithumb", PriceType.BestAsk): Decimal("101"),
            ("upbit", PriceType.BestBid): Decimal("99"),
            ("upbit", PriceType.BestAsk): Decimal("100"),
            ("bithumb", PriceType.MidPrice): Decimal("100.5"),
            ("upbit", PriceType.MidPrice): Decimal("99.5"),
        }
        self.mock_market_data_provider.get_price_by_type.side_effect = (
            lambda connector, trading_pair, price_type: prices[(connector, price_type)]
        )

        actions = self.controller.determine_executor_actions()

        self.assertEqual(1, len(actions))
        rebalance_action = actions[0]
        self.assertEqual("inventory_rebalance_executor", rebalance_action.executor_config.type)
        self.assertEqual("bithumb", rebalance_action.executor_config.entry_market.connector_name)
        self.assertEqual("upbit", rebalance_action.executor_config.hedge_market.connector_name)
        self.assertEqual(TradeType.SELL, rebalance_action.executor_config.entry_side)
        self.assertEqual("passive", rebalance_action.executor_config.entry_style)
        self.assertGreater(rebalance_action.executor_config.expected_pnl_quote, Decimal("0"))
        self.assertEqual(Decimal("5"), rebalance_action.executor_config.order_amount)

    def test_inventory_rebalance_hard_band_allows_aggressive_candidate(self):
        self.controller.config.inventory_rebalance_enabled = True
        self.controller.config.inventory_rebalance_hard_band_base = Decimal("6")
        self.controller.config.inventory_rebalance_soft_band_base = Decimal("2")
        self.controller.config.inventory_rebalance_min_slice_base = Decimal("1")
        self.controller.config.inventory_rebalance_max_slice_base = Decimal("10")
        self.controller.config.inventory_rebalance_min_expected_pnl_quote = Decimal("0")
        self.controller.config.inventory_rebalance_entry_slippage_buffer_bps = Decimal("0")
        self.controller.config.inventory_rebalance_hedge_slippage_buffer_bps = Decimal("0")
        self.controller.config.max_executors_per_side = 1
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("12")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("0")
        self.controller.executors_info = [
            SimpleNamespace(id="buy-xemm", is_done=False, side=TradeType.BUY, config=SimpleNamespace(target_profitability=Decimal("0.001"), type="xemm_executor")),
            SimpleNamespace(id="sell-xemm", is_done=False, side=TradeType.SELL, config=SimpleNamespace(target_profitability=Decimal("0.001"), type="xemm_executor")),
        ]

        prices = {
            ("bithumb", PriceType.BestBid): Decimal("100"),
            ("upbit", PriceType.BestAsk): Decimal("99"),
            ("bithumb", PriceType.MidPrice): Decimal("100.5"),
            ("upbit", PriceType.MidPrice): Decimal("99"),
        }
        self.mock_market_data_provider.get_price_by_type.side_effect = (
            lambda connector, trading_pair, price_type: prices.get((connector, price_type))
        )

        actions = self.controller.determine_executor_actions()

        self.assertEqual(1, len(actions))
        rebalance_action = actions[0]
        self.assertEqual("inventory_rebalance_executor", rebalance_action.executor_config.type)
        self.assertEqual("aggressive", rebalance_action.executor_config.entry_style)
        self.assertIn(rebalance_action.executor_config.entry_market.connector_name, {"bithumb", "upbit"})
        self.assertIn(rebalance_action.executor_config.entry_side, {TradeType.BUY, TradeType.SELL})

    def test_active_inventory_rebalance_suppresses_only_worsening_xemm_side(self):
        self.controller.config.inventory_rebalance_enabled = True
        self.controller.config.inventory_rebalance_hard_band_base = Decimal("6")
        self.controller.config.inventory_rebalance_soft_band_base = Decimal("2")
        self.controller.config.max_executors_per_side = 2
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("12")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("0")
        self.controller.executors_info = [
            SimpleNamespace(
                id="rebalance-1",
                is_done=False,
                side=TradeType.SELL,
                type="inventory_rebalance_executor",
                custom_info={"inventory_rebalance_reduces_delta": True},
                config=SimpleNamespace(type="inventory_rebalance_executor"),
            )
        ]

        actions = self.controller.determine_executor_actions()

        create_actions = [action for action in actions if isinstance(action, CreateExecutorAction)]
        self.assertGreater(len(create_actions), 0)
        self.assertTrue(all(action.executor_config.type != "inventory_rebalance_executor" for action in create_actions))
        self.assertTrue(all(action.executor_config.maker_side == TradeType.SELL for action in create_actions))

    def test_quote_mismatch_raises_validation_error(self):
        with self.assertRaises(ValueError):
            UpbitBithumbXemmControllerConfig(
                id="invalid",
                maker_connector="bithumb",
                maker_trading_pair="BTC-KRW",
                taker_connector="upbit",
                taker_trading_pair="BTC-USDT",
            )

    def test_shadow_enabled_requires_bithumb_maker_upbit_taker_path(self):
        with self.assertRaises(ValueError):
            UpbitBithumbXemmControllerConfig(
                id="invalid-shadow-path",
                maker_connector="upbit",
                maker_trading_pair="BTC-KRW",
                taker_connector="bithumb",
                taker_trading_pair="BTC-KRW",
                upbit_shadow_maker_enabled=True,
            )

    def test_shadow_config_is_propagated_and_global_cap_applied(self):
        self.controller.config.upbit_shadow_maker_enabled = True
        self.controller.config.upbit_shadow_global_notional_cap_quote = Decimal("200")
        self.controller.executors_info = []

        actions = self.controller.determine_executor_actions()
        self.assertGreater(len(actions), 0)
        enabled_flags = [action.executor_config.shadow_maker_enabled for action in actions]
        self.assertIn(True, enabled_flags)
        self.assertIn(False, enabled_flags)
        for action in actions:
            self.assertEqual(
                action.executor_config.shadow_maker_price_refresh_pct,
                self.controller.config.upbit_shadow_price_refresh_pct,
            )
            self.assertEqual(
                action.executor_config.shadow_prefill_max_retries,
                self.controller.config.upbit_shadow_prefill_max_retries,
            )

    def test_bithumb_stp_config_is_propagated(self):
        self.controller.config.bithumb_stp_prevention_enabled = True
        self.controller.config.bithumb_stp_base_offset_ticks = 2
        self.controller.config.bithumb_stp_max_offset_ticks = 5
        self.controller.config.bithumb_stp_retry_cooldown_sec = 1.5
        self.controller.config.bithumb_stp_pause_after_rejects = 4
        self.controller.config.bithumb_stp_pause_duration_sec = 45.0
        self.controller.config.bithumb_stp_consider_pending_cancel_as_conflict = False
        self.controller.executors_info = []

        actions = self.controller.determine_executor_actions()
        self.assertGreater(len(actions), 0)
        for action in actions:
            cfg = action.executor_config
            self.assertEqual(cfg.bithumb_stp_prevention_enabled, True)
            self.assertEqual(cfg.bithumb_stp_base_offset_ticks, 2)
            self.assertEqual(cfg.bithumb_stp_max_offset_ticks, 5)
            self.assertEqual(cfg.bithumb_stp_retry_cooldown_sec, 1.5)
            self.assertEqual(cfg.bithumb_stp_pause_after_rejects, 4)
            self.assertEqual(cfg.bithumb_stp_pause_duration_sec, 45.0)
            self.assertEqual(cfg.bithumb_stp_consider_pending_cancel_as_conflict, False)

    def test_bithumb_stp_config_validation(self):
        with self.assertRaises(ValueError):
            UpbitBithumbXemmControllerConfig(
                id="invalid-stp-ticks",
                maker_connector="bithumb",
                maker_trading_pair="BTC-KRW",
                taker_connector="upbit",
                taker_trading_pair="BTC-KRW",
                bithumb_stp_base_offset_ticks=2,
                bithumb_stp_max_offset_ticks=1,
            )

    def test_stale_market_data_triggers_fail_closed_stop_actions_once(self):
        self.controller.config.market_data_stale_timeout_sec = 2.0
        self.freshness_by_connector["bithumb"] = 5.0
        self.freshness_by_connector["upbit"] = 5.0
        self.controller.executors_info = [
            SimpleNamespace(
                id="exec-1",
                is_done=False,
                side=TradeType.BUY,
                config=SimpleNamespace(target_profitability=Decimal("0.001")),
            )
        ]

        first_actions = self.controller.determine_executor_actions()
        self.assertEqual(1, len(first_actions))
        self.assertIsInstance(first_actions[0], StopExecutorAction)
        self.assertEqual("exec-1", first_actions[0].executor_id)

        second_actions = self.controller.determine_executor_actions()
        self.assertEqual(0, len(second_actions))

    def test_recovery_grace_blocks_new_creations_until_elapsed(self):
        self.controller.config.market_data_stale_timeout_sec = 2.0
        self.controller.config.market_data_recovery_grace_sec = 2.0
        self.controller.config.cancel_open_orders_on_stale = True

        self.freshness_by_connector["bithumb"] = 5.0
        self.freshness_by_connector["upbit"] = 5.0
        self.controller.executors_info = []
        self.mock_market_data_provider.time.return_value = 100.0
        self.controller.determine_executor_actions()  # mark stale

        self.freshness_by_connector["bithumb"] = 0.1
        self.freshness_by_connector["upbit"] = 0.1
        self.mock_market_data_provider.time.return_value = 101.0
        actions_during_grace = self.controller.determine_executor_actions()
        self.assertEqual(0, len(actions_during_grace))

        self.mock_market_data_provider.time.return_value = 103.5
        actions_after_grace = self.controller.determine_executor_actions()
        self.assertGreater(len(actions_after_grace), 0)

    def test_opportunity_gate_requires_single_level(self):
        with self.assertRaises(ValueError):
            UpbitBithumbXemmControllerConfig(
                id="invalid-opportunity-levels",
                maker_connector="bithumb",
                maker_trading_pair="IP-KRW",
                taker_connector="upbit",
                taker_trading_pair="IP-KRW",
                buy_levels_targets_amount="0.001,1-0.002,1",
                sell_levels_targets_amount="0.001,1",
                max_executors_per_side=1,
                opportunity_gate_enabled=True,
            )

    def test_opportunity_gate_requires_single_executor(self):
        with self.assertRaises(ValueError):
            UpbitBithumbXemmControllerConfig(
                id="invalid-opportunity-executors",
                maker_connector="bithumb",
                maker_trading_pair="IP-KRW",
                taker_connector="upbit",
                taker_trading_pair="IP-KRW",
                buy_levels_targets_amount="0.001,1",
                sell_levels_targets_amount="0.001,1",
                max_executors_per_side=2,
                opportunity_gate_enabled=True,
            )

    def test_close_out_loss_cap_must_be_non_negative(self):
        with self.assertRaises(ValueError):
            UpbitBithumbXemmControllerConfig(
                id="invalid-close-out-loss-cap",
                maker_connector="bithumb",
                maker_trading_pair="IP-KRW",
                taker_connector="upbit",
                taker_trading_pair="IP-KRW",
                buy_levels_targets_amount="0.001,1",
                sell_levels_targets_amount="0.001,1",
                close_out_loss_cap_bps=Decimal("-1"),
            )

    def test_opportunity_gate_fail_stops_side_and_blocks_create(self):
        self.controller.config.buy_levels_targets_amount = [[Decimal("0.001"), Decimal("1")]]
        self.controller.config.sell_levels_targets_amount = [[Decimal("0.001"), Decimal("1")]]
        self.controller.config.max_executors_per_side = 1
        self.controller.config.opportunity_gate_enabled = True
        self.controller.config.opportunity_gate_stop_on_fail = True
        self.controller.config.maker_connector = "bithumb"
        self.controller.config.taker_connector = "upbit"

        def price_side_effect(connector_name, trading_pair, requested_price_type):
            if requested_price_type == PriceType.BestAsk:
                return Decimal("1202") if connector_name == "bithumb" else Decimal("1200")
            if requested_price_type == PriceType.BestBid:
                return Decimal("1200")
            if requested_price_type == PriceType.MidPrice:
                return Decimal("1201")
            return Decimal("1201")

        self.mock_market_data_provider.get_price_by_type.side_effect = price_side_effect
        active_buy_executor = SimpleNamespace(
            id="exec-buy-1",
            side=TradeType.BUY,
            is_done=False,
            config=SimpleNamespace(target_profitability=Decimal("0.001")),
            custom_info={},
            net_pnl_quote=Decimal("0"),
        )
        self.controller.executors_info = [active_buy_executor]

        actions = self.controller.determine_executor_actions()
        stop_actions = [action for action in actions if isinstance(action, StopExecutorAction)]
        create_actions = [action for action in actions if isinstance(action, CreateExecutorAction)]

        self.assertTrue(any(action.executor_id == "exec-buy-1" for action in stop_actions))
        self.assertTrue(all(action.executor_config.maker_side != TradeType.BUY for action in create_actions))

    def test_opportunity_gate_fail_blocks_create_without_stopping_when_stop_disabled(self):
        self.controller.config.buy_levels_targets_amount = [[Decimal("0.001"), Decimal("1")]]
        self.controller.config.sell_levels_targets_amount = [[Decimal("0.001"), Decimal("1")]]
        self.controller.config.max_executors_per_side = 1
        self.controller.config.opportunity_gate_enabled = True
        self.controller.config.opportunity_gate_stop_on_fail = False
        self.controller.config.maker_connector = "bithumb"
        self.controller.config.taker_connector = "upbit"

        def price_side_effect(connector_name, trading_pair, requested_price_type):
            if requested_price_type == PriceType.BestAsk:
                return Decimal("1202") if connector_name == "bithumb" else Decimal("1200")
            if requested_price_type == PriceType.BestBid:
                return Decimal("1200")
            if requested_price_type == PriceType.MidPrice:
                return Decimal("1201")
            return Decimal("1201")

        self.mock_market_data_provider.get_price_by_type.side_effect = price_side_effect
        active_buy_executor = SimpleNamespace(
            id="exec-buy-1",
            side=TradeType.BUY,
            is_done=False,
            config=SimpleNamespace(target_profitability=Decimal("0.001")),
            custom_info={},
            net_pnl_quote=Decimal("0"),
        )
        self.controller.executors_info = [active_buy_executor]

        actions = self.controller.determine_executor_actions()
        stop_actions = [action for action in actions if isinstance(action, StopExecutorAction)]
        create_actions = [action for action in actions if isinstance(action, CreateExecutorAction)]

        self.assertEqual([], stop_actions)
        self.assertTrue(all(action.executor_config.maker_side != TradeType.BUY for action in create_actions))

    def test_maker_price_source_propagated_to_executor_config(self):
        self.controller.config.buy_levels_targets_amount = [[Decimal("0.001"), Decimal("1")]]
        self.controller.config.sell_levels_targets_amount = [[Decimal("0.001"), Decimal("1")]]
        self.controller.config.max_executors_per_side = 1
        self.controller.config.maker_price_source = "best"
        self.controller.config.close_out_loss_cap_bps = Decimal("15")
        self.controller.executors_info = []

        actions = self.controller.determine_executor_actions()
        create_actions = [action for action in actions if isinstance(action, CreateExecutorAction)]
        self.assertGreater(len(create_actions), 0)
        self.assertTrue(all(action.executor_config.maker_price_source == "best" for action in create_actions))
        self.assertTrue(all(action.executor_config.close_out_loss_cap_bps == Decimal("15") for action in create_actions))

    def test_xrp_v3_config_loads_successfully(self):
        config = self._make_xrp_v3_config()

        self.assertEqual("upbit_bithumb_xemm_xrp_live_v3", config.id)
        self.assertEqual("best", config.maker_price_source)
        self.assertEqual(Decimal("0.0008"), config.maker_price_refresh_pct)
        self.assertTrue(config.latency_diagnostics_enabled)
        self.assertEqual("market", config.stale_fill_hedge_mode)
        self.assertEqual(Decimal("15"), config.close_out_loss_cap_bps)
        self.assertEqual(Decimal("0.0007"), config.min_profitability_guard)
        self.assertFalse(config.inventory_skew_side_gating_enabled)
        self.assertEqual(3, config.opportunity_min_ticks)
        self.assertEqual(3, config.opportunity_max_ticks)
        self.assertEqual([[Decimal("0.0010"), Decimal("1")]], config.buy_levels_targets_amount)
        self.assertEqual([[Decimal("0.0010"), Decimal("1")]], config.sell_levels_targets_amount)
        self.assertEqual(Decimal("7.5"), config.opportunity_fee_buffer_bps_buy)
        self.assertEqual(Decimal("7.5"), config.opportunity_fee_buffer_bps_sell)
        self.assertFalse(config.opportunity_gate_stop_on_fail)

    def test_xrp_v3_fixed_three_ticks_passes_at_three_tick_edge(self):
        config = self._make_xrp_v3_config()
        controller = UpbitBithumbXemmController(
            config=config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

        def price_side_effect(connector_name, trading_pair, requested_price_type):
            if requested_price_type == PriceType.BestAsk:
                return Decimal("1203") if connector_name == "bithumb" else Decimal("1200")
            if requested_price_type == PriceType.BestBid:
                return Decimal("1200") if connector_name == "bithumb" else Decimal("1203")
            if requested_price_type == PriceType.MidPrice:
                return Decimal("1201.5")
            return Decimal("1201.5")

        self.mock_market_data_provider.get_price_by_type.side_effect = price_side_effect
        controller.executors_info = []

        actions = controller.determine_executor_actions()
        create_actions = [action for action in actions if isinstance(action, CreateExecutorAction)]

        self.assertEqual(2, len(create_actions))
        self.assertEqual({TradeType.BUY, TradeType.SELL}, {action.executor_config.maker_side for action in create_actions})

    def test_xrp_v3_blocks_sub_three_tick_edge(self):
        config = self._make_xrp_v3_config()
        controller = UpbitBithumbXemmController(
            config=config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

        def price_side_effect(connector_name, trading_pair, requested_price_type):
            if requested_price_type == PriceType.BestAsk:
                return Decimal("1202") if connector_name == "bithumb" else Decimal("1200")
            if requested_price_type == PriceType.BestBid:
                return Decimal("1200") if connector_name == "bithumb" else Decimal("1202")
            if requested_price_type == PriceType.MidPrice:
                return Decimal("1201")
            return Decimal("1201")

        self.mock_market_data_provider.get_price_by_type.side_effect = price_side_effect
        controller.executors_info = []

        actions = controller.determine_executor_actions()
        create_actions = [action for action in actions if isinstance(action, CreateExecutorAction)]

        self.assertEqual(0, len(create_actions))
