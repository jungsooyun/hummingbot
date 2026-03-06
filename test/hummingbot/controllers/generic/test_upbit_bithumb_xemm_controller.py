import asyncio
import json
import os
import tempfile
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from controllers.generic.transfer_global_lease import TransferGlobalLease
from controllers.generic.transfer_guard_client import ConflictError
from controllers.generic.transfer_rebalance_state import RebalanceSnapshot, RebalanceState, TransferRebalanceStateStore
from controllers.generic.upbit_bithumb_xemm_controller import (
    UpbitBithumbXemmController,
    UpbitBithumbXemmControllerConfig,
)
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class _FakeTransferClient:
    def __init__(self, signal_state="PENDING_APPROVAL", approval_state="READY_FOR_EXECUTION", request_state="WITHDRAWAL_SUBMITTED"):
        self.signal_state = signal_state
        self.approval_state = approval_state
        self.request_state = request_state
        self.sent_signals = []
        self.approvals = []
        self.requests = []

    async def send_signal(self, route_key, amount, signal_type, event_id, metadata, callback_url=None):
        self.sent_signals.append(
            {
                "route_key": route_key,
                "amount": amount,
                "signal_type": signal_type,
                "event_id": event_id,
                "metadata": metadata,
                "callback_url": callback_url,
            }
        )
        return SimpleNamespace(
            request_id="req-1",
            state=self.signal_state,
            deduplicated=False,
            reason=None,
        )

    async def approve_request(self, request_id, approver_id):
        self.approvals.append({"request_id": request_id, "approver_id": approver_id})
        return SimpleNamespace(request_id=request_id, state=self.approval_state)

    async def get_request(self, request_id):
        self.requests.append(request_id)
        return SimpleNamespace(
            request_id=request_id,
            state=self.request_state,
            asset="XRP",
            amount="10",
            from_exchange="upbit",
            to_exchange="bithumb",
            created_at=None,
            updated_at=None,
        )

    async def close(self):
        return None


class _ConflictApprovalClient(_FakeTransferClient):
    """Fake client that raises ConflictError on approve_request, but returns a valid state on get_request."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def approve_request(self, request_id, approver_id):
        self.approvals.append({"request_id": request_id, "approver_id": approver_id})
        raise ConflictError("conflict", status=409)


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

    def test_transfer_config_trigger_must_exceed_recover(self):
        with self.assertRaises(ValueError):
            UpbitBithumbXemmControllerConfig(
                id="invalid-transfer-config",
                maker_connector="bithumb",
                maker_trading_pair="XRP-KRW",
                taker_connector="upbit",
                taker_trading_pair="XRP-KRW",
                transfer_rebalance_enabled=True,
                transfer_route_key_maker_to_taker="upbit-bithumb-xrp",
                transfer_trigger_skew_base=Decimal("10"),
                transfer_recover_skew_base=Decimal("10"),
            )

    async def test_transfer_trigger_maker_excess_sends_signal_and_pauses_buy(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-xrp"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_rebalance_enabled = True
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("10")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        fake_client = _FakeTransferClient(signal_state="PENDING_APPROVAL", approval_state="READY_FOR_EXECUTION")
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True

        await self.controller.update_processed_data()
        await asyncio.sleep(0)
        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.IN_FLIGHT, self.controller._rebalance_state)
        self.assertEqual(TradeType.BUY, self.controller._paused_side)
        self.assertEqual(1, len(fake_client.sent_signals))
        self.assertEqual("upbit-bithumb-xrp", fake_client.sent_signals[0]["route_key"])
        self.assertEqual(1, len(fake_client.approvals))

    async def test_transfer_poll_success_moves_to_waiting_recovery_then_idle(self):
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-xrp"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_poll_interval_sec = 0
        self.controller.config.transfer_rebalance_enabled = True
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller._transfer_client = _FakeTransferClient(request_state="COMPLETED")
        self.controller._transfer_client_ready = True
        self.controller._rebalance_state = RebalanceState.IN_FLIGHT
        self.controller._active_transfer_request_id = "req-1"
        self.controller._transfer_started_ts = 0
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("4")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        self.mock_market_data_provider.time.return_value = 100.0

        await self.controller.update_processed_data()
        await asyncio.sleep(0)
        await self.controller.update_processed_data()
        self.assertEqual(RebalanceState.WAITING_RECOVERY, self.controller._rebalance_state)

        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("1")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        await self.controller.update_processed_data()
        self.assertEqual(RebalanceState.IDLE, self.controller._rebalance_state)
        self.assertIsNone(self.controller._paused_side)

    async def test_transfer_poll_failure_transitions_to_cooldown(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-xrp"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_poll_interval_sec = 0
        self.controller.config.transfer_rebalance_enabled = True
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller._transfer_client = _FakeTransferClient(request_state="WITHDRAWAL_FAILED")
        self.controller._transfer_client_ready = True
        self.controller._rebalance_state = RebalanceState.IN_FLIGHT
        self.controller._active_transfer_request_id = "req-1"
        self.controller._transfer_started_ts = 0
        self.mock_market_data_provider.time.return_value = 100.0

        await self.controller.update_processed_data()
        await asyncio.sleep(0)
        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.COOLDOWN, self.controller._rebalance_state)
        self.assertIsNotNone(self.controller._transfer_retry_at)

    def test_transfer_paused_side_stops_existing_executor_and_blocks_new_side(self):
        self.controller._paused_side = TradeType.BUY
        self.controller.executors_info = [
            SimpleNamespace(
                id="buy-exec-1",
                is_done=False,
                side=TradeType.BUY,
                config=SimpleNamespace(target_profitability=Decimal("0.001")),
            )
        ]
        actions = self.controller.determine_executor_actions()
        stop_actions = [a for a in actions if isinstance(a, StopExecutorAction)]
        create_actions = [a for a in actions if isinstance(a, CreateExecutorAction)]

        self.assertEqual(1, len(stop_actions))
        self.assertEqual("buy-exec-1", stop_actions[0].executor_id)
        self.assertTrue(all(a.executor_config.maker_side != TradeType.BUY for a in create_actions))

    # ── D1. Trigger & Direction ──────────────────────────────────────────

    async def test_no_trigger_when_disabled(self):
        # transfer_rebalance_enabled is False by default
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("10")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")

        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.IDLE, self.controller._rebalance_state)

    async def test_no_trigger_below_threshold(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("20")
        self.controller.config.transfer_recover_skew_base = Decimal("5")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-btc"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("10")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        fake_client = _FakeTransferClient()
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True

        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.IDLE, self.controller._rebalance_state)

    async def test_trigger_taker_excess_pauses_sell(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_taker_to_maker = "bithumb-upbit-btc"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        # maker=1, taker=10 → delta = 1-10 = -9, taker excess
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("1")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("10")
        fake_client = _FakeTransferClient(signal_state="PENDING_APPROVAL", approval_state="READY_FOR_EXECUTION")
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True

        await self.controller.update_processed_data()
        await asyncio.sleep(0)
        await self.controller.update_processed_data()

        self.assertEqual(TradeType.SELL, self.controller._paused_side)
        self.assertEqual("taker_to_maker", self.controller._transfer_direction)

    async def test_no_trigger_when_route_key_empty(self):
        # Only set taker_to_maker route key, but delta>0 (maker excess) → direction=maker_to_taker → route_key=""
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_taker_to_maker = "bithumb-upbit-btc"
        self.controller.config.transfer_route_key_maker_to_taker = ""
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        # maker=10, taker=1 → delta = 9 > 0 (maker excess) → maker_to_taker → route_key=""
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("10")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        fake_client = _FakeTransferClient()
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True

        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.IDLE, self.controller._rebalance_state)

    # ── D2. Transfer Amount Computation ──────────────────────────────────

    def test_compute_amount_basic_formula(self):
        self.controller.config.transfer_recover_skew_base = Decimal("2")
        self.controller.config.transfer_target_buffer_base = Decimal("1")
        self.controller.config.transfer_min_amount_base = Decimal("0")
        self.controller.config.transfer_max_amount_base = Decimal("0")
        self.controller.config.transfer_source_balance_reserve_base = Decimal("0")
        self.controller.config.transfer_amount_quantum_base = Decimal("0")
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("100")

        amount = self.controller._compute_transfer_amount(Decimal("10"), "maker_to_taker")
        # (10-2)/2 + 1 = 5
        self.assertEqual(Decimal("5"), amount)

    def test_compute_amount_clamped_to_max(self):
        self.controller.config.transfer_recover_skew_base = Decimal("2")
        self.controller.config.transfer_target_buffer_base = Decimal("1")
        self.controller.config.transfer_min_amount_base = Decimal("0")
        self.controller.config.transfer_max_amount_base = Decimal("3")
        self.controller.config.transfer_source_balance_reserve_base = Decimal("0")
        self.controller.config.transfer_amount_quantum_base = Decimal("0")
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("100")

        amount = self.controller._compute_transfer_amount(Decimal("10"), "maker_to_taker")
        # formula=5, max=3 → 3
        self.assertEqual(Decimal("3"), amount)

    def test_compute_amount_clamped_to_min(self):
        self.controller.config.transfer_recover_skew_base = Decimal("2")
        self.controller.config.transfer_target_buffer_base = Decimal("0")
        self.controller.config.transfer_min_amount_base = Decimal("1")
        self.controller.config.transfer_max_amount_base = Decimal("0")
        self.controller.config.transfer_source_balance_reserve_base = Decimal("0")
        self.controller.config.transfer_amount_quantum_base = Decimal("0")
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("100")

        # delta=3 → need=(3-2)/2=0.5, buffer=0 → 0.5. min=1 → max(0.5,1)=1
        amount = self.controller._compute_transfer_amount(Decimal("3"), "maker_to_taker")
        self.assertEqual(Decimal("1"), amount)

    def test_compute_amount_source_reserve(self):
        self.controller.config.transfer_recover_skew_base = Decimal("2")
        self.controller.config.transfer_target_buffer_base = Decimal("1")
        self.controller.config.transfer_min_amount_base = Decimal("0")
        self.controller.config.transfer_max_amount_base = Decimal("0")
        self.controller.config.transfer_source_balance_reserve_base = Decimal("95")
        self.controller.config.transfer_amount_quantum_base = Decimal("0")
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("100")

        # formula=(10-2)/2+1=5, available=100-95=5 → min(5,5)=5
        amount = self.controller._compute_transfer_amount(Decimal("10"), "maker_to_taker")
        self.assertEqual(Decimal("5"), amount)

    def test_compute_amount_zero_depleted(self):
        self.controller.config.transfer_recover_skew_base = Decimal("2")
        self.controller.config.transfer_target_buffer_base = Decimal("1")
        self.controller.config.transfer_min_amount_base = Decimal("0")
        self.controller.config.transfer_max_amount_base = Decimal("0")
        self.controller.config.transfer_source_balance_reserve_base = Decimal("101")
        self.controller.config.transfer_amount_quantum_base = Decimal("0")
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("100")

        # available=100-101=-1 → return 0
        amount = self.controller._compute_transfer_amount(Decimal("10"), "maker_to_taker")
        self.assertEqual(Decimal("0"), amount)

    def test_compute_amount_quantum_rounding(self):
        self.controller.config.transfer_recover_skew_base = Decimal("2")
        self.controller.config.transfer_target_buffer_base = Decimal("2")
        self.controller.config.transfer_min_amount_base = Decimal("0")
        self.controller.config.transfer_max_amount_base = Decimal("0")
        self.controller.config.transfer_source_balance_reserve_base = Decimal("0")
        self.controller.config.transfer_amount_quantum_base = Decimal("5")
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("100")

        # delta=22 → need=(22-2)/2=10, buffer=2 → 12. quantum=5 → (12//5)*5=10
        amount = self.controller._compute_transfer_amount(Decimal("22"), "maker_to_taker")
        self.assertEqual(Decimal("10"), amount)

    # ── D3. Side Pause ───────────────────────────────────────────────────

    def test_sell_paused_during_taker_to_maker(self):
        self.controller._paused_side = TradeType.SELL
        self.controller.executors_info = [
            SimpleNamespace(
                id="sell-exec-1",
                is_done=False,
                side=TradeType.SELL,
                config=SimpleNamespace(target_profitability=Decimal("0.001")),
            )
        ]
        actions = self.controller.determine_executor_actions()
        stop_actions = [a for a in actions if isinstance(a, StopExecutorAction)]
        create_actions = [a for a in actions if isinstance(a, CreateExecutorAction)]

        self.assertEqual(1, len(stop_actions))
        self.assertEqual("sell-exec-1", stop_actions[0].executor_id)
        self.assertTrue(all(a.executor_config.maker_side != TradeType.SELL for a in create_actions))

    # ── D4. State Transitions ────────────────────────────────────────────

    async def test_signal_failed_response_to_cooldown(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-btc"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("10")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        fake_client = _FakeTransferClient(signal_state="FAILED")
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True

        await self.controller.update_processed_data()
        await asyncio.sleep(0)
        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.COOLDOWN, self.controller._rebalance_state)
        # Issue C fix: cooldown preserves last_qtg_state
        self.assertEqual("FAILED", self.controller._transfer_last_qtg_state)

    async def test_in_flight_timeout_to_error(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-btc"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_request_timeout_sec = 10
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        self.controller._transfer_client = _FakeTransferClient()
        self.controller._transfer_client_ready = True
        self.controller._rebalance_state = RebalanceState.IN_FLIGHT
        self.controller._active_transfer_request_id = "req-1"
        self.controller._transfer_started_ts = 0
        # time=100, started=0, timeout=10 → 100-0=100 > 10 → ERROR
        self.mock_market_data_provider.time.return_value = 100.0

        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.ERROR, self.controller._rebalance_state)

    async def test_in_flight_unknown_state_to_error(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-btc"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_poll_interval_sec = 0
        self.controller.config.transfer_request_timeout_sec = 1800
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        self.controller._transfer_client = _FakeTransferClient(request_state="WITHDRAWAL_UNKNOWN")
        self.controller._transfer_client_ready = True
        self.controller._rebalance_state = RebalanceState.IN_FLIGHT
        self.controller._active_transfer_request_id = "req-1"
        self.controller._transfer_started_ts = 50
        self.mock_market_data_provider.time.return_value = 100.0

        await self.controller.update_processed_data()
        await asyncio.sleep(0)
        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.ERROR, self.controller._rebalance_state)

    async def test_approval_conflict_resolves_via_poll(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-btc"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("10")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        # ConflictApprovalClient: send_signal succeeds (PENDING_APPROVAL), approve raises ConflictError,
        # get_request returns WITHDRAWAL_SUBMITTED (IN_FLIGHT state)
        fake_client = _ConflictApprovalClient(
            signal_state="PENDING_APPROVAL",
            request_state="WITHDRAWAL_SUBMITTED",
        )
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True

        await self.controller.update_processed_data()
        await asyncio.sleep(0)
        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.IN_FLIGHT, self.controller._rebalance_state)

    # ── D5. Balance Recovery & Resume ────────────────────────────────────

    async def test_no_resume_delta_still_above_recover(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-btc"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_recovery_max_wait_sec = 9999
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        self.controller._transfer_client = _FakeTransferClient()
        self.controller._transfer_client_ready = True
        self.controller._rebalance_state = RebalanceState.WAITING_RECOVERY
        self.controller._transfer_recovery_started_ts = 90.0
        # maker=4, taker=1 → delta=3 > recover=0.5
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("4")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        self.mock_market_data_provider.time.return_value = 100.0

        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.WAITING_RECOVERY, self.controller._rebalance_state)

    async def test_forced_resume_on_recovery_deadline(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-btc"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_recovery_max_wait_sec = 10
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        self.controller._transfer_client = _FakeTransferClient()
        self.controller._transfer_client_ready = True
        self.controller._rebalance_state = RebalanceState.WAITING_RECOVERY
        self.controller._transfer_recovery_started_ts = 0.0
        self.controller._paused_side = TradeType.BUY
        # maker=4, taker=1 → delta=3 > recover=0.5 (still high)
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("4")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        # time=100, started=0, max_wait=10 → 100-0=100 > 10 → deadline exceeded
        self.mock_market_data_provider.time.return_value = 100.0

        await self.controller.update_processed_data()

        # Issue A fix: recovery deadline → IDLE (not ERROR)
        self.assertEqual(RebalanceState.IDLE, self.controller._rebalance_state)
        self.assertIsNone(self.controller._paused_side)

    async def test_cooldown_to_idle_transition(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-btc"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        self.controller._transfer_client = _FakeTransferClient()
        self.controller._transfer_client_ready = True
        self.controller._rebalance_state = RebalanceState.COOLDOWN
        self.controller._transfer_retry_at = 50.0
        self.controller._paused_side = TradeType.BUY
        # time=100 >= retry_at=50 → transition to IDLE
        self.mock_market_data_provider.time.return_value = 100.0

        await self.controller.update_processed_data()

        self.assertEqual(RebalanceState.IDLE, self.controller._rebalance_state)
        self.assertIsNone(self.controller._paused_side)

    # ── D6. State File Persistence ───────────────────────────────────────

    async def test_state_file_written_on_transition(self):
        self.controller.config.transfer_trigger_skew_base = Decimal("1")
        self.controller.config.transfer_recover_skew_base = Decimal("0.5")
        self.controller.config.transfer_route_key_maker_to_taker = "upbit-bithumb-btc"
        self.controller.config.transfer_global_lock_enabled = False
        self.controller.config.transfer_state_file_path = f"{self.temp_dir.name}/state_{{controller_id}}.json"
        self.controller.config.transfer_rebalance_enabled = True
        # Re-create state store with temp_dir path
        self.controller._transfer_state_store = TransferRebalanceStateStore(
            path_template=self.controller.config.transfer_state_file_path,
            controller_id="test-controller",
        )
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal("10")
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal("1")
        fake_client = _FakeTransferClient(signal_state="PENDING_APPROVAL", approval_state="READY_FOR_EXECUTION")
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True

        await self.controller.update_processed_data()

        state_file_path = self.controller._transfer_state_store.path
        self.assertTrue(os.path.exists(state_file_path))

    def test_state_restore_in_flight_resumes_polling(self):
        state_file_path = f"{self.temp_dir.name}/state_test-restore.json"
        snapshot = RebalanceSnapshot(
            version=1,
            state="IN_FLIGHT",
            request_id="req-1",
            direction="maker_to_taker",
            paused_side="BUY",
            amount="5",
            cycle_id="cycle-1",
            event_id="ev-1",
            transfer_start_ts=50.0,
            retry_at=None,
            recovery_started_ts=None,
            last_error=None,
            last_qtg_state="WITHDRAWAL_SUBMITTED",
            lock_key=None,
        )
        store = TransferRebalanceStateStore(
            path_template=state_file_path,
            controller_id="unused",
        )
        store.save(snapshot)

        config = UpbitBithumbXemmControllerConfig(
            id="test-restore",
            maker_connector="bithumb",
            maker_trading_pair="BTC-KRW",
            taker_connector="upbit",
            taker_trading_pair="BTC-KRW",
            transfer_rebalance_enabled=True,
            transfer_trigger_skew_base=Decimal("10"),
            transfer_recover_skew_base=Decimal("5"),
            transfer_route_key_maker_to_taker="upbit-bithumb-btc",
            transfer_global_lock_enabled=False,
            transfer_state_file_path=state_file_path,
        )
        controller = UpbitBithumbXemmController(
            config=config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

        self.assertEqual(RebalanceState.IN_FLIGHT, controller._rebalance_state)
        self.assertEqual("req-1", controller._active_transfer_request_id)

    def test_state_restore_signal_submitting_to_cooldown(self):
        state_file_path = f"{self.temp_dir.name}/state_test-restore-cooldown.json"
        snapshot = RebalanceSnapshot(
            version=1,
            state="SIGNAL_SUBMITTING",
            request_id=None,
            direction="maker_to_taker",
            paused_side="BUY",
            amount="5",
            cycle_id="cycle-1",
            event_id="ev-1",
            transfer_start_ts=50.0,
            retry_at=None,
            recovery_started_ts=None,
            last_error=None,
            last_qtg_state=None,
            lock_key=None,
        )
        store = TransferRebalanceStateStore(
            path_template=state_file_path,
            controller_id="unused",
        )
        store.save(snapshot)

        config = UpbitBithumbXemmControllerConfig(
            id="test-restore-cooldown",
            maker_connector="bithumb",
            maker_trading_pair="BTC-KRW",
            taker_connector="upbit",
            taker_trading_pair="BTC-KRW",
            transfer_rebalance_enabled=True,
            transfer_trigger_skew_base=Decimal("10"),
            transfer_recover_skew_base=Decimal("5"),
            transfer_route_key_maker_to_taker="upbit-bithumb-btc",
            transfer_global_lock_enabled=False,
            transfer_state_file_path=state_file_path,
        )
        controller = UpbitBithumbXemmController(
            config=config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

        # intermediate state (SIGNAL_SUBMITTING) gets demoted to COOLDOWN
        self.assertEqual(RebalanceState.COOLDOWN, controller._rebalance_state)

    def test_state_restore_lock_reacquire_terminal_state_recovers_to_idle(self):
        lock_dir = tempfile.mkdtemp(dir=self.temp_dir.name)
        lock_key = "BTC:bithumb:upbit"
        stale_owner = "owner-old"
        lease = TransferGlobalLease(lock_dir=lock_dir, ttl_seconds=3600.0)
        self.assertTrue(lease.acquire(lock_key=lock_key, owner_id=stale_owner))

        state_file_path = f"{self.temp_dir.name}/state_terminal_restore_{{controller_id}}.json"
        store = TransferRebalanceStateStore(path_template=state_file_path, controller_id="test-restore-terminal")
        store.save(
            RebalanceSnapshot(
                version=1,
                state="WAITING_RECOVERY",
                request_id="req-terminal",
                direction="maker_to_taker",
                paused_side="BUY",
                amount="5",
                cycle_id="cycle-1",
                event_id="ev-1",
                transfer_start_ts=50.0,
                retry_at=None,
                recovery_started_ts=51.0,
                last_error=None,
                last_qtg_state="COMPLETED",
                lock_key=lock_key,
            )
        )

        config = UpbitBithumbXemmControllerConfig(
            id="test-restore-terminal",
            maker_connector="bithumb",
            maker_trading_pair="BTC-KRW",
            taker_connector="upbit",
            taker_trading_pair="BTC-KRW",
            transfer_rebalance_enabled=True,
            transfer_trigger_skew_base=Decimal("10"),
            transfer_recover_skew_base=Decimal("5"),
            transfer_route_key_maker_to_taker="bithumb-upbit-btc",
            transfer_global_lock_enabled=True,
            transfer_global_lock_dir_path=lock_dir,
            transfer_global_lock_ttl_sec=3600.0,
            transfer_state_file_path=state_file_path,
        )
        controller = UpbitBithumbXemmController(
            config=config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

        self.assertEqual(RebalanceState.IDLE, controller._rebalance_state)
        self.assertIsNone(controller._transfer_lock_key)
        self.assertFalse(os.path.exists(store.path))
        self.assertFalse(os.path.exists(lease._lock_path(lock_key)))

    def test_state_restore_terminal_state_without_lock_conflict_recovers_to_idle(self):
        lock_dir = tempfile.mkdtemp(dir=self.temp_dir.name)
        state_file_path = f"{self.temp_dir.name}/state_terminal_no_conflict_{{controller_id}}.json"
        store = TransferRebalanceStateStore(path_template=state_file_path, controller_id="test-restore-terminal-ok")
        store.save(
            RebalanceSnapshot(
                version=1,
                state="ERROR",
                request_id="req-terminal",
                direction="maker_to_taker",
                paused_side="BUY",
                amount="5",
                cycle_id="cycle-1",
                event_id="ev-1",
                transfer_start_ts=50.0,
                retry_at=None,
                recovery_started_ts=51.0,
                last_error="old error",
                last_qtg_state="COMPLETED",
                lock_key="BTC:bithumb:upbit",
            )
        )

        config = UpbitBithumbXemmControllerConfig(
            id="test-restore-terminal-ok",
            maker_connector="bithumb",
            maker_trading_pair="BTC-KRW",
            taker_connector="upbit",
            taker_trading_pair="BTC-KRW",
            transfer_rebalance_enabled=True,
            transfer_trigger_skew_base=Decimal("10"),
            transfer_recover_skew_base=Decimal("5"),
            transfer_route_key_maker_to_taker="bithumb-upbit-btc",
            transfer_global_lock_enabled=True,
            transfer_global_lock_dir_path=lock_dir,
            transfer_global_lock_ttl_sec=3600.0,
            transfer_state_file_path=state_file_path,
        )
        controller = UpbitBithumbXemmController(
            config=config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

        self.assertEqual(RebalanceState.IDLE, controller._rebalance_state)
        self.assertFalse(os.path.exists(store.path))

    def test_state_restore_lock_reacquire_non_terminal_goes_cooldown(self):
        lock_dir = tempfile.mkdtemp(dir=self.temp_dir.name)
        lock_key = "BTC:bithumb:upbit"
        lease = TransferGlobalLease(lock_dir=lock_dir, ttl_seconds=3600.0)
        self.assertTrue(lease.acquire(lock_key=lock_key, owner_id="owner-old"))

        state_file_path = f"{self.temp_dir.name}/state_non_terminal_restore_{{controller_id}}.json"
        store = TransferRebalanceStateStore(path_template=state_file_path, controller_id="test-restore-non-terminal")
        store.save(
            RebalanceSnapshot(
                version=1,
                state="IN_FLIGHT",
                request_id="req-inflight",
                direction="maker_to_taker",
                paused_side="BUY",
                amount="5",
                cycle_id="cycle-1",
                event_id="ev-1",
                transfer_start_ts=50.0,
                retry_at=None,
                recovery_started_ts=None,
                last_error=None,
                last_qtg_state="WITHDRAWAL_SUBMITTED",
                lock_key=lock_key,
            )
        )

        config = UpbitBithumbXemmControllerConfig(
            id="test-restore-non-terminal",
            maker_connector="bithumb",
            maker_trading_pair="BTC-KRW",
            taker_connector="upbit",
            taker_trading_pair="BTC-KRW",
            transfer_rebalance_enabled=True,
            transfer_trigger_skew_base=Decimal("10"),
            transfer_recover_skew_base=Decimal("5"),
            transfer_route_key_maker_to_taker="bithumb-upbit-btc",
            transfer_global_lock_enabled=True,
            transfer_global_lock_dir_path=lock_dir,
            transfer_global_lock_ttl_sec=3600.0,
            transfer_state_file_path=state_file_path,
        )
        controller = UpbitBithumbXemmController(
            config=config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

        self.assertEqual(RebalanceState.COOLDOWN, controller._rebalance_state)
        self.assertEqual(0.0, controller._transfer_retry_at)
        self.assertIn("Failed to reacquire transfer global lock", controller._transfer_last_error)

    def test_heartbeat_not_extended_in_error_or_cooldown(self):
        lock_dir = tempfile.mkdtemp(dir=self.temp_dir.name)
        lock_key = "BTC:bithumb:upbit"
        lease = TransferGlobalLease(lock_dir=lock_dir, ttl_seconds=3600.0)
        self.assertTrue(lease.acquire(lock_key=lock_key, owner_id=self.controller._transfer_lock_owner))
        self.controller._transfer_global_lease = lease
        self.controller._transfer_lock_key = lock_key
        self.controller.config.transfer_global_lock_enabled = True

        lock_path = lease._lock_path(lock_key)
        with open(lock_path, "r", encoding="utf-8") as f:
            initial_updated_at = json.load(f)["updated_at"]

        self.controller._rebalance_state = RebalanceState.ERROR
        self.controller._heartbeat_transfer_lock()
        with open(lock_path, "r", encoding="utf-8") as f:
            after_error = json.load(f)["updated_at"]
        self.assertEqual(initial_updated_at, after_error)

        self.controller._rebalance_state = RebalanceState.COOLDOWN
        self.controller._heartbeat_transfer_lock()
        with open(lock_path, "r", encoding="utf-8") as f:
            after_cooldown = json.load(f)["updated_at"]
        self.assertEqual(initial_updated_at, after_cooldown)

    # ── D7. Config Validation ────────────────────────────────────────────

    def test_route_key_required_when_enabled(self):
        with self.assertRaises(ValueError) as ctx:
            UpbitBithumbXemmControllerConfig(
                id="invalid-no-route-key",
                maker_connector="bithumb",
                maker_trading_pair="BTC-KRW",
                taker_connector="upbit",
                taker_trading_pair="BTC-KRW",
                transfer_rebalance_enabled=True,
                transfer_trigger_skew_base=Decimal("10"),
                transfer_recover_skew_base=Decimal("5"),
                transfer_route_key_maker_to_taker="",
                transfer_route_key_taker_to_maker="",
            )
        self.assertIn("route_key", str(ctx.exception).lower())

    # ── D8. Regression ───────────────────────────────────────────────────

    def test_disabled_transfer_no_side_effects(self):
        # transfer_rebalance_enabled is False by default
        self.controller.executors_info = []
        actions = self.controller.determine_executor_actions()

        self.assertEqual(4, len(actions))
        create_actions = [a for a in actions if isinstance(a, CreateExecutorAction)]
        buy_actions = [a for a in create_actions if a.executor_config.maker_side == TradeType.BUY]
        sell_actions = [a for a in create_actions if a.executor_config.maker_side == TradeType.SELL]
        self.assertEqual(2, len(buy_actions))
        self.assertEqual(2, len(sell_actions))

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

    def test_maker_price_source_propagated_to_executor_config(self):
        self.controller.config.buy_levels_targets_amount = [[Decimal("0.001"), Decimal("1")]]
        self.controller.config.sell_levels_targets_amount = [[Decimal("0.001"), Decimal("1")]]
        self.controller.config.max_executors_per_side = 1
        self.controller.config.maker_price_source = "best"
        self.controller.executors_info = []

        actions = self.controller.determine_executor_actions()
        create_actions = [action for action in actions if isinstance(action, CreateExecutorAction)]
        self.assertGreater(len(create_actions), 0)
        self.assertTrue(all(action.executor_config.maker_price_source == "best" for action in create_actions))
