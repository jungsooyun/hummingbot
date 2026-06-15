import asyncio
import tempfile
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from controllers.generic.inventory_rebalance_state import InventoryRebalanceState
from controllers.generic.upbit_bithumb_inventory_rebalance_controller import (
    UpbitBithumbInventoryRebalanceController,
    UpbitBithumbInventoryRebalanceControllerConfig,
)


class _FakeTransferClient:
    def __init__(
        self,
        signal_state="PENDING_APPROVAL",
        approval_state="COMPLETED",
        request_state="COMPLETED",
        route_enabled=True,
        route_is_paused=False,
        route_pause_reason=None,
    ):
        self.signal_state = signal_state
        self.approval_state = approval_state
        self.request_state = request_state
        self.route_enabled = route_enabled
        self.route_is_paused = route_is_paused
        self.route_pause_reason = route_pause_reason
        self.sent_signals = []
        self.approvals = []
        self.requests = []
        self.route_checks = []

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
        return SimpleNamespace(request_id="req-1", state=self.signal_state, deduplicated=False, reason=None)

    async def approve_request(self, request_id, approver_id):
        self.approvals.append({"request_id": request_id, "approver_id": approver_id})
        return SimpleNamespace(request_id=request_id, state=self.approval_state)

    async def get_request(self, request_id):
        self.requests.append(request_id)
        return SimpleNamespace(
            request_id=request_id,
            state=self.request_state,
            asset="SEI",
            amount="45",
            from_exchange="upbit",
            to_exchange="bithumb",
            created_at=None,
            updated_at=None,
        )

    async def get_route_by_key(self, route_key):
        self.route_checks.append(route_key)
        return SimpleNamespace(
            route_id="route-1",
            route_key=route_key,
            enabled=self.route_enabled,
            is_paused=self.route_is_paused,
            pause_reason=self.route_pause_reason,
        )

    async def close(self):
        return None


class TestUpbitBithumbInventoryRebalanceController(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config = UpbitBithumbInventoryRebalanceControllerConfig(
            id="inventory-rebalance-test",
            asset="SEI",
            bithumb_connector="bithumb",
            bithumb_trading_pair="SEI-KRW",
            upbit_connector="upbit",
            upbit_trading_pair="SEI-KRW",
            transfer_route_key_bithumb_to_upbit="bithumb-upbit-sei",
            transfer_route_key_upbit_to_bithumb="upbit-bithumb-sei",
            transfer_delay_before_submit_sec=0,
            transfer_settle_wait_sec=0,
            transfer_recheck_interval_sec=10,
            transfer_recheck_confirmations_required=2,
            transfer_min_amount_base=Decimal("0.1"),
            transfer_amount_quantum_base=Decimal("0.0001"),
            state_file_path=f"{self.temp_dir.name}/inventory_rebalance_state_{{controller_id}}.json",
            transfer_global_lock_dir_path=f"{self.temp_dir.name}/locks",
        )

        self.mock_market_data_provider = MagicMock()
        type(self.mock_market_data_provider).ready = PropertyMock(return_value=True)
        self.mock_market_data_provider.time.return_value = 100.0
        self.mock_market_data_provider.get_price_by_type.side_effect = lambda connector, pair, price_type: Decimal("100")

        bithumb_connector = MagicMock()
        upbit_connector = MagicMock()
        bithumb_connector.get_balance.return_value = Decimal("60")
        upbit_connector.get_balance.return_value = Decimal("40")
        bithumb_connector.get_available_balance.return_value = Decimal("60")
        upbit_connector.get_available_balance.return_value = Decimal("40")
        self.mock_market_data_provider.connectors = {
            "bithumb": bithumb_connector,
            "upbit": upbit_connector,
        }

        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)
        self.controller = UpbitBithumbInventoryRebalanceController(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

    def _set_total_balances(self, *, bithumb: str, upbit: str):
        self.mock_market_data_provider.connectors["bithumb"].get_balance.return_value = Decimal(bithumb)
        self.mock_market_data_provider.connectors["upbit"].get_balance.return_value = Decimal(upbit)

    def _set_available_balances(self, *, bithumb: str, upbit: str):
        self.mock_market_data_provider.connectors["bithumb"].get_available_balance.return_value = Decimal(bithumb)
        self.mock_market_data_provider.connectors["upbit"].get_available_balance.return_value = Decimal(upbit)

    def tearDown(self):
        self.temp_dir.cleanup()
        super().tearDown()

    def test_captures_initial_total_inventory_baseline_and_exposes_custom_info_using_total_balances(self):
        self._set_total_balances(bithumb="60", upbit="40")
        self._set_available_balances(bithumb="10", upbit="5")
        self.controller._ensure_baseline()

        self.assertEqual(Decimal("100"), self.controller._initial_total_inventory_base)
        info = self.controller.get_custom_info()
        self.assertEqual("100", info["initial_total_inventory_base"])
        self.assertEqual("100", info["current_total_inventory_base"])
        self.assertEqual("0", info["inventory_delta_base"])
        self.assertEqual("0.6", info["bithumb_inventory_ratio"])
        self.assertEqual("0.4", info["upbit_inventory_ratio"])

    def test_trigger_selects_upbit_to_bithumb_and_targets_half_of_initial_total(self):
        self.controller._ensure_baseline()
        self._set_total_balances(bithumb="5", upbit="95")
        self._set_available_balances(bithumb="5", upbit="95")

        trigger = self.controller._current_transfer_trigger()

        self.assertIsNotNone(trigger)
        self.assertEqual("upbit_to_bithumb", trigger["direction"])
        self.assertEqual(Decimal("10.00"), trigger["trigger_balance_threshold"])
        self.assertEqual(Decimal("50.00"), trigger["target_balance_threshold"])
        self.assertEqual(Decimal("45.0000"), self.controller._compute_transfer_amount(trigger))

    def test_transfer_target_falls_back_to_half_of_current_total_when_inventory_shrunk(self):
        self.controller._ensure_baseline()
        self._set_total_balances(bithumb="5", upbit="55")
        self._set_available_balances(bithumb="5", upbit="55")

        trigger = self.controller._current_transfer_trigger()

        self.assertIsNotNone(trigger)
        self.assertEqual(Decimal("30.00"), trigger["target_balance_threshold"])
        self.assertEqual(Decimal("25.0000"), self.controller._compute_transfer_amount(trigger))
        self.assertEqual(Decimal("-40"), self.controller._inventory_delta_base())
        self.assertEqual(Decimal("-0.4"), self.controller._inventory_delta_pct())
        self.assertEqual(Decimal("-4000"), self.controller._inventory_delta_pnl_quote())

    async def test_update_processed_data_submits_transfer_and_waits_for_recovery(self):
        self.controller._ensure_baseline()
        self._set_total_balances(bithumb="5", upbit="95")
        self._set_available_balances(bithumb="5", upbit="95")
        fake_client = _FakeTransferClient(signal_state="PENDING_APPROVAL", approval_state="COMPLETED")
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True

        trigger = self.controller._current_transfer_trigger()
        self.assertIsNotNone(trigger)
        self.assertEqual(Decimal("45.0000"), self.controller._compute_transfer_amount(trigger))
        self.controller._acquire_transfer_lock = MagicMock(return_value=True)
        started = self.controller._begin_transfer_submission(trigger=trigger, now=self.mock_market_data_provider.time.return_value, reason="test")
        self.assertTrue(started)
        await asyncio.sleep(0)
        if self.controller._transfer_task is not None:
            await self.controller._transfer_task

        self.assertEqual(1, len(fake_client.sent_signals))
        self.assertEqual("upbit-bithumb-sei", fake_client.sent_signals[0]["route_key"])
        self.assertEqual(Decimal("45.0000"), fake_client.sent_signals[0]["amount"])
        self.assertEqual(InventoryRebalanceState.WAITING_RECOVERY, self.controller._state)
        self.assertEqual("upbit_to_bithumb", self.controller._transfer_direction)
        self.assertEqual("bithumb", self.controller._paused_exchange)
        self.assertEqual(110.0, self.controller._transfer_retry_at)
        self.assertEqual(0, self.controller._transfer_recheck_count)

    async def test_paused_route_precheck_blocks_signal_submission(self):
        self.controller.config.transfer_route_pause_precheck_enabled = True
        self.controller._ensure_baseline()
        self._set_total_balances(bithumb="5", upbit="95")
        self._set_available_balances(bithumb="5", upbit="95")
        fake_client = _FakeTransferClient(route_is_paused=True, route_pause_reason="withdrawal limit reached")
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True
        self.controller._acquire_transfer_lock = MagicMock(return_value=True)

        trigger = self.controller._current_transfer_trigger()
        self.assertIsNotNone(trigger)
        started = self.controller._begin_transfer_submission(
            trigger=trigger,
            now=self.mock_market_data_provider.time.return_value,
            reason="test",
        )
        self.assertTrue(started)
        await asyncio.sleep(0)
        if self.controller._transfer_task is not None:
            await self.controller._transfer_task

        self.assertEqual(InventoryRebalanceState.COOLDOWN, self.controller._state)
        self.assertEqual(["upbit-bithumb-sei"], fake_client.route_checks)
        self.assertEqual(0, len(fake_client.sent_signals))
        self.assertIn("route paused during precheck", self.controller._transfer_last_error.lower())

    async def test_paused_route_cooldown_holds_until_route_is_resumed(self):
        self.controller.config.transfer_route_pause_precheck_enabled = True
        self.controller._ensure_baseline()
        self._set_total_balances(bithumb="5", upbit="95")
        self._set_available_balances(bithumb="5", upbit="95")
        fake_client = _FakeTransferClient(route_is_paused=True, route_pause_reason="withdrawal limit reached")
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True
        self.controller._acquire_transfer_lock = MagicMock(return_value=True)

        trigger = self.controller._current_transfer_trigger()
        self.assertIsNotNone(trigger)
        started = self.controller._begin_transfer_submission(
            trigger=trigger,
            now=self.mock_market_data_provider.time.return_value,
            reason="test",
        )
        self.assertTrue(started)
        await asyncio.sleep(0)
        if self.controller._transfer_task is not None:
            await self.controller._transfer_task

        self.mock_market_data_provider.time.return_value = 101.0
        await self.controller.update_processed_data()

        self.assertEqual(InventoryRebalanceState.COOLDOWN, self.controller._state)
        self.assertEqual(["upbit-bithumb-sei", "upbit-bithumb-sei"], fake_client.route_checks)
        self.assertEqual(0, len(fake_client.sent_signals))
        self.assertEqual(161.0, self.controller._transfer_retry_at)

        fake_client.route_is_paused = False
        fake_client.route_pause_reason = None
        self.mock_market_data_provider.time.return_value = 102.0
        await self.controller.update_processed_data()
        await asyncio.sleep(0)
        if self.controller._transfer_task is not None:
            await self.controller._transfer_task

        self.assertEqual(4, len(fake_client.route_checks))
        self.assertEqual(1, len(fake_client.sent_signals))
        self.assertEqual("upbit-bithumb-sei", fake_client.sent_signals[0]["route_key"])
        self.assertEqual(InventoryRebalanceState.WAITING_RECOVERY, self.controller._state)

    async def test_waiting_recovery_does_not_retrigger_until_recheck_interval(self):
        self.controller._ensure_baseline()
        self._set_total_balances(bithumb="5", upbit="95")
        self._set_available_balances(bithumb="5", upbit="95")
        fake_client = _FakeTransferClient(signal_state="PENDING_APPROVAL", approval_state="COMPLETED")
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True
        self.controller._acquire_transfer_lock = MagicMock(return_value=True)

        trigger = self.controller._current_transfer_trigger()
        self.assertIsNotNone(trigger)
        started = self.controller._begin_transfer_submission(
            trigger=trigger,
            now=self.mock_market_data_provider.time.return_value,
            reason="test",
        )
        self.assertTrue(started)
        await asyncio.sleep(0)
        if self.controller._transfer_task is not None:
            await self.controller._transfer_task

        self.mock_market_data_provider.time.return_value = 109.0
        await self.controller.update_processed_data()

        self.assertEqual(1, len(fake_client.sent_signals))
        self.assertEqual(InventoryRebalanceState.WAITING_RECOVERY, self.controller._state)
        self.assertEqual(0, self.controller._transfer_recheck_count)

    async def test_waiting_recovery_rechecks_after_interval_and_resubmits_if_still_needed(self):
        self.controller._ensure_baseline()
        self._set_total_balances(bithumb="5", upbit="95")
        self._set_available_balances(bithumb="5", upbit="95")
        fake_client = _FakeTransferClient(signal_state="PENDING_APPROVAL", approval_state="COMPLETED")
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True
        self.controller._acquire_transfer_lock = MagicMock(return_value=True)

        trigger = self.controller._current_transfer_trigger()
        self.assertIsNotNone(trigger)
        started = self.controller._begin_transfer_submission(
            trigger=trigger,
            now=self.mock_market_data_provider.time.return_value,
            reason="test",
        )
        self.assertTrue(started)
        await asyncio.sleep(0)
        if self.controller._transfer_task is not None:
            await self.controller._transfer_task

        self.mock_market_data_provider.time.return_value = 111.0
        await self.controller.update_processed_data()

        self.assertEqual(1, len(fake_client.sent_signals))
        self.assertEqual(InventoryRebalanceState.WAITING_RECOVERY, self.controller._state)
        self.assertEqual(1, self.controller._transfer_recheck_count)
        self.assertEqual(120.0, self.controller._transfer_retry_at)

        self.mock_market_data_provider.time.return_value = 121.0
        await self.controller.update_processed_data()
        await asyncio.sleep(0)
        if self.controller._transfer_task is not None:
            await self.controller._transfer_task

        self.assertEqual(2, len(fake_client.sent_signals))
        self.assertEqual(InventoryRebalanceState.WAITING_RECOVERY, self.controller._state)
        self.assertEqual("req-1", self.controller._active_transfer_request_id)
        self.assertEqual(Decimal("45.0000"), fake_client.sent_signals[-1]["amount"])
        self.assertEqual(131.0, self.controller._transfer_retry_at)
        self.assertEqual(0, self.controller._transfer_recheck_count)

    async def test_waiting_recovery_clears_when_trigger_no_longer_active_after_recheck(self):
        self.controller._ensure_baseline()
        self._set_total_balances(bithumb="5", upbit="95")
        self._set_available_balances(bithumb="5", upbit="95")
        fake_client = _FakeTransferClient(signal_state="PENDING_APPROVAL", approval_state="COMPLETED")
        self.controller._transfer_client = fake_client
        self.controller._transfer_client_ready = True
        self.controller._acquire_transfer_lock = MagicMock(return_value=True)

        trigger = self.controller._current_transfer_trigger()
        self.assertIsNotNone(trigger)
        started = self.controller._begin_transfer_submission(
            trigger=trigger,
            now=self.mock_market_data_provider.time.return_value,
            reason="test",
        )
        self.assertTrue(started)
        await asyncio.sleep(0)
        if self.controller._transfer_task is not None:
            await self.controller._transfer_task

        self._set_total_balances(bithumb="45", upbit="55")
        self.mock_market_data_provider.time.return_value = 111.0
        await self.controller.update_processed_data()

        self.assertEqual(1, len(fake_client.sent_signals))
        self.assertEqual(InventoryRebalanceState.IDLE, self.controller._state)
        self.assertIsNone(self.controller._transfer_direction)

    def test_transfer_amount_uses_total_for_trigger_but_available_for_sendable_source(self):
        self.controller._ensure_baseline()
        self._set_total_balances(bithumb="5", upbit="95")
        self._set_available_balances(bithumb="5", upbit="20")

        trigger = self.controller._current_transfer_trigger()

        self.assertIsNotNone(trigger)
        self.assertEqual("upbit_to_bithumb", trigger["direction"])
        self.assertEqual(Decimal("20.0000"), self.controller._compute_transfer_amount(trigger))
