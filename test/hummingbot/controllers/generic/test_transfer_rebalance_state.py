import json
import os
import tempfile

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from controllers.generic.transfer_rebalance_state import RebalanceSnapshot, TransferRebalanceStateStore


class TestTransferRebalanceStateStore(IsolatedAsyncioWrapperTestCase):
    def test_save_load_and_clear_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = os.path.join(temp_dir, "rebalance_{controller_id}.json")
            store = TransferRebalanceStateStore(path_template=state_path, controller_id="ctrl-a")
            snapshot = RebalanceSnapshot(
                version=1,
                state="IN_FLIGHT",
                request_id="req-1",
                direction="maker_to_taker",
                paused_side="BUY",
                amount="12.3",
                cycle_id="cycle-1",
                event_id="evt-1",
                transfer_start_ts=100.0,
                retry_at=None,
                recovery_started_ts=None,
                last_error=None,
                last_qtg_state="WITHDRAWAL_SUBMITTED",
                lock_key="XRP:upbit:bithumb",
            )
            store.save(snapshot)
            loaded = store.load()
            self.assertIsNotNone(loaded)
            self.assertEqual(snapshot.state, loaded.state)
            self.assertEqual(snapshot.request_id, loaded.request_id)
            store.clear()
            self.assertIsNone(store.load())

    def test_load_nonexistent_returns_none(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = os.path.join(temp_dir, "{controller_id}.json")
            store = TransferRebalanceStateStore(path_template=state_path, controller_id="does_not_exist")
            self.assertIsNone(store.load())

    def test_load_corrupted_json_raises(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bad_file = os.path.join(temp_dir, "test.json")
            with open(bad_file, "w", encoding="utf-8") as f:
                f.write("{not valid json")
            state_path = os.path.join(temp_dir, "{controller_id}.json")
            store = TransferRebalanceStateStore(path_template=state_path, controller_id="test")
            with self.assertRaises(json.JSONDecodeError):
                store.load()

    def test_load_wrong_version_raises(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            bad_file = os.path.join(temp_dir, "test.json")
            payload = {
                "version": 99,
                "state": "IDLE",
                "request_id": None,
                "direction": None,
                "paused_side": None,
                "amount": None,
                "cycle_id": None,
                "event_id": None,
                "transfer_start_ts": None,
                "retry_at": None,
                "recovery_started_ts": None,
                "last_error": None,
                "last_qtg_state": None,
                "lock_key": None,
            }
            with open(bad_file, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            state_path = os.path.join(temp_dir, "{controller_id}.json")
            store = TransferRebalanceStateStore(path_template=state_path, controller_id="test")
            with self.assertRaises(ValueError) as ctx:
                store.load()
            self.assertIn("Unsupported", str(ctx.exception))
