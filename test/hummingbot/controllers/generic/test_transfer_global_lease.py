import json
import os
import tempfile
import time

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from controllers.generic.transfer_global_lease import TransferGlobalLease


class TestTransferGlobalLease(IsolatedAsyncioWrapperTestCase):
    def test_acquire_release_and_reacquire(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lease = TransferGlobalLease(lock_dir=temp_dir, ttl_seconds=1.0)
            acquired = lease.acquire(lock_key="XRP:upbit:bithumb", owner_id="owner-1")
            self.assertTrue(acquired)
            acquired_by_other = lease.acquire(lock_key="XRP:upbit:bithumb", owner_id="owner-2")
            self.assertFalse(acquired_by_other)
            released = lease.release(lock_key="XRP:upbit:bithumb", owner_id="owner-1")
            self.assertTrue(released)
            reacquired = lease.acquire(lock_key="XRP:upbit:bithumb", owner_id="owner-2")
            self.assertTrue(reacquired)

    def test_stale_lock_can_be_reclaimed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lease = TransferGlobalLease(lock_dir=temp_dir, ttl_seconds=0.01)
            self.assertTrue(lease.acquire(lock_key="XRP:upbit:bithumb", owner_id="owner-1"))
            time.sleep(0.03)
            self.assertTrue(lease.acquire(lock_key="XRP:upbit:bithumb", owner_id="owner-2"))

    def test_heartbeat_refreshes_updated_at(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lease = TransferGlobalLease(lock_dir=temp_dir, ttl_seconds=10.0)
            lock_key = "XRP:upbit:bithumb"
            self.assertTrue(lease.acquire(lock_key=lock_key, owner_id="owner-1"))

            path = lease._lock_path(lock_key)
            with open(path, "r", encoding="utf-8") as f:
                original_updated_at = json.load(f)["updated_at"]

            time.sleep(0.05)
            lease.heartbeat(lock_key=lock_key, owner_id="owner-1")

            with open(path, "r", encoding="utf-8") as f:
                new_updated_at = json.load(f)["updated_at"]

            self.assertGreater(new_updated_at, original_updated_at)

    def test_heartbeat_wrong_owner_is_noop(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lease = TransferGlobalLease(lock_dir=temp_dir, ttl_seconds=10.0)
            lock_key = "XRP:upbit:bithumb"
            self.assertTrue(lease.acquire(lock_key=lock_key, owner_id="owner-1"))

            path = lease._lock_path(lock_key)
            with open(path, "r", encoding="utf-8") as f:
                original_updated_at = json.load(f)["updated_at"]

            lease.heartbeat(lock_key=lock_key, owner_id="owner-2")

            with open(path, "r", encoding="utf-8") as f:
                after_updated_at = json.load(f)["updated_at"]

            self.assertEqual(original_updated_at, after_updated_at)

    def test_release_force_overrides_owner(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lease = TransferGlobalLease(lock_dir=temp_dir, ttl_seconds=10.0)
            lock_key = "XRP:upbit:bithumb"
            self.assertTrue(lease.acquire(lock_key=lock_key, owner_id="owner-1"))

            # Wrong owner without force should fail
            self.assertFalse(lease.release(lock_key=lock_key, owner_id="owner-2", force=False))

            # Wrong owner with force should succeed
            self.assertTrue(lease.release(lock_key=lock_key, owner_id="owner-2", force=True))

            # Lock file should be gone; a new owner can acquire
            self.assertFalse(os.path.exists(lease._lock_path(lock_key)))
            self.assertTrue(lease.acquire(lock_key=lock_key, owner_id="owner-3"))
