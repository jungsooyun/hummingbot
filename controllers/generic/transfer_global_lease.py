from __future__ import annotations

import json
import os
import re
import tempfile
import time
from typing import Any, Optional


class TransferGlobalLease:
    def __init__(self, *, lock_dir: str, ttl_seconds: float):
        self._lock_dir = os.path.expanduser(lock_dir)
        self._ttl_seconds = ttl_seconds
        os.makedirs(self._lock_dir, exist_ok=True)

    def acquire(self, *, lock_key: str, owner_id: str) -> bool:
        path = self._lock_path(lock_key)
        payload = self._new_payload(lock_key=lock_key, owner_id=owner_id)
        serialized = json.dumps(payload, separators=(",", ":"), sort_keys=True)

        if self._create_lock_file(path=path, serialized=serialized):
            return True

        existing = self._read_payload(path)
        if existing is not None and not self._is_stale(existing):
            return False

        # stale or corrupted lock: best-effort reclaim
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError:
            return False

        return self._create_lock_file(path=path, serialized=serialized)

    def heartbeat(self, *, lock_key: str, owner_id: str):
        path = self._lock_path(lock_key)
        existing = self._read_payload(path)
        if existing is None:
            return
        if existing.get("owner_id") != owner_id:
            return
        existing["updated_at"] = time.time()
        self._atomic_write(path=path, payload=existing)

    def release(self, *, lock_key: str, owner_id: str, force: bool = False) -> bool:
        path = self._lock_path(lock_key)
        existing = self._read_payload(path)
        if existing is None:
            return True
        if not force and existing.get("owner_id") != owner_id:
            return False
        try:
            os.remove(path)
        except FileNotFoundError:
            return True
        return True

    def _lock_path(self, lock_key: str) -> str:
        safe_key = re.sub(r"[^a-zA-Z0-9._-]", "_", lock_key)
        return os.path.join(self._lock_dir, f"{safe_key}.lock")

    def _new_payload(self, *, lock_key: str, owner_id: str) -> dict[str, Any]:
        now = time.time()
        return {
            "version": 1,
            "lock_key": lock_key,
            "owner_id": owner_id,
            "created_at": now,
            "updated_at": now,
        }

    def _is_stale(self, payload: dict[str, Any]) -> bool:
        updated_at = payload.get("updated_at")
        if not isinstance(updated_at, (int, float)):
            return True
        return time.time() - float(updated_at) > self._ttl_seconds

    @staticmethod
    def _create_lock_file(*, path: str, serialized: str) -> bool:
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(serialized)
            f.flush()
            os.fsync(f.fileno())
        return True

    @staticmethod
    def _read_payload(path: str) -> Optional[dict[str, Any]]:
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                return None
            return payload
        except (json.JSONDecodeError, OSError):
            return None

    @staticmethod
    def _atomic_write(*, path: str, payload: dict[str, Any]):
        directory = os.path.dirname(path)
        fd, temp_path = tempfile.mkstemp(prefix=".transfer_lock_", dir=directory or ".")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, path)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
