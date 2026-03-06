from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Optional


class RebalanceState(Enum):
    IDLE = "IDLE"
    SIGNAL_SUBMITTING = "SIGNAL_SUBMITTING"
    APPROVAL_SUBMITTING = "APPROVAL_SUBMITTING"
    IN_FLIGHT = "IN_FLIGHT"
    WAITING_RECOVERY = "WAITING_RECOVERY"
    COOLDOWN = "COOLDOWN"
    ERROR = "ERROR"


@dataclass
class RebalanceSnapshot:
    version: int
    state: str
    request_id: Optional[str]
    direction: Optional[str]
    paused_side: Optional[str]
    amount: Optional[str]
    cycle_id: Optional[str]
    event_id: Optional[str]
    transfer_start_ts: Optional[float]
    retry_at: Optional[float]
    recovery_started_ts: Optional[float]
    last_error: Optional[str]
    last_qtg_state: Optional[str]
    lock_key: Optional[str]


class TransferRebalanceStateStore:
    def __init__(self, *, path_template: str, controller_id: str):
        rendered = path_template.format(controller_id=controller_id)
        self.path = os.path.expanduser(rendered)

    def load(self) -> Optional[RebalanceSnapshot]:
        if not self.path:
            return None
        if not os.path.exists(self.path):
            return None
        with open(self.path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if payload.get("version") != 1:
            raise ValueError(f"Unsupported rebalance state file version: {payload.get('version')}")
        return RebalanceSnapshot(**payload)

    def save(self, snapshot: RebalanceSnapshot):
        if not self.path:
            return
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(prefix=".rebalance_state_", dir=directory or ".")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(asdict(snapshot), f, separators=(",", ":"), sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.path)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def clear(self):
        if self.path and os.path.exists(self.path):
            os.remove(self.path)
