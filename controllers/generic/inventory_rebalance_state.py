from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Optional


class InventoryRebalanceState(Enum):
    IDLE = "IDLE"
    DELAYING = "DELAYING"
    SIGNAL_SUBMITTING = "SIGNAL_SUBMITTING"
    APPROVAL_SUBMITTING = "APPROVAL_SUBMITTING"
    IN_FLIGHT = "IN_FLIGHT"
    WAITING_RECOVERY = "WAITING_RECOVERY"
    COOLDOWN = "COOLDOWN"
    ERROR = "ERROR"


@dataclass
class InventoryRebalanceSnapshot:
    version: int
    state: str
    initial_total_inventory_base: Optional[str]
    baseline_captured_ts: Optional[float]
    request_id: Optional[str]
    direction: Optional[str]
    paused_exchange: Optional[str]
    amount: Optional[str]
    cycle_id: Optional[str]
    event_id: Optional[str]
    transfer_start_ts: Optional[float]
    retry_at: Optional[float]
    recovery_started_ts: Optional[float]
    trigger_balance_threshold: Optional[str]
    target_balance_threshold: Optional[str]
    last_reference_price_quote: Optional[str]
    last_error: Optional[str]
    last_qtg_state: Optional[str]
    lock_key: Optional[str]
    delay_started_ts: Optional[float] = None
    delay_deadline_ts: Optional[float] = None
    recovery_recheck_count: int = 0


class InventoryRebalanceStateStore:
    def __init__(self, *, path_template: str, controller_id: str):
        rendered = path_template.format(controller_id=controller_id)
        self.path = os.path.expanduser(rendered)

    def load(self) -> Optional[InventoryRebalanceSnapshot]:
        if not self.path:
            return None
        if not os.path.exists(self.path):
            return None
        with open(self.path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if payload.get("version") != 1:
            raise ValueError(f"Unsupported inventory rebalance state file version: {payload.get('version')}")
        return InventoryRebalanceSnapshot(**payload)

    def save(self, snapshot: InventoryRebalanceSnapshot):
        if not self.path:
            return
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(prefix=".inventory_rebalance_state_", dir=directory or ".")
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
