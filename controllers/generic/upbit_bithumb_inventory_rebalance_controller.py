from __future__ import annotations

import asyncio
import os
import time
import uuid
from decimal import Decimal
from typing import Dict, List, Literal, Optional

from pydantic import Field, model_validator

from controllers.generic.inventory_rebalance_state import (
    InventoryRebalanceSnapshot,
    InventoryRebalanceState,
    InventoryRebalanceStateStore,
)
from controllers.generic.transfer_global_lease import TransferGlobalLease
from controllers.generic.transfer_guard_client import (
    ApprovalResult,
    AuthError,
    ConflictError,
    NetworkError,
    NotFoundError,
    RateLimitError,
    RequestStatus,
    ServerError,
    SignalResult,
    TransferGuardClient,
    TransferGuardError,
)
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import MarketDict, PriceType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.models.executor_actions import ExecutorAction

PENDING_STATES = {"PENDING_APPROVAL", "APPROVED", "READY_FOR_EXECUTION"}
IN_FLIGHT_STATES = {
    "WITHDRAWAL_SUBMITTING",
    "WITHDRAWAL_SUBMITTED",
    "WITHDRAWAL_PROCESSING",
    "WITHDRAWAL_COMPLETED",
    "DEPOSIT_PENDING",
}
SUCCESS_STATES = {"DEPOSIT_CREDITED", "COMPLETED"}
TERMINAL_FAILURE_STATES = {
    "FAILED",
    "REJECTED",
    "EXPIRED",
    "WITHDRAWAL_FAILED",
    "DEPOSIT_TIMEOUT",
    "EXECUTION_SKIPPED",
}
UNKNOWN_STATES = {"WITHDRAWAL_UNKNOWN"}

Direction = Literal["bithumb_to_upbit", "upbit_to_bithumb"]


class UpbitBithumbInventoryRebalanceControllerConfig(ControllerConfigBase):
    controller_name: str = "upbit_bithumb_inventory_rebalance_controller"
    controller_type: str = "generic"
    total_amount_quote: Decimal = Decimal("0")

    asset: str = Field(default="SEI", json_schema_extra={"prompt": "Asset symbol: ", "prompt_on_new": True})
    bithumb_connector: str = Field(default="bithumb", json_schema_extra={"prompt": "Bithumb connector: ", "prompt_on_new": True})
    bithumb_trading_pair: str = Field(default="SEI-KRW", json_schema_extra={"prompt": "Bithumb trading pair: ", "prompt_on_new": True})
    upbit_connector: str = Field(default="upbit", json_schema_extra={"prompt": "Upbit connector: ", "prompt_on_new": True})
    upbit_trading_pair: str = Field(default="SEI-KRW", json_schema_extra={"prompt": "Upbit trading pair: ", "prompt_on_new": True})

    transfer_rebalance_enabled: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    trigger_inventory_ratio: Decimal = Field(default=Decimal("0.10"), json_schema_extra={"is_updatable": True})
    target_inventory_ratio: Decimal = Field(default=Decimal("0.50"), json_schema_extra={"is_updatable": True})
    transfer_min_amount_base: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    transfer_amount_quantum_base: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})

    transfer_guard_base_url: str = Field(default="http://127.0.0.1:8100", json_schema_extra={"is_updatable": True})
    transfer_guard_callback_url: str = Field(
        default="http://dashboard:3001/api/qtg-callbacks/receive",
        json_schema_extra={"is_updatable": True},
    )
    transfer_guard_signal_key_id: str = Field(default="", json_schema_extra={"is_updatable": True})
    transfer_guard_signal_secret_env: str = Field(default="QTG_SIGNAL_HMAC_SECRET", json_schema_extra={"is_updatable": True})
    transfer_guard_approval_key_id: str = Field(default="", json_schema_extra={"is_updatable": True})
    transfer_guard_approval_secret_env: str = Field(default="QTG_APPROVAL_HMAC_SECRET", json_schema_extra={"is_updatable": True})
    transfer_guard_read_key_id: str = Field(default="", json_schema_extra={"is_updatable": True})
    transfer_guard_read_secret_env: str = Field(default="QTG_READ_HMAC_SECRET", json_schema_extra={"is_updatable": True})
    transfer_guard_admin_key_id: str = Field(default="", json_schema_extra={"is_updatable": True})
    transfer_guard_admin_secret_env: str = Field(default="QTG_ADMIN_HMAC_SECRET", json_schema_extra={"is_updatable": True})
    transfer_route_pause_precheck_enabled: bool = Field(default=False, json_schema_extra={"is_updatable": True})

    transfer_route_key_bithumb_to_upbit: str = Field(default="", json_schema_extra={"is_updatable": True})
    transfer_route_key_upbit_to_bithumb: str = Field(default="", json_schema_extra={"is_updatable": True})

    controller_update_interval_sec: float = Field(default=0.0, json_schema_extra={"is_updatable": True})
    transfer_poll_interval_sec: float = Field(default=5.0, json_schema_extra={"is_updatable": True})
    transfer_delay_before_submit_sec: float = Field(default=30.0, json_schema_extra={"is_updatable": True})
    transfer_request_cooldown_sec: float = Field(default=120.0, json_schema_extra={"is_updatable": True})
    transfer_request_timeout_sec: float = Field(default=3600.0, json_schema_extra={"is_updatable": True})
    transfer_settle_wait_sec: float = Field(default=60.0, json_schema_extra={"is_updatable": True})
    transfer_recheck_interval_sec: float = Field(default=600.0, json_schema_extra={"is_updatable": True})
    transfer_recheck_confirmations_required: int = Field(default=6, json_schema_extra={"is_updatable": True})
    transfer_recovery_max_wait_sec: float = Field(default=900.0, json_schema_extra={"is_updatable": True})

    state_file_path: str = Field(
        default="/home/hummingbot/data/inventory_rebalance_state_{controller_id}.json",
        json_schema_extra={"is_updatable": True},
    )
    transfer_global_lock_enabled: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    transfer_global_lock_dir_path: str = Field(
        default="/home/hummingbot/data/transfer_rebalance_locks",
        json_schema_extra={"is_updatable": True},
    )
    transfer_global_lock_ttl_sec: float = Field(default=3600.0, json_schema_extra={"is_updatable": True})

    @model_validator(mode="after")
    def post_validations(self):
        bithumb_base, bithumb_quote = split_hb_trading_pair(self.bithumb_trading_pair)
        upbit_base, upbit_quote = split_hb_trading_pair(self.upbit_trading_pair)
        if bithumb_base != upbit_base:
            raise ValueError("Bithumb and Upbit base assets must match.")
        if bithumb_quote != upbit_quote:
            raise ValueError("Bithumb and Upbit quote assets must match.")
        if self.asset.upper() != bithumb_base.upper():
            raise ValueError("asset must match the base asset of the configured trading pairs.")
        if not (Decimal("0") < self.trigger_inventory_ratio < Decimal("1")):
            raise ValueError("trigger_inventory_ratio must be between 0 and 1.")
        if not (Decimal("0") < self.target_inventory_ratio < Decimal("1")):
            raise ValueError("target_inventory_ratio must be between 0 and 1.")
        if self.target_inventory_ratio <= self.trigger_inventory_ratio:
            raise ValueError("target_inventory_ratio must be greater than trigger_inventory_ratio.")
        if self.transfer_delay_before_submit_sec < 0:
            raise ValueError("transfer_delay_before_submit_sec must be >= 0")
        if self.transfer_settle_wait_sec < 0:
            raise ValueError("transfer_settle_wait_sec must be >= 0")
        if self.transfer_recheck_interval_sec <= 0:
            raise ValueError("transfer_recheck_interval_sec must be > 0")
        if self.transfer_recheck_interval_sec < self.transfer_settle_wait_sec:
            raise ValueError("transfer_recheck_interval_sec must be >= transfer_settle_wait_sec")
        if self.transfer_recheck_confirmations_required <= 0:
            raise ValueError("transfer_recheck_confirmations_required must be > 0")
        if self.transfer_rebalance_enabled and not (
            self.transfer_route_key_bithumb_to_upbit or self.transfer_route_key_upbit_to_bithumb
        ):
            raise ValueError("At least one transfer route key must be configured when transfer_rebalance_enabled is true")
        if self.transfer_amount_quantum_base < Decimal("0"):
            raise ValueError("transfer_amount_quantum_base must be >= 0")
        if self.transfer_min_amount_base < Decimal("0"):
            raise ValueError("transfer_min_amount_base must be >= 0")
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets.add_or_update(self.bithumb_connector, self.bithumb_trading_pair)
        markets.add_or_update(self.upbit_connector, self.upbit_trading_pair)
        return markets


class UpbitBithumbInventoryRebalanceController(ControllerBase):
    def __init__(self, config: UpbitBithumbInventoryRebalanceControllerConfig, *args, **kwargs):
        self.config = config
        self._state: InventoryRebalanceState = InventoryRebalanceState.IDLE
        self._initial_total_inventory_base: Optional[Decimal] = None
        self._baseline_captured_ts: Optional[float] = None
        self._paused_exchange: Optional[str] = None
        self._transfer_direction: Optional[Direction] = None
        self._active_transfer_request_id: Optional[str] = None
        self._transfer_cycle_id: Optional[str] = None
        self._transfer_event_id: Optional[str] = None
        self._transfer_amount_base: Optional[Decimal] = None
        self._transfer_started_ts: Optional[float] = None
        self._transfer_delay_started_ts: Optional[float] = None
        self._transfer_delay_deadline_ts: Optional[float] = None
        self._transfer_retry_at: Optional[float] = None
        self._transfer_recovery_started_ts: Optional[float] = None
        self._transfer_recheck_count: int = 0
        self._transfer_trigger_balance_threshold: Optional[Decimal] = None
        self._transfer_target_balance_threshold: Optional[Decimal] = None
        self._last_reference_price_quote: Optional[Decimal] = None
        self._controller_last_run_ts: float = 0.0
        self._transfer_last_poll_ts: float = 0.0
        self._transfer_last_qtg_state: Optional[str] = None
        self._transfer_last_error: Optional[str] = None
        self._transfer_task: Optional[asyncio.Task] = None
        self._transfer_poll_task: Optional[asyncio.Task] = None
        self._transfer_client: Optional[TransferGuardClient] = None
        self._transfer_client_ready: bool = False
        self._transfer_lock_owner: str = f"{os.getpid()}:{uuid.uuid4().hex[:8]}:{self._controller_id()}"
        self._transfer_lock_key: Optional[str] = None
        self._transfer_global_lease: Optional[TransferGlobalLease] = None
        self._state_store: Optional[InventoryRebalanceStateStore] = None
        if self.config.transfer_rebalance_enabled and self.config.transfer_global_lock_enabled:
            try:
                self._transfer_global_lease = TransferGlobalLease(
                    lock_dir=self.config.transfer_global_lock_dir_path,
                    ttl_seconds=self.config.transfer_global_lock_ttl_sec,
                )
            except OSError as e:
                self._transfer_last_error = f"Failed to initialize transfer lock directory: {e}"
        self._state_store = InventoryRebalanceStateStore(
            path_template=self.config.state_file_path,
            controller_id=self._controller_id(),
        ) if self.config.transfer_rebalance_enabled else None
        super().__init__(config, *args, **kwargs)
        self._restore_state()

    async def update_processed_data(self):
        now = self.market_data_provider.time()
        update_interval = float(self.config.controller_update_interval_sec)
        if (
            update_interval > 0
            and self._state in {
                InventoryRebalanceState.IDLE,
                InventoryRebalanceState.DELAYING,
                InventoryRebalanceState.COOLDOWN,
            }
            and self._controller_last_run_ts > 0
            and now - self._controller_last_run_ts < update_interval
        ):
            return
        self._controller_last_run_ts = now
        await self._inventory_rebalance_tick()

    def determine_executor_actions(self) -> List[ExecutorAction]:
        return []

    def _controller_id(self) -> str:
        return self.config.id or self.config.controller_name or self.config.asset.lower()

    def _asset(self) -> str:
        return self.config.asset.upper()

    def _quote_asset(self) -> str:
        _, quote = split_hb_trading_pair(self.config.bithumb_trading_pair)
        return quote

    def _bithumb_balance(self) -> Decimal:
        balance = self.market_data_provider.connectors[self.config.bithumb_connector].get_balance(self._asset())
        return balance if balance is not None else Decimal("0")

    def _upbit_balance(self) -> Decimal:
        balance = self.market_data_provider.connectors[self.config.upbit_connector].get_balance(self._asset())
        return balance if balance is not None else Decimal("0")

    def _bithumb_available_balance(self) -> Decimal:
        balance = self.market_data_provider.connectors[self.config.bithumb_connector].get_available_balance(self._asset())
        return balance if balance is not None else Decimal("0")

    def _upbit_available_balance(self) -> Decimal:
        balance = self.market_data_provider.connectors[self.config.upbit_connector].get_available_balance(self._asset())
        return balance if balance is not None else Decimal("0")

    def _current_total_inventory_base(self) -> Decimal:
        return self._bithumb_balance() + self._upbit_balance()

    def _reference_price(self, connector_name: str, trading_pair: str) -> Optional[Decimal]:
        for price_type in [PriceType.MidPrice, PriceType.BestBid, PriceType.BestAsk]:
            price = self.market_data_provider.get_price_by_type(connector_name, trading_pair, price_type)
            if price is not None and price > Decimal("0"):
                return Decimal(str(price))
        return None

    def _blended_reference_price_quote(self) -> Optional[Decimal]:
        prices = [
            self._reference_price(self.config.bithumb_connector, self.config.bithumb_trading_pair),
            self._reference_price(self.config.upbit_connector, self.config.upbit_trading_pair),
        ]
        prices = [price for price in prices if price is not None and price > Decimal("0")]
        if not prices:
            return None
        return sum(prices) / Decimal(len(prices))

    def _inventory_delta_base(self) -> Optional[Decimal]:
        if self._initial_total_inventory_base is None:
            return None
        return self._current_total_inventory_base() - self._initial_total_inventory_base

    def _inventory_delta_pct(self) -> Optional[Decimal]:
        if self._initial_total_inventory_base is None or self._initial_total_inventory_base <= Decimal("0"):
            return None
        delta = self._inventory_delta_base()
        if delta is None:
            return None
        return delta / self._initial_total_inventory_base

    def _inventory_delta_pnl_quote(self) -> Optional[Decimal]:
        delta = self._inventory_delta_base()
        reference_price = self._blended_reference_price_quote()
        if delta is None or reference_price is None:
            return None
        self._last_reference_price_quote = reference_price
        return delta * reference_price

    def _ensure_baseline(self):
        if self._initial_total_inventory_base is not None:
            return
        total = self._current_total_inventory_base()
        if total <= Decimal("0"):
            return
        self._initial_total_inventory_base = total
        self._baseline_captured_ts = self.market_data_provider.time()
        self._save_state()
        self.logger().info(
            f"Captured initial inventory baseline for {self._asset()}: total_base={self._initial_total_inventory_base}"
        )

    def _effective_target_total_inventory_base(self, current_total_inventory_base: Decimal) -> Optional[Decimal]:
        if self._initial_total_inventory_base is None or self._initial_total_inventory_base <= Decimal("0"):
            return None
        if current_total_inventory_base <= Decimal("0"):
            return None
        return min(self._initial_total_inventory_base, current_total_inventory_base)

    def _current_transfer_trigger(self) -> Optional[Dict[str, object]]:
        if not self.config.transfer_rebalance_enabled:
            return None
        if self._initial_total_inventory_base is None or self._initial_total_inventory_base <= Decimal("0"):
            return None

        bithumb_balance = self._bithumb_balance()
        upbit_balance = self._upbit_balance()
        current_total = bithumb_balance + upbit_balance
        if current_total <= Decimal("0"):
            return None

        trigger_threshold = self._initial_total_inventory_base * self.config.trigger_inventory_ratio
        effective_target_total = self._effective_target_total_inventory_base(current_total)
        if effective_target_total is None:
            return None
        target_balance = effective_target_total * self.config.target_inventory_ratio

        candidates: List[Dict[str, object]] = []
        if upbit_balance < trigger_threshold and self.config.transfer_route_key_bithumb_to_upbit:
            candidates.append({
                "direction": "bithumb_to_upbit",
                "route_key": self.config.transfer_route_key_bithumb_to_upbit,
                "source_exchange": self.config.bithumb_connector,
                "destination_exchange": self.config.upbit_connector,
                "destination_balance": upbit_balance,
                "trigger_balance_threshold": trigger_threshold,
                "target_balance_threshold": target_balance,
            })
        if bithumb_balance < trigger_threshold and self.config.transfer_route_key_upbit_to_bithumb:
            candidates.append({
                "direction": "upbit_to_bithumb",
                "route_key": self.config.transfer_route_key_upbit_to_bithumb,
                "source_exchange": self.config.upbit_connector,
                "destination_exchange": self.config.bithumb_connector,
                "destination_balance": bithumb_balance,
                "trigger_balance_threshold": trigger_threshold,
                "target_balance_threshold": target_balance,
            })

        if len(candidates) == 2:
            self.logger().warning(
                f"Both exchanges are below the trigger threshold for {self._asset()}; suppressing transfer until inventory recovers."
            )
            return None
        if not candidates:
            return None
        return candidates[0]

    def _compute_transfer_amount(self, trigger: Dict[str, object]) -> Decimal:
        destination_balance = Decimal(str(trigger["destination_balance"]))
        target_balance_threshold = Decimal(str(trigger["target_balance_threshold"]))
        source_balance = self._source_sendable_balance(str(trigger["direction"]))
        amount = max(Decimal("0"), target_balance_threshold - destination_balance)
        amount = min(amount, source_balance)
        quantum = self.config.transfer_amount_quantum_base
        if quantum > Decimal("0") and amount > Decimal("0"):
            amount = (amount // quantum) * quantum
        if amount < self.config.transfer_min_amount_base:
            return Decimal("0")
        return max(amount, Decimal("0"))

    def _source_balance(self, direction: str) -> Decimal:
        if direction == "bithumb_to_upbit":
            return self._bithumb_balance()
        return self._upbit_balance()

    def _route_key_for_direction(self, direction: Optional[str]) -> Optional[str]:
        if direction == "bithumb_to_upbit":
            return self.config.transfer_route_key_bithumb_to_upbit or None
        if direction == "upbit_to_bithumb":
            return self.config.transfer_route_key_upbit_to_bithumb or None
        return None

    def _source_sendable_balance(self, direction: str) -> Decimal:
        if direction == "bithumb_to_upbit":
            return self._bithumb_available_balance()
        return self._upbit_available_balance()

    def _destination_balance(self, direction: str) -> Decimal:
        if direction == "bithumb_to_upbit":
            return self._upbit_balance()
        return self._bithumb_balance()

    def _clear_transfer_delay_state(self):
        self._transfer_delay_started_ts = None
        self._transfer_delay_deadline_ts = None

    def _is_paused_route_precheck_cooldown(self) -> bool:
        if self._state != InventoryRebalanceState.COOLDOWN:
            return False
        error = (self._transfer_last_error or "").lower()
        return "qtg route paused during precheck:" in error

    def _paused_route_probe_interval(self) -> float:
        if self.config.controller_update_interval_sec > 0:
            return max(1.0, float(self.config.controller_update_interval_sec))
        return min(max(1.0, float(self.config.transfer_request_cooldown_sec)), 60.0)

    async def _refresh_paused_route_cooldown(self, *, now: float) -> bool:
        if self._state != InventoryRebalanceState.COOLDOWN:
            return False
        if self._transfer_client is None:
            return False

        route_key = self._route_key_for_direction(self._transfer_direction)
        if not route_key:
            return False

        route_allowed, route_reason = await self._precheck_transfer_route(route_key=route_key)
        if route_allowed:
            if self._is_paused_route_precheck_cooldown():
                self._state = InventoryRebalanceState.IDLE
                self._clear_transfer_runtime_state()
                self._release_transfer_lock(force=False)
                self._save_state()
                self.logger().info(
                    f"Inventory rebalance paused-route cooldown cleared for {self._asset()}: route={route_key}"
                )
            return False

        if route_reason:
            self._transfer_retry_at = now + self._paused_route_probe_interval()
            self._transfer_last_error = route_reason
            self._save_state()
            return True
        return False

    def _clear_transfer_runtime_state(self):
        self._paused_exchange = None
        self._transfer_direction = None
        self._active_transfer_request_id = None
        self._transfer_cycle_id = None
        self._transfer_event_id = None
        self._transfer_amount_base = None
        self._transfer_started_ts = None
        self._transfer_retry_at = None
        self._transfer_recovery_started_ts = None
        self._transfer_recheck_count = 0
        self._transfer_trigger_balance_threshold = None
        self._transfer_target_balance_threshold = None
        self._transfer_last_qtg_state = None
        self._transfer_last_error = None
        self._clear_transfer_delay_state()

    def _enter_transfer_delay(self, *, trigger: Dict[str, object], now: float):
        self._state = InventoryRebalanceState.DELAYING
        self._paused_exchange = str(trigger["destination_exchange"])
        self._transfer_direction = str(trigger["direction"])
        self._active_transfer_request_id = None
        self._transfer_cycle_id = None
        self._transfer_event_id = None
        self._transfer_amount_base = None
        self._transfer_started_ts = None
        self._transfer_retry_at = None
        self._transfer_recovery_started_ts = None
        self._transfer_recheck_count = 0
        self._transfer_trigger_balance_threshold = Decimal(str(trigger["trigger_balance_threshold"]))
        self._transfer_target_balance_threshold = Decimal(str(trigger["target_balance_threshold"]))
        self._transfer_last_qtg_state = None
        self._transfer_last_error = None
        self._transfer_delay_started_ts = now
        self._transfer_delay_deadline_ts = now + self.config.transfer_delay_before_submit_sec
        self._save_state()
        self.logger().info(
            f"Inventory rebalance delay armed for {self._asset()}: direction={self._transfer_direction}, "
            f"destination={self._paused_exchange}, deadline_in={self.config.transfer_delay_before_submit_sec}s"
        )

    def _complete_transfer_delay(self, *, reason: str):
        self._state = InventoryRebalanceState.IDLE
        self._clear_transfer_runtime_state()
        self._clear_state_file()
        self.logger().info(f"Inventory rebalance delay cleared for {self._asset()}: {reason}")

    def _begin_transfer_submission(self, *, trigger: Dict[str, object], now: float, reason: str) -> bool:
        amount = self._compute_transfer_amount(trigger)
        if amount <= Decimal("0"):
            self._transfer_last_error = "Computed transfer amount is zero after threshold/quantum checks"
            return False
        direction = str(trigger["direction"])
        route_key = str(trigger["route_key"])
        if not self._acquire_transfer_lock(direction):
            return False

        self._state = InventoryRebalanceState.SIGNAL_SUBMITTING
        self._paused_exchange = str(trigger["destination_exchange"])
        self._transfer_direction = direction
        self._active_transfer_request_id = None
        self._transfer_cycle_id = uuid.uuid4().hex[:12]
        self._transfer_event_id = f"{self._controller_id()}:{direction}:{self._transfer_cycle_id}"
        self._transfer_amount_base = amount
        self._transfer_started_ts = now
        self._transfer_retry_at = None
        self._transfer_recovery_started_ts = None
        self._transfer_recheck_count = 0
        self._transfer_trigger_balance_threshold = Decimal(str(trigger["trigger_balance_threshold"]))
        self._transfer_target_balance_threshold = Decimal(str(trigger["target_balance_threshold"]))
        self._transfer_last_qtg_state = None
        self._transfer_last_error = None
        self._clear_transfer_delay_state()
        self._save_state()
        self.logger().info(
            f"Inventory rebalance submission started for {self._asset()}: reason={reason}, direction={direction}, amount={amount}"
        )
        self._transfer_task = asyncio.create_task(self._signal_and_approve(route_key=route_key, amount=amount))
        return True

    async def _inventory_rebalance_tick(self):
        if not self.config.transfer_rebalance_enabled:
            return
        if not self.market_data_provider.ready:
            return
        self._ensure_transfer_runtime_components()
        self._ensure_baseline()
        self._inventory_delta_pnl_quote()
        if not self._ensure_transfer_client():
            return

        now = self.market_data_provider.time()
        self._heartbeat_transfer_lock()

        if self._transfer_task is not None and self._transfer_task.done():
            self._consume_task_result(self._transfer_task, context="transfer_task")
            self._transfer_task = None
        if self._transfer_poll_task is not None and self._transfer_poll_task.done():
            self._consume_task_result(self._transfer_poll_task, context="transfer_poll_task")
            self._transfer_poll_task = None

        if self._state == InventoryRebalanceState.COOLDOWN:
            still_blocked = await self._refresh_paused_route_cooldown(now=now)
            if self._state == InventoryRebalanceState.COOLDOWN:
                if still_blocked:
                    return
                if self._transfer_retry_at is not None and now >= self._transfer_retry_at:
                    self._state = InventoryRebalanceState.IDLE
                    self._paused_exchange = None
                    self._clear_transfer_delay_state()
                    self._release_transfer_lock(force=False)
                    self._save_state()
                else:
                    return

        if self._state == InventoryRebalanceState.ERROR:
            return

        if self._state == InventoryRebalanceState.WAITING_RECOVERY:
            target_balance = self._transfer_target_balance_threshold
            if target_balance is not None and self._transfer_direction is not None:
                destination_balance = self._destination_balance(self._transfer_direction)
                if destination_balance >= target_balance:
                    self._complete_transfer_cycle(reason="recovery_reached")
                    return

            if self._transfer_recovery_started_ts is None:
                self._transfer_recovery_started_ts = now
                self._transfer_retry_at = now + self.config.transfer_recheck_interval_sec
                self._transfer_recheck_count = 0
                self._save_state()
                return

            settle_ready_at = self._transfer_recovery_started_ts + self.config.transfer_settle_wait_sec
            recheck_due_at = self._transfer_recovery_started_ts + (
                self.config.transfer_recheck_interval_sec * (self._transfer_recheck_count + 1)
            )
            if self._transfer_retry_at != recheck_due_at:
                self._transfer_retry_at = recheck_due_at
                self._save_state()

            if now < settle_ready_at or now < recheck_due_at:
                return

            trigger = self._current_transfer_trigger()
            if trigger is None:
                self._complete_transfer_cycle(reason="recovery_recheck_trigger_cleared")
                return

            current_direction = str(trigger["direction"])
            if self._transfer_direction is not None and current_direction != self._transfer_direction:
                self._complete_transfer_cycle(reason="recovery_recheck_direction_changed")
                return

            self._transfer_recheck_count += 1
            if self._transfer_recheck_count < self.config.transfer_recheck_confirmations_required:
                self._transfer_retry_at = self._transfer_recovery_started_ts + (
                    self.config.transfer_recheck_interval_sec * (self._transfer_recheck_count + 1)
                )
                self._transfer_last_error = (
                    f"Recovery recheck {self._transfer_recheck_count}/"
                    f"{self.config.transfer_recheck_confirmations_required}: inventory still below target"
                )
                self._save_state()
                return

            self.logger().info(
                f"Inventory rebalance recovery recheck triggered for {self._asset()}; "
                f"direction={current_direction}, re-submitting transfer after "
                f"{self._transfer_recheck_count} persistent rechecks"
            )
            if self.config.transfer_delay_before_submit_sec > 0:
                self._enter_transfer_delay(trigger=trigger, now=now)
                return
            submission_started = self._begin_transfer_submission(
                trigger=trigger,
                now=now,
                reason="recovery_recheck_due",
            )
            if not submission_started:
                self._complete_transfer_cycle(reason="recovery_recheck_submission_not_started")
            return

        if self._state == InventoryRebalanceState.IN_FLIGHT:
            if self._transfer_started_ts is not None and now - self._transfer_started_ts > self.config.transfer_request_timeout_sec:
                self._enter_error("Transfer request timeout exceeded")
                return
            if (
                self._transfer_poll_task is None
                and now >= self._transfer_last_poll_ts + self.config.transfer_poll_interval_sec
                and self._active_transfer_request_id is not None
            ):
                self._transfer_last_poll_ts = now
                self._transfer_poll_task = asyncio.create_task(self._poll_transfer_request())
            return

        if self._state in {InventoryRebalanceState.SIGNAL_SUBMITTING, InventoryRebalanceState.APPROVAL_SUBMITTING}:
            return

        trigger = self._current_transfer_trigger()

        if self._state == InventoryRebalanceState.DELAYING:
            if trigger is None:
                self._complete_transfer_delay(reason="inventory trigger no longer active")
                return
            current_direction = str(trigger["direction"])
            if self._transfer_direction is not None and current_direction != self._transfer_direction:
                self.logger().info(f"Inventory rebalance delay restarted for {self._asset()} due to direction flip")
                self._enter_transfer_delay(trigger=trigger, now=now)
                return
            if self._transfer_delay_deadline_ts is not None and now >= self._transfer_delay_deadline_ts:
                submission_started = self._begin_transfer_submission(trigger=trigger, now=now, reason="delay_expired")
                if submission_started:
                    self.logger().info(f"Inventory rebalance delay expired for {self._asset()}; submitting transfer")
                else:
                    self._complete_transfer_delay(reason="delay expired but transfer submission could not start")
                return
            return

        if trigger is None:
            return
        if self.config.transfer_delay_before_submit_sec > 0:
            self._enter_transfer_delay(trigger=trigger, now=now)
            return
        self._begin_transfer_submission(trigger=trigger, now=now, reason="threshold_direct")

    async def _signal_and_approve(self, *, route_key: str, amount: Decimal):
        if self._transfer_client is None:
            self._enter_error("TransferGuardClient is not initialized")
            return
        current_time = self.market_data_provider.time()
        try:
            route_allowed, route_reason = await self._precheck_transfer_route(route_key=route_key)
            if not route_allowed:
                self.logger().info(
                    f"Skipping inventory transfer signal because paused-route precheck blocked {route_key}: {route_reason}"
                )
                self._enter_cooldown(
                    reason=route_reason or f"QTG route precheck blocked transfer: {route_key}",
                    now=current_time,
                )
                return
            signal_result: SignalResult = await self._transfer_client.send_signal(
                route_key=route_key,
                amount=amount,
                signal_type="insufficient_balance",
                event_id=self._transfer_event_id or f"{self._controller_id()}:{uuid.uuid4().hex[:12]}",
                callback_url=self.config.transfer_guard_callback_url,
                metadata={
                    "controller_id": self._controller_id(),
                    "asset": self._asset(),
                    "direction": self._transfer_direction,
                    "baseline_inventory_base": str(self._initial_total_inventory_base) if self._initial_total_inventory_base is not None else None,
                },
            )
            self._active_transfer_request_id = signal_result.request_id
            self._transfer_last_qtg_state = signal_result.state

            if signal_result.state == "FAILED":
                self._enter_cooldown(reason=f"QTG signal rejected: {signal_result.reason}", now=current_time)
                return
            if signal_result.state in SUCCESS_STATES:
                self._state = InventoryRebalanceState.WAITING_RECOVERY
                self._transfer_recovery_started_ts = current_time
                self._transfer_retry_at = current_time + self.config.transfer_recheck_interval_sec
                self._transfer_recheck_count = 0
                self._save_state()
                return

            self._state = InventoryRebalanceState.APPROVAL_SUBMITTING
            self._save_state()

            approval_result: ApprovalResult = await self._transfer_client.approve_request(
                request_id=signal_result.request_id,
                approver_id=self._controller_id(),
            )
            self._transfer_last_qtg_state = approval_result.state
            if approval_result.state in SUCCESS_STATES:
                self._state = InventoryRebalanceState.WAITING_RECOVERY
                self._transfer_recovery_started_ts = self.market_data_provider.time()
                self._transfer_retry_at = self._transfer_recovery_started_ts + self.config.transfer_recheck_interval_sec
                self._transfer_recheck_count = 0
            elif approval_result.state in TERMINAL_FAILURE_STATES:
                self._enter_cooldown(
                    reason=f"Approval returned terminal state: {approval_result.state}",
                    now=self.market_data_provider.time(),
                )
                return
            else:
                self._state = InventoryRebalanceState.IN_FLIGHT
            self._save_state()
        except ConflictError:
            await self._resolve_conflicted_request()
        except (RateLimitError, NetworkError, ServerError) as e:
            self._enter_cooldown(reason=f"Transient QTG error during signal/approval: {e}", now=self.market_data_provider.time())
        except (AuthError, NotFoundError) as e:
            self._enter_error(f"Permanent QTG error during signal/approval: {e}")
        except TransferGuardError as e:
            self._enter_cooldown(reason=f"QTG error during signal/approval: {e}", now=self.market_data_provider.time())
        except Exception as e:
            self._enter_error(f"Unexpected transfer signal/approval error: {e}")

    async def _precheck_transfer_route(self, *, route_key: str) -> Tuple[bool, Optional[str]]:
        if not self.config.transfer_route_pause_precheck_enabled:
            return True, None
        if self._transfer_client is None:
            return False, "TransferGuardClient is not initialized"
        route = await self._transfer_client.get_route_by_key(route_key=route_key)
        if route is None:
            return False, f"QTG route not found during paused-route precheck: {route_key}"
        if not route.enabled:
            return False, f"QTG route disabled during paused-route precheck: {route_key}"
        if route.is_paused:
            pause_reason = route.pause_reason or "no pause reason provided"
            return False, f"QTG route paused during precheck: {route_key} ({pause_reason})"
        return True, None

    async def _resolve_conflicted_request(self):
        if self._transfer_client is None or self._active_transfer_request_id is None:
            self._enter_error("Approval conflict without active request id")
            return
        try:
            status = await self._transfer_client.get_request(request_id=self._active_transfer_request_id)
        except TransferGuardError as e:
            self._enter_error(f"Approval conflict and unable to fetch request: {e}")
            return
        self._handle_polled_state(status)

    async def _poll_transfer_request(self):
        if self._transfer_client is None or self._active_transfer_request_id is None:
            return
        try:
            status = await self._transfer_client.get_request(request_id=self._active_transfer_request_id)
        except (RateLimitError, NetworkError, ServerError):
            return
        except NotFoundError as e:
            self._enter_cooldown(reason=f"Active transfer request not found: {e}", now=self.market_data_provider.time())
            return
        except TransferGuardError as e:
            self._enter_cooldown(reason=f"Polling failed: {e}", now=self.market_data_provider.time())
            return
        except Exception as e:
            self._enter_error(f"Unexpected transfer polling error: {e}")
            return
        self._handle_polled_state(status)

    def _handle_polled_state(self, status: RequestStatus):
        self._transfer_last_qtg_state = status.state
        if status.state in SUCCESS_STATES:
            self._state = InventoryRebalanceState.WAITING_RECOVERY
            if self._transfer_recovery_started_ts is None:
                self._transfer_recovery_started_ts = self.market_data_provider.time()
            self._transfer_retry_at = self._transfer_recovery_started_ts + self.config.transfer_recheck_interval_sec
            self._transfer_recheck_count = 0
            self._save_state()
            return
        if status.state in TERMINAL_FAILURE_STATES:
            self._enter_cooldown(reason=f"Transfer terminal failure state: {status.state}", now=self.market_data_provider.time())
            return
        if status.state in UNKNOWN_STATES:
            self._enter_error(f"Transfer entered unknown state requiring manual intervention: {status.state}")
            return
        if status.state in PENDING_STATES or status.state in IN_FLIGHT_STATES:
            self._state = InventoryRebalanceState.IN_FLIGHT
            self._save_state()
            return
        self._enter_error(f"Unexpected QTG request state: {status.state}")

    def _enter_cooldown(self, *, reason: str, now: float):
        self._state = InventoryRebalanceState.COOLDOWN
        self._transfer_retry_at = now + self.config.transfer_request_cooldown_sec
        self._transfer_last_error = reason
        self._active_transfer_request_id = None
        self._transfer_recovery_started_ts = None
        self._paused_exchange = None
        self._clear_transfer_delay_state()
        self._release_transfer_lock(force=False)
        self._save_state()

    def _enter_error(self, reason: str):
        self._state = InventoryRebalanceState.ERROR
        self._transfer_last_error = reason
        self._paused_exchange = None
        self._clear_transfer_delay_state()
        self._release_transfer_lock(force=False)
        self._save_state()
        self.logger().warning(f"Inventory rebalance entered ERROR state for {self._asset()}: {reason}")

    def _complete_transfer_cycle(self, reason: str):
        self._state = InventoryRebalanceState.IDLE
        self._clear_transfer_runtime_state()
        self._release_transfer_lock(force=False)
        self._clear_state_file()
        self.logger().info(f"Inventory rebalance cycle completed for {self._asset()}: {reason}")

    def _ensure_transfer_client(self) -> bool:
        if self._transfer_client_ready and self._transfer_client is not None:
            return True
        signal_secret = os.getenv(self.config.transfer_guard_signal_secret_env, "")
        approval_secret = os.getenv(self.config.transfer_guard_approval_secret_env, "")
        read_secret = os.getenv(self.config.transfer_guard_read_secret_env, "")
        admin_secret = os.getenv(self.config.transfer_guard_admin_secret_env, "")
        if not (
            self.config.transfer_guard_signal_key_id and
            self.config.transfer_guard_approval_key_id and
            self.config.transfer_guard_read_key_id and
            signal_secret and approval_secret and read_secret
        ):
            self._transfer_last_error = "Missing QTG credential configuration for inventory rebalance"
            return False
        if self.config.transfer_route_pause_precheck_enabled and not (
            self.config.transfer_guard_admin_key_id and admin_secret
        ):
            self._transfer_last_error = "Missing QTG admin credential configuration for paused-route precheck"
            return False
        keys = {
            "signal": (self.config.transfer_guard_signal_key_id, signal_secret),
            "approval": (self.config.transfer_guard_approval_key_id, approval_secret),
            "read": (self.config.transfer_guard_read_key_id, read_secret),
        }
        if self.config.transfer_guard_admin_key_id and admin_secret:
            keys["admin"] = (self.config.transfer_guard_admin_key_id, admin_secret)
        self._transfer_client = TransferGuardClient(
            base_url=self.config.transfer_guard_base_url,
            keys=keys,
            timeout_seconds=max(self.config.transfer_poll_interval_sec, 5.0),
        )
        self._transfer_client_ready = True
        return True

    def _ensure_transfer_runtime_components(self):
        if self._state_store is None:
            self._state_store = InventoryRebalanceStateStore(
                path_template=self.config.state_file_path,
                controller_id=self._controller_id(),
            )
        if self.config.transfer_global_lock_enabled and self._transfer_global_lease is None:
            try:
                self._transfer_global_lease = TransferGlobalLease(
                    lock_dir=self.config.transfer_global_lock_dir_path,
                    ttl_seconds=self.config.transfer_global_lock_ttl_sec,
                )
            except OSError as e:
                self._transfer_last_error = f"Failed to initialize transfer lock directory: {e}"

    def _transfer_lock_key_for_direction(self, direction: str) -> str:
        source = self.config.bithumb_connector if direction == "bithumb_to_upbit" else self.config.upbit_connector
        destination = self.config.upbit_connector if direction == "bithumb_to_upbit" else self.config.bithumb_connector
        return f"{self._asset()}:{source}:{destination}"

    def _configured_transfer_lock_keys(self) -> List[str]:
        keys: List[str] = []
        if self.config.transfer_route_key_bithumb_to_upbit:
            keys.append(self._transfer_lock_key_for_direction("bithumb_to_upbit"))
        if self.config.transfer_route_key_upbit_to_bithumb:
            keys.append(self._transfer_lock_key_for_direction("upbit_to_bithumb"))
        return list(dict.fromkeys(keys))

    def _acquire_transfer_lock(self, direction: str) -> bool:
        lock_key = self._transfer_lock_key_for_direction(direction)
        if self.config.transfer_global_lock_enabled and self._transfer_global_lease is None:
            self._transfer_last_error = "transfer_global_lock_enabled=true but lock subsystem is unavailable"
            return False
        if not self.config.transfer_global_lock_enabled:
            self._transfer_lock_key = lock_key
            return True
        if self._transfer_lock_key == lock_key:
            return True
        acquired = self._transfer_global_lease.acquire(lock_key=lock_key, owner_id=self._transfer_lock_owner)
        if acquired:
            self._transfer_lock_key = lock_key
            return True
        payload = self._transfer_global_lease.peek(lock_key=lock_key)
        if payload is not None:
            owner_id = payload.get("owner_id")
            updated_at = payload.get("updated_at")
            age_seconds = None
            if isinstance(updated_at, (int, float)):
                age_seconds = max(0.0, time.time() - float(updated_at))
            owner_text = owner_id if isinstance(owner_id, str) else "unknown-owner"
            age_text = f"{age_seconds:.1f}s" if age_seconds is not None else "unknown-age"
            self._transfer_last_error = f"transfer global lock busy: key={lock_key}, owner={owner_text}, age={age_text}"
        else:
            self._transfer_last_error = f"transfer global lock busy: key={lock_key}"
        self.logger().warning(
            f"Failed to acquire inventory rebalance global lock for {self._asset()}: {self._transfer_last_error}"
        )
        return False

    def _try_reclaim_same_controller_transfer_lock(self, lock_key: str) -> bool:
        if not self.config.transfer_global_lock_enabled or self._transfer_global_lease is None:
            return False
        payload = self._transfer_global_lease.peek(lock_key=lock_key)
        if payload is None:
            return False
        owner_id = payload.get("owner_id")
        if not isinstance(owner_id, str):
            return False
        controller_suffix = f":{self._controller_id()}"
        if not owner_id.endswith(controller_suffix) or owner_id == self._transfer_lock_owner:
            return False
        released = self._transfer_global_lease.release(lock_key=lock_key, owner_id=self._transfer_lock_owner, force=True)
        if released:
            self.logger().warning(
                f"Recovered abandoned inventory rebalance global lock for same controller: {lock_key} (owner={owner_id})"
            )
        return released

    def _recover_same_controller_transfer_locks_on_restore(self):
        if not self.config.transfer_global_lock_enabled or self._transfer_global_lease is None:
            return
        if self._active_transfer_request_id is not None:
            return
        if self._state in {
            InventoryRebalanceState.SIGNAL_SUBMITTING,
            InventoryRebalanceState.APPROVAL_SUBMITTING,
            InventoryRebalanceState.IN_FLIGHT,
            InventoryRebalanceState.WAITING_RECOVERY,
        }:
            return
        for lock_key in self._configured_transfer_lock_keys():
            self._try_reclaim_same_controller_transfer_lock(lock_key)

    def _heartbeat_transfer_lock(self):
        if (
            self.config.transfer_global_lock_enabled and
            self._transfer_global_lease is not None and
            self._transfer_lock_key is not None and
            self._state in {
                InventoryRebalanceState.SIGNAL_SUBMITTING,
                InventoryRebalanceState.APPROVAL_SUBMITTING,
                InventoryRebalanceState.IN_FLIGHT,
                InventoryRebalanceState.WAITING_RECOVERY,
            }
        ):
            self._transfer_global_lease.heartbeat(lock_key=self._transfer_lock_key, owner_id=self._transfer_lock_owner)

    def _release_transfer_lock(self, force: bool):
        if not self.config.transfer_global_lock_enabled or self._transfer_global_lease is None:
            self._transfer_lock_key = None
            return
        if self._transfer_lock_key is None:
            return
        self._transfer_global_lease.release(lock_key=self._transfer_lock_key, owner_id=self._transfer_lock_owner, force=force)
        self._transfer_lock_key = None

    def _save_state(self):
        if self._state_store is None:
            return
        snapshot = InventoryRebalanceSnapshot(
            version=1,
            state=self._state.value,
            initial_total_inventory_base=str(self._initial_total_inventory_base) if self._initial_total_inventory_base is not None else None,
            baseline_captured_ts=self._baseline_captured_ts,
            request_id=self._active_transfer_request_id,
            direction=self._transfer_direction,
            paused_exchange=self._paused_exchange,
            amount=str(self._transfer_amount_base) if self._transfer_amount_base is not None else None,
            cycle_id=self._transfer_cycle_id,
            event_id=self._transfer_event_id,
            transfer_start_ts=self._transfer_started_ts,
            retry_at=self._transfer_retry_at,
            recovery_started_ts=self._transfer_recovery_started_ts,
            trigger_balance_threshold=str(self._transfer_trigger_balance_threshold) if self._transfer_trigger_balance_threshold is not None else None,
            target_balance_threshold=str(self._transfer_target_balance_threshold) if self._transfer_target_balance_threshold is not None else None,
            last_reference_price_quote=str(self._last_reference_price_quote) if self._last_reference_price_quote is not None else None,
            last_error=self._transfer_last_error,
            last_qtg_state=self._transfer_last_qtg_state,
            lock_key=self._transfer_lock_key,
            delay_started_ts=self._transfer_delay_started_ts,
            delay_deadline_ts=self._transfer_delay_deadline_ts,
            recovery_recheck_count=self._transfer_recheck_count,
        )
        try:
            self._state_store.save(snapshot)
        except Exception as e:
            self.logger().warning(f"Failed to persist inventory rebalance state: {e}")

    def _clear_state_file(self):
        if self._state_store is None:
            return
        try:
            self._state_store.clear()
        except Exception as e:
            self.logger().warning(f"Failed to clear inventory rebalance state: {e}")

    def _restore_state(self):
        if not self.config.transfer_rebalance_enabled or self._state_store is None:
            return
        try:
            snapshot = self._state_store.load()
        except Exception as e:
            self.logger().warning(f"Failed to load inventory rebalance state. Entering ERROR state: {e}")
            self._state = InventoryRebalanceState.ERROR
            self._transfer_last_error = f"State file load error: {e}"
            return
        if snapshot is None:
            return
        try:
            self._state = InventoryRebalanceState(snapshot.state)
        except ValueError:
            self._state = InventoryRebalanceState.ERROR
            self._transfer_last_error = f"Invalid inventory rebalance state in snapshot: {snapshot.state}"
            return
        self._initial_total_inventory_base = Decimal(snapshot.initial_total_inventory_base) if snapshot.initial_total_inventory_base is not None else None
        self._baseline_captured_ts = snapshot.baseline_captured_ts
        self._active_transfer_request_id = snapshot.request_id
        self._transfer_direction = snapshot.direction
        self._paused_exchange = snapshot.paused_exchange
        self._transfer_cycle_id = snapshot.cycle_id
        self._transfer_event_id = snapshot.event_id
        self._transfer_amount_base = Decimal(snapshot.amount) if snapshot.amount is not None else None
        self._transfer_started_ts = snapshot.transfer_start_ts
        self._transfer_retry_at = snapshot.retry_at
        self._transfer_recovery_started_ts = snapshot.recovery_started_ts
        self._transfer_recheck_count = snapshot.recovery_recheck_count
        self._transfer_trigger_balance_threshold = Decimal(snapshot.trigger_balance_threshold) if snapshot.trigger_balance_threshold is not None else None
        self._transfer_target_balance_threshold = Decimal(snapshot.target_balance_threshold) if snapshot.target_balance_threshold is not None else None
        self._last_reference_price_quote = Decimal(snapshot.last_reference_price_quote) if snapshot.last_reference_price_quote is not None else None
        self._transfer_last_error = snapshot.last_error
        self._transfer_last_qtg_state = snapshot.last_qtg_state
        self._transfer_lock_key = snapshot.lock_key
        self._transfer_delay_started_ts = snapshot.delay_started_ts
        self._transfer_delay_deadline_ts = snapshot.delay_deadline_ts

        self._recover_same_controller_transfer_locks_on_restore()

        if self._state in {InventoryRebalanceState.SIGNAL_SUBMITTING, InventoryRebalanceState.APPROVAL_SUBMITTING}:
            self._state = InventoryRebalanceState.COOLDOWN
            self._transfer_retry_at = 0.0
            self._paused_exchange = None
            self._clear_transfer_delay_state()
            self._save_state()
            return

        terminal_qtg_state = (
            self._transfer_last_qtg_state is not None and
            self._transfer_last_qtg_state in (SUCCESS_STATES | TERMINAL_FAILURE_STATES)
        )
        if terminal_qtg_state:
            self._release_transfer_lock(force=True)
            self._complete_transfer_cycle(reason="restore_terminal_state_recovered")
            return

        if self._transfer_lock_key and self.config.transfer_global_lock_enabled and self._transfer_global_lease is not None:
            acquired = self._transfer_global_lease.acquire(lock_key=self._transfer_lock_key, owner_id=self._transfer_lock_owner)
            if not acquired:
                terminal_or_unknown_qtg_state = (
                    self._transfer_last_qtg_state is not None and
                    self._transfer_last_qtg_state in (SUCCESS_STATES | TERMINAL_FAILURE_STATES | UNKNOWN_STATES)
                )
                if terminal_or_unknown_qtg_state:
                    self._release_transfer_lock(force=True)
                    self._complete_transfer_cycle(reason="restore_terminal_state_recovered")
                    return
                self._state = InventoryRebalanceState.COOLDOWN
                self._transfer_retry_at = 0.0
                self._transfer_last_error = "Failed to reacquire inventory rebalance global lock during state restore (cooldown retry)"
                self._paused_exchange = None
                self._clear_transfer_delay_state()
                self._save_state()

    def _consume_task_result(self, task: asyncio.Task, context: str):
        try:
            task.result()
        except Exception as e:
            self._enter_error(f"{context} failed: {e}")

    def on_stop(self):
        for task in [self._transfer_task, self._transfer_poll_task]:
            if task is not None and not task.done():
                task.cancel()
        if self._transfer_client is not None:
            asyncio.create_task(self._transfer_client.close())
        self._release_transfer_lock(force=False)

    def to_format_status(self) -> List[str]:
        initial_total = self._initial_total_inventory_base
        current_total = self._current_total_inventory_base()
        delta = self._inventory_delta_base()
        delta_pct = self._inventory_delta_pct()
        delta_pnl = self._inventory_delta_pnl_quote()
        return [
            f"  Asset: {self._asset()} ({self.config.bithumb_connector} {self.config.bithumb_trading_pair} <-> {self.config.upbit_connector} {self.config.upbit_trading_pair})",
            f"  Baseline total inventory: {initial_total}",
            f"  Current inventory: bithumb={self._bithumb_balance()}, upbit={self._upbit_balance()}, total={current_total}",
            f"  Inventory delta: qty={delta}, pct={delta_pct}, pnl_quote={delta_pnl}",
            f"  Transfer state: {self._state.value}, direction={self._transfer_direction}, paused_exchange={self._paused_exchange}",
            f"  Transfer request: id={self._active_transfer_request_id}, qtg_state={self._transfer_last_qtg_state}, amount={self._transfer_amount_base}, retry_at={self._transfer_retry_at}, error={self._transfer_last_error}",
        ]

    def get_custom_info(self) -> dict:
        initial_total = self._initial_total_inventory_base
        current_total = self._current_total_inventory_base()
        delta = self._inventory_delta_base()
        delta_pct = self._inventory_delta_pct()
        delta_pnl_quote = self._inventory_delta_pnl_quote()
        trigger_threshold = initial_total * self.config.trigger_inventory_ratio if initial_total is not None else None
        effective_target_total = self._effective_target_total_inventory_base(current_total)
        target_balance = effective_target_total * self.config.target_inventory_ratio if effective_target_total is not None else None
        delay_remaining = 0.0
        if self._transfer_delay_deadline_ts is not None:
            delay_remaining = max(0.0, self._transfer_delay_deadline_ts - self.market_data_provider.time())
        return {
            "loop_metrics": self.get_loop_metrics(),
            "mode": "inventory_rebalance",
            "asset": self._asset(),
            "quote_asset": self._quote_asset(),
            "initial_total_inventory_base": str(initial_total) if initial_total is not None else None,
            "baseline_captured_ts": self._baseline_captured_ts,
            "current_total_inventory_base": str(current_total),
            "inventory_delta": str(delta) if delta is not None else None,
            "inventory_delta_base": str(delta) if delta is not None else None,
            "inventory_delta_pct": str(delta_pct) if delta_pct is not None else None,
            "inventory_delta_pnl_quote": str(delta_pnl_quote) if delta_pnl_quote is not None else None,
            "bithumb_inventory_base": str(self._bithumb_balance()),
            "upbit_inventory_base": str(self._upbit_balance()),
            "bithumb_available_balance": str(self._bithumb_available_balance()),
            "upbit_available_balance": str(self._upbit_available_balance()),
            "bithumb_inventory_ratio": (
                str(self._bithumb_balance() / initial_total) if initial_total is not None and initial_total > Decimal("0") else None
            ),
            "upbit_inventory_ratio": (
                str(self._upbit_balance() / initial_total) if initial_total is not None and initial_total > Decimal("0") else None
            ),
            "trigger_inventory_ratio": str(self.config.trigger_inventory_ratio),
            "target_inventory_ratio": str(self.config.target_inventory_ratio),
            "trigger_balance_threshold_base": str(trigger_threshold) if trigger_threshold is not None else None,
            "target_balance_threshold_base": str(target_balance) if target_balance is not None else None,
            "transfer_state": self._state.value,
            "transfer_direction": self._transfer_direction,
            "transfer_paused_exchange": self._paused_exchange,
            "transfer_request_id": self._active_transfer_request_id,
            "transfer_last_qtg_state": self._transfer_last_qtg_state,
            "transfer_amount_base": str(self._transfer_amount_base) if self._transfer_amount_base is not None else None,
            "transfer_last_error": self._transfer_last_error,
            "transfer_retry_at": self._transfer_retry_at,
            "transfer_delay_active": self._state == InventoryRebalanceState.DELAYING,
            "transfer_delay_started_ts": self._transfer_delay_started_ts,
            "transfer_delay_deadline_ts": self._transfer_delay_deadline_ts,
            "transfer_delay_remaining_sec": delay_remaining,
            "transfer_settle_wait_sec": self.config.transfer_settle_wait_sec,
            "transfer_recheck_interval_sec": self.config.transfer_recheck_interval_sec,
            "transfer_recheck_confirmations_required": self.config.transfer_recheck_confirmations_required,
            "transfer_recheck_count": self._transfer_recheck_count,
            "transfer_event_id": self._transfer_event_id,
            "transfer_cycle_id": self._transfer_cycle_id,
            "transfer_lock_key": self._transfer_lock_key,
            "last_reference_price_quote": str(self._last_reference_price_quote) if self._last_reference_price_quote is not None else None,
            "active_executors_total": len([executor for executor in self.executors_info if not executor.is_done]),
        }
