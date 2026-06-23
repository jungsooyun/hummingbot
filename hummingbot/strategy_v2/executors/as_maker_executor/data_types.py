"""Config for the Lane 2 A&S maker executor (Phase 3b)."""
from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import Field

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase


class AsMakerExecutorConfig(ExecutorConfigBase):
    type: Literal["as_maker_executor"] = "as_maker_executor"

    # Maker (passive) leg venue.
    connector_name: str
    trading_pair: str
    # Nominal maker entry side for base compatibility (A&S quotes BOTH sides).
    entry_side: TradeType = TradeType.BUY

    # A&S parameters (fed to as_policy)
    gamma: Decimal = Field(gt=0)
    kappa: Decimal = Field(gt=0)
    tau: Decimal = Field(default=Decimal("1"), gt=0)
    eta: Decimal = Field(default=Decimal("0"), ge=0)
    min_spread_pct: Decimal = Field(default=Decimal("0"), ge=0)
    order_amount: Decimal = Field(gt=0)
    target_inventory: Decimal = Decimal("0")       # any sign (set-point)
    max_inventory: Decimal = Field(gt=0)           # q normalizer + hard-cap backstop

    # σ indicator buffer lengths (RingBuffer modulos by length; length 0 crashes,
    # sampling needs >=2 for an inter-tick diff)
    volatility_sampling_length: int = Field(default=30, ge=2)
    volatility_processing_length: int = Field(default=15, ge=1)

    # Price quantization + reprice guards
    maker_tick: Decimal = Field(gt=0)
    min_reprice_interval_s: float = 0.75
    min_reprice_delta_ticks: Decimal = Decimal("2")

    # Perp
    leverage: int = 1

    # Hedge (aggressive) leg venue — distinct perp connector/pair (JEP-205 3c).
    hedge_connector_name: str
    hedge_trading_pair: str
    hedge_leverage: int = 1
    hedge_max_slippage_bps: Decimal = Field(default=Decimal("5"), ge=0)
    hedge_tick: Decimal = Field(default=Decimal("0.1"), gt=0)
    hedge_order_type: OrderType = OrderType.LIMIT

    # Safety / lifecycle
    kill_switch: bool = False
    observe: bool = True                           # default OBSERVE (real submission is 3d-gated)
    adopt_existing_inventory: bool = False
    latency_profiling: bool = False
    execution_purpose: str = Field(default="as_market_making")
