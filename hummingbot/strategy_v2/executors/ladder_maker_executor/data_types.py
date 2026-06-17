from decimal import Decimal
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class LadderRungConfig(BaseModel):
    """One ladder level. edge_bps = distance from fair, size = base order size,
    min_edge_bps = floor distance (never quote closer than this)."""

    edge_bps: Decimal
    size: Decimal
    min_edge_bps: Decimal = Decimal("0")
    enabled: bool = True


class LadderMakerExecutorConfig(ExecutorConfigBase):
    type: Literal["ladder_maker_executor"] = "ladder_maker_executor"

    # Markets: maker leg on a perp (e.g. hyperliquid_perpetual HIP-3), hedge leg on KIS spot
    maker_market: ConnectorPair
    hedge_market: ConnectorPair

    # Maker side on the perp. Fills are hedged on the opposite side on KIS spot.
    entry_side: TradeType

    # Ladder
    total_size_cap: Decimal
    rungs: List[LadderRungConfig]
    maker_tick: Decimal
    hedge_tick: Decimal
    buffer_ticks: Decimal = Decimal("0")

    # Fair price inputs (KIS spot KRW -> USD via FX)
    fx_connector: Optional[str] = None
    fx_trading_pair: Optional[str] = None
    static_fx_rate: Optional[Decimal] = None
    side_aware_fx: bool = True

    # Inventory skew / gate
    inventory_skew_bps_per_unit: Decimal = Decimal("0")
    target_inventory: Decimal = Decimal("0")
    max_inventory: Optional[Decimal] = None

    # Hedge (KIS spot marketable limit)
    share_per_unit: Decimal = Decimal("1")
    hedge_max_slippage_bps: Decimal = Decimal("30")
    hedge_order_type: OrderType = OrderType.LIMIT

    # Reprice guards
    min_reprice_interval_s: float = 0.75
    min_reprice_delta_ticks: Decimal = Decimal("2")

    # Perp leverage (HIP-3 markets: isolated only)
    leverage: int = 1

    # Safety
    kill_switch: bool = False
    # Observe / no-submit: compute fair + targets and LOG the intended maker quotes
    # but never call place_order (no real orders, so no fills and no hedges). For
    # safe live verification of the full decision path before enabling submission.
    observe: bool = False

    # Display / bookkeeping
    execution_purpose: str = Field(default="ladder_market_making")
