from decimal import Decimal
from typing import Literal

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class InventoryRebalanceExecutorConfig(ExecutorConfigBase):
    type: Literal["inventory_rebalance_executor"] = "inventory_rebalance_executor"
    entry_market: ConnectorPair
    hedge_market: ConnectorPair
    entry_side: TradeType
    entry_style: Literal["passive", "aggressive"] = "passive"
    order_amount: Decimal
    inventory_delta_before: Decimal
    target_inventory_delta_after: Decimal
    expected_pnl_quote: Decimal
    entry_price_refresh_pct: Decimal = Decimal("0.0002")
    entry_order_max_age_seconds: float = 30.0
    hedge_order_type: OrderType = OrderType.LIMIT
    hedge_slippage_buffer_bps: Decimal = Decimal("0")
    hedge_max_retries: int = 3
    hedge_fallback_to_market: bool = False
    execution_purpose: Literal["inventory_rebalance"] = "inventory_rebalance"
