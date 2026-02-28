from decimal import Decimal
from typing import Literal

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class XEMMExecutorConfig(ExecutorConfigBase):
    type: Literal["xemm_executor"] = "xemm_executor"
    buying_market: ConnectorPair
    selling_market: ConnectorPair
    maker_side: TradeType
    order_amount: Decimal
    min_profitability: Decimal
    target_profitability: Decimal
    max_profitability: Decimal
    taker_order_type: OrderType = OrderType.MARKET
    taker_slippage_buffer_bps: Decimal = Decimal("0")
    taker_order_max_age_seconds: float = 4.0
    taker_max_retries: int = 3
    taker_fallback_to_market: bool = False
