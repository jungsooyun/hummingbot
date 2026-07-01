from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType


@dataclass(frozen=True)
class HyperliquidBatchOrderRequest:
    trading_pair: str
    amount: Decimal
    trade_type: TradeType
    order_type: OrderType
    price: Decimal
    position_action: PositionAction = PositionAction.NIL
    metadata: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class HyperliquidBatchCancelRequest:
    client_order_id: str
    trading_pair: str


@dataclass(frozen=True)
class HyperliquidBatchModifyRequest:
    client_order_id: str
    trading_pair: str
    price: Decimal
    amount: Decimal
    trade_type: TradeType
    order_type: OrderType
    position_action: PositionAction
    metadata: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class HyperliquidBatchModifyResult:
    submitted_order_ids: list[str]
    accepted_order_ids: list[str]
    rejected_order_ids: list[str]
    ambiguous_order_ids: list[str]
