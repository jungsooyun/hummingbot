from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from hummingbot.core.data_type.common import TradeType


@dataclass(frozen=True)
class OpenOrderRecord:
    """Exchange-agnostic open-order record returned by connector.get_open_orders().
    The B-C contract (JEP-170 -> JEP-171 seed). Home verified-at-implementation with
    Spec C author; re-home if C lands a seed-side type first.
    """
    trading_pair: str
    exchange_order_id: str
    client_order_id: Optional[str]
    trade_type: TradeType
    price: Decimal
    amount: Decimal
    remaining_amount: Decimal
