from __future__ import annotations

from decimal import Decimal

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)


def build_in_flight_order(
    client_order_id: str,
    trading_pair: str = "ETH-USDT",
    trade_type: TradeType = TradeType.BUY,
    order_type: OrderType = OrderType.LIMIT,
    amount: Decimal = Decimal("1"),
    price: Decimal = Decimal("100"),
    creation_timestamp: float = 1234,
    initial_state: OrderState = OrderState.OPEN,
) -> InFlightOrder:
    return InFlightOrder(
        creation_timestamp=creation_timestamp,
        trading_pair=trading_pair,
        client_order_id=client_order_id,
        order_type=order_type,
        trade_type=trade_type,
        amount=amount,
        price=price,
        initial_state=initial_state,
    )


def build_order_filled_event(
    order_id: str,
    trading_pair: str = "ETH-USDT",
    trade_type: TradeType = TradeType.BUY,
    order_type: OrderType = OrderType.LIMIT,
    price: Decimal = Decimal("100"),
    amount: Decimal = Decimal("1"),
    timestamp: float = 1234.0,
    trade_fee=None,
) -> OrderFilledEvent:
    return OrderFilledEvent(
        timestamp=timestamp,
        order_id=order_id,
        trading_pair=trading_pair,
        trade_type=trade_type,
        order_type=order_type,
        price=price,
        amount=amount,
        trade_fee=trade_fee,
    )


def build_order_cancelled_event(
    order_id: str,
    timestamp: float = 1234.0,
    exchange_order_id: str = "exchange-order-id",
) -> OrderCancelledEvent:
    return OrderCancelledEvent(
        timestamp=timestamp,
        order_id=order_id,
        exchange_order_id=exchange_order_id,
    )


def build_order_completed_event(
    side: TradeType,
    order_id: str,
    base_asset_amount: Decimal,
    quote_asset_amount: Decimal,
    base_asset: str = "ETH",
    quote_asset: str = "USDT",
    order_type: OrderType = OrderType.LIMIT,
    timestamp: float = 1234.0,
):
    event_cls = BuyOrderCompletedEvent if side == TradeType.BUY else SellOrderCompletedEvent
    return event_cls(
        base_asset=base_asset,
        quote_asset=quote_asset,
        base_asset_amount=base_asset_amount,
        quote_asset_amount=quote_asset_amount,
        order_type=order_type,
        timestamp=timestamp,
        order_id=order_id,
    )
