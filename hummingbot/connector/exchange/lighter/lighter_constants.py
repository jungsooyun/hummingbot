# A single source of truth for constant variables related to the Lighter DEX exchange

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "lighter"

# Base URLs
REST_URL = "https://mainnet.zklighter.elliot.ai"
WSS_PUBLIC_URL = "wss://mainnet.zklighter.elliot.ai/stream"
WSS_PRIVATE_URL = "wss://mainnet.zklighter.elliot.ai/stream"  # same endpoint, auth via token

DEFAULT_DOMAIN = ""
MAX_ORDER_ID_LEN = 32
HBOT_ORDER_ID_PREFIX = ""

# REST Endpoints
CHECK_NETWORK_PATH_URL = "api/v1/orderBooks"
GET_TRADING_RULES_PATH_URL = "api/v1/orderBooks"
GET_LAST_TRADING_PRICES_PATH_URL = "api/v1/orderBookDetails"
GET_ORDER_BOOK_PATH_URL = "api/v1/orderBookOrders"
CREATE_ORDER_PATH_URL = "api/v1/sendTx"
CANCEL_ORDER_PATH_URL = "api/v1/sendTx"
GET_ACCOUNT_SUMMARY_PATH_URL = "api/v1/account"
GET_ORDER_DETAIL_PATH_URL = "api/v1/accountActiveOrders"
GET_INACTIVE_ORDERS_PATH_URL = "api/v1/accountInactiveOrders"
GET_TRADE_DETAIL_PATH_URL = "api/v1/trades"
GET_RECENT_TRADES_PATH_URL = "api/v1/recentTrades"

# WebSocket Channels
PUBLIC_TRADE_CHANNEL_NAME = "trade"
PUBLIC_DEPTH_CHANNEL_NAME = "order_book"
PUBLIC_TICKER_CHANNEL_NAME = "ticker"
PRIVATE_ORDER_CHANNEL_NAME = "account_orders"
PRIVATE_TRADE_CHANNEL_NAME = "account_trades"
PRIVATE_ACCOUNT_CHANNEL_NAME = "account_all"

# Rate Limit IDs
WS_CONNECT = "WS_CONNECT"
WS_SUBSCRIBE = "WS_SUBSCRIBE"

# Lighter has generous rate limits
RATE_LIMITS = [
    RateLimit(limit_id=CHECK_NETWORK_PATH_URL, limit=30, time_interval=1),
    RateLimit(limit_id=GET_TRADING_RULES_PATH_URL, limit=30, time_interval=1),
    RateLimit(limit_id=GET_LAST_TRADING_PRICES_PATH_URL, limit=30, time_interval=1),
    RateLimit(limit_id=GET_ORDER_BOOK_PATH_URL, limit=30, time_interval=1),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_ACCOUNT_SUMMARY_PATH_URL, limit=30, time_interval=1),
    RateLimit(limit_id=GET_ORDER_DETAIL_PATH_URL, limit=30, time_interval=1),
    RateLimit(limit_id=GET_INACTIVE_ORDERS_PATH_URL, limit=30, time_interval=1),
    RateLimit(limit_id=GET_TRADE_DETAIL_PATH_URL, limit=30, time_interval=1),
    RateLimit(limit_id=GET_RECENT_TRADES_PATH_URL, limit=30, time_interval=1),
    RateLimit(limit_id=WS_CONNECT, limit=30, time_interval=60),
    RateLimit(limit_id=WS_SUBSCRIBE, limit=100, time_interval=10),
]

# Order States (from Lighter API)
ORDER_STATE = {
    "open": OrderState.OPEN,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "canceled": OrderState.CANCELED,
    "expired": OrderState.CANCELED,
}
