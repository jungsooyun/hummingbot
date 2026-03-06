from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "upbit"

REST_URL = "https://api.upbit.com"
WSS_PUBLIC_URL = "wss://api.upbit.com/websocket/v1"
WSS_PRIVATE_URL = "wss://api.upbit.com/websocket/v1/private"
WS_HEARTBEAT_TIME_INTERVAL = 30.0

ORDERBOOK_EVENT_TYPE = "orderbook"
TRADE_EVENT_TYPE = "trade"
DEFAULT_ORDERBOOK_DEPTH = 15

PRIVATE_ORDER_CHANNEL_NAME = "myOrder"
PRIVATE_ASSET_CHANNEL_NAME = "myAsset"

DEFAULT_DOMAIN = ""
MAX_ORDER_ID_LEN = 40
HBOT_ORDER_ID_PREFIX = ""

CHECK_NETWORK_PATH_URL = "v1/market/all"
GET_TRADING_RULES_PATH_URL = "v1/market/all"
GET_LAST_TRADING_PRICES_PATH_URL = "v1/ticker"
GET_ORDER_BOOK_PATH_URL = "v1/orderbook"
GET_RECENT_TRADES_PATH_URL = "v1/trades/ticks"

CREATE_ORDER_PATH_URL = "v1/orders"
CANCEL_ORDER_PATH_URL = "v1/order"
GET_ACCOUNT_SUMMARY_PATH_URL = "v1/accounts"
GET_ORDER_DETAIL_PATH_URL = "v1/order"
GET_OPEN_ORDERS_PATH_URL = "v1/orders/open"

WS_CONNECT = "WS_CONNECT"
WS_SUBSCRIBE = "WS_SUBSCRIBE"

RATE_LIMITS = [
    RateLimit(limit_id=CHECK_NETWORK_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_TRADING_RULES_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_LAST_TRADING_PRICES_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_ORDER_BOOK_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_RECENT_TRADES_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CREATE_ORDER_PATH_URL, limit=8, time_interval=1),
    RateLimit(limit_id=CANCEL_ORDER_PATH_URL, limit=8, time_interval=1),
    RateLimit(limit_id=GET_ACCOUNT_SUMMARY_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_ORDER_DETAIL_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=GET_OPEN_ORDERS_PATH_URL, limit=10, time_interval=1),
    RateLimit(limit_id=WS_CONNECT, limit=30, time_interval=60),
    RateLimit(limit_id=WS_SUBSCRIBE, limit=100, time_interval=10),
]

ORDER_STATE = {
    "wait": OrderState.OPEN,
    "watch": OrderState.OPEN,
    "done": OrderState.FILLED,
    "cancel": OrderState.CANCELED,
}
