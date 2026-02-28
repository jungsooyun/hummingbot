# A single source of truth for constant variables related to the KIS exchange

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "kis"

# Base URLs
REST_URL = "https://openapi.koreainvestment.com:9443"
REST_SANDBOX_URL = "https://openapivts.koreainvestment.com:29443"

DEFAULT_DOMAIN = ""
MAX_ORDER_ID_LEN = 32
HBOT_ORDER_ID_PREFIX = ""

# --------------------------------------------------------------------------- #
# Auth (OAuth2)
# --------------------------------------------------------------------------- #
TOKEN_PATH_URL = "oauth2/tokenP"

# --------------------------------------------------------------------------- #
# Domestic Stock (국내 주식) — KOSPI / KOSDAQ
# --------------------------------------------------------------------------- #
DOMESTIC_STOCK_TICKER_TR_ID = "FHPST01010000"
DOMESTIC_STOCK_TICKER_PATH = "uapi/domestic-stock/v1/quotations/inquire-price-2"
DOMESTIC_STOCK_ORDERBOOK_TR_ID = "FHKST01010200"
DOMESTIC_STOCK_ORDERBOOK_PATH = "uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"

# --------------------------------------------------------------------------- #
# Domestic Futures / Options (국내 선물옵션)
# --------------------------------------------------------------------------- #
DOMESTIC_FUTURES_TICKER_TR_ID = "FHMIF10000000"
DOMESTIC_FUTURES_TICKER_PATH = "uapi/domestic-futureoption/v1/quotations/inquire-price"
DOMESTIC_FUTURES_ORDERBOOK_TR_ID = "FHMIF10010000"
DOMESTIC_FUTURES_ORDERBOOK_PATH = "uapi/domestic-futureoption/v1/quotations/inquire-asking-price"

# --------------------------------------------------------------------------- #
# Overseas Stock (해외 주식) — NASDAQ, NYSE, AMEX, etc.
# --------------------------------------------------------------------------- #
OVERSEAS_STOCK_TICKER_TR_ID = "HHDFS00000300"
OVERSEAS_STOCK_TICKER_PATH = "uapi/overseas-price/v1/quotations/price"
OVERSEAS_STOCK_ORDERBOOK_TR_ID = "HHDFS76200100"
OVERSEAS_STOCK_ORDERBOOK_PATH = "uapi/overseas-price/v1/quotations/inquire-asking-price"

# --------------------------------------------------------------------------- #
# Overseas Futures (해외 선물)
# --------------------------------------------------------------------------- #
OVERSEAS_FUTURES_TICKER_TR_ID = "HHDFC55010000"
OVERSEAS_FUTURES_TICKER_PATH = "uapi/overseas-futureoption/v1/quotations/inquire-price"
OVERSEAS_FUTURES_ORDERBOOK_TR_ID = "HHDFC86000000"
OVERSEAS_FUTURES_ORDERBOOK_PATH = "uapi/overseas-futureoption/v1/quotations/inquire-asking-price"

# --------------------------------------------------------------------------- #
# Overseas Options (해외 옵션)
# --------------------------------------------------------------------------- #
OVERSEAS_OPTIONS_TICKER_TR_ID = "HHDFO55010000"
OVERSEAS_OPTIONS_TICKER_PATH = "uapi/overseas-futureoption/v1/quotations/opt-price"
OVERSEAS_OPTIONS_ORDERBOOK_TR_ID = "HHDFO86000000"
OVERSEAS_OPTIONS_ORDERBOOK_PATH = "uapi/overseas-futureoption/v1/quotations/opt-asking-price"

# --------------------------------------------------------------------------- #
# Domestic Stock Trading (국내 주식 주문)
# --------------------------------------------------------------------------- #
DOMESTIC_STOCK_ORDER_PATH = "uapi/domestic-stock/v1/trading/order-cash"
DOMESTIC_STOCK_ORDER_BUY_TR_ID = "TTTC0802U"
DOMESTIC_STOCK_ORDER_SELL_TR_ID = "TTTC0801U"
DOMESTIC_STOCK_CANCEL_TR_ID = "TTTC0803U"
DOMESTIC_STOCK_BALANCE_PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
DOMESTIC_STOCK_BALANCE_TR_ID = "TTTC8434R"
DOMESTIC_STOCK_ORDER_DETAIL_PATH = "uapi/domestic-stock/v1/trading/inquire-daily-ccld"
DOMESTIC_STOCK_ORDER_DETAIL_TR_ID = "TTTC8001R"

# --------------------------------------------------------------------------- #
# Network check — reuse token endpoint for lightweight ping
# --------------------------------------------------------------------------- #
CHECK_NETWORK_PATH_URL = TOKEN_PATH_URL

# --------------------------------------------------------------------------- #
# Market types
# --------------------------------------------------------------------------- #


class KisMarketType:
    DOMESTIC_STOCK = "domestic_stock"
    DOMESTIC_FUTURES = "domestic_futures"
    OVERSEAS_STOCK = "overseas_stock"
    OVERSEAS_FUTURES = "overseas_futures"
    OVERSEAS_OPTIONS = "overseas_options"


# --------------------------------------------------------------------------- #
# Rate Limits — KIS allows ~20 requests/sec per TR_ID
# --------------------------------------------------------------------------- #
RATE_LIMITS = [
    # Token refresh (conservative)
    RateLimit(limit_id=TOKEN_PATH_URL, limit=1, time_interval=60),
    # Domestic Stock
    RateLimit(limit_id=DOMESTIC_STOCK_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Domestic Futures/Options
    RateLimit(limit_id=DOMESTIC_FUTURES_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=DOMESTIC_FUTURES_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Overseas Stock
    RateLimit(limit_id=OVERSEAS_STOCK_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_STOCK_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Overseas Futures
    RateLimit(limit_id=OVERSEAS_FUTURES_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_FUTURES_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Overseas Options
    RateLimit(limit_id=OVERSEAS_OPTIONS_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_OPTIONS_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Domestic Stock Trading
    RateLimit(limit_id=DOMESTIC_STOCK_ORDER_BUY_TR_ID, limit=5, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_ORDER_SELL_TR_ID, limit=5, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_CANCEL_TR_ID, limit=5, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_BALANCE_TR_ID, limit=10, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_ORDER_DETAIL_TR_ID, limit=10, time_interval=1),
]

# --------------------------------------------------------------------------- #
# Order States (for future trading support)
# --------------------------------------------------------------------------- #
ORDER_STATE = {
    "filled": OrderState.FILLED,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "canceled": OrderState.CANCELED,
    "new": OrderState.OPEN,
    "failed": OrderState.FAILED,
}
