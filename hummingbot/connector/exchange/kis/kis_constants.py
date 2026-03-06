# A single source of truth for constant variables related to the KIS exchange

from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "kis"

# Base URLs — REST
REST_URL = "https://openapi.koreainvestment.com:9443"
REST_SANDBOX_URL = "https://openapivts.koreainvestment.com:29443"

# Base URLs — WebSocket
WS_URL = "ws://ops.koreainvestment.com:21000/tryitout"
WS_SANDBOX_URL = "ws://ops.koreainvestment.com:31000/tryitout"

DEFAULT_DOMAIN = ""
MAX_ORDER_ID_LEN = 32
HBOT_ORDER_ID_PREFIX = ""

# --------------------------------------------------------------------------- #
# Auth (OAuth2)
# --------------------------------------------------------------------------- #
TOKEN_PATH_URL = "oauth2/tokenP"
WS_APPROVAL_PATH_URL = "oauth2/Approval"

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
# WebSocket TR_IDs — Domestic Stock
# --------------------------------------------------------------------------- #
WS_DOMESTIC_STOCK_ORDERBOOK_TR_ID = "H0STASP0"   # 국내주식 실시간호가 (KRX)
WS_DOMESTIC_STOCK_TRADE_TR_ID = "H0STCNT0"        # 국내주식 실시간체결가 (KRX)
WS_DOMESTIC_STOCK_EXEC_NOTICE_TR_ID = "H0STCNI0"  # 국내주식 체결통보 (실전)
WS_DOMESTIC_STOCK_EXEC_NOTICE_SANDBOX_TR_ID = "H0STCNI9"  # 체결통보 (모의)

# --------------------------------------------------------------------------- #
# WebSocket Column Definitions
# --------------------------------------------------------------------------- #

# H0STASP0 — 국내주식 실시간호가 (KRX) — 59 fields
WS_ORDERBOOK_COLUMNS = (
    "MKSC_SHRN_ISCD", "BSOP_HOUR", "HOUR_CLS_CODE",
    "ASKP1", "ASKP2", "ASKP3", "ASKP4", "ASKP5",
    "ASKP6", "ASKP7", "ASKP8", "ASKP9", "ASKP10",
    "BIDP1", "BIDP2", "BIDP3", "BIDP4", "BIDP5",
    "BIDP6", "BIDP7", "BIDP8", "BIDP9", "BIDP10",
    "ASKP_RSQN1", "ASKP_RSQN2", "ASKP_RSQN3", "ASKP_RSQN4", "ASKP_RSQN5",
    "ASKP_RSQN6", "ASKP_RSQN7", "ASKP_RSQN8", "ASKP_RSQN9", "ASKP_RSQN10",
    "BIDP_RSQN1", "BIDP_RSQN2", "BIDP_RSQN3", "BIDP_RSQN4", "BIDP_RSQN5",
    "BIDP_RSQN6", "BIDP_RSQN7", "BIDP_RSQN8", "BIDP_RSQN9", "BIDP_RSQN10",
    "TOTAL_ASKP_RSQN", "TOTAL_BIDP_RSQN", "OVTM_TOTAL_ASKP_RSQN", "OVTM_TOTAL_BIDP_RSQN",
    "ANTC_CNPR", "ANTC_CNQN", "ANTC_VOL", "ANTC_CNTG_VRSS", "ANTC_CNTG_VRSS_SIGN",
    "ANTC_CNTG_PRDY_CTRT", "ACML_VOL", "TOTAL_ASKP_RSQN_ICDC", "TOTAL_BIDP_RSQN_ICDC",
    "OVTM_TOTAL_ASKP_ICDC", "OVTM_TOTAL_BIDP_ICDC", "STCK_DEAL_CLS_CODE",
)

# H0STCNT0 — 국내주식 실시간체결가 (KRX) — 46 fields
WS_TRADE_COLUMNS = (
    "MKSC_SHRN_ISCD", "STCK_CNTG_HOUR", "STCK_PRPR", "PRDY_VRSS_SIGN",
    "PRDY_VRSS", "PRDY_CTRT", "WGHN_AVRG_STCK_PRC", "STCK_OPRC",
    "STCK_HGPR", "STCK_LWPR", "ASKP1", "BIDP1", "CNTG_VOL", "ACML_VOL",
    "ACML_TR_PBMN", "SELN_CNTG_CSNU", "SHNU_CNTG_CSNU", "NTBY_CNTG_CSNU",
    "CTTR", "SELN_CNTG_SMTN", "SHNU_CNTG_SMTN", "CCLD_DVSN", "SHNU_RATE",
    "PRDY_VOL_VRSS_ACML_VOL_RATE", "OPRC_HOUR", "OPRC_VRSS_PRPR_SIGN",
    "OPRC_VRSS_PRPR", "HGPR_HOUR", "HGPR_VRSS_PRPR_SIGN", "HGPR_VRSS_PRPR",
    "LWPR_HOUR", "LWPR_VRSS_PRPR_SIGN", "LWPR_VRSS_PRPR", "BSOP_DATE",
    "NEW_MKOP_CLS_CODE", "TRHT_YN", "ASKP_RSQN1", "BIDP_RSQN1",
    "TOTAL_ASKP_RSQN", "TOTAL_BIDP_RSQN", "VOL_TNRT",
    "PRDY_SMNS_HOUR_ACML_VOL", "PRDY_SMNS_HOUR_ACML_VOL_RATE",
    "HOUR_CLS_CODE", "MRKT_TRTM_CLS_CODE", "VI_STND_PRC",
)

# H0STCNI0 — 국내주식 체결통보 — 26 fields (encrypted)
# CNTG_YN: "2" = 체결통보, "1" = 주문·정정·취소·거부 접수통보
WS_EXEC_NOTICE_COLUMNS = (
    "CUST_ID", "ACNT_NO", "ODER_NO", "OODER_NO", "SELN_BYOV_CLS", "RCTF_CLS",
    "ODER_KIND", "ODER_COND", "STCK_SHRN_ISCD", "CNTG_QTY", "CNTG_UNPR",
    "STCK_CNTG_HOUR", "RFUS_YN", "CNTG_YN", "ACPT_YN", "BRNC_NO", "ODER_QTY",
    "ACNT_NAME", "ORD_COND_PRC", "ORD_EXG_GB", "POPUP_YN", "FILLER", "CRDT_CLS",
    "CRDT_LOAN_DATE", "CNTG_ISNM40", "ODER_PRC",
)

# Max concurrent WS subscriptions per connection
WS_MAX_SUBSCRIPTIONS = 40

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
    # Domestic Stock — by TR_ID
    RateLimit(limit_id=DOMESTIC_STOCK_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Domestic Futures/Options — by TR_ID
    RateLimit(limit_id=DOMESTIC_FUTURES_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=DOMESTIC_FUTURES_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Overseas Stock — by TR_ID
    RateLimit(limit_id=OVERSEAS_STOCK_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_STOCK_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Overseas Futures — by TR_ID
    RateLimit(limit_id=OVERSEAS_FUTURES_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_FUTURES_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Overseas Options — by TR_ID
    RateLimit(limit_id=OVERSEAS_OPTIONS_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_OPTIONS_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Domestic Stock Trading — by TR_ID
    RateLimit(limit_id=DOMESTIC_STOCK_ORDER_BUY_TR_ID, limit=5, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_ORDER_SELL_TR_ID, limit=5, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_CANCEL_TR_ID, limit=5, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_BALANCE_TR_ID, limit=10, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_ORDER_DETAIL_TR_ID, limit=10, time_interval=1),
    # Path-based rate limits (used by ExchangePyBase._api_request as default limit_id)
    RateLimit(limit_id=DOMESTIC_STOCK_TICKER_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_ORDERBOOK_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=DOMESTIC_FUTURES_TICKER_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=DOMESTIC_FUTURES_ORDERBOOK_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_STOCK_TICKER_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_STOCK_ORDERBOOK_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_FUTURES_TICKER_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_FUTURES_ORDERBOOK_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_OPTIONS_TICKER_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=OVERSEAS_OPTIONS_ORDERBOOK_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_ORDER_PATH, limit=5, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_BALANCE_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=DOMESTIC_STOCK_ORDER_DETAIL_PATH, limit=10, time_interval=1),
    # WebSocket
    RateLimit(limit_id=WS_APPROVAL_PATH_URL, limit=1, time_interval=60),
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
