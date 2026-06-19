import os

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "hyperliquid_perpetual"
BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = None
MIN_NOTIONAL_SIZE = 10

# === Builder code support (HGP-87) ===
# Attach a Foundation builder code to mainnet orders (omitted on testnet/vault). The fee only takes
# effect if the user has approved this builder in Condor; otherwise it is 0 bps.
# See HyperliquidPerpetualDerivative._initialize_builder_fee.
BUILDER_SUPPORTED = True

# Foundation builder address (set BUILDER_SUPPORTED = False to omit the builder field entirely).
FOUNDATION_BUILDER_ADDRESS = "0x10BA451e6439Efc6a17dc20d21121Aa838100705"

# Per-order builder fee, in tenths of a basis point (10 = 1 bp = 0.01%). Only takes effect if the
# user has approved this builder in Condor; otherwise the effective fee is 0 bps.
FOUNDATION_BUILDER_FEE_TENTHS_BPS = 10

# Info-endpoint request type used to query a user's approved max builder fee for a builder.
MAX_BUILDER_FEE_TYPE = "maxBuilderFee"

MARKET_ORDER_SLIPPAGE = 0.05

DOMAIN = EXCHANGE_NAME
TESTNET_DOMAIN = "hyperliquid_perpetual_testnet"

# Env-overridable for a dedicated RPC/proxy (e.g. QuikNode raw-HL base). The key-bearing
# URL stays in the runtime env_file (HYPERLIQUID_PERP_BASE_URL), never baked into the image.
PERPETUAL_BASE_URL = os.environ.get("HYPERLIQUID_PERP_BASE_URL", "https://api.hyperliquid.xyz")

TESTNET_BASE_URL = "https://api.hyperliquid-testnet.xyz"

PERPETUAL_WS_URL = os.environ.get("HYPERLIQUID_PERP_WS_URL", "wss://api.hyperliquid.xyz/ws")

TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"

FUNDING_RATE_UPDATE_INTERNAL_SECOND = 60

CURRENCY = "USD"

META_INFO = "meta"

ASSET_CONTEXT_TYPE = "metaAndAssetCtxs"
DEX_ASSET_CONTEXT_TYPE = "allPerpMetas"


TRADES_TYPE = "userFills"

ORDER_STATUS_TYPE = "orderStatus"

USER_STATE_TYPE = "clearinghouseState"
SPOT_USER_STATE_TYPE = "spotClearinghouseState"
USER_ABSTRACTION_TYPE = "userAbstraction"

SPOT_BALANCE_ABSTRACTION_MODES = {"unifiedAccount", "portfolioMargin"}

# yes
TICKER_PRICE_CHANGE_URL = "/info"
# yes
SNAPSHOT_REST_URL = "/info"

EXCHANGE_INFO_URL = "/info"

CANCEL_ORDER_URL = "/exchange"

CREATE_ORDER_URL = "/exchange"

ACCOUNT_TRADE_LIST_URL = "/info"

ORDER_URL = "/info"

ACCOUNT_INFO_URL = "/info"

POSITION_INFORMATION_URL = "/info"

SET_LEVERAGE_URL = "/exchange"

GET_LAST_FUNDING_RATE_PATH_URL = "/info"

PING_URL = "/info"

TRADES_ENDPOINT_NAME = "trades"
DEPTH_ENDPOINT_NAME = "l2Book"
FUNDING_INFO_ENDPOINT_NAME = "activeAssetCtx"


USER_ORDERS_ENDPOINT_NAME = "orderUpdates"
USEREVENT_ENDPOINT_NAME = "user"

# Order Statuses
ORDER_STATE = {
    "open": OrderState.OPEN,
    "resting": OrderState.OPEN,
    "filled": OrderState.FILLED,
    "canceled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
    "badAloPxRejected": OrderState.FAILED,
    "minTradeNtlRejected": OrderState.FAILED,
    "reduceOnlyCanceled": OrderState.CANCELED,
    "reduceOnlyRejected": OrderState.FAILED,
    "perpMarginRejected": OrderState.FAILED,
    "selfTradeCanceled": OrderState.CANCELED,
    "siblingFilledCanceled": OrderState.CANCELED,
    "delistedCanceled": OrderState.CANCELED,
    "liquidatedCanceled": OrderState.CANCELED,
}

HEARTBEAT_TIME_INTERVAL = 30.0
# Max time to wait for a websocket message before assuming the connection is half-open (no data, no close
# frame) and forcing a keepalive ping / reconnection. Set above HEARTBEAT_TIME_INTERVAL so the periodic ping
# (and its pong) keeps a healthy connection from timing out.
WS_MESSAGE_TIMEOUT = 60.0

MAX_REQUEST = 1_200
ALL_ENDPOINTS_LIMIT = "All"

# Optional per-SECOND global request cap for a rate-limited dedicated RPC/proxy (e.g.
# QuikNode free tier = 15/s; HL native is MAX_REQUEST/60s = 20/s and allows bursts that
# trip a 15/s proxy at HIP-3 startup). When HYPERLIQUID_PERP_RATE_LIMIT_PER_S > 0 the
# global pool caps at that many requests/second; otherwise the native per-minute limit
# applies. Env-overridable so the cap can be raised on a plan upgrade without a rebuild.
_HL_RPS = int(os.environ.get("HYPERLIQUID_PERP_RATE_LIMIT_PER_S", "0") or "0")
_ALL_PER_S = _HL_RPS > 0

RATE_LIMITS = [
    RateLimit(ALL_ENDPOINTS_LIMIT, limit=(_HL_RPS if _ALL_PER_S else MAX_REQUEST), time_interval=(1 if _ALL_PER_S else 60)),

    # Weight Limits for individual endpoints
    RateLimit(limit_id=SNAPSHOT_REST_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=TICKER_PRICE_CHANGE_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=EXCHANGE_INFO_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=PING_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ORDER_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=CREATE_ORDER_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=CANCEL_ORDER_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),

    RateLimit(limit_id=ACCOUNT_TRADE_LIST_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=SET_LEVERAGE_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ACCOUNT_INFO_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=POSITION_INFORMATION_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=GET_LAST_FUNDING_RATE_PATH_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),

]
ORDER_NOT_EXIST_MESSAGE = "order"
UNKNOWN_ORDER_MESSAGE = "Order was never placed, already canceled, or filled"
