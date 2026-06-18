# A single source of truth for constant variables related to the KIS Futures connector
# (국내 개인 주식선물 — domestic individual stock futures, KRX)

from hummingbot.core.api_throttler.data_types import RateLimit

# --------------------------------------------------------------------------- #
# Identity / ordering
# --------------------------------------------------------------------------- #
DEFAULT_DOMAIN = ""
HBOT_ORDER_ID_PREFIX = ""
MAX_ORDER_ID_LEN = 32

# Derivatives account product code (파생 계좌상품코드 03 = futures/options)
ACNT_PRDT_CD = "03"

# FID_COND_MRKT_DIV_CODE for domestic individual stock futures REST quotations
MRKT_DIV_STOCK_FUTURE = "JF"

# --------------------------------------------------------------------------- #
# REST — quotations
# --------------------------------------------------------------------------- #
FUT_TICKER_TR_ID = "FHMIF10000000"
FUT_TICKER_PATH = "uapi/domestic-futureoption/v1/quotations/inquire-price"

FUT_ORDERBOOK_TR_ID = "FHMIF10010000"
FUT_ORDERBOOK_PATH = "uapi/domestic-futureoption/v1/quotations/inquire-asking-price"

# --------------------------------------------------------------------------- #
# REST — trading
# --------------------------------------------------------------------------- #
FUT_ORDER_TR_ID = "TTTO1101U"
FUT_ORDER_PATH = "uapi/domestic-futureoption/v1/trading/order"

FUT_CANCEL_TR_ID = "TTTO1103U"
FUT_CANCEL_PATH = "uapi/domestic-futureoption/v1/trading/order-rvsecncl"

FUT_BALANCE_TR_ID = "CTFO6118R"
FUT_BALANCE_PATH = "uapi/domestic-futureoption/v1/trading/inquire-balance"

FUT_CCNL_TR_ID = "TTTO5201R"
FUT_CCNL_PATH = "uapi/domestic-futureoption/v1/trading/inquire-ccnl"

# --------------------------------------------------------------------------- #
# WebSocket TR_IDs
# --------------------------------------------------------------------------- #
WS_FUT_ORDERBOOK_TR_ID = "H0ZFASP0"         # 국내 주식선물 실시간호가
WS_FUT_TRADE_TR_ID = "H0ZFCNT0"              # 국내 주식선물 실시간체결가
WS_FUT_EXEC_NOTICE_TR_ID = "H0IFCNI0"        # 국내 주식선물 체결통보 (실전)
WS_FUT_EXEC_NOTICE_SANDBOX_TR_ID = "H0IFCNI9"  # 체결통보 (모의)

# --------------------------------------------------------------------------- #
# WebSocket Column Definitions
# --------------------------------------------------------------------------- #

# H0ZFCNT0 — 국내 주식선물 실시간체결가 — 49 fields
WS_TRADE_COLUMNS = (
    "futs_shrn_iscd",
    "bsop_hour",
    "stck_prpr",
    "prdy_vrss_sign",
    "prdy_vrss",
    "futs_prdy_ctrt",
    "stck_oprc",
    "stck_hgpr",
    "stck_lwpr",
    "last_cnqn",
    "acml_vol",
    "acml_tr_pbmn",
    "hts_thpr",
    "mrkt_basis",
    "dprt",
    "nmsc_fctn_stpl_prc",
    "fmsc_fctn_stpl_prc",
    "spead_prc",
    "hts_otst_stpl_qty",
    "otst_stpl_qty_icdc",
    "oprc_hour",
    "oprc_vrss_prpr_sign",
    "oprc_vrss_prpr",
    "hgpr_hour",
    "hgpr_vrss_prpr_sign",
    "hgpr_vrss_prpr",
    "lwpr_hour",
    "lwpr_vrss_prpr_sign",
    "lwpr_vrss_prpr",
    "shnu_rate",
    "cttr",
    "esdg",
    "otst_stpl_rgbf_qty_icdc",
    "thpr_basis",
    "askp1",
    "bidp1",
    "askp_rsqn1",
    "bidp_rsqn1",
    "seln_cntg_csnu",
    "shnu_cntg_csnu",
    "ntby_cntg_csnu",
    "seln_cntg_smtn",
    "shnu_cntg_smtn",
    "total_askp_rsqn",
    "total_bidp_rsqn",
    "prdy_vol_vrss_acml_vol_rate",
    "dynm_mxpr",
    "dynm_llam",
    "dynm_prc_limt_yn",
)

# H0ZFASP0 — 국내 주식선물 실시간호가 — placeholder (filled in a later slice from KIS sample)
WS_ORDERBOOK_COLUMNS = ()

# H0IFCNI0 — 국내 주식선물 체결통보 — 22 fields (encrypted)
WS_EXEC_NOTICE_COLUMNS = (
    "cust_id",
    "acnt_no",
    "oder_no",
    "ooder_no",
    "seln_byov_cls",
    "rctf_cls",
    "oder_kind2",
    "stck_shrn_iscd",
    "cntg_qty",
    "cntg_unpr",
    "stck_cntg_hour",
    "rfus_yn",
    "cntg_yn",
    "acpt_yn",
    "brnc_no",
    "oder_qty",
    "acnt_name",
    "cntg_isnm",
    "oder_cond",
    "ord_grp",
    "ord_grpseq",
    "order_prc",
)

# --------------------------------------------------------------------------- #
# Master file (선물 종목 코드 마스터)
# --------------------------------------------------------------------------- #
MASTER_URL = "https://new.real.download.dws.co.kr/common/master/fo_stk_code_mts.mst.zip"
MASTER_INNER = "fo_stk_code_mts.mst"
MASTER_REFRESH_INTERVAL = 3600.0  # seconds

# --------------------------------------------------------------------------- #
# Polling cadence
# --------------------------------------------------------------------------- #
# REST-only fallback: poll order book at this interval (seconds).
REST_ORDER_BOOK_POLL_INTERVAL = 5.0

# --------------------------------------------------------------------------- #
# Rate Limits
# --------------------------------------------------------------------------- #
RATE_LIMITS = [
    # --- by TR_ID ---
    # Quotations (ticker / orderbook) — 20/s
    RateLimit(limit_id=FUT_TICKER_TR_ID, limit=20, time_interval=1),
    RateLimit(limit_id=FUT_ORDERBOOK_TR_ID, limit=20, time_interval=1),
    # Trading (order / cancel) — 5/s
    RateLimit(limit_id=FUT_ORDER_TR_ID, limit=5, time_interval=1),
    RateLimit(limit_id=FUT_CANCEL_TR_ID, limit=5, time_interval=1),
    # Account / fills — 10/s
    RateLimit(limit_id=FUT_BALANCE_TR_ID, limit=10, time_interval=1),
    RateLimit(limit_id=FUT_CCNL_TR_ID, limit=10, time_interval=1),
    # --- by PATH (ExchangePyBase._api_request default limit_id) ---
    # Quotations
    RateLimit(limit_id=FUT_TICKER_PATH, limit=20, time_interval=1),
    RateLimit(limit_id=FUT_ORDERBOOK_PATH, limit=20, time_interval=1),
    # Trading
    RateLimit(limit_id=FUT_ORDER_PATH, limit=5, time_interval=1),
    RateLimit(limit_id=FUT_CANCEL_PATH, limit=5, time_interval=1),
    # Account / fills
    RateLimit(limit_id=FUT_BALANCE_PATH, limit=10, time_interval=1),
    RateLimit(limit_id=FUT_CCNL_PATH, limit=10, time_interval=1),
]
