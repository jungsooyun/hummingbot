from decimal import Decimal
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True

EXAMPLE_PAIR = "005930-KRW"  # Samsung Electronics

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.00015"),
    taker_percent_fee_decimal=Decimal("0.00015"),
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    KIS does not have a symbols list API -- assume valid if configured.

    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return True


def to_float(value: Any) -> Optional[float]:
    """Safely convert KIS string values to float.

    KIS API frequently returns numeric fields as strings, including empty
    strings for absent data.  This helper handles all edge cases.
    """
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def first_float(data: Dict[str, Any], keys: List[str]) -> Optional[float]:
    """Return first non-empty float from *keys* in *data*.

    KIS uses different field names across market types for the same concept
    (e.g. ``stck_prpr`` vs ``last`` vs ``last_price``).  This helper tries
    each key in order and returns the first successful conversion.
    """
    for key in keys:
        if key in data:
            val = to_float(data[key])
            if val is not None:
                return val
    return None


@dataclass(frozen=True)
class KisHaltSignals:
    """Raw per-pair halt signals computed purely from KIS WS (JEP-198).

    book_age_sec / book_static_sec use time.perf_counter (the same monotonic clock
    as the JEP-134 freshness stamp). None = no data yet (cold start) -> fail-closed
    downstream. Phase-2 latches default to the no-halt value so the Phase-1 baseline
    (no H0STMKO0) reports them benign.
    """
    hour_cls_auction: bool
    book_age_sec: Optional[float]
    book_static_sec: Optional[float]
    trht_halted: bool = False
    cb_latched: bool = False
    vi_latched: bool = False
    market_status_ready: bool = True


class KisConfigMap(BaseConnectorConfigMap):
    connector: str = "kis"
    kis_app_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your KIS App Key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    kis_app_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your KIS App Secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    kis_account_number: str = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your KIS account number",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    kis_sandbox: str = Field(
        default="false",
        json_schema_extra={
            "prompt": lambda cm: "Use KIS sandbox? (true/false)",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": True,
        },
    )
    kis_market_routing: str = Field(
        default="sor",
        json_schema_extra={
            "prompt": lambda cm: "Order routing? (krx/nxt/sor) — sor enables NXT after-market",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    # When "false", the realtime WebSocket data sources never connect (no
    # ops.koreainvestment.com:21000 traffic); the order book is served purely by
    # REST snapshot polling and fills by REST order-status polling. Use this when
    # the live WS edge refuses connections (env-gated / rate-limit blocked) so the
    # bot stops hammering it. Default "true" preserves WS behavior.
    kis_ws_enabled: str = Field(
        default="true",
        json_schema_extra={
            "prompt": lambda cm: "Use realtime WebSocket? (true/false) — false = REST-only market data",
            "is_secure": False,
            # MUST be a connect key: only is_connect_key fields are forwarded to the
            # trading-connector constructor (api_keys_from_connector_config_map filters
            # on is_connect_key). is_connect_key=False -> the kwarg never reaches
            # KisExchange and kis_ws_enabled silently defaults to "true". This mirrors
            # kis_market_routing, an operational (non-credential) field forwarded the
            # same way. prompt_on_new=False keeps it out of the interactive connect flow.
            "is_connect_key": True,
            "prompt_on_new": False,
            "is_updatable": True,
        },
    )
    kis_market_status_enabled: str = Field(
        default="false",
        json_schema_extra={
            "prompt": lambda cm: "Subscribe H0STMKO0 market-status feed for halt detection? (true/false)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": False,
            "is_updatable": True,
        },
    )
    # The customer HTS ID (고객 ID) is the tr_key required by the KIS execution-
    # notice realtime channel (H0STCNI0/H0STCNI9). It is NOT a stock code — KIS
    # rejects a stock symbol here with OPSP0017 "htsid가 잘못되었습니다", and the
    # rejected subscribe recycles the shared WS socket (taking the orderbook WS
    # down with it). The channel is account-wide: one subscription = all fills
    # for the account. Empty default -> exec-notice WS is skipped and fills fall
    # back to REST order-status polling (orderbook WS stays up). is_connect_key=True
    # is MANDATORY: only is_connect_key fields are forwarded to the connector
    # constructor (api_keys_from_connector_config_map filters on is_connect_key) —
    # see kis_ws_enabled / JEP-180. Not a secret (an identifier, like
    # kis_account_number).
    kis_hts_id: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda cm: "Enter your KIS HTS ID (고객 ID) — required for the execution-notice channel (H0STCNI0)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="kis")


KEYS = KisConfigMap.model_construct()
