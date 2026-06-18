from decimal import Decimal
from typing import Any, Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True

EXAMPLE_PAIR = "005930-KRW"  # Samsung Electronics (underlying reference)

# KRX domestic individual stock futures fee schedule:
#   Exchange + clearing: ~0.001569 % per side (KRXF 0.00156970 %).
#   No securities transaction tax (증권거래세) applies to futures.
#   Brokerage is negotiated separately and is NOT included here.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0000157"),
    taker_percent_fee_decimal=Decimal("0.0000157"),
    buy_percent_fee_deducted_from_returns=True,
)


class KisFuturesConfigMap(BaseConnectorConfigMap):
    connector: str = "kis_futures"
    kis_futures_app_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your KIS derivatives app key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    kis_futures_app_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your KIS derivatives app secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    kis_futures_account_number: str = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your KIS derivatives account (e.g. 12345678-03)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    kis_futures_hts_id: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda cm: "Enter your KIS HTS ID (for exec-notice WS)",
            "is_secure": False,
            # MUST be is_connect_key so it is forwarded to the connector constructor
            # via api_keys_from_connector_config_map (mirrors kis_ws_enabled pattern).
            "is_connect_key": True,
            "prompt_on_new": False,
        },
    )
    kis_futures_sandbox: str = Field(
        default="false",
        json_schema_extra={
            "prompt": lambda cm: "KIS sandbox/demo? (true/false)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    kis_futures_ws_enabled: str = Field(
        default="true",
        json_schema_extra={
            "prompt": lambda cm: "Enable KIS futures WebSocket? (true/false)",
            "is_secure": False,
            # MUST be is_connect_key — only is_connect_key fields are forwarded to the
            # trading-connector constructor; prompt_on_new=False keeps it out of the
            # interactive connect flow (mirrors kis_ws_enabled in spot connector).
            "is_connect_key": True,
            "prompt_on_new": False,
            "is_updatable": True,
        },
    )
    kis_futures_target_underlyings: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda cm: "Comma-separated underlying stock codes (e.g. 005930,000660)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="kis_futures")


KEYS = KisFuturesConfigMap.model_construct()


def to_float(value: Any) -> Optional[float]:
    """Safely convert KIS string values to float.

    KIS API frequently returns numeric fields as strings, including empty
    strings for absent data.  This helper handles all edge cases.
    """
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def is_exchange_information_valid(exchange_info) -> bool:
    """KIS futures does not expose a symbol-list endpoint; assume valid if configured."""
    return True
