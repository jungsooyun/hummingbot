from decimal import Decimal
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
    model_config = ConfigDict(title="kis")


KEYS = KisConfigMap.model_construct()
