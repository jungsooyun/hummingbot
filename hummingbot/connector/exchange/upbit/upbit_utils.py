from decimal import Decimal
from typing import Any, Dict, Tuple

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-KRW"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0005"),
    taker_percent_fee_decimal=Decimal("0.0005"),
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    market_warning = str(exchange_info.get("market_warning", "NONE")).upper()
    return market_warning == "NONE"


def split_market_symbol(market: str) -> Tuple[str, str]:
    if "-" not in market:
        raise ValueError(f"Invalid market symbol: {market}")
    quote, base = market.split("-", 1)
    return base, quote


class UpbitConfigMap(BaseConnectorConfigMap):
    connector: str = "upbit"
    upbit_access_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Upbit access key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    upbit_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Upbit secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )

    model_config = ConfigDict(title="upbit")


KEYS = UpbitConfigMap.model_construct()
