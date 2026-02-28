from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = False

EXAMPLE_PAIR = "ETH-USDC"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.0005"),
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information.

    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("status", "active") == "active"


def scale_to_int(value: float, decimals: int) -> int:
    """Scale a float to integer representation for Lighter API.

    Lighter uses integer representations with fixed decimal precision.
    For example, a price of 1234.56 with 2 decimals becomes 123456.

    :param value: the float value to scale
    :param decimals: the number of decimal places
    :return: the scaled integer value
    """
    d = Decimal(str(value))
    factor = Decimal(10) ** decimals
    return int((d * factor).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


class LighterConfigMap(BaseConnectorConfigMap):
    connector: str = "lighter"
    lighter_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter API private key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    lighter_account_index: str = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Lighter account index",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="lighter")


KEYS = LighterConfigMap.model_construct()
