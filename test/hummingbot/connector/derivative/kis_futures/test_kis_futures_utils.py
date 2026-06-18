from decimal import Decimal

import pytest

from hummingbot.connector.derivative.kis_futures.kis_futures_utils import (
    DEFAULT_FEES,
    KisFuturesConfigMap,
)


class TestKisFuturesUtils:
    def test_config_map_connect_keys(self):
        """All operational fields forwarded to the connector must have is_connect_key=True."""
        fields = KisFuturesConfigMap.model_fields
        for field_name in (
            "kis_futures_ws_enabled",
            "kis_futures_target_underlyings",
            "kis_futures_sandbox",
        ):
            extra = fields[field_name].json_schema_extra
            assert extra["is_connect_key"] is True, (
                f"{field_name} must have is_connect_key=True"
            )

    def test_default_fees_no_stt(self):
        """Futures fee must be well below the 0.1% securities transaction tax threshold."""
        assert DEFAULT_FEES.maker_percent_fee_decimal < Decimal("0.001"), (
            "KIS futures maker fee should reflect exchange fee only (no STT)"
        )
        assert DEFAULT_FEES.taker_percent_fee_decimal < Decimal("0.001"), (
            "KIS futures taker fee should reflect exchange fee only (no STT)"
        )

    def test_config_map_connector_name(self):
        """Connector name must match auto-discovery key."""
        assert KisFuturesConfigMap.model_fields["connector"].default == "kis_futures"

    def test_config_map_account_is_connect_key(self):
        """Account number must be a connect key so it reaches the constructor."""
        extra = KisFuturesConfigMap.model_fields["kis_futures_account_number"].json_schema_extra
        assert extra["is_connect_key"] is True

    def test_config_map_app_key_is_secure(self):
        """App key / secret must be marked secure."""
        assert KisFuturesConfigMap.model_fields["kis_futures_app_key"].json_schema_extra["is_secure"] is True
        assert KisFuturesConfigMap.model_fields["kis_futures_app_secret"].json_schema_extra["is_secure"] is True
