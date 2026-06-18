import asyncio
from decimal import Decimal

import pytest

from hummingbot.core.data_type.common import PositionMode


def _make_connector():
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    return KisFuturesDerivative(
        kis_futures_app_key="k",
        kis_futures_app_secret="s",
        kis_futures_account_number="12345678-03",
        kis_futures_hts_id="HTS",
        trading_pairs=["005930-KRW"],
        trading_required=False,
        kis_futures_sandbox="true",
        kis_futures_ws_enabled="false",
        kis_futures_target_underlyings="005930",
    )


def test_instantiates_and_perpetual_stubs():
    c = _make_connector()
    assert c.name == "kis_futures"
    assert c.supported_position_modes() == [PositionMode.ONEWAY]
    assert c.get_buy_collateral_token("005930-KRW") == "KRW"
    assert c.funding_fee_poll_interval > 0
    assert c._acnt_prdt_cd == "03"
    assert c._cano == "12345678"


def test_factory_instantiation_no_client_config_map():
    from hummingbot.client.settings import AllConnectorSettings
    s = AllConnectorSettings.get_connector_settings()["kis_futures"]
    conn = s.non_trading_connector_instance_with_default_configuration(trading_pairs=["005930-KRW"])
    assert conn.name == "kis_futures"


def test_position_mode_set_oneway_only():
    c = _make_connector()

    async def _run():
        ok, _ = await c._trading_pair_position_mode_set(PositionMode.ONEWAY, "005930-KRW")
        assert ok
        bad, msg = await c._trading_pair_position_mode_set(PositionMode.HEDGE, "005930-KRW")
        assert not bad
        assert "ONEWAY" in msg

    asyncio.run(_run())


def test_ob_ds_get_funding_info_returns_inert():
    """OB DS get_funding_info must return a FundingInfo with rate=0 (not raise)."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_api_order_book_data_source import (
        KisFuturesAPIOrderBookDataSource,
    )
    from hummingbot.core.data_type.funding_info import FundingInfo

    ds = KisFuturesAPIOrderBookDataSource(
        trading_pairs=["005930-KRW"],
        connector=None,
        api_factory=None,
        auth=None,
    )

    async def _run():
        fi = await ds.get_funding_info("005930-KRW")
        assert isinstance(fi, FundingInfo)
        assert fi.rate == Decimal("0")
        assert fi.trading_pair == "005930-KRW"

    asyncio.run(_run())
