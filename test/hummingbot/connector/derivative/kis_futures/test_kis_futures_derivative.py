"""
Tests for KisFuturesDerivative — Task 3a (scaffold) + 3b/3c/3d (orders/positions/reconcile).
"""
import asyncio
import io
import re
import zipfile
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aioresponses import aioresponses

from hummingbot.connector.derivative.kis_futures import kis_futures_constants as CONSTANTS
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState

# Sandbox base URL (openapivts)
_SANDBOX_BASE = "https://openapivts.koreainvestment.com:29443"


def _re(path: str) -> re.Pattern:
    """Return a compiled regex that matches the sandbox URL for a given path."""
    return re.compile(re.escape(_SANDBOX_BASE + "/" + path.lstrip("/")) + ".*")


def _run(coro):
    """Run a coroutine in a fresh event loop so tests don't share loop state."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connector(trading_pairs=None):
    import time
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    c = KisFuturesDerivative(
        kis_futures_app_key="k",
        kis_futures_app_secret="s",
        kis_futures_account_number="12345678-03",
        kis_futures_hts_id="HTS",
        trading_pairs=trading_pairs or ["005930-KRW"],
        trading_required=False,
        kis_futures_sandbox="true",
        kis_futures_ws_enabled="false",
        kis_futures_target_underlyings="005930",
    )
    # Pre-inject a cached OAuth2 token so tests don't attempt tokenP round-trips.
    c._auth._access_token = "test_access_token"
    c._auth._token_expires_at = time.time() + 86400
    return c


def _make_contract(short_code="A11607", standard_code="KR4A11670002",
                   underlying="005930", expiry="202607", multiplier="10"):
    from hummingbot.connector.derivative.kis_futures.kis_futures_master import FuturesContract
    return FuturesContract(
        short_code=short_code,
        standard_code=standard_code,
        underlying=underlying,
        expiry_yyyymm=expiry,
        multiplier=Decimal(multiplier),
        name=f"삼성전자 F {expiry}",
    )


def _patch_trading_rule(connector, pair="005930-KRW", tick=Decimal("5")):
    from hummingbot.connector.trading_rule import TradingRule
    connector._trading_rules[pair] = TradingRule(
        trading_pair=pair,
        min_order_size=Decimal("1"),
        min_price_increment=tick,
        min_base_amount_increment=Decimal("1"),
        buy_order_collateral_token="KRW",
        sell_order_collateral_token="KRW",
    )


def _inject_contract(connector, pair="005930-KRW", short_code="A11607"):
    connector._contract_by_pair[pair] = _make_contract(short_code=short_code)
    _patch_trading_rule(connector, pair)


# ---------------------------------------------------------------------------
# Task 3a: scaffold
# ---------------------------------------------------------------------------

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

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()


def test_ob_ds_get_funding_info_returns_inert():
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

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()


# ---------------------------------------------------------------------------
# Task 3b: _sandbox_tr_id, _validate_contract_qty, _format_price
# ---------------------------------------------------------------------------

def test_sandbox_tr_id_converts_T_prefix():
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    assert KisFuturesDerivative._sandbox_tr_id("TTTO1101U") == "VTTO1101U"
    assert KisFuturesDerivative._sandbox_tr_id("CTFO6118R") == "CTFO6118R"  # C prefix → unchanged
    assert KisFuturesDerivative._sandbox_tr_id("TTTO5201R") == "VTTO5201R"


def test_validate_contract_qty_accepts_positive_integer():
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    assert KisFuturesDerivative._validate_contract_qty(Decimal("3")) == 3
    assert KisFuturesDerivative._validate_contract_qty(Decimal("1")) == 1


def test_validate_contract_qty_rejects_non_integer():
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    with pytest.raises(ValueError):
        KisFuturesDerivative._validate_contract_qty(Decimal("1.5"))


def test_validate_contract_qty_rejects_zero_and_negative():
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    with pytest.raises(ValueError):
        KisFuturesDerivative._validate_contract_qty(Decimal("0"))
    with pytest.raises(ValueError):
        KisFuturesDerivative._validate_contract_qty(Decimal("-1"))


def test_format_price_aligned_tick():
    c = _make_connector()
    _patch_trading_rule(c, tick=Decimal("5"))
    assert c._format_price(Decimal("50000"), "005930-KRW") == "50000"
    assert c._format_price(Decimal("50005"), "005930-KRW") == "50005"


def test_format_price_strips_trailing_zeros():
    c = _make_connector()
    _patch_trading_rule(c, tick=Decimal("5"))
    result = c._format_price(Decimal("50000"), "005930-KRW")
    # Must not contain a trailing decimal zero
    assert "." not in result or not result.endswith("0")


def test_format_price_misaligned_raises():
    c = _make_connector()
    _patch_trading_rule(c, tick=Decimal("5"))
    with pytest.raises(ValueError):
        c._format_price(Decimal("50001"), "005930-KRW")


# ---------------------------------------------------------------------------
# Task 3b: _place_order
# ---------------------------------------------------------------------------

def test_place_order_buy_sends_correct_body():
    c = _make_connector()
    _inject_contract(c)

    captured = {}

    async def _run():
        with aioresponses() as m:
            m.post(
                _re(CONSTANTS.FUT_ORDER_PATH),
                payload={"rt_cd": "0", "msg1": "OK", "output": {"ODNO": "0000009999"}},
            )
            odno, ts = await c._place_order(
                order_id="test-buy-1",
                trading_pair="005930-KRW",
                amount=Decimal("2"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("50000"),
                position_action=PositionAction.OPEN,
            )
            captured["odno"] = odno

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()
    assert captured["odno"] == "0000009999"
    assert c._order_acks["test-buy-1"]["sll_buy"] == "02"


def test_place_order_sell_sll_buy_code():
    c = _make_connector()
    _inject_contract(c)

    async def _run():
        with aioresponses() as m:
            m.post(
                _re(CONSTANTS.FUT_ORDER_PATH),
                payload={"rt_cd": "0", "msg1": "OK", "output": {"ODNO": "0000008888"}},
            )
            await c._place_order(
                order_id="test-sell-1",
                trading_pair="005930-KRW",
                amount=Decimal("1"),
                trade_type=TradeType.SELL,
                order_type=OrderType.LIMIT,
                price=Decimal("50000"),
                position_action=PositionAction.OPEN,
            )

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()
    assert c._order_acks["test-sell-1"]["sll_buy"] == "01"


def test_place_order_rt_cd_nonzero_raises():
    c = _make_connector()
    _inject_contract(c)

    async def _run():
        with aioresponses() as m:
            m.post(
                _re(CONSTANTS.FUT_ORDER_PATH),
                payload={"rt_cd": "1", "msg1": "ERROR"},
            )
            await c._place_order(
                order_id="test-err-1",
                trading_pair="005930-KRW",
                amount=Decimal("1"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("50000"),
                position_action=PositionAction.OPEN,
            )

    with pytest.raises(IOError, match="KIS futures order rejected"):
        loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()


def test_place_order_non_integral_qty_raises():
    c = _make_connector()
    _inject_contract(c)

    async def _run():
        await c._place_order(
            order_id="test-frac-1",
            trading_pair="005930-KRW",
            amount=Decimal("1.5"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("50000"),
            position_action=PositionAction.OPEN,
        )

    with pytest.raises(ValueError):
        loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()


# ---------------------------------------------------------------------------
# Task 3b: _place_cancel
# ---------------------------------------------------------------------------

def test_place_cancel_sends_correct_body():
    c = _make_connector()
    tracked = MagicMock()
    tracked.exchange_order_id = "0000009999"

    async def _run():
        with aioresponses() as m:
            m.post(
                _re(CONSTANTS.FUT_CANCEL_PATH),
                payload={"rt_cd": "0", "msg1": "OK", "output": {"ODNO": "9999"}},
            )
            return await c._place_cancel("test-cancel-1", tracked)

    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(_run())
    loop.close()
    assert result is True


def test_place_cancel_rt_cd_nonzero_raises():
    c = _make_connector()
    tracked = MagicMock()
    tracked.exchange_order_id = "0000009999"

    async def _run():
        with aioresponses() as m:
            m.post(
                _re(CONSTANTS.FUT_CANCEL_PATH),
                payload={"rt_cd": "1", "msg1": "ERROR"},
            )
            await c._place_cancel("test-cancel-err", tracked)

    with pytest.raises(IOError, match="KIS futures cancel rejected"):
        loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()


# ---------------------------------------------------------------------------
# Task 3c: _update_balances
# ---------------------------------------------------------------------------

def test_update_balances_parses_output2():
    c = _make_connector()

    balance_resp = {
        "rt_cd": "0",
        "output1": [],
        "output2": {
            "dnca_tot_amt": "5000000",
            "ord_psbl_cash": "3000000",
        },
    }

    async def _run():
        with aioresponses() as m:
            m.get(
                _re(CONSTANTS.FUT_BALANCE_PATH),
                payload=balance_resp,
            )
            await c._update_balances()

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()
    assert c._account_balances["KRW"] == Decimal("5000000")
    assert c._account_available_balances["KRW"] == Decimal("3000000")


# ---------------------------------------------------------------------------
# Task 3c: _update_positions
# ---------------------------------------------------------------------------

def test_update_positions_buy_row_is_long():
    from hummingbot.core.data_type.common import PositionSide
    c = _make_connector()
    _inject_contract(c)

    balance_resp = {
        "rt_cd": "0",
        "output1": [
            {
                "shtn_pdno": "A11607",
                "sll_buy_dvsn_cd": "02",
                "cblc_qty": "3",
                "evlu_pfls_amt": "15000",
                "pchs_avg_pric": "50000",
            }
        ],
        "output2": {"dnca_tot_amt": "1000000", "ord_psbl_cash": "500000"},
    }

    async def _run():
        with aioresponses() as m:
            m.get(
                _re(CONSTANTS.FUT_BALANCE_PATH),
                payload=balance_resp,
            )
            await c._update_positions()

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()
    positions = c._perpetual_trading.account_positions
    key = c._perpetual_trading.position_key("005930-KRW", PositionSide.LONG)
    assert key in positions
    assert positions[key].amount == Decimal("3")
    assert positions[key].position_side == PositionSide.LONG


def test_update_positions_sell_row_is_short():
    from hummingbot.core.data_type.common import PositionSide
    c = _make_connector()
    _inject_contract(c)

    balance_resp = {
        "rt_cd": "0",
        "output1": [
            {
                "shtn_pdno": "A11607",
                "sll_buy_dvsn_cd": "01",
                "cblc_qty": "2",
                "evlu_pfls_amt": "-5000",
                "pchs_avg_pric": "50100",
            }
        ],
        "output2": {"dnca_tot_amt": "1000000", "ord_psbl_cash": "500000"},
    }

    async def _run():
        with aioresponses() as m:
            m.get(
                _re(CONSTANTS.FUT_BALANCE_PATH),
                payload=balance_resp,
            )
            await c._update_positions()

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()
    positions = c._perpetual_trading.account_positions
    key = c._perpetual_trading.position_key("005930-KRW", PositionSide.SHORT)
    assert key in positions
    assert positions[key].amount == Decimal("-2")


def test_update_positions_zero_qty_removed():
    from hummingbot.core.data_type.common import PositionSide
    c = _make_connector()
    _inject_contract(c)

    balance_with_pos = {
        "rt_cd": "0",
        "output1": [{"shtn_pdno": "A11607", "sll_buy_dvsn_cd": "02", "cblc_qty": "1",
                     "evlu_pfls_amt": "0", "pchs_avg_pric": "50000"}],
        "output2": {"dnca_tot_amt": "1000000", "ord_psbl_cash": "500000"},
    }
    balance_flat = {
        "rt_cd": "0",
        "output1": [{"shtn_pdno": "A11607", "sll_buy_dvsn_cd": "02", "cblc_qty": "0",
                     "evlu_pfls_amt": "0", "pchs_avg_pric": "50000"}],
        "output2": {"dnca_tot_amt": "1000000", "ord_psbl_cash": "500000"},
    }

    async def _run():
        with aioresponses() as m:
            m.get(
                _re(CONSTANTS.FUT_BALANCE_PATH),
                payload=balance_with_pos,
            )
            await c._update_positions()
        key = c._perpetual_trading.position_key("005930-KRW", PositionSide.LONG)
        assert key in c._perpetual_trading.account_positions
        with aioresponses() as m:
            m.get(
                _re(CONSTANTS.FUT_BALANCE_PATH),
                payload=balance_flat,
            )
            await c._update_positions()
        assert key not in c._perpetual_trading.account_positions

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()


# ---------------------------------------------------------------------------
# Task 3d: static helpers for reconcile
# ---------------------------------------------------------------------------

def test_infer_order_state_filled():
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    order = MagicMock()
    row = {"tot_ccld_qty": "3", "ord_qty": "3", "cncl_yn": "N"}
    assert KisFuturesDerivative._infer_order_state(row, order) == OrderState.FILLED


def test_infer_order_state_partially_filled():
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    order = MagicMock()
    row = {"tot_ccld_qty": "1", "ord_qty": "3", "cncl_yn": "N"}
    assert KisFuturesDerivative._infer_order_state(row, order) == OrderState.PARTIALLY_FILLED


def test_infer_order_state_open():
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    order = MagicMock()
    row = {"tot_ccld_qty": "0", "ord_qty": "3", "cncl_yn": "N"}
    assert KisFuturesDerivative._infer_order_state(row, order) == OrderState.OPEN


def test_infer_order_state_canceled():
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    order = MagicMock()
    row = {"tot_ccld_qty": "0", "ord_qty": "3", "cncl_yn": "Y"}
    assert KisFuturesDerivative._infer_order_state(row, order) == OrderState.CANCELED


def test_match_ccld_row_by_odno():
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    order = MagicMock()
    order.exchange_order_id = "9999"
    rows = [{"odno": "1111", "tot_ccld_qty": "0"}, {"odno": "9999", "tot_ccld_qty": "2"}]
    result = {"output1": rows}
    row = KisFuturesDerivative._match_ccld_row(result, order)
    assert row["odno"] == "9999"


# ---------------------------------------------------------------------------
# Task 3d: _all_trade_updates_for_order (reconcile)
# ---------------------------------------------------------------------------

def test_all_trade_updates_for_order_emits_increment():
    c = _make_connector()
    _inject_contract(c)

    order = MagicMock()
    order.exchange_order_id = "9999"
    order.client_order_id = "cli-1"
    order.trading_pair = "005930-KRW"
    order.trade_type = TradeType.BUY
    order.executed_amount_base = Decimal("0")

    ccnl_resp = {
        "rt_cd": "0",
        "output1": [
            {
                "odno": "9999",
                "tot_ccld_qty": "2",
                "avg_idx": "50000",
                "ord_qty": "3",
                "cncl_yn": "N",
            }
        ],
    }

    async def _run():
        with aioresponses() as m:
            m.get(
                _re(CONSTANTS.FUT_CCNL_PATH),
                payload=ccnl_resp,
            )
            return await c._all_trade_updates_for_order(order)

    loop = asyncio.new_event_loop()
    updates = loop.run_until_complete(_run())
    loop.close()
    assert len(updates) == 1
    assert updates[0].fill_base_amount == Decimal("2")
    assert updates[0].fill_price == Decimal("50000")


def test_all_trade_updates_no_double_count():
    """Already-executed amount is subtracted: delta ≤ 0 → no update emitted."""
    c = _make_connector()
    _inject_contract(c)

    order = MagicMock()
    order.exchange_order_id = "9999"
    order.client_order_id = "cli-2"
    order.trading_pair = "005930-KRW"
    order.trade_type = TradeType.BUY
    order.executed_amount_base = Decimal("2")  # already filled

    ccnl_resp = {
        "rt_cd": "0",
        "output1": [{"odno": "9999", "tot_ccld_qty": "2", "avg_idx": "50000",
                     "ord_qty": "3", "cncl_yn": "N"}],
    }

    async def _run():
        with aioresponses() as m:
            m.get(
                _re(CONSTANTS.FUT_CCNL_PATH),
                payload=ccnl_resp,
            )
            return await c._all_trade_updates_for_order(order)

    loop = asyncio.new_event_loop()
    updates = loop.run_until_complete(_run())
    loop.close()
    assert len(updates) == 0


def test_all_trade_updates_none_exchange_order_id():
    """No exchange_order_id → skip entirely (order not yet acked)."""
    c = _make_connector()
    order = MagicMock()
    order.exchange_order_id = None

    async def _run():
        return await c._all_trade_updates_for_order(order)

    loop = asyncio.new_event_loop()
    updates = loop.run_until_complete(_run())
    loop.close()
    assert updates == []


# ---------------------------------------------------------------------------
# Task 3d: front-month + rollover
# ---------------------------------------------------------------------------

def test_resolve_front_months_populates_contract_by_pair():
    from pathlib import Path
    c = _make_connector()

    fixture_path = Path(__file__).parent / "fixtures" / "fo_stk_code_sample.txt"
    inner_bytes = fixture_path.read_bytes()

    async def _run():
        with patch(
            "hummingbot.connector.derivative.kis_futures.kis_futures_derivative.download_master",
            new=AsyncMock(return_value=inner_bytes),
        ):
            await c._resolve_front_months()

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()
    assert "005930-KRW" in c._contract_by_pair
    assert c._contract_by_pair["005930-KRW"].short_code == "A11607"


def test_resolve_front_months_roll_pending_with_open_orders():
    """New front month + open order → roll pending True, code unchanged."""
    c = _make_connector()
    c._contract_by_pair["005930-KRW"] = _make_contract(short_code="A11607", expiry="202607")

    new_master_bytes = (
        "1|A11608|KR4A11680001|삼성전자   F 202608 (  10)| |00000.00|1|005930|삼성전자\n"
    ).encode("cp949")

    mock_order = MagicMock()
    mock_order.trading_pair = "005930-KRW"
    c._order_tracker._in_flight_orders = {"test-ord": mock_order}

    async def _run():
        with patch(
            "hummingbot.connector.derivative.kis_futures.kis_futures_derivative.download_master",
            new=AsyncMock(return_value=new_master_bytes),
        ):
            await c._resolve_front_months()

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()
    assert c._contract_by_pair["005930-KRW"].short_code == "A11607"
    assert c.is_roll_pending("005930-KRW") is True


def test_resolve_front_months_roll_executes_when_flat():
    """New front month + flat + no orders → code switches, roll cleared."""
    c = _make_connector()
    c._contract_by_pair["005930-KRW"] = _make_contract(short_code="A11607", expiry="202607")

    new_master_bytes = (
        "1|A11608|KR4A11680001|삼성전자   F 202608 (  10)| |00000.00|1|005930|삼성전자\n"
    ).encode("cp949")

    c._order_tracker._in_flight_orders = {}

    async def _run():
        with patch(
            "hummingbot.connector.derivative.kis_futures.kis_futures_derivative.download_master",
            new=AsyncMock(return_value=new_master_bytes),
        ):
            await c._resolve_front_months()

    loop = asyncio.new_event_loop(); loop.run_until_complete(_run()); loop.close()
    assert c._contract_by_pair["005930-KRW"].short_code == "A11608"
    assert c.is_roll_pending("005930-KRW") is False


def test_roll_accessors():
    c = _make_connector()
    _inject_contract(c)
    assert c.current_contract_code("005930-KRW") == "A11607"
    assert c.expiry_yyyymm("005930-KRW") == "202607"
    assert c.is_roll_pending("005930-KRW") is False
    assert c.roll_pending_pairs == []
