"""
Tests for KisFuturesAPIOrderBookDataSource — Slice 5 (Task 4).

Step 1: REST orderbook snapshot (FHMIF10010000, JF, 5-level output2).
Step 2: H0ZFCNT0 trade WS frame parsing (49-field caret-delimited).
"""
import asyncio
import json
import re
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aioresponses import aioresponses

from hummingbot.connector.derivative.kis_futures import kis_futures_constants as CONSTANTS
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType

# ---------------------------------------------------------------------------
# Fixtures directory
# ---------------------------------------------------------------------------

_FIXTURES = Path(__file__).parent / "fixtures"
_SANDBOX_BASE = "https://openapivts.koreainvestment.com:29443"


def _re(path: str) -> re.Pattern:
    return re.compile(re.escape(_SANDBOX_BASE + "/" + path.lstrip("/")) + ".*")


# ---------------------------------------------------------------------------
# Helper: build a minimal DS instance (no live connector needed for unit tests)
# ---------------------------------------------------------------------------

def _make_ds(trading_pairs=None, ws_enabled=False, connector=None):
    import time as _time
    from hummingbot.connector.derivative.kis_futures.kis_futures_api_order_book_data_source import (
        KisFuturesAPIOrderBookDataSource,
    )
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative

    if connector is None:
        connector = KisFuturesDerivative(
            kis_futures_app_key="k",
            kis_futures_app_secret="s",
            kis_futures_account_number="12345678-03",
            trading_pairs=trading_pairs or ["005930-KRW"],
            trading_required=False,
            kis_futures_sandbox="true",
            kis_futures_ws_enabled="false",
        )
        connector._auth._access_token = "tok"
        connector._auth._token_expires_at = _time.time() + 86400

    ds = KisFuturesAPIOrderBookDataSource(
        trading_pairs=trading_pairs or ["005930-KRW"],
        connector=connector,
        api_factory=connector._web_assistants_factory,
        auth=connector._auth,
        domain=connector.domain,
        ws_enabled=ws_enabled,
    )
    return ds, connector


def _inject_contract(connector, pair="005930-KRW", short_code="A11606"):
    from decimal import Decimal
    from hummingbot.connector.derivative.kis_futures.kis_futures_master import FuturesContract
    from hummingbot.connector.trading_rule import TradingRule
    connector._contract_by_pair[pair] = FuturesContract(
        short_code=short_code,
        standard_code="KR4A11660001",
        underlying="005930",
        expiry_yyyymm="202607",
        multiplier=Decimal("10"),
        name="test",
    )
    connector._trading_rules[pair] = TradingRule(
        trading_pair=pair,
        min_order_size=Decimal("1"),
        min_price_increment=Decimal("5"),
        min_base_amount_increment=Decimal("1"),
        buy_order_collateral_token="KRW",
        sell_order_collateral_token="KRW",
    )


# ---------------------------------------------------------------------------
# Step 1 — REST snapshot: synthetic fixture (official sample shape)
# ---------------------------------------------------------------------------

def test_order_book_snapshot_synthetic_fixture():
    """Parse the synthetic nautilus fixture (output2 bare askp/bidp keys)."""
    fixture = json.loads(
        (_FIXTURES / "rest_orderbook_stock_future_a11606.json").read_text()
    )
    ds, connector = _make_ds()
    _inject_contract(connector, short_code="A11606")

    # Wrap fixture as aioresponses payload
    async def _run():
        with aioresponses() as m:
            m.get(_re(CONSTANTS.FUT_ORDERBOOK_PATH), payload=fixture)
            return await ds._order_book_snapshot("005930-KRW")

    loop = asyncio.new_event_loop()
    msg = loop.run_until_complete(_run())
    loop.close()

    assert isinstance(msg, OrderBookMessage)
    assert msg.type == OrderBookMessageType.SNAPSHOT
    assert msg.content["trading_pair"] == "005930-KRW"
    # Fixture output2 has askp1=294500, bidp1=294000 (bare keys)
    asks = msg.content["asks"]
    bids = msg.content["bids"]
    assert len(asks) >= 1
    assert len(bids) >= 1
    assert asks[0][0] == pytest.approx(294500.0)
    assert bids[0][0] == pytest.approx(294000.0)


def test_order_book_snapshot_live_fixture():
    """Parse the live-captured nautilus fixture (output2 futs_* prefixed keys, 5 levels)."""
    fixture = json.loads(
        (_FIXTURES / "rest_orderbook_stock_future_a11606_live.json").read_text()
    )
    ds, connector = _make_ds()
    _inject_contract(connector, short_code="A11606")

    async def _run():
        with aioresponses() as m:
            m.get(_re(CONSTANTS.FUT_ORDERBOOK_PATH), payload=fixture)
            return await ds._order_book_snapshot("005930-KRW")

    loop = asyncio.new_event_loop()
    msg = loop.run_until_complete(_run())
    loop.close()

    asks = msg.content["asks"]
    bids = msg.content["bids"]
    # Live fixture: 5 ask levels (294500..296500), 5 bid levels (294000..292000)
    assert len(asks) == 5
    assert len(bids) == 5
    # Sorted ascending for asks, descending for bids
    assert asks[0][0] < asks[-1][0]
    assert bids[0][0] > bids[-1][0]
    # Spot-check first level from live fixture: futs_askp1=294500, futs_bidp1=294000
    assert asks[0][0] == pytest.approx(294500.0)
    assert bids[0][0] == pytest.approx(294000.0)
    # Size check from fixture: askp_rsqn1=55, bidp_rsqn1=74
    assert asks[0][1] == pytest.approx(55.0)
    assert bids[0][1] == pytest.approx(74.0)


def test_order_book_snapshot_rt_cd_nonzero_raises():
    """rt_cd != '0' must raise IOError (fail-closed)."""
    ds, connector = _make_ds()
    _inject_contract(connector, short_code="A11606")

    async def _run():
        with aioresponses() as m:
            m.get(
                _re(CONSTANTS.FUT_ORDERBOOK_PATH),
                payload={"rt_cd": "1", "msg1": "ERROR", "output2": {}},
            )
            return await ds._order_book_snapshot("005930-KRW")

    loop = asyncio.new_event_loop()
    with pytest.raises(IOError, match="KIS futures orderbook error"):
        loop.run_until_complete(_run())
    loop.close()


def test_order_book_snapshot_uses_front_month_code():
    """FID_INPUT_ISCD in request must be the connector's front-month short code."""
    import json as _json
    from aioresponses.core import RequestCall

    ds, connector = _make_ds()
    _inject_contract(connector, short_code="A11606")

    live_fixture = _json.loads(
        (_FIXTURES / "rest_orderbook_stock_future_a11606_live.json").read_text()
    )

    async def _run():
        with aioresponses() as m:
            m.get(_re(CONSTANTS.FUT_ORDERBOOK_PATH), payload=live_fixture)
            await ds._order_book_snapshot("005930-KRW")
            calls = list(m.requests.values())
            assert calls
            call: RequestCall = calls[0][0]
            return call.kwargs.get("params") or {}

    loop = asyncio.new_event_loop()
    params = loop.run_until_complete(_run())
    loop.close()

    assert params["FID_COND_MRKT_DIV_CODE"] == CONSTANTS.MRKT_DIV_STOCK_FUTURE
    assert params["FID_INPUT_ISCD"] == "A11606"


# ---------------------------------------------------------------------------
# Step 1 — _extract_levels_from_dict unit tests
# ---------------------------------------------------------------------------

def test_extract_levels_futs_prefix():
    """futs_askp{i} / futs_bidp{i} template is tried before bare names."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_api_order_book_data_source import (
        KisFuturesAPIOrderBookDataSource,
    )
    data = {
        "futs_askp1": "50000", "askp_rsqn1": "10",
        "futs_bidp1": "49900", "bidp_rsqn1": "20",
        "futs_askp2": "50500", "askp_rsqn2": "5",
        "futs_bidp2": "49500", "bidp_rsqn2": "8",
    }
    asks, bids = KisFuturesAPIOrderBookDataSource._extract_levels_from_dict(
        data=data, depth=5,
        ask_price_templates=["futs_askp{idx}", "askp{idx}"],
        ask_size_templates=["askp_rsqn{idx}"],
        bid_price_templates=["futs_bidp{idx}", "bidp{idx}"],
        bid_size_templates=["bidp_rsqn{idx}"],
    )
    assert len(asks) == 2
    assert len(bids) == 2
    assert asks[0] == pytest.approx((50000.0, 10.0))
    assert bids[0] == pytest.approx((49900.0, 20.0))


def test_extract_levels_bare_fallback():
    """Bare askp{i} is used when futs_askp{i} absent (synthetic fixture shape)."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_api_order_book_data_source import (
        KisFuturesAPIOrderBookDataSource,
    )
    data = {
        "askp1": "50000", "askp_rsqn1": "10",
        "bidp1": "49900", "bidp_rsqn1": "5",
    }
    asks, bids = KisFuturesAPIOrderBookDataSource._extract_levels_from_dict(
        data=data, depth=5,
        ask_price_templates=["futs_askp{idx}", "askp{idx}"],
        ask_size_templates=["askp_rsqn{idx}"],
        bid_price_templates=["futs_bidp{idx}", "bidp{idx}"],
        bid_size_templates=["bidp_rsqn{idx}"],
    )
    assert len(asks) == 1
    assert asks[0][0] == pytest.approx(50000.0)


def test_extract_levels_zero_skipped():
    """Zero price or zero size levels are excluded from the result."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_api_order_book_data_source import (
        KisFuturesAPIOrderBookDataSource,
    )
    data = {
        "futs_askp1": "0", "askp_rsqn1": "10",   # zero price → skip
        "futs_bidp1": "49900", "bidp_rsqn1": "0", # zero size → skip
    }
    asks, bids = KisFuturesAPIOrderBookDataSource._extract_levels_from_dict(
        data=data, depth=5,
        ask_price_templates=["futs_askp{idx}", "askp{idx}"],
        ask_size_templates=["askp_rsqn{idx}"],
        bid_price_templates=["futs_bidp{idx}", "bidp{idx}"],
        bid_size_templates=["bidp_rsqn{idx}"],
    )
    assert len(asks) == 0
    assert len(bids) == 0


# ---------------------------------------------------------------------------
# Step 2 — WS trade frame parsing (H0ZFCNT0)
# ---------------------------------------------------------------------------

def _make_trade_frame(
    short_code: str = "A11606",
    price: str = "294500",
    size: str = "3",
) -> str:
    """Build a synthetic 49-field H0ZFCNT0 pipe+caret WS data frame."""
    # Fill all 49 columns; only stck_prpr and last_cnqn matter for the trade message.
    fields = [""] * len(CONSTANTS.WS_TRADE_COLUMNS)
    col_idx = {col: i for i, col in enumerate(CONSTANTS.WS_TRADE_COLUMNS)}
    fields[col_idx["futs_shrn_iscd"]] = short_code
    fields[col_idx["stck_prpr"]] = price
    fields[col_idx["last_cnqn"]] = size
    data_str = "^".join(fields)
    return f"0|{CONSTANTS.WS_FUT_TRADE_TR_ID}|{short_code}|{data_str}"


def test_ws_trade_frame_produces_trade_message():
    """A valid H0ZFCNT0 frame enqueues a TRADE OrderBookMessage."""
    ds, connector = _make_ds()
    _inject_contract(connector, short_code="A11606")

    frame = _make_trade_frame(price="294500", size="3")

    async def _run():
        await ds._handle_data_message(frame)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(_run())
    loop.close()

    queue = ds._message_queue[ds._trade_messages_queue_key]
    assert not queue.empty()
    msg: OrderBookMessage = queue.get_nowait()
    assert msg.type == OrderBookMessageType.TRADE
    assert msg.content["price"] == pytest.approx(294500.0)
    assert msg.content["amount"] == pytest.approx(3.0)
    assert msg.content["trading_pair"] == "005930-KRW"


def test_ws_trade_frame_zero_price_skipped():
    """Frame with price=0 is dropped (no message enqueued)."""
    ds, connector = _make_ds()
    _inject_contract(connector, short_code="A11606")

    frame = _make_trade_frame(price="0", size="3")

    async def _run():
        await ds._handle_data_message(frame)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(_run())
    loop.close()

    queue = ds._message_queue[ds._trade_messages_queue_key]
    assert queue.empty()


def test_ws_trade_frame_wrong_field_count_skipped():
    """Frame with fewer than 49 fields is silently dropped."""
    ds, connector = _make_ds()
    _inject_contract(connector, short_code="A11606")

    short_frame = f"0|{CONSTANTS.WS_FUT_TRADE_TR_ID}|A11606|only^three^fields"

    async def _run():
        await ds._handle_data_message(short_frame)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(_run())
    loop.close()

    queue = ds._message_queue[ds._trade_messages_queue_key]
    assert queue.empty()


def test_ws_pingpong_echoed():
    """PINGPONG control message is echoed back on the websocket."""
    ds, _ = _make_ds()
    ws_mock = AsyncMock()
    pingpong_raw = json.dumps({"header": {"tr_id": "PINGPONG"}})

    async def _run():
        await ds._handle_control_message(ws_mock, pingpong_raw)

    loop = asyncio.new_event_loop()
    loop.run_until_complete(_run())
    loop.close()

    ws_mock.send_str.assert_called_once_with(pingpong_raw)


def test_ws_disabled_mode_does_not_connect():
    """ws_enabled=False: listen_for_subscriptions idles without connecting."""
    ds, _ = _make_ds(ws_enabled=False)

    async def _run():
        # Run the loop for a tiny slice; it must not raise.
        task = asyncio.create_task(ds.listen_for_subscriptions())
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    loop = asyncio.new_event_loop()
    loop.run_until_complete(_run())
    loop.close()


# ---------------------------------------------------------------------------
# FULL_ORDER_BOOK_RESET_DELTA_SECONDS constant
# ---------------------------------------------------------------------------

def test_reset_interval_matches_constant():
    from hummingbot.connector.derivative.kis_futures.kis_futures_api_order_book_data_source import (
        KisFuturesAPIOrderBookDataSource,
    )
    assert KisFuturesAPIOrderBookDataSource.FULL_ORDER_BOOK_RESET_DELTA_SECONDS == \
        CONSTANTS.REST_ORDER_BOOK_POLL_INTERVAL
