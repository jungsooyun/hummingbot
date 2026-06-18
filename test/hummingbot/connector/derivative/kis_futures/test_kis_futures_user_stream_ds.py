"""
Tests for KisFuturesAPIUserStreamDataSource (Slice 6 / Task 5):
  - AES-256-CBC decrypt round-trip
  - Fill frame (oder_kind2='0') -> TradeUpdate
  - Lifecycle frame (oder_kind2='L') -> OrderUpdate
  - Unknown oder_kind2 -> skipped (no crash)
  - Malformed / too-short frame -> skipped
  - WS disabled mode -> idles (no connect)
  - Subscription message uses HTS ID as tr_key
  - AES key stored on control message with encrypt='Y'
"""
import asyncio
import base64
import json
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hummingbot.connector.derivative.kis_futures import kis_futures_constants as CONSTANTS
from hummingbot.connector.derivative.kis_futures.kis_futures_api_user_stream_data_source import (
    KisFuturesAPIUserStreamDataSource,
    _aes_cbc_decrypt,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ds(ws_enabled=False, hts_id="HTS_USER"):
    """Minimal DS with a fake connector (ws_enabled=False → tests run synchronously)."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    connector = KisFuturesDerivative(
        kis_futures_app_key="k",
        kis_futures_app_secret="s",
        kis_futures_account_number="12345678-03",
        kis_futures_hts_id=hts_id,
        trading_pairs=["005930-KRW"],
        trading_required=False,
        kis_futures_sandbox="true",
        kis_futures_ws_enabled="false",
        kis_futures_target_underlyings="005930",
    )
    connector._auth._access_token = "test_access_token"
    connector._auth._token_expires_at = time.time() + 86400

    api_factory = MagicMock()
    auth = connector._auth
    ds = KisFuturesAPIUserStreamDataSource(
        auth=auth,
        trading_pairs=["005930-KRW"],
        connector=connector,
        api_factory=api_factory,
        domain="sandbox",
        ws_enabled=ws_enabled,
    )
    return ds, connector


def _build_exec_notice_raw(fields: dict, tr_id: str = "H0IFCNI0") -> str:
    """Build a pipe+caret data frame with the 22 WS_EXEC_NOTICE_COLUMNS fields."""
    columns = CONSTANTS.WS_EXEC_NOTICE_COLUMNS
    values = [fields.get(col, "") for col in columns]
    data_str = "^".join(values)
    return f"0|{tr_id}|0001|{data_str}"


def _make_inflight_order(client_order_id="C1", exchange_order_id="E123",
                         trading_pair="005930-KRW",
                         trade_type=TradeType.BUY) -> InFlightOrder:
    return InFlightOrder(
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        trading_pair=trading_pair,
        order_type=OrderType.LIMIT,
        trade_type=trade_type,
        amount=Decimal("1"),
        price=Decimal("50000"),
        creation_timestamp=time.time(),
    )


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# AES-256-CBC round-trip
# ---------------------------------------------------------------------------

def test_aes_cbc_decrypt_round_trip():
    """Known key/iv/ciphertext round-trips back to the expected plaintext."""
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad

    key = "0123456789ABCDEF0123456789ABCDEF"   # 32 bytes
    iv  = "0123456789ABCDEF"                   # 16 bytes
    plaintext = "cust1^acnt^E123^oE123^1^01^0^A11607^3^294500^143000^N^0^Y^001^3^홍길동^삼성전자F^0^0^0^50000"

    cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
    encrypted = base64.b64encode(cipher.encrypt(pad(plaintext.encode("utf-8"), AES.block_size))).decode("utf-8")

    result = _aes_cbc_decrypt(key, iv, encrypted)
    assert result == plaintext


def test_aes_cbc_decrypt_known_vector():
    """Reproducible vector: encrypt then decrypt recovers original fields."""
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad

    key = "AAAABBBBCCCCDDDDAAAABBBBCCCCDDDD"
    iv  = "EEEEFFFFGGGGHHHH"
    payload = "^".join(["v"] * 22)  # 22 dummy fields

    cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
    ct = base64.b64encode(cipher.encrypt(pad(payload.encode("utf-8"), AES.block_size))).decode("utf-8")

    assert _aes_cbc_decrypt(key, iv, ct) == payload


# ---------------------------------------------------------------------------
# _handle_data_message: parse without encryption
# ---------------------------------------------------------------------------

def test_handle_data_message_enqueues_event_with_key():
    """Frame with known AES key is decrypted and enqueued (post-B1: key-before-parse guard)."""
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad as aes_pad
    ds, _ = _make_ds()
    queue = asyncio.Queue()

    # Build a plaintext payload and AES-encrypt it.
    columns = CONSTANTS.WS_EXEC_NOTICE_COLUMNS
    field_vals = {col: str(i) for i, col in enumerate(columns)}
    field_vals["oder_kind2"] = "0"
    field_vals["oder_no"] = "E999"
    plaintext = "^".join(field_vals.get(col, "") for col in columns)

    key = "0123456789ABCDEF0123456789ABCDEF"
    iv  = "0123456789ABCDEF"
    cipher = AES.new(key.encode(), AES.MODE_CBC, iv.encode())
    ct_b64 = base64.b64encode(cipher.encrypt(aes_pad(plaintext.encode(), AES.block_size))).decode()

    tr_id = "H0IFCNI0"
    raw = f"0|{tr_id}|0001|{ct_b64}"

    # Pre-store the AES key (as the subscribe-response handler would).
    ds._encryption_keys[tr_id] = {"key": key, "iv": iv}

    _run(ds._handle_data_message(raw, queue))

    assert not queue.empty()
    event = queue.get_nowait()
    assert event["type"] == "execution_notification"
    assert event["data"]["oder_no"] == "E999"


def test_handle_data_message_no_key_drops_frame():
    """Frame arriving before AES key is received is dropped (key-before-parse guard)."""
    ds, _ = _make_ds()
    queue = asyncio.Queue()
    # No key stored; even a well-formed caret frame should be dropped.
    fields = {col: str(i) for i, col in enumerate(CONSTANTS.WS_EXEC_NOTICE_COLUMNS)}
    raw = _build_exec_notice_raw(fields)
    # ds._encryption_keys is empty
    _run(ds._handle_data_message(raw, queue))
    assert queue.empty()


def test_handle_data_message_too_short_frame_dropped():
    """Frame with fewer than 4 pipe-segments is silently dropped."""
    ds, _ = _make_ds()
    queue = asyncio.Queue()
    _run(ds._handle_data_message("0|H0IFCNI0|0001", queue))
    assert queue.empty()


def test_handle_data_message_too_few_fields_dropped():
    """Frame with fewer than 22 caret fields is dropped."""
    ds, _ = _make_ds()
    queue = asyncio.Queue()
    raw = "0|H0IFCNI0|0001|only^two"
    _run(ds._handle_data_message(raw, queue))
    assert queue.empty()


# ---------------------------------------------------------------------------
# _handle_control_message: PINGPONG + AES key store
# ---------------------------------------------------------------------------

def test_handle_control_message_pingpong_echoed():
    """PINGPONG control frame is echoed back on the WS; returns True (continue)."""
    ds, _ = _make_ds()
    ws = MagicMock()
    ws.send_str = AsyncMock()

    raw = json.dumps({"header": {"tr_id": "PINGPONG"}})
    result = _run(ds._handle_control_message(ws, raw))

    ws.send_str.assert_called_once_with(raw)
    assert ds._encryption_keys == {}
    assert result is True


def test_handle_control_message_subscribe_failure_returns_false():
    """Subscribe response with rt_cd != '0' returns False (trigger reconnect)."""
    ds, _ = _make_ds()
    ws = MagicMock()
    ws.send_str = AsyncMock()

    raw = json.dumps({
        "header": {"tr_id": "H0IFCNI0", "encrypt": "N"},
        "body": {"rt_cd": "1", "msg1": "SUBSCRIBE FAILED"},
    })
    result = _run(ds._handle_control_message(ws, raw))
    assert result is False


def test_handle_control_message_success_returns_true():
    """Subscribe response with rt_cd == '0' returns True (continue)."""
    ds, _ = _make_ds()
    ws = MagicMock()
    raw = json.dumps({
        "header": {"tr_id": "H0IFCNI0", "encrypt": "N"},
        "body": {"rt_cd": "0"},
    })
    result = _run(ds._handle_control_message(ws, raw))
    assert result is True


def test_handle_control_message_stores_aes_key():
    """Subscription response with encrypt='Y' stores key/iv for that tr_id."""
    ds, _ = _make_ds()
    ws = MagicMock()
    ws.send_str = AsyncMock()

    raw = json.dumps({
        "header": {"tr_id": "H0IFCNI0", "encrypt": "Y"},
        "body": {
            "rt_cd": "0",
            "msg1": "SUBSCRIBE SUCCESS",
            "output": {
                "key": "0123456789ABCDEF0123456789ABCDEF",
                "iv": "0123456789ABCDEF",
            },
        },
    })
    _run(ds._handle_control_message(ws, raw))

    assert "H0IFCNI0" in ds._encryption_keys
    assert ds._encryption_keys["H0IFCNI0"]["key"] == "0123456789ABCDEF0123456789ABCDEF"
    assert ds._encryption_keys["H0IFCNI0"]["iv"] == "0123456789ABCDEF"


# ---------------------------------------------------------------------------
# Subscription message shape
# ---------------------------------------------------------------------------

def test_subscribe_uses_hts_id_as_tr_key():
    """Subscription JSON sets tr_key = connector._hts_id."""
    ds, connector = _make_ds(hts_id="MY_HTS")

    ws = MagicMock()
    ws.send_json = AsyncMock()

    async def _fake_get_ws_approval_key():
        return "APPROVAL_XYZ"

    with patch.object(ds._auth, "get_ws_approval_key", side_effect=_fake_get_ws_approval_key):
        _run(ds._subscribe_exec_notifications(ws, "APPROVAL_XYZ"))

    ws.send_json.assert_called_once()
    msg = ws.send_json.call_args[0][0]
    assert msg["body"]["input"]["tr_key"] == "MY_HTS"
    # Sandbox domain → sandbox tr_id
    assert msg["body"]["input"]["tr_id"] == CONSTANTS.WS_FUT_EXEC_NOTICE_SANDBOX_TR_ID


# ---------------------------------------------------------------------------
# _user_stream_event_listener (connector)
# ---------------------------------------------------------------------------

def test_user_stream_listener_fill_does_not_call_process_trade_update():
    """oder_kind2='0' (fill) must NOT call process_trade_update (B1: ccnl-only fills).

    The WS exec-notice is no longer the authoritative fill source.
    _all_trade_updates_for_order (ccnl REST poll) owns fills exclusively to
    avoid cross-source double-counting with different trade_id schemes.
    """
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    c = KisFuturesDerivative(
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
    c._auth._access_token = "test_access_token"
    c._auth._token_expires_at = time.time() + 86400

    order = _make_inflight_order(exchange_order_id="E123")
    c._order_tracker._in_flight_orders[order.client_order_id] = order

    event = {
        "type": "execution_notification",
        "tr_id": "H0IFCNI0",
        "data": {
            "oder_kind2": "0",
            "oder_no": "E123",
            "stck_shrn_iscd": "A11607",
            "cntg_qty": "2",
            "cntg_unpr": "294500",
            "stck_cntg_hour": "143000",
        },
    }

    async def _mock_iter():
        yield event

    with patch.object(c, "_iter_user_event_queue", return_value=_mock_iter()), \
         patch.object(c._order_tracker, "process_trade_update") as mock_trade, \
         patch.object(c._order_tracker, "process_order_update") as mock_order:
        _run(c._user_stream_event_listener())

    # WS fill must NOT emit a TradeUpdate (ccnl poll is the sole fill source).
    mock_trade.assert_not_called()
    # WS fill must also NOT accidentally emit an OrderUpdate.
    mock_order.assert_not_called()


def test_user_stream_listener_lifecycle_cancel():
    """oder_kind2='L', acpt_yn='Y', rctf_cls='02' → CANCELED OrderUpdate."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    c = KisFuturesDerivative(
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
    c._auth._access_token = "test_access_token"
    c._auth._token_expires_at = time.time() + 86400

    order = _make_inflight_order(exchange_order_id="E456")
    c._order_tracker._in_flight_orders[order.client_order_id] = order

    event = {
        "type": "execution_notification",
        "tr_id": "H0IFCNI0",
        "data": {
            "oder_kind2": "L",
            "oder_no": "E456",
            "stck_shrn_iscd": "A11607",
            "rfus_yn": "N",
            "acpt_yn": "Y",
            "rctf_cls": "02",
        },
    }

    async def _mock_iter():
        yield event

    with patch.object(c, "_iter_user_event_queue", return_value=_mock_iter()), \
         patch.object(c._order_tracker, "process_order_update") as mock_order:
        _run(c._user_stream_event_listener())

    mock_order.assert_called_once()
    order_update = mock_order.call_args[0][0]
    assert order_update.new_state == OrderState.CANCELED


def test_user_stream_listener_lifecycle_reject():
    """oder_kind2='L', rfus_yn='Y' → FAILED OrderUpdate."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    c = KisFuturesDerivative(
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
    c._auth._access_token = "test_access_token"
    c._auth._token_expires_at = time.time() + 86400

    order = _make_inflight_order(exchange_order_id="E789")
    c._order_tracker._in_flight_orders[order.client_order_id] = order

    event = {
        "type": "execution_notification",
        "tr_id": "H0IFCNI0",
        "data": {
            "oder_kind2": "L",
            "oder_no": "E789",
            "rfus_yn": "Y",
            "acpt_yn": "N",
            "rctf_cls": "",
        },
    }

    async def _mock_iter():
        yield event

    with patch.object(c, "_iter_user_event_queue", return_value=_mock_iter()), \
         patch.object(c._order_tracker, "process_order_update") as mock_order:
        _run(c._user_stream_event_listener())

    mock_order.assert_called_once()
    assert mock_order.call_args[0][0].new_state == OrderState.FAILED


def test_user_stream_listener_order_accept_open():
    """oder_kind2='L', acpt_yn='Y', rctf_cls != '02' → OPEN."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    c = KisFuturesDerivative(
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
    c._auth._access_token = "test_access_token"
    c._auth._token_expires_at = time.time() + 86400

    order = _make_inflight_order(exchange_order_id="E901")
    c._order_tracker._in_flight_orders[order.client_order_id] = order

    event = {
        "type": "execution_notification",
        "tr_id": "H0IFCNI0",
        "data": {
            "oder_kind2": "L",
            "oder_no": "E901",
            "rfus_yn": "N",
            "acpt_yn": "Y",
            "rctf_cls": "01",  # new order accept
        },
    }

    async def _mock_iter():
        yield event

    with patch.object(c, "_iter_user_event_queue", return_value=_mock_iter()), \
         patch.object(c._order_tracker, "process_order_update") as mock_order:
        _run(c._user_stream_event_listener())

    mock_order.assert_called_once()
    assert mock_order.call_args[0][0].new_state == OrderState.OPEN


def test_user_stream_listener_unknown_order_skipped():
    """Event with unrecognised oder_no is silently dropped (no exception)."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    c = KisFuturesDerivative(
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
    c._auth._access_token = "test_access_token"
    c._auth._token_expires_at = time.time() + 86400

    event = {
        "type": "execution_notification",
        "tr_id": "H0IFCNI0",
        "data": {
            "oder_kind2": "0",
            "oder_no": "UNKNOWN_ORDER",
            "cntg_qty": "1",
            "cntg_unpr": "50000",
        },
    }

    async def _mock_iter():
        yield event

    with patch.object(c, "_iter_user_event_queue", return_value=_mock_iter()), \
         patch.object(c._order_tracker, "process_trade_update") as mock_trade:
        _run(c._user_stream_event_listener())

    mock_trade.assert_not_called()


def test_user_stream_listener_unknown_oder_kind2_skipped():
    """Unknown oder_kind2 is warned and skipped, no exception raised."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    c = KisFuturesDerivative(
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
    c._auth._access_token = "test_access_token"
    c._auth._token_expires_at = time.time() + 86400

    order = _make_inflight_order(exchange_order_id="E555")
    c._order_tracker._in_flight_orders[order.client_order_id] = order

    event = {
        "type": "execution_notification",
        "tr_id": "H0IFCNI0",
        "data": {
            "oder_kind2": "X",  # unknown
            "oder_no": "E555",
        },
    }

    async def _mock_iter():
        yield event

    with patch.object(c, "_iter_user_event_queue", return_value=_mock_iter()), \
         patch.object(c._order_tracker, "process_trade_update") as mock_trade, \
         patch.object(c._order_tracker, "process_order_update") as mock_order:
        _run(c._user_stream_event_listener())  # must not raise

    mock_trade.assert_not_called()
    mock_order.assert_not_called()


def test_user_stream_listener_malformed_fill_skipped():
    """Fill event with non-numeric cntg_qty is warned and skipped."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    c = KisFuturesDerivative(
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
    c._auth._access_token = "test_access_token"
    c._auth._token_expires_at = time.time() + 86400

    order = _make_inflight_order(exchange_order_id="E777")
    c._order_tracker._in_flight_orders[order.client_order_id] = order

    event = {
        "type": "execution_notification",
        "tr_id": "H0IFCNI0",
        "data": {
            "oder_kind2": "0",
            "oder_no": "E777",
            "cntg_qty": "NaN",  # malformed
            "cntg_unpr": "not_a_number",
        },
    }

    async def _mock_iter():
        yield event

    with patch.object(c, "_iter_user_event_queue", return_value=_mock_iter()), \
         patch.object(c._order_tracker, "process_trade_update") as mock_trade:
        _run(c._user_stream_event_listener())

    mock_trade.assert_not_called()


def test_user_stream_listener_non_execution_event_skipped():
    """Events with type != 'execution_notification' are silently skipped."""
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    c = KisFuturesDerivative(
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
    c._auth._access_token = "test_access_token"
    c._auth._token_expires_at = time.time() + 86400

    event = {"type": "order_book_update", "data": {}}

    async def _mock_iter():
        yield event

    with patch.object(c, "_iter_user_event_queue", return_value=_mock_iter()), \
         patch.object(c._order_tracker, "process_trade_update") as mock_trade, \
         patch.object(c._order_tracker, "process_order_update") as mock_order:
        _run(c._user_stream_event_listener())

    mock_trade.assert_not_called()
    mock_order.assert_not_called()
