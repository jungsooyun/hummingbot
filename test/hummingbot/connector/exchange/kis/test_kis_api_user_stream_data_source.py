import asyncio
import json
import unittest
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.kis import kis_constants as CONSTANTS
from hummingbot.connector.exchange.kis.kis_api_user_stream_data_source import (
    KisAPIUserStreamDataSource,
    _aes_cbc_decrypt,
)
from hummingbot.connector.exchange.kis.kis_auth import KisAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class _FakeAsyncCtx:
    """Helper to mock ``async with`` pattern."""

    def __init__(self, obj):
        self._obj = obj

    async def __aenter__(self):
        return self._obj

    async def __aexit__(self, *args):
        pass


class _AsyncIter:
    """Wrap a regular iterable so ``async for`` works."""

    def __init__(self, items):
        self._items = iter(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration


class _FakeWS:
    """Fake aiohttp WebSocket that supports ``async for`` iteration."""

    def __init__(self, messages):
        self._messages = messages
        self.send_json = AsyncMock()
        self.send_str = AsyncMock()

    def __aiter__(self):
        return _AsyncIter(self._messages)


class KisAPIUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    """Tests for the KIS WebSocket-based user stream data source.

    KIS provides real-time execution notifications via WebSocket:
    - H0STCNI0: domestic stock execution notices (real environment, encrypted)
    - H0STCNI9: domestic stock execution notices (sandbox environment, encrypted)
    """

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.trading_pair = "005930-KRW"  # Samsung Electronics

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.auth = KisAuth(
            app_key="test_app_key",
            app_secret="test_app_secret",
            sandbox=True,
            initial_token="test_token",
            initial_approval_key="test_approval_key",
        )
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(return_value="005930")
        self.api_factory = MagicMock()

        self.data_source = KisAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.api_factory,
            hub=MagicMock(),
            domain="sandbox",
        )

    # ------------------------------------------------------------------ #
    # Test: instantiation
    # ------------------------------------------------------------------ #

    async def test_instance_creation(self):
        """The data source can be created and is the right type."""
        self.assertIsInstance(self.data_source, KisAPIUserStreamDataSource)
        self.assertIsInstance(self.data_source, UserStreamTrackerDataSource)

    async def test_instance_creation_default_domain(self):
        """The data source accepts default domain."""
        ds = KisAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.api_factory,
            hub=MagicMock(),
        )
        self.assertIsInstance(ds, KisAPIUserStreamDataSource)
        self.assertEqual(ds._domain, CONSTANTS.DEFAULT_DOMAIN)

    async def test_instance_creation_with_sandbox_domain(self):
        """The data source stores the domain parameter."""
        self.assertEqual(self.data_source._domain, "sandbox")

    # ------------------------------------------------------------------ #
    # Test: last_recv_time always returns a value
    # ------------------------------------------------------------------ #

    async def test_last_recv_time_returns_value(self):
        """last_recv_time should return a numeric value (0 when no WS is active)."""
        recv_time = self.data_source.last_recv_time
        self.assertIsInstance(recv_time, (int, float))
        self.assertEqual(recv_time, 0)

    async def test_ws_disabled_never_connects(self):
        # kis_ws_enabled=false -> listen_for_user_stream must NOT touch the WS edge
        # (no approval-key fetch): fills are caught via REST order-status polling.
        self.data_source._ws_enabled = False
        self.data_source._auth.get_ws_approval_key = AsyncMock()
        with patch.object(self.data_source, "_sleep", new_callable=AsyncMock) as sleep_mock:
            sleep_mock.side_effect = asyncio.CancelledError
            with self.assertRaises(asyncio.CancelledError):
                await self.data_source.listen_for_user_stream(asyncio.Queue())
        self.data_source._auth.get_ws_approval_key.assert_not_called()
        sleep_mock.assert_awaited_once()

    # ------------------------------------------------------------------ #
    # Test: _connected_websocket_assistant returns None
    # ------------------------------------------------------------------ #

    async def test_connected_websocket_assistant_returns_none(self):
        """_connected_websocket_assistant should return None (not used by KIS WS)."""
        result = await self.data_source._connected_websocket_assistant()
        self.assertIsNone(result)

    # ------------------------------------------------------------------ #
    # Test: _subscribe_channels is a no-op
    # ------------------------------------------------------------------ #

    async def test_subscribe_channels_is_noop(self):
        """_subscribe_channels should complete without error."""
        await self.data_source._subscribe_channels(websocket_assistant=None)

    # ------------------------------------------------------------------ #
    # Test: _process_websocket_messages is a no-op
    # ------------------------------------------------------------------ #

    async def test_process_websocket_messages_is_noop(self):
        """_process_websocket_messages should complete without error."""
        queue = asyncio.Queue()
        await self.data_source._process_websocket_messages(
            websocket_assistant=None, queue=queue
        )
        self.assertTrue(queue.empty())

    # ------------------------------------------------------------------ #
    # Test: _handle_data_message — fill notification (CNTG_YN=2)
    # ------------------------------------------------------------------ #

    async def test_handle_data_message_fill_notification(self):
        """Execution notification with CNTG_YN='2' should be enqueued as a fill event."""
        # Build pipe-delimited data message:
        # format: encrypted_flag|tr_id|count|data
        # data is caret-delimited fields matching WS_EXEC_NOTICE_COLUMNS
        fields = [
            "CUST123",         # CUST_ID
            "12345678-01",     # ACNT_NO
            "0000001",         # ODER_NO
            "0000000",         # OODER_NO
            "02",              # SELN_BYOV_CLS
            "00",              # RCTF_CLS
            "00",              # ODER_KIND
            "0",               # ODER_COND
            "005930",          # STCK_SHRN_ISCD
            "100",             # CNTG_QTY
            "67800",           # CNTG_UNPR
            "093001",          # STCK_CNTG_HOUR
            "N",               # RFUS_YN
            "2",               # CNTG_YN  <-- fill
            "Y",               # ACPT_YN
            "1234",            # BRNC_NO
            "100",             # ODER_QTY
            "테스트계좌",       # ACNT_NAME
            "0",               # ORD_COND_PRC
            "00",              # ORD_EXG_GB
            "N",               # POPUP_YN
            "",                # FILLER
            "00",              # CRDT_CLS
            "20260228",        # CRDT_LOAN_DATE
            "삼성전자",         # CNTG_ISNM40
            "67800",           # ODER_PRC
        ]
        data_str = "^".join(fields)
        raw = f"0|H0STCNI0|001|{data_str}"

        output = asyncio.Queue()
        await self.data_source._handle_data_message(raw, output)

        self.assertFalse(output.empty())
        event = output.get_nowait()
        self.assertEqual(event["type"], "execution_notification")
        self.assertEqual(event["tr_id"], "H0STCNI0")
        self.assertEqual(event["data"]["CNTG_YN"], "2")
        self.assertEqual(event["data"]["STCK_SHRN_ISCD"], "005930")
        self.assertEqual(event["data"]["CNTG_QTY"], "100")
        self.assertEqual(event["data"]["CNTG_UNPR"], "67800")
        self.assertEqual(event["data"]["ACNT_NO"], "12345678-01")
        self.assertEqual(event["data"]["ODER_NO"], "0000001")
        self.assertEqual(event["data"]["SELN_BYOV_CLS"], "02")

    # ------------------------------------------------------------------ #
    # Test: _handle_data_message — order acceptance (CNTG_YN=1)
    # ------------------------------------------------------------------ #

    async def test_handle_data_message_order_acceptance(self):
        """Execution notification with CNTG_YN='1' should be enqueued as an order acceptance."""
        fields = [
            "CUST123",         # CUST_ID
            "12345678-01",     # ACNT_NO
            "0000002",         # ODER_NO
            "0000000",         # OODER_NO
            "01",              # SELN_BYOV_CLS (buy)
            "00",              # RCTF_CLS
            "00",              # ODER_KIND
            "0",               # ODER_COND
            "005930",          # STCK_SHRN_ISCD
            "0",               # CNTG_QTY (no fill yet)
            "0",               # CNTG_UNPR
            "093500",          # STCK_CNTG_HOUR
            "N",               # RFUS_YN
            "1",               # CNTG_YN  <-- order acceptance
            "Y",               # ACPT_YN
            "1234",            # BRNC_NO
            "50",              # ODER_QTY
            "테스트계좌",       # ACNT_NAME
            "68000",           # ORD_COND_PRC
            "00",              # ORD_EXG_GB
            "N",               # POPUP_YN
            "",                # FILLER
            "00",              # CRDT_CLS
            "20260228",        # CRDT_LOAN_DATE
            "삼성전자",         # CNTG_ISNM40
            "68000",           # ODER_PRC
        ]
        data_str = "^".join(fields)
        raw = f"0|H0STCNI0|001|{data_str}"

        output = asyncio.Queue()
        await self.data_source._handle_data_message(raw, output)

        self.assertFalse(output.empty())
        event = output.get_nowait()
        self.assertEqual(event["type"], "execution_notification")
        self.assertEqual(event["tr_id"], "H0STCNI0")
        self.assertEqual(event["data"]["CNTG_YN"], "1")
        self.assertEqual(event["data"]["ODER_QTY"], "50")
        self.assertEqual(event["data"]["SELN_BYOV_CLS"], "01")

    # ------------------------------------------------------------------ #
    # Test: _handle_data_message — too few pipe segments is ignored
    # ------------------------------------------------------------------ #

    async def test_handle_data_message_too_few_parts_ignored(self):
        """Messages with fewer than 4 pipe-delimited parts should be silently ignored."""
        output = asyncio.Queue()
        await self.data_source._handle_data_message("0|H0STCNI0|001", output)
        self.assertTrue(output.empty())

    # ------------------------------------------------------------------ #
    # Test: _handle_data_message — too few caret fields is ignored
    # ------------------------------------------------------------------ #

    async def test_handle_data_message_too_few_fields_ignored(self):
        """Messages with fewer fields than WS_EXEC_NOTICE_COLUMNS should be ignored."""
        output = asyncio.Queue()
        # Only 3 caret-delimited fields, far fewer than required columns
        await self.data_source._handle_data_message("0|H0STCNI0|001|A^B^C", output)
        self.assertTrue(output.empty())

    # ------------------------------------------------------------------ #
    # Test: _handle_control_message — stores encryption keys
    # ------------------------------------------------------------------ #

    async def test_handle_control_message_stores_encryption_keys(self):
        """Subscription response with encrypt='Y' should store encryption keys."""
        raw = json.dumps({
            "header": {
                "tr_id": "H0STCNI0",
                "tr_key": "005930",
                "encrypt": "Y",
            },
            "body": {
                "rt_cd": "0",
                "msg1": "SUBSCRIBE SUCCESS",
                "output": {
                    "key": "abcdefghijklmnopqrstuvwxyz123456",
                    "iv": "1234567890123456",
                },
            },
        })

        await self.data_source._handle_control_message(raw)

        self.assertIn("H0STCNI0", self.data_source._encryption_keys)
        keys = self.data_source._encryption_keys["H0STCNI0"]
        self.assertEqual(keys["key"], "abcdefghijklmnopqrstuvwxyz123456")
        self.assertEqual(keys["iv"], "1234567890123456")

    # ------------------------------------------------------------------ #
    # Test: _handle_control_message — does not store keys when encrypt=N
    # ------------------------------------------------------------------ #

    async def test_handle_control_message_no_keys_when_encrypt_n(self):
        """Subscription response with encrypt='N' should not store encryption keys."""
        raw = json.dumps({
            "header": {
                "tr_id": "H0STCNI0",
                "tr_key": "005930",
                "encrypt": "N",
            },
            "body": {
                "rt_cd": "0",
                "msg1": "SUBSCRIBE SUCCESS",
                "output": {},
            },
        })

        await self.data_source._handle_control_message(raw)
        self.assertNotIn("H0STCNI0", self.data_source._encryption_keys)

    # ------------------------------------------------------------------ #
    # Test: _handle_control_message — invalid JSON is silently ignored
    # ------------------------------------------------------------------ #

    async def test_handle_control_message_invalid_json_ignored(self):
        """Invalid JSON control messages should be silently ignored."""
        await self.data_source._handle_control_message("not valid json{{{")
        self.assertEqual({}, self.data_source._encryption_keys)

    # ------------------------------------------------------------------ #
    # Test: _handle_control_message — subscription error logs warning
    # ------------------------------------------------------------------ #

    async def test_handle_control_message_subscription_error(self):
        """Subscription error (rt_cd != '0') should log a warning."""
        raw = json.dumps({
            "header": {
                "tr_id": "H0STCNI0",
                "tr_key": "005930",
                "encrypt": "N",
            },
            "body": {
                "rt_cd": "1",
                "msg1": "SUBSCRIPTION FAILED",
            },
        })

        with patch.object(self.data_source, "logger") as mock_logger:
            await self.data_source._handle_control_message(raw)
            mock_logger.return_value.warning.assert_called_once()

    # ------------------------------------------------------------------ #
    # Test: WebSocket endpoint URL (nautilus-proven, no /tryitout path)
    # ------------------------------------------------------------------ #

    def test_ws_url_constants_have_no_tryitout_path(self):
        """KIS realtime WS endpoint is the bare host:port with no path.

        ``/tryitout`` is the testbed web-form path and causes a
        ServerDisconnectedError during the handshake. The nautilus_trader
        live-tested adapter uses ``ws://ops.koreainvestment.com:21000``.
        """
        self.assertNotIn("/tryitout", CONSTANTS.WS_URL)
        self.assertNotIn("/tryitout", CONSTANTS.WS_SANDBOX_URL)
        self.assertEqual("ws://ops.koreainvestment.com:21000", CONSTANTS.WS_URL)
        self.assertEqual("ws://ops.koreainvestment.com:31000", CONSTANTS.WS_SANDBOX_URL)

    # ------------------------------------------------------------------ #
    # Test: AES-256-CBC round-trip decryption (nautilus parity)
    # ------------------------------------------------------------------ #

    def test_aes_cbc_decrypt_round_trip(self):
        """_aes_cbc_decrypt should recover plaintext from a real KIS-style
        AES-256-CBC + base64 payload (32-byte key, 16-byte IV, PKCS7 pad).

        This mirrors the nautilus_trader decrypt_private_execution_payload
        contract (AES.new(key, MODE_CBC, iv) -> unpad(b64decode(ct))).
        """
        from base64 import b64encode

        from Crypto.Cipher import AES
        from Crypto.Util.Padding import pad

        key = "abcdefghijklmnopqrstuvwxyz123456"  # 32 bytes
        iv = "1234567890123456"  # 16 bytes
        plaintext = "005930^2^100^67800^093001"

        cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
        cipher_text_b64 = b64encode(
            cipher.encrypt(pad(plaintext.encode("utf-8"), AES.block_size))
        ).decode("utf-8")

        recovered = _aes_cbc_decrypt(key, iv, cipher_text_b64)
        self.assertEqual(plaintext, recovered)

    # ------------------------------------------------------------------ #
    # Test: _on_ws_frame — routes data messages
    # ------------------------------------------------------------------ #

    async def test_on_ws_frame_routes_data_message(self):
        """Messages starting with '0' or '1' should be routed to _handle_data_message."""
        fields = "^".join(["F"] * len(CONSTANTS.WS_EXEC_NOTICE_COLUMNS))
        raw = f"0|H0STCNI0|001|{fields}"

        output = asyncio.Queue()
        self.data_source._output = output

        await self.data_source._on_ws_frame(raw)

        self.assertFalse(output.empty())
        event = output.get_nowait()
        self.assertEqual(event["type"], "execution_notification")

    # ------------------------------------------------------------------ #
    # Test: _on_ws_frame — routes control messages
    # ------------------------------------------------------------------ #

    async def test_on_ws_frame_routes_control_message(self):
        """JSON messages should be routed to _handle_control_message."""
        raw = json.dumps({"header": {"tr_id": "H0STCNI0", "encrypt": "N"}, "body": {"rt_cd": "0"}})

        with patch.object(self.data_source, "_handle_control_message", new_callable=AsyncMock) as mock_handle:
            await self.data_source._on_ws_frame(raw)

        mock_handle.assert_awaited_once_with(raw)

    async def test_on_ws_frame_aes_capture_then_decrypt_and_enqueue(self):
        out = asyncio.Queue()
        self.data_source._output = out
        ctrl = json.dumps({
            "header": {"tr_id": "H0STCNI0", "encrypt": "Y"},
            "body": {"rt_cd": "0", "output": {"key": "k" * 32, "iv": "i" * 16}},
        })
        await self.data_source._on_ws_frame(ctrl)
        self.assertIn("H0STCNI0", self.data_source._encryption_keys)

        with patch(
            "hummingbot.connector.exchange.kis.kis_api_user_stream_data_source._aes_cbc_decrypt",
            return_value="^".join(["x"] * len(CONSTANTS.WS_EXEC_NOTICE_COLUMNS)),
        ):
            await self.data_source._on_ws_frame("0|H0STCNI0|005930|cipher")

        event = out.get_nowait()
        self.assertEqual("execution_notification", event["type"])
        self.assertEqual("H0STCNI0", event["tr_id"])

    async def test_exec_notice_does_not_lengthen_rest_poll(self):
        out = asyncio.Queue()
        self.data_source._output = out
        ctrl = json.dumps({
            "header": {"tr_id": "H0STCNI0", "encrypt": "Y"},
            "body": {"rt_cd": "0", "output": {"key": "k" * 32, "iv": "i" * 16}},
        })
        await self.data_source._on_ws_frame(ctrl)
        with patch(
            "hummingbot.connector.exchange.kis.kis_api_user_stream_data_source._aes_cbc_decrypt",
            return_value="^".join(["x"] * len(CONSTANTS.WS_EXEC_NOTICE_COLUMNS)),
        ):
            await self.data_source._on_ws_frame("0|H0STCNI0|005930|cipher")

        self.assertEqual(0.0, self.data_source.last_recv_time)

    # ------------------------------------------------------------------ #
    # Test: _handle_data_message with encrypted data (decryption path)
    # ------------------------------------------------------------------ #

    async def test_handle_data_message_with_encryption_keys(self):
        """When encryption keys are stored, data should be decrypted before parsing."""
        # Pre-store encryption keys
        self.data_source._encryption_keys["H0STCNI0"] = {
            "key": "abcdefghijklmnopqrstuvwxyz123456",
            "iv": "1234567890123456",
        }

        # Build caret-delimited fields
        fields = "^".join(["F"] * len(CONSTANTS.WS_EXEC_NOTICE_COLUMNS))

        # Mock _aes_cbc_decrypt to return the caret-delimited fields
        with patch(
            "hummingbot.connector.exchange.kis.kis_api_user_stream_data_source._aes_cbc_decrypt",
            return_value=fields,
        ) as mock_decrypt:
            raw = "0|H0STCNI0|001|encrypted_base64_data"
            output = asyncio.Queue()
            await self.data_source._handle_data_message(raw, output)

            mock_decrypt.assert_called_once_with(
                "abcdefghijklmnopqrstuvwxyz123456",
                "1234567890123456",
                "encrypted_base64_data",
            )
            self.assertFalse(output.empty())
            event = output.get_nowait()
            self.assertEqual(event["type"], "execution_notification")

    # ------------------------------------------------------------------ #
    # Test: _handle_data_message — decryption failure is handled gracefully
    # ------------------------------------------------------------------ #

    async def test_handle_data_message_decryption_failure(self):
        """If decryption fails, the message should be silently dropped."""
        self.data_source._encryption_keys["H0STCNI0"] = {
            "key": "badkey",
            "iv": "badiv",
        }

        with patch(
            "hummingbot.connector.exchange.kis.kis_api_user_stream_data_source._aes_cbc_decrypt",
            side_effect=Exception("decryption failed"),
        ):
            raw = "0|H0STCNI0|001|bad_encrypted_data"
            output = asyncio.Queue()
            await self.data_source._handle_data_message(raw, output)

            self.assertTrue(output.empty())

    # ------------------------------------------------------------------ #
    # Test: listen_for_user_stream — WebSocket mocking
    # ------------------------------------------------------------------ #

    async def test_listen_registers_exec_notice_with_hub(self):
        hub = MagicMock()
        hub.register = AsyncMock()
        hub.unregister = AsyncMock()
        self.data_source._hub = hub
        self.data_source._domain = CONSTANTS.DEFAULT_DOMAIN
        out = asyncio.Queue()
        with patch.object(
            self.connector,
            "exchange_symbol_associated_to_pair",
            new=AsyncMock(return_value="005930"),
        ):
            task = asyncio.create_task(self.data_source.listen_for_user_stream(out))
            await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertEqual("H0STCNI0", hub.register.await_args_list[0].args[0])
        self.assertEqual("H0STCNI0", hub.unregister.await_args_list[0].args[0])

    # ------------------------------------------------------------------ #
    # Test: AES decryption function
    # ------------------------------------------------------------------ #

    async def test_aes_decryption(self):
        """Test the _aes_cbc_decrypt function with known plaintext."""
        try:
            from Crypto.Cipher import AES
            from Crypto.Util.Padding import pad
            from base64 import b64encode
        except ImportError:
            self.skipTest("pycryptodome not installed; skipping AES test")

        key = "abcdefghijklmnopqrstuvwxyz123456"  # 32 bytes
        iv = "1234567890123456"                    # 16 bytes
        plaintext = "hello world test data"

        # Encrypt the plaintext to create test data
        cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
        padded = pad(plaintext.encode("utf-8"), AES.block_size)
        encrypted = b64encode(cipher.encrypt(padded)).decode("utf-8")

        # Decrypt using the module-level function
        result = _aes_cbc_decrypt(key, iv, encrypted)
        self.assertEqual(result, plaintext)

    # ------------------------------------------------------------------ #
    # Test: encryption keys dict starts empty
    # ------------------------------------------------------------------ #

    async def test_encryption_keys_initially_empty(self):
        """_encryption_keys should be empty on initialization."""
        self.assertEqual(self.data_source._encryption_keys, {})
