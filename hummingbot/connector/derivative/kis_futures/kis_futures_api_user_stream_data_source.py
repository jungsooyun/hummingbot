"""
KIS Futures API User Stream Data Source.

Subscribes to H0IFCNI0 (live) / H0IFCNI9 (sandbox/paper) for real-time
execution notifications (체결통보).  Messages are AES-256-CBC encrypted;
the key and IV are provided in the first subscription response.

tr_key = connector._hts_id (HTS 사용자 ID — NOT a symbol code).

H0IFCNI0 field layout: 22 fields in CONSTANTS.WS_EXEC_NOTICE_COLUMNS.
oder_kind2:
  '0' — 체결통보 (fill)
  'L' — 주문·정정·취소·거부 접수통보 (order-lifecycle)
"""
import asyncio
import json
from base64 import b64decode
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import aiohttp

from hummingbot.connector.derivative.kis_futures import (
    kis_futures_constants as CONSTANTS,
    kis_futures_web_utils as web_utils,
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    from hummingbot.connector.exchange.kis.kis_auth import KisAuth


def _aes_cbc_decrypt(key: str, iv: str, cipher_text: str) -> str:
    """Decrypt AES-256-CBC base64-encoded data (pycryptodome)."""
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad

    cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
    decrypted = unpad(cipher.decrypt(b64decode(cipher_text)), AES.block_size)
    return decrypted.decode("utf-8")


class KisFuturesAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """WebSocket user stream for KIS futures execution notifications.

    Architecture:
    - Subscribes a single channel (tr_key = HTS ID, one per account).
    - Decrypts AES-256-CBC frames using the key/iv from the subscription response.
    - Enqueues dicts of shape {'type':'execution_notification', 'tr_id':..., 'data':{...}}
      to the output queue consumed by the connector's _user_stream_event_listener.
    - Best-effort: connector readiness does NOT depend on this stream
      (_is_user_stream_initialized returns True unconditionally).  Fills are also
      reconciled by the REST ccnl poll in _all_trade_updates_for_order.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: "KisAuth",
        trading_pairs: List[str],
        connector: "KisFuturesDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        ws_enabled: bool = True,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._ws_enabled = ws_enabled

        # AES decryption state: {tr_id: {'key': ..., 'iv': ...}}
        self._encryption_keys: Dict[str, Dict[str, str]] = {}
        self._ws_failures = 0

    # ------------------------------------------------------------------
    # UserStreamTrackerDataSource interface
    # ------------------------------------------------------------------

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """Connect to KIS WebSocket and stream decrypted execution notifications."""
        if not self._ws_enabled:
            self.logger().info(
                "[kis_futures] exec-notice WebSocket disabled (kis_futures_ws_enabled=false); "
                "fills tracked via REST ccnl polling."
            )
            while True:
                await self._sleep(3600)

        while True:
            # Clear per-connect AES key state so stale keys from a previous
            # connection are never reused on a fresh reconnect.  The exchange
            # sends new key/iv in the subscription response for each connect.
            self._encryption_keys = {}
            self._subscribe_succeeded = False

            try:
                approval_key = await self._auth.get_ws_approval_key()
                ws_url = web_utils.ws_url(self._domain)

                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url) as ws:
                        await self._subscribe_exec_notifications(ws, approval_key)
                        self._ws_failures = 0

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                if not await self._process_ws_message(ws, msg.data, output):
                                    # Subscribe failure: break to trigger reconnect.
                                    break
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                raise
            except Exception:
                self._ws_failures += 1
                delay = web_utils.reconnect_backoff(self._ws_failures)
                if self._ws_failures == 1:
                    self.logger().exception(
                        "[kis_futures] user stream WebSocket failed; retrying with backoff."
                    )
                else:
                    self.logger().warning(
                        f"[kis_futures] user stream WebSocket unavailable "
                        f"(attempt {self._ws_failures}); retrying in {delay:.0f}s."
                    )
                await self._sleep(delay)

    async def _subscribe_exec_notifications(
        self, ws: aiohttp.ClientWebSocketResponse, approval_key: str
    ):
        """Subscribe H0IFCNI0 (live) or H0IFCNI9 (sandbox) with the HTS ID as tr_key."""
        sandbox = self._domain == "sandbox"
        tr_id = (
            CONSTANTS.WS_FUT_EXEC_NOTICE_SANDBOX_TR_ID
            if sandbox
            else CONSTANTS.WS_FUT_EXEC_NOTICE_TR_ID
        )
        # tr_key for futures exec-notice is the HTS user ID, not a symbol code.
        hts_id = self._connector._hts_id if self._connector is not None else ""
        msg = {
            "header": {
                "approval_key": approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": hts_id,
                },
            },
        }
        await ws.send_json(msg)
        self.logger().info(
            f"[kis_futures] Subscribed to {tr_id} (exec-notice), tr_key={hts_id!r}"
        )

    async def _process_ws_message(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        raw: str,
        output: asyncio.Queue,
    ) -> bool:
        """Route incoming WS frame to data or control handler.

        Returns False if the caller should break and reconnect (subscribe failure).
        """
        if raw and raw[0] in ("0", "1"):
            await self._handle_data_message(raw, output)
            return True
        else:
            return await self._handle_control_message(ws, raw)

    async def _handle_data_message(self, raw: str, output: asyncio.Queue):
        """Decrypt and parse an exec-notice data frame, then enqueue.

        Key-before-parse guard: if a data frame arrives for a tr_id whose AES key
        has not yet been received (i.e. the subscription response hasn't come back),
        the frame MUST NOT be parsed as plaintext — it is dropped with a warning.
        Parsing encrypted ciphertext as plaintext would corrupt order state.
        """
        parts = raw.split("|")
        if len(parts) < 4:
            self.logger().warning(f"[kis_futures] short user stream frame: {raw[:80]}")
            return

        tr_id = parts[1]
        data_str = parts[3]

        keys = self._encryption_keys.get(tr_id)
        if keys:
            try:
                data_str = _aes_cbc_decrypt(keys["key"], keys["iv"], data_str)
            except Exception:
                self.logger().exception("[kis_futures] AES decrypt failed — dropping frame")
                return
        else:
            # Key not yet received: drop the frame rather than parsing ciphertext as plaintext.
            self.logger().warning(
                f"[kis_futures] encrypted frame received for tr_id={tr_id} before AES key "
                f"was stored — dropping (expected during subscription race; normal on first connect)."
            )
            return

        fields = data_str.split("^")
        columns = CONSTANTS.WS_EXEC_NOTICE_COLUMNS
        if len(fields) < len(columns):
            self.logger().warning(
                f"[kis_futures] exec-notice frame has {len(fields)} fields, "
                f"expected {len(columns)} — dropping"
            )
            return

        parsed = dict(zip(columns, fields))
        output.put_nowait({
            "type": "execution_notification",
            "tr_id": tr_id,
            "data": parsed,
        })

    async def _handle_control_message(self, ws: aiohttp.ClientWebSocketResponse, raw: str) -> bool:
        """Handle JSON control frames: PINGPONG echo + store AES key/iv.

        Returns False if a subscribe failure is detected (rt_cd != "0") so the
        caller can break and reconnect rather than silently continuing.
        """
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.logger().warning(f"[kis_futures] invalid JSON user stream frame: {raw[:80]}")
            return True

        header = data.get("header", {})
        tr_id = header.get("tr_id", "")

        if tr_id == "PINGPONG":
            await ws.send_str(raw)
            return True

        # Store AES key/iv on subscription response.
        body = data.get("body", {})
        if header.get("encrypt", "N") == "Y":
            output_data = body.get("output", {})
            key = output_data.get("key", "")
            iv = output_data.get("iv", "")
            if key and iv:
                self._encryption_keys[tr_id] = {"key": key, "iv": iv}
                self.logger().info(
                    f"[kis_futures] stored AES key for tr_id={tr_id}"
                )

        rt_cd = body.get("rt_cd", "0")
        if rt_cd != "0":
            self.logger().warning(
                f"[kis_futures] subscribe failed rt_cd={rt_cd} msg={body.get('msg1')} "
                f"— will reconnect."
            )
            return False  # signal caller to break + reconnect
        return True

    # ------------------------------------------------------------------
    # Base-abstract no-ops (custom WS loop above replaces the framework)
    # ------------------------------------------------------------------

    async def _connected_websocket_assistant(self) -> Optional[WSAssistant]:
        return None

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        pass

    async def _process_websocket_messages(
        self, websocket_assistant: WSAssistant, queue: asyncio.Queue
    ):
        pass
