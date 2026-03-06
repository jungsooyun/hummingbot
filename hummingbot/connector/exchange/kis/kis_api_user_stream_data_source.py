"""KIS API User Stream Data Source (WebSocket-based).

Korea Investment & Securities (KIS) provides real-time execution notifications
via WebSocket:
- H0STCNI0: 국내주식 체결통보 (real environment, encrypted)
- H0STCNI9: 국내주식 체결통보 (sandbox environment, encrypted)

The notification data is AES-256-CBC encrypted.  The encryption key and IV
are provided in the first subscription response.

CNTG_YN field values:
- "2": 체결통보 (execution/fill notification)
- "1": 주문·정정·취소·거부 접수통보 (order/modify/cancel/reject acceptance)
"""

import asyncio
import json
from base64 import b64decode
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import aiohttp

from hummingbot.connector.exchange.kis import (
    kis_constants as CONSTANTS,
    kis_web_utils as web_utils,
)
from hummingbot.connector.exchange.kis.kis_auth import KisAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kis.kis_exchange import KisExchange


def _aes_cbc_decrypt(key: str, iv: str, cipher_text: str) -> str:
    """Decrypt AES-256-CBC base64-encoded data.

    Uses pycryptodome.  If not available, raises ImportError.
    """
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad

    cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
    decrypted = unpad(cipher.decrypt(b64decode(cipher_text)), AES.block_size)
    return decrypted.decode("utf-8")


class KisAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """WebSocket user stream data source for KIS execution notifications.

    Connects to the KIS WebSocket and subscribes to H0STCNI0 (real) or
    H0STCNI9 (sandbox) for real-time order execution notifications.

    The data is AES-256-CBC encrypted — the key and IV are provided in the
    first subscription response and reused for subsequent messages.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: KisAuth,
        trading_pairs: List[str],
        connector: "KisExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

        # AES decryption state (per TR_ID)
        self._encryption_keys: Dict[str, Dict[str, str]] = {}

    # ------------------------------------------------------------------ #
    # UserStreamTrackerDataSource interface
    # ------------------------------------------------------------------ #

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """Connect to KIS WebSocket and stream execution notifications."""
        while True:
            try:
                approval_key = await self._auth.get_ws_approval_key()
                ws_url = web_utils.ws_url(self._domain)

                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url) as ws:
                        await self._subscribe_exec_notifications(ws, approval_key)

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._process_ws_message(ws, msg.data, output)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error in KIS user stream WebSocket. Reconnecting..."
                )
                await self._sleep(5.0)

    async def _subscribe_exec_notifications(
        self, ws: aiohttp.ClientWebSocketResponse, approval_key: str
    ):
        """Subscribe to execution notification channel for all trading pairs."""
        sandbox = self._domain == "sandbox"
        tr_id = (
            CONSTANTS.WS_DOMESTIC_STOCK_EXEC_NOTICE_SANDBOX_TR_ID
            if sandbox
            else CONSTANTS.WS_DOMESTIC_STOCK_EXEC_NOTICE_TR_ID
        )

        for trading_pair in self._trading_pairs:
            symbol = await self._connector.exchange_symbol_associated_to_pair(
                trading_pair=trading_pair
            )
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
                        "tr_key": symbol,
                    },
                },
            }
            await ws.send_json(msg)

        self.logger().info("Subscribed to KIS execution notification channel")

    async def _process_ws_message(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        raw: str,
        output: asyncio.Queue,
    ):
        """Route incoming WS message."""
        if raw and raw[0] in ("0", "1"):
            await self._handle_data_message(raw, output)
        else:
            await self._handle_control_message(ws, raw)

    async def _handle_data_message(self, raw: str, output: asyncio.Queue):
        """Parse and decrypt execution notification data."""
        parts = raw.split("|")
        if len(parts) < 4:
            return

        tr_id = parts[1]
        data_str = parts[3]

        # Decrypt if we have keys for this TR_ID
        keys = self._encryption_keys.get(tr_id)
        if keys:
            try:
                data_str = _aes_cbc_decrypt(keys["key"], keys["iv"], data_str)
            except Exception:
                self.logger().exception("Failed to decrypt KIS WS data")
                return

        # Parse caret-delimited fields
        fields = data_str.split("^")
        columns = CONSTANTS.WS_EXEC_NOTICE_COLUMNS
        if len(fields) < len(columns):
            return

        parsed = dict(zip(columns, fields))

        # Enqueue as user stream event
        event = {
            "type": "execution_notification",
            "tr_id": tr_id,
            "data": parsed,
        }
        output.put_nowait(event)

    async def _handle_control_message(self, ws: aiohttp.ClientWebSocketResponse, raw: str):
        """Handle JSON control messages (PINGPONG, subscription responses with encryption keys)."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        header = data.get("header", {})
        tr_id = header.get("tr_id", "")

        if tr_id == "PINGPONG":
            await ws.send_str(raw)
            return

        # Store encryption keys from subscription response
        body = data.get("body", {})
        encrypt = header.get("encrypt", "N")
        if encrypt == "Y":
            output_data = body.get("output", {})
            key = output_data.get("key", "")
            iv = output_data.get("iv", "")
            if key and iv:
                self._encryption_keys[tr_id] = {"key": key, "iv": iv}

        rt_cd = body.get("rt_cd", "")
        msg1 = body.get("msg1", "")
        if rt_cd != "0":
            self.logger().warning(f"KIS user stream subscription error: {msg1}")

    # ------------------------------------------------------------------ #
    # Base class abstract methods
    # ------------------------------------------------------------------ #

    async def _connected_websocket_assistant(self) -> Optional[WSAssistant]:
        return None

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        pass

    async def _process_websocket_messages(
        self, websocket_assistant: WSAssistant, queue: asyncio.Queue
    ):
        pass
