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

from hummingbot.connector.exchange.kis import kis_constants as CONSTANTS
from hummingbot.connector.exchange.kis.kis_auth import KisAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kis.kis_exchange import KisExchange
    from hummingbot.connector.exchange.kis.kis_ws_hub import KisWsHub


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
        hub: "KisWsHub",
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        ws_enabled: bool = True,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._hub = hub
        self._domain = domain
        self._ws_enabled = ws_enabled
        self._output: Optional[asyncio.Queue] = None

        # AES decryption state (per TR_ID)
        self._encryption_keys: Dict[str, Dict[str, str]] = {}
        self._listen_gen = 0

    # ------------------------------------------------------------------ #
    # UserStreamTrackerDataSource interface
    # ------------------------------------------------------------------ #

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """Register KIS execution notifications on the shared WS hub."""
        if not self._ws_enabled:
            # REST-only mode: never touch the WS edge. Fills are caught via REST
            # order-status polling (readiness is decoupled from the user stream).
            self.logger().info(
                "KIS execution-notice WebSocket disabled (kis_ws_enabled=false); "
                "fills tracked via REST order-status polling."
            )
            while True:
                await self._sleep(3600)

        self._output = output
        self._listen_gen += 1
        my_gen = self._listen_gen
        sandbox = self._domain == "sandbox"
        tr_id = (
            CONSTANTS.WS_DOMESTIC_STOCK_EXEC_NOTICE_SANDBOX_TR_ID
            if sandbox
            else CONSTANTS.WS_DOMESTIC_STOCK_EXEC_NOTICE_TR_ID
        )
        symbols = [
            await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            for trading_pair in self._trading_pairs
        ]
        try:
            for symbol in symbols:
                await self._hub.register(tr_id, symbol, self._on_ws_frame)
            self.logger().info("Registered KIS execution-notice channel on the shared WS hub")
            while True:
                await self._sleep(3600)
        finally:
            if my_gen == self._listen_gen:
                for symbol in symbols:
                    await self._hub.unregister(tr_id, symbol)

    async def _on_ws_frame(self, raw: str):
        if raw and raw[0] in ("0", "1"):
            await self._handle_data_message(raw, self._output)
        else:
            await self._handle_control_message(raw)

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

    async def _handle_control_message(self, raw: str):
        """Capture AES key/iv from subscription responses. PINGPONG is echoed by the hub."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        header = data.get("header", {})
        tr_id = header.get("tr_id", "")

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
        if rt_cd not in ("", "0"):
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
