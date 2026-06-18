"""
KIS Futures API User Stream Data Source — 4a stub (REST-only fill polling).

WebSocket execution notifications (H0IFCNI0) are added in slice 5 (Task 4).
This stub keeps the connector alive in REST-only mode by idling the listen loop.
"""
import asyncio
from typing import TYPE_CHECKING, List, Optional

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    from hummingbot.connector.exchange.kis.kis_auth import KisAuth


class KisFuturesAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """Stub user-stream DS; WebSocket exec-notice (H0IFCNI0) added in slice 5."""

    def __init__(
        self,
        auth: Optional["KisAuth"],
        trading_pairs: List[str],
        connector: Optional["KisFuturesDerivative"],
        api_factory: Optional[WebAssistantsFactory],
        domain: str = "",
        ws_enabled: bool = True,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._ws_enabled = ws_enabled

    # ------------------------------------------------------------------
    # UserStreamTrackerDataSource abstracts
    # ------------------------------------------------------------------

    async def _connected_websocket_assistant(self):
        # No active WS in REST-only mode (slice 5 adds it).
        return None

    async def _subscribe_channels(self, websocket_assistant):
        pass

    async def _process_websocket_messages(self, websocket_assistant, queue):
        pass

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """Idle loop — WebSocket exec notifications added in slice 5 (Task 4)."""
        while True:
            await self._sleep(3600)
