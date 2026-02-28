"""KIS API User Stream Data Source (REST-polling stub).

Korea Investment & Securities (KIS) does not provide a WebSocket API for
user-level events (order updates, balance changes).  All user-data updates
are handled via REST polling in the exchange class itself.

This module exists solely to satisfy the ``UserStreamTrackerDataSource``
interface required by ``ExchangePyBase``.  Every method is either a no-op
or returns ``None``.
"""

import asyncio
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.kis.kis_auth import KisAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kis.kis_exchange import KisExchange


class KisAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """No-op user stream data source for KIS.

    KIS does not offer a private WebSocket channel.  Balance and order
    updates are fetched via REST polling (see ``KisExchange``).  This
    class satisfies the framework's requirement for a data source object
    without actually streaming anything.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: KisAuth,
        trading_pairs: List[str],
        connector: "KisExchange",
        api_factory: WebAssistantsFactory,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory

    # ------------------------------------------------------------------ #
    # UserStreamTrackerDataSource interface
    # ------------------------------------------------------------------ #

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """Sleep indefinitely.  KIS user data is polled via REST."""
        while True:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise

    async def _connected_websocket_assistant(self) -> Optional[WSAssistant]:
        """No WebSocket for KIS -- return ``None``."""
        return None

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """No-op: nothing to subscribe to."""
        pass

    async def _process_websocket_messages(
        self, websocket_assistant: WSAssistant, queue: asyncio.Queue
    ):
        """No-op: no WebSocket messages to process."""
        pass
