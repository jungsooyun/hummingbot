"""Placeholder user stream data source for Lighter DEX. Will be fully implemented in Task 6."""

from typing import Any, List

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class LighterAPIUserStreamDataSource(UserStreamTrackerDataSource):

    def __init__(
        self,
        auth: AuthBase,
        trading_pairs: List[str],
        connector: Any,
        api_factory: WebAssistantsFactory,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory

    async def _connected_websocket_assistant(self):
        raise NotImplementedError

    async def _subscribe_channels(self, websocket_assistant):
        raise NotImplementedError
