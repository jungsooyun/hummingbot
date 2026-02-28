"""Placeholder order book data source for Lighter DEX. Will be fully implemented in Task 5."""

import asyncio
from typing import Any, Dict, List, Optional

from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class LighterAPIOrderBookDataSource(OrderBookTrackerDataSource):

    def __init__(
        self,
        trading_pairs: List[str],
        connector: Any,
        api_factory: WebAssistantsFactory,
    ):
        super().__init__(trading_pairs=trading_pairs)
        self._connector = connector
        self._api_factory = api_factory

    async def get_last_traded_prices(
        self, trading_pairs: List[str], domain: Optional[str] = None
    ) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raise NotImplementedError

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        raise NotImplementedError

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        raise NotImplementedError

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        raise NotImplementedError

    async def _subscribe_channels(self, ws):
        raise NotImplementedError

    async def _connected_websocket_assistant(self):
        raise NotImplementedError

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return False
