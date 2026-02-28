import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.upbit import upbit_constants as CONSTANTS, upbit_web_utils as web_utils
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.upbit.upbit_exchange import UpbitExchange


class UpbitAPIOrderBookDataSource(OrderBookTrackerDataSource):
    ORDER_BOOK_SNAPSHOT_DELAY = 5.0

    def __init__(self, trading_pairs: List[str], connector: "UpbitExchange", api_factory: WebAssistantsFactory):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    snapshot = await self._order_book_snapshot(trading_pair=trading_pair)
                    output.put_nowait(snapshot)
                await self._sleep(self.ORDER_BOOK_SNAPSHOT_DELAY)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error fetching Upbit order book snapshots")
                await self._sleep(1.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        pass

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        pass

    async def listen_for_subscriptions(self):
        await asyncio.Event().wait()

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response = await self._request_order_book_snapshot(trading_pair)
        data = snapshot_response[0] if isinstance(snapshot_response, list) and len(snapshot_response) > 0 else {}
        units = data.get("orderbook_units", [])

        asks = [(float(unit["ask_price"]), float(unit["ask_size"])) for unit in units if float(unit.get("ask_size", 0)) > 0]
        bids = [(float(unit["bid_price"]), float(unit["bid_size"])) for unit in units if float(unit.get("bid_size", 0)) > 0]

        timestamp = time.time()
        update_id = int(data.get("timestamp", timestamp * 1e3))

        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": bids,
                "asks": asks,
            },
            timestamp=timestamp,
        )

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.GET_ORDER_BOOK_PATH_URL),
            method=RESTMethod.GET,
            params={"markets": symbol},
            throttler_limit_id=CONSTANTS.GET_ORDER_BOOK_PATH_URL,
        )

    async def _connected_websocket_assistant(self) -> WSAssistant:
        raise NotImplementedError("Upbit WebSocket order book stream is not implemented yet")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        raise NotImplementedError("Upbit WebSocket order book stream is not implemented yet")

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        pass

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        pass

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        pass

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True
