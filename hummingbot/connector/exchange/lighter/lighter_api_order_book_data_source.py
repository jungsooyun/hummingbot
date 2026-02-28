import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.lighter import (
    lighter_constants as CONSTANTS,
    lighter_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.lighter.lighter_exchange import LighterExchange


class LighterAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        trading_pairs: List[str],
        connector: 'LighterExchange',
        api_factory: WebAssistantsFactory,
    ):
        super().__init__(trading_pairs)
        self._connector: LighterExchange = connector
        self._api_factory = api_factory

    async def get_last_traded_prices(
        self,
        trading_pairs: List[str],
        domain: Optional[str] = None,
    ) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _get_market_id(self, trading_pair: str) -> int:
        """Get the market_id for a trading pair from the connector's market_id_map."""
        exchange_symbol = await self._connector.exchange_symbol_associated_to_pair(
            trading_pair=trading_pair
        )
        market_id_map = getattr(self._connector, "_market_id_map", {})
        return market_id_map.get(exchange_symbol, 0)

    def _get_trading_pair_from_channel(self, channel: str) -> Optional[str]:
        """Extract market_id from channel string (e.g. 'order_book:0' -> 0) and resolve trading pair."""
        parts = channel.split(":")
        if len(parts) != 2:
            return None
        try:
            market_id = int(parts[1])
        except ValueError:
            return None

        market_id_map = getattr(self._connector, "_market_id_map", {})
        for ex_symbol, mid in market_id_map.items():
            if mid == market_id:
                # Use the bidict in connector to resolve trading pair
                try:
                    trading_pair_symbol_map = self._connector._trading_pair_symbol_map
                    if trading_pair_symbol_map and ex_symbol in trading_pair_symbol_map:
                        return trading_pair_symbol_map[ex_symbol]
                except Exception:
                    pass
        return None

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        update_id: int = int(snapshot_timestamp * 1e3)

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [
                (bid["price"], bid["remaining_base_amount"])
                for bid in snapshot_response.get("bids", [])
            ],
            "asks": [
                (ask["price"], ask["remaining_base_amount"])
                for ask in snapshot_response.get("asks", [])
            ],
        }
        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            snapshot_timestamp,
        )

        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved
        :return: the response from the exchange (JSON dictionary)
        """
        market_id = await self._get_market_id(trading_pair)

        params = {
            "market_id": market_id,
            "limit": 100,
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.GET_ORDER_BOOK_PATH_URL),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.GET_ORDER_BOOK_PATH_URL,
        )

        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                market_id = await self._get_market_id(trading_pair)

                trade_payload = {
                    "type": "subscribe",
                    "channel": f"{CONSTANTS.PUBLIC_TRADE_CHANNEL_NAME}/{market_id}",
                }
                subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trade_payload)

                ob_payload = {
                    "type": "subscribe",
                    "channel": f"{CONSTANTS.PUBLIC_DEPTH_CHANNEL_NAME}/{market_id}",
                }
                subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=ob_payload)

                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                    await ws.send(subscribe_trade_request)
                async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                    await ws.send(subscribe_orderbook_request)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(
                "Unexpected error occurred subscribing to order book trading and delta streams..."
            )
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        event_type = event_message.get("type", "")

        if event_type in ("update/trade", "subscribed/trade"):
            channel = self._trade_messages_queue_key
        elif event_type == "subscribed/order_book":
            channel = self._snapshot_messages_queue_key
        elif event_type == "update/order_book":
            channel = self._diff_messages_queue_key

        return channel

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        channel = raw_message.get("channel", "")
        trading_pair = self._get_trading_pair_from_channel(channel)

        for trade_data in raw_message["trades"]:
            timestamp_ms = int(trade_data["timestamp"])
            timestamp_s = timestamp_ms * 1e-3

            message_content = {
                "trade_id": timestamp_ms,
                "trading_pair": trading_pair or self._trading_pairs[0],
                "trade_type": float(TradeType.BUY.value)
                if trade_data["side"] == "buy"
                else float(TradeType.SELL.value),
                "amount": trade_data["amount"],
                "price": trade_data["price"],
            }
            trade_message: OrderBookMessage = OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content=message_content,
                timestamp=timestamp_s,
            )

            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        channel = raw_message.get("channel", "")
        trading_pair = self._get_trading_pair_from_channel(channel) or self._trading_pairs[0]

        order_book_data = raw_message.get("order_book", {})
        timestamp = time.time()
        update_id = int(timestamp * 1e3)

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [
                (bid["price"], bid["remaining_base_amount"])
                for bid in order_book_data.get("bids", [])
            ],
            "asks": [
                (ask["price"], ask["remaining_base_amount"])
                for ask in order_book_data.get("asks", [])
            ],
        }
        diff_message: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.DIFF,
            order_book_message_content,
            timestamp,
        )

        message_queue.put_nowait(diff_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        channel = raw_message.get("channel", "")
        trading_pair = self._get_trading_pair_from_channel(channel) or self._trading_pairs[0]

        order_book_data = raw_message.get("order_book", {})
        timestamp = time.time()
        update_id = int(timestamp * 1e3)

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": [
                (bid["price"], bid["remaining_base_amount"])
                for bid in order_book_data.get("bids", [])
            ],
            "asks": [
                (ask["price"], ask["remaining_base_amount"])
                for ask in order_book_data.get("asks", [])
            ],
        }
        snapshot_message: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            timestamp,
        )

        message_queue.put_nowait(snapshot_message)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECT):
            await ws.connect(
                ws_url=CONSTANTS.WSS_PUBLIC_URL,
                ping_timeout=30,
            )
        return ws

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        """
        Subscribe to order book and trade channels for a single trading pair.

        :param trading_pair: the trading pair to subscribe to
        :return: True if successful, False otherwise
        """
        if self._ws_assistant is None:
            self.logger().warning("Cannot subscribe: WebSocket connection not established")
            return False

        try:
            market_id = await self._get_market_id(trading_pair)

            trade_payload = {
                "type": "subscribe",
                "channel": f"{CONSTANTS.PUBLIC_TRADE_CHANNEL_NAME}/{market_id}",
            }
            subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trade_payload)

            ob_payload = {
                "type": "subscribe",
                "channel": f"{CONSTANTS.PUBLIC_DEPTH_CHANNEL_NAME}/{market_id}",
            }
            subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=ob_payload)

            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                await self._ws_assistant.send(subscribe_trade_request)
            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                await self._ws_assistant.send(subscribe_orderbook_request)

            self.add_trading_pair(trading_pair)
            self.logger().info(f"Subscribed to public order book and trade channels of {trading_pair}...")
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                f"Unexpected error occurred subscribing to {trading_pair}...",
                exc_info=True,
            )
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        """
        Unsubscribe from order book and trade channels for a single trading pair.

        :param trading_pair: the trading pair to unsubscribe from
        :return: True if successful, False otherwise
        """
        if self._ws_assistant is None:
            self.logger().warning("Cannot unsubscribe: WebSocket connection not established")
            return False

        try:
            market_id = await self._get_market_id(trading_pair)

            trade_payload = {
                "type": "unsubscribe",
                "channel": f"{CONSTANTS.PUBLIC_TRADE_CHANNEL_NAME}/{market_id}",
            }
            unsubscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trade_payload)

            ob_payload = {
                "type": "unsubscribe",
                "channel": f"{CONSTANTS.PUBLIC_DEPTH_CHANNEL_NAME}/{market_id}",
            }
            unsubscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=ob_payload)

            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                await self._ws_assistant.send(unsubscribe_trade_request)
            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_SUBSCRIBE):
                await self._ws_assistant.send(unsubscribe_orderbook_request)

            self.remove_trading_pair(trading_pair)
            self.logger().info(f"Unsubscribed from public order book and trade channels of {trading_pair}...")
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                f"Unexpected error occurred unsubscribing from {trading_pair}...",
                exc_info=True,
            )
            return False
