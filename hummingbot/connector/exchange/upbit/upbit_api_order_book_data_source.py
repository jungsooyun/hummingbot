import asyncio
import json
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.upbit import upbit_constants as CONSTANTS, upbit_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.upbit.upbit_exchange import UpbitExchange


class UpbitAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _DYNAMIC_SUBSCRIBE_ID_START = 100
    _next_subscribe_id: int = _DYNAMIC_SUBSCRIBE_ID_START

    def __init__(self, trading_pairs: List[str], connector: "UpbitExchange", api_factory: WebAssistantsFactory):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._trade_messages_queue_key = CONSTANTS.TRADE_EVENT_TYPE
        # Upbit orderbook WS messages are full-book snapshots for requested depth.
        self._snapshot_messages_queue_key = CONSTANTS.ORDERBOOK_EVENT_TYPE

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.GET_ORDER_BOOK_PATH_URL),
            method=RESTMethod.GET,
            params={"markets": symbol},
            throttler_limit_id=CONSTANTS.GET_ORDER_BOOK_PATH_URL,
        )

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response = await self._request_order_book_snapshot(trading_pair)
        data = snapshot_response[0] if isinstance(snapshot_response, list) and len(snapshot_response) > 0 else {}
        return self._snapshot_message_from_data(data=data, trading_pair=trading_pair)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.WSS_PUBLIC_URL,
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
        )
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        try:
            await self._send_subscription_message(websocket_assistant=websocket_assistant, trading_pairs=self._trading_pairs)
            self.logger().info("Subscribed to Upbit public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to Upbit order book and trade streams...",
                exc_info=True,
            )
            raise

    async def _send_subscription_message(self, websocket_assistant: WSAssistant, trading_pairs: List[str]):
        if len(trading_pairs) == 0:
            return
        codes = [await self._upbit_code_for_pair(trading_pair) for trading_pair in trading_pairs]
        payload = [
            {"ticket": str(uuid.uuid4())},
            {"type": CONSTANTS.ORDERBOOK_EVENT_TYPE, "codes": codes},
            {"type": CONSTANTS.TRADE_EVENT_TYPE, "codes": codes},
            {"format": "DEFAULT"},
        ]
        await websocket_assistant.send(WSJSONRequest(payload=payload, throttler_limit_id=CONSTANTS.WS_SUBSCRIBE))

    async def _upbit_code_for_pair(self, trading_pair: str) -> str:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        # Upbit supports per-code depth suffix (e.g. KRW-BTC.15). Keep default at 15 levels.
        if "." in symbol:
            return symbol
        return f"{symbol}.{CONSTANTS.DEFAULT_ORDERBOOK_DEPTH}"

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        code = raw_message.get("code")
        if code is None:
            return
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=code.split(".")[0])
        ask_bid = str(raw_message.get("ask_bid", "")).upper()
        trade_type = float(TradeType.SELL.value) if ask_bid == "ASK" else float(TradeType.BUY.value)
        timestamp_ms = int(raw_message.get("timestamp", self._time() * 1e3))
        trade_message = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_type": trade_type,
                "trade_id": int(raw_message.get("sequential_id", timestamp_ms)),
                "update_id": timestamp_ms,
                "price": float(raw_message.get("trade_price", 0)),
                "amount": float(raw_message.get("trade_volume", 0)),
            },
            timestamp=timestamp_ms * 1e-3,
        )
        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # Upbit orderbook channel pushes full top-N orderbook state; treat as snapshot updates.
        await self._parse_order_book_snapshot_message(raw_message=raw_message, message_queue=message_queue)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        code = raw_message.get("code")
        if code is None:
            return
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=code.split(".")[0])
        snapshot_message = self._snapshot_message_from_data(data=raw_message, trading_pair=trading_pair)
        message_queue.put_nowait(snapshot_message)

    def _snapshot_message_from_data(self, data: Dict[str, Any], trading_pair: str) -> OrderBookMessage:
        units = data.get("orderbook_units", []) or []
        asks = [
            (float(unit["ask_price"]), float(unit["ask_size"]))
            for unit in units
            if float(unit.get("ask_size", 0)) > 0
        ]
        bids = [
            (float(unit["bid_price"]), float(unit["bid_size"]))
            for unit in units
            if float(unit.get("bid_size", 0)) > 0
        ]

        timestamp = self._time()
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

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        event_type = event_message.get("type", "")
        if event_type == CONSTANTS.ORDERBOOK_EVENT_TYPE:
            return self._snapshot_messages_queue_key
        if event_type == CONSTANTS.TRADE_EVENT_TYPE:
            return self._trade_messages_queue_key
        return ""

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            event_message = self._normalize_ws_message(ws_response.data)
            if event_message is None:
                continue
            channel: str = self._channel_originating_message(event_message=event_message)
            if channel in self._get_messages_queue_keys():
                self._message_queue[channel].put_nowait(event_message)
            else:
                await self._process_message_for_unknown_channel(
                    event_message=event_message, websocket_assistant=websocket_assistant
                )

    def _normalize_ws_message(self, raw_message: Any) -> Optional[Dict[str, Any]]:
        if raw_message is None:
            return None
        if isinstance(raw_message, dict):
            return raw_message
        if isinstance(raw_message, bytes):
            try:
                raw_message = raw_message.decode("utf-8")
            except Exception:
                return None
        if isinstance(raw_message, str):
            try:
                parsed = json.loads(raw_message)
            except json.JSONDecodeError:
                return None
            if isinstance(parsed, dict):
                return parsed
        return None

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot subscribe to {trading_pair}: WebSocket not connected")
            return False
        try:
            if trading_pair not in self._trading_pairs:
                self._trading_pairs.append(trading_pair)
            await self._send_subscription_message(self._ws_assistant, self._trading_pairs)
            self.logger().info(f"Subscribed to {trading_pair} order book and trade channels")
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Unexpected error subscribing to {trading_pair} channels")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot unsubscribe from {trading_pair}: WebSocket not connected")
            return False
        try:
            self.remove_trading_pair(trading_pair)
            # Upbit WS protocol does not provide explicit unsubscribe command.
            # Re-send the active subscriptions so the current connection tracks remaining pairs.
            await self._send_subscription_message(self._ws_assistant, self._trading_pairs)
            self.logger().info(f"Unsubscribed from {trading_pair} order book and trade channels")
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Unexpected error unsubscribing from {trading_pair} channels")
            return False
