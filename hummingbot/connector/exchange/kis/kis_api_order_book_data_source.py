import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import aiohttp

from hummingbot.connector.exchange.kis import (
    kis_constants as CONSTANTS,
    kis_utils,
    kis_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kis.kis_auth import KisAuth
    from hummingbot.connector.exchange.kis.kis_exchange import KisExchange


class KisAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    KIS WebSocket-based order book data source.

    Korea Investment & Securities (KIS) provides WebSocket streams for
    real-time market data:
    - H0STASP0: 국내주식 실시간호가 (orderbook, 10 levels)
    - H0STCNT0: 국내주식 실시간체결가 (trade executions)

    KIS WebSocket specifics:
    - Auth uses approval_key (separate from REST OAuth token)
    - Data arrives as pipe-delimited text: ``0|TR_ID|TR_KEY|field1^field2^...``
    - Control messages (subscription responses, PINGPONG) are JSON
    - Subscription: JSON with ``tr_type: "1"`` (subscribe) / ``"2"`` (unsubscribe)
    - Field separator within data: ``^`` (caret)
    - PINGPONG heartbeat: echo the raw message back
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "KisExchange",
        api_factory: WebAssistantsFactory,
        auth: "KisAuth",
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._auth = auth
        self._domain = domain

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def get_last_traded_prices(
        self,
        trading_pairs: List[str],
        domain: Optional[str] = None,
    ) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    # ------------------------------------------------------------------
    # WebSocket subscription loop
    # ------------------------------------------------------------------

    async def listen_for_subscriptions(self):
        """Connect to KIS WebSocket and stream orderbook + trade data.

        Overrides the base class to handle KIS's custom message format:
        - Data messages: pipe-delimited text with caret-separated fields
        - Control messages: JSON (subscription responses, PINGPONG)
        """
        while True:
            try:
                approval_key = await self._auth.get_ws_approval_key()
                ws_url = web_utils.ws_url(self._domain)

                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url) as ws:
                        # Subscribe to channels
                        await self._subscribe_ws_channels(ws, approval_key)

                        # Process messages
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._process_ws_message(ws, msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error in KIS WebSocket subscription. Reconnecting..."
                )
                await self._sleep(5.0)

    async def _subscribe_ws_channels(self, ws: aiohttp.ClientWebSocketResponse, approval_key: str):
        """Subscribe to orderbook and trade channels for all trading pairs."""
        for trading_pair in self._trading_pairs:
            symbol = await self._connector.exchange_symbol_associated_to_pair(
                trading_pair=trading_pair
            )

            # Subscribe to orderbook (H0STASP0)
            ob_msg = self._build_subscription_message(
                approval_key=approval_key,
                tr_id=CONSTANTS.WS_DOMESTIC_STOCK_ORDERBOOK_TR_ID,
                tr_key=symbol,
                tr_type="1",
            )
            await ws.send_json(ob_msg)

            # Subscribe to trades (H0STCNT0)
            trade_msg = self._build_subscription_message(
                approval_key=approval_key,
                tr_id=CONSTANTS.WS_DOMESTIC_STOCK_TRADE_TR_ID,
                tr_key=symbol,
                tr_type="1",
            )
            await ws.send_json(trade_msg)

        self.logger().info("Subscribed to KIS WebSocket orderbook and trade channels")

    async def _process_ws_message(self, ws: aiohttp.ClientWebSocketResponse, raw: str):
        """Route incoming WS message to the appropriate handler."""
        if raw and raw[0] in ("0", "1"):
            # Data message: 0|TR_ID|TR_KEY|data
            await self._handle_data_message(raw)
        else:
            # JSON control message (subscription response, PINGPONG)
            await self._handle_control_message(ws, raw)

    async def _handle_data_message(self, raw: str):
        """Parse pipe-delimited data message and enqueue."""
        parts = raw.split("|")
        if len(parts) < 4:
            self.logger().warning(f"Invalid KIS WS data message: {raw[:100]}")
            return

        tr_id = parts[1]
        tr_key = parts[2]
        data_str = parts[3]

        if tr_id == CONSTANTS.WS_DOMESTIC_STOCK_ORDERBOOK_TR_ID:
            parsed = self._parse_caret_fields(data_str, CONSTANTS.WS_ORDERBOOK_COLUMNS)
            if parsed:
                await self._process_orderbook_data(tr_key, parsed)
        elif tr_id == CONSTANTS.WS_DOMESTIC_STOCK_TRADE_TR_ID:
            parsed = self._parse_caret_fields(data_str, CONSTANTS.WS_TRADE_COLUMNS)
            if parsed:
                await self._process_trade_data(tr_key, parsed)

    async def _handle_control_message(self, ws: aiohttp.ClientWebSocketResponse, raw: str):
        """Handle JSON control messages (PINGPONG, subscription responses)."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.logger().warning(f"Invalid JSON from KIS WS: {raw[:100]}")
            return

        header = data.get("header", {})
        tr_id = header.get("tr_id", "")

        if tr_id == "PINGPONG":
            # Echo the raw message back as pong
            await ws.send_str(raw)
            return

        # Subscription response
        body = data.get("body", {})
        rt_cd = body.get("rt_cd", "")
        msg1 = body.get("msg1", "")
        if rt_cd != "0":
            self.logger().warning(f"KIS WS subscription error: {msg1}")

    # ------------------------------------------------------------------
    # Data parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_caret_fields(data_str: str, columns: tuple) -> Optional[Dict[str, str]]:
        """Parse caret-separated fields into a dict using column definitions."""
        fields = data_str.split("^")
        if len(fields) < len(columns):
            return None
        return dict(zip(columns, fields))

    async def _process_orderbook_data(self, tr_key: str, data: Dict[str, str]):
        """Convert parsed orderbook data to OrderBookMessage and enqueue."""
        trading_pair = await self._resolve_trading_pair(tr_key)
        if not trading_pair:
            return

        timestamp = time.time()
        update_id = int(timestamp * 1e3)

        asks = []
        bids = []
        for i in range(1, 11):
            ask_price = kis_utils.to_float(data.get(f"ASKP{i}", "0"))
            ask_size = kis_utils.to_float(data.get(f"ASKP_RSQN{i}", "0"))
            bid_price = kis_utils.to_float(data.get(f"BIDP{i}", "0"))
            bid_size = kis_utils.to_float(data.get(f"BIDP_RSQN{i}", "0"))

            if ask_price and ask_price > 0 and ask_size and ask_size > 0:
                asks.append((ask_price, ask_size))
            if bid_price and bid_price > 0 and bid_size and bid_size > 0:
                bids.append((bid_price, bid_size))

        asks.sort(key=lambda x: x[0])
        bids.sort(key=lambda x: x[0], reverse=True)

        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": bids,
            "asks": asks,
        }
        snapshot_msg = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            content,
            timestamp,
        )
        self._message_queue[self._snapshot_messages_queue_key].put_nowait(snapshot_msg)

    async def _process_trade_data(self, tr_key: str, data: Dict[str, str]):
        """Convert parsed trade data to OrderBookMessage and enqueue."""
        trading_pair = await self._resolve_trading_pair(tr_key)
        if not trading_pair:
            return

        timestamp = time.time()
        price = kis_utils.to_float(data.get("STCK_PRPR", "0")) or 0.0
        amount = kis_utils.to_float(data.get("CNTG_VOL", "0")) or 0.0

        # CCLD_DVSN: "1" = sell, "2" = buy (per KIS docs)
        ccld_dvsn = data.get("CCLD_DVSN", "")
        trade_type = float(TradeType.SELL.value) if ccld_dvsn == "1" else float(TradeType.BUY.value)

        content = {
            "trade_id": int(timestamp * 1e3),
            "trading_pair": trading_pair,
            "trade_type": trade_type,
            "amount": amount,
            "price": price,
        }
        trade_msg = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=content,
            timestamp=timestamp,
        )
        self._message_queue[self._trade_messages_queue_key].put_nowait(trade_msg)

    # ------------------------------------------------------------------
    # REST snapshot fallback (for initial load and recovery)
    # ------------------------------------------------------------------

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Build an OrderBookMessage from a KIS REST orderbook response."""
        snapshot_response = await self._request_order_book_snapshot(trading_pair)
        output = snapshot_response.get("output1", {})

        timestamp = time.time()
        update_id = int(timestamp * 1e3)

        asks, bids = self._extract_levels_from_dict(
            data=output,
            depth=10,
            ask_price_templates=["askp{idx}"],
            ask_size_templates=["askp_rsqn{idx}"],
            bid_price_templates=["bidp{idx}"],
            bid_size_templates=["bidp_rsqn{idx}"],
        )

        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": bids,
            "asks": asks,
        }
        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            content,
            timestamp,
        )

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """Fetch the raw orderbook from KIS REST API."""
        symbol = await self._connector.exchange_symbol_associated_to_pair(
            trading_pair=trading_pair
        )
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
        }
        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(
                path_url=CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH
            ),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_TR_ID,
        )

    # ------------------------------------------------------------------
    # Template-based level extraction (REST fallback)
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_levels_from_dict(
        data: Dict[str, Any],
        depth: int,
        ask_price_templates: List[str],
        ask_size_templates: List[str],
        bid_price_templates: List[str],
        bid_size_templates: List[str],
    ) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Extract price/size levels from a KIS dict-formatted orderbook."""
        asks: List[Tuple[float, float]] = []
        bids: List[Tuple[float, float]] = []

        for idx in range(1, depth + 1):
            ask_price = _first_float_by_templates(data, ask_price_templates, idx)
            ask_size = _first_float_by_templates(data, ask_size_templates, idx)
            bid_price = _first_float_by_templates(data, bid_price_templates, idx)
            bid_size = _first_float_by_templates(data, bid_size_templates, idx)

            if ask_price and ask_price > 0 and ask_size and ask_size > 0:
                asks.append((ask_price, ask_size))
            if bid_price and bid_price > 0 and bid_size and bid_size > 0:
                bids.append((bid_price, bid_size))

        asks.sort(key=lambda x: x[0])
        bids.sort(key=lambda x: x[0], reverse=True)
        return asks, bids

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _resolve_trading_pair(self, stock_code: str) -> Optional[str]:
        """Resolve stock code to hummingbot trading pair."""
        try:
            symbol_map = self._connector._trading_pair_symbol_map
            if symbol_map and stock_code in symbol_map:
                return symbol_map[stock_code]
        except Exception:
            pass
        # Fallback: return first trading pair
        if self._trading_pairs:
            return self._trading_pairs[0]
        return None

    @staticmethod
    def _build_subscription_message(
        approval_key: str, tr_id: str, tr_key: str, tr_type: str,
    ) -> dict:
        """Build a KIS WebSocket subscription JSON message."""
        return {
            "header": {
                "approval_key": approval_key,
                "custtype": "P",
                "tr_type": tr_type,
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": tr_key,
                },
            },
        }

    # ------------------------------------------------------------------
    # Base class abstract methods (no-op for WS-based source)
    # ------------------------------------------------------------------

    async def _connected_websocket_assistant(self) -> WSAssistant:
        raise NotImplementedError("KIS uses custom WS handling via listen_for_subscriptions")

    async def _subscribe_channels(self, ws: WSAssistant):
        raise NotImplementedError("KIS uses custom WS handling via listen_for_subscriptions")

    async def _parse_trade_message(self, raw_message: Any, message_queue: asyncio.Queue):
        # WS messages are pre-parsed into OrderBookMessage by _process_trade_data
        if isinstance(raw_message, OrderBookMessage):
            message_queue.put_nowait(raw_message)

    async def _parse_order_book_diff_message(self, raw_message: Any, message_queue: asyncio.Queue):
        pass  # KIS sends full snapshots, not diffs

    async def _parse_order_book_snapshot_message(self, raw_message: Any, message_queue: asyncio.Queue):
        # WS messages are pre-parsed into OrderBookMessage by _process_orderbook_data
        if isinstance(raw_message, OrderBookMessage):
            message_queue.put_nowait(raw_message)

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        # KIS manages subscriptions in listen_for_subscriptions via custom WS handling
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        # KIS manages subscriptions in listen_for_subscriptions via custom WS handling
        return True


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _first_float_by_templates(
    data: Dict[str, Any], templates: List[str], idx: int
) -> Optional[float]:
    """Try each template with ``{idx}`` substitution and return the first valid float."""
    keys = [t.format(idx=idx) for t in templates]
    return kis_utils.first_float(data, keys)
