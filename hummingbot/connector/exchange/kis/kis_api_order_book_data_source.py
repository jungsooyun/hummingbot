import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

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
    from hummingbot.connector.exchange.kis.kis_ws_hub import KisWsHub


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
    - Control messages (subscription responses, hub-owned PINGPONG) are JSON
    - Subscription: JSON with ``tr_type: "1"`` (subscribe) / ``"2"`` (unsubscribe)
    - Field separator within data: ``^`` (caret)
    - PINGPONG heartbeat: echoed centrally by KisWsHub
    """

    _logger: Optional[HummingbotLogger] = None

    # KIS realtime WS is unreliable in many environments; when it is down, this is
    # the only thing that keeps the spot order book fresh. Override the 1-hour
    # upstream default so the base REST fallback re-snapshots every few seconds.
    FULL_ORDER_BOOK_RESET_DELTA_SECONDS = CONSTANTS.REST_ORDER_BOOK_POLL_INTERVAL

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "KisExchange",
        api_factory: WebAssistantsFactory,
        auth: "KisAuth",
        hub: "KisWsHub",
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        market_routing: str = CONSTANTS.MARKET_ROUTING_KRX,
        ws_enabled: bool = True,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._auth = auth
        self._hub = hub
        self._listen_gen = 0
        self._domain = domain
        self._market_routing = market_routing
        self._ws_enabled = ws_enabled
        self._ob_tr_id = CONSTANTS.WS_ORDERBOOK_TR_ID_BY_ROUTING[market_routing]
        self._trade_tr_id = CONSTANTS.WS_TRADE_TR_ID_BY_ROUTING[market_routing]
        # REST snapshot market-division code, routing-aware like the WS TR_IDs above.
        # NB: sor maps to 'UN' (통합) — unified KRX+NXT quotes stay live across the KRX
        # close into the NXT after-market (JEP-180). See REST_QUOTE_MRKT_DIV_BY_ROUTING
        # in kis_constants.py for the rationale and the live-probe evidence.
        self._rest_ob_mrkt_div = CONSTANTS.REST_QUOTE_MRKT_DIV_BY_ROUTING[market_routing]
        # All market-data TR_IDs across routing modes — used to detect channel drift
        self._known_market_tr_ids = (
            set(CONSTANTS.WS_ORDERBOOK_TR_ID_BY_ROUTING.values())
            | set(CONSTANTS.WS_TRADE_TR_ID_BY_ROUTING.values())
        )

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
        """Register KIS orderbook + trade channels on the shared WS hub."""
        if not self._ws_enabled:
            # REST-only mode: never touch the WS edge. The order book is kept fresh
            # by the base REST snapshot poll (FULL_ORDER_BOOK_RESET_DELTA_SECONDS).
            self.logger().info(
                "KIS realtime orderbook WebSocket disabled (kis_ws_enabled=false); "
                "serving order book via REST snapshot polling only."
            )
            while True:
                await self._sleep(3600)

        self._listen_gen += 1
        my_gen = self._listen_gen
        symbols = [
            await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            for trading_pair in self._trading_pairs
        ]
        try:
            for symbol in symbols:
                await self._hub.register(self._ob_tr_id, symbol, self._on_ws_frame)
                await self._hub.register(self._trade_tr_id, symbol, self._on_ws_frame)
            self.logger().info("Registered KIS orderbook + trade channels on the shared WS hub")
            while True:
                await self._sleep(3600)
        finally:
            if my_gen == self._listen_gen:
                for symbol in symbols:
                    await self._hub.unregister(self._ob_tr_id, symbol)
                    await self._hub.unregister(self._trade_tr_id, symbol)

    async def _on_ws_frame(self, raw: str):
        """Hub dispatch entry-point: route a raw frame like the old socket loop."""
        if raw and raw[0] in ("0", "1"):
            await self._handle_data_message(raw)
        else:
            await self._handle_control_message(raw)

    async def _handle_data_message(self, raw: str):
        """Parse pipe-delimited data message and enqueue."""
        parts = raw.split("|")
        if len(parts) < 4:
            self.logger().warning(f"Invalid KIS WS data message: {raw[:100]}")
            return

        tr_id = parts[1]
        tr_key = parts[2]
        data_str = parts[3]

        if tr_id == self._ob_tr_id:
            parsed = self._parse_caret_fields(data_str, CONSTANTS.WS_ORDERBOOK_COLUMNS)
            if parsed:
                await self._process_orderbook_data(tr_key, parsed)
        elif tr_id == self._trade_tr_id:
            parsed = self._parse_caret_fields(data_str, CONSTANTS.WS_TRADE_COLUMNS)
            if parsed:
                await self._process_trade_data(tr_key, parsed)
        elif tr_id in self._known_market_tr_ids:
            # A market-data channel for a different routing mode than configured —
            # subscription/dispatch drift. Surface it instead of dropping silently.
            self.logger().warning(
                f"Dropped KIS market-data TR_ID '{tr_id}' not matching active "
                f"'{self._market_routing}' routing (orderbook={self._ob_tr_id}, "
                f"trade={self._trade_tr_id})."
            )

    async def _handle_control_message(self, raw: str):
        """Handle JSON subscription responses. PINGPONG is echoed centrally by the hub."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.logger().warning(f"Invalid JSON from KIS WS: {raw[:100]}")
            return

        body = data.get("body", {})
        rt_cd = body.get("rt_cd", "")
        msg1 = body.get("msg1", "")
        if rt_cd not in ("", "0"):
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
        if bids and asks:
            # JEP-134: only a genuinely mapped frame proves WS freshness for its pair;
            # never let the _resolve_trading_pair fallback stamp a non-target pair fresh.
            symbol_map = None
            map_ready = getattr(self._connector, "trading_pair_symbol_map_ready", None)
            map_getter = getattr(self._connector, "trading_pair_symbol_map", None)
            if callable(map_getter) and (not callable(map_ready) or map_ready()):
                symbol_map = await map_getter()
            else:
                symbol_map = getattr(self._connector, "_trading_pair_symbol_map", None)
            if symbol_map and symbol_map.get(tr_key) == trading_pair:
                self._mark_ws_orderbook_frame(trading_pair)
                self._connector.note_top_of_book(trading_pair, bids[0][0], asks[0][0])
                self._connector.note_hour_cls_code(trading_pair, data.get("HOUR_CLS_CODE"))

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

        # Fail closed: KIS returns HTTP 200 with rt_cd != "0" on logical errors. Parsing
        # the missing output1 (-> {}) would publish an empty book and silently poison the
        # fair-price source. Raise instead (JEP-161, mirrors _get_last_traded_price).
        if snapshot_response.get("rt_cd") != "0":
            raise IOError(
                f"KIS orderbook snapshot failed for {trading_pair}: "
                f"rt_cd={snapshot_response.get('rt_cd')} msg={snapshot_response.get('msg1')}"
            )
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

        # Fail closed unless BOTH sides have a valid level. A one-sided book is unusable as
        # a two-sided fair source: get_price_by_type on the empty side returns float NaN,
        # which slips past _compute_fair's `not px / px<=0` guard (NaN comparison raises
        # InvalidOperation) and either fakes readiness with a NaN fair or crashes the
        # control loop. Refuse to publish and let the tracker retry (JEP-161; codex 2026-06-19).
        if not asks or not bids:
            raise IOError(
                f"KIS orderbook snapshot for {trading_pair} is one-sided/empty "
                f"(asks={len(asks)} bids={len(bids)}) — refusing to publish an unusable book"
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
            # Routing-aware: KRX 'J' / NXT 'NX' / 통합 'UN'. Hardcoding 'J' froze the
            # spot after the KRX regular close (15:30 KST) while NXT after-market kept
            # trading -> stale fair (JEP-148). Mirrors the WS TR_ID routing.
            "FID_COND_MRKT_DIV_CODE": self._rest_ob_mrkt_div,
            "FID_INPUT_ISCD": symbol,
        }
        rest_assistant = await self._api_factory.get_rest_assistant()
        # KIS market-data REST endpoints require auth (Bearer token + appkey/
        # appsecret) AND a per-API ``tr_id`` header. Omitting them returns HTTP 500
        # EGW00304 ("appSecret invalid") and the order book never bootstraps.
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(
                path_url=CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH
            ),
            params=params,
            method=RESTMethod.GET,
            is_auth_required=True,
            headers={"tr_id": CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_TR_ID},
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
