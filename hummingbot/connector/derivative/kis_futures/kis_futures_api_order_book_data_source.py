"""
KIS Futures API Order Book Data Source.

Step 1: REST orderbook polling (FHMIF10010000, JF market-div, 5-level output2).
Step 2: H0ZFCNT0 WebSocket trade stream (49 fields, caret-delimited).
Step 3: H0ZFASP0 WS orderbook — NOT implemented (field order unconfirmed in any
        authoritative source; KIS official GitHub has no H0ZFASP0 sample, and the
        nautilus adapter marks it "unsupported/catalog-only/pending-refresh").
        WS_ORDERBOOK_COLUMNS stays empty; orderbook is served by REST-only polling.
        This is tracked as a follow-up once KIS publishes the H0ZFASP0 field spec.
"""
import asyncio
import json
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import aiohttp

from hummingbot.connector.derivative.kis_futures import (
    kis_futures_constants as CONSTANTS,
    kis_futures_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    from hummingbot.connector.exchange.kis.kis_auth import KisAuth


class KisFuturesAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """
    KIS stock futures order book data source.

    Market data architecture:
    - Order book: REST polling (FHMIF10010000 / JF) at FULL_ORDER_BOOK_RESET_DELTA_SECONDS.
    - Trades: H0ZFCNT0 WebSocket when ws_enabled=True; REST-only when False.
    - H0ZFASP0 (real-time quote WS): NOT subscribed — field order unconfirmed.

    KIS WebSocket protocol (mirrors spot):
    - Frame format: ``0|TR_ID|TR_KEY|field1^field2^...``
    - Control frames: JSON (subscription responses, PINGPONG echo).
    - Auth: approval_key obtained via OAuth REST call.
    """

    _logger: Optional[HummingbotLogger] = None

    # Re-snapshot the order book every 5 s so REST-only mode stays fresh.
    FULL_ORDER_BOOK_RESET_DELTA_SECONDS = CONSTANTS.REST_ORDER_BOOK_POLL_INTERVAL

    def __init__(
        self,
        trading_pairs: List[str],
        connector: Optional["KisFuturesDerivative"],
        api_factory: Optional[WebAssistantsFactory],
        auth: Optional["KisAuth"],
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        ws_enabled: bool = True,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._auth = auth
        self._domain = domain
        self._ws_enabled = ws_enabled
        self._ws_failures = 0

    # ------------------------------------------------------------------
    # PerpetualAPIOrderBookDataSource — funding info (inert; no periodic funding)
    # ------------------------------------------------------------------

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """Return inert zero-rate FundingInfo — KIS stock futures have no periodic funding."""
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal("0"),
            mark_price=Decimal("0"),
            next_funding_utc_timestamp=int(time.time() + 3600),
            rate=Decimal("0"),
        )

    async def _parse_funding_info_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        # No funding stream on KIS stock futures.
        return

    # ------------------------------------------------------------------
    # Step 1: REST orderbook snapshot
    # ------------------------------------------------------------------

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Fetch orderbook from KIS REST API and return a SNAPSHOT OrderBookMessage."""
        snapshot_response = await self._request_order_book_snapshot(trading_pair)
        # RT_CD check: fail-closed — do not emit a zero book on exchange error.
        if snapshot_response.get("rt_cd", "0") != "0":
            raise IOError(
                f"KIS futures orderbook error: rt_cd={snapshot_response.get('rt_cd')} "
                f"msg={snapshot_response.get('msg1')} for {trading_pair}"
            )

        # KIS futures orderbook lives in output2 (not output1 like spot).
        output = snapshot_response.get("output2", {}) or {}
        timestamp = time.time()
        update_id = int(timestamp * 1e3)

        # Primary field names use futs_* prefix (live captured); synthetic fixtures may use bare names.
        asks, bids = self._extract_levels_from_dict(
            data=output,
            depth=5,
            ask_price_templates=["futs_askp{idx}", "askp{idx}"],
            ask_size_templates=["askp_rsqn{idx}"],
            bid_price_templates=["futs_bidp{idx}", "bidp{idx}"],
            bid_size_templates=["bidp_rsqn{idx}"],
        )

        # Fail-closed: do not emit an empty/zero book — it would wipe the local
        # order book and disable trading until the next successful poll.
        if not asks and not bids:
            raise IOError(
                f"[kis_futures] _order_book_snapshot: no valid levels in response "
                f"for {trading_pair} — refusing to emit empty order book"
            )

        content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": bids,
            "asks": asks,
        }
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, content, timestamp)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """Call FHMIF10010000 with JF market-div and front-month short code."""
        # Front-month code from connector (set by _resolve_front_months).
        code = (
            self._connector.current_contract_code(trading_pair)
            if self._connector is not None
            else trading_pair.split("-")[0]
        )
        params = {
            "FID_COND_MRKT_DIV_CODE": CONSTANTS.MRKT_DIV_STOCK_FUTURE,
            "FID_INPUT_ISCD": code or trading_pair.split("-")[0],
        }
        rest_assistant = await self._api_factory.get_rest_assistant()
        # Auth + tr_id header are mandatory: omitting either returns HTTP 500 EGW00304.
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(
                path_url=CONSTANTS.FUT_ORDERBOOK_PATH,
                domain=self._domain,
            ),
            params=params,
            method=RESTMethod.GET,
            is_auth_required=True,
            headers={"tr_id": CONSTANTS.FUT_ORDERBOOK_TR_ID},
            throttler_limit_id=CONSTANTS.FUT_ORDERBOOK_TR_ID,
        )

    # ------------------------------------------------------------------
    # Template-based level extraction (same helper pattern as spot)
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
        """Extract ask/bid levels via template-based key lookup with fallback chain."""
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
    # Step 2: WebSocket trade stream (H0ZFCNT0)
    # ------------------------------------------------------------------

    async def listen_for_subscriptions(self):
        """Connect to KIS WebSocket and stream H0ZFCNT0 trade data.

        ws_enabled=False: idle loop; order book is served purely by REST snapshot polling.
        H0ZFASP0 (real-time orderbook): not subscribed — field order unconfirmed.
        """
        if not self._ws_enabled:
            self.logger().info(
                "[kis_futures] WebSocket disabled (kis_futures_ws_enabled=false); "
                "serving order book via REST snapshot polling only."
            )
            while True:
                await self._sleep(3600)

        while True:
            try:
                approval_key = await self._auth.get_ws_approval_key()
                ws_url = web_utils.ws_url(self._domain)

                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url) as ws:
                        await self._subscribe_ws_channels(ws, approval_key)
                        self._ws_failures = 0

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._process_ws_message(ws, msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                raise
            except Exception:
                self._ws_failures += 1
                delay = web_utils.reconnect_backoff(self._ws_failures)
                if self._ws_failures == 1:
                    self.logger().exception(
                        "[kis_futures] orderbook WS failed; retrying with backoff."
                    )
                else:
                    self.logger().warning(
                        f"[kis_futures] orderbook WS still unavailable "
                        f"(attempt {self._ws_failures}); retrying in {delay:.0f}s."
                    )
                await self._sleep(delay)

    async def _subscribe_ws_channels(
        self, ws: aiohttp.ClientWebSocketResponse, approval_key: str
    ):
        """Subscribe H0ZFCNT0 (trades) for each configured trading pair."""
        for trading_pair in self._trading_pairs:
            code = (
                self._connector.current_contract_code(trading_pair)
                if self._connector is not None
                else trading_pair.split("-")[0]
            )
            tr_key = code or trading_pair.split("-")[0]
            trade_msg = _build_subscription_message(
                approval_key=approval_key,
                tr_id=CONSTANTS.WS_FUT_TRADE_TR_ID,
                tr_key=tr_key,
                tr_type="1",
            )
            await ws.send_json(trade_msg)
        self.logger().info(
            "[kis_futures] Subscribed to H0ZFCNT0 (futures trade stream)"
        )

    async def _process_ws_message(
        self, ws: aiohttp.ClientWebSocketResponse, raw: str
    ):
        """Route incoming WS frame to data or control handler."""
        if raw and raw[0] in ("0", "1"):
            await self._handle_data_message(raw)
        else:
            await self._handle_control_message(ws, raw)

    async def _handle_data_message(self, raw: str):
        """Parse pipe-delimited data frame and enqueue trade message."""
        parts = raw.split("|")
        if len(parts) < 4:
            self.logger().warning(f"[kis_futures] invalid WS data frame: {raw[:100]}")
            return

        tr_id = parts[1]
        tr_key = parts[2]
        data_str = parts[3]

        if tr_id == CONSTANTS.WS_FUT_TRADE_TR_ID:
            parsed = _parse_caret_fields(data_str, CONSTANTS.WS_TRADE_COLUMNS)
            if parsed:
                await self._process_trade_data(tr_key, parsed)

    async def _handle_control_message(
        self, ws: aiohttp.ClientWebSocketResponse, raw: str
    ):
        """Handle JSON control frames: PINGPONG echo, subscription responses."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.logger().warning(f"[kis_futures] invalid JSON WS frame: {raw[:100]}")
            return

        header = data.get("header", {})
        tr_id = header.get("tr_id", "")

        if tr_id == "PINGPONG":
            await ws.send_str(raw)
            return

        body = data.get("body", {})
        if body.get("rt_cd", "0") != "0":
            self.logger().warning(
                f"[kis_futures] WS subscription error: {body.get('msg1')}"
            )

    async def _process_trade_data(self, tr_key: str, data: Dict[str, str]):
        """Convert parsed H0ZFCNT0 frame to a TRADE OrderBookMessage and enqueue."""
        trading_pair = self._resolve_trading_pair_for_code(tr_key)
        if not trading_pair:
            return

        timestamp = time.time()
        # stck_prpr = 현재가 (trade price); last_cnqn = 당시 체결량 (trade size)
        price = _to_float(data.get("stck_prpr", "0")) or 0.0
        amount = _to_float(data.get("last_cnqn", "0")) or 0.0
        if price <= 0 or amount <= 0:
            return

        content = {
            "trade_id": int(timestamp * 1e3),
            "trading_pair": trading_pair,
            "trade_type": float(TradeType.BUY.value),  # KIS futures no buy/sell side in H0ZFCNT0
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
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_trading_pair_for_code(self, short_code: str) -> Optional[str]:
        """Map front-month short code back to hummingbot trading pair."""
        if self._connector is not None:
            for tp in self._trading_pairs:
                if self._connector.current_contract_code(tp) == short_code:
                    return tp
        if self._trading_pairs:
            return self._trading_pairs[0]
        return None

    # ------------------------------------------------------------------
    # Remaining OrderBookTrackerDataSource / base abstracts
    # ------------------------------------------------------------------

    async def get_last_traded_prices(
        self, trading_pairs: List[str], domain: Optional[str] = None
    ) -> Dict[str, float]:
        """Fetch last traded prices via the connector's _get_last_traded_price.

        Fail-closed: raises IOError (propagates from _get_last_traded_price) on
        any bad response or price <= 0.  Never silently returns 0.0.
        """
        if self._connector is None:
            raise IOError(
                "[kis_futures] get_last_traded_prices: no connector attached"
            )
        result: Dict[str, float] = {}
        for tp in trading_pairs:
            result[tp] = await self._connector._get_last_traded_price(tp)
        return result

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def _connected_websocket_assistant(self) -> WSAssistant:
        raise NotImplementedError("KIS futures uses custom WS handling via listen_for_subscriptions")

    async def _subscribe_channels(self, ws: WSAssistant):
        raise NotImplementedError("KIS futures uses custom WS handling via listen_for_subscriptions")

    async def _parse_trade_message(
        self, raw_message: Any, message_queue: asyncio.Queue
    ):
        # WS trade messages are pre-parsed by _process_trade_data.
        if isinstance(raw_message, OrderBookMessage):
            message_queue.put_nowait(raw_message)

    async def _parse_order_book_diff_message(
        self, raw_message: Any, message_queue: asyncio.Queue
    ):
        # KIS sends full snapshots (REST); no diff stream.
        pass

    async def _parse_order_book_snapshot_message(
        self, raw_message: Any, message_queue: asyncio.Queue
    ):
        if isinstance(raw_message, OrderBookMessage):
            message_queue.put_nowait(raw_message)


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _first_float_by_templates(
    data: Dict[str, Any], templates: List[str], idx: int
) -> Optional[float]:
    """Try each template with {idx} substitution; return the first valid float."""
    for tmpl in templates:
        key = tmpl.format(idx=idx)
        val = data.get(key)
        if val is not None:
            f = _to_float(str(val))
            if f is not None:
                return f
    return None


def _to_float(s: str) -> Optional[float]:
    """Safe string-to-float conversion; returns None on empty/invalid input."""
    if not s or s.strip() == "":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_caret_fields(data_str: str, columns: tuple) -> Optional[Dict[str, str]]:
    """Parse caret-separated WS data into a dict keyed by column names."""
    fields = data_str.split("^")
    if len(fields) < len(columns):
        return None
    return dict(zip(columns, fields))


def _build_subscription_message(
    approval_key: str, tr_id: str, tr_key: str, tr_type: str
) -> dict:
    """Build KIS WebSocket subscription JSON."""
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
