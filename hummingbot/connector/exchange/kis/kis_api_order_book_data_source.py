import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from hummingbot.connector.exchange.kis import (
    kis_constants as CONSTANTS,
    kis_utils,
    kis_web_utils as web_utils,
)
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kis.kis_exchange import KisExchange


class KisAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    KIS REST-only order book data source.

    Korea Investment & Securities (KIS) does NOT provide WebSocket streams for
    public market data (order book, trades).  This data source fetches order
    book snapshots via REST polling at a configurable interval.

    Key design decisions:
    - ``listen_for_subscriptions()`` blocks indefinitely (no WS).
    - ``listen_for_order_book_diffs()`` is a no-op (REST snapshots only).
    - ``listen_for_order_book_snapshots()`` polls REST at intervals.
    - ``listen_for_trades()`` is a no-op (no trade stream).
    - ``_connected_websocket_assistant()`` raises NotImplementedError.
    - ``_subscribe_channels()`` raises NotImplementedError.

    KIS orderbook responses use a flat dict with indexed field names:
        askp1, askp2, ..., askp10, askp_rsqn1, ..., askp_rsqn10
        bidp1, bidp2, ..., bidp10, bidp_rsqn1, ..., bidp_rsqn10

    The ``_extract_levels_from_dict`` static method uses template-based
    extraction (inspired by ccxt-trading's KIS connector) to parse these
    into (price, size) tuples.
    """

    _logger: Optional[HummingbotLogger] = None

    # Delay between REST polling cycles (seconds)
    ORDER_BOOK_SNAPSHOT_DELAY = 5.0

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "KisExchange",
        api_factory: WebAssistantsFactory,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def get_last_traded_prices(
        self,
        trading_pairs: List[str],
        domain: Optional[str] = None,
    ) -> Dict[str, float]:
        """Delegate to the exchange connector's last traded price method."""
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    # ------------------------------------------------------------------
    # Order book snapshots (REST polling)
    # ------------------------------------------------------------------

    async def listen_for_order_book_snapshots(
        self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue
    ):
        """
        Poll REST endpoint for order book snapshots at regular intervals.

        Unlike WebSocket-based connectors that receive push updates, KIS
        requires active polling.  Each cycle fetches a full snapshot for
        every tracked trading pair and enqueues it for the order book
        tracker to apply.
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    snapshot: OrderBookMessage = await self._order_book_snapshot(
                        trading_pair=trading_pair
                    )
                    output.put_nowait(snapshot)
                await self._sleep(self.ORDER_BOOK_SNAPSHOT_DELAY)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error when processing public order book snapshots from exchange"
                )
                await self._sleep(1.0)

    async def listen_for_order_book_diffs(
        self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue
    ):
        """
        No-op: KIS does not provide order book diff streams.
        All order book updates come as full snapshots via REST.
        """
        pass

    async def listen_for_trades(
        self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue
    ):
        """
        No-op: KIS does not provide a public trade stream.
        """
        pass

    async def listen_for_subscriptions(self):
        """
        Block indefinitely. KIS has no WebSocket to subscribe to.

        The base class ``listen_for_subscriptions`` tries to connect to a
        WebSocket and subscribe, which is not applicable for KIS.  Instead
        we simply wait forever so the calling coroutine does not restart
        in a tight loop.
        """
        await asyncio.Event().wait()

    # ------------------------------------------------------------------
    # Snapshot construction
    # ------------------------------------------------------------------

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Build an OrderBookMessage from a KIS REST orderbook response.
        """
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

        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": update_id,
            "bids": bids,
            "asks": asks,
        }
        snapshot_msg = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            order_book_message_content,
            timestamp,
        )
        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Fetch the raw orderbook from KIS REST API.

        Uses the domestic stock orderbook endpoint with TR_ID: FHKST01010200.
        """
        symbol = await self._connector.exchange_symbol_associated_to_pair(
            trading_pair=trading_pair
        )

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(
                path_url=CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH
            ),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_TR_ID,
        )

        return data

    # ------------------------------------------------------------------
    # Template-based level extraction (from ccxt-trading pattern)
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
        """
        Extract price/size levels from a KIS dict-formatted orderbook.

        KIS uses indexed field names like ``askp1``, ``askp2``, ... ``askp10``
        rather than arrays.  This method applies template strings with
        ``{idx}`` placeholders to read each level.

        Levels with zero price or zero size are filtered out.

        :param data: the flat dictionary from KIS API (output1)
        :param depth: max number of levels to read (1-10 for domestic stock)
        :param ask_price_templates: templates for ask price keys, e.g. ["askp{idx}"]
        :param ask_size_templates: templates for ask size keys
        :param bid_price_templates: templates for bid price keys
        :param bid_size_templates: templates for bid size keys
        :return: (asks, bids) as lists of (price, size) tuples
        """
        asks: List[Tuple[float, float]] = []
        bids: List[Tuple[float, float]] = []

        for idx in range(1, depth + 1):
            ask_price = _first_float_by_templates(data, ask_price_templates, idx)
            ask_size = _first_float_by_templates(data, ask_size_templates, idx)
            bid_price = _first_float_by_templates(data, bid_price_templates, idx)
            bid_size = _first_float_by_templates(data, bid_size_templates, idx)

            if (
                ask_price is not None
                and ask_price > 0
                and ask_size is not None
                and ask_size > 0
            ):
                asks.append((ask_price, ask_size))

            if (
                bid_price is not None
                and bid_price > 0
                and bid_size is not None
                and bid_size > 0
            ):
                bids.append((bid_price, bid_size))

        # Sort asks ascending, bids descending by price
        asks.sort(key=lambda x: x[0])
        bids.sort(key=lambda x: x[0], reverse=True)
        return asks, bids

    # ------------------------------------------------------------------
    # No-op WebSocket methods
    # ------------------------------------------------------------------

    async def _connected_websocket_assistant(self) -> WSAssistant:
        raise NotImplementedError("KIS does not support WebSocket order book streams")

    async def _subscribe_channels(self, ws: WSAssistant):
        raise NotImplementedError("KIS does not support WebSocket order book streams")

    async def _parse_trade_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        """No-op: KIS has no trade stream."""
        pass

    async def _parse_order_book_diff_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        """No-op: KIS does not send diff messages."""
        pass

    async def _parse_order_book_snapshot_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        """No-op: Snapshots are fetched via REST polling, not WS."""
        pass

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        """No-op: KIS has no WS order book streams. Always returns True."""
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        """No-op: KIS has no WS order book streams. Always returns True."""
        return True


# ------------------------------------------------------------------
# Module-level helpers (mirrors kis_utils.first_float pattern)
# ------------------------------------------------------------------


def _first_float_by_templates(
    data: Dict[str, Any], templates: List[str], idx: int
) -> Optional[float]:
    """Try each template with ``{idx}`` substitution and return the first valid float."""
    keys = [t.format(idx=idx) for t in templates]
    return kis_utils.first_float(data, keys)
