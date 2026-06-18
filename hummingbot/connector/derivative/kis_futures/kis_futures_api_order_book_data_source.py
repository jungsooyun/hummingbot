"""
KIS Futures API Order Book Data Source — 4a stub (inert funding; REST/WS feeds added in later slice).

Implements the PerpetualAPIOrderBookDataSource interface so KisFuturesDerivative can
instantiate and reach READY.  All live-data methods are no-ops or return inert defaults;
they are replaced in Task 4 (slice 5).
"""
import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.connector.derivative.kis_futures.kis_futures_derivative import KisFuturesDerivative
    from hummingbot.connector.exchange.kis.kis_auth import KisAuth


class KisFuturesAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """Stub OB DS for KIS futures.

    Provides inert FundingInfo so start_network() readiness check passes without
    a live connection.  REST-polling and WebSocket feeds are added in slice 5.
    """

    # Override the 1-hour upstream default so REST fallback re-snapshots quickly.
    from hummingbot.connector.derivative.kis_futures import kis_futures_constants as _C
    FULL_ORDER_BOOK_RESET_DELTA_SECONDS = _C.REST_ORDER_BOOK_POLL_INTERVAL

    def __init__(
        self,
        trading_pairs: List[str],
        connector: Optional["KisFuturesDerivative"],
        api_factory: Optional[WebAssistantsFactory],
        auth: Optional["KisAuth"],
        domain: str = "",
        ws_enabled: bool = True,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._auth = auth
        self._domain = domain
        self._ws_enabled = ws_enabled

    # ------------------------------------------------------------------
    # PerpetualAPIOrderBookDataSource abstracts
    # ------------------------------------------------------------------

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """Return inert zero-rate FundingInfo so perpetual readiness check passes."""
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
        # No-op until slice 5 adds WebSocket funding-info parsing.
        return

    # ------------------------------------------------------------------
    # OrderBookTrackerDataSource abstracts
    # ------------------------------------------------------------------

    async def get_last_traded_prices(
        self, trading_pairs: List[str], domain: Optional[str] = None
    ) -> Dict[str, float]:
        return {tp: 0.0 for tp in trading_pairs}

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def listen_for_subscriptions(self):
        """Idle loop — REST/WS feeds added in slice 5 (Task 4)."""
        while True:
            await self._sleep(3600)

    # ------------------------------------------------------------------
    # No-op stubs for remaining base-abstract methods (slice 5)
    # ------------------------------------------------------------------

    async def _connected_websocket_assistant(self):
        return None

    async def _subscribe_channels(self, ws):
        pass

    async def _parse_order_book_diff_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        raise NotImplementedError("slice 5")

    async def _parse_order_book_snapshot_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        raise NotImplementedError("slice 5")

    async def _parse_trade_message(
        self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        raise NotImplementedError("slice 5")

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raise NotImplementedError("slice 5")
