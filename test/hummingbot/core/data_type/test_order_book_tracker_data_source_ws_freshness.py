from typing import Dict, List, Optional

from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource


class _DS(OrderBookTrackerDataSource):
    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return {}

    async def _connected_websocket_assistant(self):
        pass

    async def _subscribe_channels(self, ws):
        pass

    async def _order_book_snapshot(self, trading_pair):
        pass

    async def _request_order_book_snapshots(self, output):
        pass

    async def _parse_order_book_snapshot_message(self, raw_message, message_queue):
        pass

    async def _parse_order_book_diff_message(self, raw_message, message_queue):
        pass

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True


def test_last_ws_orderbook_time_unproven_is_none():
    ds = _DS(["BTC-USD"])

    assert ds.last_ws_orderbook_time("BTC-USD") is None


def test_mark_then_read_is_per_pair():
    ds = _DS(["A-USD", "B-USD"])

    ds._mark_ws_orderbook_frame("A-USD")

    assert ds.last_ws_orderbook_time("A-USD") is not None
    assert ds.last_ws_orderbook_time("B-USD") is None
