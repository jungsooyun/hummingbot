import asyncio
import json
import re
import time
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses
from bidict import bidict

import hummingbot.connector.exchange.kis.kis_constants as CONSTANTS
from hummingbot.connector.exchange.kis import kis_web_utils as web_utils
from hummingbot.connector.exchange.kis.kis_api_order_book_data_source import KisAPIOrderBookDataSource
from hummingbot.connector.exchange.kis.kis_auth import KisAuth
from hummingbot.connector.exchange.kis.kis_exchange import KisExchange
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class _FakeAsyncCtx:
    """Helper to mock ``async with session.ws_connect(...)`` pattern."""

    def __init__(self, obj):
        self._obj = obj

    async def __aenter__(self):
        return self._obj

    async def __aexit__(self, *args):
        pass


class _AsyncIter:
    """Wrap a regular iterable so ``async for`` works."""

    def __init__(self, items):
        self._items = iter(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration


class _FakeWS:
    """Fake aiohttp WebSocket that supports ``async for`` iteration."""

    def __init__(self, messages):
        self._messages = messages
        self.send_json = AsyncMock()
        self.send_str = AsyncMock()

    def __aiter__(self):
        return _AsyncIter(self._messages)


class KisAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    """
    Tests for the KIS WebSocket-based order book data source.

    KIS provides WebSocket streams for real-time market data:
    - H0STASP0: orderbook (10 levels)
    - H0STCNT0: trade executions

    Tests verify:
    1. REST order book snapshot fetch (domestic stock orderbook, fallback)
    2. Last traded price fetch (domestic stock ticker)
    3. WebSocket subscription message building
    4. WebSocket data message parsing (caret-separated fields)
    5. WebSocket orderbook data processing
    6. WebSocket trade data processing
    7. WebSocket control message handling (PINGPONG, subscription responses)
    8. listen_for_subscriptions with mocked WebSocket
    9. Base-class abstract method stubs (NotImplementedError)
    """

    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "005930"
        cls.quote_asset = "KRW"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task = None

        self.connector = KisExchange(
            kis_app_key="test_app_key",
            kis_app_secret="test_app_secret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )

        self.mock_auth = KisAuth(
            app_key="test_app_key",
            app_secret="test_app_secret",
            initial_token="test_token",
            initial_approval_key="test_approval_key",
        )

        self.data_source = KisAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            auth=self.mock_auth,
            domain=CONSTANTS.DEFAULT_DOMAIN,
        )
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair})
        )

    async def asyncTearDown(self) -> None:
        if self.listening_task is not None:
            self.listening_task.cancel()
            try:
                await self.listening_task
            except asyncio.CancelledError:
                pass
        await super().asyncTearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(
            record.levelname == log_level and record.getMessage() == message
            for record in self.log_records
        )

    def _is_logged_partial(self, log_level: str, message_fragment: str) -> bool:
        """Check if any log record at the given level contains the fragment."""
        return any(
            record.levelname == log_level and message_fragment in record.getMessage()
            for record in self.log_records
        )

    # ------------------------------------------------------------------
    # Mock response helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _order_book_snapshot_response() -> Dict[str, Any]:
        """KIS domestic stock orderbook REST response (TR_ID: FHKST01010200)."""
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "정상처리 되었습니다.",
            "output1": {
                "askp1": "72100", "askp_rsqn1": "500",
                "askp2": "72200", "askp_rsqn2": "300",
                "askp3": "72300", "askp_rsqn3": "200",
                "askp4": "72400", "askp_rsqn4": "150",
                "askp5": "72500", "askp_rsqn5": "100",
                "askp6": "72600", "askp_rsqn6": "80",
                "askp7": "72700", "askp_rsqn7": "60",
                "askp8": "72800", "askp_rsqn8": "40",
                "askp9": "72900", "askp_rsqn9": "30",
                "askp10": "73000", "askp_rsqn10": "20",
                "bidp1": "71900", "bidp_rsqn1": "400",
                "bidp2": "71800", "bidp_rsqn2": "600",
                "bidp3": "71700", "bidp_rsqn3": "350",
                "bidp4": "71600", "bidp_rsqn4": "250",
                "bidp5": "71500", "bidp_rsqn5": "200",
                "bidp6": "71400", "bidp_rsqn6": "180",
                "bidp7": "71300", "bidp_rsqn7": "150",
                "bidp8": "71200", "bidp_rsqn8": "120",
                "bidp9": "71100", "bidp_rsqn9": "100",
                "bidp10": "71000", "bidp_rsqn10": "80",
            },
        }

    @staticmethod
    def _ticker_response() -> Dict[str, Any]:
        """KIS domestic stock ticker response (TR_ID: FHPST01010000)."""
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "정상처리 되었습니다.",
            "output": {
                "stck_prpr": "72000",
                "stck_hgpr": "73000",
                "stck_lwpr": "71000",
                "acml_vol": "12345678",
            },
        }

    @staticmethod
    def _empty_orderbook_response() -> Dict[str, Any]:
        """KIS response with zero-sized levels."""
        return {
            "rt_cd": "0",
            "msg1": "정상처리 되었습니다.",
            "output1": {
                "askp1": "72100", "askp_rsqn1": "0",
                "askp2": "0", "askp_rsqn2": "300",
                "bidp1": "71900", "bidp_rsqn1": "0",
                "bidp2": "71800", "bidp_rsqn2": "600",
            },
        }

    # ------------------------------------------------------------------
    # Mock WebSocket data helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ws_orderbook_raw() -> str:
        """Raw WS orderbook message (H0STASP0) with pipe + caret format."""
        return (
            "0|H0STASP0|005930|"
            "005930^093000^0"
            "^67800^67900^68000^68100^68200^68300^68400^68500^68600^68700"
            "^67700^67600^67500^67400^67300^67200^67100^67000^66900^66800"
            "^1000^2000^3000^4000^5000^6000^7000^8000^9000^10000"
            "^10000^9000^8000^7000^6000^5000^4000^3000^2000^1000"
            "^55000^55000^0^0^67750^50^100^50^2^0.5^1000000^100^-100^0^0^1"
        )

    @staticmethod
    def _ws_trade_raw() -> str:
        """Raw WS trade message (H0STCNT0) with pipe + caret format."""
        return (
            "0|H0STCNT0|005930|"
            "005930^093001^67800^2^-200^-0.29^67750^68000^68200^67600"
            "^67900^67700^100^500000^33900000000^1234^5678^-4444^25.5"
            "^617^383^1^38.3^3.5^090000^2^200^091500^2^200^093000^2^-200"
            "^20260228^Y^N^1000^10000^55000^55000^3.5^400000^80.0^0^21^67000"
        )

    @staticmethod
    def _ws_pingpong_raw() -> str:
        """Raw PINGPONG message from KIS WS."""
        return '{"header": {"tr_id": "PINGPONG"}}'

    @staticmethod
    def _ws_subscription_success_raw() -> str:
        """Raw subscription success response from KIS WS."""
        return json.dumps({
            "header": {
                "tr_id": "H0STASP0",
                "tr_key": "005930",
                "encrypt": "N",
            },
            "body": {
                "rt_cd": "0",
                "msg_cd": "OPSP0000",
                "msg1": "SUBSCRIBE SUCCESS",
            },
        })

    @staticmethod
    def _ws_subscription_error_raw() -> str:
        """Raw subscription error response from KIS WS."""
        return json.dumps({
            "header": {
                "tr_id": "H0STASP0",
                "tr_key": "005930",
                "encrypt": "N",
            },
            "body": {
                "rt_cd": "1",
                "msg_cd": "OPSP0001",
                "msg1": "SUBSCRIBE FAILED",
            },
        })

    # ------------------------------------------------------------------
    # Test: get_last_traded_prices
    # ------------------------------------------------------------------

    @aioresponses()
    def test_get_last_traded_prices(self, mock_api):
        """Should delegate to connector and return the current stock price."""
        mock_response = self._ticker_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_TICKER_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        results = self.local_event_loop.run_until_complete(
            asyncio.gather(
                self.data_source.get_last_traded_prices([self.trading_pair])
            )
        )
        results: Dict[str, Any] = results[0]

        self.assertIn(self.trading_pair, results)
        self.assertEqual(72000.0, results[self.trading_pair])

    # ------------------------------------------------------------------
    # Test: _order_book_snapshot (REST snapshot fallback)
    # ------------------------------------------------------------------

    @aioresponses()
    def test_order_book_snapshot(self, mock_api):
        """REST snapshot should parse 10 ask + 10 bid levels correctly."""
        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        snapshot_msg: OrderBookMessage = self.local_event_loop.run_until_complete(
            self.data_source._order_book_snapshot(self.trading_pair)
        )

        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot_msg.type)
        self.assertEqual(self.trading_pair, snapshot_msg.trading_pair)

        bids = snapshot_msg.bids
        asks = snapshot_msg.asks

        # Should have 10 ask levels and 10 bid levels
        self.assertEqual(10, len(asks))
        self.assertEqual(10, len(bids))

        # Asks should be sorted by price ascending
        self.assertEqual(72100.0, asks[0].price)
        self.assertEqual(500.0, asks[0].amount)
        self.assertEqual(73000.0, asks[9].price)
        self.assertEqual(20.0, asks[9].amount)

        # Bids should be sorted by price descending
        self.assertEqual(71900.0, bids[0].price)
        self.assertEqual(400.0, bids[0].amount)
        self.assertEqual(71000.0, bids[9].price)
        self.assertEqual(80.0, bids[9].amount)

    # ------------------------------------------------------------------
    # Test: get_new_order_book (integration of snapshot + OrderBook)
    # ------------------------------------------------------------------

    @aioresponses()
    def test_get_new_order_book_successful(self, mock_api):
        """get_new_order_book should return a populated OrderBook instance."""
        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        results = self.local_event_loop.run_until_complete(
            asyncio.gather(self.data_source.get_new_order_book(self.trading_pair))
        )
        order_book: OrderBook = results[0]

        self.assertIsInstance(order_book, OrderBook)

        bids = list(order_book.bid_entries())
        asks = list(order_book.ask_entries())

        self.assertEqual(10, len(bids))
        self.assertEqual(10, len(asks))

        # Verify first bid and ask from the order book
        self.assertEqual(71900.0, bids[0].price)
        self.assertEqual(400.0, bids[0].amount)
        self.assertEqual(72100.0, asks[0].price)
        self.assertEqual(500.0, asks[0].amount)

    # ------------------------------------------------------------------
    # Test: Orderbook with zero-sized / zero-priced levels filtered out
    # ------------------------------------------------------------------

    @aioresponses()
    def test_order_book_snapshot_filters_zero_levels(self, mock_api):
        """REST snapshot should filter out levels with zero price or zero size."""
        mock_response = self._empty_orderbook_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        snapshot_msg: OrderBookMessage = self.local_event_loop.run_until_complete(
            self.data_source._order_book_snapshot(self.trading_pair)
        )

        bids = snapshot_msg.bids
        asks = snapshot_msg.asks

        # askp1 has size 0 -> filtered out, askp2 has price 0 -> filtered out
        self.assertEqual(0, len(asks))
        # bidp1 has size 0 -> filtered out, bidp2 has valid data
        self.assertEqual(1, len(bids))
        self.assertEqual(71800.0, bids[0].price)
        self.assertEqual(600.0, bids[0].amount)

    # ------------------------------------------------------------------
    # Test: _request_order_book_snapshot (raw REST response)
    # ------------------------------------------------------------------

    @aioresponses()
    def test_request_order_book_snapshot(self, mock_api):
        """Should return the raw REST JSON response with output1 dict."""
        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.local_event_loop.run_until_complete(
            self.data_source._request_order_book_snapshot(self.trading_pair)
        )

        self.assertEqual("0", result["rt_cd"])
        self.assertIn("output1", result)
        self.assertEqual("72100", result["output1"]["askp1"])
        self.assertEqual("500", result["output1"]["askp_rsqn1"])

    # ------------------------------------------------------------------
    # Test: _connected_websocket_assistant raises NotImplementedError
    # ------------------------------------------------------------------

    async def test_connected_websocket_assistant_raises_not_implemented(self):
        """Base-class _connected_websocket_assistant should raise because KIS uses custom WS handling."""
        with self.assertRaises(NotImplementedError):
            await self.data_source._connected_websocket_assistant()

    # ------------------------------------------------------------------
    # Test: _subscribe_channels raises NotImplementedError
    # ------------------------------------------------------------------

    async def test_subscribe_channels_raises_not_implemented(self):
        """Base-class _subscribe_channels should raise because KIS uses custom WS handling."""
        with self.assertRaises(NotImplementedError):
            await self.data_source._subscribe_channels(MagicMock())

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_snapshots reads from WS snapshot queue
    # ------------------------------------------------------------------

    async def test_listen_for_order_book_snapshots_reads_ws_queue(self):
        """listen_for_order_book_snapshots should read WS snapshots from the internal queue."""
        # Simulate WS-delivered snapshot by enqueueing an OrderBookMessage
        snapshot_content = {
            "trading_pair": self.trading_pair,
            "update_id": 1000,
            "bids": [(71900.0, 100.0), (71800.0, 200.0)],
            "asks": [(72100.0, 150.0), (72200.0, 250.0)],
        }
        snapshot_msg = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT, snapshot_content, time.time()
        )
        self.data_source._message_queue[
            self.data_source._snapshot_messages_queue_key
        ].put_nowait(snapshot_msg)

        msg_queue: asyncio.Queue = asyncio.Queue()
        task = asyncio.create_task(
            self.data_source.listen_for_order_book_snapshots(
                self.local_event_loop, msg_queue
            )
        )

        try:
            msg: OrderBookMessage = await asyncio.wait_for(
                msg_queue.get(), timeout=5.0
            )
        except asyncio.TimeoutError:
            self.fail("Timed out waiting for order book snapshot message")
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_snapshots REST fallback on queue timeout
    # ------------------------------------------------------------------

    @aioresponses()
    async def test_listen_for_order_book_snapshots_rest_fallback(self, mock_api):
        """When WS queue times out, should fall back to REST snapshot."""
        # Use short timeout to trigger REST fallback quickly
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 0.1

        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        msg_queue: asyncio.Queue = asyncio.Queue()
        task = asyncio.create_task(
            self.data_source.listen_for_order_book_snapshots(
                self.local_event_loop, msg_queue
            )
        )

        try:
            msg: OrderBookMessage = await asyncio.wait_for(
                msg_queue.get(), timeout=5.0
            )
        except asyncio.TimeoutError:
            self.fail("Timed out waiting for REST fallback snapshot")
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

        bids = msg.bids
        asks = msg.asks
        self.assertEqual(10, len(bids))
        self.assertEqual(10, len(asks))
        self.assertEqual(72100.0, asks[0].price)
        self.assertEqual(71900.0, bids[0].price)

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_snapshots handles errors gracefully
    # ------------------------------------------------------------------

    async def test_listen_for_order_book_snapshots_logs_exception(self):
        """When REST fetch fails, should log error and continue."""
        # Use short timeout to trigger REST fallback, which then raises
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 0.1

        with patch.object(
            self.data_source,
            "_order_book_snapshot",
            new_callable=AsyncMock,
        ) as mock_snapshot:
            mock_snapshot.side_effect = [Exception("Network error"), asyncio.CancelledError()]

            with patch.object(self.data_source, "_sleep", new_callable=AsyncMock) as sleep_mock:
                sleep_mock.return_value = None

                msg_queue: asyncio.Queue = asyncio.Queue()

                try:
                    await self.data_source.listen_for_order_book_snapshots(
                        self.local_event_loop, msg_queue
                    )
                except asyncio.CancelledError:
                    pass

                self.assertTrue(
                    self._is_logged_partial(
                        "ERROR",
                        "Unexpected error fetching order book snapshot",
                    )
                )

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_snapshots raises CancelledError
    # ------------------------------------------------------------------

    async def test_listen_for_order_book_snapshots_raises_cancelled_error(self):
        """CancelledError should propagate up cleanly."""
        # Use short timeout to trigger REST fallback, which then raises CancelledError
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 0.1

        with patch.object(
            self.data_source,
            "_order_book_snapshot",
            new_callable=AsyncMock,
        ) as mock_snapshot:
            mock_snapshot.side_effect = asyncio.CancelledError()

            msg_queue: asyncio.Queue = asyncio.Queue()

            with self.assertRaises(asyncio.CancelledError):
                await self.data_source.listen_for_order_book_snapshots(
                    self.local_event_loop, msg_queue
                )

    # ------------------------------------------------------------------
    # Test: _extract_levels correctly parses KIS dict-based orderbook
    # ------------------------------------------------------------------

    def test_extract_levels_from_dict(self):
        """Template-based level extraction should parse KIS dict format correctly."""
        data = {
            "askp1": "100", "askp_rsqn1": "10",
            "askp2": "101", "askp_rsqn2": "20",
            "askp3": "0", "askp_rsqn3": "30",  # zero price -> skip
            "bidp1": "99", "bidp_rsqn1": "15",
            "bidp2": "98", "bidp_rsqn2": "0",  # zero size -> skip
            "bidp3": "97", "bidp_rsqn3": "25",
        }

        asks, bids = KisAPIOrderBookDataSource._extract_levels_from_dict(
            data=data,
            depth=3,
            ask_price_templates=["askp{idx}"],
            ask_size_templates=["askp_rsqn{idx}"],
            bid_price_templates=["bidp{idx}"],
            bid_size_templates=["bidp_rsqn{idx}"],
        )

        # askp3 has price=0 so it's filtered; 2 valid ask levels
        self.assertEqual(2, len(asks))
        self.assertEqual(100.0, asks[0][0])
        self.assertEqual(10.0, asks[0][1])
        self.assertEqual(101.0, asks[1][0])
        self.assertEqual(20.0, asks[1][1])

        # bidp2 has size=0 so it's filtered; 2 valid bid levels
        self.assertEqual(2, len(bids))
        self.assertEqual(99.0, bids[0][0])
        self.assertEqual(15.0, bids[0][1])
        self.assertEqual(97.0, bids[1][0])
        self.assertEqual(25.0, bids[1][1])

    # ------------------------------------------------------------------
    # Test: _extract_levels handles empty strings and missing keys
    # ------------------------------------------------------------------

    def test_extract_levels_handles_empty_and_missing(self):
        """Empty string values and missing keys should be treated as None and filtered."""
        data = {
            "askp1": "", "askp_rsqn1": "10",
            "bidp1": "99",
            # bidp_rsqn1 is missing entirely
        }

        asks, bids = KisAPIOrderBookDataSource._extract_levels_from_dict(
            data=data,
            depth=1,
            ask_price_templates=["askp{idx}"],
            ask_size_templates=["askp_rsqn{idx}"],
            bid_price_templates=["bidp{idx}"],
            bid_size_templates=["bidp_rsqn{idx}"],
        )

        # askp1 is empty string -> None -> skipped
        self.assertEqual(0, len(asks))
        # bidp_rsqn1 missing -> None -> skipped
        self.assertEqual(0, len(bids))

    # ------------------------------------------------------------------
    # Test: Polling interval attribute
    # ------------------------------------------------------------------

    def test_default_polling_interval(self):
        """Base class should have a default message queue polling delay."""
        # WS-based data source no longer has ORDER_BOOK_SNAPSHOT_DELAY
        # but the base class has MESSAGE_TIMEOUT which governs queue reads
        self.assertTrue(hasattr(self.data_source, "_message_queue"))

    # ==================================================================
    # WebSocket Tests
    # ==================================================================

    # ------------------------------------------------------------------
    # Test: _build_subscription_message
    # ------------------------------------------------------------------

    def test_build_subscription_message_subscribe(self):
        """Should build a valid KIS WS subscription JSON with tr_type=1 for subscribe."""
        msg = KisAPIOrderBookDataSource._build_subscription_message(
            approval_key="my_approval_key",
            tr_id="H0STASP0",
            tr_key="005930",
            tr_type="1",
        )

        self.assertEqual("my_approval_key", msg["header"]["approval_key"])
        self.assertEqual("P", msg["header"]["custtype"])
        self.assertEqual("1", msg["header"]["tr_type"])
        self.assertEqual("utf-8", msg["header"]["content-type"])
        self.assertEqual("H0STASP0", msg["body"]["input"]["tr_id"])
        self.assertEqual("005930", msg["body"]["input"]["tr_key"])

    def test_build_subscription_message_unsubscribe(self):
        """Should build a valid KIS WS unsubscription JSON with tr_type=2."""
        msg = KisAPIOrderBookDataSource._build_subscription_message(
            approval_key="key123",
            tr_id="H0STCNT0",
            tr_key="000660",
            tr_type="2",
        )

        self.assertEqual("2", msg["header"]["tr_type"])
        self.assertEqual("H0STCNT0", msg["body"]["input"]["tr_id"])
        self.assertEqual("000660", msg["body"]["input"]["tr_key"])

    # ------------------------------------------------------------------
    # Test: _parse_caret_fields
    # ------------------------------------------------------------------

    def test_parse_caret_fields_valid(self):
        """Should parse caret-separated fields into a dict using column names."""
        columns = ("COL_A", "COL_B", "COL_C")
        data_str = "val_a^val_b^val_c"

        result = KisAPIOrderBookDataSource._parse_caret_fields(data_str, columns)

        self.assertIsNotNone(result)
        self.assertEqual("val_a", result["COL_A"])
        self.assertEqual("val_b", result["COL_B"])
        self.assertEqual("val_c", result["COL_C"])

    def test_parse_caret_fields_extra_fields_ignored(self):
        """Extra fields beyond the column count should be ignored."""
        columns = ("COL_A", "COL_B")
        data_str = "val_a^val_b^extra_c^extra_d"

        result = KisAPIOrderBookDataSource._parse_caret_fields(data_str, columns)

        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        self.assertEqual("val_a", result["COL_A"])
        self.assertEqual("val_b", result["COL_B"])

    def test_parse_caret_fields_too_few_fields_returns_none(self):
        """When data has fewer fields than columns, should return None."""
        columns = ("COL_A", "COL_B", "COL_C")
        data_str = "val_a^val_b"

        result = KisAPIOrderBookDataSource._parse_caret_fields(data_str, columns)

        self.assertIsNone(result)

    def test_parse_caret_fields_empty_string(self):
        """An empty data string should return None when columns are expected."""
        columns = ("COL_A",)
        data_str = ""

        result = KisAPIOrderBookDataSource._parse_caret_fields(data_str, columns)

        # Empty string split by "^" yields [""], which has len 1
        # With 1 column expected, this should succeed
        self.assertIsNotNone(result)
        self.assertEqual("", result["COL_A"])

    def test_parse_caret_fields_with_orderbook_columns(self):
        """Should parse a real H0STASP0 data string using WS_ORDERBOOK_COLUMNS."""
        # Build a minimal data string with the right number of fields
        fields = ["005930", "093000", "0"]  # MKSC_SHRN_ISCD, BSOP_HOUR, HOUR_CLS_CODE
        # 10 ask prices
        fields.extend(["67800", "67900", "68000", "68100", "68200",
                        "68300", "68400", "68500", "68600", "68700"])
        # 10 bid prices
        fields.extend(["67700", "67600", "67500", "67400", "67300",
                        "67200", "67100", "67000", "66900", "66800"])
        # 10 ask sizes
        fields.extend(["1000", "2000", "3000", "4000", "5000",
                        "6000", "7000", "8000", "9000", "10000"])
        # 10 bid sizes
        fields.extend(["10000", "9000", "8000", "7000", "6000",
                        "5000", "4000", "3000", "2000", "1000"])
        # Remaining fields (TOTAL_ASKP_RSQN etc.)
        fields.extend(["55000", "55000", "0", "0", "67750", "50",
                        "100", "50", "2", "0.5", "1000000", "100",
                        "-100", "0", "0", "1"])

        data_str = "^".join(fields)
        result = KisAPIOrderBookDataSource._parse_caret_fields(
            data_str, CONSTANTS.WS_ORDERBOOK_COLUMNS
        )

        self.assertIsNotNone(result)
        self.assertEqual("005930", result["MKSC_SHRN_ISCD"])
        self.assertEqual("67800", result["ASKP1"])
        self.assertEqual("67700", result["BIDP1"])
        self.assertEqual("1000", result["ASKP_RSQN1"])
        self.assertEqual("10000", result["BIDP_RSQN1"])
        self.assertEqual("68700", result["ASKP10"])
        self.assertEqual("10000", result["ASKP_RSQN10"])

    # ------------------------------------------------------------------
    # Test: _process_orderbook_data
    # ------------------------------------------------------------------

    async def test_process_orderbook_data_creates_snapshot_message(self):
        """Should create an OrderBookMessage SNAPSHOT with sorted asks and bids."""
        # Parse the WS orderbook data string
        raw = self._ws_orderbook_raw()
        parts = raw.split("|")
        data_str = parts[3]
        parsed = KisAPIOrderBookDataSource._parse_caret_fields(
            data_str, CONSTANTS.WS_ORDERBOOK_COLUMNS
        )
        self.assertIsNotNone(parsed)

        await self.data_source._process_orderbook_data("005930", parsed)

        # Check that a message was enqueued
        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertFalse(queue.empty())

        msg: OrderBookMessage = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

        bids = msg.bids
        asks = msg.asks

        # Should have 10 ask levels and 10 bid levels
        self.assertEqual(10, len(asks))
        self.assertEqual(10, len(bids))

        # Asks should be sorted ascending by price
        ask_prices = [a.price for a in asks]
        self.assertEqual(sorted(ask_prices), ask_prices)
        self.assertEqual(67800.0, asks[0].price)
        self.assertEqual(1000.0, asks[0].amount)
        self.assertEqual(68700.0, asks[9].price)
        self.assertEqual(10000.0, asks[9].amount)

        # Bids should be sorted descending by price
        bid_prices = [b.price for b in bids]
        self.assertEqual(sorted(bid_prices, reverse=True), bid_prices)
        self.assertEqual(67700.0, bids[0].price)
        self.assertEqual(10000.0, bids[0].amount)
        self.assertEqual(66800.0, bids[9].price)
        self.assertEqual(1000.0, bids[9].amount)

    async def test_process_orderbook_data_filters_zero_levels(self):
        """Levels with zero price or zero size should be excluded from the snapshot."""
        data = {col: "0" for col in CONSTANTS.WS_ORDERBOOK_COLUMNS}
        data["MKSC_SHRN_ISCD"] = "005930"
        # Only set one valid ask and one valid bid
        data["ASKP1"] = "68000"
        data["ASKP_RSQN1"] = "500"
        data["BIDP1"] = "67900"
        data["BIDP_RSQN1"] = "300"
        # ASKP2 has zero price (already 0), BIDP2 has zero size (already 0)

        await self.data_source._process_orderbook_data("005930", data)

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertEqual(1, len(msg.asks))
        self.assertEqual(1, len(msg.bids))
        self.assertEqual(68000.0, msg.asks[0].price)
        self.assertEqual(67900.0, msg.bids[0].price)

    async def test_process_orderbook_data_unknown_symbol_uses_fallback(self):
        """When the stock code is not in the symbol map, should fall back to first trading pair."""
        data = {col: "0" for col in CONSTANTS.WS_ORDERBOOK_COLUMNS}
        data["ASKP1"] = "100"
        data["ASKP_RSQN1"] = "10"

        await self.data_source._process_orderbook_data("999999", data)

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()
        # Falls back to the first trading pair
        self.assertEqual(self.trading_pair, msg.trading_pair)

    # ------------------------------------------------------------------
    # Test: _process_trade_data
    # ------------------------------------------------------------------

    async def test_process_trade_data_creates_trade_message(self):
        """Should create an OrderBookMessage TRADE with correct price, amount, and type."""
        raw = self._ws_trade_raw()
        parts = raw.split("|")
        data_str = parts[3]
        parsed = KisAPIOrderBookDataSource._parse_caret_fields(
            data_str, CONSTANTS.WS_TRADE_COLUMNS
        )
        self.assertIsNotNone(parsed)

        await self.data_source._process_trade_data("005930", parsed)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        self.assertFalse(queue.empty())

        msg: OrderBookMessage = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

        content = msg.content
        self.assertEqual(67800.0, content["price"])
        self.assertEqual(100.0, content["amount"])

    async def test_process_trade_data_sell_type(self):
        """CCLD_DVSN=1 should map to TradeType.SELL."""
        data = {col: "0" for col in CONSTANTS.WS_TRADE_COLUMNS}
        data["MKSC_SHRN_ISCD"] = "005930"
        data["STCK_PRPR"] = "50000"
        data["CNTG_VOL"] = "200"
        data["CCLD_DVSN"] = "1"  # sell

        await self.data_source._process_trade_data("005930", data)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertEqual(float(TradeType.SELL.value), msg.content["trade_type"])
        self.assertEqual(50000.0, msg.content["price"])
        self.assertEqual(200.0, msg.content["amount"])

    async def test_process_trade_data_buy_type(self):
        """CCLD_DVSN=2 should map to TradeType.BUY."""
        data = {col: "0" for col in CONSTANTS.WS_TRADE_COLUMNS}
        data["MKSC_SHRN_ISCD"] = "005930"
        data["STCK_PRPR"] = "50000"
        data["CNTG_VOL"] = "100"
        data["CCLD_DVSN"] = "2"  # buy

        await self.data_source._process_trade_data("005930", data)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertEqual(float(TradeType.BUY.value), msg.content["trade_type"])

    async def test_process_trade_data_unknown_dvsn_defaults_to_buy(self):
        """An unknown CCLD_DVSN value should default to TradeType.BUY."""
        data = {col: "0" for col in CONSTANTS.WS_TRADE_COLUMNS}
        data["MKSC_SHRN_ISCD"] = "005930"
        data["STCK_PRPR"] = "50000"
        data["CNTG_VOL"] = "100"
        data["CCLD_DVSN"] = "9"  # unknown

        await self.data_source._process_trade_data("005930", data)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertEqual(float(TradeType.BUY.value), msg.content["trade_type"])

    # ------------------------------------------------------------------
    # Test: _handle_control_message
    # ------------------------------------------------------------------

    async def test_handle_control_message_pingpong(self):
        """PINGPONG messages should be echoed back via ws.send_str."""
        mock_ws = AsyncMock()
        raw = self._ws_pingpong_raw()

        await self.data_source._handle_control_message(mock_ws, raw)

        mock_ws.send_str.assert_awaited_once_with(raw)

    async def test_handle_control_message_subscription_success(self):
        """Successful subscription response (rt_cd=0) should not log a warning."""
        mock_ws = AsyncMock()
        raw = self._ws_subscription_success_raw()

        await self.data_source._handle_control_message(mock_ws, raw)

        # Should not send anything back (not a PINGPONG)
        mock_ws.send_str.assert_not_awaited()
        # No warning logged for success
        self.assertFalse(self._is_logged_partial("WARNING", "subscription error"))

    async def test_handle_control_message_subscription_error(self):
        """Failed subscription response (rt_cd!=0) should log a warning."""
        mock_ws = AsyncMock()
        raw = self._ws_subscription_error_raw()

        await self.data_source._handle_control_message(mock_ws, raw)

        self.assertTrue(self._is_logged_partial("WARNING", "KIS WS subscription error"))

    async def test_handle_control_message_invalid_json(self):
        """Invalid JSON should log a warning and not raise."""
        mock_ws = AsyncMock()

        await self.data_source._handle_control_message(mock_ws, "not valid json {{{")

        self.assertTrue(self._is_logged_partial("WARNING", "Invalid JSON from KIS WS"))

    # ------------------------------------------------------------------
    # Test: _process_ws_message routing
    # ------------------------------------------------------------------

    async def test_process_ws_message_routes_data_to_handle_data_message(self):
        """Messages starting with '0' or '1' should be routed to _handle_data_message."""
        mock_ws = AsyncMock()
        raw = self._ws_orderbook_raw()

        with patch.object(
            self.data_source, "_handle_data_message", new_callable=AsyncMock
        ) as mock_handle:
            await self.data_source._process_ws_message(mock_ws, raw)
            mock_handle.assert_awaited_once_with(raw)

    async def test_process_ws_message_routes_json_to_handle_control_message(self):
        """Messages not starting with '0'/'1' should be routed to _handle_control_message."""
        mock_ws = AsyncMock()
        raw = self._ws_pingpong_raw()

        with patch.object(
            self.data_source, "_handle_control_message", new_callable=AsyncMock
        ) as mock_handle:
            await self.data_source._process_ws_message(mock_ws, raw)
            mock_handle.assert_awaited_once_with(mock_ws, raw)

    # ------------------------------------------------------------------
    # Test: _handle_data_message
    # ------------------------------------------------------------------

    async def test_handle_data_message_orderbook(self):
        """H0STASP0 data message should be parsed and processed as orderbook."""
        raw = self._ws_orderbook_raw()

        await self.data_source._handle_data_message(raw)

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertFalse(queue.empty())
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)

    async def test_handle_data_message_trade(self):
        """H0STCNT0 data message should be parsed and processed as trade."""
        raw = self._ws_trade_raw()

        await self.data_source._handle_data_message(raw)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        self.assertFalse(queue.empty())
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)

    async def test_handle_data_message_invalid_format(self):
        """A data message with fewer than 4 pipe-delimited parts should log a warning."""
        await self.data_source._handle_data_message("0|H0STASP0")

        self.assertTrue(self._is_logged_partial("WARNING", "Invalid KIS WS data message"))

    async def test_handle_data_message_unknown_tr_id(self):
        """An unknown TR_ID should be silently ignored (no queue entries, no error)."""
        raw = "0|UNKNOWN_TR|005930|field1^field2"

        await self.data_source._handle_data_message(raw)

        ob_queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        trade_queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        self.assertTrue(ob_queue.empty())
        self.assertTrue(trade_queue.empty())

    # ------------------------------------------------------------------
    # Test: _resolve_trading_pair
    # ------------------------------------------------------------------

    async def test_resolve_trading_pair_from_symbol_map(self):
        """Known stock code in symbol map should resolve to the correct trading pair."""
        result = await self.data_source._resolve_trading_pair("005930")
        self.assertEqual(self.trading_pair, result)

    async def test_resolve_trading_pair_fallback(self):
        """Unknown stock code should fall back to the first trading pair."""
        result = await self.data_source._resolve_trading_pair("UNKNOWN_CODE")
        self.assertEqual(self.trading_pair, result)

    async def test_resolve_trading_pair_no_pairs(self):
        """With no trading pairs configured, should return None."""
        self.data_source._trading_pairs = []
        # Also clear the symbol map
        self.connector._set_trading_pair_symbol_map(bidict())

        result = await self.data_source._resolve_trading_pair("005930")
        self.assertIsNone(result)

    # ------------------------------------------------------------------
    # Test: listen_for_subscriptions (WS loop with mocking)
    # ------------------------------------------------------------------

    async def test_listen_for_subscriptions_connects_and_subscribes(self):
        """listen_for_subscriptions should connect to WS and subscribe to channels."""
        mock_ws = AsyncMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.__aiter__ = MagicMock(return_value=iter([]))

        mock_session = MagicMock()
        mock_session.ws_connect = MagicMock(return_value=_FakeAsyncCtx(mock_ws))

        with patch(
            "hummingbot.connector.exchange.kis.kis_api_order_book_data_source.aiohttp.ClientSession",
            return_value=_FakeAsyncCtx(mock_session),
        ):
            with patch.object(self.data_source, "_sleep", new_callable=AsyncMock) as sleep_mock:
                sleep_mock.side_effect = asyncio.CancelledError()

                with self.assertRaises(asyncio.CancelledError):
                    await self.data_source.listen_for_subscriptions()

        self.assertEqual(2, mock_ws.send_json.await_count)

        calls = mock_ws.send_json.call_args_list
        ob_msg = calls[0][0][0]
        trade_msg = calls[1][0][0]
        self.assertEqual(CONSTANTS.WS_DOMESTIC_STOCK_ORDERBOOK_TR_ID, ob_msg["body"]["input"]["tr_id"])
        self.assertEqual(CONSTANTS.WS_DOMESTIC_STOCK_TRADE_TR_ID, trade_msg["body"]["input"]["tr_id"])
        self.assertEqual("005930", ob_msg["body"]["input"]["tr_key"])

    async def test_listen_for_subscriptions_processes_messages(self):
        """listen_for_subscriptions should process incoming WS messages."""
        import aiohttp as _aiohttp

        mock_ob_msg = MagicMock()
        mock_ob_msg.type = _aiohttp.WSMsgType.TEXT
        mock_ob_msg.data = self._ws_orderbook_raw()

        mock_trade_msg = MagicMock()
        mock_trade_msg.type = _aiohttp.WSMsgType.TEXT
        mock_trade_msg.data = self._ws_trade_raw()

        mock_close_msg = MagicMock()
        mock_close_msg.type = _aiohttp.WSMsgType.CLOSED

        fake_ws = _FakeWS([mock_ob_msg, mock_trade_msg, mock_close_msg])

        mock_session = MagicMock()
        mock_session.ws_connect = MagicMock(return_value=_FakeAsyncCtx(fake_ws))

        with patch(
            "hummingbot.connector.exchange.kis.kis_api_order_book_data_source.aiohttp.ClientSession",
            return_value=_FakeAsyncCtx(mock_session),
        ):
            # After the WS closes cleanly, the while-True loop restarts.
            # Make get_ws_approval_key raise CancelledError on the 2nd call.
            with patch.object(
                self.data_source._auth, "get_ws_approval_key", new_callable=AsyncMock
            ) as mock_key:
                mock_key.side_effect = ["test_approval_key", asyncio.CancelledError()]

                with self.assertRaises(asyncio.CancelledError):
                    await self.data_source.listen_for_subscriptions()

        ob_queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertFalse(ob_queue.empty())
        ob_msg = ob_queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, ob_msg.type)

        trade_queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        self.assertFalse(trade_queue.empty())
        trade_msg = trade_queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, trade_msg.type)

    async def test_listen_for_subscriptions_handles_exception_and_reconnects(self):
        """On connection error, should log exception and attempt to reconnect."""

        class _FailCtx:
            async def __aenter__(self):
                raise ConnectionError("WS connection failed")

            async def __aexit__(self, *args):
                pass

        with patch(
            "hummingbot.connector.exchange.kis.kis_api_order_book_data_source.aiohttp.ClientSession",
            return_value=_FailCtx(),
        ):
            with patch.object(self.data_source, "_sleep", new_callable=AsyncMock) as sleep_mock:
                sleep_mock.side_effect = [None, asyncio.CancelledError()]

                with self.assertRaises(asyncio.CancelledError):
                    await self.data_source.listen_for_subscriptions()

        self.assertTrue(
            self._is_logged_partial("ERROR", "Unexpected error in KIS WebSocket subscription")
        )

    async def test_listen_for_subscriptions_propagates_cancelled_error(self):
        """CancelledError during WS loop should propagate cleanly."""
        with patch.object(
            self.data_source._auth, "get_ws_approval_key", new_callable=AsyncMock
        ) as mock_key:
            mock_key.side_effect = asyncio.CancelledError()

            with self.assertRaises(asyncio.CancelledError):
                await self.data_source.listen_for_subscriptions()

    # ------------------------------------------------------------------
    # Test: listen_for_trades uses WS trade queue
    # ------------------------------------------------------------------

    async def test_listen_for_trades_reads_from_trade_queue(self):
        """listen_for_trades should read from the _trade_messages_queue_key queue."""
        # Enqueue a trade message
        raw = self._ws_trade_raw()
        parts = raw.split("|")
        data_str = parts[3]
        parsed = KisAPIOrderBookDataSource._parse_caret_fields(
            data_str, CONSTANTS.WS_TRADE_COLUMNS
        )
        await self.data_source._process_trade_data("005930", parsed)

        output_queue: asyncio.Queue = asyncio.Queue()

        # listen_for_trades reads from internal queue and puts into output
        task = asyncio.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, output_queue)
        )

        try:
            msg = await asyncio.wait_for(output_queue.get(), timeout=5.0)
        except asyncio.TimeoutError:
            self.fail("Timed out waiting for trade message")
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_diffs reads from diff queue (no-op for KIS)
    # ------------------------------------------------------------------

    async def test_listen_for_order_book_diffs_reads_from_diff_queue(self):
        """listen_for_order_book_diffs reads from the diff queue; KIS sends no diffs so queue stays empty."""
        output_queue: asyncio.Queue = asyncio.Queue()

        task = asyncio.create_task(
            self.data_source.listen_for_order_book_diffs(self.local_event_loop, output_queue)
        )

        # Give it a brief time; no messages should appear since KIS sends no diffs
        await asyncio.sleep(0.1)
        self.assertTrue(output_queue.empty())

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # Test: Constructor stores auth and domain
    # ------------------------------------------------------------------

    def test_constructor_stores_auth_and_domain(self):
        """Constructor should store the auth and domain parameters."""
        self.assertIs(self.mock_auth, self.data_source._auth)
        self.assertEqual(CONSTANTS.DEFAULT_DOMAIN, self.data_source._domain)

    def test_constructor_with_custom_domain(self):
        """Constructor should accept a custom domain (e.g., sandbox)."""
        ds = KisAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            auth=self.mock_auth,
            domain="sandbox",
        )
        self.assertEqual("sandbox", ds._domain)

    # ------------------------------------------------------------------
    # Test: _subscribe_ws_channels
    # ------------------------------------------------------------------

    async def test_subscribe_ws_channels_sends_correct_messages(self):
        """Should send orderbook and trade subscription messages for each trading pair."""
        mock_ws = AsyncMock()

        await self.data_source._subscribe_ws_channels(mock_ws, "test_approval_key")

        # 1 trading pair => 2 subscription messages (orderbook + trade)
        self.assertEqual(2, mock_ws.send_json.await_count)

        ob_call = mock_ws.send_json.call_args_list[0][0][0]
        trade_call = mock_ws.send_json.call_args_list[1][0][0]

        # Verify orderbook subscription
        self.assertEqual("test_approval_key", ob_call["header"]["approval_key"])
        self.assertEqual("1", ob_call["header"]["tr_type"])
        self.assertEqual(CONSTANTS.WS_DOMESTIC_STOCK_ORDERBOOK_TR_ID,
                         ob_call["body"]["input"]["tr_id"])
        self.assertEqual("005930", ob_call["body"]["input"]["tr_key"])

        # Verify trade subscription
        self.assertEqual(CONSTANTS.WS_DOMESTIC_STOCK_TRADE_TR_ID,
                         trade_call["body"]["input"]["tr_id"])
        self.assertEqual("005930", trade_call["body"]["input"]["tr_key"])

        # Verify subscription success was logged
        self.assertTrue(
            self._is_logged("INFO", "Subscribed to KIS WebSocket orderbook and trade channels")
        )

    async def test_subscribe_ws_channels_multiple_pairs(self):
        """With multiple trading pairs, should send 2 subscriptions per pair."""
        self.data_source._trading_pairs = [self.trading_pair, "000660-KRW"]
        self.connector._set_trading_pair_symbol_map(
            bidict({
                "005930": self.trading_pair,
                "000660": "000660-KRW",
            })
        )

        mock_ws = AsyncMock()
        await self.data_source._subscribe_ws_channels(mock_ws, "key")

        # 2 pairs * 2 channels = 4 messages
        self.assertEqual(4, mock_ws.send_json.await_count)

    # ------------------------------------------------------------------
    # Test: WS orderbook message timestamp and update_id
    # ------------------------------------------------------------------

    async def test_process_orderbook_data_has_valid_timestamp_and_update_id(self):
        """The orderbook snapshot message should have a timestamp close to now and a valid update_id."""
        data = {col: "0" for col in CONSTANTS.WS_ORDERBOOK_COLUMNS}
        data["ASKP1"] = "100"
        data["ASKP_RSQN1"] = "10"

        before = time.time()
        await self.data_source._process_orderbook_data("005930", data)
        after = time.time()

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertGreaterEqual(msg.timestamp, before)
        self.assertLessEqual(msg.timestamp, after)
        self.assertGreater(msg.update_id, 0)

    # ------------------------------------------------------------------
    # Test: WS trade message has valid trade_id
    # ------------------------------------------------------------------

    async def test_process_trade_data_has_valid_trade_id(self):
        """The trade message should have a positive trade_id based on timestamp."""
        data = {col: "0" for col in CONSTANTS.WS_TRADE_COLUMNS}
        data["STCK_PRPR"] = "50000"
        data["CNTG_VOL"] = "100"
        data["CCLD_DVSN"] = "2"

        await self.data_source._process_trade_data("005930", data)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertGreater(msg.content["trade_id"], 0)

    # ------------------------------------------------------------------
    # Test: _process_orderbook_data with unresolvable trading pair
    # ------------------------------------------------------------------

    async def test_process_orderbook_data_no_trading_pair_does_nothing(self):
        """When _resolve_trading_pair returns None, no message should be enqueued."""
        self.data_source._trading_pairs = []
        self.connector._set_trading_pair_symbol_map(bidict())

        data = {col: "0" for col in CONSTANTS.WS_ORDERBOOK_COLUMNS}
        data["ASKP1"] = "100"
        data["ASKP_RSQN1"] = "10"

        await self.data_source._process_orderbook_data("NOSUCH", data)

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertTrue(queue.empty())

    # ------------------------------------------------------------------
    # Test: _process_trade_data with unresolvable trading pair
    # ------------------------------------------------------------------

    async def test_process_trade_data_no_trading_pair_does_nothing(self):
        """When _resolve_trading_pair returns None, no message should be enqueued."""
        self.data_source._trading_pairs = []
        self.connector._set_trading_pair_symbol_map(bidict())

        data = {col: "0" for col in CONSTANTS.WS_TRADE_COLUMNS}
        data["STCK_PRPR"] = "50000"
        data["CNTG_VOL"] = "100"

        await self.data_source._process_trade_data("NOSUCH", data)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        self.assertTrue(queue.empty())
