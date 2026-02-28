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
from hummingbot.connector.exchange.kis.kis_exchange import KisExchange
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class KisAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    """
    Tests for the KIS REST-only order book data source.

    KIS has no WebSocket for public market data, so all order book data
    is fetched via REST polling. Tests verify:
    1. REST order book snapshot fetch (domestic stock orderbook)
    2. Last traded price fetch (domestic stock ticker)
    3. No WebSocket functionality (KIS has no WS for market data)
    4. REST polling for order book snapshots
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

        self.data_source = KisAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
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

    # ------------------------------------------------------------------
    # Mock response helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _order_book_snapshot_response() -> Dict[str, Any]:
        """KIS domestic stock orderbook response (TR_ID: FHKST01010200)."""
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
    # Test: get_last_traded_prices
    # ------------------------------------------------------------------

    @aioresponses()
    def test_get_last_traded_prices(self, mock_api):
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
    # Test: _order_book_snapshot (REST snapshot fetch)
    # ------------------------------------------------------------------

    @aioresponses()
    def test_order_book_snapshot(self, mock_api):
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
    # Test: No WebSocket — _connected_websocket_assistant raises
    # ------------------------------------------------------------------

    async def test_connected_websocket_assistant_raises_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            await self.data_source._connected_websocket_assistant()

    # ------------------------------------------------------------------
    # Test: No WebSocket — _subscribe_channels raises
    # ------------------------------------------------------------------

    async def test_subscribe_channels_raises_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            await self.data_source._subscribe_channels(MagicMock())

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_diffs is no-op
    # ------------------------------------------------------------------

    async def test_listen_for_order_book_diffs_is_noop(self):
        """KIS has no diffs — method should return immediately (no-op)."""
        msg_queue: asyncio.Queue = asyncio.Queue()
        # Should return without blocking or producing messages
        await self.data_source.listen_for_order_book_diffs(
            self.local_event_loop, msg_queue
        )
        self.assertTrue(msg_queue.empty())

    # ------------------------------------------------------------------
    # Test: listen_for_subscriptions blocks indefinitely (no WS)
    # ------------------------------------------------------------------

    async def test_listen_for_subscriptions_blocks_indefinitely(self):
        """listen_for_subscriptions should block forever (no WS to connect to)."""
        task = self.local_event_loop.create_task(
            self.data_source.listen_for_subscriptions()
        )

        # Give it a brief time to start, then check it hasn't exited
        await asyncio.sleep(0.1)
        self.assertFalse(task.done())

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_snapshots polls REST
    # ------------------------------------------------------------------

    @aioresponses()
    async def test_listen_for_order_book_snapshots_polls_rest(self, mock_api):
        """listen_for_order_book_snapshots should poll REST and produce snapshots."""
        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        # Provide enough responses for at least one poll
        mock_api.get(regex_url, body=json.dumps(mock_response))
        mock_api.get(regex_url, body=json.dumps(mock_response))

        msg_queue: asyncio.Queue = asyncio.Queue()

        with patch.object(self.data_source, "_sleep", new_callable=AsyncMock) as sleep_mock:
            sleep_mock.side_effect = [None, asyncio.CancelledError()]

            self.listening_task = self.local_event_loop.create_task(
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
                    self._is_logged(
                        "ERROR",
                        "Unexpected error when processing public order book snapshots from exchange",
                    )
                )

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_snapshots raises CancelledError
    # ------------------------------------------------------------------

    async def test_listen_for_order_book_snapshots_raises_cancelled_error(self):
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
    # Test: subscribe / unsubscribe are no-ops (no WS)
    # ------------------------------------------------------------------

    async def test_subscribe_to_trading_pair_is_noop(self):
        result = await self.data_source.subscribe_to_trading_pair("ETH-KRW")
        self.assertTrue(result)

    async def test_unsubscribe_from_trading_pair_is_noop(self):
        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)
        self.assertTrue(result)

    # ------------------------------------------------------------------
    # Test: listen_for_trades is no-op (no WS trade stream)
    # ------------------------------------------------------------------

    async def test_listen_for_trades_is_noop(self):
        """KIS has no trade stream — method should return immediately."""
        msg_queue: asyncio.Queue = asyncio.Queue()
        await self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
        self.assertTrue(msg_queue.empty())

    # ------------------------------------------------------------------
    # Test: _extract_levels correctly parses KIS dict-based orderbook
    # ------------------------------------------------------------------

    def test_extract_levels_from_dict(self):
        """Test the template-based level extraction from KIS dict format."""
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
        """Empty string values and missing keys should be treated as None."""
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
        """Data source should have a reasonable default polling interval."""
        self.assertGreater(self.data_source.ORDER_BOOK_SNAPSHOT_DELAY, 0)
