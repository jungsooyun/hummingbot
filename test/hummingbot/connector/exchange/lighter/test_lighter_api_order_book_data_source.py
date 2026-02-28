import asyncio
import json
import re
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses
from bidict import bidict

import hummingbot.connector.exchange.lighter.lighter_constants as CONSTANTS
from hummingbot.connector.exchange.lighter.lighter_api_order_book_data_source import LighterAPIOrderBookDataSource
from hummingbot.connector.exchange.lighter.lighter_exchange import LighterExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class LighterAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    # logging.Level required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}/{cls.quote_asset}"
        cls.market_id = 0

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task = None
        self.mocking_assistant = NetworkMockingAssistant()

        self.connector = LighterExchange(
            lighter_api_key="test_api_key",
            lighter_account_index="1",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )

        self.data_source = LighterAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
        )
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair}))
        # Set market_id mapping on the connector
        self.connector._market_id_map = {self.ex_trading_pair: self.market_id}

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(
            record.levelname == log_level and record.getMessage() == message
            for record in self.log_records
        )

    def _order_book_snapshot_rest_example(self):
        return {
            "code": 200,
            "total_asks": 1,
            "asks": [{"price": "10500.0000", "remaining_base_amount": "0.50", "order_id": "281476452537901"}],
            "total_bids": 1,
            "bids": [{"price": "10400.0000", "remaining_base_amount": "0.30", "order_id": "562948495433943"}],
        }

    def _trade_event_ws_example(self):
        return {
            "type": "update/trade",
            "channel": f"trade:{self.market_id}",
            "trades": [
                {
                    "price": "10450.5",
                    "amount": "0.1",
                    "side": "buy",
                    "timestamp": 1709136000000,
                }
            ],
        }

    def _order_book_snapshot_ws_example(self):
        return {
            "type": "subscribed/order_book",
            "channel": f"order_book:{self.market_id}",
            "order_book": {
                "asks": [{"price": "10500.0", "remaining_base_amount": "0.5"}],
                "bids": [{"price": "10400.0", "remaining_base_amount": "0.3"}],
            },
        }

    def _order_book_update_ws_example(self):
        return {
            "type": "update/order_book",
            "channel": f"order_book:{self.market_id}",
            "order_book": {
                "asks": [{"price": "10500.0", "remaining_base_amount": "0.8"}],
                "bids": [],
            },
        }

    # ---------- REST: order book snapshot ----------

    @aioresponses()
    def test_request_order_book_snapshot(self, mock_get):
        mock_response = self._order_book_snapshot_rest_example()
        regex_url = re.compile(f"{CONSTANTS.REST_URL}/{CONSTANTS.GET_ORDER_BOOK_PATH_URL}")
        mock_get.get(regex_url, body=json.dumps(mock_response))

        result = self.local_event_loop.run_until_complete(
            self.data_source._request_order_book_snapshot(self.trading_pair)
        )

        self.assertEqual(result, mock_response)

    @aioresponses()
    def test_get_new_order_book_successful(self, mock_get):
        mock_response = self._order_book_snapshot_rest_example()
        regex_url = re.compile(f"{CONSTANTS.REST_URL}/{CONSTANTS.GET_ORDER_BOOK_PATH_URL}")
        mock_get.get(regex_url, body=json.dumps(mock_response))

        result = self.local_event_loop.run_until_complete(
            self.data_source.get_new_order_book(self.trading_pair)
        )

        self.assertIsInstance(result, OrderBook)
        bids = list(result.bid_entries())
        asks = list(result.ask_entries())
        self.assertEqual(1, len(bids))
        self.assertEqual(1, len(asks))
        self.assertEqual(10400.0, bids[0].price)
        self.assertEqual(0.30, bids[0].amount)
        self.assertEqual(10500.0, asks[0].price)
        self.assertEqual(0.50, asks[0].amount)

    # ---------- WS: subscription ----------

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_subscribes_to_trades_and_order_book(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        # Add subscription confirmation messages so ws loop does something
        result_subscribe_ob = {
            "type": "subscribed/order_book",
            "channel": f"order_book:{self.market_id}",
            "order_book": {"asks": [], "bids": []},
        }
        result_subscribe_trade = {
            "type": "subscribed/trade",
            "channel": f"trade:{self.market_id}",
        }

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_ob),
        )
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_trade),
        )

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_subscriptions()
        )

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(
            ws_connect_mock.return_value
        )

        sent_subscription_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value
        )

        self.assertEqual(2, len(sent_subscription_messages))
        expected_trade_sub = {"type": "subscribe", "channel": f"trade/{self.market_id}"}
        expected_ob_sub = {"type": "subscribe", "channel": f"order_book/{self.market_id}"}
        self.assertEqual(expected_trade_sub, sent_subscription_messages[0])
        self.assertEqual(expected_ob_sub, sent_subscription_messages[1])

        self.assertTrue(
            self._is_logged(
                "INFO",
                "Subscribed to public order book and trade channels...",
            )
        )

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_listen_for_subscriptions_raises_cancel_exception(self, mock_ws, _):
        mock_ws.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_subscriptions()

    @patch("hummingbot.core.data_type.order_book_tracker_data_source.OrderBookTrackerDataSource._sleep")
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_subscriptions_logs_exception_details(self, mock_ws, sleep_mock):
        mock_ws.side_effect = Exception("TEST ERROR.")
        sleep_mock.side_effect = asyncio.CancelledError

        try:
            await self.data_source.listen_for_subscriptions()
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
            )
        )

    async def test_subscribe_channels_raises_cancel_exception(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(mock_ws)

    async def test_subscribe_channels_raises_exception_and_logs_error(self):
        mock_ws = MagicMock()
        mock_ws.send.side_effect = Exception("Test Error")

        with self.assertRaises(Exception):
            await self.data_source._subscribe_channels(mock_ws)

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error occurred subscribing to order book trading and delta streams...",
            )
        )

    # ---------- Channel routing ----------

    def test_channel_originating_message_trade(self):
        msg = self._trade_event_ws_example()
        channel = self.data_source._channel_originating_message(msg)
        self.assertEqual(self.data_source._trade_messages_queue_key, channel)

    def test_channel_originating_message_order_book_snapshot(self):
        msg = self._order_book_snapshot_ws_example()
        channel = self.data_source._channel_originating_message(msg)
        self.assertEqual(self.data_source._snapshot_messages_queue_key, channel)

    def test_channel_originating_message_order_book_diff(self):
        msg = self._order_book_update_ws_example()
        channel = self.data_source._channel_originating_message(msg)
        self.assertEqual(self.data_source._diff_messages_queue_key, channel)

    def test_channel_originating_message_unknown(self):
        msg = {"type": "connected"}
        channel = self.data_source._channel_originating_message(msg)
        self.assertEqual("", channel)

    # ---------- Trade message parsing ----------

    async def test_parse_trade_message(self):
        msg_queue: asyncio.Queue = asyncio.Queue()
        trade_event = self._trade_event_ws_example()

        await self.data_source._parse_trade_message(
            raw_message=trade_event, message_queue=msg_queue
        )

        self.assertFalse(msg_queue.empty())
        trade_msg: OrderBookMessage = msg_queue.get_nowait()

        self.assertEqual(OrderBookMessageType.TRADE, trade_msg.type)
        self.assertEqual(1709136000000, trade_msg.trade_id)
        self.assertEqual(self.trading_pair, trade_msg.trading_pair)
        self.assertEqual(1709136000.0, trade_msg.timestamp)

    async def test_listen_for_trades(self):
        msg_queue: asyncio.Queue = asyncio.Queue()
        mock_queue = AsyncMock()

        trade_event = self._trade_event_ws_example()
        mock_queue.get.side_effect = [trade_event, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
        )

        trade_msg: OrderBookMessage = await msg_queue.get()

        self.assertTrue(msg_queue.empty())
        self.assertEqual(1709136000000, int(trade_msg.trade_id))
        self.assertEqual(self.trading_pair, trade_msg.trading_pair)

    async def test_listen_for_trades_raises_cancelled_exception(self):
        mock_queue = MagicMock()
        mock_queue.get.side_effect = asyncio.CancelledError
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_trades(self.local_event_loop, msg_queue)

    async def test_listen_for_trades_logs_exception(self):
        incomplete_resp = {
            "type": "update/trade",
            "channel": f"trade:{self.market_id}",
            # Missing "trades" key
        }

        mock_queue = AsyncMock()
        mock_queue.get.side_effect = [incomplete_resp, asyncio.CancelledError()]
        self.data_source._message_queue[self.data_source._trade_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        try:
            await self.data_source.listen_for_trades(self.local_event_loop, msg_queue)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged(
                "ERROR",
                "Unexpected error when processing public trade updates from exchange",
            )
        )

    # ---------- Order book snapshot message parsing ----------

    async def test_parse_order_book_snapshot_message(self):
        msg_queue: asyncio.Queue = asyncio.Queue()
        snapshot_event = self._order_book_snapshot_ws_example()

        await self.data_source._parse_order_book_snapshot_message(
            raw_message=snapshot_event, message_queue=msg_queue
        )

        self.assertFalse(msg_queue.empty())
        snapshot_msg: OrderBookMessage = msg_queue.get_nowait()

        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot_msg.type)
        self.assertEqual(self.trading_pair, snapshot_msg.trading_pair)
        self.assertEqual(-1, snapshot_msg.trade_id)

        bids = snapshot_msg.bids
        asks = snapshot_msg.asks
        self.assertEqual(1, len(bids))
        self.assertEqual(10400.0, bids[0].price)
        self.assertEqual(0.3, bids[0].amount)
        self.assertEqual(1, len(asks))
        self.assertEqual(10500.0, asks[0].price)
        self.assertEqual(0.5, asks[0].amount)

    async def test_listen_for_order_book_snapshots(self):
        mock_queue = AsyncMock()
        snapshot_event = self._order_book_snapshot_ws_example()
        mock_queue.get.side_effect = [snapshot_event, asyncio.CancelledError]
        self.data_source._message_queue[self.data_source._snapshot_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_snapshots(self.local_event_loop, msg_queue)
        )

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

    # ---------- Order book diff message parsing ----------

    async def test_parse_order_book_diff_message(self):
        msg_queue: asyncio.Queue = asyncio.Queue()
        diff_event = self._order_book_update_ws_example()

        await self.data_source._parse_order_book_diff_message(
            raw_message=diff_event, message_queue=msg_queue
        )

        self.assertFalse(msg_queue.empty())
        diff_msg: OrderBookMessage = msg_queue.get_nowait()

        self.assertEqual(OrderBookMessageType.DIFF, diff_msg.type)
        self.assertEqual(self.trading_pair, diff_msg.trading_pair)

        asks = diff_msg.asks
        bids = diff_msg.bids
        self.assertEqual(1, len(asks))
        self.assertEqual(10500.0, asks[0].price)
        self.assertEqual(0.8, asks[0].amount)
        self.assertEqual(0, len(bids))

    async def test_listen_for_order_book_diffs(self):
        mock_queue = AsyncMock()
        diff_event = self._order_book_update_ws_example()
        mock_queue.get.side_effect = [diff_event, asyncio.CancelledError]
        self.data_source._message_queue[self.data_source._diff_messages_queue_key] = mock_queue

        msg_queue: asyncio.Queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_order_book_diffs(self.local_event_loop, msg_queue)
        )

        msg: OrderBookMessage = await msg_queue.get()

        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

    # ---------- Last traded price delegation ----------

    async def test_get_last_traded_prices(self):
        mock_prices = {self.trading_pair: 10450.5}
        self.connector.get_last_traded_prices = AsyncMock(return_value=mock_prices)

        result = await self.data_source.get_last_traded_prices([self.trading_pair])

        self.assertEqual(mock_prices, result)
        self.connector.get_last_traded_prices.assert_awaited_once_with(
            trading_pairs=[self.trading_pair]
        )

    # ---------- Dynamic subscription tests ----------

    async def test_subscribe_to_trading_pair_successful(self):
        new_pair = "ETH-USDT"
        ex_new_pair = "ETH_USDT"
        new_market_id = 1
        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair, ex_new_pair: new_pair})
        )
        self.connector._market_id_map[ex_new_pair] = new_market_id

        mock_ws = AsyncMock()
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.subscribe_to_trading_pair(new_pair)

        self.assertTrue(result)
        self.assertIn(new_pair, self.data_source._trading_pairs)
        self.assertEqual(2, mock_ws.send.call_count)
        self.assertTrue(
            self._is_logged("INFO", f"Subscribed to public order book and trade channels of {new_pair}...")
        )

    async def test_subscribe_to_trading_pair_websocket_not_connected(self):
        new_pair = "ETH-USDT"
        self.data_source._ws_assistant = None

        result = await self.data_source.subscribe_to_trading_pair(new_pair)

        self.assertFalse(result)
        self.assertTrue(
            self._is_logged("WARNING", "Cannot subscribe: WebSocket connection not established")
        )

    async def test_subscribe_to_trading_pair_raises_cancel_exception(self):
        new_pair = "ETH-USDT"
        ex_new_pair = "ETH_USDT"
        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair, ex_new_pair: new_pair})
        )
        self.connector._market_id_map[ex_new_pair] = 1

        mock_ws = AsyncMock()
        mock_ws.send.side_effect = asyncio.CancelledError
        self.data_source._ws_assistant = mock_ws

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.subscribe_to_trading_pair(new_pair)

    async def test_subscribe_to_trading_pair_raises_exception_and_logs_error(self):
        new_pair = "ETH-USDT"
        ex_new_pair = "ETH_USDT"
        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair, ex_new_pair: new_pair})
        )
        self.connector._market_id_map[ex_new_pair] = 1

        mock_ws = AsyncMock()
        mock_ws.send.side_effect = Exception("Test Error")
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.subscribe_to_trading_pair(new_pair)

        self.assertFalse(result)
        self.assertTrue(
            self._is_logged("ERROR", f"Unexpected error occurred subscribing to {new_pair}...")
        )

    async def test_unsubscribe_from_trading_pair_successful(self):
        mock_ws = AsyncMock()
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

        self.assertTrue(result)
        self.assertNotIn(self.trading_pair, self.data_source._trading_pairs)
        self.assertEqual(2, mock_ws.send.call_count)
        self.assertTrue(
            self._is_logged(
                "INFO",
                f"Unsubscribed from public order book and trade channels of {self.trading_pair}...",
            )
        )

    async def test_unsubscribe_from_trading_pair_websocket_not_connected(self):
        self.data_source._ws_assistant = None

        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

        self.assertFalse(result)
        self.assertTrue(
            self._is_logged("WARNING", "Cannot unsubscribe: WebSocket connection not established")
        )

    async def test_unsubscribe_from_trading_pair_raises_cancel_exception(self):
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = asyncio.CancelledError
        self.data_source._ws_assistant = mock_ws

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

    async def test_unsubscribe_from_trading_pair_raises_exception_and_logs_error(self):
        mock_ws = AsyncMock()
        mock_ws.send.side_effect = Exception("Test Error")
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.unsubscribe_from_trading_pair(self.trading_pair)

        self.assertFalse(result)
        self.assertTrue(
            self._is_logged(
                "ERROR",
                f"Unexpected error occurred unsubscribing from {self.trading_pair}...",
            )
        )
