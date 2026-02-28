import asyncio
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

from bidict import bidict

import hummingbot.connector.exchange.lighter.lighter_constants as CONSTANTS
from hummingbot.connector.exchange.lighter.lighter_api_user_stream_data_source import LighterAPIUserStreamDataSource
from hummingbot.connector.exchange.lighter.lighter_auth import LighterAuth
from hummingbot.connector.exchange.lighter.lighter_exchange import LighterExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant


class LighterAPIUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    # the level is required to receive logs from the data source logger
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}_{cls.quote_asset}"

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant()

        self.auth = LighterAuth(
            api_key="test_api_key",
            account_index=42,
        )

        self.connector = LighterExchange(
            lighter_api_key="test_api_key",
            lighter_account_index="42",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )
        self.connector._web_assistants_factory._auth = self.auth

        self.data_source = LighterAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair}))

    def tearDown(self) -> None:
        self.listening_task and self.listening_task.cancel()
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(record.levelname == log_level and record.getMessage() == message
                   for record in self.log_records)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_subscribes_to_account_channels(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        # First message: subscription confirmation for account_orders
        result_subscribe_orders = {
            "type": "subscribed",
            "channel": CONSTANTS.PRIVATE_ORDER_CHANNEL_NAME,
        }
        # Second message: subscription confirmation for account_trades
        result_subscribe_trades = {
            "type": "subscribed",
            "channel": CONSTANTS.PRIVATE_TRADE_CHANNEL_NAME,
        }

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_orders))
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(result_subscribe_trades))

        output_queue = asyncio.Queue()

        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue))

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(2, len(sent_messages))
        expected_orders_subscription = {
            "type": "subscribe",
            "channel": CONSTANTS.PRIVATE_ORDER_CHANNEL_NAME,
            "account_index": 42,
        }
        expected_trades_subscription = {
            "type": "subscribe",
            "channel": CONSTANTS.PRIVATE_TRADE_CHANNEL_NAME,
            "account_index": 42,
        }
        self.assertEqual(expected_orders_subscription, sent_messages[0])
        self.assertEqual(expected_trades_subscription, sent_messages[1])

        self.assertTrue(self._is_logged(
            "INFO",
            "Subscribed to private account orders and trades channels..."
        ))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_does_not_queue_invalid_payload(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # Message without "type" field should be ignored
        event_without_type = {
            "channel": CONSTANTS.PRIVATE_ORDER_CHANNEL_NAME,
            "data": {"order_index": 123},
        }
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(event_without_type))

        # Message with a non-update type should be ignored
        event_with_subscribed_type = {
            "type": "subscribed",
            "channel": CONSTANTS.PRIVATE_ORDER_CHANNEL_NAME,
        }
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(event_with_subscribed_type))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.user_stream_tracker_data_source.UserStreamTrackerDataSource._sleep")
    async def test_listen_for_user_stream_connection_failed(self, sleep_mock, mock_ws):
        mock_ws.side_effect = Exception("TEST ERROR")
        sleep_mock.side_effect = asyncio.CancelledError

        msg_queue = asyncio.Queue()

        try:
            await self.data_source.listen_for_user_stream(msg_queue)
        except asyncio.CancelledError:
            pass

        self.assertTrue(
            self._is_logged("ERROR",
                            "Unexpected error while listening to user stream. Retrying after 5 seconds..."))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listening_process_canceled_when_cancel_exception_during_initialization(self, ws_connect_mock):
        messages = asyncio.Queue()
        ws_connect_mock.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_user_stream(messages)

    async def test_subscribe_channels_raises_cancel_exception(self):
        ws_assistant = AsyncMock()
        ws_assistant.send.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(ws_assistant)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.user_stream_tracker_data_source.UserStreamTrackerDataSource._sleep")
    async def test_listening_process_logs_exception_during_events_subscription(self, sleep_mock, mock_ws):
        messages = asyncio.Queue()
        sleep_mock.side_effect = asyncio.CancelledError
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # Force an exception in _subscribe_channels by making send raise
        mock_ws.return_value.send_json.side_effect = Exception("Subscription error")

        try:
            await self.data_source.listen_for_user_stream(messages)
        except asyncio.CancelledError:
            pass

        self.assertTrue(self._is_logged(
            "ERROR",
            "Unexpected error occurred subscribing to private user stream channels..."))
        self.assertTrue(self._is_logged(
            "ERROR",
            "Unexpected error while listening to user stream. Retrying after 5 seconds..."))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_processes_order_update_event(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        order_event = {
            "type": "update/account_orders",
            "orders": [
                {
                    "order_index": 123,
                    "status": "filled",
                    "market_id": 0,
                }
            ],
        }
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(order_event))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(1, msg_queue.qsize())
        order_event_message = msg_queue.get_nowait()
        self.assertEqual(order_event, order_event_message)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_processes_trade_update_event(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        trade_event = {
            "type": "update/account_trades",
            "trades": [
                {
                    "trade_id": 456,
                    "order_index": 123,
                    "price": "10500",
                    "amount": "0.5",
                }
            ],
        }
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(trade_event))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(1, msg_queue.qsize())
        trade_event_message = msg_queue.get_nowait()
        self.assertEqual(trade_event, trade_event_message)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_processes_multiple_events(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        order_event = {
            "type": "update/account_orders",
            "orders": [{"order_index": 123, "status": "open", "market_id": 0}],
        }
        trade_event = {
            "type": "update/account_trades",
            "trades": [{"trade_id": 456, "order_index": 123, "price": "10500", "amount": "0.5"}],
        }
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(order_event))
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(trade_event))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(2, msg_queue.qsize())
        first_msg = msg_queue.get_nowait()
        second_msg = msg_queue.get_nowait()
        self.assertEqual(order_event, first_msg)
        self.assertEqual(trade_event, second_msg)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.user_stream_tracker_data_source.UserStreamTrackerDataSource._sleep")
    async def test_listen_for_user_stream_raises_on_error_message(self, sleep_mock, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        sleep_mock.side_effect = asyncio.CancelledError

        error_event = {
            "error": "unauthorized",
            "message": "Invalid account_index",
        }
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(error_event))

        msg_queue = asyncio.Queue()

        try:
            await self.data_source.listen_for_user_stream(msg_queue)
        except asyncio.CancelledError:
            pass

        self.assertEqual(0, msg_queue.qsize())
        self.assertTrue(self._is_logged(
            "ERROR",
            "Unexpected error while listening to user stream. Retrying after 5 seconds..."
        ))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_listen_for_user_stream_handles_invalid_json(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message="invalid json content {{{")

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue)
        )

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())
        self.assertTrue(self._is_logged(
            "WARNING",
            "Invalid event message received through the user stream connection (invalid json content {{{)"
        ))
