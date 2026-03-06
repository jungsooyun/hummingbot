import asyncio
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Optional
from unittest.mock import AsyncMock, patch

from bidict import bidict

import hummingbot.connector.exchange.bithumb.bithumb_constants as CONSTANTS
from hummingbot.connector.exchange.bithumb.bithumb_api_user_stream_data_source import BithumbAPIUserStreamDataSource
from hummingbot.connector.exchange.bithumb.bithumb_auth import BithumbAuth
from hummingbot.connector.exchange.bithumb.bithumb_exchange import BithumbExchange
from hummingbot.connector.test_support.network_mocking_assistant import NetworkMockingAssistant


class BithumbAPIUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC"
        cls.quote_asset = "KRW"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.quote_asset}-{cls.base_asset}"

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task: Optional[asyncio.Task] = None
        self.mocking_assistant = NetworkMockingAssistant()

        self.auth = BithumbAuth(access_key="test_access_key", secret_key="test_secret_key")

        self.connector = BithumbExchange(
            bithumb_access_key="test_access_key",
            bithumb_secret_key="test_secret_key",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )
        self.connector._web_assistants_factory._auth = self.auth

        self.data_source = BithumbAPIUserStreamDataSource(
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
    async def test_subscribes_to_private_channels(self, ws_connect_mock):
        ws_connect_mock.return_value = self.mocking_assistant.create_websocket_mock()

        # Subscription confirmation (not queued since it's not myOrder/myAsset type)
        confirm = {"status": "UP"}
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=ws_connect_mock.return_value,
            message=json.dumps(confirm))

        output_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue))

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(ws_connect_mock.return_value)

        sent_messages = self.mocking_assistant.json_messages_sent_through_websocket(
            websocket_mock=ws_connect_mock.return_value)

        self.assertEqual(1, len(sent_messages))
        sub_msg = sent_messages[0]

        # Verify ticket field
        self.assertIn("ticket", sub_msg[0])

        # Verify myOrder subscription
        self.assertEqual(CONSTANTS.PRIVATE_ORDER_CHANNEL_NAME, sub_msg[1]["type"])
        self.assertIn(self.ex_trading_pair, sub_msg[1]["codes"])

        # Verify myAsset subscription
        self.assertEqual(CONSTANTS.PRIVATE_ASSET_CHANNEL_NAME, sub_msg[2]["type"])

        # Verify format
        self.assertEqual("DEFAULT", sub_msg[3]["format"])

        self.assertTrue(self._is_logged(
            "INFO", "Subscribed to private order and asset channels..."))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_does_not_queue_invalid_payload(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        # Non-order event
        event_non_order = {"type": "trade", "code": "KRW-BTC"}
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(event_non_order))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue))

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.user_stream_tracker_data_source.UserStreamTrackerDataSource._sleep")
    async def test_connection_failed(self, sleep_mock, mock_ws):
        mock_ws.side_effect = Exception("TEST ERROR")
        sleep_mock.side_effect = asyncio.CancelledError

        msg_queue = asyncio.Queue()
        try:
            await self.data_source.listen_for_user_stream(msg_queue)
        except asyncio.CancelledError:
            pass

        self.assertTrue(self._is_logged(
            "ERROR", "Unexpected error while listening to user stream. Retrying after 5 seconds..."))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_cancel_exception_during_init(self, ws_connect_mock):
        messages = asyncio.Queue()
        ws_connect_mock.side_effect = asyncio.CancelledError

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_user_stream(messages)

    async def test_subscribe_raises_cancel_exception(self):
        ws_assistant = AsyncMock()
        ws_assistant.send.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(ws_assistant)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.user_stream_tracker_data_source.UserStreamTrackerDataSource._sleep")
    async def test_logs_exception_during_subscription(self, sleep_mock, mock_ws):
        messages = asyncio.Queue()
        sleep_mock.side_effect = asyncio.CancelledError
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        mock_ws.return_value.send_json.side_effect = Exception("Subscription error")

        try:
            await self.data_source.listen_for_user_stream(messages)
        except asyncio.CancelledError:
            pass

        self.assertTrue(self._is_logged(
            "ERROR", "Unexpected error occurred subscribing to private user stream channels..."))
        self.assertTrue(self._is_logged(
            "ERROR", "Unexpected error while listening to user stream. Retrying after 5 seconds..."))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_processes_order_event(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        order_event = {
            "type": "myOrder",
            "code": "KRW-BTC",
            "uuid": "order-123",
            "state": "done",
            "executed_volume": 0.1,
            "remaining_volume": 0,
        }
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(order_event))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue))

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(1, msg_queue.qsize())
        queued_msg = msg_queue.get_nowait()
        self.assertEqual(order_event, queued_msg)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_processes_asset_event(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        asset_event = {
            "type": "myAsset",
            "currency": "BTC",
            "balance": "1.5",
            "locked": "0.3",
        }
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(asset_event))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue))

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(1, msg_queue.qsize())
        queued_msg = msg_queue.get_nowait()
        self.assertEqual(asset_event, queued_msg)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_processes_multiple_events(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        order_event_1 = {
            "type": "myOrder",
            "code": "KRW-BTC",
            "uuid": "order-1",
            "state": "wait",
        }
        order_event_2 = {
            "type": "myOrder",
            "code": "KRW-BTC",
            "uuid": "order-2",
            "state": "done",
        }
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(order_event_1))
        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message=json.dumps(order_event_2))

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue))

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(2, msg_queue.qsize())
        first_msg = msg_queue.get_nowait()
        second_msg = msg_queue.get_nowait()
        self.assertEqual(order_event_1, first_msg)
        self.assertEqual(order_event_2, second_msg)

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    @patch("hummingbot.core.data_type.user_stream_tracker_data_source.UserStreamTrackerDataSource._sleep")
    async def test_raises_on_error_message(self, sleep_mock, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()
        sleep_mock.side_effect = asyncio.CancelledError

        error_event = {"error": "unauthorized", "message": "Invalid credentials"}
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
            "ERROR", "Unexpected error while listening to user stream. Retrying after 5 seconds..."))

    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_handles_invalid_json(self, mock_ws):
        mock_ws.return_value = self.mocking_assistant.create_websocket_mock()

        self.mocking_assistant.add_websocket_aiohttp_message(
            websocket_mock=mock_ws.return_value,
            message="invalid json content {{{")

        msg_queue = asyncio.Queue()
        self.listening_task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(msg_queue))

        await self.mocking_assistant.run_until_all_aiohttp_messages_delivered(mock_ws.return_value)

        self.assertEqual(0, msg_queue.qsize())
        self.assertTrue(self._is_logged(
            "WARNING",
            "Invalid event message received through the user stream connection (invalid json content {{{)"))
