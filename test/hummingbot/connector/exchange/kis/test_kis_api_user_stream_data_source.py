import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Optional
from unittest.mock import MagicMock

from hummingbot.connector.exchange.kis.kis_api_user_stream_data_source import KisAPIUserStreamDataSource
from hummingbot.connector.exchange.kis.kis_auth import KisAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class KisAPIUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    """Minimal tests for the KIS user stream data source.

    KIS does not offer a WebSocket API for user data (balances, orders).
    The data source exists only to satisfy the interface contract required
    by ExchangePyBase.  All user-data updates are handled via REST polling
    in the exchange class.
    """

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.trading_pair = "005930-KRW"  # Samsung Electronics

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.auth = KisAuth(
            app_key="test_app_key",
            app_secret="test_app_secret",
            sandbox=True,
            initial_token="test_token",
        )
        self.connector = MagicMock()
        self.api_factory = MagicMock()

        self.data_source = KisAPIUserStreamDataSource(
            auth=self.auth,
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.api_factory,
        )

    # ------------------------------------------------------------------ #
    # Test: instantiation
    # ------------------------------------------------------------------ #

    async def test_instance_creation(self):
        """The data source can be created and is the right type."""
        self.assertIsInstance(self.data_source, KisAPIUserStreamDataSource)
        self.assertIsInstance(self.data_source, UserStreamTrackerDataSource)

    # ------------------------------------------------------------------ #
    # Test: last_recv_time always returns a value
    # ------------------------------------------------------------------ #

    async def test_last_recv_time_returns_value(self):
        """last_recv_time should return a numeric value (0 when no WS is active)."""
        recv_time = self.data_source.last_recv_time
        self.assertIsInstance(recv_time, (int, float))
        self.assertEqual(recv_time, 0)

    # ------------------------------------------------------------------ #
    # Test: listen_for_user_stream is a no-op that can be cancelled
    # ------------------------------------------------------------------ #

    async def test_listen_for_user_stream_can_be_cancelled(self):
        """listen_for_user_stream should run without error and be cancellable."""
        output_queue = asyncio.Queue()
        task = self.local_event_loop.create_task(
            self.data_source.listen_for_user_stream(output=output_queue)
        )
        # Give it a moment to start the sleep loop
        await asyncio.sleep(0.1)

        # It should not have produced any messages
        self.assertTrue(output_queue.empty())

        # Cancel and verify clean shutdown
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------ #
    # Test: _connected_websocket_assistant returns None
    # ------------------------------------------------------------------ #

    async def test_connected_websocket_assistant_returns_none(self):
        """_connected_websocket_assistant should return None (no WS for KIS)."""
        result = await self.data_source._connected_websocket_assistant()
        self.assertIsNone(result)

    # ------------------------------------------------------------------ #
    # Test: _subscribe_channels is a no-op
    # ------------------------------------------------------------------ #

    async def test_subscribe_channels_is_noop(self):
        """_subscribe_channels should complete without error."""
        # Should not raise
        await self.data_source._subscribe_channels(websocket_assistant=None)

    # ------------------------------------------------------------------ #
    # Test: _process_websocket_messages is a no-op
    # ------------------------------------------------------------------ #

    async def test_process_websocket_messages_is_noop(self):
        """_process_websocket_messages should complete without error."""
        queue = asyncio.Queue()
        # Should not raise
        await self.data_source._process_websocket_messages(
            websocket_assistant=None, queue=queue
        )
        self.assertTrue(queue.empty())
