import asyncio
from unittest.mock import AsyncMock, MagicMock

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange.bithumb import bithumb_constants as CONSTANTS
from hummingbot.connector.exchange.bithumb.bithumb_api_order_book_data_source import BithumbAPIOrderBookDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class BithumbAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(return_value="KRW-XRP")
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value="XRP-KRW")
        self.api_factory = MagicMock()
        self.data_source = BithumbAPIOrderBookDataSource(
            trading_pairs=["XRP-KRW"],
            connector=self.connector,
            api_factory=self.api_factory,
        )

    async def test_send_orderbook_subscription_uses_depth_suffix(self):
        ws = AsyncMock()
        await self.data_source._send_orderbook_subscription(ws, ["XRP-KRW"])

        ws.send.assert_called_once()
        sent_request = ws.send.call_args[0][0]
        payload = sent_request.payload
        self.assertEqual(CONSTANTS.ORDERBOOK_EVENT_TYPE, payload[1]["type"])
        self.assertEqual(["KRW-XRP.15"], payload[1]["codes"])
        self.assertEqual(1, payload[1]["level"])

    async def test_parse_order_book_snapshot_message(self):
        output = asyncio.Queue()
        raw_message = {
            "type": "orderbook",
            "code": "KRW-XRP.15",
            "timestamp": 1700000000000,
            "orderbook_units": [
                {"ask_price": "2000", "ask_size": "10", "bid_price": "1999", "bid_size": "11"},
            ],
        }

        await self.data_source._parse_order_book_snapshot_message(raw_message, output)
        message = output.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual("XRP-KRW", message.content["trading_pair"])
        self.assertEqual([(1999.0, 11.0)], message.content["bids"])
        self.assertEqual([(2000.0, 10.0)], message.content["asks"])

    def test_normalize_ws_message_from_string(self):
        normalized = self.data_source._normalize_ws_message('{"type":"orderbook","code":"KRW-XRP"}')
        self.assertEqual("orderbook", normalized["type"])

    async def test_subscribe_to_trading_pair_returns_false_when_ws_not_connected(self):
        success = await self.data_source.subscribe_to_trading_pair("BTC-KRW")
        self.assertFalse(success)
