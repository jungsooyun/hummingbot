import asyncio
from unittest.mock import AsyncMock, MagicMock

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange.upbit import upbit_constants as CONSTANTS
from hummingbot.connector.exchange.upbit.upbit_api_order_book_data_source import UpbitAPIOrderBookDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class UpbitAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.connector = MagicMock()
        self.connector.exchange_symbol_associated_to_pair = AsyncMock(return_value="KRW-BTC")
        self.connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value="BTC-KRW")
        self.api_factory = MagicMock()
        self.data_source = UpbitAPIOrderBookDataSource(
            trading_pairs=["BTC-KRW"],
            connector=self.connector,
            api_factory=self.api_factory,
        )

    async def test_send_subscription_message_uses_depth_suffix(self):
        ws = AsyncMock()
        await self.data_source._send_subscription_message(ws, ["BTC-KRW"])

        ws.send.assert_called_once()
        sent_request = ws.send.call_args[0][0]
        payload = sent_request.payload
        self.assertEqual(CONSTANTS.ORDERBOOK_EVENT_TYPE, payload[1]["type"])
        self.assertEqual(["KRW-BTC.15"], payload[1]["codes"])
        self.assertEqual(CONSTANTS.TRADE_EVENT_TYPE, payload[2]["type"])

    async def test_parse_order_book_snapshot_message(self):
        output = asyncio.Queue()
        raw_message = {
            "type": "orderbook",
            "code": "KRW-BTC.15",
            "timestamp": 1700000000000,
            "orderbook_units": [
                {"ask_price": "100", "ask_size": "2", "bid_price": "99", "bid_size": "3"},
            ],
        }

        await self.data_source._parse_order_book_snapshot_message(raw_message, output)
        message = output.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual("BTC-KRW", message.content["trading_pair"])
        self.assertEqual([(99.0, 3.0)], message.content["bids"])
        self.assertEqual([(100.0, 2.0)], message.content["asks"])

    async def test_parse_trade_message(self):
        output = asyncio.Queue()
        raw_message = {
            "type": "trade",
            "code": "KRW-BTC",
            "ask_bid": "ASK",
            "timestamp": 1700000000000,
            "trade_price": "101",
            "trade_volume": "0.1",
            "sequential_id": 123456,
        }

        await self.data_source._parse_trade_message(raw_message, output)
        message = output.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, message.type)
        self.assertEqual("BTC-KRW", message.content["trading_pair"])
        self.assertEqual(101.0, message.content["price"])
        self.assertEqual(0.1, message.content["amount"])

    def test_normalize_ws_message_from_bytes(self):
        message = b'{"type":"orderbook","code":"KRW-BTC"}'
        normalized = self.data_source._normalize_ws_message(message)
        self.assertEqual("orderbook", normalized["type"])
