from decimal import Decimal
from unittest.mock import AsyncMock, patch

from bidict import bidict
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange.upbit import upbit_constants as CONSTANTS
from hummingbot.connector.exchange.upbit.upbit_exchange import UpbitExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


class UpbitExchangeTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.exchange = UpbitExchange(
            upbit_access_key="test-access",
            upbit_secret_key="test-secret",
            trading_pairs=["BTC-KRW"],
        )
        self.exchange._set_trading_pair_symbol_map(bidict({"KRW-BTC": "BTC-KRW"}))

    async def test_get_last_traded_price(self):
        with patch.object(self.exchange, "_api_get", AsyncMock(return_value=[{"market": "KRW-BTC", "trade_price": 123456.0}])) as mock_api:
            price = await self.exchange._get_last_traded_price("BTC-KRW")

        self.assertEqual(price, 123456.0)
        mock_api.assert_called_once_with(
            path_url=CONSTANTS.GET_LAST_TRADING_PRICES_PATH_URL,
            params={"markets": "KRW-BTC"},
        )

    async def test_update_balances(self):
        with patch.object(
            self.exchange,
            "_api_get",
            AsyncMock(
                return_value=[
                    {"currency": "BTC", "balance": "1.2", "locked": "0.3"},
                    {"currency": "KRW", "balance": "1000000", "locked": "0"},
                ]
            ),
        ):
            await self.exchange._update_balances()

        self.assertEqual(self.exchange.available_balances["BTC"], Decimal("1.2"))
        self.assertEqual(self.exchange.get_balance("BTC"), Decimal("1.5"))
        self.assertEqual(self.exchange.available_balances["KRW"], Decimal("1000000"))

    async def test_place_limit_order_builds_payload(self):
        with patch.object(self.exchange, "_api_post", AsyncMock(return_value={"uuid": "order-1"})) as mock_post:
            exchange_order_id, _ = await self.exchange._place_order(
                order_id="OID-1",
                trading_pair="BTC-KRW",
                amount=Decimal("0.1"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("100000"),
            )

        self.assertEqual(exchange_order_id, "order-1")
        kwargs = mock_post.call_args.kwargs
        self.assertEqual(kwargs["path_url"], CONSTANTS.CREATE_ORDER_PATH_URL)
        self.assertTrue(kwargs["is_auth_required"])
        self.assertEqual(kwargs["data"]["market"], "KRW-BTC")
        self.assertEqual(kwargs["data"]["side"], "bid")
        self.assertEqual(kwargs["data"]["ord_type"], "limit")

    async def test_request_order_status_maps_state(self):
        order = InFlightOrder(
            client_order_id="OID-1",
            exchange_order_id="order-1",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )

        with patch.object(
            self.exchange,
            "_api_get",
            AsyncMock(return_value={"uuid": "order-1", "state": "done", "market": "KRW-BTC", "trades": []}),
        ):
            order_update = await self.exchange._request_order_status(order)

        self.assertEqual(order_update.exchange_order_id, "order-1")
        self.assertEqual(order_update.new_state, OrderState.FILLED)

    async def test_initialize_symbol_map_from_exchange_info(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(
            [
                {"market": "KRW-BTC", "market_warning": "NONE"},
                {"market": "KRW-ETH", "market_warning": "CAUTION"},
            ]
        )

        symbol = await self.exchange.exchange_symbol_associated_to_pair("BTC-KRW")
        self.assertEqual(symbol, "KRW-BTC")
