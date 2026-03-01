from decimal import Decimal
from unittest.mock import AsyncMock, patch

from bidict import bidict
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange.bithumb import bithumb_constants as CONSTANTS
from hummingbot.connector.exchange.bithumb.bithumb_exchange import BithumbExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


class BithumbExchangeTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.exchange = BithumbExchange(
            bithumb_access_key="test-access",
            bithumb_secret_key="test-secret",
            trading_pairs=["BTC-KRW"],
        )
        self.exchange._set_trading_pair_symbol_map(bidict({"KRW-BTC": "BTC-KRW"}))

    async def test_get_last_traded_price(self):
        with patch.object(self.exchange, "_api_get", AsyncMock(return_value=[{"market": "KRW-BTC", "trade_price": 55500000.0}])) as mock_api:
            price = await self.exchange._get_last_traded_price("BTC-KRW")

        self.assertEqual(price, 55500000.0)
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
                    {"currency": "BTC", "balance": "1.0", "locked": "0.2"},
                    {"currency": "KRW", "balance": "500000", "locked": "0"},
                ]
            ),
        ):
            await self.exchange._update_balances()

        self.assertEqual(self.exchange.available_balances["BTC"], Decimal("1.0"))
        self.assertEqual(self.exchange.get_balance("BTC"), Decimal("1.2"))
        self.assertEqual(self.exchange.available_balances["KRW"], Decimal("500000"))

    async def test_place_limit_order_builds_payload(self):
        with patch.object(self.exchange, "_api_post", AsyncMock(return_value={"uuid": "order-2"})) as mock_post:
            exchange_order_id, _ = await self.exchange._place_order(
                order_id="OID-2",
                trading_pair="BTC-KRW",
                amount=Decimal("0.2"),
                trade_type=TradeType.SELL,
                order_type=OrderType.LIMIT,
                price=Decimal("56000000"),
            )

        self.assertEqual(exchange_order_id, "order-2")
        kwargs = mock_post.call_args.kwargs
        self.assertEqual(kwargs["path_url"], CONSTANTS.CREATE_ORDER_PATH_URL)
        self.assertEqual(kwargs["data"]["market"], "KRW-BTC")
        self.assertEqual(kwargs["data"]["side"], "ask")
        self.assertEqual(kwargs["data"]["order_type"], "limit")

    async def test_place_cancel_uses_order_id_parameter(self):
        order = InFlightOrder(
            client_order_id="OID-3",
            exchange_order_id="ex-order-3",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("0.001"),
            creation_timestamp=self.exchange.current_timestamp,
        )

        with patch.object(self.exchange, "_api_delete", AsyncMock(return_value={"order_id": "ex-order-3"})) as mock_delete:
            result = await self.exchange._place_cancel(order.client_order_id, order)

        self.assertTrue(result)
        kwargs = mock_delete.call_args.kwargs
        self.assertEqual(kwargs["path_url"], CONSTANTS.CANCEL_ORDER_PATH_URL)
        self.assertEqual(kwargs["params"]["order_id"], "ex-order-3")

    async def test_request_order_status_maps_state(self):
        order = InFlightOrder(
            client_order_id="OID-2",
            exchange_order_id="order-2",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            price=Decimal("56000000"),
            amount=Decimal("0.2"),
            creation_timestamp=self.exchange.current_timestamp,
        )

        with patch.object(
            self.exchange,
            "_api_get",
            AsyncMock(return_value={"uuid": "order-2", "state": "cancel", "market": "KRW-BTC", "trades": []}),
        ):
            order_update = await self.exchange._request_order_status(order)

        self.assertEqual(order_update.exchange_order_id, "order-2")
        self.assertEqual(order_update.new_state, OrderState.CANCELED)

    async def test_initialize_symbol_map_from_exchange_info(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(
            [
                {"market": "KRW-BTC", "market_warning": "NONE"},
                {"market": "KRW-XRP", "market_warning": "CAUTION"},
            ]
        )

        symbol = await self.exchange.exchange_symbol_associated_to_pair("BTC-KRW")
        self.assertEqual(symbol, "KRW-BTC")
