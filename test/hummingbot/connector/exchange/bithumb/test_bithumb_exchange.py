import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from bidict import bidict
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.exchange.bithumb import bithumb_constants as CONSTANTS
from hummingbot.connector.exchange.bithumb.bithumb_exceptions import BithumbSelfTradePreventionError
from hummingbot.connector.exchange.bithumb.bithumb_exchange import BithumbExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate


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

    async def test_place_limit_order_accepts_order_id_response(self):
        with patch.object(self.exchange, "_api_post", AsyncMock(return_value={"order_id": "order-3"})):
            exchange_order_id, _ = await self.exchange._place_order(
                order_id="OID-3",
                trading_pair="BTC-KRW",
                amount=Decimal("0.1"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("55000000"),
            )

        self.assertEqual(exchange_order_id, "order-3")

    async def test_place_limit_order_maps_cross_trading_error(self):
        with patch.object(
            self.exchange,
            "_api_post",
            AsyncMock(side_effect=IOError("Error executing request POST ... Error: {'name':'cross_trading'}")),
        ):
            with self.assertRaises(BithumbSelfTradePreventionError):
                await self.exchange._place_order(
                    order_id="OID-STP-1",
                    trading_pair="BTC-KRW",
                    amount=Decimal("0.1"),
                    trade_type=TradeType.BUY,
                    order_type=OrderType.LIMIT,
                    price=Decimal("55000000"),
                )

    async def test_place_limit_order_maps_cross_trading_error_korean_message(self):
        with patch.object(
            self.exchange,
            "_api_post",
            AsyncMock(side_effect=IOError("자전거래 위험 주문으로 주문이 거부되었습니다.")),
        ):
            with self.assertRaises(BithumbSelfTradePreventionError):
                await self.exchange._place_order(
                    order_id="OID-STP-2",
                    trading_pair="BTC-KRW",
                    amount=Decimal("0.1"),
                    trade_type=TradeType.SELL,
                    order_type=OrderType.LIMIT,
                    price=Decimal("56000000"),
                )

    async def test_place_limit_order_non_stp_error_passthrough(self):
        with patch.object(
            self.exchange,
            "_api_post",
            AsyncMock(side_effect=IOError("Error executing request POST ... Error: {'name':'insufficient_funds'}")),
        ):
            with self.assertRaises(IOError):
                await self.exchange._place_order(
                    order_id="OID-ERR-1",
                    trading_pair="BTC-KRW",
                    amount=Decimal("0.1"),
                    trade_type=TradeType.BUY,
                    order_type=OrderType.LIMIT,
                    price=Decimal("55000000"),
                )

    async def test_place_limit_order_maps_cross_trading_error_payload(self):
        with patch.object(
            self.exchange,
            "_api_post",
            AsyncMock(return_value={"name": "cross_trading", "message": "자전거래 위험 주문"}),
        ):
            with self.assertRaises(BithumbSelfTradePreventionError):
                await self.exchange._place_order(
                    order_id="OID-STP-3",
                    trading_pair="BTC-KRW",
                    amount=Decimal("0.1"),
                    trade_type=TradeType.BUY,
                    order_type=OrderType.LIMIT,
                    price=Decimal("55000000"),
                )

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

    async def test_request_order_status_uses_order_id_response_field(self):
        order = InFlightOrder(
            client_order_id="OID-4",
            exchange_order_id="order-4",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )

        with patch.object(
            self.exchange,
            "_api_get",
            AsyncMock(return_value={"order_id": "order-4", "state": "done", "market": "KRW-BTC", "trades": []}),
        ):
            order_update = await self.exchange._request_order_status(order)

        self.assertEqual(order_update.exchange_order_id, "order-4")
        self.assertEqual(order_update.new_state, OrderState.FILLED)

    async def test_request_order_status_trade_state_maps_partially_filled(self):
        order = InFlightOrder(
            client_order_id="OID-4A",
            exchange_order_id="order-4a",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("1"),
            creation_timestamp=self.exchange.current_timestamp,
        )

        with patch.object(
            self.exchange,
            "_request_order_detail",
            AsyncMock(
                return_value={
                    "order_id": "order-4a",
                    "state": "trade",
                    "executed_volume": "0.4",
                    "remaining_volume": "0.6",
                    "volume": "1",
                    "trades": [],
                }
            ),
        ):
            order_update = await self.exchange._request_order_status(order)

        self.assertEqual(order_update.new_state, OrderState.PARTIALLY_FILLED)

    async def test_request_order_detail_fallbacks_to_uuid(self):
        async def mock_api_get(path_url, params, is_auth_required):
            if "order_id" in params:
                raise IOError("order_id not accepted")
            return {"uuid": "order-5", "state": "wait"}

        with patch.object(self.exchange, "_api_get", AsyncMock(side_effect=mock_api_get)) as mock_get:
            order_data = await self.exchange._request_order_detail("order-5")

        self.assertEqual(order_data["uuid"], "order-5")
        self.assertEqual(mock_get.call_count, 2)

    async def test_initialize_symbol_map_from_exchange_info(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(
            [
                {"market": "KRW-BTC", "market_warning": "NONE"},
                {"market": "KRW-XRP", "market_warning": "CAUTION"},
            ]
        )

        symbol = await self.exchange.exchange_symbol_associated_to_pair("BTC-KRW")
        self.assertEqual(symbol, "KRW-BTC")

    async def test_user_stream_processes_order_filled(self):
        order = InFlightOrder(
            client_order_id="OID-1",
            exchange_order_id="order-ws-1",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-1"] = order
        order.current_state = OrderState.OPEN

        event_data = {
            "type": "myOrder",
            "code": "KRW-BTC",
            "uuid": "order-ws-1",
            "state": "done",
            "executed_volume": 0.1,
            "remaining_volume": 0,
        }

        trade_response = {
            "uuid": "order-ws-1",
            "state": "done",
            "trades": [
                {"uuid": "trade-1", "price": "55000000", "volume": "0.1", "funds": "5500000", "fee": "2750"}
            ],
        }

        with patch.object(self.exchange, "_request_order_detail", AsyncMock(return_value=trade_response)):
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)

        self.assertEqual(order.current_state, OrderState.FILLED)

    async def test_user_stream_processes_order_partially_filled(self):
        order = InFlightOrder(
            client_order_id="OID-2",
            exchange_order_id="order-ws-2",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("1.0"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-2"] = order
        order.current_state = OrderState.OPEN

        # state="wait" with executed_volume > 0 → PARTIALLY_FILLED, no REST call
        event_data = {
            "type": "myOrder",
            "code": "KRW-BTC",
            "uuid": "order-ws-2",
            "state": "wait",
            "executed_volume": 0.3,
            "remaining_volume": 0.7,
        }

        await self.exchange._process_order_event(event_data)
        await asyncio.sleep(0)

        self.assertEqual(order.current_state, OrderState.PARTIALLY_FILLED)

    async def test_user_stream_processes_order_canceled(self):
        order = InFlightOrder(
            client_order_id="OID-3",
            exchange_order_id="order-ws-3",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-3"] = order
        order.current_state = OrderState.OPEN

        event_data = {
            "type": "myOrder",
            "code": "KRW-BTC",
            "uuid": "order-ws-3",
            "state": "cancel",
            "executed_volume": 0,
            "remaining_volume": 0.1,
        }

        await self.exchange._process_order_event(event_data)
        await asyncio.sleep(0)

        self.assertEqual(order.current_state, OrderState.CANCELED)

    async def test_user_stream_processes_balance_update(self):
        event_data = {
            "type": "myAsset",
            "currency": "BTC",
            "balance": "1.5",
            "locked": "0.3",
        }

        self.exchange._process_balance_event(event_data)

        self.assertEqual(self.exchange.available_balances["BTC"], Decimal("1.5"))
        self.assertEqual(self.exchange.get_balance("BTC"), Decimal("1.8"))

    # ── WS Trade Direct Processing Tests ──

    async def test_ws_trade_event_creates_trade_update_without_rest(self):
        """state='trade' WS event → TradeUpdate created, REST call 0 times."""
        order = InFlightOrder(
            client_order_id="OID-WS-T1",
            exchange_order_id="order-wst-1",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("0.5"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-T1"] = order
        order.current_state = OrderState.OPEN

        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-1",
            "state": "trade",
            "trade_uuid": "fill-uuid-1",
            "price": "55000000",
            "volume": "0.2",
            "paid_fee": "5500",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.2",
            "remaining_volume": "0.3",
            "code": "KRW-BTC",
        }

        with patch.object(self.exchange, "_request_order_detail", AsyncMock()) as mock_api:
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)

        mock_api.assert_not_called()
        self.assertEqual(len(order.order_fills), 1)
        self.assertIn("fill-uuid-1", order.order_fills)
        fill = order.order_fills["fill-uuid-1"]
        self.assertEqual(fill.fill_price, Decimal("55000000"))
        self.assertEqual(fill.fill_base_amount, Decimal("0.2"))

    async def test_ws_trade_fee_from_paid_fee_delta(self):
        """Two consecutive fills: paid_fee delta gives correct per-trade fee."""
        order = InFlightOrder(
            client_order_id="OID-WS-FEE",
            exchange_order_id="order-wst-fee",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("1.0"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-FEE"] = order
        order.current_state = OrderState.OPEN

        # First fill: paid_fee=2750 (delta from 0 = 2750)
        event1 = {
            "type": "myOrder",
            "uuid": "order-wst-fee",
            "state": "trade",
            "trade_uuid": "fill-fee-1",
            "price": "55000000",
            "volume": "0.1",
            "paid_fee": "2750",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0.9",
            "code": "KRW-BTC",
        }
        await self.exchange._process_order_event(event1)

        # Second fill: paid_fee=8250 (delta from 2750 = 5500)
        event2 = {
            "type": "myOrder",
            "uuid": "order-wst-fee",
            "state": "trade",
            "trade_uuid": "fill-fee-2",
            "price": "55000000",
            "volume": "0.2",
            "paid_fee": "8250",
            "trade_timestamp": 1709700001000,
            "executed_volume": "0.3",
            "remaining_volume": "0.7",
            "code": "KRW-BTC",
        }
        await self.exchange._process_order_event(event2)

        self.assertEqual(len(order.order_fills), 2)
        fill1 = order.order_fills["fill-fee-1"]
        fill2 = order.order_fills["fill-fee-2"]
        self.assertEqual(fill1.fee.flat_fees[0].amount, Decimal("2750"))
        self.assertEqual(fill2.fee.flat_fees[0].amount, Decimal("5500"))

    async def test_ws_paid_fee_cleanup_on_done(self):
        """state='done' → _ws_order_paid_fees dict entry removed."""
        order = InFlightOrder(
            client_order_id="OID-WS-CLN",
            exchange_order_id="order-wst-cln",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-CLN"] = order
        order.current_state = OrderState.OPEN

        # Simulate a trade to populate _ws_order_paid_fees
        trade_event = {
            "type": "myOrder",
            "uuid": "order-wst-cln",
            "state": "trade",
            "trade_uuid": "fill-cln-1",
            "price": "55000000",
            "volume": "0.1",
            "paid_fee": "2750",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }
        await self.exchange._process_order_event(trade_event)
        self.assertIn("order-wst-cln", self.exchange._ws_order_paid_fees)

        # state="done" → cleanup
        done_event = {
            "type": "myOrder",
            "uuid": "order-wst-cln",
            "state": "done",
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }
        with patch.object(self.exchange, "_request_order_detail", AsyncMock(return_value={"trades": []})):
            await self.exchange._process_order_event(done_event)

        self.assertNotIn("order-wst-cln", self.exchange._ws_order_paid_fees)

    async def test_ws_done_event_with_missing_trades_falls_back_to_rest(self):
        """state='done' + executed_volume mismatch → REST fallback."""
        order = InFlightOrder(
            client_order_id="OID-WS-FB",
            exchange_order_id="order-wst-fb",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-FB"] = order
        order.current_state = OrderState.OPEN

        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-fb",
            "state": "done",
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }

        trade_response = {
            "uuid": "order-wst-fb",
            "state": "done",
            "trades": [
                {"uuid": "trade-rest-1", "price": "55000000", "volume": "0.1", "funds": "5500000", "fee": "2750"}
            ],
        }

        with patch.object(self.exchange, "_request_order_detail", AsyncMock(return_value=trade_response)) as mock_api:
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)

        mock_api.assert_called_once()
        self.assertEqual(len(order.order_fills), 1)

    async def test_ws_trade_without_trade_uuid_ignored(self):
        """trade_uuid absent → immediate REST reconciliation."""
        order = InFlightOrder(
            client_order_id="OID-WS-NO-UUID",
            exchange_order_id="order-wst-nouuid",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-NO-UUID"] = order
        order.current_state = OrderState.OPEN

        # state="trade" but no trade_uuid
        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-nouuid",
            "state": "trade",
            "price": "55000000",
            "volume": "0.1",
            "paid_fee": "2750",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }

        with patch.object(self.exchange, "_all_trade_updates_for_order", AsyncMock(return_value=[])) as mock_trade_updates:
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)

        mock_trade_updates.assert_called_once_with(order)
        self.assertEqual(len(order.order_fills), 0)

    async def test_duplicate_trade_uuid_does_not_mutate_paid_fee_tracker(self):
        order = InFlightOrder(
            client_order_id="OID-WS-DUP",
            exchange_order_id="order-wst-dup",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-DUP"] = order
        order.current_state = OrderState.OPEN

        first_event = {
            "type": "myOrder",
            "uuid": "order-wst-dup",
            "state": "trade",
            "trade_uuid": "dup-fill-1",
            "price": "55000000",
            "volume": "0.1",
            "paid_fee": "2750",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0.9",
            "code": "KRW-BTC",
        }
        duplicate_event = dict(first_event)

        await self.exchange._process_order_event(first_event)
        with patch.object(self.exchange, "_all_trade_updates_for_order", AsyncMock(return_value=[])) as mock_trade_updates:
            await self.exchange._process_order_event(duplicate_event)
            await asyncio.sleep(0)

        mock_trade_updates.assert_not_called()
        self.assertEqual(len(order.order_fills), 1)
        self.assertEqual(self.exchange._ws_order_paid_fees["order-wst-dup"], Decimal("2750"))

    async def test_paid_fee_decrease_triggers_rest_fallback(self):
        order = InFlightOrder(
            client_order_id="OID-WS-REG",
            exchange_order_id="order-wst-reg",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-REG"] = order
        order.current_state = OrderState.OPEN

        event1 = {
            "type": "myOrder",
            "uuid": "order-wst-reg",
            "state": "trade",
            "trade_uuid": "reg-fill-1",
            "price": "55000000",
            "volume": "0.1",
            "paid_fee": "2750",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0.9",
            "code": "KRW-BTC",
        }
        await self.exchange._process_order_event(event1)

        rest_trade_update = TradeUpdate(
            trade_id="reg-fill-2-rest",
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            trading_pair=order.trading_pair,
            fee=self.exchange._get_fee("BTC", "KRW", order.order_type, order.trade_type, Decimal("0.1"), Decimal("55000000")),
            fill_base_amount=Decimal("0.1"),
            fill_quote_amount=Decimal("5500000"),
            fill_price=Decimal("55000000"),
            fill_timestamp=self.exchange.current_timestamp,
        )
        regression_event = {
            "type": "myOrder",
            "uuid": "order-wst-reg",
            "state": "trade",
            "trade_uuid": "reg-fill-2",
            "price": "55000000",
            "volume": "0.1",
            "paid_fee": "1000",
            "trade_timestamp": 1709700001000,
            "executed_volume": "0.2",
            "remaining_volume": "0.8",
            "code": "KRW-BTC",
        }

        with patch.object(
            self.exchange,
            "_all_trade_updates_for_order",
            AsyncMock(return_value=[rest_trade_update]),
        ) as mock_trade_updates:
            await self.exchange._process_order_event(regression_event)
            await asyncio.sleep(0)

        mock_trade_updates.assert_called_once_with(order)
        self.assertEqual(len(order.order_fills), 2)
        self.assertIn("reg-fill-2-rest", order.order_fills)
        self.assertEqual(self.exchange._ws_order_paid_fees["order-wst-reg"], Decimal("2750"))

    async def test_trades_count_gap_triggers_rest_fallback(self):
        order = InFlightOrder(
            client_order_id="OID-WS-GAP",
            exchange_order_id="order-wst-gap",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-GAP"] = order
        order.current_state = OrderState.OPEN

        rest_trade_update = TradeUpdate(
            trade_id="gap-fill-rest",
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            trading_pair=order.trading_pair,
            fee=self.exchange._get_fee("BTC", "KRW", order.order_type, order.trade_type, Decimal("0.1"), Decimal("55000000")),
            fill_base_amount=Decimal("0.1"),
            fill_quote_amount=Decimal("5500000"),
            fill_price=Decimal("55000000"),
            fill_timestamp=self.exchange.current_timestamp,
        )
        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-gap",
            "state": "trade",
            "trade_uuid": "gap-fill-ws",
            "price": "55000000",
            "volume": "0.1",
            "paid_fee": "2750",
            "trades_count": "2",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0.9",
            "code": "KRW-BTC",
        }

        with patch.object(
            self.exchange,
            "_all_trade_updates_for_order",
            AsyncMock(return_value=[rest_trade_update]),
        ) as mock_trade_updates:
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)

        mock_trade_updates.assert_called_once_with(order)
        self.assertEqual(len(order.order_fills), 1)
        self.assertIn("gap-fill-rest", order.order_fills)

    async def test_final_state_from_rest_cleans_paid_fee_tracker(self):
        order = InFlightOrder(
            client_order_id="OID-WS-CLEAN",
            exchange_order_id="order-wst-clean",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._ws_order_paid_fees["order-wst-clean"] = Decimal("2750")

        with patch.object(
            self.exchange,
            "_request_order_detail",
            AsyncMock(return_value={"order_id": "order-wst-clean", "state": "done", "executed_volume": "1"}),
        ):
            order_update = await self.exchange._request_order_status(order)

        self.assertEqual(order_update.new_state, OrderState.FILLED)
        self.assertNotIn("order-wst-clean", self.exchange._ws_order_paid_fees)

    async def test_invalid_trade_payload_triggers_immediate_rest_fallback(self):
        order = InFlightOrder(
            client_order_id="OID-WS-BAD",
            exchange_order_id="order-wst-bad",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-BAD"] = order
        order.current_state = OrderState.OPEN

        rest_trade_update = TradeUpdate(
            trade_id="bad-fill-rest",
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            trading_pair=order.trading_pair,
            fee=self.exchange._get_fee("BTC", "KRW", order.order_type, order.trade_type, Decimal("0.1"), Decimal("55000000")),
            fill_base_amount=Decimal("0.1"),
            fill_quote_amount=Decimal("5500000"),
            fill_price=Decimal("55000000"),
            fill_timestamp=self.exchange.current_timestamp,
        )
        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-bad",
            "state": "trade",
            "trade_uuid": "bad-fill-ws",
            "price": "55000000",
            "volume": "0",
            "paid_fee": "2750",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0.9",
            "code": "KRW-BTC",
        }

        with patch.object(
            self.exchange,
            "_all_trade_updates_for_order",
            AsyncMock(return_value=[rest_trade_update]),
        ) as mock_trade_updates:
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)

        mock_trade_updates.assert_called_once_with(order)
        self.assertIn("bad-fill-rest", order.order_fills)

    async def test_bithumb_anomaly_fallback_anchors_paid_fee_tracker(self):
        order = InFlightOrder(
            client_order_id="OID-WS-ANCHOR",
            exchange_order_id="order-wst-anchor",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-ANCHOR"] = order
        order.current_state = OrderState.OPEN

        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-anchor",
            "state": "trade",
            "trade_uuid": "anchor-fill-ws",
            "price": "55000000",
            "volume": "0.1",
            "paid_fee": "3000",
            "trades_count": "2",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0.9",
            "code": "KRW-BTC",
        }

        with patch.object(
            self.exchange,
            "_all_trade_updates_for_order",
            AsyncMock(return_value=[]),
        ):
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)

        self.assertEqual(self.exchange._ws_order_paid_fees["order-wst-anchor"], Decimal("3000"))

    # ── Integration / Regression Tests ──

    async def test_existing_rest_only_path_still_works(self):
        """_all_trade_updates_for_order() REST path works independently."""
        order = InFlightOrder(
            client_order_id="OID-REST-1",
            exchange_order_id="order-rest-1",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("55000000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-REST-1"] = order

        rest_response = {
            "uuid": "order-rest-1",
            "state": "done",
            "trades": [
                {"uuid": "trade-r1", "price": "55000000", "volume": "0.05", "funds": "2750000", "fee": "1375"},
                {"uuid": "trade-r2", "price": "55100000", "volume": "0.05", "funds": "2755000", "fee": "1377"},
            ],
        }

        with patch.object(self.exchange, "_request_order_detail", AsyncMock(return_value=rest_response)):
            trade_updates = await self.exchange._all_trade_updates_for_order(order)

        self.assertEqual(len(trade_updates), 2)
        self.assertEqual(trade_updates[0].trade_id, "trade-r1")
        self.assertEqual(trade_updates[1].trade_id, "trade-r2")
