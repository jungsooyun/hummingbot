import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

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

    async def test_place_limit_maker_order_builds_post_only_payload(self):
        with patch.object(self.exchange, "_api_post", AsyncMock(return_value={"uuid": "order-2"})) as mock_post:
            exchange_order_id, _ = await self.exchange._place_order(
                order_id="OID-2",
                trading_pair="BTC-KRW",
                amount=Decimal("0.1"),
                trade_type=TradeType.SELL,
                order_type=OrderType.LIMIT_MAKER,
                price=Decimal("101000"),
            )

        self.assertEqual(exchange_order_id, "order-2")
        kwargs = mock_post.call_args.kwargs
        self.assertEqual(kwargs["data"]["market"], "KRW-BTC")
        self.assertEqual(kwargs["data"]["side"], "ask")
        self.assertEqual(kwargs["data"]["ord_type"], "limit")
        self.assertEqual(kwargs["data"]["time_in_force"], "post_only")

    def test_supported_order_types_includes_limit_maker(self):
        supported = self.exchange.supported_order_types()
        self.assertIn(OrderType.LIMIT, supported)
        self.assertIn(OrderType.LIMIT_MAKER, supported)
        self.assertIn(OrderType.MARKET, supported)

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

    async def test_request_order_status_trade_state_maps_partially_filled(self):
        order = InFlightOrder(
            client_order_id="OID-1A",
            exchange_order_id="order-1a",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("1"),
            creation_timestamp=self.exchange.current_timestamp,
        )

        with patch.object(
            self.exchange,
            "_api_get",
            AsyncMock(
                return_value={
                    "uuid": "order-1a",
                    "state": "trade",
                    "executed_volume": "0.3",
                    "remaining_volume": "0.7",
                    "volume": "1",
                    "trades": [],
                }
            ),
        ):
            order_update = await self.exchange._request_order_status(order)

        self.assertEqual(order_update.new_state, OrderState.PARTIALLY_FILLED)

    async def test_request_order_status_trade_state_with_zero_remaining_maps_filled(self):
        order = InFlightOrder(
            client_order_id="OID-1B",
            exchange_order_id="order-1b",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("1"),
            creation_timestamp=self.exchange.current_timestamp,
        )

        with patch.object(
            self.exchange,
            "_api_get",
            AsyncMock(
                return_value={
                    "uuid": "order-1b",
                    "state": "trade",
                    "executed_volume": "1",
                    "remaining_volume": "0",
                    "volume": "1",
                    "trades": [],
                }
            ),
        ):
            order_update = await self.exchange._request_order_status(order)

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

    async def test_user_stream_processes_order_filled(self):
        order = InFlightOrder(
            client_order_id="OID-1",
            exchange_order_id="order-ws-1",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-1"] = order
        order.current_state = OrderState.OPEN

        event_data = {
            "type": "myOrder",
            "uuid": "order-ws-1",
            "order_state": "done",
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }

        trade_response = {
            "uuid": "order-ws-1",
            "state": "done",
            "trades": [
                {"uuid": "trade-1", "price": "100000", "volume": "0.1", "funds": "10000", "fee": "5"}
            ],
        }

        with patch.object(self.exchange, "_api_get", AsyncMock(return_value=trade_response)):
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)  # let safe_ensure_future complete

        self.assertEqual(order.current_state, OrderState.FILLED)

    async def test_user_stream_processes_order_partially_filled(self):
        order = InFlightOrder(
            client_order_id="OID-2",
            exchange_order_id="order-ws-2",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("1.0"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-2"] = order
        order.current_state = OrderState.OPEN

        # state="wait" with executed_volume > 0 → PARTIALLY_FILLED, no REST call
        event_data = {
            "type": "myOrder",
            "uuid": "order-ws-2",
            "order_state": "wait",
            "executed_volume": "0.3",
            "remaining_volume": "0.7",
            "code": "KRW-BTC",
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
            price=Decimal("100000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-3"] = order
        order.current_state = OrderState.OPEN

        event_data = {
            "type": "myOrder",
            "uuid": "order-ws-3",
            "order_state": "cancel",
            "executed_volume": "0",
            "remaining_volume": "0.1",
            "code": "KRW-BTC",
        }

        await self.exchange._process_order_event(event_data)
        await asyncio.sleep(0)  # let safe_ensure_future complete

        self.assertEqual(order.current_state, OrderState.CANCELED)

    async def test_user_stream_processes_balance_update(self):
        event_data = {
            "type": "myAsset",
            "currency": "BTC",
            "balance": "5.0",
            "locked": "1.5",
        }

        self.exchange._process_balance_event(event_data)

        self.assertEqual(self.exchange.available_balances["BTC"], Decimal("5.0"))
        self.assertEqual(self.exchange.get_balance("BTC"), Decimal("6.5"))

    # ── WS Trade Direct Processing Tests ──

    async def test_ws_trade_event_creates_trade_update_without_rest(self):
        """state='trade' WS event → TradeUpdate created, REST call 0 times."""
        order = InFlightOrder(
            client_order_id="OID-WS-T1",
            exchange_order_id="order-wst-1",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("0.5"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-T1"] = order
        order.current_state = OrderState.OPEN

        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-1",
            "order_state": "trade",
            "trade_uuid": "fill-uuid-1",
            "price": "100000",
            "volume": "0.2",
            "trade_fee": "10",
            "is_maker": False,
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.2",
            "remaining_volume": "0.3",
            "code": "KRW-BTC",
        }

        with patch.object(self.exchange, "_api_get", AsyncMock()) as mock_api:
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)

        mock_api.assert_not_called()
        self.assertEqual(len(order.order_fills), 1)
        self.assertIn("fill-uuid-1", order.order_fills)
        fill = order.order_fills["fill-uuid-1"]
        self.assertEqual(fill.fill_price, Decimal("100000"))
        self.assertEqual(fill.fill_base_amount, Decimal("0.2"))

    async def test_ws_trade_event_partial_fill_sets_partially_filled(self):
        """remaining_volume > 0 → PARTIALLY_FILLED state."""
        order = InFlightOrder(
            client_order_id="OID-WS-T2",
            exchange_order_id="order-wst-2",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("1.0"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-T2"] = order
        order.current_state = OrderState.OPEN

        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-2",
            "order_state": "trade",
            "trade_uuid": "fill-uuid-2",
            "price": "100000",
            "volume": "0.3",
            "trade_fee": "15",
            "is_maker": True,
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.3",
            "remaining_volume": "0.7",
            "code": "KRW-BTC",
        }

        await self.exchange._process_order_event(event_data)
        await asyncio.sleep(0)

        self.assertEqual(order.current_state, OrderState.PARTIALLY_FILLED)
        fill = order.order_fills["fill-uuid-2"]
        self.assertFalse(fill.is_taker)  # is_maker=True → is_taker=False

    async def test_ws_trade_event_final_fill_sets_filled(self):
        """remaining_volume == 0 → FILLED state."""
        order = InFlightOrder(
            client_order_id="OID-WS-T3",
            exchange_order_id="order-wst-3",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.SELL,
            price=Decimal("100000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-T3"] = order
        order.current_state = OrderState.OPEN

        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-3",
            "order_state": "trade",
            "trade_uuid": "fill-uuid-3",
            "price": "100000",
            "volume": "0.1",
            "trade_fee": "5",
            "is_maker": False,
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }

        await self.exchange._process_order_event(event_data)
        await asyncio.sleep(0)

        self.assertEqual(order.current_state, OrderState.FILLED)

    async def test_ws_done_event_with_missing_trades_falls_back_to_rest(self):
        """state='done' + executed_volume mismatch → REST fallback 1 time."""
        order = InFlightOrder(
            client_order_id="OID-WS-T4",
            exchange_order_id="order-wst-4",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-T4"] = order
        order.current_state = OrderState.OPEN

        # state="done" but no prior trade events received
        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-4",
            "order_state": "done",
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }

        trade_response = {
            "uuid": "order-wst-4",
            "state": "done",
            "trades": [
                {"uuid": "trade-rest-1", "price": "100000", "volume": "0.1", "funds": "10000", "fee": "5"}
            ],
        }

        with patch.object(self.exchange, "_api_get", AsyncMock(return_value=trade_response)) as mock_api:
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)

        mock_api.assert_called_once()
        self.assertEqual(len(order.order_fills), 1)

    async def test_ws_done_event_all_trades_received_no_rest(self):
        """state='done' + all trades already processed → REST call 0 times."""
        order = InFlightOrder(
            client_order_id="OID-WS-T5",
            exchange_order_id="order-wst-5",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-T5"] = order
        order.current_state = OrderState.OPEN

        # First: simulate WS trade event
        trade_event = {
            "type": "myOrder",
            "uuid": "order-wst-5",
            "order_state": "trade",
            "trade_uuid": "fill-uuid-5",
            "price": "100000",
            "volume": "0.1",
            "trade_fee": "5",
            "is_maker": False,
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }
        await self.exchange._process_order_event(trade_event)

        # Then: state="done" event
        done_event = {
            "type": "myOrder",
            "uuid": "order-wst-5",
            "order_state": "done",
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }

        with patch.object(self.exchange, "_api_get", AsyncMock()) as mock_api:
            await self.exchange._process_order_event(done_event)
            await asyncio.sleep(0)

        mock_api.assert_not_called()

    async def test_ws_trade_without_trade_uuid_triggers_immediate_rest_fallback(self):
        order = InFlightOrder(
            client_order_id="OID-WS-T6",
            exchange_order_id="order-wst-6",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("0.2"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-T6"] = order
        order.current_state = OrderState.OPEN

        rest_trade_update = self.exchange._parse_trade_from_ws_event(
            {
                "trade_uuid": "rest-fill-1",
                "price": "100000",
                "volume": "0.2",
                "trade_fee": "10",
                "trade_timestamp": 1709700000000,
                "is_maker": False,
            },
            order,
        )

        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-6",
            "order_state": "trade",
            "price": "100000",
            "volume": "0.2",
            "trade_fee": "10",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.2",
            "remaining_volume": "0",
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
        self.assertIn("rest-fill-1", order.order_fills)

    async def test_ws_trade_with_zero_price_triggers_immediate_rest_fallback(self):
        order = InFlightOrder(
            client_order_id="OID-WS-T7",
            exchange_order_id="order-wst-7",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("0.2"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-T7"] = order
        order.current_state = OrderState.OPEN

        rest_trade_update = self.exchange._parse_trade_from_ws_event(
            {
                "trade_uuid": "rest-fill-2",
                "price": "100000",
                "volume": "0.2",
                "trade_fee": "10",
                "trade_timestamp": 1709700000000,
                "is_maker": False,
            },
            order,
        )

        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-7",
            "order_state": "trade",
            "trade_uuid": "fill-uuid-invalid",
            "price": "0",
            "volume": "0.2",
            "trade_fee": "10",
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.2",
            "remaining_volume": "0",
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
        self.assertIn("rest-fill-2", order.order_fills)

    async def test_ws_done_reconciles_before_filled_order_update(self):
        order = InFlightOrder(
            client_order_id="OID-WS-T8",
            exchange_order_id="order-wst-8",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-WS-T8"] = order
        order.current_state = OrderState.OPEN

        rest_trade_update = self.exchange._parse_trade_from_ws_event(
            {
                "trade_uuid": "rest-fill-3",
                "price": "100000",
                "volume": "0.1",
                "trade_fee": "5",
                "trade_timestamp": 1709700000000,
                "is_maker": False,
            },
            order,
        )
        call_order = []
        original_process_trade_update = self.exchange._order_tracker.process_trade_update
        original_process_order_update = self.exchange._order_tracker.process_order_update

        def track_trade_update(trade_update):
            call_order.append(("trade", trade_update.trade_id))
            return original_process_trade_update(trade_update)

        def track_order_update(order_update):
            call_order.append(("order", order_update.new_state))
            return original_process_order_update(order_update)

        event_data = {
            "type": "myOrder",
            "uuid": "order-wst-8",
            "order_state": "done",
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }

        with patch.object(
            self.exchange,
            "_all_trade_updates_for_order",
            AsyncMock(return_value=[rest_trade_update]),
        ), patch.object(
            self.exchange._order_tracker,
            "process_trade_update",
            side_effect=track_trade_update,
        ), patch.object(
            self.exchange._order_tracker,
            "process_order_update",
            side_effect=track_order_update,
        ):
            await self.exchange._process_order_event(event_data)
            await asyncio.sleep(0)

        self.assertGreaterEqual(len(call_order), 2)
        self.assertEqual(call_order[0], ("trade", "rest-fill-3"))
        self.assertEqual(call_order[1], ("order", OrderState.FILLED))

    # ── Integration / Regression Tests ──

    async def test_ws_trade_then_rest_poll_dedup(self):
        """WS trade processed, then REST poll returns same trade → dedup, no double processing."""
        order = InFlightOrder(
            client_order_id="OID-DEDUP-1",
            exchange_order_id="order-dedup-1",
            trading_pair="BTC-KRW",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            price=Decimal("100000"),
            amount=Decimal("0.1"),
            creation_timestamp=self.exchange.current_timestamp,
        )
        self.exchange._order_tracker._in_flight_orders["OID-DEDUP-1"] = order
        order.current_state = OrderState.OPEN

        # WS trade event
        trade_event = {
            "type": "myOrder",
            "uuid": "order-dedup-1",
            "order_state": "trade",
            "trade_uuid": "fill-dedup-1",
            "price": "100000",
            "volume": "0.1",
            "trade_fee": "5",
            "is_maker": False,
            "trade_timestamp": 1709700000000,
            "executed_volume": "0.1",
            "remaining_volume": "0",
            "code": "KRW-BTC",
        }
        await self.exchange._process_order_event(trade_event)
        self.assertEqual(len(order.order_fills), 1)

        # REST poll returns the same trade
        rest_response = {
            "uuid": "order-dedup-1",
            "state": "done",
            "trades": [
                {"uuid": "fill-dedup-1", "price": "100000", "volume": "0.1", "funds": "10000", "fee": "5"}
            ],
        }
        with patch.object(self.exchange, "_api_get", AsyncMock(return_value=rest_response)):
            trade_updates = await self.exchange._all_trade_updates_for_order(order)
            for tu in trade_updates:
                self.exchange._order_tracker.process_trade_update(tu)

        # Still only 1 fill (dedup by trade_id)
        self.assertEqual(len(order.order_fills), 1)
