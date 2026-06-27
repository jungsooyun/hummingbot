import asyncio
import json
import re
import time
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

from aioresponses import aioresponses
from bidict import bidict

import hummingbot.connector.exchange.kis.kis_constants as CONSTANTS
from hummingbot.connector.exchange.kis import kis_web_utils as web_utils
from hummingbot.connector.exchange.kis.kis_api_order_book_data_source import KisAPIOrderBookDataSource
from hummingbot.connector.exchange.kis.kis_auth import KisAuth
from hummingbot.connector.exchange.kis.kis_exchange import KisExchange
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class _FakeAsyncCtx:
    """Helper to mock ``async with session.ws_connect(...)`` pattern."""

    def __init__(self, obj):
        self._obj = obj

    async def __aenter__(self):
        return self._obj

    async def __aexit__(self, *args):
        pass


class _AsyncIter:
    """Wrap a regular iterable so ``async for`` works."""

    def __init__(self, items):
        self._items = iter(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration


class _FakeWS:
    """Fake aiohttp WebSocket that supports ``async for`` iteration."""

    def __init__(self, messages):
        self._messages = messages
        self.send_json = AsyncMock()
        self.send_str = AsyncMock()

    def __aiter__(self):
        return _AsyncIter(self._messages)


def _reprice(parsed: dict, best_ask: str, best_bid: str) -> dict:
    p = dict(parsed)
    p["ASKP1"] = best_ask
    p["BIDP1"] = best_bid
    return p


class KisAPIOrderBookDataSourceUnitTests(IsolatedAsyncioWrapperTestCase):
    """
    Tests for the KIS WebSocket-based order book data source.

    KIS provides WebSocket streams for real-time market data:
    - H0STASP0: orderbook (10 levels)
    - H0STCNT0: trade executions

    Tests verify:
    1. REST order book snapshot fetch (domestic stock orderbook, fallback)
    2. Last traded price fetch (domestic stock ticker)
    3. WebSocket subscription message building
    4. WebSocket data message parsing (caret-separated fields)
    5. WebSocket orderbook data processing
    6. WebSocket trade data processing
    7. WebSocket control message handling (PINGPONG, subscription responses)
    8. listen_for_subscriptions with mocked WebSocket
    9. Base-class abstract method stubs (NotImplementedError)
    """

    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "005930"
        cls.quote_asset = "KRW"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = cls.base_asset

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.listening_task = None

        self.connector = KisExchange(
            kis_app_key="test_app_key",
            kis_app_secret="test_app_secret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )
        # Seed the connector OAuth token so authenticated market-data requests
        # (e.g. the REST orderbook snapshot) don't hit the unmocked tokenP endpoint.
        self.connector._auth._access_token = "test_token"
        self.connector._auth._token_expires_at = time.time() + 86400

        self.mock_auth = KisAuth(
            app_key="test_app_key",
            app_secret="test_app_secret",
            initial_token="test_token",
            initial_approval_key="test_approval_key",
        )

        self.data_source = KisAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            auth=self.mock_auth,
            hub=MagicMock(),
            domain=CONSTANTS.DEFAULT_DOMAIN,
            market_routing=CONSTANTS.MARKET_ROUTING_SOR,
            # JEP-217: keep the out-of-session snapshot gate OPEN by default so the
            # pre-existing REST-fallback tests stay wall-clock-independent (they
            # predate the gate and assume the loop always fetches). Gate-specific
            # tests override ``_session_open_fn`` explicitly.
            session_open_fn=lambda: True,
        )
        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

        self.connector._set_trading_pair_symbol_map(
            bidict({self.ex_trading_pair: self.trading_pair})
        )

    async def asyncTearDown(self) -> None:
        if self.listening_task is not None:
            self.listening_task.cancel()
            try:
                await self.listening_task
            except asyncio.CancelledError:
                pass
        await super().asyncTearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(
            record.levelname == log_level and record.getMessage() == message
            for record in self.log_records
        )

    def test_rest_snapshot_poll_interval_is_fast(self):
        # KIS realtime WS is environment-gated and frequently unavailable; when the
        # WS push is down, the ONLY thing that refreshes the spot order book is the
        # base-class REST fallback in listen_for_order_book_snapshots, which fires
        # after FULL_ORDER_BOOK_RESET_DELTA_SECONDS of WS silence. The upstream
        # default is 3600s (1 hour) -> the maker would quote off an hour-stale spot.
        # KIS must override it to a fast cadence so REST keeps the book fresh.
        self.assertLessEqual(
            self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS,
            CONSTANTS.REST_ORDER_BOOK_POLL_INTERVAL,
            "KIS order book REST fallback must poll fast (WS is unreliable), "
            "not inherit the 1-hour upstream default.",
        )
        self.assertEqual(
            CONSTANTS.REST_ORDER_BOOK_POLL_INTERVAL,
            self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS,
        )

    async def test_ws_disabled_never_connects(self):
        # kis_ws_enabled=false -> listen_for_subscriptions must NOT touch the WS
        # edge (no approval-key fetch, no ws_connect): the bot stops hammering
        # ops.koreainvestment.com:21000. The order book is served by REST polling.
        self.data_source._ws_enabled = False
        self.data_source._auth.get_ws_approval_key = AsyncMock()
        with patch.object(self.data_source, "_sleep", new_callable=AsyncMock) as sleep_mock:
            sleep_mock.side_effect = asyncio.CancelledError
            with self.assertRaises(asyncio.CancelledError):
                await self.data_source.listen_for_subscriptions()
        self.data_source._auth.get_ws_approval_key.assert_not_called()
        sleep_mock.assert_awaited_once()

    def _is_logged_partial(self, log_level: str, message_fragment: str) -> bool:
        """Check if any log record at the given level contains the fragment."""
        return any(
            record.levelname == log_level and message_fragment in record.getMessage()
            for record in self.log_records
        )

    # ------------------------------------------------------------------
    # Mock response helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _order_book_snapshot_response() -> Dict[str, Any]:
        """KIS domestic stock orderbook REST response (TR_ID: FHKST01010200)."""
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "정상처리 되었습니다.",
            "output1": {
                "askp1": "72100", "askp_rsqn1": "500",
                "askp2": "72200", "askp_rsqn2": "300",
                "askp3": "72300", "askp_rsqn3": "200",
                "askp4": "72400", "askp_rsqn4": "150",
                "askp5": "72500", "askp_rsqn5": "100",
                "askp6": "72600", "askp_rsqn6": "80",
                "askp7": "72700", "askp_rsqn7": "60",
                "askp8": "72800", "askp_rsqn8": "40",
                "askp9": "72900", "askp_rsqn9": "30",
                "askp10": "73000", "askp_rsqn10": "20",
                "bidp1": "71900", "bidp_rsqn1": "400",
                "bidp2": "71800", "bidp_rsqn2": "600",
                "bidp3": "71700", "bidp_rsqn3": "350",
                "bidp4": "71600", "bidp_rsqn4": "250",
                "bidp5": "71500", "bidp_rsqn5": "200",
                "bidp6": "71400", "bidp_rsqn6": "180",
                "bidp7": "71300", "bidp_rsqn7": "150",
                "bidp8": "71200", "bidp_rsqn8": "120",
                "bidp9": "71100", "bidp_rsqn9": "100",
                "bidp10": "71000", "bidp_rsqn10": "80",
            },
        }

    @staticmethod
    def _ticker_response() -> Dict[str, Any]:
        """KIS domestic stock ticker response (TR_ID: FHPST01010000)."""
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "정상처리 되었습니다.",
            "output": {
                "stck_prpr": "72000",
                "stck_hgpr": "73000",
                "stck_lwpr": "71000",
                "acml_vol": "12345678",
            },
        }

    @staticmethod
    def _empty_orderbook_response() -> Dict[str, Any]:
        """KIS response mixing zero-sized levels with valid ones. askp1 (size 0) and bidp1
        (size 0) are filtered; askp2/bidp2 survive so BOTH sides keep a valid level."""
        return {
            "rt_cd": "0",
            "msg1": "정상처리 되었습니다.",
            "output1": {
                "askp1": "72100", "askp_rsqn1": "0",
                "askp2": "72200", "askp_rsqn2": "300",
                "bidp1": "71900", "bidp_rsqn1": "0",
                "bidp2": "71800", "bidp_rsqn2": "600",
            },
        }

    @staticmethod
    def _all_zero_orderbook_response() -> Dict[str, Any]:
        """KIS HTTP-200 response where every level is zero/invalid -> both sides empty
        after filtering. Unusable as a fair source; must fail closed (JEP-161)."""
        return {
            "rt_cd": "0",
            "msg1": "정상처리 되었습니다.",
            "output1": {
                "askp1": "0", "askp_rsqn1": "0",
                "bidp1": "0", "bidp_rsqn1": "0",
            },
        }

    @staticmethod
    def _one_sided_orderbook_response() -> Dict[str, Any]:
        """Valid asks but every bid zero -> bids empty after filter (one-sided book).
        Unusable as a two-sided fair source -> must fail closed (JEP-161)."""
        return {
            "rt_cd": "0",
            "msg1": "정상처리 되었습니다.",
            "output1": {
                "askp1": "72100", "askp_rsqn1": "500",
                "askp2": "72200", "askp_rsqn2": "300",
                "bidp1": "0", "bidp_rsqn1": "0",
            },
        }

    # ------------------------------------------------------------------
    # Mock WebSocket data helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ws_orderbook_raw(hour_cls_code: str = "0") -> str:
        """Raw WS orderbook message (H0UNASP0) with pipe + caret format.

        JEP-202: parts[2] is the KIS record count ("데이터건수"), NOT the stock code —
        the stock code is the first caret field (MKSC_SHRN_ISCD). The fixture uses the
        realistic record count so it cannot re-mask the parts[2] resolution bug."""
        return (
            "0|H0UNASP0|001|"
            f"005930^093000^{hour_cls_code}"
            "^67800^67900^68000^68100^68200^68300^68400^68500^68600^68700"
            "^67700^67600^67500^67400^67300^67200^67100^67000^66900^66800"
            "^1000^2000^3000^4000^5000^6000^7000^8000^9000^10000"
            "^10000^9000^8000^7000^6000^5000^4000^3000^2000^1000"
            "^55000^55000^0^0^67750^50^100^50^2^0.5^1000000^100^-100^0^0^1"
        )

    @staticmethod
    def _ws_trade_raw() -> str:
        """Raw WS trade message (H0UNCNT0) with pipe + caret format.

        JEP-202: parts[2] is the record count ("데이터건수"), not the stock code."""
        return (
            "0|H0UNCNT0|001|"
            "005930^093001^67800^2^-200^-0.29^67750^68000^68200^67600"
            "^67900^67700^100^500000^33900000000^1234^5678^-4444^25.5"
            "^617^383^1^38.3^3.5^090000^2^200^091500^2^200^093000^2^-200"
            "^20260228^Y^N^1000^10000^55000^55000^3.5^400000^80.0^0^21^67000"
        )

    @staticmethod
    def _ws_pingpong_raw() -> str:
        """Raw PINGPONG message from KIS WS."""
        return '{"header": {"tr_id": "PINGPONG"}}'

    @staticmethod
    def _ws_subscription_success_raw() -> str:
        """Raw subscription success response from KIS WS."""
        return json.dumps({
            "header": {
                "tr_id": "H0STASP0",
                "tr_key": "005930",
                "encrypt": "N",
            },
            "body": {
                "rt_cd": "0",
                "msg_cd": "OPSP0000",
                "msg1": "SUBSCRIBE SUCCESS",
            },
        })

    @staticmethod
    def _ws_market_status_subscription_success_raw(
        tr_id: str = "H0UNMKO0", tr_key: str = "005930"
    ) -> str:
        return json.dumps({
            "header": {
                "tr_id": tr_id,
                "tr_key": tr_key,
                "encrypt": "N",
            },
            "body": {
                "rt_cd": "0",
                "msg_cd": "OPSP0000",
                "msg1": "SUBSCRIBE SUCCESS",
            },
        })

    @staticmethod
    def _ws_market_status_payload(**overrides: str) -> str:
        data = {col: "0" for col in CONSTANTS.WS_MARKET_STATUS_COLUMNS}
        data["MKSC_SHRN_ISCD"] = "005930"
        data.update(overrides)
        return "^".join(data[col] for col in CONSTANTS.WS_MARKET_STATUS_COLUMNS)

    @staticmethod
    def _ws_subscription_error_raw() -> str:
        """Raw subscription error response from KIS WS."""
        return json.dumps({
            "header": {
                "tr_id": "H0STASP0",
                "tr_key": "005930",
                "encrypt": "N",
            },
            "body": {
                "rt_cd": "1",
                "msg_cd": "OPSP0001",
                "msg1": "SUBSCRIBE FAILED",
            },
        })

    # ------------------------------------------------------------------
    # Test: get_last_traded_prices
    # ------------------------------------------------------------------

    @aioresponses()
    def test_get_last_traded_prices(self, mock_api):
        """Should delegate to connector and return the current stock price."""
        mock_response = self._ticker_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_TICKER_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        results = self.local_event_loop.run_until_complete(
            asyncio.gather(
                self.data_source.get_last_traded_prices([self.trading_pair])
            )
        )
        results: Dict[str, Any] = results[0]

        self.assertIn(self.trading_pair, results)
        self.assertEqual(72000.0, results[self.trading_pair])

    # ------------------------------------------------------------------
    # Test: _order_book_snapshot (REST snapshot fallback)
    # ------------------------------------------------------------------

    @aioresponses()
    def test_order_book_snapshot(self, mock_api):
        """REST snapshot should parse 10 ask + 10 bid levels correctly."""
        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        snapshot_msg: OrderBookMessage = self.local_event_loop.run_until_complete(
            self.data_source._order_book_snapshot(self.trading_pair)
        )

        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot_msg.type)
        self.assertEqual(self.trading_pair, snapshot_msg.trading_pair)

        bids = snapshot_msg.bids
        asks = snapshot_msg.asks

        # Should have 10 ask levels and 10 bid levels
        self.assertEqual(10, len(asks))
        self.assertEqual(10, len(bids))

        # Asks should be sorted by price ascending
        self.assertEqual(72100.0, asks[0].price)
        self.assertEqual(500.0, asks[0].amount)
        self.assertEqual(73000.0, asks[9].price)
        self.assertEqual(20.0, asks[9].amount)

        # Bids should be sorted by price descending
        self.assertEqual(71900.0, bids[0].price)
        self.assertEqual(400.0, bids[0].amount)
        self.assertEqual(71000.0, bids[9].price)
        self.assertEqual(80.0, bids[9].amount)

    # ------------------------------------------------------------------
    # Test: get_new_order_book (integration of snapshot + OrderBook)
    # ------------------------------------------------------------------

    @aioresponses()
    def test_get_new_order_book_successful(self, mock_api):
        """get_new_order_book should return a populated OrderBook instance."""
        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        results = self.local_event_loop.run_until_complete(
            asyncio.gather(self.data_source.get_new_order_book(self.trading_pair))
        )
        order_book: OrderBook = results[0]

        self.assertIsInstance(order_book, OrderBook)

        bids = list(order_book.bid_entries())
        asks = list(order_book.ask_entries())

        self.assertEqual(10, len(bids))
        self.assertEqual(10, len(asks))

        # Verify first bid and ask from the order book
        self.assertEqual(71900.0, bids[0].price)
        self.assertEqual(400.0, bids[0].amount)
        self.assertEqual(72100.0, asks[0].price)
        self.assertEqual(500.0, asks[0].amount)

    # ------------------------------------------------------------------
    # Test: Orderbook with zero-sized / zero-priced levels filtered out
    # ------------------------------------------------------------------

    @aioresponses()
    def test_order_book_snapshot_filters_zero_levels(self, mock_api):
        """REST snapshot should filter out levels with zero price or zero size."""
        mock_response = self._empty_orderbook_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        snapshot_msg: OrderBookMessage = self.local_event_loop.run_until_complete(
            self.data_source._order_book_snapshot(self.trading_pair)
        )

        bids = snapshot_msg.bids
        asks = snapshot_msg.asks

        # askp1 (size 0) and bidp1 (size 0) filtered out; askp2/bidp2 survive on BOTH sides
        self.assertEqual(1, len(asks))
        self.assertEqual(72200.0, asks[0].price)
        self.assertEqual(300.0, asks[0].amount)
        self.assertEqual(1, len(bids))
        self.assertEqual(71800.0, bids[0].price)
        self.assertEqual(600.0, bids[0].amount)

    @aioresponses()
    def test_order_book_snapshot_fails_closed_on_rt_cd_error(self, mock_api):
        """KIS returns HTTP 200 with rt_cd != '0' on logical errors. The REST snapshot
        must raise (fail closed), never publish an empty/garbage book that would silently
        poison the fair-price source (JEP-161, mirrors _get_last_traded_price)."""
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps({"rt_cd": "1", "msg1": "no data", "output1": {}}))
        with self.assertRaisesRegex(IOError, "rt_cd"):
            self.local_event_loop.run_until_complete(
                self.data_source._order_book_snapshot(self.trading_pair)
            )

    @aioresponses()
    def test_order_book_snapshot_fails_closed_on_empty_book(self, mock_api):
        """rt_cd '0' but every level zero/invalid (both sides empty after filter) -> raise:
        an empty book is unusable as a fair source (JEP-161)."""
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(self._all_zero_orderbook_response()))
        with self.assertRaisesRegex(IOError, "one-sided/empty|unusable"):
            self.local_event_loop.run_until_complete(
                self.data_source._order_book_snapshot(self.trading_pair)
            )

    @aioresponses()
    def test_order_book_snapshot_fails_closed_on_one_sided_book(self, mock_api):
        """Valid asks but all bids zero -> bids empty after filter. A one-sided book makes
        best-bid NaN downstream (poisons the two-sided fair / raises InvalidOperation in
        _compute_fair), so it must fail closed, not publish (JEP-161, codex review)."""
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(self._one_sided_orderbook_response()))
        with self.assertRaisesRegex(IOError, "one-sided/empty|unusable"):
            self.local_event_loop.run_until_complete(
                self.data_source._order_book_snapshot(self.trading_pair)
            )

    # ------------------------------------------------------------------
    # Test: _request_order_book_snapshot (raw REST response)
    # ------------------------------------------------------------------

    @aioresponses()
    def test_request_order_book_snapshot(self, mock_api):
        """Should return the raw REST JSON response with output1 dict."""
        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        result = self.local_event_loop.run_until_complete(
            self.data_source._request_order_book_snapshot(self.trading_pair)
        )

        self.assertEqual("0", result["rt_cd"])
        self.assertIn("output1", result)
        self.assertEqual("72100", result["output1"]["askp1"])
        self.assertEqual("500", result["output1"]["askp_rsqn1"])

    @aioresponses()
    def test_request_order_book_snapshot_is_authenticated(self, mock_api):
        """Regression: the REST snapshot must carry KIS auth (Bearer token +
        appkey/appsecret) and the orderbook TR_ID header. Without them KIS returns
        HTTP 500 EGW00304 ("고객식별키 ... appSecret 유효하지 않습니다") and the
        000660 order book never initializes -> KIS stays not-ready.

        (The connector OAuth token is seeded in asyncSetUp.)"""
        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        self.local_event_loop.run_until_complete(
            self.data_source._request_order_book_snapshot(self.trading_pair)
        )

        sent = next(calls[0] for calls in mock_api.requests.values() if calls)
        headers = sent.kwargs["headers"]
        self.assertTrue(headers["Authorization"].startswith("Bearer "))
        self.assertEqual("test_app_key", headers["appkey"])
        self.assertEqual(CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_TR_ID, headers["tr_id"])

    @aioresponses()
    def test_request_order_book_snapshot_market_div_code_follows_routing(self, mock_api):
        """The REST orderbook snapshot must derive FID_COND_MRKT_DIV_CODE from
        kis_market_routing (J=KRX / NX=NXT / UN=통합), NOT hardcode it.

        SOR maps to 'UN' (통합/unified): the EC2 live probe (JEP-180, 2026-06-19)
        confirmed the unified code streams real quotes on inquire-asking-price-exp-ccn
        across the KRX close into the NXT after-market (15:40-20:00 KST). The earlier
        'UN times out' observation (JEP-162, 2026-06-18) did NOT reproduce. 'J' (KRX)
        freezes after the 15:30 KST regular close, so a regression back to 'J' would
        stale the fair during NXT hours — this test pins the outgoing code so that
        regression fails here, not live."""
        expected = {
            CONSTANTS.MARKET_ROUTING_KRX: "J",
            CONSTANTS.MARKET_ROUTING_NXT: "NX",
            CONSTANTS.MARKET_ROUTING_SOR: "UN",
        }
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        for routing, code in expected.items():
            with self.subTest(routing=routing):
                mock_api.requests.clear()
                ds = self._make_ds(routing)
                mock_api.get(regex_url, body=json.dumps(self._order_book_snapshot_response()))
                self.local_event_loop.run_until_complete(
                    ds._request_order_book_snapshot(self.trading_pair)
                )
                sent = next(calls[-1] for calls in mock_api.requests.values() if calls)
                self.assertEqual(code, sent.kwargs["params"]["FID_COND_MRKT_DIV_CODE"])

    # ------------------------------------------------------------------
    # Test: _connected_websocket_assistant raises NotImplementedError
    # ------------------------------------------------------------------

    async def test_connected_websocket_assistant_raises_not_implemented(self):
        """Base-class _connected_websocket_assistant should raise because KIS uses custom WS handling."""
        with self.assertRaises(NotImplementedError):
            await self.data_source._connected_websocket_assistant()

    # ------------------------------------------------------------------
    # Test: _subscribe_channels raises NotImplementedError
    # ------------------------------------------------------------------

    async def test_subscribe_channels_raises_not_implemented(self):
        """Base-class _subscribe_channels should raise because KIS uses custom WS handling."""
        with self.assertRaises(NotImplementedError):
            await self.data_source._subscribe_channels(MagicMock())

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_snapshots reads from WS snapshot queue
    # ------------------------------------------------------------------

    async def test_listen_for_order_book_snapshots_reads_ws_queue(self):
        """listen_for_order_book_snapshots should read WS snapshots from the internal queue."""
        # Simulate WS-delivered snapshot by enqueueing an OrderBookMessage
        snapshot_content = {
            "trading_pair": self.trading_pair,
            "update_id": 1000,
            "bids": [(71900.0, 100.0), (71800.0, 200.0)],
            "asks": [(72100.0, 150.0), (72200.0, 250.0)],
        }
        snapshot_msg = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT, snapshot_content, time.time()
        )
        self.data_source._message_queue[
            self.data_source._snapshot_messages_queue_key
        ].put_nowait(snapshot_msg)

        msg_queue: asyncio.Queue = asyncio.Queue()
        task = asyncio.create_task(
            self.data_source.listen_for_order_book_snapshots(
                self.local_event_loop, msg_queue
            )
        )

        try:
            msg: OrderBookMessage = await asyncio.wait_for(
                msg_queue.get(), timeout=5.0
            )
        except asyncio.TimeoutError:
            self.fail("Timed out waiting for order book snapshot message")
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_snapshots REST fallback on queue timeout
    # ------------------------------------------------------------------

    @aioresponses()
    async def test_listen_for_order_book_snapshots_rest_fallback(self, mock_api):
        """When WS queue times out, should fall back to REST snapshot."""
        # Use short timeout to trigger REST fallback quickly
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 0.1

        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )
        mock_api.get(regex_url, body=json.dumps(mock_response))

        msg_queue: asyncio.Queue = asyncio.Queue()
        task = asyncio.create_task(
            self.data_source.listen_for_order_book_snapshots(
                self.local_event_loop, msg_queue
            )
        )

        try:
            msg: OrderBookMessage = await asyncio.wait_for(
                msg_queue.get(), timeout=5.0
            )
        except asyncio.TimeoutError:
            self.fail("Timed out waiting for REST fallback snapshot")
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

        bids = msg.bids
        asks = msg.asks
        self.assertEqual(10, len(bids))
        self.assertEqual(10, len(asks))
        self.assertEqual(72100.0, asks[0].price)
        self.assertEqual(71900.0, bids[0].price)

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_snapshots handles errors gracefully
    # ------------------------------------------------------------------

    async def test_listen_for_order_book_snapshots_logs_exception(self):
        """When REST fetch fails, should log error and continue."""
        # Use short timeout to trigger REST fallback, which then raises
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 0.1

        with patch.object(
            self.data_source,
            "_order_book_snapshot",
            new_callable=AsyncMock,
        ) as mock_snapshot:
            mock_snapshot.side_effect = [Exception("Network error"), asyncio.CancelledError()]

            with patch.object(self.data_source, "_sleep", new_callable=AsyncMock) as sleep_mock:
                sleep_mock.return_value = None

                msg_queue: asyncio.Queue = asyncio.Queue()

                try:
                    await self.data_source.listen_for_order_book_snapshots(
                        self.local_event_loop, msg_queue
                    )
                except asyncio.CancelledError:
                    pass

                self.assertTrue(
                    self._is_logged_partial(
                        "ERROR",
                        "Unexpected error fetching order book snapshot",
                    )
                )

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_snapshots raises CancelledError
    # ------------------------------------------------------------------

    async def test_listen_for_order_book_snapshots_raises_cancelled_error(self):
        """CancelledError should propagate up cleanly."""
        # Use short timeout to trigger REST fallback, which then raises CancelledError
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 0.1

        with patch.object(
            self.data_source,
            "_order_book_snapshot",
            new_callable=AsyncMock,
        ) as mock_snapshot:
            mock_snapshot.side_effect = asyncio.CancelledError()

            msg_queue: asyncio.Queue = asyncio.Queue()

            with self.assertRaises(asyncio.CancelledError):
                await self.data_source.listen_for_order_book_snapshots(
                    self.local_event_loop, msg_queue
                )

    # ------------------------------------------------------------------
    # Test: _extract_levels correctly parses KIS dict-based orderbook
    # ------------------------------------------------------------------

    def test_extract_levels_from_dict(self):
        """Template-based level extraction should parse KIS dict format correctly."""
        data = {
            "askp1": "100", "askp_rsqn1": "10",
            "askp2": "101", "askp_rsqn2": "20",
            "askp3": "0", "askp_rsqn3": "30",  # zero price -> skip
            "bidp1": "99", "bidp_rsqn1": "15",
            "bidp2": "98", "bidp_rsqn2": "0",  # zero size -> skip
            "bidp3": "97", "bidp_rsqn3": "25",
        }

        asks, bids = KisAPIOrderBookDataSource._extract_levels_from_dict(
            data=data,
            depth=3,
            ask_price_templates=["askp{idx}"],
            ask_size_templates=["askp_rsqn{idx}"],
            bid_price_templates=["bidp{idx}"],
            bid_size_templates=["bidp_rsqn{idx}"],
        )

        # askp3 has price=0 so it's filtered; 2 valid ask levels
        self.assertEqual(2, len(asks))
        self.assertEqual(100.0, asks[0][0])
        self.assertEqual(10.0, asks[0][1])
        self.assertEqual(101.0, asks[1][0])
        self.assertEqual(20.0, asks[1][1])

        # bidp2 has size=0 so it's filtered; 2 valid bid levels
        self.assertEqual(2, len(bids))
        self.assertEqual(99.0, bids[0][0])
        self.assertEqual(15.0, bids[0][1])
        self.assertEqual(97.0, bids[1][0])
        self.assertEqual(25.0, bids[1][1])

    # ------------------------------------------------------------------
    # Test: _extract_levels handles empty strings and missing keys
    # ------------------------------------------------------------------

    def test_extract_levels_handles_empty_and_missing(self):
        """Empty string values and missing keys should be treated as None and filtered."""
        data = {
            "askp1": "", "askp_rsqn1": "10",
            "bidp1": "99",
            # bidp_rsqn1 is missing entirely
        }

        asks, bids = KisAPIOrderBookDataSource._extract_levels_from_dict(
            data=data,
            depth=1,
            ask_price_templates=["askp{idx}"],
            ask_size_templates=["askp_rsqn{idx}"],
            bid_price_templates=["bidp{idx}"],
            bid_size_templates=["bidp_rsqn{idx}"],
        )

        # askp1 is empty string -> None -> skipped
        self.assertEqual(0, len(asks))
        # bidp_rsqn1 missing -> None -> skipped
        self.assertEqual(0, len(bids))

    # ------------------------------------------------------------------
    # Test: Polling interval attribute
    # ------------------------------------------------------------------

    def test_default_polling_interval(self):
        """Base class should have a default message queue polling delay."""
        # WS-based data source no longer has ORDER_BOOK_SNAPSHOT_DELAY
        # but the base class has MESSAGE_TIMEOUT which governs queue reads
        self.assertTrue(hasattr(self.data_source, "_message_queue"))

    # ==================================================================
    # WebSocket Tests
    # ==================================================================

    # ------------------------------------------------------------------
    # Test: WebSocket endpoint URL (nautilus-proven, no /tryitout path)
    # ------------------------------------------------------------------

    def test_ws_url_has_no_tryitout_path(self):
        """The live KIS realtime WS endpoint is the bare host:port with no path.

        The ``/tryitout`` suffix is the testbed web-form path and triggers a
        ServerDisconnectedError during the handshake. The nautilus_trader
        live-tested adapter connects to ``ws://ops.koreainvestment.com:21000``.
        """
        prod_url = web_utils.ws_url("")
        sandbox_url = web_utils.ws_url("sandbox")

        self.assertNotIn("/tryitout", prod_url)
        self.assertNotIn("/tryitout", sandbox_url)
        self.assertEqual("ws://ops.koreainvestment.com:21000", prod_url)
        self.assertEqual("ws://ops.koreainvestment.com:31000", sandbox_url)
        self.assertEqual("ws://ops.koreainvestment.com:21000", CONSTANTS.WS_URL)
        self.assertEqual("ws://ops.koreainvestment.com:31000", CONSTANTS.WS_SANDBOX_URL)

    # ------------------------------------------------------------------
    # Test: _parse_caret_fields
    # ------------------------------------------------------------------

    def test_parse_caret_fields_valid(self):
        """Should parse caret-separated fields into a dict using column names."""
        columns = ("COL_A", "COL_B", "COL_C")
        data_str = "val_a^val_b^val_c"

        result = KisAPIOrderBookDataSource._parse_caret_fields(data_str, columns)

        self.assertIsNotNone(result)
        self.assertEqual("val_a", result["COL_A"])
        self.assertEqual("val_b", result["COL_B"])
        self.assertEqual("val_c", result["COL_C"])

    def test_parse_caret_fields_extra_fields_ignored(self):
        """Extra fields beyond the column count should be ignored."""
        columns = ("COL_A", "COL_B")
        data_str = "val_a^val_b^extra_c^extra_d"

        result = KisAPIOrderBookDataSource._parse_caret_fields(data_str, columns)

        self.assertIsNotNone(result)
        self.assertEqual(2, len(result))
        self.assertEqual("val_a", result["COL_A"])
        self.assertEqual("val_b", result["COL_B"])

    def test_parse_caret_fields_too_few_fields_returns_none(self):
        """When data has fewer fields than columns, should return None."""
        columns = ("COL_A", "COL_B", "COL_C")
        data_str = "val_a^val_b"

        result = KisAPIOrderBookDataSource._parse_caret_fields(data_str, columns)

        self.assertIsNone(result)

    def test_parse_caret_fields_empty_string(self):
        """An empty data string should return None when columns are expected."""
        columns = ("COL_A",)
        data_str = ""

        result = KisAPIOrderBookDataSource._parse_caret_fields(data_str, columns)

        # Empty string split by "^" yields [""], which has len 1
        # With 1 column expected, this should succeed
        self.assertIsNotNone(result)
        self.assertEqual("", result["COL_A"])

    def test_parse_caret_fields_with_orderbook_columns(self):
        """Should parse a real H0STASP0 data string using WS_ORDERBOOK_COLUMNS."""
        # Build a minimal data string with the right number of fields
        fields = ["005930", "093000", "0"]  # MKSC_SHRN_ISCD, BSOP_HOUR, HOUR_CLS_CODE
        # 10 ask prices
        fields.extend(["67800", "67900", "68000", "68100", "68200",
                        "68300", "68400", "68500", "68600", "68700"])
        # 10 bid prices
        fields.extend(["67700", "67600", "67500", "67400", "67300",
                        "67200", "67100", "67000", "66900", "66800"])
        # 10 ask sizes
        fields.extend(["1000", "2000", "3000", "4000", "5000",
                        "6000", "7000", "8000", "9000", "10000"])
        # 10 bid sizes
        fields.extend(["10000", "9000", "8000", "7000", "6000",
                        "5000", "4000", "3000", "2000", "1000"])
        # Remaining fields (TOTAL_ASKP_RSQN etc.)
        fields.extend(["55000", "55000", "0", "0", "67750", "50",
                        "100", "50", "2", "0.5", "1000000", "100",
                        "-100", "0", "0", "1"])

        data_str = "^".join(fields)
        result = KisAPIOrderBookDataSource._parse_caret_fields(
            data_str, CONSTANTS.WS_ORDERBOOK_COLUMNS
        )

        self.assertIsNotNone(result)
        self.assertEqual("005930", result["MKSC_SHRN_ISCD"])
        self.assertEqual("67800", result["ASKP1"])
        self.assertEqual("67700", result["BIDP1"])
        self.assertEqual("1000", result["ASKP_RSQN1"])
        self.assertEqual("10000", result["BIDP_RSQN1"])
        self.assertEqual("68700", result["ASKP10"])
        self.assertEqual("10000", result["ASKP_RSQN10"])

    # ------------------------------------------------------------------
    # Test: _process_orderbook_data
    # ------------------------------------------------------------------

    async def test_process_orderbook_data_creates_snapshot_message(self):
        """Should create an OrderBookMessage SNAPSHOT with sorted asks and bids."""
        # Parse the WS orderbook data string
        raw = self._ws_orderbook_raw()
        parts = raw.split("|")
        data_str = parts[3]
        parsed = KisAPIOrderBookDataSource._parse_caret_fields(
            data_str, CONSTANTS.WS_ORDERBOOK_COLUMNS
        )
        self.assertIsNotNone(parsed)

        await self.data_source._process_orderbook_data("005930", parsed)

        # Check that a message was enqueued
        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertFalse(queue.empty())

        msg: OrderBookMessage = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

        bids = msg.bids
        asks = msg.asks

        # Should have 10 ask levels and 10 bid levels
        self.assertEqual(10, len(asks))
        self.assertEqual(10, len(bids))

        # Asks should be sorted ascending by price
        ask_prices = [a.price for a in asks]
        self.assertEqual(sorted(ask_prices), ask_prices)
        self.assertEqual(67800.0, asks[0].price)
        self.assertEqual(1000.0, asks[0].amount)
        self.assertEqual(68700.0, asks[9].price)
        self.assertEqual(10000.0, asks[9].amount)

        # Bids should be sorted descending by price
        bid_prices = [b.price for b in bids]
        self.assertEqual(sorted(bid_prices, reverse=True), bid_prices)
        self.assertEqual(67700.0, bids[0].price)
        self.assertEqual(10000.0, bids[0].amount)
        self.assertEqual(66800.0, bids[9].price)
        self.assertEqual(1000.0, bids[9].amount)

    async def test_process_orderbook_data_filters_zero_levels(self):
        """Levels with zero price or zero size should be excluded from the snapshot."""
        data = {col: "0" for col in CONSTANTS.WS_ORDERBOOK_COLUMNS}
        data["MKSC_SHRN_ISCD"] = "005930"
        # Only set one valid ask and one valid bid
        data["ASKP1"] = "68000"
        data["ASKP_RSQN1"] = "500"
        data["BIDP1"] = "67900"
        data["BIDP_RSQN1"] = "300"
        # ASKP2 has zero price (already 0), BIDP2 has zero size (already 0)

        await self.data_source._process_orderbook_data("005930", data)

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertEqual(1, len(msg.asks))
        self.assertEqual(1, len(msg.bids))
        self.assertEqual(68000.0, msg.asks[0].price)
        self.assertEqual(67900.0, msg.bids[0].price)

    async def test_ws_orderbook_frame_stamps_freshness(self):
        self.assertIsNone(self.data_source.last_ws_orderbook_time(self.trading_pair))
        raw = self._ws_orderbook_raw()
        parsed = KisAPIOrderBookDataSource._parse_caret_fields(
            raw.split("|")[3], CONSTANTS.WS_ORDERBOOK_COLUMNS
        )

        await self.data_source._process_orderbook_data("005930", parsed)

        self.assertIsNotNone(self.data_source.last_ws_orderbook_time(self.trading_pair))

    async def test_empty_book_does_not_stamp(self):
        empty = {k: "0" for k in CONSTANTS.WS_ORDERBOOK_COLUMNS}

        await self.data_source._process_orderbook_data("005930", empty)

        q = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertFalse(q.empty())
        self.assertIsNone(self.data_source.last_ws_orderbook_time(self.trading_pair))

    async def test_identical_top_of_book_does_not_reset_book_change(self):
        raw = self._ws_orderbook_raw()
        parsed = KisAPIOrderBookDataSource._parse_caret_fields(
            raw.split("|")[3], CONSTANTS.WS_ORDERBOOK_COLUMNS
        )
        with patch("time.perf_counter", side_effect=[100.0, 100.0, 110.0, 110.0, 120.0]):
            await self.data_source._process_orderbook_data("005930", parsed)
            await self.data_source._process_orderbook_data("005930", parsed)
            sig = self.connector.get_session_halt_signals(self.trading_pair)
        self.assertGreaterEqual(sig.book_static_sec, 9.0)
        self.assertFalse(sig.hour_cls_auction)

    async def test_moved_top_of_book_resets_book_change(self):
        raw = self._ws_orderbook_raw()
        base = KisAPIOrderBookDataSource._parse_caret_fields(
            raw.split("|")[3], CONSTANTS.WS_ORDERBOOK_COLUMNS
        )
        a = _reprice(base, best_ask="70100", best_bid="70000")
        b = _reprice(base, best_ask="70200", best_bid="70100")
        with patch("time.perf_counter", side_effect=[100.0, 100.0, 110.0, 110.0, 110.0]):
            await self.data_source._process_orderbook_data("005930", a)
            await self.data_source._process_orderbook_data("005930", b)
            sig = self.connector.get_session_halt_signals(self.trading_pair)
        self.assertLessEqual(sig.book_static_sec, 0.001)

    async def test_depth_change_with_static_top_resets_book_change(self):
        # JEP-198/202: the best bid/ask can hold static for many seconds in a quiet
        # market while deeper levels keep moving. book_static must track the full
        # depth (the data source feeds the whole book), so a deeper-level change on
        # an unchanged top still proves the feed is live -> no false book_frozen.
        raw = self._ws_orderbook_raw()
        base = KisAPIOrderBookDataSource._parse_caret_fields(
            raw.split("|")[3], CONSTANTS.WS_ORDERBOOK_COLUMNS
        )
        a = dict(base)
        b = dict(base)
        # Top of book unchanged; only a deeper bid-level size moves (BIDP_RSQN2).
        b["BIDP_RSQN2"] = str(int(float(base["BIDP_RSQN2"])) + 100)
        with patch("time.perf_counter", side_effect=[100.0, 100.0, 110.0, 110.0, 110.0]):
            await self.data_source._process_orderbook_data("005930", a)
            await self.data_source._process_orderbook_data("005930", b)
            sig = self.connector.get_session_halt_signals(self.trading_pair)
        self.assertLessEqual(sig.book_static_sec, 0.001)

    async def test_hour_cls_code_auction_flag(self):
        raw = self._ws_orderbook_raw(hour_cls_code="C")
        parsed = KisAPIOrderBookDataSource._parse_caret_fields(
            raw.split("|")[3], CONSTANTS.WS_ORDERBOOK_COLUMNS
        )
        self.connector._set_trading_pair_symbol_map(bidict({"005930": self.trading_pair}))

        await self.data_source._process_orderbook_data("005930", parsed)

        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).hour_cls_auction)

    async def test_wrong_pair_frame_does_not_clear_auction_flag(self):
        raw = self._ws_orderbook_raw(hour_cls_code="C")
        parsed = KisAPIOrderBookDataSource._parse_caret_fields(
            raw.split("|")[3], CONSTANTS.WS_ORDERBOOK_COLUMNS
        )
        self.connector._set_trading_pair_symbol_map(bidict({"005930": self.trading_pair}))
        await self.data_source._process_orderbook_data("005930", parsed)
        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).hour_cls_auction)

        wrong_pair = dict(parsed)
        wrong_pair["HOUR_CLS_CODE"] = "0"
        await self.data_source._process_orderbook_data("999999", wrong_pair)

        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).hour_cls_auction)

    async def test_control_message_subscription_error_does_not_stamp(self):
        await self.data_source._handle_control_message(self._ws_subscription_error_raw())

        self.assertIsNone(self.data_source.last_ws_orderbook_time(self.trading_pair))

    async def test_process_orderbook_data_unknown_symbol_uses_fallback(self):
        """When the stock code is not in the symbol map, should fall back to first trading pair."""
        data = {col: "0" for col in CONSTANTS.WS_ORDERBOOK_COLUMNS}
        data["ASKP1"] = "100"
        data["ASKP_RSQN1"] = "10"

        await self.data_source._process_orderbook_data("999999", data)

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()
        # Falls back to the first trading pair
        self.assertEqual(self.trading_pair, msg.trading_pair)

    async def test_unknown_two_sided_orderbook_frame_enqueues_but_does_not_stamp_freshness(self):
        data = {col: "0" for col in CONSTANTS.WS_ORDERBOOK_COLUMNS}
        data["ASKP1"] = "100"
        data["ASKP_RSQN1"] = "10"
        data["BIDP1"] = "99"
        data["BIDP_RSQN1"] = "11"

        await self.data_source._process_orderbook_data("999999", data)

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertFalse(queue.empty())
        msg: OrderBookMessage = queue.get_nowait()
        self.assertEqual(self.trading_pair, msg.trading_pair)
        for trading_pair in self.data_source._trading_pairs:
            self.assertIsNone(self.data_source.last_ws_orderbook_time(trading_pair))

    # ------------------------------------------------------------------
    # Test: _process_trade_data
    # ------------------------------------------------------------------

    async def test_process_trade_data_creates_trade_message(self):
        """Should create an OrderBookMessage TRADE with correct price, amount, and type."""
        raw = self._ws_trade_raw()
        parts = raw.split("|")
        data_str = parts[3]
        parsed = KisAPIOrderBookDataSource._parse_caret_fields(
            data_str, CONSTANTS.WS_TRADE_COLUMNS
        )
        self.assertIsNotNone(parsed)

        await self.data_source._process_trade_data("005930", parsed)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        self.assertFalse(queue.empty())

        msg: OrderBookMessage = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

        content = msg.content
        self.assertEqual(67800.0, content["price"])
        self.assertEqual(100.0, content["amount"])

    async def test_process_trade_data_sell_type(self):
        """CCLD_DVSN=1 should map to TradeType.SELL."""
        data = {col: "0" for col in CONSTANTS.WS_TRADE_COLUMNS}
        data["MKSC_SHRN_ISCD"] = "005930"
        data["STCK_PRPR"] = "50000"
        data["CNTG_VOL"] = "200"
        data["CCLD_DVSN"] = "1"  # sell

        await self.data_source._process_trade_data("005930", data)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertEqual(float(TradeType.SELL.value), msg.content["trade_type"])
        self.assertEqual(50000.0, msg.content["price"])
        self.assertEqual(200.0, msg.content["amount"])

    async def test_process_trade_data_buy_type(self):
        """CCLD_DVSN=2 should map to TradeType.BUY."""
        data = {col: "0" for col in CONSTANTS.WS_TRADE_COLUMNS}
        data["MKSC_SHRN_ISCD"] = "005930"
        data["STCK_PRPR"] = "50000"
        data["CNTG_VOL"] = "100"
        data["CCLD_DVSN"] = "2"  # buy

        await self.data_source._process_trade_data("005930", data)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertEqual(float(TradeType.BUY.value), msg.content["trade_type"])

    async def test_process_trade_data_unknown_dvsn_defaults_to_buy(self):
        """An unknown CCLD_DVSN value should default to TradeType.BUY."""
        data = {col: "0" for col in CONSTANTS.WS_TRADE_COLUMNS}
        data["MKSC_SHRN_ISCD"] = "005930"
        data["STCK_PRPR"] = "50000"
        data["CNTG_VOL"] = "100"
        data["CCLD_DVSN"] = "9"  # unknown

        await self.data_source._process_trade_data("005930", data)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertEqual(float(TradeType.BUY.value), msg.content["trade_type"])

    # ------------------------------------------------------------------
    # Test: _handle_control_message
    # ------------------------------------------------------------------

    async def test_handle_control_message_subscription_success(self):
        """Successful subscription response (rt_cd=0) should not log a warning."""
        raw = self._ws_subscription_success_raw()

        await self.data_source._handle_control_message(raw)

        # No warning logged for success
        self.assertFalse(self._is_logged_partial("WARNING", "subscription error"))

    async def test_handle_control_message_subscription_error(self):
        """Failed subscription response (rt_cd!=0) should log a warning."""
        raw = self._ws_subscription_error_raw()

        await self.data_source._handle_control_message(raw)

        self.assertTrue(self._is_logged_partial("WARNING", "KIS WS subscription error"))

    async def test_handle_control_message_invalid_json(self):
        """Invalid JSON should log a warning and not raise."""
        await self.data_source._handle_control_message("not valid json {{{")

        self.assertTrue(self._is_logged_partial("WARNING", "Invalid JSON from KIS WS"))

    # ------------------------------------------------------------------
    # Test: _on_ws_frame routing
    # ------------------------------------------------------------------

    async def test_on_ws_frame_routes_data_to_handle_data_message(self):
        """Messages starting with '0' or '1' should be routed to _handle_data_message."""
        raw = self._ws_orderbook_raw()

        with patch.object(
            self.data_source, "_handle_data_message", new_callable=AsyncMock
        ) as mock_handle:
            await self.data_source._on_ws_frame(raw)
            mock_handle.assert_awaited_once_with(raw)

    async def test_on_ws_frame_routes_json_to_handle_control_message(self):
        """Messages not starting with '0'/'1' should be routed to _handle_control_message."""
        raw = self._ws_subscription_success_raw()

        with patch.object(
            self.data_source, "_handle_control_message", new_callable=AsyncMock
        ) as mock_handle:
            await self.data_source._on_ws_frame(raw)
            mock_handle.assert_awaited_once_with(raw)

    async def test_on_ws_frame_routes_orderbook_data(self):
        await self.data_source._on_ws_frame(self._ws_orderbook_raw())

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertFalse(queue.empty())

    async def test_jep134_stamp_preserved(self):
        marked = []
        self.data_source._mark_ws_orderbook_frame = lambda trading_pair: marked.append(trading_pair)
        self.connector._trading_pair_symbol_map = {"005930": self.trading_pair}

        await self.data_source._on_ws_frame(self._ws_orderbook_raw())

        self.assertEqual([self.trading_pair], marked)

    # ------------------------------------------------------------------
    # Test: _handle_data_message
    # ------------------------------------------------------------------

    async def test_handle_data_message_orderbook(self):
        """H0STASP0 data message should be parsed and processed as orderbook."""
        raw = self._ws_orderbook_raw()

        await self.data_source._handle_data_message(raw)

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertFalse(queue.empty())
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)

    async def test_handle_data_message_trade(self):
        """H0STCNT0 data message should be parsed and processed as trade."""
        raw = self._ws_trade_raw()

        await self.data_source._handle_data_message(raw)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        self.assertFalse(queue.empty())
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, msg.type)

    # ------------------------------------------------------------------
    # JEP-202: real KIS frames carry the record count ("데이터건수") in parts[2],
    # NOT the stock code. The stock code is the first caret field (MKSC_SHRN_ISCD).
    # Keying freshness/resolution off parts[2] permanently failed the JEP-134 stamp
    # guard (age=None) and misattributed frames to _trading_pairs[0] under multi-pair.
    # The shared _ws_*_raw() fixtures happen to put the stock code in parts[2], which
    # masked the bug; these tests use the realistic wire layout instead.
    # ------------------------------------------------------------------

    async def test_jep202_orderbook_freshness_stamped_from_caret_symbol(self):
        """A healthy WS orderbook frame must stamp JEP-134 freshness off MKSC_SHRN_ISCD,
        even though parts[2] is the record count ('001'), not the stock code."""
        # Realistic frame: parts[2] = record count "001"; stock code lives in the caret data.
        raw = "0|H0UNASP0|001|" + self._ws_orderbook_raw().split("|", 3)[3]

        self.assertIsNone(self.data_source.last_ws_orderbook_time(self.trading_pair))
        await self.data_source._handle_data_message(raw)

        self.assertIsNotNone(
            self.data_source.last_ws_orderbook_time(self.trading_pair),
            "JEP-134 freshness must be stamped from MKSC_SHRN_ISCD, not the parts[2] "
            "record count — otherwise a healthy feed is judged stale forever.",
        )

    async def test_jep202_orderbook_attributed_to_caret_symbol_not_fallback(self):
        """Under multiple pairs, the orderbook snapshot must be attributed to the frame's
        own MKSC_SHRN_ISCD, not the _trading_pairs[0] fallback."""
        self.data_source._trading_pairs = ["000660-KRW", self.trading_pair]
        self.connector._set_trading_pair_symbol_map(
            bidict({"000660": "000660-KRW", "005930": self.trading_pair})
        )
        raw = "0|H0UNASP0|001|" + self._ws_orderbook_raw().split("|", 3)[3]

        await self.data_source._handle_data_message(raw)

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertFalse(queue.empty())
        msg = queue.get_nowait()
        self.assertEqual(self.trading_pair, msg.content["trading_pair"])

    async def test_jep202_trade_attributed_to_caret_symbol_not_fallback(self):
        """Under multiple pairs, the trade message must be attributed to the frame's own
        MKSC_SHRN_ISCD, not the _trading_pairs[0] fallback."""
        self.data_source._trading_pairs = ["000660-KRW", self.trading_pair]
        self.connector._set_trading_pair_symbol_map(
            bidict({"000660": "000660-KRW", "005930": self.trading_pair})
        )
        raw = "0|H0UNCNT0|001|" + self._ws_trade_raw().split("|", 3)[3]

        await self.data_source._handle_data_message(raw)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        self.assertFalse(queue.empty())
        msg = queue.get_nowait()
        self.assertEqual(self.trading_pair, msg.content["trading_pair"])

    async def test_handle_data_message_invalid_format(self):
        """A data message with fewer than 4 pipe-delimited parts should log a warning."""
        await self.data_source._handle_data_message("0|H0STASP0")

        self.assertTrue(self._is_logged_partial("WARNING", "Invalid KIS WS data message"))

    async def test_handle_data_message_unknown_tr_id(self):
        """An unknown TR_ID should be silently ignored (no queue entries, no error)."""
        raw = "0|UNKNOWN_TR|005930|field1^field2"

        await self.data_source._handle_data_message(raw)

        ob_queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        trade_queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        self.assertTrue(ob_queue.empty())
        self.assertTrue(trade_queue.empty())

    # ------------------------------------------------------------------
    # Test: SOR/NXT routing-aware subscription + dispatch
    # ------------------------------------------------------------------

    def _make_ds(self, market_routing, market_status_enabled: bool = False,
                 market_status_capture_only: bool = False):
        return KisAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            auth=self.mock_auth,
            hub=MagicMock(),
            domain=CONSTANTS.DEFAULT_DOMAIN,
            market_routing=market_routing,
            market_status_enabled=market_status_enabled,
            market_status_capture_only=market_status_capture_only,
        )

    def _make_two_pair_market_status_ds(self):
        second_pair = "000660-KRW"
        connector = KisExchange(
            kis_app_key="test_app_key",
            kis_app_secret="test_app_secret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair, second_pair],
            trading_required=False,
            kis_market_routing=CONSTANTS.MARKET_ROUTING_KRX,
            kis_market_status_enabled="true",
        )
        connector._auth._access_token = "test_token"
        connector._auth._token_expires_at = time.time() + 86400
        connector._set_trading_pair_symbol_map(bidict({
            "005930": self.trading_pair,
            "000660": second_pair,
        }))
        ds = KisAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair, second_pair],
            connector=connector,
            api_factory=connector._web_assistants_factory,
            auth=self.mock_auth,
            hub=MagicMock(),
            domain=CONSTANTS.DEFAULT_DOMAIN,
            market_routing=CONSTANTS.MARKET_ROUTING_KRX,
            market_status_enabled=True,
        )
        ds.logger().setLevel(1)
        ds.logger().addHandler(self)
        return connector, ds, second_pair

    def _ws_orderbook_payload(self) -> str:
        """Caret body of an orderbook message (without the 0|TR_ID|TR_KEY| prefix)."""
        return self._ws_orderbook_raw().split("|", 3)[3]

    async def test_handle_data_message_unified_orderbook_dispatched(self):
        ds = self._make_ds("sor")
        raw = "0|H0UNASP0|005930|" + self._ws_orderbook_payload()
        with patch.object(ds, "_process_orderbook_data", new_callable=AsyncMock) as m:
            await ds._handle_data_message(raw)
            m.assert_awaited_once()

    async def test_handle_data_message_krx_ignored_under_sor(self):
        ds = self._make_ds("sor")
        ds.logger().setLevel(1)
        ds.logger().addHandler(self)
        raw = "0|H0STASP0|005930|" + self._ws_orderbook_payload()
        with patch.object(ds, "_process_orderbook_data", new_callable=AsyncMock) as m:
            await ds._handle_data_message(raw)
            m.assert_not_awaited()
        # Channel drift (KRX TR_ID under SOR) must be surfaced, not silently dropped
        self.assertTrue(self._is_logged_partial("WARNING", "Dropped KIS market-data TR_ID"))

    async def test_handle_data_message_nxt_orderbook_dispatched(self):
        ds = self._make_ds("nxt")
        raw = "0|H0NXASP0|005930|" + self._ws_orderbook_payload()
        with patch.object(ds, "_process_orderbook_data", new_callable=AsyncMock) as m:
            await ds._handle_data_message(raw)
            m.assert_awaited_once()

    async def test_trht_yn_latches_halt(self):
        ds = self._make_ds("sor", market_status_enabled=True)
        raw = "0|H0UNMKO0|005930|" + self._ws_market_status_payload(TRHT_YN="Y")

        await ds._handle_data_message(raw)

        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).trht_halted)

    async def test_cb_set_and_clear(self):
        ds = self._make_ds("sor", market_status_enabled=True)

        await ds._handle_data_message(
            "0|H0UNMKO0|005930|" + self._ws_market_status_payload(MKOP_CLS_CODE="174")
        )
        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).cb_latched)

        await ds._handle_data_message(
            "0|H0UNMKO0|005930|" + self._ws_market_status_payload(MKOP_CLS_CODE="175")
        )
        self.assertFalse(self.connector.get_session_halt_signals(self.trading_pair).cb_latched)

        await ds._handle_data_message(
            "0|H0UNMKO0|005930|" + self._ws_market_status_payload(MKOP_CLS_CODE="184")
        )
        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).cb_latched)

        await ds._handle_data_message(
            "0|H0UNMKO0|005930|" + self._ws_market_status_payload(MKOP_CLS_CODE="185")
        )
        self.assertFalse(self.connector.get_session_halt_signals(self.trading_pair).cb_latched)

    async def test_unknown_mkop_code_fail_closed(self):
        ds = self._make_ds("sor", market_status_enabled=True)
        self.connector.logger().setLevel(1)
        self.connector.logger().addHandler(self)

        await ds._handle_data_message(
            "0|H0UNMKO0|005930|" + self._ws_market_status_payload(MKOP_CLS_CODE="174")
        )
        await ds._handle_data_message(
            "0|H0UNMKO0|005930|" + self._ws_market_status_payload(MKOP_CLS_CODE="999")
        )

        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).cb_latched)
        self.assertTrue(self._is_logged_partial("WARNING", "unknown MKOP_CLS_CODE"))

    async def test_cb_latch_retained_after_reconnect(self):
        ds = self._make_ds("sor", market_status_enabled=True)

        await ds._handle_data_message(
            "0|H0UNMKO0|005930|" + self._ws_market_status_payload(MKOP_CLS_CODE="174")
        )
        ds_reconnected = self._make_ds("sor", market_status_enabled=True)

        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).cb_latched)
        self.assertIsNot(ds, ds_reconnected)

    async def test_short_h0unmko0_frame_does_not_flip_latch(self):
        ds = self._make_ds("sor", market_status_enabled=True)
        raw_halted = "0|H0UNMKO0|005930|" + self._ws_market_status_payload(TRHT_YN="Y")
        raw_short = "0|H0UNMKO0|005930|005930^N"

        await ds._handle_data_message(raw_halted)
        await ds._handle_data_message(raw_short)

        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).trht_halted)

    async def test_market_status_ready_per_pair(self):
        ds = self._make_ds("sor", market_status_enabled=True)
        self.connector._market_status_enabled = True
        second_pair = "000660-KRW"
        self.connector._set_trading_pair_symbol_map(bidict({
            "005930": self.trading_pair,
            "000660": second_pair,
        }))

        await ds._handle_control_message(self._ws_market_status_subscription_success_raw())

        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).market_status_ready)
        self.assertFalse(self.connector.get_session_halt_signals(second_pair).market_status_ready)

    async def test_market_status_ack_unresolvable_tr_key_does_not_confirm_first_pair(self):
        connector, ds, _ = self._make_two_pair_market_status_ds()

        await ds._handle_control_message(
            self._ws_market_status_subscription_success_raw(tr_id="H0STMKO0", tr_key="GARBAGE")
        )

        self.assertNotIn(self.trading_pair, connector._sh_market_status_confirmed)
        self.assertFalse(connector.get_session_halt_signals(self.trading_pair).market_status_ready)

    async def test_market_status_data_unresolvable_tr_key_does_not_mutate_latch(self):
        connector, ds, _ = self._make_two_pair_market_status_ds()
        halted = "0|H0STMKO0|005930|" + self._ws_market_status_payload(
            TRHT_YN="Y",
            MKOP_CLS_CODE="174",
        )
        unresolved_clear = "0|H0STMKO0|GARBAGE|" + self._ws_market_status_payload(
            TRHT_YN="N",
            MKOP_CLS_CODE="175",
        )

        await ds._handle_data_message(halted)
        await ds._handle_data_message(unresolved_clear)

        signals = connector.get_session_halt_signals(self.trading_pair)
        self.assertTrue(signals.trht_halted)
        self.assertTrue(signals.cb_latched)

    async def test_none_field_does_not_clear_latch(self):
        self.connector.note_market_status(
            self.trading_pair,
            trht_yn=None,
            mkop_cls_code="174",
            vi_cls_code=None,
            ovtm_vi_cls_code=None,
        )

        self.connector.note_market_status(
            self.trading_pair,
            trht_yn=None,
            mkop_cls_code=None,
            vi_cls_code=None,
            ovtm_vi_cls_code=None,
        )

        self.assertTrue(self.connector.get_session_halt_signals(self.trading_pair).cb_latched)

    # ------------------------------------------------------------------
    # Test: _resolve_trading_pair
    # ------------------------------------------------------------------

    async def test_resolve_trading_pair_from_symbol_map(self):
        """Known stock code in symbol map should resolve to the correct trading pair."""
        result = await self.data_source._resolve_trading_pair("005930")
        self.assertEqual(self.trading_pair, result)

    async def test_resolve_trading_pair_fallback(self):
        """Unknown stock code should fall back to the first trading pair."""
        result = await self.data_source._resolve_trading_pair("UNKNOWN_CODE")
        self.assertEqual(self.trading_pair, result)

    async def test_resolve_trading_pair_no_pairs(self):
        """With no trading pairs configured, should return None."""
        self.data_source._trading_pairs = []
        # Also clear the symbol map
        self.connector._set_trading_pair_symbol_map(bidict())

        result = await self.data_source._resolve_trading_pair("005930")
        self.assertIsNone(result)

    # ------------------------------------------------------------------
    # Test: listen_for_subscriptions (WS loop with mocking)
    # ------------------------------------------------------------------

    async def test_listen_registers_market_trs_with_hub(self):
        hub = MagicMock()
        hub.register = AsyncMock()
        hub.unregister = AsyncMock()
        self.data_source._hub = hub
        with patch.object(
            self.connector,
            "exchange_symbol_associated_to_pair",
            new=AsyncMock(return_value="005930"),
        ):
            task = asyncio.create_task(self.data_source.listen_for_subscriptions())
            await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        reg_trs = {c.args[0] for c in hub.register.await_args_list}
        self.assertEqual({"H0UNASP0", "H0UNCNT0"}, reg_trs)
        unreg_trs = {c.args[0] for c in hub.unregister.await_args_list}
        self.assertEqual({"H0UNASP0", "H0UNCNT0"}, unreg_trs)

    async def test_listen_two_symbols_one_hub_no_dispatch_collision(self):
        """JEP-207 regression. Two symbols on ONE shared connector => ONE orderbook DS whose
        listen loop registers the SAME orderbook tr_id (H0UNASP0) once per symbol, passing
        ``self._on_ws_frame`` (a bound method = a FRESH object on each access). Against a REAL
        hub the old identity guard rejected the 2nd symbol as a 'different handler', the KIS
        hedge orderbook WS never went fresh, and the JEP-134 staleness kill-switch halted the
        live 2-symbol run. With the equality guard both symbols subscribe on the one hub.
        The other listen tests use a MagicMock hub, so they never exercised the real guard —
        this one deliberately uses the real KisWsHub, which is why the bug escaped CI."""
        from hummingbot.connector.exchange.kis.kis_ws_hub import KisWsHub

        hub = KisWsHub(auth=self.mock_auth, domain=CONSTANTS.DEFAULT_DOMAIN,
                       ws_enabled=True, sleep=AsyncMock())
        hub._ensure_running = lambda: None  # unit test: never spawn the real connect loop

        ds = KisAPIOrderBookDataSource(
            trading_pairs=["005930-KRW", "000660-KRW"],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            auth=self.mock_auth,
            hub=hub,
            domain=CONSTANTS.DEFAULT_DOMAIN,
            market_routing=CONSTANTS.MARKET_ROUTING_SOR,
        )
        sym_map = {"005930-KRW": "005930", "000660-KRW": "000660"}
        with patch.object(
            self.connector,
            "exchange_symbol_associated_to_pair",
            new=AsyncMock(side_effect=lambda trading_pair: sym_map[trading_pair]),
        ):
            task = asyncio.create_task(ds.listen_for_subscriptions())
            # settle until the last registration lands (or the task errors out on collision)
            for _ in range(50):
                if task.done() or ("H0UNCNT0", "000660") in hub._subs:
                    break
                await asyncio.sleep(0)
            if task.done() and task.exception() is not None:
                raise AssertionError(
                    "2-symbol listen raised instead of registering cleanly (JEP-207 collision)"
                ) from task.exception()
            # assert WHILE the listen task is parked on the idle wait (post-register,
            # pre-teardown); the finally-block unregisters everything on cancel.
            self.assertIn(("H0UNASP0", "005930"), hub._subs)
            self.assertIn(("H0UNASP0", "000660"), hub._subs)   # 2nd symbol — the collision case
            self.assertIn(("H0UNCNT0", "005930"), hub._subs)
            self.assertIn(("H0UNCNT0", "000660"), hub._subs)
            # one handler per tr_id (it demuxes both symbols by tr_key), equal to the DS entry
            self.assertEqual(hub._dispatch["H0UNASP0"], ds._on_ws_frame)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_listen_two_symbols_market_status_registers_without_collision(self):
        """JEP-207 review (Finding 1 coverage): with market status on, H0UNMKO0 is ALSO
        registered once per symbol and would hit the same identity collision. Prove the fix
        covers all THREE doubly-registered tr_ids (orderbook/trade/market-status). NB: the
        H0UNMKO0 DATA path still has the separate parts[2] record-count hazard owned by
        JEP-198/201 — out of scope here; this only asserts collision-free registration."""
        from hummingbot.connector.exchange.kis.kis_ws_hub import KisWsHub

        hub = KisWsHub(auth=self.mock_auth, domain=CONSTANTS.DEFAULT_DOMAIN,
                       ws_enabled=True, sleep=AsyncMock())
        hub._ensure_running = lambda: None
        ds = KisAPIOrderBookDataSource(
            trading_pairs=["005930-KRW", "000660-KRW"],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            auth=self.mock_auth,
            hub=hub,
            domain=CONSTANTS.DEFAULT_DOMAIN,
            market_routing=CONSTANTS.MARKET_ROUTING_SOR,
            market_status_capture_only=True,
        )
        sym_map = {"005930-KRW": "005930", "000660-KRW": "000660"}
        with patch.object(
            self.connector,
            "exchange_symbol_associated_to_pair",
            new=AsyncMock(side_effect=lambda trading_pair: sym_map[trading_pair]),
        ):
            task = asyncio.create_task(ds.listen_for_subscriptions())
            for _ in range(60):
                if task.done() or ("H0UNMKO0", "000660") in hub._subs:
                    break
                await asyncio.sleep(0)
            if task.done() and task.exception() is not None:
                raise AssertionError(
                    "market-status 2-symbol listen collided (JEP-207)"
                ) from task.exception()
            self.assertIn(("H0UNMKO0", "005930"), hub._subs)
            self.assertIn(("H0UNMKO0", "000660"), hub._subs)   # 3rd tr_id, 2nd symbol
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def test_listen_registers_market_status_tr_when_enabled(self):
        hub = MagicMock()
        hub.register = AsyncMock()
        hub.unregister = AsyncMock()
        ds = self._make_ds("sor", market_status_enabled=True)
        ds._hub = hub
        self.connector.mark_market_status_confirmed(self.trading_pair)
        with patch.object(
            self.connector,
            "exchange_symbol_associated_to_pair",
            new=AsyncMock(return_value="005930"),
        ):
            task = asyncio.create_task(ds.listen_for_subscriptions())
            await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        reg_trs = {c.args[0] for c in hub.register.await_args_list}
        self.assertEqual({"H0UNASP0", "H0UNCNT0", "H0UNMKO0"}, reg_trs)
        unreg_trs = {c.args[0] for c in hub.unregister.await_args_list}
        self.assertEqual({"H0UNASP0", "H0UNCNT0", "H0UNMKO0"}, unreg_trs)
        self.assertNotIn(self.trading_pair, self.connector._sh_market_status_confirmed)

    async def test_listen_registers_market_status_tr_when_capture_only(self):
        # JEP-201 capture-only: subscribe H0?MKO0 to harvest frames, WITHOUT feeding the latch.
        hub = MagicMock()
        hub.register = AsyncMock()
        hub.unregister = AsyncMock()
        ds = self._make_ds("sor", market_status_capture_only=True)
        ds._hub = hub
        with patch.object(
            self.connector,
            "exchange_symbol_associated_to_pair",
            new=AsyncMock(return_value="005930"),
        ):
            task = asyncio.create_task(ds.listen_for_subscriptions())
            await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        reg_trs = {c.args[0] for c in hub.register.await_args_list}
        self.assertEqual({"H0UNASP0", "H0UNCNT0", "H0UNMKO0"}, reg_trs)   # subscribed for capture
        # capture-only must NOT confirm the latch (Phase-1 status stays independent)
        self.assertNotIn(self.trading_pair, self.connector._sh_market_status_confirmed)

    async def test_capture_only_logs_frame_without_latching(self):
        ds = self._make_ds("sor", market_status_capture_only=True)
        raw = "0|H0UNMKO0|005930|" + self._ws_market_status_payload(TRHT_YN="Y", MKOP_CLS_CODE="174")
        with patch.object(ds, "_process_market_status_data", new_callable=AsyncMock) as m:
            await ds._handle_data_message(raw)
            m.assert_not_awaited()   # capture-only NEVER feeds the latch path
        # the connector latch is untouched (cb stays clear despite a CB-code frame)
        self.assertFalse(self.connector.get_session_halt_signals(self.trading_pair).cb_latched)

    async def test_capture_only_ack_does_not_confirm_latch(self):
        ds = self._make_ds("sor", market_status_capture_only=True)
        await ds._handle_control_message(
            self._ws_market_status_subscription_success_raw(tr_id="H0UNMKO0", tr_key="005930")
        )
        self.assertNotIn(self.trading_pair, self.connector._sh_market_status_confirmed)

    async def test_stale_listen_finally_skips_unregister_after_restart(self):
        hub = MagicMock()
        hub.register = AsyncMock()
        hub.unregister = AsyncMock()
        self.data_source._hub = hub
        with patch.object(
            self.connector,
            "exchange_symbol_associated_to_pair",
            new=AsyncMock(return_value="005930"),
        ):
            task_a = asyncio.create_task(self.data_source.listen_for_subscriptions())
            await asyncio.sleep(0)
            task_b = asyncio.create_task(self.data_source.listen_for_subscriptions())
            await asyncio.sleep(0)
            task_a.cancel()
            try:
                await task_a
            except asyncio.CancelledError:
                pass
            unreg_after_stale = hub.unregister.await_count
            task_b.cancel()
            try:
                await task_b
            except asyncio.CancelledError:
                pass
        self.assertEqual(0, unreg_after_stale)
        self.assertEqual({"H0UNASP0", "H0UNCNT0"}, {c.args[0] for c in hub.unregister.await_args_list})

    async def test_listen_for_subscriptions_propagates_cancelled_error(self):
        """CancelledError during hub-backed listen should propagate cleanly."""
        self.data_source._hub.register = AsyncMock(side_effect=asyncio.CancelledError())
        self.data_source._hub.unregister = AsyncMock()
        with patch.object(self.connector, "exchange_symbol_associated_to_pair", new=AsyncMock(return_value="005930")):
            with self.assertRaises(asyncio.CancelledError):
                await self.data_source.listen_for_subscriptions()
        self.assertEqual({"H0UNASP0", "H0UNCNT0"}, {c.args[0] for c in self.data_source._hub.unregister.await_args_list})

    # ------------------------------------------------------------------
    # Test: listen_for_trades uses WS trade queue
    # ------------------------------------------------------------------

    async def test_listen_for_trades_reads_from_trade_queue(self):
        """listen_for_trades should read from the _trade_messages_queue_key queue."""
        # Enqueue a trade message
        raw = self._ws_trade_raw()
        parts = raw.split("|")
        data_str = parts[3]
        parsed = KisAPIOrderBookDataSource._parse_caret_fields(
            data_str, CONSTANTS.WS_TRADE_COLUMNS
        )
        await self.data_source._process_trade_data("005930", parsed)

        output_queue: asyncio.Queue = asyncio.Queue()

        # listen_for_trades reads from internal queue and puts into output
        task = asyncio.create_task(
            self.data_source.listen_for_trades(self.local_event_loop, output_queue)
        )

        try:
            msg = await asyncio.wait_for(output_queue.get(), timeout=5.0)
        except asyncio.TimeoutError:
            self.fail("Timed out waiting for trade message")
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.assertEqual(OrderBookMessageType.TRADE, msg.type)
        self.assertEqual(self.trading_pair, msg.trading_pair)

    # ------------------------------------------------------------------
    # Test: listen_for_order_book_diffs reads from diff queue (no-op for KIS)
    # ------------------------------------------------------------------

    async def test_listen_for_order_book_diffs_reads_from_diff_queue(self):
        """listen_for_order_book_diffs reads from the diff queue; KIS sends no diffs so queue stays empty."""
        output_queue: asyncio.Queue = asyncio.Queue()

        task = asyncio.create_task(
            self.data_source.listen_for_order_book_diffs(self.local_event_loop, output_queue)
        )

        # Give it a brief time; no messages should appear since KIS sends no diffs
        await asyncio.sleep(0.1)
        self.assertTrue(output_queue.empty())

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------------
    # Test: Constructor stores auth and domain
    # ------------------------------------------------------------------

    def test_constructor_stores_auth_and_domain(self):
        """Constructor should store the auth and domain parameters."""
        self.assertIs(self.mock_auth, self.data_source._auth)
        self.assertEqual(CONSTANTS.DEFAULT_DOMAIN, self.data_source._domain)

    def test_constructor_with_custom_domain(self):
        """Constructor should accept a custom domain (e.g., sandbox)."""
        ds = KisAPIOrderBookDataSource(
            trading_pairs=[self.trading_pair],
            connector=self.connector,
            api_factory=self.connector._web_assistants_factory,
            auth=self.mock_auth,
            hub=MagicMock(),
            domain="sandbox",
        )
        self.assertEqual("sandbox", ds._domain)

    # ------------------------------------------------------------------
    # Test: WS orderbook message timestamp and update_id
    # ------------------------------------------------------------------

    async def test_process_orderbook_data_has_valid_timestamp_and_update_id(self):
        """The orderbook snapshot message should have a timestamp close to now and a valid update_id."""
        data = {col: "0" for col in CONSTANTS.WS_ORDERBOOK_COLUMNS}
        data["ASKP1"] = "100"
        data["ASKP_RSQN1"] = "10"

        before = time.time()
        await self.data_source._process_orderbook_data("005930", data)
        after = time.time()

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertGreaterEqual(msg.timestamp, before)
        self.assertLessEqual(msg.timestamp, after)
        self.assertGreater(msg.update_id, 0)

    # ------------------------------------------------------------------
    # Test: WS trade message has valid trade_id
    # ------------------------------------------------------------------

    async def test_process_trade_data_has_valid_trade_id(self):
        """The trade message should have a positive trade_id based on timestamp."""
        data = {col: "0" for col in CONSTANTS.WS_TRADE_COLUMNS}
        data["STCK_PRPR"] = "50000"
        data["CNTG_VOL"] = "100"
        data["CCLD_DVSN"] = "2"

        await self.data_source._process_trade_data("005930", data)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        msg: OrderBookMessage = queue.get_nowait()

        self.assertGreater(msg.content["trade_id"], 0)

    # ------------------------------------------------------------------
    # Test: _process_orderbook_data with unresolvable trading pair
    # ------------------------------------------------------------------

    async def test_process_orderbook_data_no_trading_pair_does_nothing(self):
        """When _resolve_trading_pair returns None, no message should be enqueued."""
        self.data_source._trading_pairs = []
        self.connector._set_trading_pair_symbol_map(bidict())

        data = {col: "0" for col in CONSTANTS.WS_ORDERBOOK_COLUMNS}
        data["ASKP1"] = "100"
        data["ASKP_RSQN1"] = "10"

        await self.data_source._process_orderbook_data("NOSUCH", data)

        queue = self.data_source._message_queue[self.data_source._snapshot_messages_queue_key]
        self.assertTrue(queue.empty())

    # ------------------------------------------------------------------
    # Test: _process_trade_data with unresolvable trading pair
    # ------------------------------------------------------------------

    async def test_process_trade_data_no_trading_pair_does_nothing(self):
        """When _resolve_trading_pair returns None, no message should be enqueued."""
        self.data_source._trading_pairs = []
        self.connector._set_trading_pair_symbol_map(bidict())

        data = {col: "0" for col in CONSTANTS.WS_TRADE_COLUMNS}
        data["STCK_PRPR"] = "50000"
        data["CNTG_VOL"] = "100"

        await self.data_source._process_trade_data("NOSUCH", data)

        queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        self.assertTrue(queue.empty())

    # ------------------------------------------------------------------
    # Test: JEP-217 out-of-session snapshot gate
    # ------------------------------------------------------------------

    async def test_request_snapshots_skipped_out_of_session(self):
        """Out-of-session: _request_order_book_snapshots must NOT call super()
        (no REST fetch, so the empty-book IOError path is never entered)."""
        self.data_source._session_open_fn = lambda: False
        with patch(
            "hummingbot.core.data_type.order_book_tracker_data_source."
            "OrderBookTrackerDataSource._request_order_book_snapshots",
            new_callable=AsyncMock,
        ) as mock_super:
            await self.data_source._request_order_book_snapshots(asyncio.Queue())
        mock_super.assert_not_called()

    async def test_request_snapshots_runs_in_session(self):
        """In-session: _request_order_book_snapshots delegates to super()."""
        self.data_source._session_open_fn = lambda: True
        with patch(
            "hummingbot.core.data_type.order_book_tracker_data_source."
            "OrderBookTrackerDataSource._request_order_book_snapshots",
            new_callable=AsyncMock,
        ) as mock_super:
            q = asyncio.Queue()
            await self.data_source._request_order_book_snapshots(q)
        mock_super.assert_awaited_once()

    def test_session_open_fn_defaults_to_real_window(self):
        """With no injection, _is_session_open delegates to in_session_window."""
        self.data_source._session_open_fn = None  # undo the setUp default
        with patch(
            "hummingbot.connector.exchange.kis.kis_api_order_book_data_source."
            "in_session_window",
            return_value=True,
        ) as mock_win:
            self.assertTrue(self.data_source._is_session_open())
        mock_win.assert_called_once()

    # ------------------------------------------------------------------
    # Test: JEP-217 boundary-survival repro (internal skip->resume path)
    # ------------------------------------------------------------------

    async def test_loop_survives_open_boundary_and_resumes(self):
        """JEP-217 repro: out-of-session the loop publishes nothing (no empty-book
        churn); when the session flips open the SAME listen loop resumes fetching
        and publishes — i.e. it survived the boundary without re-supervision."""
        self.data_source.FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 0.05

        # Session starts CLOSED, flips OPEN after a few poll cycles.
        state = {"open": False}
        self.data_source._session_open_fn = lambda: state["open"]

        mock_response = self._order_book_snapshot_response()
        regex_url = re.compile(
            f"{CONSTANTS.REST_URL}/{CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH}"
        )

        msg_queue: asyncio.Queue = asyncio.Queue()
        with aioresponses() as mock_api:
            mock_api.get(regex_url, body=json.dumps(mock_response), repeat=True)
            task = asyncio.create_task(
                self.data_source.listen_for_order_book_snapshots(
                    self.local_event_loop, msg_queue
                )
            )
            try:
                # While closed: nothing should be published.
                await asyncio.sleep(0.2)  # ~4 poll cycles, all skipped
                self.assertTrue(msg_queue.empty(), "published a snapshot while out-of-session")

                # Flip to open; the SAME loop must resume and publish.
                state["open"] = True
                msg = await asyncio.wait_for(msg_queue.get(), timeout=5.0)
                self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)
                self.assertEqual(self.trading_pair, msg.trading_pair)
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
