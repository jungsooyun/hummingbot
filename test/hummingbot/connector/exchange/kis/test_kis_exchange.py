import json
import re
import time
from decimal import Decimal
from typing import Any, Callable, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

from aioresponses import aioresponses
from aioresponses.core import RequestCall
from bidict import bidict

from hummingbot.connector.exchange.kis import kis_constants as CONSTANTS, kis_web_utils as web_utils
from hummingbot.connector.exchange.kis.kis_exchange import KisExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.network_iterator import NetworkStatus


class KisExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):
    """
    Tests for the KIS (Korea Investment & Securities) exchange connector.

    KIS-specific characteristics:
    - No symbols list API (trading pairs configured externally)
    - TR_ID header-based request routing
    - OAuth2 Bearer token authentication (REST) + approval_key (WebSocket)
    - WebSocket support for orderbook/trades (KisAPIOrderBookDataSource)
      and execution notifications (KisAPIUserStreamDataSource)
    - Balances remain REST-polled (no WS balance updates)
    - Response format: {"rt_cd": "0", "msg1": "...", "output": {...}}
    - Trading pair format: stock code (e.g. "COINALPHA") not a pair symbol
    """

    @property
    def all_symbols_url(self):
        # KIS has no symbols list API. We use the ticker endpoint as a proxy
        # so the abstract test framework has a URL to mock.
        return web_utils.public_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_TICKER_PATH)

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_TICKER_PATH)
        # create_exchange_instance() uses the default 'sor' routing -> unified 'UN' market-div code
        # (routing-aware ticker; hardcoded 'J' froze the last price after the KRX close — JEP-148).
        mrkt_div = CONSTANTS.REST_QUOTE_MRKT_DIV_BY_ROUTING[CONSTANTS.MARKET_ROUTING_SOR]
        url = f"{url}?FID_COND_MRKT_DIV_CODE={mrkt_div}&FID_INPUT_ISCD={self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}"
        return url

    @property
    def network_status_url(self):
        # The health probe uses the authenticated balance inquiry (NOT oauth2/tokenP,
        # which is POST-only and hangs on GET -> see _make_network_check_request).
        # Regex (path + optional query) so the framework mock matches the request's
        # account/TR query string.
        base = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_BALANCE_PATH)
        return re.compile(f"^{re.escape(base)}.*")

    @property
    def trading_rules_url(self):
        # KIS has no trading rules endpoint. Reuse the symbols/ticker endpoint.
        return web_utils.public_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_TICKER_PATH)

    @property
    def order_creation_url(self):
        return web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_PATH)

    @property
    def balance_url(self):
        return web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_BALANCE_PATH)

    @property
    def all_symbols_request_mock_response(self):
        # KIS has no symbols list API. This synthetic response is returned by
        # the exchange connector to satisfy the abstract test framework.
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "symbols": [
                    {
                        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "base_currency": self.base_asset,
                        "quote_currency": self.quote_asset,
                        "name": "CoinAlpha Inc",
                        "market": "J",
                        "status": "trading",
                    }
                ]
            }
        }

    @property
    def latest_prices_request_mock_response(self):
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "stck_prpr": str(int(self.expected_latest_price)),
                "stck_hgpr": "73000",
                "stck_lwpr": "71000",
                "acml_vol": "12345678",
                "bidp1": "71900",
                "askp1": "72100",
            }
        }

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "symbols": [
                    {
                        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "base_currency": self.base_asset,
                        "quote_currency": self.quote_asset,
                        "name": "CoinAlpha Inc",
                        "market": "J",
                        "status": "trading",
                    },
                    {
                        "symbol": self.exchange_symbol_for_tokens("INVALID", "PAIR"),
                        "base_currency": "INVALID",
                        "quote_currency": "PAIR",
                        "name": "Invalid Corp",
                        "market": "J",
                        "status": "suspended",
                    },
                ]
            }
        }
        return "INVALID-PAIR", response

    @property
    def network_status_request_successful_mock_response(self):
        # Balance inquiry success shape — the network probe is an authenticated
        # balance GET (24/7, reuses the cached OAuth token), not a tokenP GET.
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "정상처리 되었습니다.",
            "output1": [],
            "output2": [],
        }

    @property
    def trading_rules_request_mock_response(self):
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "symbols": [
                    {
                        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "base_currency": self.base_asset,
                        "quote_currency": self.quote_asset,
                        "name": "CoinAlpha Inc",
                        "market": "J",
                        "status": "trading",
                        "min_order_size": "1",
                        "price_unit": "100",
                        "lot_size": "1",
                    }
                ]
            }
        }

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "symbols": [
                    {
                        "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                        "name": "CoinAlpha Inc",
                        "market": "J",
                        "status": "trading",
                        # Deliberately missing: min_order_size, price_unit, lot_size
                    }
                ]
            }
        }

    @property
    def order_creation_request_successful_mock_response(self):
        return {
            "rt_cd": "0",
            "msg_cd": "APBK0013",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "ODNO": str(self.expected_exchange_order_id),
                "ORD_TMD": "131500",
            }
        }

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output1": [
                {
                    "pdno": self.base_asset,
                    "prdt_name": "CoinAlpha Inc",
                    "hldg_qty": "15",
                    "ord_psbl_qty": "10",
                    "pchs_avg_pric": "70000.00",
                    "prpr": "72000",
                    "evlu_amt": "1080000",
                }
            ],
            "output2": [
                {
                    "dnca_tot_amt": "2000",
                    "nxdy_excc_amt": "2000",
                    "tot_evlu_amt": "3080000",
                }
            ]
        }

    @property
    def balance_request_mock_response_only_base(self):
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output1": [
                {
                    "pdno": self.base_asset,
                    "prdt_name": "CoinAlpha Inc",
                    "hldg_qty": "15",
                    "ord_psbl_qty": "10",
                    "pchs_avg_pric": "70000.00",
                    "prpr": "72000",
                    "evlu_amt": "1080000",
                }
            ],
            "output2": [
                {
                    "dnca_tot_amt": "0",
                    "nxdy_excc_amt": "0",
                    "tot_evlu_amt": "1080000",
                }
            ]
        }

    @property
    def balance_event_websocket_update(self):
        # KIS does not provide balance updates through WebSocket;
        # balances are REST-polled via _update_balances().
        raise NotImplementedError("KIS does not support WebSocket balance updates")

    @property
    def expected_latest_price(self):
        return 72000.0

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    @property
    def expected_trading_rule(self):
        # KIS has no trading-rules API; rules are synthesised per configured pair
        # (whole-share size, integer-KRW price increment). See
        # KisExchange._update_trading_rules.
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("1"),
            min_price_increment=Decimal("1"),
            min_base_amount_increment=Decimal("1"),
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response["output"]["symbols"][0]
        return f"Error parsing the trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return "0000123456"

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        # KIS WS execution notifications include fill data inline;
        # no separate HTTP fill fetch is needed during WS event processing.
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal("72000")

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("50")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        # KIS inquire-daily-ccld returns no per-fill commission, so the connector derives the
        # fill fee from the fee schema (percent, no flat fees) via new_spot_fee. Mirror that
        # exact construction here (framework fill tests use BUY orders) so the assertion checks
        # the real behavior instead of a flat fee KIS never reports.
        return TradeFeeBase.new_spot_fee(
            fee_schema=self.exchange.trade_fee_schema(),
            trade_type=TradeType.BUY,
            percent_token=self.quote_asset,
            flat_fees=[],
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return "1234567890"

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        # KIS uses stock code directly (e.g. "COINALPHA"), not a pair like "COINALPHA_KRW"
        return base_token

    def create_exchange_instance(self):
        exchange = KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
        )
        # Inject a cached OAuth2 access token so framework auth tests don't hit
        # the (unmocked) KIS token endpoint. Production code fetches a real token
        # lazily — this mirrors that state only for the test connector instance.
        exchange._auth._access_token = "test_access_token"
        exchange._auth._token_expires_at = time.time() + 86400
        return exchange

    def test_both_data_sources_share_one_hub(self):
        ob = self.exchange._create_order_book_data_source()
        us = self.exchange._create_user_stream_data_source()

        self.assertIs(ob._hub, us._hub)
        self.assertIs(ob._hub, self.exchange.ws_hub)

    async def test_stop_network_stops_hub_once(self):
        hub = MagicMock()
        hub.stop = AsyncMock()
        self.exchange._ws_hub = hub

        await self.exchange.stop_network()

        hub.stop.assert_awaited_once()

    async def test_ws_enabled_false_hub_never_connects(self):
        conn = KisExchange(
            kis_app_key="k",
            kis_app_secret="s",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            trading_required=False,
            kis_ws_enabled="false",
        )

        self.assertFalse(conn.ws_hub._ws_enabled)
        await conn.ws_hub.register("H0UNASP0", "005930", AsyncMock())
        self.assertIsNone(conn.ws_hub._run_task)

    def test_session_halt_signals_cold_start_has_no_book_ages(self):
        sig = self.exchange.get_session_halt_signals(self.trading_pair)

        self.assertFalse(sig.hour_cls_auction)
        self.assertIsNone(sig.book_age_sec)
        self.assertIsNone(sig.book_static_sec)
        self.assertFalse(sig.trht_halted)
        self.assertFalse(sig.cb_latched)
        self.assertFalse(sig.vi_latched)
        self.assertTrue(sig.market_status_ready)

    def test_book_static_resets_on_any_depth_change_even_if_top_unchanged(self):
        # JEP-198/202: a quiet but live market holds the best bid/ask static for
        # many seconds (the touch can persist for 30+ minutes) while deeper levels
        # and sizes keep churning. "book static" must track the FULL depth, not the
        # top alone, or the session-halt gate false-trips book_frozen on a healthy feed.
        from unittest.mock import patch
        tp = self.trading_pair
        bids_a = [(100.0, 5.0), (99.0, 3.0)]
        asks_a = [(101.0, 4.0), (102.0, 6.0)]
        # Top of book identical (100.0 / 101.0); only a deeper bid size moves 3.0 -> 9.0.
        bids_b = [(100.0, 5.0), (99.0, 9.0)]
        asks_b = [(101.0, 4.0), (102.0, 6.0)]
        with patch("time.perf_counter", side_effect=[100.0, 200.0]):
            self.exchange.note_book_snapshot(tp, bids_a, asks_a)
            self.assertEqual(100.0, self.exchange._sh_last_book_change[tp])
            self.exchange.note_book_snapshot(tp, bids_b, asks_b)
            self.assertEqual(200.0, self.exchange._sh_last_book_change[tp])

    def test_book_static_does_not_reset_when_full_book_identical(self):
        # A genuinely frozen book (every level identical across frames) must NOT
        # reset book_static, so the session-halt gate can still detect a real freeze.
        from unittest.mock import patch
        tp = self.trading_pair
        bids = [(100.0, 5.0), (99.0, 3.0)]
        asks = [(101.0, 4.0), (102.0, 6.0)]
        with patch("time.perf_counter", side_effect=[100.0, 999.0]):
            self.exchange.note_book_snapshot(tp, bids, asks)
            self.assertEqual(100.0, self.exchange._sh_last_book_change[tp])
            # Identical full book -> last_change MUST NOT advance.
            self.exchange.note_book_snapshot(tp, list(bids), list(asks))
            self.assertEqual(100.0, self.exchange._sh_last_book_change[tp])

    @aioresponses()
    async def test_all_trading_pairs_does_not_raise_exception(self, mock_api):
        # Override the base test: KIS has no symbols-list API, so trading pairs
        # come from configuration rather than the network. all_trading_pairs()
        # therefore never depends on (or is broken by) a failed request — it
        # returns the externally configured pairs and never raises.
        self.exchange._set_trading_pair_symbol_map(None)

        result: List[str] = await self.exchange.all_trading_pairs()

        self.assertEqual([self.trading_pair], result)

    async def test_update_trading_rules_synthesised_network_free(self):
        # KIS has no trading-rules API: rules are synthesised for the configured
        # pairs with no HTTP request. Regression for the live-startup hang where
        # the ticker-based fetch was sent unauthenticated (EGW00304) and returned
        # no symbol list, so trading rules never initialised and the connector
        # never reached "ready".
        exchange = KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=["000660-KRW", "005930-KRW"],
        )

        await exchange._update_trading_rules()

        self.assertEqual({"000660-KRW", "005930-KRW"}, set(exchange.trading_rules.keys()))
        rule = exchange.trading_rules["000660-KRW"]
        self.assertEqual(Decimal("1"), rule.min_order_size)
        self.assertEqual(Decimal("1"), rule.min_price_increment)
        self.assertEqual(Decimal("1"), rule.min_base_amount_increment)

    async def test_symbol_map_built_from_configured_pairs_network_free(self):
        # The symbol map is derived from the configured pairs (exchange symbol =
        # the numeric stock code, the base of "<code>-KRW"), with no network call.
        exchange = KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=["000660-KRW"],
        )

        await exchange._initialize_trading_pair_symbol_map()

        self.assertEqual("000660", await exchange.exchange_symbol_associated_to_pair("000660-KRW"))
        self.assertEqual("000660-KRW", await exchange.trading_pair_associated_to_exchange_symbol("000660"))

    # ------------------------------------------------------------------
    # SOR/NXT routing — constructor wiring
    # ------------------------------------------------------------------

    def _make_exchange(self, kis_market_routing="sor", domain=CONSTANTS.DEFAULT_DOMAIN, kis_ws_enabled="true"):
        return KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            kis_market_routing=kis_market_routing,
            kis_ws_enabled=kis_ws_enabled,
            domain=domain,
        )

    def _open_orders_response(self, rows, nk100="", tr_cont=""):
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "ok",
            "ctx_area_fk100": "",
            "ctx_area_nk100": nk100,
            "tr_cont": tr_cont,
            "output1": rows,
        }

    @aioresponses()
    async def test_get_open_orders_returns_normalized_records(self, mock_api):
        symbol = self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
        ex = self._make_exchange()
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        ex._set_trading_pair_symbol_map(bidict({symbol: self.trading_pair}))
        rows = [
            {"odno": "K-buy", "pdno": symbol, "sll_buy_dvsn_cd": "02",
             "ord_unpr": "70000", "ord_qty": "10", "tot_ccld_qty": "0", "rmn_qty": "10", "cncl_yn": "N"},
            {"odno": "K-sell", "pdno": symbol, "sll_buy_dvsn_cd": "01",
             "ord_unpr": "72000", "ord_qty": "5", "tot_ccld_qty": "3", "rmn_qty": "2", "cncl_yn": "N"},
        ]
        url = re.compile(f"^{re.escape(web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH))}.*")
        mock_api.get(url, body=json.dumps(self._open_orders_response(rows)))
        records = await ex.get_open_orders()
        self.assertEqual(len(records), 2)
        buy = next(r for r in records if r.exchange_order_id == "K-buy")
        sell = next(r for r in records if r.exchange_order_id == "K-sell")
        self.assertEqual(buy.trade_type, TradeType.BUY)
        self.assertEqual(buy.trading_pair, self.trading_pair)
        self.assertEqual(buy.price, Decimal("70000"))
        self.assertEqual(buy.amount, Decimal("10"))
        self.assertEqual(buy.remaining_amount, Decimal("10"))
        self.assertEqual(sell.trade_type, TradeType.SELL)
        self.assertEqual(sell.remaining_amount, Decimal("2"))

    @aioresponses()
    async def test_get_open_orders_remaining_ignores_non_ccld_ord_psbl_qty(self, mock_api):
        """rmn_qty (잔여수량) is the inquire-daily-ccld remaining field — verified against the
        Intrect-io/kis-agent community client (responses/order.py:113) + its fixture and KIS docs
        (TR TTTC8001R). ord_psbl_qty is NOT a field of this endpoint (it belongs to balance/
        orderable-qty endpoints), so when rmn_qty is absent the remaining must fall back to
        ord_qty - tot_ccld_qty, never an orderable-qty field that happens to be on the row."""
        symbol = self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
        ex = self._make_exchange()
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        ex._set_trading_pair_symbol_map(bidict({symbol: self.trading_pair}))
        rows = [
            # No rmn_qty; a bogus ord_psbl_qty must be ignored (it is not a daily-ccld field).
            {"odno": "no-rmn", "pdno": symbol, "sll_buy_dvsn_cd": "02",
             "ord_unpr": "1", "ord_qty": "10", "tot_ccld_qty": "3", "ord_psbl_qty": "99", "cncl_yn": "N"},
        ]
        url = re.compile(f"^{re.escape(web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH))}.*")
        mock_api.get(url, body=json.dumps(self._open_orders_response(rows)))
        records = await ex.get_open_orders()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].remaining_amount, Decimal("7"))  # ord_qty - ccld_qty, not 99

    @aioresponses()
    async def test_get_open_orders_excludes_terminal_rows(self, mock_api):
        symbol = self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
        ex = self._make_exchange()
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        ex._set_trading_pair_symbol_map(bidict({symbol: self.trading_pair}))
        rows = [
            {"odno": "resting", "pdno": symbol, "sll_buy_dvsn_cd": "02",
             "ord_unpr": "1", "ord_qty": "10", "tot_ccld_qty": "4", "rmn_qty": "6", "cncl_yn": "N"},
            {"odno": "filled", "pdno": symbol, "sll_buy_dvsn_cd": "02",
             "ord_unpr": "1", "ord_qty": "10", "tot_ccld_qty": "10", "rmn_qty": "0", "cncl_yn": "N"},
            {"odno": "cancelled", "pdno": symbol, "sll_buy_dvsn_cd": "01",
             "ord_unpr": "1", "ord_qty": "5", "tot_ccld_qty": "0", "rmn_qty": "5", "cncl_yn": "Y"},
        ]
        url = re.compile(f"^{re.escape(web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH))}.*")
        mock_api.get(url, body=json.dumps(self._open_orders_response(rows)))
        records = await ex.get_open_orders()
        self.assertEqual({r.exchange_order_id for r in records}, {"resting"})

    @aioresponses()
    async def test_get_open_orders_empty_returns_empty_list(self, mock_api):
        ex = self._make_exchange()
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        url = re.compile(f"^{re.escape(web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH))}.*")
        mock_api.get(url, body=json.dumps(self._open_orders_response([])))
        self.assertEqual(await ex.get_open_orders(), [])

    @aioresponses()
    async def test_get_open_orders_external_order_has_no_client_id(self, mock_api):
        symbol = self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
        ex = self._make_exchange()
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        ex._set_trading_pair_symbol_map(bidict({symbol: self.trading_pair}))
        rows = [{"odno": "untracked", "pdno": symbol, "sll_buy_dvsn_cd": "02",
                 "ord_unpr": "1", "ord_qty": "1", "tot_ccld_qty": "0", "rmn_qty": "1", "cncl_yn": "N"}]
        url = re.compile(f"^{re.escape(web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH))}.*")
        mock_api.get(url, body=json.dumps(self._open_orders_response(rows)))
        records = await ex.get_open_orders()
        self.assertIsNone(records[0].client_order_id)

    @aioresponses()
    async def test_get_open_orders_hits_correct_endpoint_and_auth(self, mock_api):
        ex = self._make_exchange()
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        url = re.compile(f"^{re.escape(web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH))}.*")
        mock_api.get(url, body=json.dumps(self._open_orders_response([])))
        await ex.get_open_orders()
        sent = next(calls[-1] for calls in mock_api.requests.values() if calls)
        matched = [k for k in mock_api.requests if CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH in str(k[1])]
        self.assertTrue(matched, f"expected inquire-daily-ccld URL, saw {list(mock_api.requests)}")
        self._assert_request_authenticated(sent, CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_TR_ID)

    @aioresponses()
    async def test_get_open_orders_paginates_continuation(self, mock_api):
        symbol = self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
        ex = self._make_exchange()
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        ex._set_trading_pair_symbol_map(bidict({symbol: self.trading_pair}))
        page1 = self._open_orders_response(
            [{"odno": "p1", "pdno": symbol, "sll_buy_dvsn_cd": "02",
              "ord_unpr": "1", "ord_qty": "1", "tot_ccld_qty": "0", "rmn_qty": "1", "cncl_yn": "N"}],
            nk100="NEXT", tr_cont="M")
        page2 = self._open_orders_response(
            [{"odno": "p2", "pdno": symbol, "sll_buy_dvsn_cd": "01",
              "ord_unpr": "2", "ord_qty": "1", "tot_ccld_qty": "0", "rmn_qty": "1", "cncl_yn": "N"}],
            nk100="", tr_cont="")
        url = re.compile(f"^{re.escape(web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH))}.*")
        mock_api.get(url, body=json.dumps(page1))
        mock_api.get(url, body=json.dumps(page2))
        records = await ex.get_open_orders()
        self.assertEqual({r.exchange_order_id for r in records}, {"p1", "p2"})

    @aioresponses()
    async def test_get_open_orders_routes_per_kis_market_routing(self, mock_api):
        symbol = self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
        ex = self._make_exchange(kis_market_routing="sor")
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        ex._set_trading_pair_symbol_map(bidict({symbol: self.trading_pair}))
        rows = [{"odno": "sor-1", "pdno": symbol, "sll_buy_dvsn_cd": "02",
                 "ord_unpr": "1", "ord_qty": "1", "tot_ccld_qty": "0", "rmn_qty": "1", "cncl_yn": "N"}]
        url = re.compile(f"^{re.escape(web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH))}.*")
        mock_api.get(url, body=json.dumps(self._open_orders_response(rows)))
        records = await ex.get_open_orders()
        self.assertEqual(len(records), 1)
        sent = next(calls[-1] for calls in mock_api.requests.values() if calls)
        self.assertEqual(sent.kwargs["params"]["EXCG_ID_DVSN_CD"], ex._excg_for_routing())

    @aioresponses()
    async def test_get_last_traded_price_market_div_code_follows_routing(self, mock_api):
        """The last-traded-price (ticker) REST call must derive FID_COND_MRKT_DIV_CODE
        from kis_market_routing (J:KRX / NX:NXT / UN:통합), exactly like the orderbook
        snapshot.

        SOR maps to 'UN' (통합/unified): the EC2 live probe (JEP-180, 2026-06-19)
        confirmed unified quotes stream across the KRX close into the NXT after-market;
        the earlier 'UN times out' observation (JEP-162) did NOT reproduce. This test
        pins the outgoing code so a regression back to 'J' (which freezes after 15:30
        KST) fails here instead of only in live."""
        expected = {
            CONSTANTS.MARKET_ROUTING_KRX: "J",
            CONSTANTS.MARKET_ROUTING_NXT: "NX",
            CONSTANTS.MARKET_ROUTING_SOR: "UN",
        }
        symbol = self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
        regex_url = re.compile(
            f"{web_utils.public_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_TICKER_PATH)}.*"
        )
        for routing, code in expected.items():
            with self.subTest(routing=routing):
                mock_api.requests.clear()
                ex = self._make_exchange(kis_market_routing=routing)
                ex._auth._access_token = "test_access_token"
                ex._auth._token_expires_at = time.time() + 86400
                ex._set_trading_pair_symbol_map(bidict({symbol: self.trading_pair}))
                mock_api.get(regex_url, body=json.dumps(self.latest_prices_request_mock_response))
                await ex._get_last_traded_price(self.trading_pair)
                sent = next(calls[-1] for calls in mock_api.requests.values() if calls)
                self.assertEqual(code, sent.kwargs["params"]["FID_COND_MRKT_DIV_CODE"])

    @aioresponses()
    async def test_get_last_traded_price_fails_closed_on_error_or_zero(self, mock_api):
        """KIS returns HTTP 200 with rt_cd != '0' on logical errors, and a 0/empty price
        during venue closure or a rejected market-div code. _get_last_traded_price must
        raise (fail closed), not silently publish 0.0 into PriceType.LastTrade."""
        symbol = self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
        regex_url = re.compile(
            f"{web_utils.public_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_TICKER_PATH)}.*"
        )
        ex = self._make_exchange()
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        ex._set_trading_pair_symbol_map(bidict({symbol: self.trading_pair}))

        # rt_cd != "0" (logical error) -> raise
        mock_api.get(regex_url, body=json.dumps({"rt_cd": "1", "msg1": "no data", "output": {}}))
        with self.assertRaises(IOError):
            await ex._get_last_traded_price(self.trading_pair)

        # rt_cd "0" but zero price (venue closed) -> raise
        mock_api.requests.clear()
        mock_api.get(regex_url, body=json.dumps({"rt_cd": "0", "msg1": "ok", "output": {"stck_prpr": "0"}}))
        with self.assertRaises(IOError):
            await ex._get_last_traded_price(self.trading_pair)

    @aioresponses()
    async def test_update_balances_fails_closed_on_rt_cd_error(self, mock_api):
        """KIS returns HTTP 200 with rt_cd != '0' on logical errors (e.g. EGW00304).
        _update_balances must raise (fail closed) rather than accept the empty output1/
        output2 and silently zero out holdings + cash — a money-path input (JEP-161)."""
        regex_url = re.compile(f"{self.balance_url}.*")
        ex = self._make_exchange()
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        ex._set_trading_pair_symbol_map(
            bidict({self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset): self.trading_pair})
        )
        mock_api.get(regex_url, body=json.dumps({"rt_cd": "1", "msg1": "no data", "output1": [], "output2": []}))
        with self.assertRaisesRegex(IOError, "rt_cd"):
            await ex._update_balances()

    # ------------------------------------------------------------------
    # JEP-182: remaining KIS REST fail-open surfaces (rt_cd not inspected)
    # ------------------------------------------------------------------

    def _order_detail_url_regex(self):
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH)
        return re.compile(f"^{re.escape(url)}.*")

    def _authed_exchange(self):
        ex = self._make_exchange()
        ex._auth._access_token = "test_access_token"
        ex._auth._token_expires_at = time.time() + 86400
        return ex

    def _tracked_limit_buy(self, eoid="0000123456", initial_state=OrderState.OPEN):
        return InFlightOrder(
            client_order_id="OID-jep182",
            exchange_order_id=eoid,
            trading_pair=self.trading_pair,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("10"),
            price=Decimal("70000"),
            creation_timestamp=time.time(),
            initial_state=initial_state,
        )

    @aioresponses()
    async def test_check_network_fails_closed_after_sustained_rt_cd_error(self, mock_api):
        """check_network probes the balance endpoint. KIS returns HTTP 200 with rt_cd != '0' on
        logical auth/account errors; _make_network_check_request raises. With the JEP-203 debounce a
        SINGLE failure is tolerated (transient), but a SUSTAINED error (>= threshold consecutive)
        still reports NOT_CONNECTED — the fail-closed readiness guard (JEP-182) holds, just debounced
        so one transport blip cannot tear down the healthy WS hub."""
        regex_url = re.compile(f"{self.balance_url}.*")
        ex = self._authed_exchange()
        mock_api.get(
            regex_url,
            body=json.dumps({"rt_cd": "1", "msg1": "EGW00304", "output1": [], "output2": []}),
            repeat=True,
        )
        statuses = [await ex.check_network() for _ in range(ex._NET_CHECK_FAILURE_THRESHOLD)]
        self.assertEqual(NetworkStatus.CONNECTED, statuses[0])        # single failure tolerated
        self.assertEqual(NetworkStatus.NOT_CONNECTED, statuses[-1])   # sustained -> fail-closed

    @aioresponses()
    async def test_check_network_connected_on_healthy_balance(self, mock_api):
        """Companion: a healthy rt_cd == '0' balance response keeps check_network CONNECTED — the
        fail-closed guard must not break the normal readiness path (JEP-182)."""
        regex_url = re.compile(f"{self.balance_url}.*")
        ex = self._authed_exchange()
        mock_api.get(regex_url, body=json.dumps({"rt_cd": "0", "msg1": "ok", "output1": [], "output2": {}}))
        self.assertEqual(NetworkStatus.CONNECTED, await ex.check_network())

    async def test_check_network_tolerates_single_transient_failure(self):
        """JEP-203: a single transport blip (timeout/connection error) on the balance probe must NOT
        flip NetworkStatus -> stop_network() -> tear down the healthy WS hub (the observed ~10s
        hedge-feed gaps). One failure stays CONNECTED; the JEP-134 WS-staleness gate (3s) is the fast
        hedge-readiness safety net."""
        from unittest.mock import patch, AsyncMock
        ex = self._authed_exchange()
        with patch.object(ex, "_make_network_check_request", new=AsyncMock(side_effect=IOError("transient timeout"))):
            self.assertEqual(NetworkStatus.CONNECTED, await ex.check_network())

    async def test_check_network_not_connected_after_threshold_consecutive_failures(self):
        """JEP-203: a genuine sustained outage (>= threshold consecutive probe failures) still reports
        NOT_CONNECTED so the connector lifecycle reacts to a real KIS outage."""
        from unittest.mock import patch, AsyncMock
        ex = self._authed_exchange()
        with patch.object(ex, "_make_network_check_request", new=AsyncMock(side_effect=IOError("down"))):
            statuses = [await ex.check_network() for _ in range(ex._NET_CHECK_FAILURE_THRESHOLD)]
        self.assertTrue(all(s == NetworkStatus.CONNECTED for s in statuses[:-1]))
        self.assertEqual(NetworkStatus.NOT_CONNECTED, statuses[-1])

    async def test_check_network_resets_failure_count_on_success(self):
        """JEP-203: a successful probe resets the consecutive-failure counter, so the observed flap
        pattern (isolated single failures with healthy probes between) never reaches the threshold."""
        from unittest.mock import patch, AsyncMock
        ex = self._authed_exchange()
        seq = [IOError("blip"), IOError("blip"), None, IOError("blip")]  # fail, fail, success(reset), fail
        with patch.object(ex, "_make_network_check_request", new=AsyncMock(side_effect=seq)):
            results = [await ex.check_network() for _ in range(4)]
        # Without the reset the final failure would be the 3rd cumulative -> NOT_CONNECTED (threshold=3).
        self.assertEqual([NetworkStatus.CONNECTED] * 4, results)

    @aioresponses()
    async def test_check_network_failure(self, mock_api):
        # JEP-203 override of the abstract base test: KIS debounces transient probe failures, so a
        # SINGLE failed probe stays CONNECTED (one REST blip must not flip NetworkStatus ->
        # stop_network() and tear down the healthy WS hub). A SUSTAINED failure (>= threshold
        # consecutive) still reports NOT_CONNECTED.
        regex_url = re.compile(f"{self.balance_url}.*")
        mock_api.get(regex_url, status=500, repeat=True)
        statuses = [await self.exchange.check_network() for _ in range(self.exchange._NET_CHECK_FAILURE_THRESHOLD)]
        self.assertEqual(NetworkStatus.CONNECTED, statuses[0])
        self.assertEqual(NetworkStatus.NOT_CONNECTED, statuses[-1])

    @aioresponses()
    async def test_request_order_status_no_op_on_rt_cd_error(self, mock_api):
        """Order-status path: a logical fetch error (HTTP 200 + rt_cd != '0') carries no state info.
        It must NOT infer OPEN from the empty output1 (the pre-JEP-182 bug) AND must NOT raise — a
        raised status exception would let the base escalate to process_order_not_found() and mark a
        live order FAILED after a few transient errors (codex challenge). So the update leaves the
        state UNCHANGED: a PARTIALLY_FILLED order stays PARTIALLY_FILLED, not flipped to OPEN and not
        raised (JEP-182)."""
        ex = self._authed_exchange()
        order = self._tracked_limit_buy(initial_state=OrderState.PARTIALLY_FILLED)
        mock_api.get(self._order_detail_url_regex(),
                     body=json.dumps({"rt_cd": "1", "msg1": "token expired", "output1": []}))
        update = await ex._request_order_status(order)
        self.assertEqual(OrderState.PARTIALLY_FILLED, update.new_state)

    @aioresponses()
    async def test_all_trade_updates_fails_closed_on_rt_cd_error(self, mock_api):
        """Same shared fetch on the fills path: rt_cd != '0' yields an empty output1 and
        _create_order_fill_updates returns [] — a fill would silently vanish. Must fail closed (JEP-182)."""
        ex = self._authed_exchange()
        mock_api.get(self._order_detail_url_regex(),
                     body=json.dumps({"rt_cd": "1", "msg1": "token expired", "output1": []}))
        with self.assertRaisesRegex(IOError, "rt_cd"):
            await ex._all_trade_updates_for_order(self._tracked_limit_buy())

    @aioresponses()
    async def test_order_detail_empty_but_ok_does_not_raise(self, mock_api):
        """no-data-vs-error: a legitimate 'no fills yet' poll carries rt_cd == '0' with output1 == [].
        The guard keys ONLY on rt_cd, so a successful-empty response must NOT raise — trade-updates
        stays [] and order-status stays OPEN (JEP-182)."""
        ex = self._authed_exchange()
        mock_api.get(self._order_detail_url_regex(), body=json.dumps(self._empty_fill_mock_response()))
        self.assertEqual([], await ex._all_trade_updates_for_order(self._tracked_limit_buy()))
        mock_api.get(self._order_detail_url_regex(), body=json.dumps(self._empty_fill_mock_response()))
        update = await ex._request_order_status(self._tracked_limit_buy())
        self.assertEqual(OrderState.OPEN, update.new_state)

    @aioresponses()
    async def test_get_open_orders_fails_closed_on_rt_cd_error(self, mock_api):
        """get_open_orders paginates inquire-daily-ccld with its own fetch. On rt_cd != '0' the empty
        output1 would make it report ZERO resting orders — fatal to reconcile/seed (it would conclude
        the account is flat and skip cancel-all). Must fail closed (JEP-182). The rt_cd == '0' + []
        no-raise case is already covered by test_get_open_orders_empty_returns_empty_list."""
        symbol = self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)
        ex = self._authed_exchange()
        ex._set_trading_pair_symbol_map(bidict({symbol: self.trading_pair}))
        mock_api.get(self._order_detail_url_regex(),
                     body=json.dumps({"rt_cd": "1", "msg1": "EGW00121", "output1": []}))
        with self.assertRaisesRegex(IOError, "rt_cd"):
            await ex.get_open_orders()

    def test_ws_enabled_default_true(self):
        self.assertTrue(self._make_exchange()._ws_enabled)

    def test_ws_enabled_false_disables(self):
        # "false" (any case) disables; the order book data source receives the flag
        # so it never connects to the WS edge (REST-only).
        ex = self._make_exchange(kis_ws_enabled="False")
        self.assertFalse(ex._ws_enabled)
        self.assertFalse(ex._create_order_book_data_source()._ws_enabled)
        self.assertFalse(ex._create_user_stream_data_source()._ws_enabled)

    def test_market_routing_default_is_sor(self):
        self.assertEqual("sor", self._make_exchange()._market_routing)

    def test_market_routing_explicit_krx(self):
        self.assertEqual("krx", self._make_exchange(kis_market_routing="krx")._market_routing)

    def test_sandbox_forces_krx_routing(self):
        ex = self._make_exchange(kis_market_routing="sor", domain="sandbox")
        self.assertEqual("krx", ex._market_routing)

    def test_invalid_routing_fails_closed(self):
        # Live: a typo must NOT silently route to KRX — fail closed
        with self.assertRaises(ValueError):
            self._make_exchange(kis_market_routing="bogus")

    def test_excg_for_routing_mapping(self):
        self.assertEqual("SOR", self._make_exchange(kis_market_routing="sor")._excg_for_routing())
        self.assertEqual("NXT", self._make_exchange(kis_market_routing="nxt")._excg_for_routing())
        self.assertEqual("KRX", self._make_exchange(kis_market_routing="krx")._excg_for_routing())
        self.assertEqual(
            "KRX",
            self._make_exchange(kis_market_routing="sor", domain="sandbox")._excg_for_routing(),
        )

    def test_authenticator_has_no_placeholder_token(self):
        # The authenticator must NOT inject a fake/placeholder access token.
        # A non-None initial token sets a 24h expiry, so _get_access_token
        # returns the placeholder instead of fetching a real OAuth2 token,
        # and every live request fails KIS EGW00121 "유효하지 않은 token".
        auth = self._make_exchange().authenticator
        self.assertIsNone(auth._access_token)
        self.assertEqual(0.0, auth._token_expires_at)

    def test_authenticator_wires_disk_token_cache(self):
        # The authenticator must auto-derive a token_cache_path so the daily
        # OAuth token / approval key survive process restarts. We point
        # hummingbot.data_path() at an isolated temp dir to prove the wiring
        # without touching the real data directory.
        import os
        import shutil
        import tempfile
        from unittest.mock import patch

        tmp = tempfile.mkdtemp(prefix="kis_exch_cache_")
        try:
            with patch("hummingbot.data_path", return_value=tmp):
                auth = self._make_exchange().authenticator
            self.assertTrue(auth.token_cache_path.startswith(tmp))
            self.assertTrue(auth.token_cache_path.endswith(".json"))
            self.assertIn("kis_token_cache_", os.path.basename(auth.token_cache_path))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_network_check_path_is_not_token_endpoint(self):
        # Lesson (live RCA): the base health probe does an unauthenticated GET on
        # ``check_network_request_path``. KIS pointed it at ``oauth2/tokenP`` — a
        # POST-only token-issue endpoint that does NOT answer GET. Live, the GET
        # hangs until timeout every cycle, check_network swallows the exception
        # and returns NOT_CONNECTED, so the connector NEVER becomes ready (silent:
        # nothing in errors.log). The probe must target the balance endpoint.
        ex = self._make_exchange()
        self.assertEqual(CONSTANTS.DOMESTIC_STOCK_BALANCE_PATH, ex.check_network_request_path)
        self.assertNotEqual(CONSTANTS.TOKEN_PATH_URL, ex.check_network_request_path)

    def test_open_orders_source_endpoint_is_verified_inquire_daily_ccld(self):
        # Default open-orders source = the EXISTING, proven inquire-daily-ccld endpoint
        # (HIGH-1: inquire-psbl-rvsecncl/TTTC0084R is unverified - do not assert it here).
        self.assertEqual(
            CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH,
            "uapi/domestic-stock/v1/trading/inquire-daily-ccld",
        )
        self.assertEqual(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_TR_ID, "TTTC8001R")
        rate_limit_ids = {rl.limit_id for rl in CONSTANTS.RATE_LIMITS}
        self.assertIn(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH, rate_limit_ids)

    @aioresponses()
    async def test_network_check_uses_authenticated_balance_request(self, mock_api):
        # The network probe must hit the authenticated balance inquiry (works 24/7,
        # reuses the cached OAuth token) — never the token endpoint.
        balance_url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_BALANCE_PATH)
        token_url = web_utils.public_rest_url(CONSTANTS.TOKEN_PATH_URL)
        mock_api.get(
            re.compile(f"^{re.escape(balance_url)}.*"),
            body=json.dumps(self.network_status_request_successful_mock_response),
        )

        await self.exchange._make_network_check_request()

        balance_reqs = self._all_executed_requests(mock_api, balance_url)
        self.assertEqual(1, len(balance_reqs))
        # No request ever went to the token endpoint.
        self.assertEqual(0, len(self._all_executed_requests(mock_api, token_url)))
        # Authenticated, with the balance TR_ID header.
        request_headers = balance_reqs[0].kwargs["headers"]
        self.assertTrue(request_headers["Authorization"].startswith("Bearer "))
        self.assertEqual(CONSTANTS.DOMESTIC_STOCK_BALANCE_TR_ID, request_headers["tr_id"])

    # ------------------------------------------------------------------
    # JEP-153: readiness ready==True integration + "no-raise" crash guards.
    #
    # The live session had a cascade where every unit test was green (mocks
    # returned only rt_cd:"0" success) yet the connector never reached
    # ready==True:
    #   - check_network did GET on POST-only oauth2/tokenP -> NOT_CONNECTED,
    #   - REST snapshot was unauthenticated -> EGW00304,
    #   - _update_time_synchronizer hit an absent web_utils.get_current_server_time
    #     -> AttributeError crashed the status loop,
    #   - the exec-notification WS gated readiness.
    # These tests drive the actual connector entry points end-to-end so the
    # 5 status_dict items all flip True (and never raise).
    # ------------------------------------------------------------------

    def _configure_orderbook_snapshot_response(self, mock_api: aioresponses):
        """Mock the authenticated KIS REST orderbook snapshot (used to bootstrap
        order_books_initialized via the order-book tracker)."""
        url = web_utils.public_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH)
        regex_url = re.compile(f"^{re.escape(url)}.*")
        snapshot = {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "정상처리 되었습니다.",
            "output1": {
                "askp1": "72100", "askp_rsqn1": "500",
                "askp2": "72200", "askp_rsqn2": "300",
                "bidp1": "71900", "bidp_rsqn1": "400",
                "bidp2": "71800", "bidp_rsqn2": "200",
            },
        }
        mock_api.get(regex_url, body=json.dumps(snapshot))
        return url

    @aioresponses()
    async def test_connector_reaches_ready_true_integration(self, mock_api):
        # Integration: drive every readiness source the live boot exercises and
        # assert all 5 status_dict items become True -> exchange.ready == True.
        # WS is disabled so order books bootstrap purely off the authenticated
        # REST snapshot (the exact path that returned EGW00304 unauthenticated).
        exchange = self._make_exchange(kis_ws_enabled="false")
        exchange._auth._access_token = "test_access_token"
        exchange._auth._token_expires_at = time.time() + 86400

        # 1. symbols_mapping_initialized — built from configured pairs (no network).
        await exchange._initialize_trading_pair_symbol_map()
        # 2. trading_rule_initialized — synthesised per pair (no network).
        await exchange._update_trading_rules()
        # 3. account_balance — authenticated balance inquiry.
        mock_api.get(
            re.compile(f"^{re.escape(self.balance_url)}.*"),
            body=json.dumps(self.balance_request_mock_response_for_base_and_quote),
        )
        await exchange._update_balances()
        # 4. order_books_initialized — authenticated REST snapshot bootstraps the tracker.
        self._configure_orderbook_snapshot_response(mock_api)
        await exchange.order_book_tracker._init_order_books()
        # 5. user_stream_initialized — decoupled from the WS (always True).

        status = exchange.status_dict
        self.assertTrue(status["symbols_mapping_initialized"], status)
        self.assertTrue(status["trading_rule_initialized"], status)
        self.assertTrue(status["account_balance"], status)
        self.assertTrue(status["order_books_initialized"], status)
        self.assertTrue(status["user_stream_initialized"], status)
        self.assertTrue(exchange.ready, status)

    @aioresponses()
    async def test_time_sync_update_does_not_raise(self, mock_api):
        # Guards the AttributeError crash: the base _update_time_synchronizer calls
        # web_utils.get_current_server_time(), absent on KIS -> it would crash the
        # status-polling loop and flap to NOT_CONNECTED. The override must be a
        # silent no-op end-to-end (both signatures).
        await self.exchange._update_time_synchronizer()
        await self.exchange._update_time_synchronizer(pass_on_non_cancelled_error=True)

    @aioresponses()
    async def test_check_network_balance_probe_does_not_raise(self, mock_api):
        # Guards the permanent-NOT_CONNECTED crash class: the health probe must run
        # end-to-end (authenticated balance GET) without raising. If it were aimed
        # at POST-only oauth2/tokenP, a GET hangs/times out every cycle.
        mock_api.get(
            re.compile(f"^{re.escape(self.balance_url)}.*"),
            body=json.dumps(self.network_status_request_successful_mock_response),
        )
        await self.exchange._make_network_check_request()

    @aioresponses()
    async def test_update_balances_does_not_raise(self, mock_api):
        # Guards the account-update crash class: _update_balances must complete
        # end-to-end and populate balances without raising.
        mock_api.get(
            re.compile(f"^{re.escape(self.balance_url)}.*"),
            body=json.dumps(self.balance_request_mock_response_for_base_and_quote),
        )
        await self.exchange._update_balances()
        self.assertGreater(len(self.exchange._account_balances), 0)

    @aioresponses()
    async def test_krw_available_uses_ord_psbl_cash(self, mock_api):
        resp = {
            "rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok",
            "output1": [],
            "output2": [{"dnca_tot_amt": "1000000", "ord_psbl_cash": "600000",
                         "nxdy_excc_amt": "0", "tot_evlu_amt": "1000000"}],
        }
        mock_api.get(re.compile(f"^{re.escape(self.balance_url)}.*"), body=json.dumps(resp))
        await self.exchange._update_balances()
        self.assertEqual(self.exchange.get_available_balance(self.quote_asset), Decimal("600000"))
        self.assertEqual(self.exchange.get_balance(self.quote_asset), Decimal("1000000"))

    @aioresponses()
    async def test_krw_available_uses_nrcvb_buy_amt_when_ord_psbl_cash_missing(self, mock_api):
        # HIGH-3 precedence: ord_psbl_cash absent -> use nrcvb_buy_amt (NOT dnca_tot_amt).
        resp = {
            "rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok",
            "output1": [],
            "output2": [{"dnca_tot_amt": "1000000", "nrcvb_buy_amt": "700000",
                         "nxdy_excc_amt": "0", "tot_evlu_amt": "1000000"}],
        }
        mock_api.get(re.compile(f"^{re.escape(self.balance_url)}.*"), body=json.dumps(resp))
        await self.exchange._update_balances()
        self.assertEqual(self.exchange.get_available_balance(self.quote_asset), Decimal("700000"))
        self.assertEqual(self.exchange.get_balance(self.quote_asset), Decimal("1000000"))

    @aioresponses()
    async def test_krw_available_falls_back_to_dnca_tot_amt_when_field_missing(self, mock_api):
        resp = {
            "rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok",
            "output1": [],
            "output2": [{"dnca_tot_amt": "1000000", "nxdy_excc_amt": "0", "tot_evlu_amt": "1000000"}],
        }
        mock_api.get(re.compile(f"^{re.escape(self.balance_url)}.*"), body=json.dumps(resp))
        await self.exchange._update_balances()
        self.assertEqual(self.exchange.get_available_balance(self.quote_asset), Decimal("1000000"))
        self.assertEqual(self.exchange.get_balance(self.quote_asset), Decimal("1000000"))

    @aioresponses()
    async def test_balance_snapshot_refresh_rest_only(self, mock_api):
        base = self.base_asset
        quote = self.quote_asset
        first = {
            "rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok",
            "output1": [{"pdno": base, "hldg_qty": "10", "ord_psbl_qty": "10"}],
            "output2": [{"dnca_tot_amt": "1000000", "ord_psbl_cash": "1000000"}],
        }
        mock_api.get(re.compile(f"^{re.escape(self.balance_url)}.*"), body=json.dumps(first))
        await self.exchange._update_balances()
        self.assertEqual(self.exchange.get_balance(base), Decimal("10"))
        self.assertEqual(self.exchange.get_available_balance(quote), Decimal("1000000"))

        second = {
            "rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok",
            "output1": [{"pdno": base, "hldg_qty": "3", "ord_psbl_qty": "3"}],
            "output2": [{"dnca_tot_amt": "1000000", "ord_psbl_cash": "400000"}],
        }
        mock_api.get(re.compile(f"^{re.escape(self.balance_url)}.*"), body=json.dumps(second))
        await self.exchange._update_balances()
        self.assertEqual(self.exchange.get_balance(base), Decimal("3"))
        self.assertEqual(self.exchange.get_available_balance(base), Decimal("3"))
        self.assertEqual(self.exchange.get_available_balance(quote), Decimal("400000"))
        self.assertEqual(self.exchange.get_balance(quote), Decimal("1000000"))

    @aioresponses()
    async def test_balance_snapshot_removes_vanished_asset(self, mock_api):
        base = self.base_asset
        first = {
            "rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok",
            "output1": [{"pdno": base, "hldg_qty": "10", "ord_psbl_qty": "10"}],
            "output2": [{"dnca_tot_amt": "1000000", "ord_psbl_cash": "1000000"}],
        }
        mock_api.get(re.compile(f"^{re.escape(self.balance_url)}.*"), body=json.dumps(first))
        await self.exchange._update_balances()
        self.assertIn(base, self.exchange._account_balances)

        second = {
            "rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok",
            "output1": [],
            "output2": [{"dnca_tot_amt": "1000000", "ord_psbl_cash": "1000000"}],
        }
        mock_api.get(re.compile(f"^{re.escape(self.balance_url)}.*"), body=json.dumps(second))
        await self.exchange._update_balances()
        self.assertNotIn(base, self.exchange._account_balances)
        self.assertNotIn(base, self.exchange._account_available_balances)

    @aioresponses()
    async def test_update_trading_rules_does_not_raise(self, mock_api):
        # Guards trading-rule init: rules are synthesised network-free and must
        # populate without raising (no unauthenticated ticker fetch -> EGW00304).
        await self.exchange._update_trading_rules()
        self.assertGreater(len(self.exchange._trading_rules), 0)

    def test_user_stream_init_does_not_raise_and_is_true(self):
        # Guards the WS-gates-readiness crash class: readiness must not depend on
        # the exec-notification WS connecting. _is_user_stream_initialized must
        # return True without raising even with no WS data received.
        self.assertTrue(self.exchange._is_user_stream_initialized())

    # ------------------------------------------------------------------
    # JEP-154: request-auth assertions for every is_auth_required KIS REST call
    # + rejection-response fixtures and their handling.
    #
    # The live session passed unit tests while the connector was firing
    # unauthenticated / mis-routed requests (EGW00304) because the mocks only
    # ever returned rt_cd:"0". These assert that the ACTUALLY-SENT request
    # carried Authorization: Bearer <token> + appkey + the correct tr_id, and
    # that KIS rejection payloads are surfaced (raise / re-auth) not swallowed.
    # ------------------------------------------------------------------

    def _assert_request_authenticated(self, request_call: RequestCall, expected_tr_id: str):
        """Reusable: every is_auth_required=True KIS REST call must carry the
        Bearer token, appkey, appsecret, and the per-API tr_id header.
        Modeled on test_request_order_book_snapshot_is_authenticated."""
        headers = request_call.kwargs["headers"]
        self.assertIn("Authorization", headers)
        self.assertTrue(
            headers["Authorization"].startswith("Bearer "),
            f"Authorization must be 'Bearer <token>', got {headers.get('Authorization')!r}",
        )
        self.assertEqual("test_access_token", headers["Authorization"].split(" ", 1)[1])
        self.assertIn("appkey", headers)
        self.assertEqual("testAppKey", headers["appkey"])
        self.assertIn("appsecret", headers)
        self.assertEqual("testAppSecret", headers["appsecret"])
        self.assertEqual(expected_tr_id, headers["tr_id"])

    def _track_order(self, exchange) -> InFlightOrder:
        exchange.start_tracking_order(
            order_id="HBOT-test-1",
            exchange_order_id=str(self.expected_exchange_order_id),
            trading_pair=self.trading_pair,
            trade_type=TradeType.BUY,
            price=Decimal("72000"),
            amount=Decimal("10"),
            order_type=OrderType.LIMIT,
        )
        return exchange.in_flight_orders["HBOT-test-1"]

    @aioresponses()
    async def test_place_order_request_is_authenticated(self, mock_api):
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_PATH)
        mock_api.post(
            re.compile(f"^{re.escape(url)}.*"),
            body=json.dumps(self.order_creation_request_successful_mock_response),
        )
        await self.exchange._place_order(
            order_id="HBOT-1",
            trading_pair=self.trading_pair,
            amount=Decimal("10"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("72000"),
        )
        req = self._all_executed_requests(mock_api, url)[0]
        self._assert_request_authenticated(req, CONSTANTS.DOMESTIC_STOCK_ORDER_BUY_TR_ID)

    @aioresponses()
    async def test_place_order_sell_request_is_authenticated(self, mock_api):
        # SELL routes to a different tr_id (TTTC0011U) than BUY (TTTC0012U);
        # lock the SELL branch so tr_id routing regressions go red too.
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_PATH)
        mock_api.post(
            re.compile(f"^{re.escape(url)}.*"),
            body=json.dumps(self.order_creation_request_successful_mock_response),
        )
        await self.exchange._place_order(
            order_id="HBOT-2",
            trading_pair=self.trading_pair,
            amount=Decimal("10"),
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("72000"),
        )
        req = self._all_executed_requests(mock_api, url)[0]
        self._assert_request_authenticated(req, CONSTANTS.DOMESTIC_STOCK_ORDER_SELL_TR_ID)

    async def test_place_order_market_maps_to_best_price_ord_dvsn_03(self):
        # JEP-219: a MARKET hedge must transmit KIS 최유리지정가 ("03"), NOT 시장가 ("01")
        # (rejected on the NXT after-market), with ORD_UNPR="0". The NaN market price must
        # also NOT crash (the old str(int(price)) raised on NaN).
        captured = {}

        async def _capture(path_url, data, is_auth_required, headers):
            captured["data"] = data
            return self.order_creation_request_successful_mock_response

        self.exchange._api_post = _capture
        oid, _ts = await self.exchange._place_order(
            order_id="HBOT-MKT",
            trading_pair=self.trading_pair,
            amount=Decimal("1"),
            trade_type=TradeType.SELL,
            order_type=OrderType.MARKET,
            price=Decimal("NaN"),
        )
        self.assertEqual(captured["data"]["ORD_DVSN"], "03")
        self.assertEqual(captured["data"]["ORD_UNPR"], "0")
        self.assertEqual(captured["data"]["ORD_QTY"], "1")

    async def test_place_order_limit_maps_to_ord_dvsn_00_with_price(self):
        # Regression: a LIMIT order is unchanged — 지정가 ("00") at the integer KRW price.
        captured = {}

        async def _capture(path_url, data, is_auth_required, headers):
            captured["data"] = data
            return self.order_creation_request_successful_mock_response

        self.exchange._api_post = _capture
        await self.exchange._place_order(
            order_id="HBOT-LMT",
            trading_pair=self.trading_pair,
            amount=Decimal("2"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("72000"),
        )
        self.assertEqual(captured["data"]["ORD_DVSN"], "00")
        self.assertEqual(captured["data"]["ORD_UNPR"], "72000")

    @aioresponses()
    async def test_cancel_order_request_is_authenticated(self, mock_api):
        order = self._track_order(self.exchange)
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_CANCEL_PATH)
        mock_api.post(
            re.compile(f"^{re.escape(url)}.*"),
            body=json.dumps(self._order_cancelation_request_successful_mock_response(order)),
        )
        await self.exchange._place_cancel("HBOT-test-1", order)
        req = self._all_executed_requests(mock_api, url)[0]
        self._assert_request_authenticated(req, CONSTANTS.DOMESTIC_STOCK_CANCEL_TR_ID)

    @aioresponses()
    async def test_balance_request_is_authenticated(self, mock_api):
        url = self.balance_url
        mock_api.get(
            re.compile(f"^{re.escape(url)}.*"),
            body=json.dumps(self.balance_request_mock_response_for_base_and_quote),
        )
        await self.exchange._update_balances()
        req = self._all_executed_requests(mock_api, url)[0]
        self._assert_request_authenticated(req, CONSTANTS.DOMESTIC_STOCK_BALANCE_TR_ID)

    @aioresponses()
    async def test_daily_ccld_fill_query_request_is_authenticated(self, mock_api):
        order = self._track_order(self.exchange)
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH)
        mock_api.get(
            re.compile(f"^{re.escape(url)}.*"),
            body=json.dumps(self._order_fills_request_full_fill_mock_response(order)),
        )
        await self.exchange._all_trade_updates_for_order(order)
        req = self._all_executed_requests(mock_api, url)[0]
        self._assert_request_authenticated(req, CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_TR_ID)

    @aioresponses()
    async def test_order_book_snapshot_request_is_authenticated(self, mock_api):
        url = web_utils.public_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH)
        self._configure_orderbook_snapshot_response(mock_api)
        ds = self.exchange._create_order_book_data_source()
        await ds._request_order_book_snapshot(self.trading_pair)
        req = self._all_executed_requests(mock_api, url)[0]
        self._assert_request_authenticated(req, CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_TR_ID)

    # --- Rejection fixtures ---

    @staticmethod
    def _rejection_response(rt_cd: str, msg_cd: str, msg1: str) -> dict:
        return {"rt_cd": rt_cd, "msg_cd": msg_cd, "msg1": msg1, "output": {}}

    @aioresponses()
    async def test_place_order_rt_cd_one_raises(self, mock_api):
        # rt_cd:"1" generic failure on order placement must raise (ValueError),
        # never silently treated as success.
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_PATH)
        mock_api.post(
            re.compile(f"^{re.escape(url)}.*"),
            body=json.dumps(self._rejection_response("1", "APBK0919", "주문수량 초과")),
        )
        with self.assertRaises(ValueError):
            await self.exchange._place_order(
                order_id="HBOT-1",
                trading_pair=self.trading_pair,
                amount=Decimal("10"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("72000"),
            )

    @aioresponses()
    async def test_cancel_rt_cd_one_raises(self, mock_api):
        # rt_cd:"1" on cancel must raise, not report success.
        order = self._track_order(self.exchange)
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_CANCEL_PATH)
        mock_api.post(
            re.compile(f"^{re.escape(url)}.*"),
            body=json.dumps(self._rejection_response("1", "APBK1234", "취소 불가")),
        )
        with self.assertRaises(ValueError):
            await self.exchange._place_cancel("HBOT-test-1", order)

    @aioresponses()
    async def test_egw00304_app_secret_invalid_raises_on_balance(self, mock_api):
        # EGW00304 ("appSecret 유효하지 않습니다") is returned by KIS as HTTP 500.
        # The web assistant turns HTTP>=400 into IOError -> the balance probe / poll
        # must surface it (so check_network reports NOT_CONNECTED), not swallow it.
        url = self.balance_url
        mock_api.get(
            re.compile(f"^{re.escape(url)}.*"),
            status=500,
            body=json.dumps(self._rejection_response("1", "EGW00304", "고객식별키(appSecret)가 유효하지 않습니다")),
        )
        with self.assertRaises(IOError):
            await self.exchange._update_balances()

    @aioresponses()
    async def test_egw00304_app_secret_invalid_raises_on_order_book_snapshot(self, mock_api):
        # Same EGW00304 on the REST orderbook snapshot: the order book must fail to
        # bootstrap (raise) rather than silently initialise empty -> stays not-ready.
        url = web_utils.public_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_ORDERBOOK_PATH)
        mock_api.get(
            re.compile(f"^{re.escape(url)}.*"),
            status=500,
            body=json.dumps(self._rejection_response("1", "EGW00304", "appSecret invalid")),
        )
        ds = self.exchange._create_order_book_data_source()
        with self.assertRaises(IOError):
            await ds._request_order_book_snapshot(self.trading_pair)

    def _isolated_auth(self):
        # KisAuth pointed at an isolated temp cache dir so token fetch/persist
        # never reads or writes the real hummingbot data directory (which would
        # poison other tests' on-disk token hydration).
        import shutil
        import tempfile
        from hummingbot.connector.exchange.kis.kis_auth import KisAuth
        tmp = tempfile.mkdtemp(prefix="kis_exch_reauth_")
        self.addCleanup(shutil.rmtree, tmp, True)
        return KisAuth(app_key="testAppKey", app_secret="testAppSecret", token_cache_path=tmp)

    async def test_egw00123_token_expired_triggers_reauth_fetch(self):
        # EGW00123/EGW00121 = expired/invalid token. KIS auth refreshes by EXPIRY
        # (no error-driven re-auth path), so a token whose expiry has passed must
        # cause _get_access_token to FETCH A NEW ONE rather than reuse the stale
        # placeholder (the live EGW00121 loop was a never-expiring placeholder token).
        from unittest.mock import patch as _patch
        from datetime import datetime as _dt, timedelta as _td
        auth = self._isolated_auth()
        # Stale token already past expiry -> must re-fetch.
        auth._access_token = "stale_token"
        auth._token_expires_at = time.time() - 1
        future = (_dt.now() + _td(hours=23)).strftime("%Y-%m-%d %H:%M:%S")

        from test.hummingbot.connector.exchange.kis import test_kis_auth as _ta
        session_cls, captured = _ta._make_mock_session(
            lambda url, json=None, **kw: _ta._make_mock_response(
                {"access_token": "fresh_token", "access_token_token_expired": future}
            )
        )
        with _patch("aiohttp.ClientSession", session_cls):
            token = await auth._get_access_token()
        self.assertEqual("fresh_token", token)
        self.assertEqual(1, len(captured["calls"]))

    async def test_get_tokenp_timeout_propagates_not_swallowed(self):
        # GET-tokenP-timeout class: a token fetch that times out must propagate
        # (so the caller fails closed / reports NOT_CONNECTED), NOT return a fake
        # token. We force the POST to raise asyncio.TimeoutError.
        import asyncio as _asyncio
        from unittest.mock import patch as _patch
        auth = self._isolated_auth()
        auth._access_token = None
        auth._token_expires_at = 0.0

        class _TimeoutSession:
            def __init__(self, *a, **k):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                pass

            def post(self, *a, **k):
                class _C:
                    async def __aenter__(self_inner):
                        raise _asyncio.TimeoutError("tokenP GET/POST timed out")

                    async def __aexit__(self_inner, *a):
                        pass
                return _C()

        with _patch("aiohttp.ClientSession", _TimeoutSession):
            with self.assertRaises(_asyncio.TimeoutError):
                await auth._get_access_token()
        # No fake token was cached on timeout.
        self.assertIsNone(auth._access_token)

    def _expected_initial_status_dict(self) -> dict:
        # KIS decouples readiness from the exec-notification WebSocket (fills are
        # caught via REST order polling), so user_stream_initialized is always True.
        return {
            "symbols_mapping_initialized": False,
            "order_books_initialized": False,
            "account_balance": False,
            "trading_rule_initialized": False,
            "user_stream_initialized": True,
        }

    async def test_time_synchronizer_is_noop(self):
        # KIS uses OAuth Bearer auth (no HMAC timestamp signing) and needs no
        # server-time sync. The base _update_time_synchronizer calls
        # web_utils.get_current_server_time(), which KIS does not provide -> the
        # AttributeError propagates into _status_polling_loop and flaps the
        # connector to NOT_CONNECTED (account-update failures, never stably ready).
        # The override must be a no-op that never touches the absent helper.
        ex = self._make_exchange()
        self.assertFalse(hasattr(web_utils, "get_current_server_time"))
        await ex._update_time_synchronizer()
        await ex._update_time_synchronizer(pass_on_non_cancelled_error=True)

    def test_user_stream_does_not_gate_readiness(self):
        # KIS's real-time exec-notification WebSocket (ops.koreainvestment.com:21000)
        # is environment-gated and frequently unavailable (e.g. real-time WS cap /
        # entitlement). Fills are caught via REST order-status polling
        # (ExchangePyBase._update_order_status), so the user stream must NOT gate
        # readiness -- otherwise the connector never becomes ready and cannot trade
        # even though REST order/balance polling works fine.
        ex = self._make_exchange()
        self.assertTrue(ex.is_trading_required)
        self.assertEqual(0, ex._user_stream_tracker.data_source.last_recv_time)
        self.assertTrue(ex._is_user_stream_initialized())

    def test_constructor_accepts_kis_sandbox_kwarg(self):
        # The non-trading instantiation path (settings.
        # non_trading_connector_instance_with_default_configuration, used by the
        # trading-pair fetcher) forwards EVERY config-map field as a kwarg,
        # including kis_sandbox (is_connect_key=False). The constructor must
        # accept it or trading-pair fetching raises TypeError.
        ex = KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            kis_sandbox="false",
            kis_market_routing="sor",
        )
        self.assertFalse(ex._sandbox)
        self.assertEqual("sor", ex._market_routing)

    def test_kis_sandbox_field_enables_sandbox(self):
        # kis_sandbox="true" must activate sandbox (config field is wired) and,
        # because sandbox does not support sor/nxt, fall back to krx routing.
        ex = KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            kis_sandbox="true",
            kis_market_routing="sor",
        )
        self.assertTrue(ex._sandbox)
        self.assertEqual("krx", ex._market_routing)

    def test_full_config_map_kwargs_bind_to_constructor(self):
        # Guard against future config-map fields that have no constructor param:
        # simulate the non-trading factory call shape and assert it binds.
        import inspect
        factory_kwargs = dict(
            kis_app_key="k",
            kis_app_secret="s",
            kis_account_number="12345678-01",
            kis_sandbox="false",
            kis_market_routing="sor",
            kis_hts_id="myhts",
            trading_pairs=[self.trading_pair],
            trading_required=False,
            balance_asset_limit={},
        )
        # Raises TypeError if any kwarg has no matching parameter.
        inspect.signature(KisExchange.__init__).bind(object(), **factory_kwargs)

    def test_constructor_accepts_balance_asset_limit(self):
        # The connector factory (settings.conn_init_parameters) ALWAYS forwards
        # balance_asset_limit (and rate_limits_share_pct for sub-domains) to every
        # connector __init__. KIS must accept them and forward balance_asset_limit
        # to ExchangePyBase, or live instantiation raises TypeError.
        limit = {"kis": {"KRW": Decimal("1000")}}
        ex = KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            kis_market_routing="sor",
            balance_asset_limit=limit,
            rate_limits_share_pct=Decimal("50"),
        )
        self.assertEqual("sor", ex._market_routing)
        self.assertEqual(limit, ex._balance_asset_limit)

    def test_constructor_stores_and_forwards_hts_id(self):
        # kis_hts_id must reach the user-stream DS so the exec-notice channel
        # (H0STCNI0) can subscribe with the customer HTS ID as tr_key.
        ex = KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            kis_market_routing="sor",
            kis_hts_id="myhts",
        )
        self.assertEqual("myhts", ex._hts_id)
        ds = ex._create_user_stream_data_source()
        self.assertEqual("myhts", ds._hts_id)

    def test_hts_id_defaults_empty(self):
        # Omitted kis_hts_id -> empty (exec-notice WS skipped, REST fallback).
        self.assertEqual("", self._make_exchange()._hts_id)

    def test_market_status_capture_only_flows_to_data_source(self):
        # JEP-201 capture-only: flag parses + forwards to the orderbook DS, with the latch off.
        ex = KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            kis_market_routing="sor",
            kis_market_status_capture_only="true",
        )
        self.assertTrue(ex._market_status_capture_only)
        self.assertFalse(ex._market_status_enabled)
        ds = ex._create_order_book_data_source()
        self.assertTrue(ds._market_status_capture_only)
        self.assertTrue(ds._market_status_subscribed)
        self.assertFalse(ds._market_status_enabled)

    def test_market_status_enabled_takes_precedence_over_capture_only(self):
        # Both set -> full mode wins (latch active), capture-only suppressed.
        ex = KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            kis_market_routing="sor",
            kis_market_status_enabled="true",
            kis_market_status_capture_only="true",
        )
        self.assertTrue(ex._market_status_enabled)
        self.assertFalse(ex._market_status_capture_only)

    def test_capture_only_survives_config_map(self):
        # is_connect_key=True is MANDATORY for the field to reach the constructor.
        from hummingbot.client.config.config_helpers import (
            ClientConfigAdapter,
            api_keys_from_connector_config_map,
        )
        from hummingbot.connector.exchange.kis.kis_utils import KisConfigMap

        cm = ClientConfigAdapter(
            KisConfigMap(
                kis_app_key="k",
                kis_app_secret="s",
                kis_account_number="12345678-01",
                kis_market_status_capture_only="true",
            )
        )
        api_keys = api_keys_from_connector_config_map(cm)
        self.assertEqual("true", api_keys["kis_market_status_capture_only"])
        ex = KisExchange(**api_keys, trading_pairs=[self.trading_pair])
        self.assertTrue(ex._market_status_capture_only)

    def test_dollar_prefixed_hts_id_preserved_end_to_end(self):
        # REGRESSION (JEP-200 live, 2026-06-23): KIS provisions numeric HTS IDs
        # DOLLAR-PREFIXED — it SMSes "고객 ID : $1766930". The '$' is part of the
        # htsid; dropping it (entering "1766930") is rejected with OPSP0017
        # "htsid가 잘못되었습니다" and recycles the shared WS hub (EC2-confirmed).
        # Pin that the config->connect-keys->constructor->DS path preserves the '$'
        # VERBATIM, and that .strip() removes only surrounding whitespace.
        from hummingbot.client.config.config_helpers import (
            ClientConfigAdapter,
            api_keys_from_connector_config_map,
        )
        from hummingbot.connector.exchange.kis.kis_utils import KisConfigMap

        cm = ClientConfigAdapter(
            KisConfigMap(
                kis_app_key="k",
                kis_app_secret="s",
                kis_account_number="12345678-01",
                kis_hts_id="$1766930",
            )
        )
        api_keys = api_keys_from_connector_config_map(cm)
        self.assertEqual("$1766930", api_keys["kis_hts_id"])  # '$' survives config map
        ex = KisExchange(**api_keys, trading_pairs=[self.trading_pair])
        self.assertEqual("$1766930", ex._hts_id)  # '$' preserved through constructor
        ds = ex._create_user_stream_data_source()
        self.assertEqual("$1766930", ds._hts_id)  # forwarded verbatim to the DS
        # .strip() removes whitespace only, NOT the '$' or digits.
        ex2 = KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
            kis_market_routing="sor",
            kis_hts_id="  $1766930  ",
        )
        self.assertEqual("$1766930", ex2._hts_id)

    def test_kis_hts_id_forwarded_as_connect_key(self):
        # is_connect_key=True is the REAL forwarding switch: only is_connect_key
        # fields are passed to the connector constructor
        # (config_helpers.api_keys_from_connector_config_map -> settings.
        # conn_init_parameters -> connector_class(**init_params)). If kis_hts_id
        # is not a connect key it never reaches KisExchange.__init__ and
        # self._hts_id is always "" -> silent REST-only fallback (JEP-180 trap).
        from hummingbot.client.config.config_helpers import (
            ClientConfigAdapter,
            api_keys_from_connector_config_map,
        )
        from hummingbot.connector.exchange.kis.kis_utils import KisConfigMap

        cm = ClientConfigAdapter(
            KisConfigMap(
                kis_app_key="k",
                kis_app_secret="s",
                kis_account_number="12345678-01",
                kis_hts_id="myhts",
            )
        )
        api_keys = api_keys_from_connector_config_map(cm)
        self.assertIn("kis_hts_id", api_keys)
        self.assertEqual("myhts", api_keys["kis_hts_id"])

        # End-to-end (Plan Review F1): the forwarded connect-keys must actually
        # CONSTRUCT the connector (this is what the live factory does:
        # connector_class(**init_params)) and reach self._hts_id. kis_sandbox is
        # is_connect_key=False so it is absent from api_keys -> constructor uses
        # its default; trading_pairs is added the way the factory does.
        ex = KisExchange(**api_keys, trading_pairs=[self.trading_pair])
        self.assertEqual("myhts", ex._hts_id)

    def validate_auth_credentials_present(self, request_call: RequestCall):
        request_headers = request_call.kwargs["headers"]
        self.assertIn("Authorization", request_headers)
        self.assertTrue(
            request_headers["Authorization"].startswith("Bearer "),
            "Authorization header should start with 'Bearer '",
        )
        self.assertIn("appkey", request_headers)
        self.assertEqual("testAppKey", request_headers["appkey"])
        self.assertIn("appsecret", request_headers)

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        # KIS order body fields
        self.assertIn("PDNO", request_data)
        self.assertEqual(
            self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            request_data["PDNO"],
        )
        self.assertEqual(str(int(order.amount)), request_data["ORD_QTY"])
        self.assertEqual(str(int(order.price)), request_data["ORD_UNPR"])
        # SOR/NXT routing: default routing is "sor" (create_exchange_instance)
        self.assertEqual("SOR", request_data["EXCG_ID_DVSN_CD"])
        tr_id = request_call.kwargs["headers"]["tr_id"]
        expected_tr_id = (
            CONSTANTS.DOMESTIC_STOCK_ORDER_BUY_TR_ID
            if order.trade_type == TradeType.BUY
            else CONSTANTS.DOMESTIC_STOCK_ORDER_SELL_TR_ID
        )
        self.assertEqual(expected_tr_id, tr_id)

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(order.exchange_order_id, request_data["ORGN_ODNO"])
        self.assertEqual("SOR", request_data["EXCG_ID_DVSN_CD"])
        self.assertIn("KRX_FWDG_ORD_ORGNO", request_data)
        self.assertEqual(
            CONSTANTS.DOMESTIC_STOCK_CANCEL_TR_ID,
            request_call.kwargs["headers"]["tr_id"],
        )

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual("ALL", request_params["EXCG_ID_DVSN_CD"])
        self.assertEqual(order.exchange_order_id, request_params["ODNO"])

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs["params"]
        self.assertEqual("ALL", request_params["EXCG_ID_DVSN_CD"])
        self.assertEqual(order.exchange_order_id, request_params["ODNO"])

    def configure_successful_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_CANCEL_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_cancelation_request_successful_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_CANCEL_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.post(regex_url, status=400, callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
        self,
        successful_order: InFlightOrder,
        erroneous_order: InFlightOrder,
        mock_api: aioresponses,
    ) -> List[str]:
        all_urls = []
        url = self.configure_successful_cancelation_response(
            order=successful_order, mock_api=mock_api
        )
        all_urls.append(url)
        url = self.configure_erroneous_cancelation_response(
            order=erroneous_order, mock_api=mock_api
        )
        all_urls.append(url)
        return all_urls

    def configure_order_not_found_error_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        # KIS connector has not been updated to validate order not found during cancellation
        raise NotImplementedError

    def configure_order_not_found_error_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> List[str]:
        # KIS connector has not been updated to validate order not found during status update
        raise NotImplementedError

    def configure_completely_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_completely_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_canceled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        # KIS uses the same endpoint for fills and status. Add an empty fill
        # response first (consumed by _all_trade_updates_for_order), then the
        # actual status response (consumed by _request_order_status).
        mock_api.get(regex_url, body=json.dumps(self._empty_fill_mock_response()))
        response = self._order_status_request_canceled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_open_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        # Add empty fill response for _all_trade_updates_for_order
        mock_api.get(regex_url, body=json.dumps(self._empty_fill_mock_response()))
        response = self._order_status_request_open_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_http_error_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        # Add empty fill response for _all_trade_updates_for_order
        mock_api.get(regex_url, body=json.dumps(self._empty_fill_mock_response()))
        mock_api.get(regex_url, status=401, callback=callback)
        return url

    def configure_partially_filled_order_status_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_partially_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_partial_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_fills_request_partial_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=400, callback=callback)
        return url

    def configure_full_fill_trade_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = None,
    ) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_fills_request_full_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    # ------------------------------------------------------------------
    # WebSocket event methods (KIS execution notifications via H0STCNI0)
    # ------------------------------------------------------------------

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        """KIS execution notification for a new (accepted) order.

        CNTG_YN="1" means order acceptance; RCTF_CLS="00" means new order.
        """
        return {
            "type": "execution_notification",
            "tr_id": "H0STCNI0",
            "data": {
                "CUST_ID": "test",
                "ACNT_NO": "12345678-01",
                "ODER_NO": order.exchange_order_id or "EOID1",
                "OODER_NO": "",
                "SELN_BYOV_CLS": "02" if order.trade_type == TradeType.SELL else "01",
                "RCTF_CLS": "00",
                "ODER_KIND": "00",
                "ODER_COND": "0",
                "STCK_SHRN_ISCD": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "CNTG_QTY": "0",
                "CNTG_UNPR": "0",
                "STCK_CNTG_HOUR": "093000",
                "RFUS_YN": "N",
                "CNTG_YN": "1",
                "ACPT_YN": "Y",
                "BRNC_NO": "",
                "ODER_QTY": str(int(order.amount)),
                "ACNT_NAME": "test",
                "ORD_COND_PRC": "0",
                "ORD_EXG_GB": "00",
                "POPUP_YN": "N",
                "FILLER": "",
                "CRDT_CLS": "00",
                "CRDT_LOAN_DATE": "",
                "CNTG_ISNM40": "TEST",
                "ODER_PRC": str(int(order.price)),
            },
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        """KIS execution notification for a canceled order.

        CNTG_YN="1" means order event; RCTF_CLS="02" means cancel.
        """
        return {
            "type": "execution_notification",
            "tr_id": "H0STCNI0",
            "data": {
                "CUST_ID": "test",
                "ACNT_NO": "12345678-01",
                "ODER_NO": order.exchange_order_id or "EOID1",
                "OODER_NO": "",
                "SELN_BYOV_CLS": "02" if order.trade_type == TradeType.SELL else "01",
                "RCTF_CLS": "02",
                "ODER_KIND": "00",
                "ODER_COND": "0",
                "STCK_SHRN_ISCD": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "CNTG_QTY": "0",
                "CNTG_UNPR": "0",
                "STCK_CNTG_HOUR": "093100",
                "RFUS_YN": "N",
                "CNTG_YN": "1",
                "ACPT_YN": "Y",
                "BRNC_NO": "",
                "ODER_QTY": str(int(order.amount)),
                "ACNT_NAME": "test",
                "ORD_COND_PRC": "0",
                "ORD_EXG_GB": "00",
                "POPUP_YN": "N",
                "FILLER": "",
                "CRDT_CLS": "00",
                "CRDT_LOAN_DATE": "",
                "CNTG_ISNM40": "TEST",
                "ODER_PRC": str(int(order.price)),
            },
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        """KIS execution notification for a fully filled order.

        CNTG_YN="2" means fill notification; CNTG_QTY and CNTG_UNPR carry fill data.
        """
        return {
            "type": "execution_notification",
            "tr_id": "H0STCNI0",
            "data": {
                "CUST_ID": "test",
                "ACNT_NO": "12345678-01",
                "ODER_NO": order.exchange_order_id or "EOID1",
                "OODER_NO": "",
                "SELN_BYOV_CLS": "02" if order.trade_type == TradeType.SELL else "01",
                "RCTF_CLS": "00",
                "ODER_KIND": "00",
                "ODER_COND": "0",
                "STCK_SHRN_ISCD": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "CNTG_QTY": str(int(order.amount)),
                "CNTG_UNPR": str(int(order.price)),
                "STCK_CNTG_HOUR": "093200",
                "RFUS_YN": "N",
                "CNTG_YN": "2",
                "ACPT_YN": "Y",
                "BRNC_NO": "",
                "ODER_QTY": str(int(order.amount)),
                "ACNT_NAME": "test",
                "ORD_COND_PRC": "0",
                "ORD_EXG_GB": "00",
                "POPUP_YN": "N",
                "FILLER": "",
                "CRDT_CLS": "00",
                "CRDT_LOAN_DATE": "",
                "CNTG_ISNM40": "TEST",
                "ODER_PRC": str(int(order.price)),
            },
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        """KIS execution notification carrying trade/fill data.

        Same structure as the full fill order event (CNTG_YN="2").
        """
        return {
            "type": "execution_notification",
            "tr_id": "H0STCNI0",
            "data": {
                "CUST_ID": "test",
                "ACNT_NO": "12345678-01",
                "ODER_NO": order.exchange_order_id or "EOID1",
                "OODER_NO": "",
                "SELN_BYOV_CLS": "02" if order.trade_type == TradeType.SELL else "01",
                "RCTF_CLS": "00",
                "ODER_KIND": "00",
                "ODER_COND": "0",
                "STCK_SHRN_ISCD": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                "CNTG_QTY": str(int(order.amount)),
                "CNTG_UNPR": str(int(order.price)),
                "STCK_CNTG_HOUR": "093200",
                "RFUS_YN": "N",
                "CNTG_YN": "2",
                "ACPT_YN": "Y",
                "BRNC_NO": "",
                "ODER_QTY": str(int(order.amount)),
                "ACNT_NAME": "test",
                "ORD_COND_PRC": "0",
                "ORD_EXG_GB": "00",
                "POPUP_YN": "N",
                "FILLER": "",
                "CRDT_CLS": "00",
                "CRDT_LOAN_DATE": "",
                "CNTG_ISNM40": "TEST",
                "ODER_PRC": str(int(order.price)),
            },
        }

    # ------------------------------------------------------------------
    # Tests disabled for KIS (order-not-found not implemented, synthetic
    # trading rules, WS tests need mock infrastructure)
    # ------------------------------------------------------------------

    @aioresponses()
    def test_cancel_order_not_found_in_the_exchange(self, mock_api):
        # Disabled: KIS connector has not been updated to validate
        # order not found during cancellation
        pass

    @aioresponses()
    def test_lost_order_removed_if_not_found_during_order_status_update(self, mock_api):
        # Disabled: KIS connector has not been updated to validate
        # order not found during status update
        pass

    @aioresponses()
    def test_update_trading_rules_ignores_rule_with_error(self, mock_api):
        # Disabled: KIS uses a synthetic trading rules response
        pass

    async def test_user_stream_update_for_new_order(self):
        # TODO: Re-enable when WS mock infrastructure is ready
        pass

    async def test_user_stream_update_for_canceled_order(self):
        # TODO: Re-enable when WS mock infrastructure is ready
        pass

    @aioresponses()
    async def test_user_stream_update_for_order_full_fill(self, mock_api):
        # TODO: Re-enable when WS mock infrastructure is ready
        pass

    async def test_user_stream_balance_update(self):
        # Disabled: KIS does not provide balance updates through WebSocket
        # (balances are REST-polled)
        pass

    async def test_user_stream_raises_cancel_exception(self):
        # TODO: Re-enable when WS mock infrastructure is ready
        pass

    async def test_user_stream_logs_errors(self):
        # TODO: Re-enable when WS mock infrastructure is ready
        pass

    async def test_lost_order_removed_after_cancel_status_user_event_received(self):
        # TODO: Re-enable when WS mock infrastructure is ready
        pass

    @aioresponses()
    async def test_lost_order_user_stream_full_fill_events_are_processed(self, mock_api):
        # TODO: Re-enable when WS mock infrastructure is ready
        pass

    # ------------------------------------------------------------------
    # Regression: REST fill detection must parse the REAL KIS
    # inquire-daily-ccld response shape (output1 list + output2 + lowercase fields).
    #
    # The live KIS hedge (JEP-162, 2026-06-18) FILLED on the exchange, but the
    # connector never detected it and the order stayed OPEN forever. Root cause:
    # the parser read container "output" + UPPERCASE keys (CCLD_QTY/ORD_QTY/
    # CNCL_YN/ODNO/AVG_PRVS/TOT_CCLD_AMT), while KIS returns "output1" (+ "output2")
    # with lowercase tot_ccld_qty/ord_qty/cncl_yn/odno/avg_prvs/tot_ccld_amt. The
    # whole prior mock suite used the fictional shape, so every fill test was GREEN
    # against a response KIS never sends (confirmed vs the stratops nautilus KIS
    # adapter). These pin the real shape so the bug cannot return.
    # ------------------------------------------------------------------

    @staticmethod
    def _real_daily_ccld_filled_response(odno: str, ord_qty: int, fill_price: int) -> dict:
        """Real KIS inquire-daily-ccld (TTTC8001R) FULL-fill shape: output1 list of
        order rows (lowercase fields) + output2 summary."""
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "정상처리 되었습니다.",
            "output1": [
                {
                    "ord_dt": "20260618",
                    "odno": odno,
                    "orgn_odno": "",
                    "pdno": "005930",
                    "ord_qty": str(ord_qty),
                    "ord_unpr": str(fill_price),
                    "tot_ccld_qty": str(ord_qty),
                    "avg_prvs": str(fill_price),
                    "cncl_yn": "N",
                    "tot_ccld_amt": str(ord_qty * fill_price),
                    "rmn_qty": "0",
                    "sll_buy_dvsn_cd": "02",
                }
            ],
            "output2": {
                "tot_ord_qty": str(ord_qty),
                "tot_ccld_qty": str(ord_qty),
                "tot_ccld_amt": str(ord_qty * fill_price),
                "pchs_avg_pric": str(fill_price),
                "prsm_tlex_smtl": "0",
            },
        }

    def _make_kis_order(self, odno="0000111122223333", amount="1", executed="0",
                        trade_type=TradeType.BUY):
        from unittest.mock import MagicMock
        order = MagicMock()
        order.client_order_id = "CID-hedge-1"
        order.exchange_order_id = odno
        order.trading_pair = self.trading_pair
        order.trade_type = trade_type
        order.base_asset = self.base_asset
        order.quote_asset = self.quote_asset
        order.amount = Decimal(amount)
        order.executed_amount_base = Decimal(executed)
        return order

    def test_rest_fill_updates_parse_real_daily_ccld_shape(self):
        # RED before fix: parser read container "output" + UPPERCASE keys, so a real
        # output1/lowercase response yielded ZERO fills -> live hedge stayed OPEN.
        order = self._make_kis_order(odno="ODNO-REAL-1", amount="1", executed="0")
        resp = self._real_daily_ccld_filled_response(odno="ODNO-REAL-1", ord_qty=1, fill_price=360000)
        updates = self.exchange._create_order_fill_updates(order=order, fill_data=resp)
        self.assertEqual(1, len(updates), "real KIS output1 full fill must yield exactly one TradeUpdate")
        self.assertEqual(Decimal("1"), updates[0].fill_base_amount)
        self.assertEqual(Decimal("360000"), updates[0].fill_price)

    def test_rest_order_state_infers_filled_from_real_row(self):
        # RED before fix: _infer_order_state read UPPERCASE CCLD_QTY/ORD_QTY -> 0 -> OPEN forever.
        order = self._make_kis_order(odno="ODNO-REAL-2", amount="1")
        row = self._real_daily_ccld_filled_response(odno="ODNO-REAL-2", ord_qty=1, fill_price=360000)["output1"][0]
        self.assertEqual(OrderState.FILLED, self.exchange._infer_order_state(row, order))

    def test_rest_fill_updates_emit_delta_not_cumulative(self):
        # tot_ccld_qty is CUMULATIVE per order; the connector must emit the INCREMENT vs the
        # order's already-executed base so re-polling never double-counts and partial->full is correct.
        order = self._make_kis_order(odno="ODNO-REAL-3", amount="2", executed="1")
        resp = self._real_daily_ccld_filled_response(odno="ODNO-REAL-3", ord_qty=2, fill_price=100)
        updates = self.exchange._create_order_fill_updates(order=order, fill_data=resp)
        self.assertEqual(1, len(updates))
        self.assertEqual(Decimal("1"), updates[0].fill_base_amount, "delta = 2 cumulative - 1 already executed")

    def test_rest_order_state_infers_canceled_from_real_row(self):
        order = self._make_kis_order(odno="ODNO-REAL-4", amount="1")
        row = {"odno": "ODNO-REAL-4", "ord_qty": "1", "tot_ccld_qty": "0", "cncl_yn": "Y", "rmn_qty": "1"}
        self.assertEqual(OrderState.CANCELED, self.exchange._infer_order_state(row, order))

    # ------------------------------------------------------------------
    # Private helpers: KIS-formatted mock responses
    # ------------------------------------------------------------------

    def _order_cancelation_request_successful_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "rt_cd": "0",
            "msg_cd": "APBK0013",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "ODNO": order.exchange_order_id or self.expected_exchange_order_id,
                "ORD_TMD": "131530",
            }
        }

    def _kis_ccld_response(self, order: InFlightOrder, *, filled_qty, cncl: str = "N",
                           price=None, ord_qty=None) -> dict:
        """Build a REAL KIS inquire-daily-ccld response: output1 (list of order rows,
        lowercase fields) + output2 (summary). KIS uses the same endpoint for status and
        fills, and reports cumulative tot_ccld_qty per order (no per-fill commission).
        ``ord_qty`` defaults to the order amount; partial-fill fixtures pass a larger
        ord_qty so tot_ccld_qty < ord_qty reads as PARTIALLY_FILLED (state is inferred,
        not parsed — KIS has no explicit status field on this endpoint)."""
        odno = str(order.exchange_order_id or self.expected_exchange_order_id)
        fq = int(filled_qty)
        px = int(price) if price is not None else int(order.price)
        ord_qty = int(ord_qty) if ord_qty is not None else int(order.amount)
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output1": [
                {
                    "ord_dt": "20260618",
                    "odno": odno,
                    "orgn_odno": "",
                    "pdno": self.exchange_symbol_for_tokens(order.base_asset, order.quote_asset),
                    "sll_buy_dvsn_cd": "02" if order.trade_type == TradeType.BUY else "01",
                    "ord_qty": str(ord_qty),
                    "ord_unpr": str(int(order.price)),
                    "tot_ccld_qty": str(fq),
                    "avg_prvs": str(px) if fq > 0 else "0",
                    "cncl_yn": cncl,
                    "tot_ccld_amt": str(fq * px),
                    "rmn_qty": str(ord_qty - fq),
                }
            ],
            "output2": {
                "tot_ord_qty": str(ord_qty),
                "tot_ccld_qty": str(fq),
                "tot_ccld_amt": str(fq * px),
                "pchs_avg_pric": str(px) if fq > 0 else "0",
                "prsm_tlex_smtl": "0",
            },
        }

    def _order_status_request_completely_filled_mock_response(self, order: InFlightOrder) -> Any:
        # Real shape: full fill -> tot_ccld_qty == ord_qty, avg_prvs == order.price so the
        # framework's fill_event.price == order.price assertion holds.
        return self._kis_ccld_response(order, filled_qty=int(order.amount), price=int(order.price))

    def _order_status_request_canceled_mock_response(self, order: InFlightOrder) -> Any:
        return self._kis_ccld_response(order, filled_qty=0, cncl="Y")

    def _order_status_request_open_mock_response(self, order: InFlightOrder) -> Any:
        return self._kis_ccld_response(order, filled_qty=0, cncl="N")

    def _order_status_request_partially_filled_mock_response(self, order: InFlightOrder) -> Any:
        partial = int(self.expected_partial_fill_amount)
        return self._kis_ccld_response(
            order,
            filled_qty=partial,
            price=int(self.expected_partial_fill_price),
            ord_qty=partial * 2,  # real partial: filled < ordered -> PARTIALLY_FILLED
        )

    def _order_fills_request_full_fill_mock_response(self, order: InFlightOrder) -> Any:
        return self._kis_ccld_response(order, filled_qty=int(order.amount), price=int(order.price))

    def _empty_fill_mock_response(self) -> dict:
        """No-fill inquire-daily-ccld response (empty output1) for _all_trade_updates_for_order."""
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output1": [],
            "output2": {
                "tot_ord_qty": "0", "tot_ccld_qty": "0", "tot_ccld_amt": "0",
                "pchs_avg_pric": "0", "prsm_tlex_smtl": "0",
            },
        }

    def _order_fills_request_partial_fill_mock_response(self, order: InFlightOrder) -> Any:
        partial = int(self.expected_partial_fill_amount)
        return self._kis_ccld_response(
            order,
            filled_qty=partial,
            price=int(self.expected_partial_fill_price),
            ord_qty=partial * 2,  # real partial: filled < ordered -> PARTIALLY_FILLED
        )

    # ------------------------------------------------------------------
    # JEP-206: EGW00123 expired-token deadlock break
    # (check_network refresh + single retry; EGW00215 stays on the debounce;
    #  >=60s forced-reauth cooldown; classify on structured msg_cd + buried-500 substring)
    # ------------------------------------------------------------------

    def _egw_ioerror(self, code: str) -> IOError:
        # Mirrors the live HTTP-500 surface: the EGW code is buried as a SUBSTRING
        # inside the IOError message the shared REST layer raises on a 500. A plain
        # IOError (no .msg_cd attr) forces classification down the substring fallback.
        body = json.dumps(self._rejection_response("1", code, f"err {code}"))
        return IOError(
            f"Error executing request GET .../inquire-balance. "
            f"HTTP status is 500. Error: {body}"
        )

    def _token_post_handler(self, fresh: str = "fresh_token") -> dict:
        from datetime import datetime as _dt, timedelta as _td
        future = (_dt.now() + _td(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        return {"access_token": fresh, "access_token_token_expired": future}

    # --- injected-error branch tests (logic) -------------------------------------

    async def test_check_network_egw00123_refreshes_token_and_recovers(self):
        """JEP-206: a SUSTAINED expired-token signal (EGW00123) must NOT deadlock at
        NOT_CONNECTED. check_network invalidates the cached token and retries the probe
        ONCE; the retry succeeds (fresh token) -> CONNECTED, counter reset, WS hub stays
        up. RED before the fix: today both the probe and its (nonexistent) retry funnel
        into the debounce; after 3x consecutive it flips NOT_CONNECTED -> deadlock."""
        from unittest.mock import patch, AsyncMock
        ex = self._authed_exchange()
        ex._auth.invalidate_token = AsyncMock()
        # First call: expired-token error. Retry (2nd call): success (returns None).
        probe = AsyncMock(side_effect=[self._egw_ioerror(CONSTANTS.KIS_ERR_TOKEN_EXPIRED), None])
        with patch.object(ex, "_make_network_check_request", new=probe):
            status = await ex.check_network()
        self.assertEqual(NetworkStatus.CONNECTED, status)
        self.assertEqual(0, ex._net_check_consec_failures)       # no failure counted on recovery
        ex._auth.invalidate_token.assert_awaited_once()          # token was force-refreshed
        self.assertEqual(2, probe.await_count)                   # probe retried exactly once

    async def test_check_network_egw00123_retry_also_fails_counts_one(self):
        """JEP-206: if the refresh+retry ALSO fails, it counts as exactly ONE failure and
        falls through to the unchanged JEP-204 debounce (stays CONNECTED on the first, not
        NOT_CONNECTED). Guards against double-counting the probe + the retry."""
        from unittest.mock import patch, AsyncMock
        ex = self._authed_exchange()
        ex._auth.invalidate_token = AsyncMock()
        probe = AsyncMock(side_effect=self._egw_ioerror(CONSTANTS.KIS_ERR_TOKEN_EXPIRED))
        with patch.object(ex, "_make_network_check_request", new=probe):
            status = await ex.check_network()
        self.assertEqual(NetworkStatus.CONNECTED, status)        # debounce still tolerates the 1st
        self.assertEqual(1, ex._net_check_consec_failures)       # counted exactly once (not twice)
        ex._auth.invalidate_token.assert_awaited_once()
        self.assertEqual(2, probe.await_count)                   # probe + one retry

    async def test_check_network_egw00123_reauth_cooldown_skips_second_refresh(self):
        """JEP-206 (review fix #2/#4): a genuinely revoked appkey keeps returning EGW00123.
        The first cycle re-auths once; a second cycle WITHIN the 60s cooldown must NOT re-auth
        again (no tokenP thrash) and instead applies the plain debounce. So invalidate_token is
        awaited exactly ONCE across two cycles, and the probe is hit probe+retry (cycle 1) +
        probe-only (cycle 2) = 3 times. Counter advances to 2 (one failure per cycle)."""
        from unittest.mock import patch, AsyncMock
        ex = self._authed_exchange()
        ex._auth.invalidate_token = AsyncMock()
        probe = AsyncMock(side_effect=self._egw_ioerror(CONSTANTS.KIS_ERR_TOKEN_EXPIRED))
        with patch.object(ex, "_make_network_check_request", new=probe):
            await ex.check_network()   # cycle 1: re-auth + retry (retry fails) -> count 1
            await ex.check_network()   # cycle 2: within cooldown -> no re-auth -> count 2
        self.assertEqual(1, ex._auth.invalidate_token.await_count)  # cooldown blocked the 2nd re-auth
        self.assertEqual(3, probe.await_count)                      # 2 (cycle1) + 1 (cycle2)
        self.assertEqual(2, ex._net_check_consec_failures)

    async def test_check_network_egw00215_throttle_does_not_refresh(self):
        """JEP-206: EGW00215 (per-second throttle) is a TRANSIENT non-auth failure — it must
        NOT force a token refresh (which would hammer the 1/min tokenP limit) and must stay on
        the existing debounce. First throttle stays CONNECTED, no invalidate_token call, no retry."""
        from unittest.mock import patch, AsyncMock
        ex = self._authed_exchange()
        ex._auth.invalidate_token = AsyncMock()
        probe = AsyncMock(side_effect=self._egw_ioerror(CONSTANTS.KIS_ERR_THROTTLE))
        with patch.object(ex, "_make_network_check_request", new=probe):
            status = await ex.check_network()
        self.assertEqual(NetworkStatus.CONNECTED, status)        # debounced as transient
        self.assertEqual(1, ex._net_check_consec_failures)
        ex._auth.invalidate_token.assert_not_awaited()           # NO refresh on throttle
        self.assertEqual(1, probe.await_count)                   # NO retry on throttle

    # --- REAL-PATH tests (review fix #3): drive the genuine
    #     check_network -> _make_network_check_request -> _api_get chain via aioresponses,
    #     mocking ONLY the tokenP POST. These prove the LIVE error string/field is actually
    #     classifiable — not just a hand-crafted IOError the matcher was written to catch.

    @aioresponses()
    async def test_check_network_http200_egw00123_real_path_refreshes(self, mock_api):
        """JEP-206 review #2/#6: the HTTP-200 + rt_cd=1 expired-token variant carries the code
        ONLY in msg_cd (msg1 is Korean text with no 'EGW00123' substring). The connector's own
        raise must surface msg_cd structurally so check_network classifies it, re-auths via the
        REAL _api_get -> rest_authenticate -> _get_access_token -> tokenP POST chain, and recovers.
        RED before the fix: the old raise dropped msg_cd, so EGW00123 was invisible here."""
        from hummingbot.connector.exchange.kis.kis_auth import _REST_URL, _TOKEN_PATH
        ex = self._authed_exchange()
        import tempfile as _tf, shutil as _sh
        _d = _tf.mkdtemp(prefix="kis_reauth_iso_"); self.addCleanup(_sh.rmtree, _d, True)
        ex._auth._token_cache_path = _tf.mkstemp(dir=_d, suffix=".json")[1]
        # Server has invalidated the token early; local clock still says valid.
        ex._auth._access_token = "server_dead_token"
        ex._auth._token_expires_at = time.time() + 86400
        balance_regex = re.compile(f"^{re.escape(self.balance_url)}.*")
        # Probe #1 (HTTP 200, rt_cd=1, msg_cd=EGW00123, no code in msg1) -> raises KisNetworkCheckError.
        mock_api.get(
            balance_regex,
            body=json.dumps({
                "rt_cd": "1",
                "msg_cd": CONSTANTS.KIS_ERR_TOKEN_EXPIRED,
                "msg1": "기간이 만료된 token 입니다.",
            }),
        )
        # Forced re-auth: tokenP POST returns a fresh token.
        mock_api.post(
            f"{_REST_URL}/{_TOKEN_PATH}",
            body=json.dumps(self._token_post_handler()),
        )
        # Probe #2 (retry) succeeds.
        mock_api.get(
            balance_regex,
            body=json.dumps(self.network_status_request_successful_mock_response),
        )
        status = await ex.check_network()
        self.assertEqual(NetworkStatus.CONNECTED, status)
        self.assertEqual("fresh_token", ex._auth._access_token)  # token was actually re-issued
        self.assertEqual(0, ex._net_check_consec_failures)

    @aioresponses()
    async def test_check_network_http500_egw00123_real_path_refreshes(self, mock_api):
        """JEP-206 review #3: the live incident shape — EGW00123 returned as HTTP 500 with the
        code BURIED in the IOError body string (no structured msg_cd on the raised exception).
        The substring fallback must classify it, re-auth via the real chain, and recover."""
        from hummingbot.connector.exchange.kis.kis_auth import _REST_URL, _TOKEN_PATH
        ex = self._authed_exchange()
        import tempfile as _tf, shutil as _sh
        _d = _tf.mkdtemp(prefix="kis_reauth_iso_"); self.addCleanup(_sh.rmtree, _d, True)
        ex._auth._token_cache_path = _tf.mkstemp(dir=_d, suffix=".json")[1]
        ex._auth._access_token = "server_dead_token"
        ex._auth._token_expires_at = time.time() + 86400
        balance_regex = re.compile(f"^{re.escape(self.balance_url)}.*")
        # Probe #1 (HTTP 500, EGW00123 in body) -> shared REST layer raises plain IOError w/ body text.
        mock_api.get(
            balance_regex,
            status=500,
            body=json.dumps(self._rejection_response(
                "1", CONSTANTS.KIS_ERR_TOKEN_EXPIRED, "기간이 만료된 token 입니다.")),
        )
        mock_api.post(
            f"{_REST_URL}/{_TOKEN_PATH}",
            body=json.dumps(self._token_post_handler()),
        )
        # Probe #2 (retry) succeeds.
        mock_api.get(
            balance_regex,
            body=json.dumps(self.network_status_request_successful_mock_response),
        )
        status = await ex.check_network()
        self.assertEqual(NetworkStatus.CONNECTED, status)
        self.assertEqual("fresh_token", ex._auth._access_token)
        self.assertEqual(0, ex._net_check_consec_failures)

    @aioresponses()
    async def test_check_network_http500_egw00215_real_path_no_refresh(self, mock_api):
        """JEP-206 review #3: real-path EGW00215 throttle (HTTP 500) must NOT re-auth — the token
        is untouched and the failure stays on the debounce (CONNECTED on the first). No tokenP POST
        is registered, so a spurious re-auth would raise an aioresponses 'no mock' error."""
        ex = self._authed_exchange()
        ex._auth._access_token = "live_token"
        ex._auth._token_expires_at = time.time() + 86400
        balance_regex = re.compile(f"^{re.escape(self.balance_url)}.*")
        mock_api.get(
            balance_regex,
            status=500,
            body=json.dumps(self._rejection_response(
                "1", CONSTANTS.KIS_ERR_THROTTLE, "초당 거래건수를 초과하였습니다.")),
            repeat=True,
        )
        status = await ex.check_network()
        self.assertEqual(NetworkStatus.CONNECTED, status)        # transient -> debounced
        self.assertEqual(1, ex._net_check_consec_failures)
        self.assertEqual("live_token", ex._auth._access_token)   # token NOT re-issued

    # --- structured-error + invalidate primitive ---------------------------------

    def test_make_network_check_request_raises_with_msg_cd_attr(self):
        """JEP-206 review #2: the HTTP-200 rt_cd!=0 raise must carry msg_cd as a structured
        attribute (KisNetworkCheckError) so check_network classifies on the field, not free text."""
        from hummingbot.connector.exchange.kis.kis_exchange import KisNetworkCheckError
        err = KisNetworkCheckError("boom", msg_cd=CONSTANTS.KIS_ERR_TOKEN_EXPIRED)
        self.assertEqual(CONSTANTS.KIS_ERR_TOKEN_EXPIRED, err.msg_cd)
        self.assertIsInstance(err, IOError)   # still an IOError -> base fail-closed paths unchanged

    async def test_invalidate_token_forces_refetch(self):
        """KisAuth.invalidate_token zeroes the cached token + expiry so the next
        _get_access_token re-issues (the primitive the check_network fix relies on)."""
        ex = self._authed_exchange()
        self.assertIsNotNone(ex._auth._access_token)
        await ex._auth.invalidate_token()
        self.assertIsNone(ex._auth._access_token)
        self.assertEqual(0.0, ex._auth._token_expires_at)
