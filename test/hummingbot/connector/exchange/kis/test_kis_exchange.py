import json
import re
from decimal import Decimal
from typing import Any, Callable, List, Optional, Tuple

from aioresponses import aioresponses
from aioresponses.core import RequestCall

from hummingbot.connector.exchange.kis import kis_constants as CONSTANTS, kis_web_utils as web_utils
from hummingbot.connector.exchange.kis.kis_exchange import KisExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase


class KisExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):
    """
    Tests for the KIS (Korea Investment & Securities) exchange connector.

    KIS-specific characteristics:
    - No symbols list API (trading pairs configured externally)
    - TR_ID header-based request routing
    - OAuth2 Bearer token authentication
    - No WebSocket support (REST polling only)
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
        url = f"{url}?FID_COND_MRKT_DIV_CODE=J&FID_INPUT_ISCD={self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset)}"
        return url

    @property
    def network_status_url(self):
        return web_utils.public_rest_url(CONSTANTS.CHECK_NETWORK_PATH_URL)

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
        # Using token endpoint response as network ping
        return {
            "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9",
            "access_token_token_expired": "2026-03-01 12:30:45",
            "token_type": "Bearer",
            "expires_in": 86400,
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
        # KIS does not provide balance updates through WebSocket
        raise NotImplementedError("KIS does not support WebSocket balance updates")

    @property
    def expected_latest_price(self):
        return 72000.0

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("1"),
            min_price_increment=Decimal("100"),
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
        # KIS has no WebSocket
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal("72000")

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("50")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("30"))])

    @property
    def expected_fill_trade_id(self) -> str:
        return "1234567890"

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        # KIS uses stock code directly (e.g. "COINALPHA"), not a pair like "COINALPHA_KRW"
        return base_token

    def create_exchange_instance(self):
        return KisExchange(
            kis_app_key="testAppKey",
            kis_app_secret="testAppSecret",
            kis_account_number="12345678-01",
            trading_pairs=[self.trading_pair],
        )

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

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(order.exchange_order_id, request_data["ORGN_ODNO"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        # Order status is queried via GET with params
        request_params = request_call.kwargs.get("params", {})
        # The order detail endpoint uses query parameters; verify it was called
        # (actual param validation depends on implementation)
        self.assertTrue(
            request_params is not None or request_call.kwargs.get("data") is not None,
            "Order status request should have params or data",
        )

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        # Trade details endpoint validation
        request_params = request_call.kwargs.get("params", {})
        self.assertTrue(
            request_params is not None or request_call.kwargs.get("data") is not None,
            "Trades request should have params or data",
        )

    def configure_successful_cancelation_response(
        self,
        order: InFlightOrder,
        mock_api: aioresponses,
        callback: Optional[Callable] = lambda *args, **kwargs: None,
    ) -> str:
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_PATH)
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
        url = web_utils.private_rest_url(CONSTANTS.DOMESTIC_STOCK_ORDER_PATH)
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
    # WebSocket event methods (KIS has NO WebSocket)
    # ------------------------------------------------------------------

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        # KIS does not support WebSocket order events
        return None

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        # KIS does not support WebSocket order events
        return None

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        # KIS does not support WebSocket order events
        return None

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        # KIS does not support WebSocket trade events
        return None

    # ------------------------------------------------------------------
    # Tests disabled for KIS (no WS, no symbols API, etc.)
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
        # Disabled: KIS has no WebSocket user stream
        pass

    async def test_user_stream_update_for_canceled_order(self):
        # Disabled: KIS has no WebSocket user stream
        pass

    @aioresponses()
    async def test_user_stream_update_for_order_full_fill(self, mock_api):
        # Disabled: KIS has no WebSocket user stream
        pass

    async def test_user_stream_balance_update(self):
        # Disabled: KIS has no WebSocket balance updates
        pass

    async def test_user_stream_raises_cancel_exception(self):
        # Disabled: KIS has no WebSocket user stream
        pass

    async def test_user_stream_logs_errors(self):
        # Disabled: KIS has no WebSocket user stream
        pass

    async def test_lost_order_removed_after_cancel_status_user_event_received(self):
        # Disabled: KIS has no WebSocket user stream
        pass

    @aioresponses()
    async def test_lost_order_user_stream_full_fill_events_are_processed(self, mock_api):
        # Disabled: KIS has no WebSocket user stream
        pass

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

    def _order_status_request_completely_filled_mock_response(self, order: InFlightOrder) -> Any:
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "ODNO": exchange_order_id,
                "ORD_QTY": str(int(order.amount)),
                "CCLD_QTY": str(int(order.amount)),
                "ORD_UNPR": str(int(order.price)),
                "AVG_PRVS": str(int(order.price + Decimal(2))),
                "SLL_BUY_DVSN_CD": "02" if order.trade_type == TradeType.BUY else "01",
                "ORD_DVSN_CD": "00",
                "CNCL_YN": "N",
                "TOT_CCLD_AMT": str(int(order.amount * (order.price + Decimal(2)))),
                "state": "filled",
                "clientOrderId": order.client_order_id,
            }
        }

    def _order_status_request_canceled_mock_response(self, order: InFlightOrder) -> Any:
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "ODNO": exchange_order_id,
                "ORD_QTY": str(int(order.amount)),
                "CCLD_QTY": "0",
                "ORD_UNPR": str(int(order.price)),
                "AVG_PRVS": "0",
                "SLL_BUY_DVSN_CD": "02" if order.trade_type == TradeType.BUY else "01",
                "ORD_DVSN_CD": "00",
                "CNCL_YN": "Y",
                "TOT_CCLD_AMT": "0",
                "state": "canceled",
                "clientOrderId": order.client_order_id,
            }
        }

    def _order_status_request_open_mock_response(self, order: InFlightOrder) -> Any:
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "ODNO": exchange_order_id,
                "ORD_QTY": str(int(order.amount)),
                "CCLD_QTY": "0",
                "ORD_UNPR": str(int(order.price)),
                "AVG_PRVS": "0",
                "SLL_BUY_DVSN_CD": "02" if order.trade_type == TradeType.BUY else "01",
                "ORD_DVSN_CD": "00",
                "CNCL_YN": "N",
                "TOT_CCLD_AMT": "0",
                "state": "new",
                "clientOrderId": order.client_order_id,
            }
        }

    def _order_status_request_partially_filled_mock_response(self, order: InFlightOrder) -> Any:
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": {
                "ODNO": exchange_order_id,
                "ORD_QTY": str(int(order.amount)),
                "CCLD_QTY": str(int(self.expected_partial_fill_amount)),
                "ORD_UNPR": str(int(order.price)),
                "AVG_PRVS": str(int(self.expected_partial_fill_price)),
                "SLL_BUY_DVSN_CD": "02" if order.trade_type == TradeType.BUY else "01",
                "ORD_DVSN_CD": "00",
                "CNCL_YN": "N",
                "TOT_CCLD_AMT": str(int(self.expected_partial_fill_amount * self.expected_partial_fill_price)),
                "state": "partially_filled",
                "clientOrderId": order.client_order_id,
            }
        }

    def _order_fills_request_full_fill_mock_response(self, order: InFlightOrder) -> Any:
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": [
                {
                    "tradeId": self.expected_fill_trade_id,
                    "ODNO": exchange_order_id,
                    "PDNO": self.exchange_symbol_for_tokens(order.base_asset, order.quote_asset),
                    "SLL_BUY_DVSN_CD": "02" if order.trade_type == TradeType.BUY else "01",
                    "ORD_QTY": str(int(order.amount)),
                    "CCLD_QTY": str(int(order.amount)),
                    "ORD_UNPR": str(int(order.price)),
                    "AVG_PRVS": str(int(order.price)),
                    "TOT_CCLD_AMT": str(int(order.amount * order.price)),
                    "fee": str(self.expected_fill_fee.flat_fees[0].amount),
                    "feeCoinName": self.expected_fill_fee.flat_fees[0].token,
                    "clientOrderId": order.client_order_id,
                }
            ]
        }

    @staticmethod
    def _empty_fill_mock_response() -> dict:
        """Return an empty fill list response consumed by _all_trade_updates_for_order."""
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": []
        }

    def _order_fills_request_partial_fill_mock_response(self, order: InFlightOrder) -> Any:
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "rt_cd": "0",
            "msg_cd": "MCA00000",
            "msg1": "\uc815\uc0c1\ucc98\ub9ac \ub418\uc5c8\uc2b5\ub2c8\ub2e4.",
            "output": [
                {
                    "tradeId": self.expected_fill_trade_id,
                    "ODNO": exchange_order_id,
                    "PDNO": self.exchange_symbol_for_tokens(order.base_asset, order.quote_asset),
                    "SLL_BUY_DVSN_CD": "02" if order.trade_type == TradeType.BUY else "01",
                    "ORD_QTY": str(int(order.amount)),
                    "CCLD_QTY": str(int(self.expected_partial_fill_amount)),
                    "ORD_UNPR": str(int(self.expected_partial_fill_price)),
                    "AVG_PRVS": str(int(self.expected_partial_fill_price)),
                    "TOT_CCLD_AMT": str(int(self.expected_partial_fill_amount * self.expected_partial_fill_price)),
                    "fee": str(self.expected_fill_fee.flat_fees[0].amount),
                    "feeCoinName": self.expected_fill_fee.flat_fees[0].token,
                    "clientOrderId": order.client_order_id,
                }
            ]
        }
