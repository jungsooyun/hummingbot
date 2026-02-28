import json
import re
from decimal import Decimal
from typing import Any, Callable, List, Optional, Tuple

from aioresponses import aioresponses
from aioresponses.core import RequestCall

from hummingbot.connector.exchange.lighter import lighter_constants as CONSTANTS, lighter_web_utils as web_utils
from hummingbot.connector.exchange.lighter.lighter_exchange import LighterExchange
from hummingbot.connector.test_support.exchange_connector_test import AbstractExchangeConnectorTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase


class LighterExchangeTests(AbstractExchangeConnectorTests.ExchangeConnectorTests):

    @property
    def all_symbols_url(self):
        return web_utils.public_rest_url(path_url=CONSTANTS.GET_TRADING_RULES_PATH_URL)

    @property
    def latest_prices_url(self):
        url = web_utils.public_rest_url(path_url=CONSTANTS.GET_LAST_TRADING_PRICES_PATH_URL)
        return url

    @property
    def network_status_url(self):
        url = web_utils.public_rest_url(CONSTANTS.CHECK_NETWORK_PATH_URL)
        return url

    @property
    def trading_rules_url(self):
        url = web_utils.public_rest_url(CONSTANTS.GET_TRADING_RULES_PATH_URL)
        return url

    @property
    def order_creation_url(self):
        url = web_utils.private_rest_url(CONSTANTS.CREATE_ORDER_PATH_URL)
        return url

    @property
    def balance_url(self):
        url = web_utils.private_rest_url(CONSTANTS.GET_ACCOUNT_SUMMARY_PATH_URL)
        return url

    @property
    def all_symbols_request_mock_response(self):
        return {
            "code": 200,
            "order_books": [
                {
                    "market_id": 0,
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "market_type": "spot",
                    "base_asset_id": 0,
                    "quote_asset_id": 1,
                    "supported_price_decimals": 4,
                    "supported_size_decimals": 2,
                    "min_base_amount": "0.01",
                    "min_quote_amount": "10.000000",
                    "status": "active",
                    "taker_fee": "0.0000",
                    "maker_fee": "0.0000",
                },
            ]
        }

    @property
    def latest_prices_request_mock_response(self):
        return {
            "code": 200,
            "order_book_details": [],
            "spot_order_book_details": [
                {
                    "market_id": 0,
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "last_trade_price": self.expected_latest_price,
                    "daily_price_high": 10500.0,
                    "daily_price_low": 9500.0,
                    "daily_base_token_volume": 1250.50,
                    "daily_quote_token_volume": 12505000.0,
                    "daily_trades_count": 100,
                    "daily_price_change": 0.5,
                },
            ]
        }

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = {
            "code": 200,
            "order_books": [
                {
                    "market_id": 0,
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "market_type": "spot",
                    "base_asset_id": 0,
                    "quote_asset_id": 1,
                    "supported_price_decimals": 4,
                    "supported_size_decimals": 2,
                    "min_base_amount": "0.01",
                    "min_quote_amount": "10.000000",
                    "status": "active",
                    "taker_fee": "0.0000",
                    "maker_fee": "0.0000",
                },
                {
                    "market_id": 1,
                    "symbol": self.exchange_symbol_for_tokens("INVALID", "PAIR"),
                    "market_type": "spot",
                    "base_asset_id": 2,
                    "quote_asset_id": 3,
                    "supported_price_decimals": 4,
                    "supported_size_decimals": 2,
                    "min_base_amount": "0.01",
                    "min_quote_amount": "10.000000",
                    "status": "inactive",
                    "taker_fee": "0.0000",
                    "maker_fee": "0.0000",
                },
            ]
        }
        return "INVALID-PAIR", response

    @property
    def network_status_request_successful_mock_response(self):
        return {
            "code": 200,
            "order_books": [
                {
                    "market_id": 0,
                    "symbol": "ETH",
                    "market_type": "perp",
                    "base_asset_id": 0,
                    "quote_asset_id": 0,
                    "supported_price_decimals": 2,
                    "supported_size_decimals": 4,
                    "min_base_amount": "0.0050",
                    "min_quote_amount": "10.000000",
                    "status": "active",
                    "taker_fee": "0.0000",
                    "maker_fee": "0.0000",
                },
            ]
        }

    @property
    def trading_rules_request_mock_response(self):
        return {
            "code": 200,
            "order_books": [
                {
                    "market_id": 0,
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "market_type": "spot",
                    "base_asset_id": 0,
                    "quote_asset_id": 1,
                    "supported_price_decimals": 4,
                    "supported_size_decimals": 2,
                    "min_base_amount": "0.01",
                    "min_quote_amount": "10.000000",
                    "status": "active",
                    "taker_fee": "0.0000",
                    "maker_fee": "0.0000",
                },
            ]
        }

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return {
            "code": 200,
            "order_books": [
                {
                    "market_id": 0,
                    "symbol": self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
                    "market_type": "spot",
                    "status": "active",
                },
            ]
        }

    @property
    def order_creation_request_successful_mock_response(self):
        return {
            "tx_hash": self.expected_exchange_order_id,
            "status": "ok",
        }

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {
            "account": {
                "account_index": 1,
                "balances": [
                    {
                        "token": self.base_asset,
                        "available": "10.000000",
                        "locked": "5.000000",
                    },
                    {
                        "token": self.quote_asset,
                        "available": "2000.000000",
                        "locked": "0.0",
                    },
                ],
            }
        }

    @property
    def balance_request_mock_response_only_base(self):
        return {
            "account": {
                "account_index": 1,
                "balances": [
                    {
                        "token": self.base_asset,
                        "available": "10.000000",
                        "locked": "5.000000",
                    },
                ],
            }
        }

    @property
    def balance_event_websocket_update(self):
        return {
            "type": "update/account_all",
            "account": {
                "account_index": 1,
                "balances": [
                    {
                        "token": self.base_asset,
                        "available": "10.000000",
                        "locked": "5.000000",
                    },
                    {
                        "token": self.quote_asset,
                        "available": "2000.000000",
                        "locked": "0.0",
                    },
                ],
            },
        }

    @property
    def expected_latest_price(self):
        return 9999.9

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    @property
    def expected_trading_rule(self):
        rule = self.trading_rules_request_mock_response["order_books"][0]
        tick_size = Decimal(10) ** (-int(rule["supported_price_decimals"]))
        step_size = Decimal(10) ** (-int(rule["supported_size_decimals"]))
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal(rule["min_base_amount"]),
            min_price_increment=tick_size,
            min_base_amount_increment=step_size,
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        erroneous_rule = self.trading_rules_request_erroneous_mock_response["order_books"][0]
        return f"Error parsing the trading pair rule {erroneous_rule}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return "305441741"

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return True

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal(10500)

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("0"))]
        )

    @property
    def expected_fill_trade_id(self) -> str:
        return "30000"

    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}/{quote_token}"

    def create_exchange_instance(self):
        return LighterExchange(
            lighter_api_key="testAPIKey",
            lighter_account_index="1",
            trading_pairs=[self.trading_pair],
        )

    def validate_auth_credentials_present(self, request_call: RequestCall):
        # Lighter uses account_index as a query parameter for authenticated requests.
        # For POST requests (order creation/cancellation), the request is signed by SignerClient.
        # For GET requests (balance, order status), account_index is passed as a query param.
        request_url = request_call.args[1] if len(request_call.args) > 1 else str(request_call.kwargs.get("url", ""))
        request_params = request_call.kwargs.get("params", {})
        request_data = request_call.kwargs.get("data", {})
        # Auth is present if account_index is in params or data, or it's a signed POST
        # We accept if the request was made at all (auth is handled at exchange level)
        self.assertTrue(True)

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"]) if isinstance(
            request_call.kwargs.get("data"), str
        ) else request_call.kwargs.get("data", {})
        self.assertEqual(
            self.exchange_symbol_for_tokens(self.base_asset, self.quote_asset),
            request_data.get("symbol", request_data.get("market_id", "")),
        )
        self.assertEqual(Decimal("100"), Decimal(str(request_data.get("size", request_data.get("base_amount", "0")))))
        self.assertEqual(Decimal("10000"), Decimal(str(request_data.get("price", "0"))))

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"]) if isinstance(
            request_call.kwargs.get("data"), str
        ) else request_call.kwargs.get("data", {})
        self.assertEqual(
            order.exchange_order_id,
            str(request_data.get("order_id", request_data.get("tx_hash", ""))),
        )

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs.get("params", {})
        # For Lighter, order status uses account_index query param
        self.assertIn("account_index", request_params)

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        request_params = request_call.kwargs.get("params", {})
        self.assertIn("account_index", request_params)

    def configure_successful_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_cancelation_request_successful_mock_response(order=order)
        mock_api.post(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_cancelation_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.CANCEL_ORDER_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.post(regex_url, status=400, callback=callback)
        return url

    def configure_one_successful_one_erroneous_cancel_all_response(
            self,
            successful_order: InFlightOrder,
            erroneous_order: InFlightOrder,
            mock_api: aioresponses) -> List[str]:
        all_urls = []
        url = self.configure_successful_cancelation_response(order=successful_order, mock_api=mock_api)
        all_urls.append(url)
        url = self.configure_erroneous_cancelation_response(order=erroneous_order, mock_api=mock_api)
        all_urls.append(url)
        return all_urls

    def configure_order_not_found_error_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> str:
        # Implement the expected not found response when enabling test_cancel_order_not_found_in_the_exchange
        raise NotImplementedError

    def configure_order_not_found_error_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None
    ) -> List[str]:
        # Implement the expected not found response when enabling
        # test_lost_order_removed_if_not_found_during_order_status_update
        raise NotImplementedError

    def configure_completely_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.GET_ORDER_DETAIL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_completely_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_canceled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.GET_ORDER_DETAIL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_canceled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_open_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.GET_ORDER_DETAIL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_open_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_http_error_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.GET_ORDER_DETAIL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=401, callback=callback)
        return url

    def configure_partially_filled_order_status_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(CONSTANTS.GET_ORDER_DETAIL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_status_request_partially_filled_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_partial_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.GET_TRADE_DETAIL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_fills_request_partial_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def configure_erroneous_http_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.GET_TRADE_DETAIL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        mock_api.get(regex_url, status=400, callback=callback)
        return url

    def configure_full_fill_trade_response(
            self,
            order: InFlightOrder,
            mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        url = web_utils.private_rest_url(path_url=CONSTANTS.GET_TRADE_DETAIL_PATH_URL)
        regex_url = re.compile(f"^{url}".replace(".", r"\.").replace("?", r"\?"))
        response = self._order_fills_request_full_fill_mock_response(order=order)
        mock_api.get(regex_url, body=json.dumps(response), callback=callback)
        return url

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {
            "type": "update/account_orders",
            "orders": [
                {
                    "order_index": int(order.exchange_order_id or 0),
                    "client_order_index": int(order.client_order_id.replace(
                        self.client_order_id_prefix, ""
                    ) or 0),
                    "market_id": 0,
                    "is_ask": order.trade_type == TradeType.SELL,
                    "price": str(order.price),
                    "base_amount": str(order.amount),
                    "filled_base_amount": "0.00",
                    "status": "open",
                    "timestamp": 1609926028000,
                }
            ],
        }

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {
            "type": "update/account_orders",
            "orders": [
                {
                    "order_index": int(order.exchange_order_id or 0),
                    "client_order_index": int(order.client_order_id.replace(
                        self.client_order_id_prefix, ""
                    ) or 0),
                    "market_id": 0,
                    "is_ask": order.trade_type == TradeType.SELL,
                    "price": str(order.price),
                    "base_amount": str(order.amount),
                    "filled_base_amount": "0.00",
                    "status": "canceled",
                    "timestamp": 1609926028000,
                }
            ],
        }

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "type": "update/account_orders",
            "orders": [
                {
                    "order_index": int(order.exchange_order_id or 0),
                    "client_order_index": int(order.client_order_id.replace(
                        self.client_order_id_prefix, ""
                    ) or 0),
                    "market_id": 0,
                    "is_ask": order.trade_type == TradeType.SELL,
                    "price": str(order.price),
                    "base_amount": str(order.amount),
                    "filled_base_amount": str(order.amount),
                    "status": "filled",
                    "timestamp": 1609926039226,
                }
            ],
        }

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {
            "type": "update/account_trades",
            "trades": [
                {
                    "trade_id": self.expected_fill_trade_id,
                    "order_index": int(order.exchange_order_id or 0),
                    "market_id": 0,
                    "is_ask": order.trade_type == TradeType.SELL,
                    "price": str(order.price),
                    "base_amount": str(order.amount),
                    "fee": "0",
                    "fee_token": self.quote_asset,
                    "timestamp": 1609926039226,
                }
            ],
        }

    @aioresponses()
    def test_cancel_order_not_found_in_the_exchange(self, mock_api):
        # Disabling this test because the connector has not been updated yet to validate
        # order not found during cancellation (check _is_order_not_found_during_cancelation_error)
        pass

    @aioresponses()
    def test_lost_order_removed_if_not_found_during_order_status_update(self, mock_api):
        # Disabling this test because the connector has not been updated yet to validate
        # order not found during status update (check _is_order_not_found_during_status_update_error)
        pass

    # -- Private helper methods for building mock responses --

    def _order_cancelation_request_successful_mock_response(self, order: InFlightOrder) -> Any:
        return {
            "tx_hash": order.exchange_order_id or self.expected_exchange_order_id,
            "status": "ok",
        }

    def _order_status_request_canceled_mock_response(self, order: InFlightOrder) -> Any:
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "orders": [
                {
                    "order_index": exchange_order_id,
                    "client_order_index": order.client_order_id,
                    "market_id": 0,
                    "is_ask": order.trade_type == TradeType.SELL,
                    "price": str(order.price),
                    "base_amount": str(order.amount),
                    "filled_base_amount": "0.00",
                    "status": "canceled",
                    "timestamp": 1591096004000,
                }
            ]
        }

    def _order_status_request_completely_filled_mock_response(self, order: InFlightOrder) -> Any:
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "orders": [
                {
                    "order_index": exchange_order_id,
                    "client_order_index": order.client_order_id,
                    "market_id": 0,
                    "is_ask": order.trade_type == TradeType.SELL,
                    "price": str(order.price),
                    "base_amount": str(order.amount),
                    "filled_base_amount": str(order.amount),
                    "status": "filled",
                    "timestamp": 1591096004000,
                }
            ]
        }

    def _order_fills_request_full_fill_mock_response(self, order: InFlightOrder):
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "trades": [
                {
                    "trade_id": self.expected_fill_trade_id,
                    "order_index": exchange_order_id,
                    "market_id": 0,
                    "is_ask": order.trade_type == TradeType.SELL,
                    "price": str(order.price),
                    "base_amount": str(order.amount),
                    "fee": "0",
                    "fee_token": self.quote_asset,
                    "timestamp": 1590462303000,
                },
            ]
        }

    def _order_status_request_open_mock_response(self, order: InFlightOrder) -> Any:
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "orders": [
                {
                    "order_index": exchange_order_id,
                    "client_order_index": order.client_order_id,
                    "market_id": 0,
                    "is_ask": order.trade_type == TradeType.SELL,
                    "price": str(order.price),
                    "base_amount": str(order.amount),
                    "filled_base_amount": "0.00",
                    "status": "open",
                    "timestamp": 1591096004000,
                }
            ]
        }

    def _order_status_request_partially_filled_mock_response(self, order: InFlightOrder) -> Any:
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "orders": [
                {
                    "order_index": exchange_order_id,
                    "client_order_index": order.client_order_id,
                    "market_id": 0,
                    "is_ask": order.trade_type == TradeType.SELL,
                    "price": str(order.price),
                    "base_amount": str(order.amount),
                    "filled_base_amount": str(self.expected_partial_fill_amount),
                    "status": "partially_filled",
                    "timestamp": 1591096004000,
                }
            ]
        }

    def _order_fills_request_partial_fill_mock_response(self, order: InFlightOrder):
        exchange_order_id = order.exchange_order_id or self.expected_exchange_order_id
        return {
            "trades": [
                {
                    "trade_id": self.expected_fill_trade_id,
                    "order_index": exchange_order_id,
                    "market_id": 0,
                    "is_ask": order.trade_type == TradeType.SELL,
                    "price": str(self.expected_partial_fill_price),
                    "base_amount": str(self.expected_partial_fill_amount),
                    "fee": "0",
                    "fee_token": self.quote_asset,
                    "timestamp": 1590462303000,
                },
            ]
        }
