import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.bithumb import bithumb_constants as CONSTANTS, bithumb_utils, bithumb_web_utils as web_utils
from hummingbot.connector.exchange.bithumb.bithumb_api_order_book_data_source import BithumbAPIOrderBookDataSource
from hummingbot.connector.exchange.bithumb.bithumb_api_user_stream_data_source import BithumbAPIUserStreamDataSource
from hummingbot.connector.exchange.bithumb.bithumb_auth import BithumbAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class BithumbExchange(ExchangePyBase):
    API_CALL_TIMEOUT = 10.0
    POLL_INTERVAL = 1.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    UPDATE_TRADE_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(
        self,
        bithumb_access_key: str,
        bithumb_secret_key: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
    ):
        self._access_key = bithumb_access_key
        self._secret_key = bithumb_secret_key
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []

        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self.real_time_balance_update = False

    @property
    def authenticator(self):
        return BithumbAuth(access_key=self._access_key, secret_key=self._secret_key)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return CONSTANTS.DEFAULT_DOMAIN

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.GET_TRADING_RULES_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.GET_TRADING_RULES_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.CHECK_NETWORK_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        error_text = str(status_update_exception).lower()
        return "not found" in error_text

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        error_text = str(cancelation_exception).lower()
        return "not found" in error_text

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BithumbAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BithumbAPIUserStreamDataSource()

    async def _make_trading_rules_request(self) -> Any:
        return await self._api_get(
            path_url=self.trading_rules_request_path,
            params={"isDetails": "true"},
        )

    async def _make_trading_pairs_request(self) -> Any:
        return await self._api_get(
            path_url=self.trading_pairs_request_path,
            params={"isDetails": "true"},
        )

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> AddedToCostTradeFee:
        is_maker = is_maker or (order_type is OrderType.LIMIT)
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        side = "bid" if trade_type == TradeType.BUY else "ask"

        if order_type == OrderType.LIMIT:
            order_data: Dict[str, Any] = {
                "market": symbol,
                "side": side,
                "ord_type": "limit",
                "volume": f"{amount:f}",
                "price": f"{price:f}",
            }
        else:
            if side == "bid":
                order_data = {
                    "market": symbol,
                    "side": side,
                    "ord_type": "price",
                    "price": f"{price:f}",
                }
            else:
                order_data = {
                    "market": symbol,
                    "side": side,
                    "ord_type": "market",
                    "volume": f"{amount:f}",
                }

        order_result = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
            data=order_data,
            is_auth_required=True,
        )

        exchange_order_id = str(order_result.get("uuid", ""))
        if exchange_order_id == "":
            raise ValueError(f"Failed to place Bithumb order: {order_result}")

        return exchange_order_id, self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        cancel_result = await self._api_delete(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            params={"uuid": tracked_order.exchange_order_id},
            is_auth_required=True,
        )
        return str(cancel_result.get("uuid", "")) != ""

    async def _format_trading_rules(self, exchange_info_dict: Any) -> List[TradingRule]:
        result = []
        exchange_info = exchange_info_dict if isinstance(exchange_info_dict, list) else exchange_info_dict.get("markets", [])

        for market in exchange_info:
            if not bithumb_utils.is_exchange_information_valid(market):
                continue
            try:
                base, quote = bithumb_utils.split_market_symbol(market["market"])
                trading_pair = combine_to_hb_trading_pair(base=base, quote=quote)
                result.append(
                    TradingRule(
                        trading_pair=trading_pair,
                        min_order_size=Decimal("0.00000001"),
                        min_price_increment=Decimal("0.00000001"),
                        min_base_amount_increment=Decimal("0.00000001"),
                    )
                )
            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {market}. Skipping.")

        return result

    async def _update_trading_fees(self):
        pass

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        balances = await self._api_get(
            path_url=CONSTANTS.GET_ACCOUNT_SUMMARY_PATH_URL,
            is_auth_required=True,
        )

        for balance in balances:
            asset_name = balance["currency"]
            available = Decimal(str(balance.get("balance", "0")))
            locked = Decimal(str(balance.get("locked", "0")))
            self._account_available_balances[asset_name] = available
            self._account_balances[asset_name] = available + locked
            remote_asset_names.add(asset_name)

        for asset_name in local_asset_names.difference(remote_asset_names):
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            order_data = await self._api_get(
                path_url=CONSTANTS.GET_ORDER_DETAIL_PATH_URL,
                params={"uuid": order.exchange_order_id},
                is_auth_required=True,
            )

            for trade in order_data.get("trades", []):
                fill_price = Decimal(str(trade.get("price", order_data.get("price", "0"))))
                fill_base_amount = Decimal(str(trade.get("volume", "0")))
                fill_quote_amount = Decimal(str(trade.get("funds", fill_price * fill_base_amount)))
                fee_amount = Decimal(str(trade.get("fee", "0")))
                fee_token = order.quote_asset

                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=fee_token,
                    flat_fees=[TokenAmount(amount=fee_amount, token=fee_token)],
                )

                trade_updates.append(
                    TradeUpdate(
                        trade_id=str(trade.get("uuid", trade.get("created_at", ""))),
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        trading_pair=order.trading_pair,
                        fee=fee,
                        fill_base_amount=fill_base_amount,
                        fill_quote_amount=fill_quote_amount,
                        fill_price=fill_price,
                        fill_timestamp=self.current_timestamp,
                    )
                )

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        order_data = await self._api_get(
            path_url=CONSTANTS.GET_ORDER_DETAIL_PATH_URL,
            params={"uuid": tracked_order.exchange_order_id},
            is_auth_required=True,
        )

        state = str(order_data.get("state", "wait"))
        new_state = CONSTANTS.ORDER_STATE.get(state, OrderState.OPEN)

        if state in {"wait", "watch"}:
            executed = Decimal(str(order_data.get("executed_volume", "0")))
            total = Decimal(str(order_data.get("volume", tracked_order.amount)))
            if executed > Decimal("0") and executed < total:
                new_state = OrderState.PARTIALLY_FILLED

        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order_data.get("uuid", tracked_order.exchange_order_id)),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=new_state,
        )

    async def _user_stream_event_listener(self):
        await asyncio.Event().wait()

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        mapping = bidict()
        data = exchange_info if isinstance(exchange_info, list) else exchange_info.get("markets", [])

        for market in data:
            if not bithumb_utils.is_exchange_information_valid(market):
                continue
            exchange_symbol = market.get("market")
            if exchange_symbol is None:
                continue
            try:
                base, quote = bithumb_utils.split_market_symbol(exchange_symbol)
                mapping[exchange_symbol] = combine_to_hb_trading_pair(base=base, quote=quote)
            except Exception:
                continue

        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        response = await self._api_get(
            path_url=CONSTANTS.GET_LAST_TRADING_PRICES_PATH_URL,
            params={"markets": symbol},
        )

        if isinstance(response, list) and len(response) > 0:
            return float(response[0].get("trade_price", 0.0))

        return 0.0
