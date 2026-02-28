import asyncio
import json
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.lighter import (
    lighter_constants as CONSTANTS,
    lighter_utils,
    lighter_web_utils as web_utils,
)
from hummingbot.connector.exchange.lighter.lighter_api_order_book_data_source import LighterAPIOrderBookDataSource
from hummingbot.connector.exchange.lighter.lighter_api_user_stream_data_source import LighterAPIUserStreamDataSource
from hummingbot.connector.exchange.lighter.lighter_auth import LighterAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class LighterExchange(ExchangePyBase):
    """
    LighterExchange connects with the Lighter DEX and provides order book pricing, user account tracking,
    and trading functionality.
    """
    API_CALL_TIMEOUT = 10.0
    POLL_INTERVAL = 1.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(
        self,
        lighter_api_key: str,
        lighter_account_index: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
    ):
        self._api_key = lighter_api_key
        self._account_index = int(lighter_account_index)
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs

        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self.real_time_balance_update = False

    @property
    def authenticator(self):
        return LighterAuth(
            api_key=self._api_key,
            account_index=self._account_index,
        )

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
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        # Lighter is a DEX and does not require time synchronization
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return LighterAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return LighterAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
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
        is_maker = order_type is OrderType.LIMIT_MAKER
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

        api_params = {
            "symbol": symbol,
            "side": "sell" if trade_type == TradeType.SELL else "buy",
            "type": order_type.name.lower(),
            "size": f"{amount:f}",
            "base_amount": f"{amount:f}",
            "price": f"{price:f}",
            "client_order_id": order_id,
        }

        order_result = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True,
        )

        exchange_order_id = str(order_result["tx_hash"])
        return exchange_order_id, self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        api_params = {
            "order_id": tracked_order.exchange_order_id,
            "tx_hash": tracked_order.exchange_order_id,
        }

        await self._api_post(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True,
        )
        return True

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        result = []
        for rule in exchange_info_dict.get("orderBooks", []):
            if lighter_utils.is_exchange_information_valid(rule):
                try:
                    trading_pair = await self.trading_pair_associated_to_exchange_symbol(rule["symbol"])
                except KeyError:
                    # Symbol not tracked by connector
                    continue
                try:
                    result.append(
                        TradingRule(
                            trading_pair=trading_pair,
                            min_order_size=Decimal(str(rule["min_base_amount"])),
                            min_price_increment=Decimal(str(rule["tick_size"])),
                            min_base_amount_increment=Decimal(str(rule["step_size"])),
                        )
                    )
                except Exception:
                    self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return result

    async def _update_trading_fees(self):
        pass

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_get(
            path_url=CONSTANTS.GET_ACCOUNT_SUMMARY_PATH_URL,
            params={"account_index": self._account_index},
            is_auth_required=True,
        )

        account_data = account_info.get("account", {})
        for balance in account_data.get("balances", []):
            asset_name = balance["token"]
            available = Decimal(str(balance["available"]))
            locked = Decimal(str(balance["locked"]))
            self._account_available_balances[asset_name] = available
            self._account_balances[asset_name] = available + locked
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            try:
                all_fills_response = await self._api_get(
                    path_url=CONSTANTS.GET_TRADE_DETAIL_PATH_URL,
                    params={"account_index": self._account_index},
                    is_auth_required=True,
                )

                for trade in all_fills_response.get("trades", []):
                    exchange_order_id = str(trade["order_index"])
                    if exchange_order_id != order.exchange_order_id:
                        continue

                    fee_token = trade.get("fee_token", order.quote_asset)
                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        percent_token=fee_token,
                        flat_fees=[TokenAmount(amount=Decimal(str(trade["fee"])), token=fee_token)],
                    )

                    fill_price = Decimal(str(trade["price"]))
                    fill_amount = Decimal(str(trade["base_amount"]))

                    trade_update = TradeUpdate(
                        trade_id=str(trade["trade_id"]),
                        client_order_id=order.client_order_id,
                        exchange_order_id=exchange_order_id,
                        trading_pair=order.trading_pair,
                        fee=fee,
                        fill_base_amount=fill_amount,
                        fill_quote_amount=fill_price * fill_amount,
                        fill_price=fill_price,
                        fill_timestamp=int(trade["timestamp"]) * 1e-3,
                    )
                    trade_updates.append(trade_update)

            except asyncio.CancelledError:
                raise
            except Exception as ex:
                raise

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        updated_order_data = await self._api_get(
            path_url=CONSTANTS.GET_ORDER_DETAIL_PATH_URL,
            params={"account_index": self._account_index},
            is_auth_required=True,
        )

        orders_data = updated_order_data.get("orders", [])

        # Find our order in the response
        order_data = None
        for o in orders_data:
            if str(o["order_index"]) == str(tracked_order.exchange_order_id):
                order_data = o
                break

        if order_data is None:
            # Check inactive orders
            try:
                inactive_data = await self._api_get(
                    path_url=CONSTANTS.GET_INACTIVE_ORDERS_PATH_URL,
                    params={"account_index": self._account_index},
                    is_auth_required=True,
                )
                for o in inactive_data.get("orders", []):
                    if str(o["order_index"]) == str(tracked_order.exchange_order_id):
                        order_data = o
                        break
            except Exception:
                pass

        if order_data is not None:
            new_state = CONSTANTS.ORDER_STATE.get(order_data["status"], OrderState.OPEN)
            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=str(order_data["order_index"]),
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=new_state,
            )
        else:
            # Order not found; return current state
            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=tracked_order.current_state,
            )

        return order_update

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type", "")

                if "account_orders" in event_type:
                    for order_data in event_message.get("orders", []):
                        await self._process_order_event(order_data)

                elif "account_trades" in event_type:
                    for trade_data in event_message.get("trades", []):
                        await self._process_trade_event(trade_data)

                elif "account_all" in event_type:
                    # Balance update
                    account_data = event_message.get("account", {})
                    for balance in account_data.get("balances", []):
                        asset_name = balance["token"]
                        available = Decimal(str(balance["available"]))
                        locked = Decimal(str(balance["locked"]))
                        self._account_available_balances[asset_name] = available
                        self._account_balances[asset_name] = available + locked

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")
                await self._sleep(5.0)

    async def _process_trade_event(self, trade_data: Dict[str, Any]):
        exchange_order_id = str(trade_data["order_index"])

        fillable_order = None
        for order in self._order_tracker.all_fillable_orders.values():
            if order.exchange_order_id == exchange_order_id:
                fillable_order = order
                break

        if fillable_order is not None:
            fee_token = trade_data.get("fee_token", fillable_order.quote_asset)
            fee = TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=fillable_order.trade_type,
                percent_token=fee_token,
                flat_fees=[TokenAmount(amount=Decimal(str(trade_data["fee"])), token=fee_token)],
            )

            trade_update = TradeUpdate(
                trade_id=str(trade_data["trade_id"]),
                client_order_id=fillable_order.client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=fillable_order.trading_pair,
                fee=fee,
                fill_base_amount=Decimal(str(trade_data["base_amount"])),
                fill_quote_amount=Decimal(str(trade_data["price"])) * Decimal(str(trade_data["base_amount"])),
                fill_price=Decimal(str(trade_data["price"])),
                fill_timestamp=int(trade_data["timestamp"]) * 1e-3,
            )
            self._order_tracker.process_trade_update(trade_update)

    async def _process_order_event(self, order_data: Dict[str, Any]):
        exchange_order_id = str(order_data["order_index"])
        client_order_id = str(order_data.get("client_order_index", ""))

        # Try to find the tracked order by exchange_order_id
        tracked_order = None
        for order in list(self._order_tracker.all_updatable_orders.values()):
            if order.exchange_order_id == exchange_order_id:
                tracked_order = order
                break

        if tracked_order is None:
            return

        new_state = CONSTANTS.ORDER_STATE.get(order_data["status"], OrderState.OPEN)
        event_timestamp = int(order_data.get("timestamp", 0)) * 1e-3

        order_update = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=event_timestamp,
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
        )
        self._order_tracker.process_order_update(order_update=order_update)

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in filter(lighter_utils.is_exchange_information_valid, exchange_info.get("orderBooks", [])):
            exchange_symbol = symbol_data["symbol"]
            base = symbol_data["base_token"]
            quote = symbol_data["quote_token"]
            mapping[exchange_symbol] = combine_to_hb_trading_pair(base=base, quote=quote)
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        resp_json = await self._api_get(
            path_url=CONSTANTS.GET_LAST_TRADING_PRICES_PATH_URL,
        )

        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        for detail in resp_json.get("orderBookDetails", []):
            if detail.get("symbol") == symbol:
                return float(detail["last_trade_price"])

        return 0.0
