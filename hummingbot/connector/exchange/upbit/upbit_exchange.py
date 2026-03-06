import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.upbit import upbit_constants as CONSTANTS, upbit_utils, upbit_web_utils as web_utils
from hummingbot.connector.exchange.upbit.upbit_api_order_book_data_source import UpbitAPIOrderBookDataSource
from hummingbot.connector.exchange.upbit.upbit_api_user_stream_data_source import UpbitAPIUserStreamDataSource
from hummingbot.connector.exchange.upbit.upbit_auth import UpbitAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class UpbitExchange(ExchangePyBase):
    API_CALL_TIMEOUT = 10.0
    POLL_INTERVAL = 1.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    UPDATE_TRADE_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(
        self,
        upbit_access_key: str,
        upbit_secret_key: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
    ):
        self._access_key = upbit_access_key
        self._secret_key = upbit_secret_key
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._ws_trade_fallback_timestamps: Dict[str, float] = {}

        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self.real_time_balance_update = False

    @property
    def authenticator(self):
        return UpbitAuth(access_key=self._access_key, secret_key=self._secret_key)

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
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def get_order_price_quantum(self, trading_pair: str, price: Decimal) -> Decimal:
        _, quote = trading_pair.split("-")
        if quote == "KRW":
            inferred_quantum = self._infer_price_quantum_from_order_book(trading_pair)
            if inferred_quantum is not None:
                return inferred_quantum
            return self._krw_price_quantum_from_price(price)
        return super().get_order_price_quantum(trading_pair, price)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        error_text = str(status_update_exception).lower()
        return "not found" in error_text

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        error_text = str(cancelation_exception).lower()
        return "not found" in error_text

    def _infer_price_quantum_from_order_book(self, trading_pair: str) -> Optional[Decimal]:
        try:
            order_book = self.get_order_book(trading_pair)
            asks = list(order_book.ask_entries())[:20]
            bids = list(order_book.bid_entries())[:20]

            diffs: List[Decimal] = []
            for i in range(len(asks) - 1):
                diff = Decimal(str(asks[i + 1].price)) - Decimal(str(asks[i].price))
                if diff > Decimal("0"):
                    diffs.append(diff)
            for i in range(len(bids) - 1):
                diff = Decimal(str(bids[i].price)) - Decimal(str(bids[i + 1].price))
                if diff > Decimal("0"):
                    diffs.append(diff)

            if len(diffs) == 0:
                return None

            quantum = min(diffs)
            return quantum if quantum > Decimal("0") else None
        except Exception:
            return None

    @staticmethod
    def _krw_price_quantum_from_price(price: Decimal) -> Decimal:
        # Fallback table aligned with KRW market price units.
        if price >= Decimal("2000000"):
            return Decimal("1000")
        elif price >= Decimal("1000000"):
            return Decimal("500")
        elif price >= Decimal("500000"):
            return Decimal("100")
        elif price >= Decimal("100000"):
            return Decimal("50")
        elif price >= Decimal("10000"):
            return Decimal("10")
        elif price >= Decimal("1000"):
            return Decimal("1")
        elif price >= Decimal("100"):
            return Decimal("0.1")
        elif price >= Decimal("10"):
            return Decimal("0.01")
        elif price >= Decimal("1"):
            return Decimal("0.001")
        elif price >= Decimal("0.1"):
            return Decimal("0.0001")
        return Decimal("0.00001")

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return UpbitAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return UpbitAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

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

        if order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER):
            order_data: Dict[str, Any] = {
                "market": symbol,
                "side": side,
                "ord_type": "limit",
                "volume": f"{amount:f}",
                "price": f"{price:f}",
            }
            if order_type == OrderType.LIMIT_MAKER:
                order_data["time_in_force"] = "post_only"
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
            raise ValueError(f"Failed to place Upbit order: {order_result}")

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
            if not upbit_utils.is_exchange_information_valid(market):
                continue
            try:
                base, quote = upbit_utils.split_market_symbol(market["market"])
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
        executed = Decimal(str(order_data.get("executed_volume", "0")))
        if "remaining_volume" in order_data:
            remaining = Decimal(str(order_data.get("remaining_volume", "0")))
        else:
            total = Decimal(str(order_data.get("volume", tracked_order.amount)))
            remaining = total - executed
        new_state = self._resolve_order_state(state=state, executed_volume=executed, remaining_volume=remaining)
        exchange_order_id = str(order_data.get("uuid", tracked_order.exchange_order_id))

        if new_state in (OrderState.FILLED, OrderState.CANCELED):
            self._clear_ws_order_tracking(exchange_order_id)

        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=new_state,
        )

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type", "")
                if event_type == CONSTANTS.PRIVATE_ORDER_CHANNEL_NAME:
                    await self._process_order_event(event_message)
                elif event_type == CONSTANTS.PRIVATE_ASSET_CHANNEL_NAME:
                    self._process_balance_event(event_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")
                await self._sleep(5.0)

    async def _process_order_event(self, event_data: Dict[str, Any]):
        exchange_order_id = str(event_data.get("uuid", ""))
        if not exchange_order_id:
            return

        fillable_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)
        updatable_order = self._order_tracker.all_updatable_orders_by_exchange_order_id.get(exchange_order_id)

        if fillable_order is None and updatable_order is None:
            return

        state = str(event_data.get("order_state", "wait"))
        reference_order = updatable_order or fillable_order
        executed = Decimal(str(event_data.get("executed_volume", "0")))
        if "remaining_volume" in event_data:
            remaining = Decimal(str(event_data.get("remaining_volume", "0")))
        else:
            total = Decimal(str(event_data.get("volume", reference_order.amount)))
            remaining = total - executed

        if state == "trade" and fillable_order is not None:
            trade_uuid = event_data.get("trade_uuid")
            trade_update = self._parse_trade_from_ws_event(event_data, fillable_order)
            if trade_update is not None:
                self._order_tracker.process_trade_update(trade_update)
            elif trade_uuid is None or str(trade_uuid) not in fillable_order.order_fills:
                await self._reconcile_trades_for_order(
                    order=fillable_order,
                    exchange_order_id=exchange_order_id,
                    reason="invalid_ws_trade_event",
                )

        if state == "done" and fillable_order is not None and executed > Decimal("0"):
            if fillable_order.executed_amount_base < executed:
                await self._reconcile_trades_for_order(
                    order=fillable_order,
                    exchange_order_id=exchange_order_id,
                    reason="done_mismatch",
                    force=True,
                )

        if updatable_order is not None:
            new_state = self._resolve_order_state(state=state, executed_volume=executed, remaining_volume=remaining)
            order_update = OrderUpdate(
                client_order_id=updatable_order.client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=updatable_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=new_state,
            )
            self._order_tracker.process_order_update(order_update)

        if state in {"done", "cancel"}:
            self._clear_ws_order_tracking(exchange_order_id)

    def _parse_trade_from_ws_event(
        self, event_data: Dict[str, Any], order: InFlightOrder
    ) -> Optional[TradeUpdate]:
        trade_uuid = event_data.get("trade_uuid")
        if not trade_uuid:
            return None
        trade_id = str(trade_uuid)
        if trade_id in order.order_fills:
            return None

        fill_price = Decimal(str(event_data.get("price", "0")))
        fill_base_amount = Decimal(str(event_data.get("volume", "0")))
        if fill_price <= Decimal("0") or fill_base_amount <= Decimal("0"):
            return None

        fill_quote_amount = fill_price * fill_base_amount
        fee_amount = Decimal(str(event_data.get("trade_fee", "0")))
        fee_token = order.quote_asset

        fee = TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=order.trade_type,
            percent_token=fee_token,
            flat_fees=[TokenAmount(amount=fee_amount, token=fee_token)],
        )

        ts = event_data.get("trade_timestamp")
        fill_ts = float(ts) / 1000.0 if ts else self.current_timestamp

        return TradeUpdate(
            trade_id=trade_id,
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            trading_pair=order.trading_pair,
            fee=fee,
            fill_base_amount=fill_base_amount,
            fill_quote_amount=fill_quote_amount,
            fill_price=fill_price,
            fill_timestamp=fill_ts,
            is_taker=not bool(event_data.get("is_maker", False)),
        )

    def _resolve_order_state(
        self,
        state: str,
        executed_volume: Decimal,
        remaining_volume: Decimal,
    ) -> OrderState:
        if state == "done":
            return OrderState.FILLED
        elif state == "cancel":
            return OrderState.CANCELED
        elif state == "trade":
            return OrderState.FILLED if remaining_volume <= Decimal("0") else OrderState.PARTIALLY_FILLED
        else:
            new_state = CONSTANTS.ORDER_STATE.get(state, OrderState.OPEN)
            if executed_volume > Decimal("0") and remaining_volume > Decimal("0"):
                return OrderState.PARTIALLY_FILLED
            return new_state

    def _should_run_ws_trade_rest_fallback(self, exchange_order_id: str, force: bool = False) -> bool:
        if force:
            self._ws_trade_fallback_timestamps[exchange_order_id] = self.current_timestamp
            return True

        last_ts = self._ws_trade_fallback_timestamps.get(exchange_order_id)
        if last_ts is None or self.current_timestamp - last_ts >= 5.0:
            self._ws_trade_fallback_timestamps[exchange_order_id] = self.current_timestamp
            return True
        return False

    async def _reconcile_trades_for_order(
        self,
        order: InFlightOrder,
        exchange_order_id: str,
        reason: str,
        force: bool = False,
    ) -> None:
        if not self._should_run_ws_trade_rest_fallback(exchange_order_id=exchange_order_id, force=force):
            self.logger().debug(
                f"Skipping WS trade reconciliation for {exchange_order_id}: debounce active ({reason})"
            )
            return

        self.logger().warning(f"WS trade reconciliation triggered for {exchange_order_id}: {reason}")
        trade_updates = await self._all_trade_updates_for_order(order)
        for trade_update in trade_updates:
            self._order_tracker.process_trade_update(trade_update)

    def _clear_ws_order_tracking(self, exchange_order_id: str) -> None:
        self._ws_trade_fallback_timestamps.pop(exchange_order_id, None)

    def _process_balance_event(self, event_data: Dict[str, Any]):
        asset = event_data.get("currency", "")
        if not asset:
            return
        available = Decimal(str(event_data.get("balance", "0")))
        locked = Decimal(str(event_data.get("locked", "0")))
        self._account_available_balances[asset] = available
        self._account_balances[asset] = available + locked

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        mapping = bidict()
        data = exchange_info if isinstance(exchange_info, list) else exchange_info.get("markets", [])

        for market in data:
            if not upbit_utils.is_exchange_information_valid(market):
                continue
            exchange_symbol = market.get("market")
            if exchange_symbol is None:
                continue
            try:
                base, quote = upbit_utils.split_market_symbol(exchange_symbol)
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
