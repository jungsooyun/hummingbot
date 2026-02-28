import asyncio
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.kis import (
    kis_constants as CONSTANTS,
    kis_utils,
    kis_web_utils as web_utils,
)
from hummingbot.connector.exchange.kis.kis_auth import KisAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class _KisNoOpUserStreamDataSource(UserStreamTrackerDataSource):
    """No-op user stream data source for KIS (no WebSocket support)."""

    async def listen_for_user_stream(self, output: asyncio.Queue):
        # KIS has no WS user stream. Block forever to keep the tracker alive.
        await asyncio.Event().wait()


class _KisNoOpOrderBookDataSource(OrderBookTrackerDataSource):
    """Minimal order book data source placeholder for KIS."""

    def __init__(self, trading_pairs: List[str], connector: "KisExchange"):
        super().__init__(trading_pairs=trading_pairs)
        self._connector = connector

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        return {"bids": [], "asks": []}

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        return {"bids": [], "asks": []}

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        pass

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        pass

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        pass

    async def listen_for_subscriptions(self):
        await asyncio.Event().wait()

    async def _connected_websocket_assistant(self):
        raise NotImplementedError("KIS does not support WebSocket order book streams")

    async def _subscribe_channels(self, websocket_assistant):
        raise NotImplementedError("KIS does not support WebSocket order book streams")

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        # KIS has no WS order book streams; no-op
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        # KIS has no WS order book streams; no-op
        return True


class KisExchange(ExchangePyBase):
    """
    KisExchange connects with Korea Investment & Securities (KIS) exchange
    and provides order book pricing, user account tracking, and trading
    functionality for domestic (KOSPI/KOSDAQ) stocks.

    KIS-specific characteristics:
    - TR_ID header-based request routing (each API call needs a tr_id header)
    - OAuth2 Bearer token authentication
    - No WebSocket support (REST polling only)
    - Response format: {"rt_cd": "0", "msg1": "...", "output": {...}}
    - No symbols list API (trading pairs configured externally)
    """

    API_CALL_TIMEOUT = 10.0
    POLL_INTERVAL = 1.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    UPDATE_TRADE_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(
        self,
        kis_app_key: str,
        kis_app_secret: str,
        kis_account_number: str,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self._app_key = kis_app_key
        self._app_secret = kis_app_secret
        self._account_number = kis_account_number
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._domain = domain
        self._sandbox = domain == "sandbox"

        # Parse account parts: "12345678-01" -> cano="12345678", acnt_prdt_cd="01"
        parts = kis_account_number.split("-")
        self._cano = parts[0] if len(parts) > 1 else kis_account_number
        self._acnt_prdt_cd = parts[1] if len(parts) > 1 else "01"

        super().__init__()
        self.real_time_balance_update = False

    # ------------------------------------------------------------------
    # Abstract property implementations
    # ------------------------------------------------------------------

    @property
    def authenticator(self):
        return KisAuth(
            app_key=self._app_key,
            app_secret=self._app_secret,
            sandbox=self._sandbox,
            initial_token=f"placeholder_{self._app_key}",
        )

    @property
    def name(self) -> str:
        return "kis"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.DOMESTIC_STOCK_TICKER_PATH

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.DOMESTIC_STOCK_TICKER_PATH

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

    # ------------------------------------------------------------------
    # Abstract method stubs (time sync, order-not-found)
    # ------------------------------------------------------------------

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        # KIS does not require time synchronization
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return False

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return _KisNoOpOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return _KisNoOpUserStreamDataSource()

    # ------------------------------------------------------------------
    # Fee
    # ------------------------------------------------------------------

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
        is_maker = is_maker or (order_type is OrderType.LIMIT_MAKER)
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    # ------------------------------------------------------------------
    # Trading: place order / cancel
    # ------------------------------------------------------------------

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

        if trade_type == TradeType.BUY:
            tr_id = CONSTANTS.DOMESTIC_STOCK_ORDER_BUY_TR_ID
        else:
            tr_id = CONSTANTS.DOMESTIC_STOCK_ORDER_SELL_TR_ID

        # ORD_DVSN: "00" = limit, "01" = market
        ord_dvsn = "00" if order_type == OrderType.LIMIT else "01"

        body = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": symbol,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(int(amount)),
            "ORD_UNPR": str(int(price)),
        }

        if self._sandbox:
            tr_id = self._sandbox_tr_id(tr_id)

        result = await self._api_post(
            path_url=CONSTANTS.DOMESTIC_STOCK_ORDER_PATH,
            data=body,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )

        if result.get("rt_cd") != "0":
            raise ValueError(f"KIS order failed: {result.get('msg1', 'Unknown error')}")

        output = result["output"]
        exchange_order_id = str(output["ODNO"])
        return exchange_order_id, self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        tr_id = CONSTANTS.DOMESTIC_STOCK_CANCEL_TR_ID
        if self._sandbox:
            tr_id = self._sandbox_tr_id(tr_id)

        symbol = await self.exchange_symbol_associated_to_pair(tracked_order.trading_pair)

        body = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": symbol,
            "ORGN_ODNO": tracked_order.exchange_order_id,
            "ORD_DVSN": "00",
            "RVSE_CNCL_DVSN_CD": "02",  # 02 = cancel
            "ORD_QTY": str(int(tracked_order.amount)),
            "ORD_UNPR": "0",
            "QTY_ALL_ORD_YN": "Y",
        }

        result = await self._api_post(
            path_url=CONSTANTS.DOMESTIC_STOCK_ORDER_PATH,
            data=body,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )

        if result.get("rt_cd") != "0":
            raise ValueError(f"KIS cancel failed: {result.get('msg1', 'Unknown error')}")

        return True

    # ------------------------------------------------------------------
    # Trading rules
    # ------------------------------------------------------------------

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        result = []
        symbols = exchange_info_dict.get("output", {}).get("symbols", [])
        for symbol_data in symbols:
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol_data["symbol"])
                min_order_size = Decimal(str(symbol_data.get("min_order_size", "1")))
                price_unit = Decimal(str(symbol_data.get("price_unit", "1")))
                lot_size = Decimal(str(symbol_data.get("lot_size", "1")))
                result.append(TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_order_size,
                    min_price_increment=price_unit,
                    min_base_amount_increment=lot_size,
                ))
            except KeyError:
                continue
            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {symbol_data}. Skipping.")
        return result

    # ------------------------------------------------------------------
    # Update trading fees (no-op for KIS)
    # ------------------------------------------------------------------

    async def _update_trading_fees(self):
        pass

    # ------------------------------------------------------------------
    # Balances
    # ------------------------------------------------------------------

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        tr_id = CONSTANTS.DOMESTIC_STOCK_BALANCE_TR_ID
        if self._sandbox:
            tr_id = self._sandbox_tr_id(tr_id)

        params = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        result = await self._api_get(
            path_url=CONSTANTS.DOMESTIC_STOCK_BALANCE_PATH,
            params=params,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )

        # Parse stock holdings from output1
        for holding in result.get("output1", []):
            asset_name = holding["pdno"]
            total_qty = Decimal(str(holding["hldg_qty"]))
            available_qty = Decimal(str(holding["ord_psbl_qty"]))
            self._account_balances[asset_name] = total_qty
            self._account_available_balances[asset_name] = available_qty
            remote_asset_names.add(asset_name)

        # Parse KRW cash from output2
        for cash_info in result.get("output2", []):
            krw_total = Decimal(str(cash_info.get("dnca_tot_amt", "0")))
            krw_available = Decimal(str(cash_info.get("dnca_tot_amt", "0")))
            if krw_total > 0:
                # Use quote asset name from trading pairs
                for tp in self._trading_pairs:
                    quote = tp.split("-")[-1]
                    self._account_balances[quote] = krw_total
                    self._account_available_balances[quote] = krw_available
                    remote_asset_names.add(quote)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    # ------------------------------------------------------------------
    # Order status
    # ------------------------------------------------------------------

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        result = await self._request_kis_order_detail(tracked_order)

        # Process any fill data embedded in the response
        fill_updates = self._create_order_fill_updates(order=tracked_order, fill_data=result)
        for fill_update in fill_updates:
            self._order_tracker.process_trade_update(fill_update)

        output = result.get("output", {})
        # When output is a list (fill response format), extract status from first element
        if isinstance(output, list):
            output = output[0] if output else {}

        # Determine order state from response
        state_str = output.get("state")
        if state_str:
            new_state = CONSTANTS.ORDER_STATE.get(state_str, OrderState.OPEN)
        else:
            # Infer state from fill data when explicit state is absent
            new_state = self._infer_order_state(output, tracked_order)

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(output.get("ODNO", tracked_order.exchange_order_id or "")),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=new_state,
        )
        return order_update

    @staticmethod
    def _infer_order_state(output: Dict[str, Any], order: InFlightOrder) -> OrderState:
        """Infer order state from KIS fill data when explicit 'state' field is missing."""
        ccld_qty_str = output.get("CCLD_QTY", "0")
        ord_qty_str = output.get("ORD_QTY", "0")
        cncl_yn = output.get("CNCL_YN", "N")

        try:
            ccld_qty = int(ccld_qty_str)
            ord_qty = int(ord_qty_str)
        except (ValueError, TypeError):
            return OrderState.OPEN

        if cncl_yn == "Y":
            return OrderState.CANCELED
        if ord_qty > 0 and ccld_qty >= ord_qty:
            return OrderState.FILLED
        if ccld_qty > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    async def _request_kis_order_detail(self, order: InFlightOrder) -> Dict[str, Any]:
        """Shared method to query KIS order detail endpoint."""
        tr_id = CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_TR_ID
        if self._sandbox:
            tr_id = self._sandbox_tr_id(tr_id)

        params = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "INQR_STRT_DT": "",
            "INQR_END_DT": "",
            "SLL_BUY_DVSN_CD": "00",
            "INQR_DVSN": "00",
            "PDNO": "",
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": order.exchange_order_id or "",
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        return await self._api_get(
            path_url=CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH,
            params=params,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            result = await self._request_kis_order_detail(order)
            trade_updates = self._create_order_fill_updates(order=order, fill_data=result)

        return trade_updates

    def _create_order_fill_updates(self, order: InFlightOrder, fill_data: Dict[str, Any]) -> List[TradeUpdate]:
        updates = []
        output = fill_data.get("output", [])

        # output can be a list (fill responses) or a dict (order status responses)
        if isinstance(output, dict):
            # Single order status response, not a fills list
            return updates

        for fill in output:
            fee = TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=order.trade_type,
                percent_token=fill.get("feeCoinName", order.quote_asset),
                flat_fees=[TokenAmount(
                    amount=Decimal(str(fill.get("fee", "0"))),
                    token=fill.get("feeCoinName", order.quote_asset),
                )],
            )
            trade_update = TradeUpdate(
                trade_id=str(fill.get("tradeId", fill.get("ODNO", ""))),
                client_order_id=order.client_order_id,
                exchange_order_id=str(fill.get("ODNO", order.exchange_order_id or "")),
                trading_pair=order.trading_pair,
                fee=fee,
                fill_base_amount=Decimal(str(fill.get("CCLD_QTY", "0"))),
                fill_quote_amount=Decimal(str(fill.get("TOT_CCLD_AMT", "0"))),
                fill_price=Decimal(str(fill.get("AVG_PRVS", "0"))),
                fill_timestamp=self.current_timestamp,
            )
            updates.append(trade_update)

        return updates

    # ------------------------------------------------------------------
    # User stream (no-op for KIS)
    # ------------------------------------------------------------------

    async def _user_stream_event_listener(self):
        # KIS has no WebSocket user stream. This is a no-op.
        # Just wait forever to avoid the base class restarting the loop.
        await asyncio.Event().wait()

    # ------------------------------------------------------------------
    # Trading pair symbol map
    # ------------------------------------------------------------------

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        symbols = exchange_info.get("output", {}).get("symbols", [])
        for symbol_data in symbols:
            if not kis_utils.is_exchange_information_valid(symbol_data):
                continue
            # Skip suspended or delisted symbols
            status = symbol_data.get("status", "trading")
            if status != "trading":
                continue
            exchange_symbol = symbol_data["symbol"]
            base = symbol_data.get("base_currency", exchange_symbol)
            quote = symbol_data.get("quote_currency", "KRW")
            hb_trading_pair = combine_to_hb_trading_pair(base=base, quote=quote)
            mapping[exchange_symbol] = hb_trading_pair
        self._set_trading_pair_symbol_map(mapping)

    # ------------------------------------------------------------------
    # Last traded price
    # ------------------------------------------------------------------

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
        }

        result = await self._api_get(
            path_url=CONSTANTS.DOMESTIC_STOCK_TICKER_PATH,
            params=params,
        )

        output = result.get("output", {})
        price_str = output.get("stck_prpr", "0")
        return float(price_str)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sandbox_tr_id(tr_id: str) -> str:
        """Convert production TR_ID to sandbox TR_ID.

        KIS sandbox uses ``V`` prefix instead of ``T`` for trading TR_IDs.
        """
        if tr_id.startswith("T"):
            return "V" + tr_id[1:]
        return tr_id
