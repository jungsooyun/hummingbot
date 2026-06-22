import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.open_order_record import OpenOrderRecord
from hummingbot.connector.exchange.kis import (
    kis_constants as CONSTANTS,
    kis_utils,
    kis_web_utils as web_utils,
)
from hummingbot.connector.exchange.kis.kis_api_order_book_data_source import KisAPIOrderBookDataSource
from hummingbot.connector.exchange.kis.kis_api_user_stream_data_source import KisAPIUserStreamDataSource
from hummingbot.connector.exchange.kis.kis_auth import KisAuth
from hummingbot.connector.exchange.kis.kis_ws_hub import KisWsHub
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


class KisExchange(ExchangePyBase):
    """
    KisExchange connects with Korea Investment & Securities (KIS) exchange
    and provides order book pricing, user account tracking, and trading
    functionality for domestic (KOSPI/KOSDAQ) stocks.

    KIS-specific characteristics:
    - TR_ID header-based request routing (each API call needs a tr_id header)
    - OAuth2 Bearer token authentication (REST) + approval_key (WebSocket)
    - WebSocket for real-time orderbook (H0STASP0), trades (H0STCNT0),
      and execution notifications (H0STCNI0/H0STCNI9, AES-encrypted)
    - WS data format: pipe-delimited text with caret (^) field separators
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
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        kis_sandbox: str = "false",
        kis_market_routing: str = CONSTANTS.MARKET_ROUTING_SOR,
        kis_ws_enabled: str = "true",
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self._app_key = kis_app_key
        self._app_secret = kis_app_secret
        self._account_number = kis_account_number
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._domain = domain
        # Sandbox is driven by the kis_sandbox config field; domain == "sandbox"
        # is kept as an override for back-compat (tests / direct construction).
        # The factory's non_trading instantiation path forwards EVERY config-map
        # field (including kis_sandbox) as a kwarg, so the constructor MUST accept
        # it or trading-pair fetching raises TypeError.
        self._sandbox = str(kis_sandbox).strip().lower() == "true" or domain == "sandbox"
        # Realtime WS toggle: "false" -> data sources never connect to the WS edge
        # (REST-only market data + fills). Default enabled. The factory forwards
        # every config-map field as a kwarg, so the constructor MUST accept it.
        self._ws_enabled = str(kis_ws_enabled).strip().lower() != "false"
        self._ws_hub = None

        # Resolve order routing BEFORE super().__init__() — ExchangePyBase.__init__
        # constructs the order book data source, which needs self._market_routing.
        # Invalid live values fail closed (raise); only sandbox sor/nxt -> krx falls back.
        routing = (kis_market_routing or CONSTANTS.MARKET_ROUTING_SOR).lower()
        self._routing_warnings: List[str] = []
        if routing not in CONSTANTS.VALID_MARKET_ROUTINGS:
            raise ValueError(
                f"Invalid kis_market_routing '{kis_market_routing}'. "
                f"Expected one of {CONSTANTS.VALID_MARKET_ROUTINGS}."
            )
        if self._sandbox and routing != CONSTANTS.MARKET_ROUTING_KRX:
            self._routing_warnings.append(
                f"KIS sandbox does not support '{routing}' routing; using 'krx'."
            )
            routing = CONSTANTS.MARKET_ROUTING_KRX
        self._market_routing = routing

        # Parse account parts: "12345678-01" -> cano="12345678", acnt_prdt_cd="01"
        parts = kis_account_number.split("-")
        self._cano = parts[0] if len(parts) > 1 else kis_account_number
        self._acnt_prdt_cd = parts[1] if len(parts) > 1 else "01"

        super().__init__(balance_asset_limit, rate_limits_share_pct)
        for _warning in self._routing_warnings:
            self.logger().warning(_warning)
        # Balance updates are still REST-polled; order/fill events come via WS
        self.real_time_balance_update = False

    def _excg_for_routing(self) -> str:
        """Map the active routing mode to its EXCG_ID_DVSN_CD value."""
        return CONSTANTS.EXCG_BY_ROUTING[self._market_routing]

    # ------------------------------------------------------------------
    # Abstract property implementations
    # ------------------------------------------------------------------

    @property
    def authenticator(self):
        return KisAuth(
            app_key=self._app_key,
            app_secret=self._app_secret,
            sandbox=self._sandbox,
            # token_cache_path=None -> KisAuth auto-derives hummingbot's
            # persistent data dir so the daily token/approval key survive
            # process/container restarts (KIS issues 1 token/app_key/day).
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
        # NOT oauth2/tokenP: that endpoint is POST-only (token issuance) and does
        # not answer GET, so the base health probe hangs until timeout every cycle,
        # check_network swallows the error -> NOT_CONNECTED forever -> the connector
        # never becomes ready (silent, nothing in errors.log). Probe the balance
        # endpoint instead (authenticated GET, available 24/7, reuses cached token).
        # The actual request is issued by _make_network_check_request (auth + params).
        return CONSTANTS.DOMESTIC_STOCK_BALANCE_PATH

    async def _make_network_check_request(self):
        # Override the base (unauthenticated, param-less GET on the path above):
        # KIS requires auth + account params + a TR_ID header. A successful balance
        # inquiry proves network + auth + account are all healthy.
        tr_id, params = self._balance_request_args()
        result = await self._api_get(
            path_url=CONSTANTS.DOMESTIC_STOCK_BALANCE_PATH,
            params=params,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )
        # Fail closed: KIS returns HTTP 200 with rt_cd != "0" on logical auth/account errors.
        # The base check_network() treats a non-exception as CONNECTED, so an un-inspected
        # logical error would mark the connector healthy while _update_balances (fail-closed
        # since JEP-161) raises — a readiness/balance inconsistency that hides a broken account.
        # Raise so check_network() reports NOT_CONNECTED instead (JEP-182).
        if result.get("rt_cd") != "0":
            raise IOError(
                f"KIS network check failed: "
                f"rt_cd={result.get('rt_cd')} msg={result.get('msg1')}"
            )

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def _is_user_stream_initialized(self) -> bool:
        # KIS catches order fills via REST order-status polling
        # (ExchangePyBase._update_order_status). Its real-time exec-notification
        # WebSocket (ops.koreainvestment.com:21000) is environment-gated and
        # frequently unavailable (real-time WS cap / entitlement), so it must NOT
        # gate connector readiness -- otherwise the connector never becomes ready
        # and cannot trade despite working REST order/balance polling. The user
        # stream WS, when it connects, still delivers faster notifications; this
        # only decouples *readiness* from it.
        return True

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False):
        # KIS authenticates via OAuth Bearer token (no HMAC timestamp signing), so
        # it needs no server-time synchronization. The base implementation calls
        # web_utils.get_current_server_time(), which KIS does not provide; letting
        # it run raises AttributeError inside _status_polling_loop, which flaps the
        # connector to NOT_CONNECTED (account-update failures) and prevents stable
        # readiness. No-op by design.
        return

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

    @property
    def ws_hub(self) -> KisWsHub:
        """The single shared KIS WS transport hub."""
        if self._ws_hub is None:
            self._ws_hub = KisWsHub(
                auth=self._auth,
                domain=self._domain,
                ws_enabled=self._ws_enabled,
            )
        return self._ws_hub

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return KisAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            auth=self._auth,
            hub=self.ws_hub,
            domain=self._domain,
            market_routing=self._market_routing,
            ws_enabled=self._ws_enabled,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return KisAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            hub=self.ws_hub,
            domain=self._domain,
            ws_enabled=self._ws_enabled,
        )

    async def stop_network(self):
        await super().stop_network()
        if self._ws_hub is not None:
            await self._ws_hub.stop()

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
            "EXCG_ID_DVSN_CD": self._excg_for_routing(),
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
            "KRX_FWDG_ORD_ORGNO": "",  # KIS looks up the original forwarding org when blank
            "PDNO": symbol,
            "ORGN_ODNO": tracked_order.exchange_order_id,
            "ORD_DVSN": "00",
            "RVSE_CNCL_DVSN_CD": "02",  # 02 = cancel
            "ORD_QTY": str(int(tracked_order.amount)),
            "ORD_UNPR": "0",
            "QTY_ALL_ORD_YN": "Y",
            "EXCG_ID_DVSN_CD": self._excg_for_routing(),
        }

        result = await self._api_post(
            path_url=CONSTANTS.DOMESTIC_STOCK_CANCEL_PATH,
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

    async def _update_trading_rules(self):
        # KIS exposes no trading-rules API, and its ticker endpoint returns a
        # single quote rather than a symbol list, so a network fetch yields
        # nothing (and would fail auth — there is no symbols payload to parse).
        # Trading pairs are configured externally; synthesise one rule per pair.
        # KRX trades whole shares (size increment 1) at integer-KRW prices; the
        # price-band tick is applied per order by the strategy/config rather than
        # via a single static increment here.
        self._trading_rules.clear()
        for trading_pair in self._trading_pairs:
            self._trading_rules[trading_pair] = TradingRule(
                trading_pair=trading_pair,
                min_order_size=Decimal("1"),
                min_price_increment=Decimal("1"),
                min_base_amount_increment=Decimal("1"),
            )

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

    def _balance_request_args(self) -> Tuple[str, Dict[str, str]]:
        """TR_ID + query params for the domestic-stock balance inquiry.

        Shared by ``_update_balances`` and the ``_make_network_check_request``
        health probe so the two never drift."""
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
        return tr_id, params

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        tr_id, params = self._balance_request_args()

        result = await self._api_get(
            path_url=CONSTANTS.DOMESTIC_STOCK_BALANCE_PATH,
            params=params,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )

        # Fail closed: KIS returns HTTP 200 with rt_cd != "0" on logical errors (e.g.
        # EGW00304). Parsing the empty output1/output2 would silently zero out holdings +
        # cash — a money-path input to fair/hedge/readiness. Raise instead (JEP-161,
        # mirrors _get_last_traded_price).
        if result.get("rt_cd") != "0":
            raise IOError(
                f"KIS balance request failed: "
                f"rt_cd={result.get('rt_cd')} msg={result.get('msg1')}"
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
            krw_available = next(
                (Decimal(str(cash_info[k])) for k in ("ord_psbl_cash", "nrcvb_buy_amt", "dnca_tot_amt") if k in cash_info),
                Decimal("0"),
            )  # orderable cash precedence: ord_psbl_cash -> nrcvb_buy_amt -> dnca_tot_amt (first present wins). VERIFY exact key against live output2
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

        # Fail closed WITHOUT escalating: a logical fetch error (HTTP 200 + rt_cd != "0") carries
        # no order-state information. Do NOT infer OPEN from the empty output1 (the pre-JEP-182
        # bug — _match_ccld_row returns {} and _infer_order_state falls through to OPEN), and do
        # NOT raise: the base _handle_update_error_for_active_order routes ANY raised status
        # exception to process_order_not_found(), which marks a still-live order FAILED after
        # _lost_order_count_limit consecutive errors (e.g. a transient token/account rt_cd error
        # spanning a few polls). Return a no-op update (state unchanged) so the order keeps polling;
        # check_network independently reports NOT_CONNECTED on the same condition. Transport errors
        # from _api_get still propagate (they raise before this check), preserving the framework
        # "request fails -> not found" contract. (JEP-182; codex challenge 2026-06-19.)
        if result.get("rt_cd") != "0":
            self.logger().debug(
                f"KIS order-status fetch for {tracked_order.client_order_id} returned "
                f"rt_cd={result.get('rt_cd')} ({result.get('msg1')}); leaving state unchanged."
            )
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=tracked_order.current_state,
            )

        # State only. Fills are emitted by _all_trade_updates_for_order (the dedicated
        # trade-update path), so that a failed fill fetch never double-emits a fill the
        # status path would have surfaced. KIS inquire-daily-ccld has no explicit status
        # field; derive state from the matched row's cumulative tot_ccld_qty vs ord_qty + cncl_yn.
        row = self._match_ccld_row(result, tracked_order)
        new_state = self._infer_order_state(row, tracked_order)

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(row.get("odno", tracked_order.exchange_order_id or "")),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=new_state,
        )
        return order_update

    @staticmethod
    def _match_ccld_row(result: Dict[str, Any], order: InFlightOrder) -> Dict[str, Any]:
        """Return this order's inquire-daily-ccld output1 row (matched by odno), else the
        first row, else {} — KIS returns output1 (list) + output2 (summary), NOT 'output'."""
        rows = result.get("output1", [])
        if not isinstance(rows, list) or not rows:
            return {}
        eoid = str(order.exchange_order_id) if order.exchange_order_id else ""
        if eoid:
            for row in rows:
                if str(row.get("odno", "")) == eoid:
                    return row
        return rows[0]

    @staticmethod
    def _infer_order_state(row: Dict[str, Any], order: InFlightOrder) -> OrderState:
        """Infer state from a KIS inquire-daily-ccld output1 row (lowercase fields:
        tot_ccld_qty cumulative filled / ord_qty / cncl_yn cancel flag)."""
        try:
            ccld_qty = Decimal(str(row.get("tot_ccld_qty", "0") or "0"))
            ord_qty = Decimal(str(row.get("ord_qty", "0") or "0"))
        except (InvalidOperation, ValueError, TypeError):
            return OrderState.OPEN

        if str(row.get("cncl_yn", "N")).upper() == "Y":
            return OrderState.CANCELED
        if ord_qty > 0 and ccld_qty >= ord_qty:
            return OrderState.FILLED
        if ccld_qty > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    @staticmethod
    def _kst_today() -> str:
        """KIS inquire-daily-ccld requires a YYYYMMDD date range; same-day fills live under
        today's KST date. The engine host runs UTC, so derive KST (UTC+9) explicitly —
        an empty date range returns no rows, so the order would never reconcile (JEP-162)."""
        return datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")

    async def _request_kis_order_detail(self, order: InFlightOrder) -> Dict[str, Any]:
        """Shared method to query KIS order detail endpoint (inquire-daily-ccld)."""
        tr_id = CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_TR_ID
        if self._sandbox:
            tr_id = self._sandbox_tr_id(tr_id)

        today = self._kst_today()
        params = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "INQR_STRT_DT": today,
            "INQR_END_DT": today,
            "SLL_BUY_DVSN_CD": "00",
            "INQR_DVSN": "00",
            "PDNO": "",
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": order.exchange_order_id or "",
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "EXCG_ID_DVSN_CD": CONSTANTS.EXCG_ALL,  # reconcile KRX+NXT+SOR fills
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        # NOTE: rt_cd is intentionally NOT inspected here. The two consumers must treat a logical
        # error (HTTP 200 + rt_cd != "0") differently: _all_trade_updates_for_order RAISES (safe —
        # _update_orders_fills logs and retries it without touching order state), while
        # _request_order_status returns a NO-OP update — raising on the status path would let the
        # base _handle_update_error_for_active_order escalate to process_order_not_found() and
        # wrongly mark a live order FAILED after a few transient errors (JEP-182; codex challenge
        # 2026-06-19).
        return await self._api_get(
            path_url=CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH,
            params=params,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )

    async def get_open_orders(self, trading_pair: Optional[str] = None) -> List[OpenOrderRecord]:
        # HIGH-1: built on the EXISTING, verified inquire-daily-ccld endpoint, filtered to
        # NON-TERMINAL (resting) rows. (A dedicated inquire-psbl-rvsecncl endpoint may replace
        # this ONLY after its path+TR_ID+field contract is verified against KIS docs/live.)
        tr_id = CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_TR_ID
        if self._sandbox:
            tr_id = self._sandbox_tr_id(tr_id)
        today = self._kst_today()
        records: List[OpenOrderRecord] = []
        by_eoid = self._order_tracker.all_fillable_orders_by_exchange_order_id
        fk100, nk100 = "", ""
        max_pages = 50
        for _ in range(max_pages):
            params = {
                "CANO": self._cano,
                "ACNT_PRDT_CD": self._acnt_prdt_cd,
                "INQR_STRT_DT": today,
                "INQR_END_DT": today,
                "SLL_BUY_DVSN_CD": "00",
                "INQR_DVSN": "00",
                "PDNO": "",
                "CCLD_DVSN": "00",
                "ORD_GNO_BRNO": "",
                "ODNO": "",
                "INQR_DVSN_3": "00",
                "INQR_DVSN_1": "",
                "EXCG_ID_DVSN_CD": self._excg_for_routing(),
                "CTX_AREA_FK100": fk100,
                "CTX_AREA_NK100": nk100,
            }
            result = await self._api_get(
                path_url=CONSTANTS.DOMESTIC_STOCK_ORDER_DETAIL_PATH,
                params=params,
                is_auth_required=True,
                headers={"tr_id": tr_id},
            )
            # Fail closed: a logical error (rt_cd != "0") must not be read as "no open orders" —
            # that would mis-seed reconcile (JEP-170/171), which would then wrongly conclude the
            # account is flat and skip cancel-all, leaking resting orders. A genuine "no open orders"
            # response carries rt_cd == "0", so this never blocks the empty-but-valid case. Guard
            # per page so an error on any page (not only page 1) fails closed (JEP-182).
            if result.get("rt_cd") != "0":
                raise IOError(
                    f"KIS open-orders request failed: "
                    f"rt_cd={result.get('rt_cd')} msg={result.get('msg1')}"
                )
            for row in result.get("output1", []) or []:
                if str(row.get("cncl_yn", "N")).upper() == "Y":
                    continue
                ord_qty = Decimal(str(row.get("ord_qty", "0") or "0"))
                ccld_qty = Decimal(str(row.get("tot_ccld_qty", "0") or "0"))
                if ord_qty > 0 and ccld_qty >= ord_qty:
                    continue
                pdno = str(row.get("pdno", ""))
                try:
                    tp = await self.trading_pair_associated_to_exchange_symbol(pdno)
                except KeyError:
                    continue
                if trading_pair is not None and tp != trading_pair:
                    continue
                trade_type = TradeType.SELL if str(row.get("sll_buy_dvsn_cd", "")) == "01" else TradeType.BUY
                eoid = str(row.get("odno", ""))
                tracked = by_eoid.get(eoid)
                # VERIFY: inquire-daily-ccld remaining-qty field name (rmn_qty/ord_psbl_qty) + ord_unpr unit-price key against the connector's existing parser / KIS docs
                remaining = row.get("rmn_qty", row.get("ord_psbl_qty"))
                remaining_amount = Decimal(str(remaining)) if remaining is not None else (ord_qty - ccld_qty)
                records.append(OpenOrderRecord(
                    trading_pair=tp,
                    exchange_order_id=eoid,
                    client_order_id=tracked.client_order_id if tracked else None,
                    trade_type=trade_type,
                    price=Decimal(str(row.get("ord_unpr", "0"))),
                    amount=ord_qty,
                    remaining_amount=remaining_amount,
                ))
            # VERIFY: pagination (ctx_area_nk100/tr_cont) + after-15:30 resting-order visibility against KIS live response
            nk100 = str(result.get("ctx_area_nk100", "")).strip()
            fk100 = str(result.get("ctx_area_fk100", "")).strip()
            if not nk100 and not str(result.get("tr_cont", "")).strip():
                break
            if not nk100:
                break
        return records

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            result = await self._request_kis_order_detail(order)
            # Fail closed: rt_cd != "0" (HTTP 200 logical error) yields an empty output1; without
            # this guard _create_order_fill_updates would return [] and a real fill would silently
            # vanish. A genuine "no fills yet" poll carries rt_cd == "0" with an empty output1, so
            # this never blocks valid polling. Raising here is safe — _update_orders_fills logs and
            # retries it without escalating order state (unlike the status path) (JEP-182).
            if result.get("rt_cd") != "0":
                raise IOError(
                    f"KIS order-detail (fills) request failed for {order.client_order_id}: "
                    f"rt_cd={result.get('rt_cd')} msg={result.get('msg1')}"
                )
            trade_updates = self._create_order_fill_updates(order=order, fill_data=result)

        return trade_updates

    def _create_order_fill_updates(self, order: InFlightOrder, fill_data: Dict[str, Any]) -> List[TradeUpdate]:
        """Build TradeUpdates from a KIS inquire-daily-ccld response.

        KIS returns ``output1`` (a list of order rows, lowercase fields) — NOT ``output``.
        Each row's ``tot_ccld_qty`` is the CUMULATIVE filled quantity for that order, so we
        emit the INCREMENT versus the order's already-executed base: re-polling the same
        cumulative never double-counts, and a partial -> full transition is reported as the
        remaining delta. KIS does not return a per-fill commission on this endpoint, so the
        fee is taken from the connector fee schema (computed, not parsed)."""
        updates: List[TradeUpdate] = []
        rows = fill_data.get("output1", [])
        if not isinstance(rows, list):
            return updates

        eoid = str(order.exchange_order_id) if order.exchange_order_id else ""
        for row in rows:
            odno = str(row.get("odno", "") or "")
            if eoid and odno and odno != eoid:
                continue
            try:
                cum_filled = Decimal(str(row.get("tot_ccld_qty", "0") or "0"))
                avg_price = Decimal(str(row.get("avg_prvs", "0") or "0"))
                cum_amount = Decimal(str(row.get("tot_ccld_amt", "0") or "0"))
            except (InvalidOperation, ValueError, TypeError):
                continue
            if cum_filled <= Decimal("0"):
                continue
            already = order.executed_amount_base or Decimal("0")
            delta = cum_filled - already
            if delta <= Decimal("0"):
                continue
            fill_quote = cum_amount - (already * avg_price) if cum_amount > Decimal("0") else delta * avg_price
            if fill_quote <= Decimal("0"):
                fill_quote = delta * avg_price
            fee = TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=order.trade_type,
                percent_token=order.quote_asset,
                flat_fees=[],
            )
            updates.append(TradeUpdate(
                trade_id=f"{odno or order.client_order_id}-{cum_filled}",
                client_order_id=order.client_order_id,
                exchange_order_id=odno or eoid,
                trading_pair=order.trading_pair,
                fee=fee,
                fill_base_amount=delta,
                fill_quote_amount=fill_quote,
                fill_price=avg_price,
                fill_timestamp=self.current_timestamp,
            ))

        return updates

    # ------------------------------------------------------------------
    # User stream (WebSocket execution notifications)
    # ------------------------------------------------------------------

    async def _user_stream_event_listener(self):
        """Process KIS WebSocket execution notification events.

        Events from H0STCNI0/H0STCNI9 contain order/fill notifications:
        - CNTG_YN == "2": fill (체결통보)
        - CNTG_YN == "1": order acceptance/modification/cancel/reject
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type", "")
                if event_type != "execution_notification":
                    continue

                data = event_message.get("data", {})
                cntg_yn = data.get("CNTG_YN", "")
                order_no = data.get("ODER_NO", "")
                stock_code = data.get("STCK_SHRN_ISCD", "")

                # Find the tracked order by exchange_order_id
                tracked_order = self._find_tracked_order_by_exchange_id(order_no)
                if tracked_order is None:
                    continue

                if cntg_yn == "2":
                    # Fill notification
                    fill_qty = Decimal(str(data.get("CNTG_QTY", "0")))
                    fill_price = Decimal(str(data.get("CNTG_UNPR", "0")))
                    fill_amount = fill_qty * fill_price

                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=tracked_order.trade_type,
                        percent_token=tracked_order.quote_asset,
                        flat_fees=[TokenAmount(amount=Decimal("0"), token=tracked_order.quote_asset)],
                    )
                    trade_update = TradeUpdate(
                        trade_id=f"{order_no}_{data.get('STCK_CNTG_HOUR', '')}",
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=order_no,
                        trading_pair=tracked_order.trading_pair,
                        fee=fee,
                        fill_base_amount=fill_qty,
                        fill_quote_amount=fill_amount,
                        fill_price=fill_price,
                        fill_timestamp=self.current_timestamp,
                    )
                    self._order_tracker.process_trade_update(trade_update)

                elif cntg_yn == "1":
                    # Order acceptance/cancel/reject
                    rfus_yn = data.get("RFUS_YN", "N")
                    acpt_yn = data.get("ACPT_YN", "N")

                    if rfus_yn == "Y":
                        new_state = OrderState.FAILED
                    elif acpt_yn == "Y":
                        rctf_cls = data.get("RCTF_CLS", "")
                        if rctf_cls == "02":  # cancel
                            new_state = OrderState.CANCELED
                        else:
                            new_state = OrderState.OPEN
                    else:
                        continue

                    order_update = OrderUpdate(
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=order_no,
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=self.current_timestamp,
                        new_state=new_state,
                    )
                    self._order_tracker.process_order_update(order_update)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in KIS user stream event processing")
                await self._sleep(1.0)

    def _find_tracked_order_by_exchange_id(self, exchange_order_id: str) -> Optional[InFlightOrder]:
        """Find a tracked in-flight order by exchange order ID."""
        for order in self._order_tracker.active_orders.values():
            if order.exchange_order_id == exchange_order_id:
                return order
        return None

    # ------------------------------------------------------------------
    # Trading pair symbol map
    # ------------------------------------------------------------------

    async def _initialize_trading_pair_symbol_map(self):
        # KIS exposes no symbols-list API; build the map from the externally
        # configured pairs instead of a network request. The exchange symbol is
        # the numeric stock code, i.e. the base token of the "<code>-KRW" pair.
        mapping = bidict()
        for trading_pair in self._trading_pairs:
            base, _, _quote = trading_pair.partition("-")
            if base:
                mapping[base] = trading_pair
        self._set_trading_pair_symbol_map(mapping)

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
            # Routing-aware (J:KRX / NX:NXT / UN:통합), same as the orderbook REST snapshot.
            # Hardcoding 'J' froze the last price after the KRX close (15:30 KST) while NXT
            # after-market kept trading (JEP-148).
            "FID_COND_MRKT_DIV_CODE": CONSTANTS.REST_QUOTE_MRKT_DIV_BY_ROUTING[self._market_routing],
            "FID_INPUT_ISCD": symbol,
        }

        result = await self._api_get(
            path_url=CONSTANTS.DOMESTIC_STOCK_TICKER_PATH,
            params=params,
        )

        # Fail closed: KIS returns HTTP 200 with rt_cd != "0" on logical errors, and an
        # empty/0 price during venue closure or a rejected market-div code. Returning 0.0
        # (the old default) silently poisons PriceType.LastTrade (fail-open) — raise instead.
        if result.get("rt_cd") != "0":
            raise IOError(
                f"KIS last-price request failed for {trading_pair}: "
                f"rt_cd={result.get('rt_cd')} msg={result.get('msg1')}"
            )
        price_str = (result.get("output") or {}).get("stck_prpr")
        price = float(price_str) if price_str else 0.0
        if price <= 0:
            raise IOError(
                f"KIS last-price returned no/zero price for {trading_pair}: {result.get('output')}"
            )
        return price

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
