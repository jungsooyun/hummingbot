"""
KIS Futures Derivative connector (국내 개인 주식선물 — domestic individual stock futures, KRX).

4a: scaffold — instantiates, auto-discovers, reaches READY with inert funding.
4b: trading methods (order/cancel/balance/position) added in next slice.
"""
import asyncio
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.kis_futures import (
    kis_futures_constants as CONSTANTS,
    kis_futures_web_utils as web_utils,
)
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class KisFuturesDerivative(PerpetualDerivativePyBase):
    """
    KIS domestic individual stock futures connector.

    Key characteristics (mirroring KIS spot):
    - REST-only in 4a; WebSocket feeds added in 4b/slice 5.
    - ONEWAY position mode only (KIS stock futures are one-directional per account).
    - No securities transaction tax on futures (unlike spot).
    - KRW-collateral only.
    """

    web_utils = web_utils

    API_CALL_TIMEOUT = 10.0
    POLL_INTERVAL = 1.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    UPDATE_TRADE_STATUS_MIN_INTERVAL = 10.0

    def __init__(
        self,
        kis_futures_app_key: str,
        kis_futures_app_secret: str,
        kis_futures_account_number: str,
        kis_futures_hts_id: str = "",
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        balance_asset_limit: Optional[Dict] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        kis_futures_sandbox: str = "false",
        kis_futures_ws_enabled: str = "true",
        kis_futures_target_underlyings: str = "",
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self._app_key = kis_futures_app_key
        self._app_secret = kis_futures_app_secret
        self._account_number = kis_futures_account_number
        self._hts_id = kis_futures_hts_id
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._domain = domain

        # Parse sandbox/ws flags the same way KIS spot does.
        self._sandbox = str(kis_futures_sandbox).strip().lower() == "true" or domain == "sandbox"
        self._ws_enabled = str(kis_futures_ws_enabled).strip().lower() != "false"

        # Parse underlying filter.
        self._target_underlyings = [
            u.strip()
            for u in str(kis_futures_target_underlyings).split(",")
            if u.strip()
        ]

        # Parse account: "12345678-03" -> cano="12345678", acnt_prdt_cd="03"
        self._cano, _, prdt = kis_futures_account_number.partition("-")
        self._acnt_prdt_cd = prdt or CONSTANTS.ACNT_PRDT_CD

        # Futures master state (populated in 4b roll logic).
        self._contract_by_pair: Dict = {}
        self._pending_front: Dict = {}
        self._roll_pending: Dict = {}
        self._order_acks: Dict = {}

        # super().__init__ builds DS factories and starts orchestration — all
        # instance state needed by _create_* factories must be set BEFORE this.
        super().__init__(balance_asset_limit, rate_limits_share_pct)

        # Balance events are REST-polled; no WS balance stream on KIS futures.
        self.real_time_balance_update = False

    # ------------------------------------------------------------------
    # ExchangePyBase / PerpetualDerivativePyBase abstract properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "kis_futures"

    @property
    def authenticator(self):
        from hummingbot.connector.exchange.kis.kis_auth import KisAuth
        return KisAuth(
            app_key=self._app_key,
            app_secret=self._app_secret,
            sandbox=self._sandbox,
        )

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.FUT_TICKER_PATH

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.FUT_TICKER_PATH

    @property
    def check_network_request_path(self) -> str:
        # Use balance endpoint: authenticated GET available 24/7.
        # TOKEN_PATH_URL is POST-only; probing it GET → silent timeout → NOT_CONNECTED.
        return CONSTANTS.FUT_BALANCE_PATH

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    # ------------------------------------------------------------------
    # PerpetualDerivativePyBase abstracts — funding / leverage / collateral
    # ------------------------------------------------------------------

    @property
    def funding_fee_poll_interval(self) -> int:
        # KIS stock futures have no continuous funding; poll hourly as a no-op.
        return 3600

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        return "KRW"

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        return "KRW"

    async def _get_position_mode(self) -> Optional[PositionMode]:
        return PositionMode.ONEWAY

    async def _trading_pair_position_mode_set(
        self, mode: PositionMode, trading_pair: str
    ) -> Tuple[bool, str]:
        if mode != PositionMode.ONEWAY:
            return False, "kis_futures only supports the ONEWAY position mode."
        return True, ""

    async def _set_trading_pair_leverage(
        self, trading_pair: str, leverage: int
    ) -> Tuple[bool, str]:
        # KIS futures are not leveraged in the margin sense; always return success.
        return True, ""

    async def _fetch_last_fee_payment(
        self, trading_pair: str
    ) -> Tuple[float, Decimal, Decimal]:
        # KIS stock futures have no periodic funding payment; return sentinel values.
        return 0, Decimal("-1"), Decimal("-1")

    # ------------------------------------------------------------------
    # Live-hardening (mirror KIS spot)
    # ------------------------------------------------------------------

    def _is_user_stream_initialized(self) -> bool:
        # REST-only mode: treat user stream as always ready.
        return True

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False):
        # KIS does not require time synchronization.
        return

    def _is_request_exception_related_to_time_synchronizer(
        self, request_exception: Exception
    ) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(
        self, status_update_exception: Exception
    ) -> bool:
        return False

    def _is_order_not_found_during_cancelation_error(
        self, cancelation_exception: Exception
    ) -> bool:
        return False

    async def _update_trading_fees(self):
        pass

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT]

    # ------------------------------------------------------------------
    # Factory methods (called by ExchangePyBase.__init__)
    # ------------------------------------------------------------------

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        from hummingbot.connector.derivative.kis_futures.kis_futures_api_order_book_data_source import (
            KisFuturesAPIOrderBookDataSource,
        )
        return KisFuturesAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            auth=self._auth,
            domain=self._domain,
            ws_enabled=self._ws_enabled,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        from hummingbot.connector.derivative.kis_futures.kis_futures_api_user_stream_data_source import (
            KisFuturesAPIUserStreamDataSource,
        )
        return KisFuturesAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self._domain,
            ws_enabled=self._ws_enabled,
        )

    # ------------------------------------------------------------------
    # Symbol map (minimal — master-file population added in 4b)
    # ------------------------------------------------------------------

    async def _initialize_trading_pair_symbol_map(self):
        mapping: bidict = bidict()
        for tp in self._trading_pairs:
            base = tp.partition("-")[0]
            if base:
                mapping[base] = tp
        self._set_trading_pair_symbol_map(mapping)

    async def _update_trading_rules(self):
        self._trading_rules.clear()
        for tp in self._trading_pairs:
            self._trading_rules[tp] = TradingRule(
                trading_pair=tp,
                min_order_size=Decimal("1"),
                min_price_increment=Decimal("5"),
                min_base_amount_increment=Decimal("1"),
                buy_order_collateral_token="KRW",
                sell_order_collateral_token="KRW",
            )

    # ------------------------------------------------------------------
    # 4b stubs — must exist for abstract base to instantiate
    # ------------------------------------------------------------------

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:
        raise NotImplementedError("4b")

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        raise NotImplementedError("4b")

    async def _update_balances(self):
        # 4a: no-op; replaced in 4b with real KIS balance REST call.
        return

    async def _update_positions(self):
        # 4a: no-op; replaced in 4b with real KIS position REST call.
        return

    async def _format_trading_rules(self, exchange_info_dict: Dict) -> List[TradingRule]:
        return []

    async def _request_order_status(self, tracked_order: InFlightOrder):
        raise NotImplementedError("4b")

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        return []

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        return 0.0

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict):
        # KIS futures has no symbols-list API; symbol map is built from configured pairs.
        mapping: bidict = bidict()
        for tp in self._trading_pairs:
            base = tp.partition("-")[0]
            if base:
                mapping[base] = tp
        self._set_trading_pair_symbol_map(mapping)

    async def _user_stream_event_listener(self):
        # 4a: no-op; WebSocket exec notifications added in slice 5 (Task 4).
        async for event_message in self._iter_user_event_queue():
            pass

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        from hummingbot.core.utils.estimate_fee import build_trade_fee
        is_maker = is_maker if is_maker is not None else (order_type is OrderType.LIMIT)
        return build_trade_fee(
            self.name, is_maker, base_currency, quote_currency,
            order_type, order_side, amount, price,
        )
