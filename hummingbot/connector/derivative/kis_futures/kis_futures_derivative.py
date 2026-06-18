"""
KIS Futures Derivative connector (국내 개인 주식선물 — domestic individual stock futures, KRX).

4a: scaffold — instantiates, auto-discovers, reaches READY with inert funding.
4b: orders/cancel/balance/positions/reconcile/front-month rollover.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.kis_futures import (
    kis_futures_constants as CONSTANTS,
    kis_futures_web_utils as web_utils,
)
from hummingbot.connector.derivative.kis_futures.kis_futures_master import (
    RemapGuard,
    download_master,
    parse_master_bytes,
    resolve_front_month,
)
from hummingbot.connector.derivative.kis_futures.kis_futures_tick import (
    floor_to_tick,
    tick_size_for_price,
)
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class KisFuturesDerivative(PerpetualDerivativePyBase):
    """
    KIS domestic individual stock futures connector.

    Key characteristics (mirroring KIS spot):
    - REST order lifecycle + REST-polled fills (inquire-ccnl / FUT_CCNL_PATH).
    - ONEWAY position mode only (KIS stock futures are one-directional per account).
    - No securities transaction tax on futures (unlike spot).
    - KRW-collateral only; leverage = 1 (contract multiplier handled at strategy layer).
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

        # Futures master state (populated by _resolve_front_months).
        self._contract_by_pair: Dict = {}
        # Previous front-month contracts, retained until the old code is flat
        # (no open orders + no open position).  Used by _pair_for_code so that
        # fills/positions on the old contract still map correctly after a roll.
        self._prev_contract_by_pair: Dict = {}
        self._pending_front: Dict = {}
        self._roll_pending: Dict = {}
        self._order_acks: Dict = {}

        # Last successfully-resolved tick per pair (sticky fallback when a fresh
        # reference price cannot be fetched during a trading-rules refresh).
        self._last_tick_by_pair: Dict[str, Decimal] = {}

        # Per-pair roll guard: prevents _resolve_front_months swap from interleaving
        # with _place_order reading fc.short_code.
        self._roll_lock: asyncio.Lock = asyncio.Lock()

        # Set True after the FIRST successful _update_positions poll.
        # Prevents an unsafe auto-roll before cached positions are confirmed.
        self._positions_polled_once: bool = False

        # super().__init__ builds DS factories — all instance state must be ready before.
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
        # Propagate sandbox flag to the URL builder (public_rest_url checks for "sandbox").
        return "sandbox" if self._sandbox else self._domain

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
        # FUT_BALANCE_PATH is a GET; TOKEN_PATH_URL is POST-only → silent timeout.
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
        return True, ""

    async def _fetch_last_fee_payment(
        self, trading_pair: str
    ) -> Tuple[float, Decimal, Decimal]:
        # KIS stock futures have no periodic funding payment.
        return 0, Decimal("-1"), Decimal("-1")

    # ------------------------------------------------------------------
    # Live-hardening (mirror KIS spot)
    # ------------------------------------------------------------------

    def _is_user_stream_initialized(self) -> bool:
        return True

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False):
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
    # Factory methods
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
    # Symbol map + trading rules
    # ------------------------------------------------------------------

    async def _initialize_trading_pair_symbol_map(self):
        mapping: bidict = bidict()
        for tp in self._trading_pairs:
            base = tp.partition("-")[0]
            if base:
                mapping[base] = tp
        self._set_trading_pair_symbol_map(mapping)

    # Fallback tick used only on the very first rules build when no reference
    # price is yet available (off-hours / pre-warmup). _format_price re-floors
    # every order price to the exact per-price KRX tier, so this value only
    # affects pre-floor strategy quote spacing until a live price arrives.
    _DEFAULT_TICK = Decimal("50")

    async def _update_trading_rules(self):
        """Resolve front-month contracts first, then build per-pair trading rules
        with a price-tiered min_price_increment (KRX 호가가격단위)."""
        # Resolve/refresh front-months FIRST so contract codes exist for the
        # reference-price lookups below; fail-closed on download error.
        try:
            await self._resolve_front_months()
        except Exception as e:
            self.logger().warning(
                f"[kis_futures] front-month resolution failed (using prior map): {e}"
            )
        self._trading_rules.clear()
        for tp in self._trading_pairs:
            tick = await self._resolve_pair_tick(tp)
            self._trading_rules[tp] = TradingRule(
                trading_pair=tp,
                min_order_size=Decimal("1"),
                min_price_increment=tick,
                min_base_amount_increment=Decimal("1"),
                buy_order_collateral_token="KRW",
                sell_order_collateral_token="KRW",
            )

    async def _resolve_pair_tick(self, trading_pair: str) -> Decimal:
        """Best-effort KRX tick (호가가격단위) for the pair's current price tier.

        Fetches the last traded price and maps it to the KRX tier. Fail-soft:
        on any error, reuse the last good tick, else the default — never block
        rule-building (which would halt trading). Order-price correctness is
        independently guaranteed by _format_price's per-price tiered floor.
        """
        try:
            ref = Decimal(str(await self._get_last_traded_price(trading_pair)))
            tick = tick_size_for_price(ref)
            self._last_tick_by_pair[trading_pair] = tick
            return tick
        except Exception as e:
            prior = self._last_tick_by_pair.get(trading_pair)
            if prior is not None:
                return prior
            self.logger().warning(
                f"[kis_futures] tick reference price unavailable for {trading_pair} "
                f"({e}); using default tick {self._DEFAULT_TICK}"
            )
            return self._DEFAULT_TICK

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict):
        # KIS futures has no symbols-list API; symbol map is built from configured pairs.
        mapping: bidict = bidict()
        for tp in self._trading_pairs:
            base = tp.partition("-")[0]
            if base:
                mapping[base] = tp
        self._set_trading_pair_symbol_map(mapping)

    async def _user_stream_event_listener(self):
        """Process H0IFCNI0 execution-notification events from the user stream DS.

        oder_kind2 values:
          '0' — 체결통보 (fill): WS fills are NOT emitted as TradeUpdates here.
                Fills are owned exclusively by _all_trade_updates_for_order (ccnl
                REST poll) to avoid double-counting with a different trade_id scheme.
                The exec-notice simply acts as an early signal; the ccnl poll is the
                single authoritative fill source.  TODO: unify dedup when fill events
                are confirmed to carry a stable unique fill ID.
          'L' — 주문·정정·취소·거부 접수통보; derive OrderState and build OrderUpdate.

        Best-effort: malformed or unrecognised events are logged and dropped —
        the listener must never crash.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type", "")
                if event_type != "execution_notification":
                    continue

                data = event_message.get("data", {})
                oder_kind2 = data.get("oder_kind2", "")
                oder_no = data.get("oder_no", "")

                tracked_order = self._find_tracked_order_by_exchange_id(oder_no)
                if tracked_order is None:
                    # Possibly an order from a previous connector session — skip.
                    continue

                if oder_kind2 == "0":
                    # --- fill notification: do NOT emit TradeUpdate from WS ---
                    # ccnl REST poll (_all_trade_updates_for_order) is the single
                    # authoritative fill source.  No-op here prevents double-counting.
                    pass

                elif oder_kind2 == "L":
                    # --- order lifecycle ---
                    rfus_yn = data.get("rfus_yn", "N")
                    acpt_yn = data.get("acpt_yn", "N")
                    rctf_cls = data.get("rctf_cls", "")

                    if rfus_yn == "Y":
                        new_state = OrderState.FAILED
                    elif acpt_yn == "Y":
                        # rctf_cls "02" = cancel; otherwise new/revise acceptance
                        new_state = OrderState.CANCELED if rctf_cls == "02" else OrderState.OPEN
                    else:
                        # Neither confirmed accepted nor rejected — ignore.
                        continue

                    order_update = OrderUpdate(
                        client_order_id=tracked_order.client_order_id,
                        exchange_order_id=oder_no,
                        trading_pair=tracked_order.trading_pair,
                        update_timestamp=self.current_timestamp,
                        new_state=new_state,
                    )
                    self._order_tracker.process_order_update(order_update)

                else:
                    self.logger().warning(
                        f"[kis_futures] unknown oder_kind2={oder_kind2!r} in exec-notice "
                        f"oder_no={oder_no} — skipping"
                    )

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "[kis_futures] unexpected error in user stream event listener"
                )
                await self._sleep(1.0)

    def _find_tracked_order_by_exchange_id(self, exchange_order_id: str) -> Optional[InFlightOrder]:
        """Return the in-flight order matching exchange_order_id, or None."""
        for order in self._order_tracker.active_orders.values():
            if order.exchange_order_id == exchange_order_id:
                return order
        return None

    # ------------------------------------------------------------------
    # Network check (mirror KIS spot)
    # ------------------------------------------------------------------

    async def _make_network_check_request(self):
        """Authenticated balance GET proves network + auth are healthy (24/7)."""
        tr_id, params = self._balance_request_args()
        await self._api_get(
            path_url=CONSTANTS.FUT_BALANCE_PATH,
            params=params,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )

    # ------------------------------------------------------------------
    # Task 3b: helpers + orders
    # ------------------------------------------------------------------

    @staticmethod
    def _sandbox_tr_id(tr_id: str) -> str:
        """KIS sandbox replaces the leading 'T' with 'V' in trading TR_IDs."""
        return ("V" + tr_id[1:]) if tr_id.startswith("T") else tr_id

    @staticmethod
    def _validate_contract_qty(amount: Decimal) -> int:
        """Raise ValueError unless amount is a positive integer (whole contracts only)."""
        if amount != amount.to_integral_value() or amount <= 0:
            raise ValueError(
                f"kis_futures qty must be a positive integer number of contracts: {amount}"
            )
        return int(amount)

    def _format_price(self, price: Decimal, trading_pair: str) -> str:
        """Format an order price as a plain integer string, floored to the exact
        KRX 호가가격단위 (호가단위) for the price's own tier.

        Authoritative tick gate: the tick is derived from the order price itself
        (price-tiered, per KRX 2026 rule) rather than the static per-pair
        min_price_increment, so the result is always a KRX-valid price regardless
        of where the price sits relative to the rule tick. Mirrors the
        live-verified nautilus gate5c floor_to_tick.

        Fail-closed: raises ValueError for non-positive prices (or prices that
        floor to <= 0).
        """
        if price <= 0:
            raise ValueError(f"kis_futures price must be positive: {price}")
        aligned = floor_to_tick(price)
        if aligned <= 0:
            raise ValueError(
                f"kis_futures price {price} floored to non-positive tick boundary"
            )
        text = format(aligned, "f")
        # Strip trailing decimal zeros (e.g. "50000.0" → "50000")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

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
        # Read contract code under _roll_lock to prevent interleaving with a concurrent
        # _resolve_front_months swap that could change fc.short_code mid-flight.
        async with self._roll_lock:
            fc = self._contract_by_pair[trading_pair]  # KeyError intentional: caller ensures resolution
        qty = self._validate_contract_qty(amount)
        # 매수(long)=02, 매도(short)=01
        sll_buy = "02" if trade_type == TradeType.BUY else "01"
        body = {
            "ORD_PRCS_DVSN_CD": "02",
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "SLL_BUY_DVSN_CD": sll_buy,
            "SHTN_PDNO": fc.short_code,
            "ORD_QTY": str(qty),
            "UNIT_PRICE": self._format_price(price, trading_pair),
            "NMPR_TYPE_CD": "01",
            "KRX_NMPR_CNDT_CD": "0",
            "ORD_DVSN_CD": "01",
            "CTAC_TLNO": "",
            "FUOP_ITEM_DVSN_CD": "",
        }
        tr_id = self._sandbox_tr_id(CONSTANTS.FUT_ORDER_TR_ID)
        resp = await self._api_post(
            path_url=CONSTANTS.FUT_ORDER_PATH,
            data=body,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )
        if resp.get("rt_cd") != "0":
            raise IOError(
                f"KIS futures order rejected: rt_cd={resp.get('rt_cd')} msg={resp.get('msg1')}"
            )
        odno = resp["output"]["ODNO"]
        self._order_acks[order_id] = {"odno": odno, "sll_buy": sll_buy}
        return str(odno), self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        body = {
            "ORD_PRCS_DVSN_CD": "02",
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "RVSE_CNCL_DVSN_CD": "02",
            "ORGN_ODNO": tracked_order.exchange_order_id,
            "ORD_QTY": "0",
            "UNIT_PRICE": "0",
            "NMPR_TYPE_CD": "01",
            "KRX_NMPR_CNDT_CD": "0",
            "ORD_DVSN_CD": "01",
            "RMN_QTY_YN": "Y",
            "FUOP_ITEM_DVSN_CD": "",
        }
        tr_id = self._sandbox_tr_id(CONSTANTS.FUT_CANCEL_TR_ID)
        resp = await self._api_post(
            path_url=CONSTANTS.FUT_CANCEL_PATH,
            data=body,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )
        if resp.get("rt_cd") != "0":
            raise IOError(
                f"KIS futures cancel rejected: rt_cd={resp.get('rt_cd')} msg={resp.get('msg1')}"
            )
        return True

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

    # ------------------------------------------------------------------
    # Task 3c: balances + positions
    # ------------------------------------------------------------------

    def _balance_request_args(self) -> Tuple[str, Dict[str, str]]:
        tr_id = self._sandbox_tr_id(CONSTANTS.FUT_BALANCE_TR_ID)
        params = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "MGNA_DVSN": "01",
            "EXCC_STAT_CD": "1",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }
        return tr_id, params

    async def _fetch_balance(self) -> Dict[str, Any]:
        tr_id, params = self._balance_request_args()
        return await self._api_get(
            path_url=CONSTANTS.FUT_BALANCE_PATH,
            params=params,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )

    async def _update_positions(self):
        """Refresh positions from KIS balance response (output1).

        Atomic parse-then-apply pattern:
        1. PARSE the entire snapshot into memory — any malformed row raises IOError
           BEFORE any set_position/remove_position call (prior state fully intact,
           _positions_polled_once stays False).
        2. APPLY atomically only after the whole snapshot parses cleanly.
        3. Set _positions_polled_once = True only at the very end.

        Fail-closed:
        - Raises IOError if rt_cd != "0".
        - Raises IOError if output1 is not a list.
        - Raises IOError on any malformed numeric field in any row.
        """
        from hummingbot.connector.derivative.position import Position
        from hummingbot.core.data_type.common import PositionSide

        resp = await self._fetch_balance()
        if resp.get("rt_cd") != "0":
            raise IOError(
                f"KIS futures _update_positions: rt_cd={resp.get('rt_cd')} "
                f"msg={resp.get('msg1')} — keeping prior positions"
            )
        output1 = resp.get("output1")
        if not isinstance(output1, list):
            raise IOError(
                "[kis_futures] _update_positions: output1 missing or not a list "
                "— keeping prior positions"
            )

        # Phase 1: PARSE — no state mutation; raise on any malformed row.
        parsed: Dict[str, Any] = {}   # pos_key -> Position
        roll_codes: List[Tuple[str, Dict]] = []  # non-current codes to expose roll

        for row in output1:
            code = (row.get("shtn_pdno") or row.get("pdno") or "").strip()
            pair = self._pair_for_code(code)
            if pair is None:
                roll_codes.append((code, row))
                continue
            try:
                qty = Decimal(str(row.get("cblc_qty", "0") or "0"))
            except (InvalidOperation, ValueError, TypeError) as exc:
                raise IOError(
                    f"[kis_futures] _update_positions: malformed cblc_qty in row {row}"
                ) from exc
            if qty == 0:
                continue
            side = (
                PositionSide.LONG
                if str(row.get("sll_buy_dvsn_cd")) == "02"
                else PositionSide.SHORT
            )
            entry_price_raw = row.get("pchs_avg_pric") or row.get("ccld_avg_unpr1") or "0"
            try:
                unrealized_pnl = Decimal(str(row.get("evlu_pfls_amt", "0") or "0"))
                entry_price = Decimal(str(entry_price_raw))
            except (InvalidOperation, ValueError, TypeError) as exc:
                raise IOError(
                    f"[kis_futures] _update_positions: malformed pnl/price in row {row}"
                ) from exc
            pos_key = self._perpetual_trading.position_key(pair, side)
            parsed[pos_key] = Position(
                trading_pair=pair,
                position_side=side,
                unrealized_pnl=unrealized_pnl,
                entry_price=entry_price,
                amount=(qty if side == PositionSide.LONG else -qty),
                leverage=Decimal("1"),
            )

        # Phase 2: APPLY — only reached when the whole snapshot parsed cleanly.
        for pos_key, pos in parsed.items():
            self._perpetual_trading.set_position(pos_key, pos)
        for k in list(self._perpetual_trading.account_positions.keys()):
            if k not in parsed:
                self._perpetual_trading.remove_position(k)
        for code, row in roll_codes:
            self._maybe_expose_roll_position(code, row)

        # Mark verified only after a successful apply.
        self._positions_polled_once = True

    async def _update_balances(self):
        """Refresh KRW balances from KIS balance response (output2).

        Fail-closed:
        - Raises IOError if rt_cd != "0" — caller retries; prior balances kept.
        - Raises IOError if output2 is missing/empty — no zeroing of KRW.
        """
        resp = await self._fetch_balance()
        if resp.get("rt_cd") != "0":
            raise IOError(
                f"KIS futures _update_balances: rt_cd={resp.get('rt_cd')} "
                f"msg={resp.get('msg1')} — keeping prior balances"
            )
        out2 = resp.get("output2")
        if isinstance(out2, list):
            out2 = out2[0] if out2 else None
        if not out2:
            raise IOError(
                "[kis_futures] _update_balances: output2 missing/empty "
                "— keeping prior balances"
            )
        total = Decimal(str(out2.get("dnca_tot_amt") or out2.get("tot_dncl_amt") or "0"))
        free = Decimal(str(out2.get("ord_psbl_cash") or "0"))
        self._account_balances["KRW"] = total
        self._account_available_balances["KRW"] = free

    async def _format_trading_rules(self, exchange_info_dict: Dict) -> List[TradingRule]:
        return []

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """Fetch last traded price from KIS ticker endpoint (FHMIF10000000).

        Fail-closed: raises IOError on rt_cd != "0" or price <= 0, never returns 0.0.
        """
        code = self.current_contract_code(trading_pair)
        if not code:
            raise IOError(
                f"[kis_futures] _get_last_traded_price: no contract code for {trading_pair}"
            )
        params = {
            "FID_COND_MRKT_DIV_CODE": CONSTANTS.MRKT_DIV_STOCK_FUTURE,
            "FID_INPUT_ISCD": code,
        }
        resp = await self._api_get(
            path_url=CONSTANTS.FUT_TICKER_PATH,
            params=params,
            is_auth_required=True,
            headers={"tr_id": CONSTANTS.FUT_TICKER_TR_ID},
        )
        if resp.get("rt_cd") != "0":
            raise IOError(
                f"[kis_futures] _get_last_traded_price: rt_cd={resp.get('rt_cd')} "
                f"msg={resp.get('msg1')} for {trading_pair}"
            )
        output = resp.get("output") or resp.get("output1") or {}
        if isinstance(output, list):
            output = output[0] if output else {}
        raw_price = output.get("futs_prpr") or output.get("prpr") or "0"
        try:
            price = float(raw_price)
        except (ValueError, TypeError):
            raise IOError(
                f"[kis_futures] _get_last_traded_price: malformed price {raw_price!r} "
                f"for {trading_pair}"
            )
        if price <= 0:
            raise IOError(
                f"[kis_futures] _get_last_traded_price: price={price} <= 0 for {trading_pair}"
            )
        return price

    # ------------------------------------------------------------------
    # Task 3d: reconcile (inquire-ccnl + order status)
    # ------------------------------------------------------------------

    @staticmethod
    def _kst_today() -> str:
        """Return today's KST date as YYYYMMDD (UTC+9).

        KIS inquire-ccnl requires a date range; same-day fills live under
        today's KST date.  The engine host runs UTC, so we derive KST explicitly —
        an empty date range returns no rows, preventing order reconciliation.
        """
        return datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")

    async def _request_futures_ccnl(self, order: InFlightOrder) -> Dict[str, Any]:
        """Query KIS futures fill-history endpoint (inquire-ccnl)."""
        tr_id = self._sandbox_tr_id(CONSTANTS.FUT_CCNL_TR_ID)
        today = self._kst_today()
        params = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "INQR_STRT_DT": today,
            "INQR_END_DT": today,
            "SLL_BUY_DVSN_CD": "00",
            "CCLD_DVSN": "00",
            "FUOP_ITEM_DVSN_CD": "",
            "PDNO": "",
            "ODNO": order.exchange_order_id or "",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }
        return await self._api_get(
            path_url=CONSTANTS.FUT_CCNL_PATH,
            params=params,
            is_auth_required=True,
            headers={"tr_id": tr_id},
        )

    @staticmethod
    @staticmethod
    def _match_ccld_row(result: Dict[str, Any], order: InFlightOrder) -> Optional[Dict[str, Any]]:
        """Return the ccnl output1 row matching this order's exchange_order_id.

        Returns None (not found sentinel) when no row matches — callers must treat
        None as "no update available" and NOT mutate order state.  The previous
        first-row fallback was removed: inferring state from an unrelated order's
        row can corrupt order state.
        """
        rows = result.get("output1", [])
        if not isinstance(rows, list) or not rows:
            return None
        eoid = str(order.exchange_order_id) if order.exchange_order_id else ""
        if eoid:
            for row in rows:
                if str(row.get("odno", "")) == eoid:
                    return row
        return None

    def _infer_order_state(self, row: Dict[str, Any], order: InFlightOrder) -> OrderState:
        """Derive state from a KIS ccnl output1 row.

        Fields: tot_ccld_qty (cumulative filled), ord_qty, cncl_yn.
        """
        try:
            ccld_qty = Decimal(str(row.get("tot_ccld_qty", "0") or "0"))
            ord_qty = Decimal(str(row.get("ord_qty", "0") or "0"))
        except (InvalidOperation, ValueError, TypeError):
            self.logger().warning(
                f"[kis_futures] _infer_order_state: malformed numeric field in ccnl row "
                f"for order {order.client_order_id} — defaulting to OPEN. row={row}"
            )
            return OrderState.OPEN

        if str(row.get("cncl_yn", "N")).upper() == "Y":
            return OrderState.CANCELED
        if ord_qty > 0 and ccld_qty >= ord_qty:
            return OrderState.FILLED
        if ccld_qty > 0:
            return OrderState.PARTIALLY_FILLED
        return OrderState.OPEN

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        result = await self._request_futures_ccnl(tracked_order)
        row = self._match_ccld_row(result, tracked_order)
        if row is None:
            # No matching ccnl row yet (order may still be pending or page didn't include it).
            # Return a non-mutating OPEN update so the tracker keeps the current state.
            return OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=tracked_order.exchange_order_id or "",
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=tracked_order.current_state,
            )
        new_state = self._infer_order_state(row, tracked_order)
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(
                row.get("odno", tracked_order.exchange_order_id or "")
            ),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=new_state,
        )

    def _multiplier_for(self, trading_pair: str) -> Decimal:
        """Return the contract multiplier for a pair (shares per contract).

        KIS stock futures: 1 contract = multiplier × underlying shares.
        Default 10 if the contract map hasn't been resolved yet.
        The multiplier converts contract qty to notional:
          notional_KRW = qty_contracts × price × multiplier
        """
        fc = self._contract_by_pair.get(trading_pair)
        if fc is not None and fc.multiplier and fc.multiplier > 0:
            return fc.multiplier
        return Decimal("10")

    def _create_order_fill_updates(
        self, order: InFlightOrder, fill_data: Dict[str, Any]
    ) -> List[TradeUpdate]:
        """Build TradeUpdates from KIS futures ccnl response.

        KIS returns cumulative tot_ccld_qty per row; we emit the INCREMENT vs the
        order's already-executed base to avoid double-counting on repeated polls.
        avg_idx = weighted average fill price for futures (vs avg_prvs used in spot).

        Multiplier: fill_quote_amount = delta_contracts × price × multiplier.
        fill_base_amount = delta_contracts (hummingbot tracks in contract units).
        Fee is computed from notional (not parsed); no STT on futures.
        """
        updates: List[TradeUpdate] = []
        rows = fill_data.get("output1", [])
        if not isinstance(rows, list):
            return updates

        multiplier = self._multiplier_for(order.trading_pair)
        eoid = str(order.exchange_order_id) if order.exchange_order_id else ""
        for row in rows:
            odno = str(row.get("odno", "") or "")
            if eoid and odno and odno != eoid:
                continue
            try:
                cum_filled = Decimal(str(row.get("tot_ccld_qty", "0") or "0"))
                avg_price = Decimal(str(row.get("avg_idx", "0") or "0"))
            except (InvalidOperation, ValueError, TypeError):
                self.logger().warning(
                    f"[kis_futures] _create_order_fill_updates: malformed numeric field "
                    f"for order {order.client_order_id} — skipping row. row={row}"
                )
                continue
            if cum_filled <= Decimal("0"):
                continue
            already = order.executed_amount_base or Decimal("0")
            delta = cum_filled - already
            if delta <= Decimal("0"):
                continue
            # Notional = contracts × price × multiplier (KRW value of the fill)
            fill_quote = delta * avg_price * multiplier
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=PositionAction.OPEN,
                percent_token=order.quote_asset,
                flat_fees=[],
            )
            updates.append(
                TradeUpdate(
                    trade_id=f"{odno or order.client_order_id}-{cum_filled}",
                    client_order_id=order.client_order_id,
                    exchange_order_id=odno or eoid,
                    trading_pair=order.trading_pair,
                    fee=fee,
                    fill_base_amount=delta,
                    fill_quote_amount=fill_quote,
                    fill_price=avg_price,
                    fill_timestamp=self.current_timestamp,
                )
            )
        return updates

    async def _all_trade_updates_for_order(
        self, order: InFlightOrder
    ) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []
        result = await self._request_futures_ccnl(order)
        return self._create_order_fill_updates(order=order, fill_data=result)

    # ------------------------------------------------------------------
    # Task 3d: front-month resolution + rollover accessors
    # ------------------------------------------------------------------

    def _position_amount(self, pair: str) -> Decimal:
        pos = self._perpetual_trading.account_positions.get(pair)
        return abs(pos.amount) if pos else Decimal("0")

    def _pair_for_code(self, code: str) -> Optional[str]:
        """Return the trading pair for a contract short code.

        Checks the active map first, then the previous-contract map so that
        fills and positions arriving on the old contract code after a roll
        are still resolved correctly.  The prev entry is retained until the
        old code is confirmed flat (see _resolve_front_months).
        """
        for pair, fc in self._contract_by_pair.items():
            if fc.short_code == code:
                return pair
        # Fall back to prev-contract map (post-roll stale fills/positions).
        for pair, fc in self._prev_contract_by_pair.items():
            if fc.short_code == code:
                return pair
        return None

    def _maybe_expose_roll_position(self, code: str, row: Dict[str, Any]):
        """Mark roll_pending if a balance row belongs to a pending-front contract."""
        for pair, fc in self._contract_by_pair.items():
            if self._pending_front.get(pair) and code == fc.short_code:
                self._roll_pending[pair] = True
                return

    def current_contract_code(self, pair: str) -> Optional[str]:
        fc = self._contract_by_pair.get(pair)
        return fc.short_code if fc else None

    def front_month_code(self, pair: str) -> Optional[str]:
        fc = self._pending_front.get(pair) or self._contract_by_pair.get(pair)
        return fc.short_code if fc else None

    def expiry_yyyymm(self, pair: str) -> Optional[str]:
        fc = self._contract_by_pair.get(pair)
        return fc.expiry_yyyymm if fc else None

    def is_roll_pending(self, pair: str) -> bool:
        return bool(self._roll_pending.get(pair))

    @property
    def roll_pending_pairs(self) -> List[str]:
        return [p for p, v in self._roll_pending.items() if v]

    async def _resolve_front_months(self):
        """Download master, resolve front-month for each configured pair, apply fail-closed roll.

        Fail-closed: if download raises, the caller (typically _update_trading_rules)
        catches and warns, keeping the prior _contract_by_pair map intact.

        Atomic rollover:
        - _roll_lock serialises the contract-map swap with _place_order reads of
          fc.short_code, preventing interleave races.
        - When a roll IS applied, the outgoing contract is moved to
          _prev_contract_by_pair[pair] so that stale fills and positions on the
          old code are still resolved correctly by _pair_for_code.
        - The prev entry is cleared only once the old code is flat (no position
          + no open orders on it).
        """
        import aiohttp
        async with aiohttp.ClientSession() as session:
            raw = await download_master(session)
        contracts = parse_master_bytes(raw)
        async with self._roll_lock:
            for pair in self._trading_pairs:
                underlying = pair.split("-")[0]
                fc = resolve_front_month(contracts, underlying)
                if fc is None:
                    continue
                cur = self._contract_by_pair.get(pair)
                if cur is None:
                    self._contract_by_pair[pair] = fc
                    self._roll_pending[pair] = False
                    continue
                if fc.short_code != cur.short_code:
                    has_open = any(
                        o.trading_pair == pair
                        for o in self._order_tracker.active_orders.values()
                    )
                    # Fail-closed: do NOT auto-switch until at least one _update_positions
                    # poll has confirmed the cached position state.  On fresh start the
                    # positions cache is empty and looks flat — switching here could drop
                    # an existing old-contract position silently.
                    if self._positions_polled_once and RemapGuard().can_remap(has_open, self._position_amount(pair)):
                        # Atomic swap: retain old contract for stale-fill resolution.
                        self._prev_contract_by_pair[pair] = cur
                        self._contract_by_pair[pair] = fc
                        self._roll_pending[pair] = False
                        self._pending_front.pop(pair, None)
                        self.logger().info(
                            f"[kis_futures] rolled {pair}: "
                            f"{cur.short_code} → {fc.short_code}; "
                            f"prev retained for stale-fill resolution."
                        )
                    else:
                        self._roll_pending[pair] = True
                        self._pending_front[pair] = fc
                        reason = (
                            "positions not yet polled — deferring roll until first _update_positions"
                            if not self._positions_polled_once
                            else "open orders or non-zero position — flatten to roll"
                        )
                        self.logger().warning(
                            f"[kis_futures] roll pending {pair}: "
                            f"{cur.short_code}->{fc.short_code}; {reason}."
                        )
                else:
                    # Same contract; check if prev entry can now be cleared.
                    prev = self._prev_contract_by_pair.get(pair)
                    if prev is not None:
                        old_has_open = any(
                            o.trading_pair == pair and o.exchange_order_id is not None
                            for o in self._order_tracker.active_orders.values()
                        )
                        old_pos_flat = self._position_amount(pair) == Decimal("0")
                        if old_pos_flat and not old_has_open:
                            del self._prev_contract_by_pair[pair]
                            self.logger().info(
                                f"[kis_futures] cleared prev contract for {pair} "
                                f"(old code {prev.short_code} confirmed flat)."
                            )
