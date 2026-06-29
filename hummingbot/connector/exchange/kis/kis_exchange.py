import asyncio
import copy
import threading
import time
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
from hummingbot.connector.exchange.kis.kis_holiday_cache import KisHolidayCache
from hummingbot.connector.exchange.kis.kis_ws_hub import KisWsHub
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class KisNetworkCheckError(IOError):
    """Raised by ``_make_network_check_request`` on an HTTP-200 KIS logical error
    (rt_cd != "0"). Carries the structured ``msg_cd`` so ``check_network`` can
    classify an EGW00123 expired-token signal (which appears ONLY in msg_cd on the
    HTTP-200 shape — msg1 has no code) without fragile free-text matching (JEP-206).
    """

    def __init__(self, message: str, msg_cd: str = "") -> None:
        super().__init__(message)
        self.msg_cd = msg_cd


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
    # NOTE: balance/poll cadence is governed by ExchangePyBase SHORT_POLL_INTERVAL(5s) /
    # LONG_POLL_INTERVAL(120s) via _get_poll_interval; a `POLL_INTERVAL` attr here is dead (the
    # base never reads it). Removed (2026-06-29) so it cannot mislead readers into thinking
    # balances poll at 1s. To change cadence, override SHORT_POLL_INTERVAL or _get_poll_interval.
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    UPDATE_TRADE_STATUS_MIN_INTERVAL = 10.0
    # JEP-203: consecutive check_network probe failures tolerated before reporting
    # NOT_CONNECTED. Debounces transient REST blips so one balance-probe timeout does not
    # tear down the healthy WS hub (the JEP-134 WS-staleness gate is the fast safety net).
    _NET_CHECK_FAILURE_THRESHOLD = 3
    # JEP-206: minimum interval between two FORCED token re-auths driven by an EGW00123
    # expired-token signal on the balance probe. KIS rate-limits oauth2/tokenP issuance to
    # ~1/min; a genuinely revoked appkey keeps returning EGW00123, so without a cooldown the
    # connector would POST tokenP on every check_network cycle. >=60s bounds tokenP pressure
    # to KIS's ceiling while still recovering instantly from a normal early expiry.
    _FORCED_REAUTH_COOLDOWN_S = 60.0

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
        kis_market_status_enabled: str = "false",
        kis_market_status_capture_only: str = "false",
        kis_hts_id: str = "",
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
        # Customer HTS ID for the execution-notice realtime channel (H0STCNI0).
        # Empty -> exec-notice WS skipped (REST fill fallback); see kis_utils /
        # the user-stream data source. The factory forwards every config-map
        # field as a kwarg, so the constructor MUST accept it.
        self._hts_id = (kis_hts_id or "").strip()
        self._ws_hub = None
        # JEP-198 session-halt per-pair state (perf_counter clock)
        self._sh_hour_cls_code: Dict[str, str] = {}
        self._sh_prev_book_sig: Dict[str, tuple] = {}
        self._sh_last_book_change: Dict[str, float] = {}
        self._net_check_consec_failures: int = 0  # JEP-203 check_network debounce counter
        self._last_forced_reauth_ts: float = 0.0  # JEP-206 EGW00123 forced-reauth cooldown clock
        # Phase-2 latches (populated only when kis_market_status_enabled)
        self._market_status_enabled = str(kis_market_status_enabled).strip().lower() == "true"
        # JEP-201 capture-only: subscribe H0?MKO0 + log raw frames for decode verification,
        # WITHOUT feeding the latch (no over-pause). Full enabled takes precedence.
        self._market_status_capture_only = (
            str(kis_market_status_capture_only).strip().lower() == "true" and not self._market_status_enabled
        )
        if self._market_status_capture_only:
            self.logger().info(
                "JEP-201: kis_market_status_capture_only=True — subscribing the H0STMKO0 "
                "market-status feed in LOG-ONLY mode (no latch, no gate effect) to capture "
                "live CB/VI/normal frames for decode verification."
            )
        if self._market_status_enabled and not CONSTANTS.KNOWN_NORMAL_MKOP:
            self.logger().warning(
                "JEP-198: kis_market_status_enabled=True but KNOWN_NORMAL_MKOP is empty — "
                "the H0STMKO0 CB latch will fail-closed (permanent over-pause) on the first "
                "normal frame. Populate KNOWN_NORMAL_MKOP from a live KRX-hours capture (§9) "
                "before relying on this feed."
            )
        self._sh_trht_halted: Dict[str, bool] = {}
        self._sh_cb_latched: bool = False           # market-wide CB (retained on reconnect)
        self._sh_vi_latched: Dict[str, bool] = {}
        self._sh_market_status_confirmed: set = set()   # trading_pairs whose own H0STMKO0 sub is ack-confirmed (PER-PAIR — not a global flag)

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
        self._account_truth_lock = threading.Lock()
        self._account_truth: Optional[Dict[str, Any]] = None
        for _warning in self._routing_warnings:
            self.logger().warning(_warning)
        # Balance updates are still REST-polled; order/fill events come via WS
        self.real_time_balance_update = False
        # JEP-231 holiday cache — disk-persisted, stdlib only (sync read by SessionCalendar)
        self._holiday_cache = KisHolidayCache(cache_path=self._holiday_cache_path())
        self._holiday_refreshed_day: Optional[str] = None

    def _holiday_cache_path(self) -> str:
        """JEP-231: 토큰 캐시와 같은 디렉토리에 휴장일 캐시를 배치.
        KisAuth는 매번 새 인스턴스를 만들지만 _token_cache_path는 파라미터로 결정적(deterministic).
        """
        import os
        token_path = KisAuth(
            app_key=self._app_key,
            app_secret=self._app_secret,
            sandbox=self._sandbox,
        )._token_cache_path
        return os.path.join(os.path.dirname(token_path), "kis_holiday_cache.json")

    def is_trading_day(self, d) -> Optional[bool]:
        """date -> True(거래일)/False(휴장)/None(미상). 캐시(디스크 hydrate)에서 sync 읽기.
        SessionCalendar.trading_day_fn 주입 용도.
        """
        return self._holiday_cache.is_trading_day(d)

    async def refresh_holiday_cache(self, base_yyyymmdd: str) -> bool:
        """JEP-231: 오늘~연말 범위 휴장일을 1회 조회해 캐시 갱신.
        실패 시 기존 캐시 보존(상위 fail-closed). 성공=True 반환.
        """
        try:
            result = await self._api_get(
                path_url=CONSTANTS.DOMESTIC_STOCK_HOLIDAY_PATH,
                params={"BASS_DT": base_yyyymmdd, "CTX_AREA_NK": "", "CTX_AREA_FK": ""},
                is_auth_required=True,
                headers={"tr_id": CONSTANTS.DOMESTIC_STOCK_HOLIDAY_TR_ID},
            )
            if result.get("rt_cd") != "0":
                self.logger().warning(
                    "JEP-231 holiday fetch rt_cd=%s msg=%s — keeping cache",
                    result.get("rt_cd"), result.get("msg1"),
                )
                return False
            rows = result.get("output") or []
            self._holiday_cache.update(rows)
            # JEP-231 fix (MED): only report success if TODAY (base_yyyymmdd) is actually covered.
            # An rt_cd=0 with empty/partial output would otherwise let the daily guard mark the day
            # refreshed, leaving today unknown (fail-closed) until the next KST date — i.e. a cold
            # boot at open could stop quoting all day. Returning False keeps the retry open.
            try:
                base_date = datetime.strptime(base_yyyymmdd, "%Y%m%d").date()
            except ValueError:
                return False
            if self._holiday_cache.is_trading_day(base_date) is None:
                self.logger().warning(
                    "JEP-231 holiday fetch covered no row for %s — keeping retry open", base_yyyymmdd
                )
                return False
            return True
        except Exception as e:
            self.logger().warning("JEP-231 holiday fetch failed (%s) — keeping cache", e)
            return False

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

    async def check_network(self) -> NetworkStatus:
        # JEP-203: debounce transient transport blips on the balance-inquiry probe. The base
        # ExchangePyBase.check_network swallows the exception with NO logging and reports
        # NOT_CONNECTED on the FIRST failure; because the framework ties the WS lifecycle to
        # NetworkStatus, a single REST timeout flips NOT_CONNECTED -> stop_network() and tears
        # down the healthy KIS WS hub (observed as recurring ~10s hedge-feed gaps -> false
        # WS-staleness). Tolerate up to (threshold - 1) CONSECUTIVE probe failures as transient;
        # a genuine outage still trips after `threshold` consecutive failures. The JEP-134
        # WS-staleness gate (3s) remains the fast hedge-readiness safety net independent of this.
        try:
            await self._make_network_check_request()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # JEP-206: an expired access token (KIS msg_cd EGW00123) is a SERVER-authoritative
            # "your token is dead now" signal the local TTL clock cannot see — _get_access_token
            # refreshes by expiry only, so the cached (server-dead) token is re-sent on every probe,
            # check_network reports NOT_CONNECTED, the strategy halts, and no order/auth call ever
            # fires the refresh path -> deadlock until a manual restart. Break it here: on EGW00123,
            # force-invalidate the cached token and retry the probe ONCE before counting the failure.
            #
            # Classification is on the STRUCTURED msg_cd first (KisNetworkCheckError carries it from
            # the HTTP-200 rt_cd!=0 raise below) and falls back to a qualified substring on the
            # HTTP-500 shape, where the shared REST layer buries the code in the IOError text. The
            # qualified `"EGW00123"` form (the raw code) is matched only when no structured msg_cd
            # is present. EGW00215 (per-second throttle) is a transient NON-auth failure and is
            # explicitly excluded so it stays on the debounce below (refreshing on throttle would
            # hammer the 1/min tokenP limit). A >=60s cooldown bounds re-auth against a genuinely
            # revoked appkey to KIS's issuance ceiling.
            msg_cd = getattr(e, "msg_cd", "") or ""
            err_text = repr(e)
            is_throttle = (
                msg_cd == CONSTANTS.KIS_ERR_THROTTLE
                or CONSTANTS.KIS_ERR_THROTTLE in err_text
            )
            is_expired = not is_throttle and (
                msg_cd == CONSTANTS.KIS_ERR_TOKEN_EXPIRED
                or (not msg_cd and CONSTANTS.KIS_ERR_TOKEN_EXPIRED in err_text)
            )
            now = self.current_timestamp if self.current_timestamp > 0 else time.time()
            if is_expired and (now - self._last_forced_reauth_ts) >= self._FORCED_REAUTH_COOLDOWN_S:
                self.logger().warning(
                    f"KIS check_network: expired-token signal ({CONSTANTS.KIS_ERR_TOKEN_EXPIRED}) "
                    f"on the balance probe — forcing a token re-auth and retrying once: {e!r}"
                )
                self._last_forced_reauth_ts = now
                try:
                    await self._auth.invalidate_token()
                    await self._make_network_check_request()
                except asyncio.CancelledError:
                    raise
                except Exception as retry_exc:
                    e = retry_exc  # refresh+retry also failed -> fall through to the debounce
                else:
                    self._net_check_consec_failures = 0
                    return NetworkStatus.CONNECTED
            elif is_expired:
                self.logger().warning(
                    f"KIS check_network: expired-token signal ({CONSTANTS.KIS_ERR_TOKEN_EXPIRED}) "
                    f"but within the {self._FORCED_REAUTH_COOLDOWN_S:.0f}s re-auth cooldown — "
                    f"skipping re-auth, applying debounce: {e!r}"
                )
            self._net_check_consec_failures += 1
            if self._net_check_consec_failures < self._NET_CHECK_FAILURE_THRESHOLD:
                # jep266: EGW00215 (per-second throttle) is expected under multi-symbol load, is
                # debounced here, and keeps the connector CONNECTED -> a per-occurrence WARNING is
                # cosmetic noise that spammed the operator alarm. Log throttle transients at DEBUG;
                # keep genuine (non-throttle) transients at WARNING. Sustained failures (below)
                # always report NOT_CONNECTED regardless of cause.
                log_transient = self.logger().debug if is_throttle else self.logger().warning
                log_transient(
                    f"KIS check_network transient failure "
                    f"{self._net_check_consec_failures}/{self._NET_CHECK_FAILURE_THRESHOLD} "
                    f"— keeping CONNECTED (WS hub not torn down): {e!r}"
                )
                return NetworkStatus.CONNECTED
            self.logger().warning(
                f"KIS check_network sustained failure "
                f"({self._net_check_consec_failures}x consecutive) — reporting NOT_CONNECTED: {e!r}"
            )
            return NetworkStatus.NOT_CONNECTED
        self._net_check_consec_failures = 0
        return NetworkStatus.CONNECTED

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
        # Raise so check_network() reports NOT_CONNECTED instead (JEP-182). Carry msg_cd as a
        # STRUCTURED attribute (KisNetworkCheckError) so check_network classifies EGW00123 vs
        # EGW00215 on the field, not free text — the HTTP-200 variant has no code in msg1 (JEP-206).
        if result.get("rt_cd") != "0":
            raise KisNetworkCheckError(
                f"KIS network check failed: "
                f"rt_cd={result.get('rt_cd')} "
                f"msg_cd={result.get('msg_cd')} msg={result.get('msg1')}",
                msg_cd=str(result.get("msg_cd") or ""),
            )
        # JEP-231: 일1회 휴장일 캐시 갱신 (성공 경로에서만 — 실패 시 다음 tick 재시도)
        today = self._kst_today()
        if self._holiday_refreshed_day != today:
            if await self.refresh_holiday_cache(today):
                self._holiday_refreshed_day = today   # 성공 후에만 guard set

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
            market_status_enabled=self._market_status_enabled,
            market_status_capture_only=self._market_status_capture_only,
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
            hts_id=self._hts_id,
        )

    def note_hour_cls_code(self, trading_pair: str, code: Optional[str]) -> None:
        if code is not None:
            self._sh_hour_cls_code[trading_pair] = code

    def note_book_snapshot(self, trading_pair: str, bids: list, asks: list) -> None:
        # JEP-198/202: detect a genuinely frozen book by comparing the FULL depth
        # snapshot (every level's price + size), not just the top of book. A quiet
        # but live market routinely holds the best bid/ask static for many seconds
        # (the touch can persist for tens of minutes) while deeper levels and sizes
        # keep moving; keying "book static" off the top alone false-trips the
        # session-halt gate's book_frozen rule on a perfectly healthy feed. A real
        # halt (CB / 거래정지) freezes every level, so an unchanged full-depth
        # signature is the correct freeze signal.
        sig = (tuple(bids), tuple(asks))
        if self._sh_prev_book_sig.get(trading_pair) != sig:
            self._sh_prev_book_sig[trading_pair] = sig
            self._sh_last_book_change[trading_pair] = time.perf_counter()

    def mark_market_status_confirmed(self, trading_pair: str) -> None:
        self._sh_market_status_confirmed.add(trading_pair)

    def discard_market_status_confirmed(self, trading_pair: str) -> None:
        self._sh_market_status_confirmed.discard(trading_pair)

    def note_market_status(
        self,
        trading_pair: str,
        *,
        trht_yn: Optional[str],
        mkop_cls_code: Optional[str],
        vi_cls_code: Optional[str],
        ovtm_vi_cls_code: Optional[str],
    ) -> None:
        if trht_yn is not None:
            self._sh_trht_halted[trading_pair] = trht_yn == CONSTANTS.TRHT_HALTED
        if vi_cls_code is not None or ovtm_vi_cls_code is not None:
            self._sh_vi_latched[trading_pair] = (
                vi_cls_code in CONSTANTS.VI_ACTIVE_VALUES
                or ovtm_vi_cls_code in CONSTANTS.VI_ACTIVE_VALUES
            )
        if mkop_cls_code is None:
            return
        if (
            mkop_cls_code in CONSTANTS.MKOP_CLS_SET_CB
            or mkop_cls_code in CONSTANTS.MKOP_CLS_SET_TEMP_STOP
        ):
            self._sh_cb_latched = True
        elif mkop_cls_code in CONSTANTS.MKOP_CLS_CLEAR_CB:
            self._sh_cb_latched = False
        elif mkop_cls_code not in CONSTANTS.KNOWN_NORMAL_MKOP:
            self.logger().warning(
                f"JEP-198 unknown MKOP_CLS_CODE={mkop_cls_code!r}; fail-closed (latch retained)."
            )
            self._sh_cb_latched = True

    def get_session_halt_signals(self, trading_pair: str) -> kis_utils.KisHaltSignals:
        now = time.perf_counter()
        ds = self.order_book_tracker.data_source if self.order_book_tracker else None
        last_frame = ds.last_ws_orderbook_time(trading_pair) if ds else None
        book_age = (now - last_frame) if last_frame is not None else None
        last_change = self._sh_last_book_change.get(trading_pair)
        book_static = (now - last_change) if last_change is not None else None
        # Phase 1 (status feed off) -> ready True; Phase 2 -> THIS pair's own H0STMKO0 ack confirmed
        status_ready = True if not self._market_status_enabled else (trading_pair in self._sh_market_status_confirmed)
        return kis_utils.KisHaltSignals(
            hour_cls_auction=(self._sh_hour_cls_code.get(trading_pair) == CONSTANTS.HOUR_CLS_AUCTION),
            book_age_sec=book_age,
            book_static_sec=book_static,
            trht_halted=bool(self._sh_trht_halted.get(trading_pair, False)),
            cb_latched=self._sh_cb_latched,
            vi_latched=bool(self._sh_vi_latched.get(trading_pair, False)),
            market_status_ready=status_ready,
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

        # ORD_DVSN order-type code. A LIMIT carries an explicit price (지정가, "00").
        # A non-LIMIT (MARKET) order maps to 최유리지정가 ("03", best-price marketable),
        # NOT 시장가 ("01"): true MARKET is REJECTED on the NXT after-market (15:40-20:00 KST)
        # while 03 fills at the best opposite quote on BOTH KRX continuous and NXT (the unwind
        # tooling hardcodes 03 for the same reason). 03 ignores the price, so ORD_UNPR="0" —
        # this also fixes a latent crash: price is NaN for a market order and str(int(NaN)) raises.
        if order_type == OrderType.LIMIT:
            ord_dvsn = "00"
            ord_unpr = str(int(price))
        else:
            ord_dvsn = "03"
            ord_unpr = "0"

        body = {
            "CANO": self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO": symbol,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(int(amount)),
            "ORD_UNPR": ord_unpr,
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

        self._capture_account_truth(result)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _capture_account_truth(self, result: Dict[str, Any]) -> None:
        positions = {}
        for holding in result.get("output1", []):
            qty = Decimal(str(holding.get("hldg_qty", "0")))
            if qty == 0:
                continue
            positions[holding["pdno"]] = {
                "qty": qty,
                "avg_entry": Decimal(str(holding.get("pchs_avg_pric", "0"))),
                "mark": Decimal(str(holding.get("prpr", "0"))),
                "ccy": "KRW",
            }
        cash_by_ccy = {
            "KRW": Decimal(str((result.get("output2") or [{}])[0].get("dnca_tot_amt", "0")))
        }
        truth = {
            "account_id": self._cano,
            "venue": "kis",
            "snapshot_ts": time.time(),
            "source": "inquire-balance",
            "positions": positions,
            "cash_by_ccy": cash_by_ccy,
        }
        with self._account_truth_lock:
            self._account_truth = truth

    def get_account_truth(self) -> Optional[Dict[str, Any]]:
        with self._account_truth_lock:
            return copy.deepcopy(self._account_truth) if self._account_truth is not None else None

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
                # rmn_qty (잔여수량) is the inquire-daily-ccld remaining field — verified against the
                # Intrect-io/kis-agent community client (responses/order.py:113 + test fixture) and KIS
                # docs (TR TTTC8001R). ord_psbl_qty is NOT a field of this endpoint (it belongs to the
                # balance/orderable-qty endpoints), so a row lacking rmn_qty falls back to
                # ord_qty - tot_ccld_qty — never an unrelated orderable-qty field. (JEP-135)
                remaining = row.get("rmn_qty")
                remaining_amount = Decimal(str(remaining)) if remaining not in (None, "") else (ord_qty - ccld_qty)
                # A REJECTED row is terminal yet escapes the guards above: it is not a cancel
                # (cncl_yn != "Y") and nothing filled (ccld < ord), but rmn_qty == 0 / rjct_qty > 0
                # means it can never fill. Drop it — reporting a never-fill order as resting makes
                # the cross-venue adopt-seed (_has_resting_orders) fail-close forever (2026-06-29:
                # the 09:00 auction rejected the SMSN hedge -> zombie -> no MM). rmn_qty is the
                # primary signal; rjct_qty is the fallback when rmn_qty is absent (ord_qty - ccld
                # would otherwise be > 0 for a rejected row with no rmn_qty).
                rjct_qty = Decimal(str(row.get("rjct_qty", "0") or "0"))
                if remaining_amount <= 0:
                    continue
                if ord_qty > 0 and (ccld_qty + rjct_qty) >= ord_qty:
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
                records.append(OpenOrderRecord(
                    trading_pair=tp,
                    exchange_order_id=eoid,
                    client_order_id=tracked.client_order_id if tracked else None,
                    trade_type=trade_type,
                    price=Decimal(str(row.get("ord_unpr", "0"))),
                    amount=ord_qty,
                    remaining_amount=remaining_amount,
                ))
            # Pagination keys (ctx_area_nk100/fk100, tr_cont) verified against the Intrect-io/kis-agent
            # community client (account/profit_api.py) + KIS docs. NOTE (live-only, not a field-name
            # question): confirm resting-order visibility in a live after-15:30 NXT response. (JEP-135)
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
