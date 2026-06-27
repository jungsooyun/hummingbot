"""Trading-session calendar seam (JEP-187).

Isolates the venue wall clock + end-of-day pressure from the ladder executor so a
venue-pair generalization (e.g. a 24/7 CEX hedge) supplies its own calendar without
touching the executor. KIS impl = ``KrxSessionCalendar`` (KST, 15:30 close); neutral
default = ``TwentyFourSevenCalendar`` (UTC, no EOD ramp). The pure ramp math stays in
``ladder_policy.compute_eod_pressure``.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Protocol

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.fair_math import compute_eod_pressure

ZERO = Decimal("0")


class SessionCalendar(Protocol):
    def now(self, current_timestamp: float) -> datetime:
        """Timezone-aware wall clock for gate/EOD logic (KIS=KST, 24/7=UTC)."""
        ...

    def eod_pressure(self, current_timestamp: float, wind_minutes: int) -> Decimal:
        """0..1 linear ramp toward session close; ZERO when wind_minutes<=0 or always-open."""
        ...

    def in_auction_window(self, current_timestamp: float) -> bool:
        """True during a scheduled single-price call auction (no continuous matching)."""
        ...

    def in_session(self, current_timestamp: float) -> bool:
        """True면 연속체결 거래시간(WS 프레임 기대 가능). 게이트/스태일니스 단일 기준."""
        ...


class TwentyFourSevenCalendar:
    """Neutral default: an always-open venue (e.g. a 24/7 CEX). No EOD pressure."""

    def now(self, current_timestamp: float) -> datetime:
        return datetime.fromtimestamp(current_timestamp, tz=timezone.utc)

    def eod_pressure(self, current_timestamp: float, wind_minutes: int) -> Decimal:
        return ZERO

    def in_auction_window(self, current_timestamp: float) -> bool:
        return False

    def in_session(self, current_timestamp: float) -> bool:
        return True


class KrxSessionCalendar:
    """KIS impl #1: KRX regular session, KST wall clock, 15:30 close."""

    _KST = timezone(timedelta(hours=9))
    _KRX_CLOSE_MIN = 15 * 60 + 30  # 15:30 KST
    _SESSION_OPEN_MIN = 8 * 60          # 08:00 KST
    _SESSION_CLOSE_MIN = 20 * 60        # 20:00 KST (NXT 애프터마켓 종료)
    _KRX_OPEN_AUCTION = (8 * 60 + 30, 9 * 60)          # [08:30, 09:00) KST opening call auction
    _KRX_CLOSE_AUCTION = (15 * 60 + 20, 15 * 60 + 30)  # [15:20, 15:30) KST closing call auction
    # JEP-226: 시간외단일가 [16:00, 18:00) KST — 10-min single-price batch auctions (no continuous
    # matching). A marketable hedge here rests/fills at the batch match, not at a continuous quote;
    # surfaced as in_auction_window -> the hedge taker defers-then-forces (force-eligible). 시간외종가
    # [15:40,16:00) is continuous-at-close-price (fillable) and NXT (15:30-20:00) is continuous, so
    # neither is a defer window.
    _KRX_AFTERHOURS_SINGLE = (16 * 60, 18 * 60)        # [16:00, 18:00) KST 시간외단일가

    def __init__(self, trading_day_fn=None):
        # trading_day_fn(date) -> Optional[bool]: True=거래일, False=휴장, None=미상(fail-closed).
        # 기본 None-반환 -> fail-closed (거래일 미상 시 비-세션).
        self._trading_day_fn = trading_day_fn if trading_day_fn is not None else (lambda _d: None)

    def now(self, current_timestamp: float) -> datetime:
        return datetime.fromtimestamp(current_timestamp, tz=self._KST)

    def eod_pressure(self, current_timestamp: float, wind_minutes: int) -> Decimal:
        if not wind_minutes or wind_minutes <= 0:
            return ZERO
        now = self.now(current_timestamp)
        now_kst_min = now.hour * 60 + now.minute
        return compute_eod_pressure(now_kst_min, self._KRX_CLOSE_MIN, wind_minutes)

    def in_auction_window(self, current_timestamp: float) -> bool:
        now = self.now(current_timestamp)
        m = now.hour * 60 + now.minute
        return (self._KRX_OPEN_AUCTION[0] <= m < self._KRX_OPEN_AUCTION[1]
                or self._KRX_CLOSE_AUCTION[0] <= m < self._KRX_CLOSE_AUCTION[1]
                or self._KRX_AFTERHOURS_SINGLE[0] <= m < self._KRX_AFTERHOURS_SINGLE[1])

    def in_session(self, current_timestamp: float) -> bool:
        now = self.now(current_timestamp)
        is_td = self._trading_day_fn(now.date())
        if is_td is not True:                 # False(휴장) 또는 None(미상) -> fail-closed
            return False
        m = now.hour * 60 + now.minute
        return self._SESSION_OPEN_MIN <= m < self._SESSION_CLOSE_MIN
