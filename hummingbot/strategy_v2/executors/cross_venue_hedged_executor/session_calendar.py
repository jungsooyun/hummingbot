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


class TwentyFourSevenCalendar:
    """Neutral default: an always-open venue (e.g. a 24/7 CEX). No EOD pressure."""

    def now(self, current_timestamp: float) -> datetime:
        return datetime.fromtimestamp(current_timestamp, tz=timezone.utc)

    def eod_pressure(self, current_timestamp: float, wind_minutes: int) -> Decimal:
        return ZERO

    def in_auction_window(self, current_timestamp: float) -> bool:
        return False


class KrxSessionCalendar:
    """KIS impl #1: KRX regular session, KST wall clock, 15:30 close."""

    _KST = timezone(timedelta(hours=9))
    _KRX_CLOSE_MIN = 15 * 60 + 30  # 15:30 KST
    _KRX_OPEN_AUCTION = (8 * 60 + 30, 9 * 60)          # [08:30, 09:00) KST opening call auction
    _KRX_CLOSE_AUCTION = (15 * 60 + 20, 15 * 60 + 30)  # [15:20, 15:30) KST closing call auction

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
                or self._KRX_CLOSE_AUCTION[0] <= m < self._KRX_CLOSE_AUCTION[1])
