"""JEP-187 SessionCalendar seam: KRX impl reproduces the executor EOD/clock; 24/7 is neutral."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_calendar import (
    KrxSessionCalendar,
    TwentyFourSevenCalendar,
)

KST = timezone(timedelta(hours=9))


def _ts(hour, minute):
    return datetime(2026, 6, 18, hour, minute, 0, tzinfo=KST).timestamp()


def test_krx_now_is_kst():
    cal = KrxSessionCalendar()
    now = cal.now(_ts(15, 20))
    assert now.utcoffset() == timedelta(hours=9)
    assert (now.hour, now.minute) == (15, 20)


@pytest.mark.parametrize(
    "hour,minute,wind,expected",
    [
        (15, 20, 20, Decimal("0.5")),   # halfway inside the 20-min wind window
        (15, 20, 0, Decimal("0")),      # disabled
        (15, 0, 20, Decimal("0")),      # before the window
        (16, 0, 20, Decimal("1")),      # saturated after close
    ],
)
def test_krx_eod_pressure_matches_policy(hour, minute, wind, expected):
    assert KrxSessionCalendar().eod_pressure(_ts(hour, minute), wind) == expected


def test_krx_eod_pressure_wind_nonpositive_is_zero():
    assert KrxSessionCalendar().eod_pressure(_ts(15, 20), 0) == Decimal("0")
    assert KrxSessionCalendar().eod_pressure(_ts(15, 20), -5) == Decimal("0")


@pytest.mark.parametrize("hour,minute,expected", [
    (8, 29, False), (8, 30, True), (8, 59, True), (9, 0, False),      # opening call auction [08:30,09:00)
    (15, 19, False), (15, 20, True), (15, 29, True), (15, 30, False),  # closing call auction [15:20,15:30)
    (15, 50, False),                                                 # 시간외종가 -> fillable, NOT a defer window
    (15, 59, False), (16, 0, True), (17, 59, True), (18, 0, False),  # 시간외단일가 [16:00,18:00)
    (12, 0, False),  # mid-session continuous
])
def test_krx_in_auction_window(hour, minute, expected):
    assert KrxSessionCalendar().in_auction_window(_ts(hour, minute)) is expected


def test_twentyfourseven_now_is_utc():
    now = TwentyFourSevenCalendar().now(_ts(15, 20))
    assert now.utcoffset() == timedelta(0)


def test_twentyfourseven_eod_pressure_always_zero():
    cal = TwentyFourSevenCalendar()
    assert cal.eod_pressure(_ts(15, 20), 20) == Decimal("0")
    assert cal.eod_pressure(_ts(16, 0), 5) == Decimal("0")


def test_twentyfourseven_in_auction_window_always_false():
    assert TwentyFourSevenCalendar().in_auction_window(_ts(8, 30)) is False


# --- JEP-284: non-finite current_timestamp guard ---
# A strategy stop (drawdown guard / manual kill / cash-out) freezes the Hummingbot clock so
# self._strategy.current_timestamp becomes NaN. Orphaned executor control_loops keep reading it,
# so a non-finite timestamp reaching datetime.fromtimestamp() must NOT raise (it crash-looped live).

_NONFINITE = (float("nan"), float("inf"), float("-inf"))


def test_krx_now_finite_equals_fromtimestamp():
    # Behavior preservation: finite input is byte-for-byte the old fromtimestamp path.
    ts = _ts(15, 20)
    assert KrxSessionCalendar().now(ts) == datetime.fromtimestamp(ts, tz=KST)


@pytest.mark.parametrize("bad", _NONFINITE)
def test_krx_now_nonfinite_falls_back_to_kst_wallclock(bad):
    now = KrxSessionCalendar().now(bad)  # must NOT raise
    assert now.utcoffset() == timedelta(hours=9)  # tz-aware KST wall clock


@pytest.mark.parametrize("bad", _NONFINITE)
def test_krx_predicates_nonfinite_never_raise(bad):
    cal = KrxSessionCalendar(trading_day_fn=lambda _d: True)
    # None of these may raise on a non-finite clock.
    cal.in_auction_window(bad)
    cal.in_session(bad)
    cal.eod_pressure(bad, 20)


@pytest.mark.parametrize("bad", _NONFINITE)
def test_twentyfourseven_now_nonfinite_falls_back_to_utc_wallclock(bad):
    now = TwentyFourSevenCalendar().now(bad)  # must NOT raise
    assert now.utcoffset() == timedelta(0)  # tz-aware UTC wall clock
