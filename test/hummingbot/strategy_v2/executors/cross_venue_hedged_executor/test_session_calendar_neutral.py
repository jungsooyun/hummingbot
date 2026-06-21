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


def test_twentyfourseven_now_is_utc():
    now = TwentyFourSevenCalendar().now(_ts(15, 20))
    assert now.utcoffset() == timedelta(0)


def test_twentyfourseven_eod_pressure_always_zero():
    cal = TwentyFourSevenCalendar()
    assert cal.eod_pressure(_ts(15, 20), 20) == Decimal("0")
    assert cal.eod_pressure(_ts(16, 0), 5) == Decimal("0")
