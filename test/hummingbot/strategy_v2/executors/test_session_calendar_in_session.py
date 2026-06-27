from datetime import datetime, timezone, timedelta
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_calendar import (
    KrxSessionCalendar, TwentyFourSevenCalendar,
)

KST = timezone(timedelta(hours=9))


def _ts(y, mo, d, h, mi, s=0):
    return datetime(y, mo, d, h, mi, s, tzinfo=KST).timestamp()


def test_in_session_open_close_boundaries_trading_day():
    cal = KrxSessionCalendar(trading_day_fn=lambda day: True)
    # 2026-06-26 = Fri (평일)
    assert cal.in_session(_ts(2026, 6, 26, 7, 59)) is False   # pre-open
    assert cal.in_session(_ts(2026, 6, 26, 8, 0)) is True     # open boundary (inclusive)
    assert cal.in_session(_ts(2026, 6, 26, 19, 59)) is True
    assert cal.in_session(_ts(2026, 6, 26, 20, 0)) is False   # close boundary (half-open)
    assert cal.in_session(_ts(2026, 6, 26, 20, 1)) is False


def test_in_session_non_trading_day_is_false_even_in_hours():
    cal = KrxSessionCalendar(trading_day_fn=lambda day: False)  # holiday/weekend
    assert cal.in_session(_ts(2026, 6, 26, 11, 0)) is False


def test_in_session_unknown_trading_day_fail_closed_false():
    cal = KrxSessionCalendar(trading_day_fn=lambda day: None)   # provider unknown
    assert cal.in_session(_ts(2026, 6, 26, 11, 0)) is False


def test_in_session_default_fn_treats_as_unknown_fail_closed():
    cal = KrxSessionCalendar()  # no fn injected
    assert cal.in_session(_ts(2026, 6, 26, 11, 0)) is False


def test_twentyfourseven_always_in_session():
    cal = TwentyFourSevenCalendar()
    assert cal.in_session(_ts(2026, 6, 26, 3, 0)) is True
    assert cal.in_session(_ts(2026, 6, 27, 23, 0)) is True  # weekend
