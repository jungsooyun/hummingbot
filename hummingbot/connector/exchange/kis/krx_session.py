"""Pure KRX trading-session time-window helper (stdlib-only).

This is the single source of the KRX session BOUNDARY CONSTANTS used by the KIS
order-book data source to suppress out-of-session REST snapshot polling
(JEP-217). It is deliberately a TIME WINDOW ONLY — no holiday awareness — so it
fails OPEN: on a real trading day with the holiday cache unknown, the data
source still fetches and the existing empty-book guard
(``_order_book_snapshot``) safely refuses to publish. Trading-permission safety
(holiday-aware, fail-CLOSED) lives upstream in
``strategy_v2/.../session_calendar.py`` (``KrxSessionCalendar``) +
``TradingHoursGate``; that module intentionally duplicates these two boundary
constants rather than importing across the strategy->connector layer (JEP-217
design 3.4). Keep the two definitions in sync.
"""
from datetime import datetime, timedelta, timezone

# Korea Standard Time (no DST -- Korea does not observe summer time).
KST = timezone(timedelta(hours=9))

# KRX continuous-trading + NXT after-market envelope, minutes-of-day in KST.
# MUST stay equal to KrxSessionCalendar._SESSION_OPEN_MIN / _SESSION_CLOSE_MIN.
SESSION_OPEN_MIN = 8 * 60     # 08:00 KST (inclusive)
SESSION_CLOSE_MIN = 20 * 60   # 20:00 KST (exclusive)


def in_session_window(now_ts: float) -> bool:
    """True if ``now_ts`` (epoch seconds) is within [08:00, 20:00) KST.

    Time window only -- no holiday/weekend check (fail-open by design).
    """
    now = datetime.fromtimestamp(now_ts, tz=KST)
    minute_of_day = now.hour * 60 + now.minute
    return SESSION_OPEN_MIN <= minute_of_day < SESSION_CLOSE_MIN
