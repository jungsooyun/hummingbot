import unittest
from datetime import datetime, timezone

from hummingbot.connector.exchange.kis.krx_session import (
    KST,
    SESSION_CLOSE_MIN,
    SESSION_OPEN_MIN,
    in_session_window,
)


def _ts(hh: int, mm: int) -> float:
    """Epoch seconds for a given KST wall-clock time on a fixed date."""
    dt = datetime(2026, 6, 29, hh, mm, 0, tzinfo=KST)  # 2026-06-29 is a Monday
    return dt.timestamp()


class KrxSessionWindowTests(unittest.TestCase):
    def test_constants(self):
        self.assertEqual(SESSION_OPEN_MIN, 8 * 60)
        self.assertEqual(SESSION_CLOSE_MIN, 20 * 60)

    def test_before_open_is_closed(self):
        self.assertFalse(in_session_window(_ts(7, 59)))

    def test_open_boundary_is_inclusive(self):
        self.assertTrue(in_session_window(_ts(8, 0)))

    def test_midday_is_open(self):
        self.assertTrue(in_session_window(_ts(13, 30)))

    def test_last_minute_before_close_is_open(self):
        self.assertTrue(in_session_window(_ts(19, 59)))

    def test_close_boundary_is_exclusive(self):
        self.assertFalse(in_session_window(_ts(20, 0)))

    def test_after_close_is_closed(self):
        self.assertFalse(in_session_window(_ts(22, 0)))

    def test_midnight_is_closed(self):
        self.assertFalse(in_session_window(_ts(0, 0)))

    def test_window_is_timezone_anchored_not_utc(self):
        # 23:30 UTC == 08:30 KST next day => inside the window.
        utc_2330 = datetime(2026, 6, 28, 23, 30, 0, tzinfo=timezone.utc).timestamp()
        self.assertTrue(in_session_window(utc_2330))


if __name__ == "__main__":
    unittest.main()
