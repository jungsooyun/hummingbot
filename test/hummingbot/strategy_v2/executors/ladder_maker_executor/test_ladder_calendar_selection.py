"""JEP-231 fix (BLOCKER): LadderMakerExecutor selects KrxSessionCalendar only when the
hedge connector exposes is_trading_day (KIS/session-aware). Sessionless/non-KIS connectors
fall back to TwentyFourSevenCalendar (behavior-neutral) instead of fail-closing every
timestamp (which would silently stop quoting 24/7 pairs when trading_hours_gate_enabled
defaults True)."""
import unittest
from types import SimpleNamespace

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_calendar import (
    KrxSessionCalendar,
    TwentyFourSevenCalendar,
)

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    _IMPORTABLE = True
except Exception:  # pragma: no cover
    LadderMakerExecutor = None
    _IMPORTABLE = False


@unittest.skipUnless(_IMPORTABLE, "requires V2 stack (paho) — run in Docker/CI")
class LadderCalendarSelectionTest(unittest.TestCase):
    def test_kis_connector_uses_krx_calendar(self):
        conn = SimpleNamespace(is_trading_day=lambda d: True)
        cal = LadderMakerExecutor._select_session_calendar(conn)
        self.assertIsInstance(cal, KrxSessionCalendar)

    def test_non_kis_connector_uses_24_7(self):
        conn = SimpleNamespace()  # no is_trading_day → sessionless
        cal = LadderMakerExecutor._select_session_calendar(conn)
        self.assertIsInstance(cal, TwentyFourSevenCalendar)
        self.assertTrue(cal.in_session(1_750_000_000.0))  # behavior-neutral: always in session

    def test_missing_connector_uses_24_7(self):
        cal = LadderMakerExecutor._select_session_calendar(None)
        self.assertIsInstance(cal, TwentyFourSevenCalendar)
        self.assertTrue(cal.in_session(1_750_000_000.0))


if __name__ == "__main__":
    unittest.main()
