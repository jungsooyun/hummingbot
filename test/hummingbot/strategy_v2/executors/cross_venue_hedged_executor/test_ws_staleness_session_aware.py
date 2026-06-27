"""JEP-231 Task 5: staleness latch suppression out-of-session.

Regression tests: KIS WS silence after NXT 20:00 close must NOT trip the kill-switch.
"""
import unittest
from types import SimpleNamespace
from decimal import Decimal

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    _IMPORTABLE = True
except Exception:  # pragma: no cover
    LadderMakerExecutor = None
    _IMPORTABLE = False


def _make_executor(ws_enabled=True, grace=90.0):
    """Minimal executor for staleness tests — uses LadderMakerExecutor (concrete subclass)."""
    ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
    ex.config = SimpleNamespace(
        ws_staleness_kill_switch_enabled=ws_enabled,
        max_hl_ws_age_s=12.0,
        max_kis_ws_age_s=3.0,
        ws_staleness_grace_s=grace,
    )
    ex.maker_connector = "hyperliquid_perpetual"
    ex.maker_trading_pair = "X-USD"
    ex.hedge_connector = "kis"
    ex.hedge_trading_pair = "005930-KRW"
    ex._init_ws_staleness_state()
    ex._strategy = SimpleNamespace(current_timestamp=1_000_000.0)
    return ex


def _stub_ws_age(ex, hedge_age, maker_age=0.0):
    """Stub _ws_freshness_sec to return fixed ages."""
    ex._ws_freshness_sec = lambda conn, pair: (
        hedge_age if conn == ex.hedge_connector else maker_age
    )


@unittest.skipUnless(_IMPORTABLE, "requires V2 stack — run in Docker/CI")
class WsStalenessSessionAwareTest(unittest.TestCase):

    def test_out_of_session_does_not_latch_after_grace(self):
        """JEP-231 regression: out-of-session WS silence (장 마감) must NOT latch."""
        ex = _make_executor(ws_enabled=True, grace=90.0)
        ex._session_in_session = False           # 마감(20:00 이후)
        _stub_ws_age(ex, hedge_age=999)          # KIS WS 침묵(정상 마감)
        t0 = 1_000_000.0
        ex._strategy.current_timestamp = t0
        ex._evaluate_ws_staleness()
        ex._strategy.current_timestamp = t0 + 95  # grace 초과
        ex._evaluate_ws_staleness()
        self.assertFalse(ex._staleness_kill_switch)   # latch 안 됨
        self.assertIsNone(ex._staleness_since_ts)

    def test_in_session_still_latches_after_grace(self):
        """대조군: in-session에서 WS 침묵 시 grace 후 latch 발생해야 함."""
        ex = _make_executor(ws_enabled=True, grace=90.0)
        ex._session_in_session = True
        _stub_ws_age(ex, hedge_age=999)
        t0 = 1_000_000.0
        ex._strategy.current_timestamp = t0
        ex._evaluate_ws_staleness()
        ex._strategy.current_timestamp = t0 + 95
        ex._evaluate_ws_staleness()
        self.assertTrue(ex._staleness_kill_switch)

    def test_out_of_session_clears_existing_staleness_timer(self):
        """out-of-session 진입 시 기존 적립된 grace 타이머를 클리어해야 함."""
        ex = _make_executor(ws_enabled=True, grace=90.0)
        ex._session_in_session = True
        _stub_ws_age(ex, hedge_age=999)
        t0 = 1_000_000.0
        ex._strategy.current_timestamp = t0
        ex._evaluate_ws_staleness()           # 타이머 시작
        self.assertIsNotNone(ex._staleness_since_ts)

        # 세션 종료 → 타이머 클리어
        ex._session_in_session = False
        ex._strategy.current_timestamp = t0 + 50  # grace(90s) 미달인데도 클리어
        ex._evaluate_ws_staleness()
        self.assertIsNone(ex._staleness_since_ts)
        self.assertFalse(ex._staleness_kill_switch)
