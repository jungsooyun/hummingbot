"""JEP-231 Task 3: _session_in_session cache in _evaluate_session_state.

Tests that:
- With trading_hours_gate_enabled=True: in-session → True, out-of-session → False, non-trading → False.
- With trading_hours_gate_enabled=False: always True (현행 동작 보존).
"""
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_halt_source import (
    NoHaltSource,
)

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover
    LadderMakerExecutor = None
    _EXECUTOR_IMPORTABLE = False


def _make_executor(in_session_val: bool, gate_enabled: bool = True, in_auction_val: bool = False):
    """Minimal executor fixture for _evaluate_session_state tests."""
    ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
    ex.config = SimpleNamespace(
        kill_switch=False,
        ws_staleness_kill_switch_enabled=False,
        session_halt_gate_enabled=False,
        session_halt_cooldown_s=0.0,
        session_halt_max_ws_age_s=3.0,
        session_halt_max_book_static_s=15.0,
        trading_hours_gate_enabled=gate_enabled,
        target_inventory=Decimal("0"),
        inventory_skew_bps_per_unit=Decimal("0"),
    )
    ex.maker_connector = "hyperliquid_perpetual"
    ex.maker_trading_pair = "X-USD"
    ex.hedge_connector = "kis"
    ex.hedge_trading_pair = "005930-KRW"
    ex._hedge_kill_switch = False
    ex._halt_source = NoHaltSource()
    ex._halt_cooldown_until = 0.0
    # Calendar stub: in_auction_window returns in_auction_val, in_session returns in_session_val
    ex._calendar = SimpleNamespace(
        in_auction_window=lambda ts: in_auction_val,
        in_session=lambda ts: in_session_val,
        now=lambda ts: None,
    )
    ex._strategy = SimpleNamespace(current_timestamp=1_750_000_000.0)
    ex._init_ws_staleness_state()
    return ex


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "requires V2 stack (paho) — run in Docker/CI")
class LadderSessionInSessionTest(unittest.TestCase):
    def test_in_session_trading_day_sets_true(self):
        ex = _make_executor(in_session_val=True, gate_enabled=True)
        ex._evaluate_session_state()
        self.assertTrue(ex._session_in_session)

    def test_out_of_session_sets_false(self):
        ex = _make_executor(in_session_val=False, gate_enabled=True)
        ex._evaluate_session_state()
        self.assertFalse(ex._session_in_session)

    def test_gate_disabled_always_true_regardless_of_calendar(self):
        """trading_hours_gate_enabled=False → _session_in_session always True (현행 동작)."""
        ex = _make_executor(in_session_val=False, gate_enabled=False)
        ex._evaluate_session_state()
        self.assertTrue(ex._session_in_session)

    # ---- per-leg auction-aware hedge staleness wiring (JEP-231 follow-up) ----

    def test_in_auction_masks_hedge_expect_continuous(self):
        """During a hedge-venue single-price call auction, _evaluate_session_state sets
        _hedge_expect_continuous False so the base staleness latch masks the hedge leg.
        Window-boundary coverage (open/close/시간외단일가) lives in the session_calendar suite."""
        ex = _make_executor(in_session_val=True, gate_enabled=True, in_auction_val=True)
        ex._evaluate_session_state()
        self.assertFalse(ex._hedge_expect_continuous)

    def test_continuous_session_arms_hedge_expect_continuous(self):
        ex = _make_executor(in_session_val=True, gate_enabled=True, in_auction_val=False)
        ex._evaluate_session_state()
        self.assertTrue(ex._hedge_expect_continuous)

    def test_gate_disabled_hedge_expect_continuous_true(self):
        """gate off → _hedge_expect_continuous always True (현행 동작 보존, no auction masking)."""
        ex = _make_executor(in_session_val=True, gate_enabled=False, in_auction_val=True)
        ex._evaluate_session_state()
        self.assertTrue(ex._hedge_expect_continuous)
