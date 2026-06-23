import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_halt_source import (
    NoHaltSource,
    SessionHaltState,
)
from hummingbot.strategy_v2.gates.gate_chain import GateChain, KillSwitchGate, SessionHaltGate, WsStalenessGate

try:
    # The executor module pulls the V2 strategy base (paho), absent in the local
    # py312 env -> these tests run in Docker/CI where the full stack is available.
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    _EXECUTOR_IMPORTABLE = False


def _kst(hour: int):
    from datetime import datetime, timezone, timedelta

    return datetime(2026, 6, 22, hour, 0, tzinfo=timezone(timedelta(hours=9)))


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack (paho) - run in Docker/CI")
class LadderMakerWsStalenessWiringTest(unittest.TestCase):
    def _make(self, *, enabled=True, kis_age=None, hl_age=0.0, latched=False):
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex.config = SimpleNamespace(
            kill_switch=False,
            ws_staleness_kill_switch_enabled=enabled,
            max_kis_ws_age_s=3.0,
            max_hl_ws_age_s=12.0,
            ws_staleness_grace_s=90.0,
            session_halt_gate_enabled=False,
            target_inventory=Decimal("0"),
            inventory_skew_bps_per_unit=Decimal("0"),
        )
        ex.maker_connector = "hyperliquid_perpetual"
        ex.maker_trading_pair = "X-USD"
        ex.hedge_connector = "kis"
        ex.hedge_trading_pair = "005930-KRW"
        ex._hedge_kill_switch = False
        ex._init_ws_staleness_state()
        ex._staleness_kill_switch = latched
        ex._gate_chain = GateChain([KillSwitchGate(), WsStalenessGate(3.0, 12.0)])
        ex._halt_source = NoHaltSource()
        ex._calendar = SimpleNamespace(now=lambda ts: _kst(10), in_auction_window=lambda ts: False)
        ex._seed_fail_closed = False
        ex._unhedged_base_signed = MagicMock(return_value=Decimal("0"))
        ex._open_maker_orders = MagicMock(return_value=[])
        ex._pending_maker_notional = MagicMock(return_value=Decimal("0"))
        ex._compute_fair = MagicMock(return_value=Decimal("1"))
        ex._policy_side = MagicMock(return_value=None)
        ex._fake = {("kis", "005930-KRW"): kis_age, ("hyperliquid_perpetual", "X-USD"): hl_age}
        ex._strategy = SimpleNamespace(
            current_timestamp=1000.0,
            market_data_provider=SimpleNamespace(get_ws_freshness_sec=lambda c, p: ex._fake.get((c, p))),
        )
        return ex

    def test_gates_closed_when_kis_ws_stale(self):
        ex = self._make(enabled=True, kis_age=None)
        ex._evaluate_ws_staleness()
        self.assertFalse(ex._gates_open())

    def test_gates_open_when_fresh(self):
        ex = self._make(enabled=True, kis_age=0.5, hl_age=0.5)
        ex._evaluate_ws_staleness()
        self.assertTrue(ex._gates_open())

    def test_latch_ors_into_kill_switch(self):
        ex = self._make(enabled=True, kis_age=0.5, hl_age=0.5, latched=True)
        ex._evaluate_ws_staleness()
        self.assertFalse(ex._gates_open())

    def test_gates_closed_when_session_halted(self):
        ex = self._make(enabled=True)
        ex._gate_chain = GateChain([KillSwitchGate(), SessionHaltGate()])
        ex.config.session_halt_gate_enabled = True
        ex._halt_source = SimpleNamespace(
            evaluate=lambda p, **kw: SessionHaltState(halted=True, ready=True, reason="book_frozen"))
        ex._calendar = SimpleNamespace(now=lambda ts: _kst(10), in_auction_window=lambda ts: False)
        self.assertFalse(ex._gates_open())

    def test_gates_open_when_not_halted(self):
        ex = self._make(enabled=True)
        ex._gate_chain = GateChain([KillSwitchGate(), SessionHaltGate()])
        ex.config.session_halt_gate_enabled = True
        ex._halt_source = SimpleNamespace(
            evaluate=lambda p, **kw: SessionHaltState(halted=False, ready=True, reason=""))
        ex._calendar = SimpleNamespace(now=lambda ts: _kst(10), in_auction_window=lambda ts: False)
        # other gates open + fair computable -> open
        self.assertTrue(ex._gates_open())

    def _make_halt_seq(self, cooldown_s):
        """Executor whose halt source returns whatever ``state`` currently holds (mutate across ticks)."""
        ex = self._make(enabled=True)
        ex._gate_chain = GateChain([KillSwitchGate(), SessionHaltGate()])
        ex.config.session_halt_gate_enabled = True
        ex.config.session_halt_cooldown_s = cooldown_s
        state = {"halted": True, "reason": "book_frozen"}
        ex._halt_source = SimpleNamespace(
            evaluate=lambda p, **kw: SessionHaltState(
                halted=state["halted"], ready=True, reason=state["reason"]))
        ex._calendar = SimpleNamespace(now=lambda ts: _kst(10), in_auction_window=lambda ts: False)
        return ex, state

    def test_cooldown_holds_halt_after_freeze_clears(self):
        ex, state = self._make_halt_seq(1800.0)
        # tick 1: real freeze -> gates closed + cooldown armed (until 1000+1800=2800)
        ex._strategy.current_timestamp = 1000.0
        self.assertFalse(ex._gates_open())
        # tick 2: book un-froze (CB single-price auction) but within cooldown -> STILL closed
        state["halted"], state["reason"] = False, ""
        ex._strategy.current_timestamp = 1500.0
        self.assertFalse(ex._gates_open())
        # tick 3: cooldown expired -> gates open again
        ex._strategy.current_timestamp = 2801.0
        self.assertTrue(ex._gates_open())

    def test_cooldown_zero_does_not_hold(self):
        ex, state = self._make_halt_seq(0.0)   # cooldown disabled
        ex._strategy.current_timestamp = 1000.0
        self.assertFalse(ex._gates_open())      # frozen -> closed by the freeze itself
        state["halted"], state["reason"] = False, ""
        ex._strategy.current_timestamp = 1001.0
        self.assertTrue(ex._gates_open())       # no cooldown -> reopens immediately
