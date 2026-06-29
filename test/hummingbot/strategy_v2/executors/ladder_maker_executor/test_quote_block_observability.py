"""2026-06-29 (Bug C): the quote-gate early-return path was fully silent — a seeded-OK but
non-quoting executor (the SKHX incident) emitted ZERO diagnostics on any blocking branch
(closed gate reason discarded; fair=None silent). These tests pin:
  - _gates_open records WHY it closed on self._last_gate_block (gate reason or fair=None+inputs),
  - control_task surfaces it via a throttled WARNING (_log_quote_block).
Behavior-neutral: gate/control-flow semantics unchanged, logging only.
"""
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_halt_source import NoHaltSource
from hummingbot.strategy_v2.gates.gate_chain import GateChain, KillSwitchGate, WsStalenessGate

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    _EXECUTOR_IMPORTABLE = False


def _kst(hour: int):
    from datetime import datetime, timezone, timedelta
    return datetime(2026, 6, 22, hour, 0, tzinfo=timezone(timedelta(hours=9)))


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack (paho) - run in Docker/CI")
class QuoteBlockObservabilityTest(unittest.TestCase):
    def _make(self, *, kill_switch=False, fair=Decimal("1"),
              bid=Decimal("100"), ask=Decimal("101"), fx_legs=(Decimal("1300"), Decimal("1301"))):
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex.config = SimpleNamespace(
            kill_switch=kill_switch,
            ws_staleness_kill_switch_enabled=False,
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
        ex._staleness_kill_switch = False
        ex._gate_chain = GateChain([KillSwitchGate(), WsStalenessGate(3.0, 12.0)])
        ex._halt_source = NoHaltSource()
        ex._calendar = SimpleNamespace(now=lambda ts: _kst(10), in_auction_window=lambda ts: False)
        ex._seed_fail_closed = False
        ex._unhedged_base_signed = MagicMock(return_value=Decimal("0"))
        ex._open_maker_orders = MagicMock(return_value=[])
        ex._pending_maker_notional = MagicMock(return_value=Decimal("0"))
        ex._compute_fair = MagicMock(return_value=fair)
        ex._policy_side = MagicMock(return_value=None)
        ex.connectors = {
            ex.hedge_connector: SimpleNamespace(
                get_price_by_type=lambda p, t: bid if "Bid" in str(t) else ask)
        }
        ex._fair = SimpleNamespace(observe_fx_legs=lambda: fx_legs)
        ex._fake = {("kis", "005930-KRW"): 0.5, ("hyperliquid_perpetual", "X-USD"): 0.5}
        ex._strategy = SimpleNamespace(
            current_timestamp=1000.0,
            market_data_provider=SimpleNamespace(get_ws_freshness_sec=lambda c, p: ex._fake.get((c, p))),
        )
        ex._evaluate_ws_staleness()
        return ex

    def test_gates_open_records_fair_none_reason(self):
        ex = self._make(fair=None)
        self.assertFalse(ex._gates_open())
        self.assertIsNotNone(ex._last_gate_block)
        self.assertIn("fair=None", ex._last_gate_block)
        # concrete inputs are surfaced for diagnosis
        self.assertIn("hedge_bid", ex._last_gate_block)
        self.assertIn("fx_legs", ex._last_gate_block)

    def test_gates_open_records_gate_chain_reason(self):
        ex = self._make(kill_switch=True)
        self.assertFalse(ex._gates_open())
        self.assertEqual("kill_switch=True", ex._last_gate_block)

    def test_gates_open_clears_block_reason_when_open(self):
        ex = self._make(fair=Decimal("100"))
        self.assertTrue(ex._gates_open())
        self.assertIsNone(ex._last_gate_block)

    def test_log_quote_block_throttles_same_reason_logs_on_change(self):
        ex = self._make()
        with self.assertLogs(level="WARNING") as cm:
            ex._log_quote_block("reason_a")
            ex._log_quote_block("reason_a")  # same reason within interval -> throttled
            ex._log_quote_block("reason_b")  # reason changed -> logs immediately
        blocked = [line for line in cm.output if "quoting blocked" in line]
        self.assertEqual(len(blocked), 2)
        self.assertIn("reason_a", blocked[0])
        self.assertIn("reason_b", blocked[1])

    def test_log_quote_block_benign_at_debug_actionable_at_warning(self):
        # Codex finding 5: out_of_session / seed_pending are EXPECTED (overnight close, cold-boot)
        # -> DEBUG so they do not spam the operator alarm; a real in-session block stays WARNING.
        ex = self._make()
        with self.assertLogs(level="DEBUG") as cm:
            ex._log_quote_block("out_of_session")
            ex._log_quote_block("fair=None (hedge_bid=0 hedge_ask=0 fx_legs=(None, None))")
        recs = [(r.levelname, r.getMessage()) for r in cm.records if "quoting blocked" in r.getMessage()]
        oos = [lvl for lvl, msg in recs if "out_of_session" in msg]
        fair = [lvl for lvl, msg in recs if "fair=None" in msg]
        self.assertEqual(["DEBUG"], oos)
        self.assertEqual(["WARNING"], fair)
