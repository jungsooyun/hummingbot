import json
import unittest
from decimal import Decimal

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    _EXECUTOR_IMPORTABLE = True
except Exception:
    _EXECUTOR_IMPORTABLE = False


class _Clock:
    def __init__(self, t=0.0):
        self.t = t
    def __call__(self):
        return self.t
    def advance(self, dt):
        self.t += dt


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack (paho) — run in Docker/CI")
class LadderLatencyFairTargetsTest(unittest.TestCase):
    def _executor_single_sided(self, recorder):
        from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import Side
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex._latency_recorder = recorder
        ex.entry_side = __import__("hummingbot.core.data_type.common", fromlist=["TradeType"]).TradeType.SELL
        # stub the pieces _compute_targets calls
        ex._policy_side = lambda: Side.SELL
        ex._compute_fair = lambda side: Decimal("100")
        ex._is_two_sided = lambda: False
        ex._unhedged_base_signed = lambda: Decimal("0")
        ex._maker_executed_base = Decimal("0")
        from types import SimpleNamespace
        ex.config = SimpleNamespace(
            rungs=[SimpleNamespace(edge_bps=Decimal("10"), size=Decimal("10"),
                                   min_edge_bps=Decimal("0"), enabled=True)],
            total_size_cap=Decimal("100"), maker_tick=Decimal("0.001"),
            buffer_ticks=Decimal("0"), max_inventory=Decimal("8"),
            round_trip_cost_bps=Decimal("0"),
        )
        return ex

    def test_compute_targets_marks_fair_and_targets(self):
        out, clk = [], _Clock(0.0)
        from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.latency_recorder import LatencyRecorder
        rec = LatencyRecorder(symbol="EWY-USD", sink=out.append, perf_counter=clk)
        ex = self._executor_single_sided(rec)
        rec.tick_start(maker_freshness_ms=None, fair_freshness_ms=None, two_sided=False, ts_wall=1.0)
        ex._compute_targets()
        rec.mark("compute"); rec.mark("decision"); rec.tick_end()
        r = json.loads(out[0])
        self.assertIsNotNone(r["fair_ms"])
        self.assertIsNotNone(r["targets_ms"])
