import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.latency_recorder import LatencyRecorder


class _Clock:
    def __init__(self, t=0.0):
        self.t = t
    def __call__(self):
        return self.t
    def advance(self, dt):
        self.t += dt


class _Harness(CrossVenueHedgedExecutorBase):
    """Minimal: only what _reconcile_maker touches on the observe path."""
    def __init__(self, recorder=None, observe=True):
        self.maker_connector = "hl"
        self.maker_trading_pair = "EWY-USD"
        self.hedge_connector = "kis"
        self.hedge_trading_pair = "069500-KRW"
        self.config = SimpleNamespace(observe=observe, two_sided=False)
        self._last_reprice_ts = 0.0
        self._latency_recorder = recorder
        fresh = {("hl", "EWY-USD"): 0.4, ("kis", "069500-KRW"): 1.1}  # seconds
        mdp = SimpleNamespace(
            get_order_book_freshness_sec=lambda c, p: fresh.get((c, p))
        )
        self._strategy = SimpleNamespace(current_timestamp=123.0, market_data_provider=mdp)
        self._placed = []

    # stub hooks the observe path calls
    def _compute_targets(self):
        return ["T"]
    def _should_reprice(self, targets):
        return True
    def _place_targets(self, targets):
        self._placed.append(targets)
    def _gates_open(self):
        return True
    def _size_hedge(self, pending_base: Decimal):
        return None
    def _maker_balance_candidate(self):
        return None


def test_reconcile_records_one_tick_with_coarse_boundaries():
    out, clk = [], _Clock(10.0)
    rec = LatencyRecorder(symbol="EWY-USD", sink=out.append, perf_counter=clk)
    h = _Harness(recorder=rec, observe=True)
    # advance the clock as each stage marks (compute/decision/submit)
    orig_compute = h._compute_targets
    h._compute_targets = lambda: (clk.advance(0.001), orig_compute())[1]
    h._reconcile_maker()
    assert len(out) == 1
    r = json.loads(out[0])
    assert r["symbol"] == "EWY-USD" and r["repriced"] is True
    assert r["maker_freshness_ms"] == 400.0 and r["fair_freshness_ms"] == 1100.0
    assert r["compute_ms"] is not None and r["decision_ms"] is not None and r["submit_ms"] is not None
    assert h._placed == [["T"]]            # behavior unchanged: order still "placed"


def test_flag_off_no_records_and_unchanged_behavior():
    h = _Harness(recorder=None, observe=True)
    h._reconcile_maker()
    assert h._placed == [["T"]]            # observe place still happens, no recorder, no error


def test_no_reprice_still_records_decision_no_submit():
    out, clk = [], _Clock(0.0)
    rec = LatencyRecorder(symbol="EWY-USD", sink=out.append, perf_counter=clk)
    h = _Harness(recorder=rec, observe=True)
    h._should_reprice = lambda targets: False
    h._reconcile_maker()
    r = json.loads(out[0])
    assert r["repriced"] is False and r["submit_ms"] is None and r["decision_ms"] is not None
    assert h._placed == []                 # no placement when reprice gate is closed


def test_on_stop_closes_recorder():
    closed = {"v": 0}
    class _Rec:
        def close(self):
            closed["v"] += 1
    h = _Harness(recorder=_Rec(), observe=True)
    h.on_stop()
    assert closed["v"] == 1                 # recorder drained/closed on executor stop


def test_on_stop_noop_when_recorder_absent():
    h = _Harness(recorder=None, observe=True)
    h.on_stop()                             # must not raise when profiling is off


def test_reconcile_survives_freshness_raising():
    out, clk = [], _Clock(0.0)
    rec = LatencyRecorder(symbol="EWY-USD", sink=out.append, perf_counter=clk)
    h = _Harness(recorder=rec, observe=True)
    def boom(c, p):
        raise RuntimeError("md broken")
    h._strategy.market_data_provider.get_order_book_freshness_sec = boom
    h._reconcile_maker()
    assert h._placed == [["T"]]
