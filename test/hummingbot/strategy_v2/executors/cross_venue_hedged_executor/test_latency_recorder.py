# test_latency_recorder.py
import json
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.latency_recorder import (
    LatencyRecorder,
    MM_LATENCY_FIELDS,
)


class _Clock:
    """Deterministic monotonic clock injected as perf_counter."""
    def __init__(self, t=0.0):
        self.t = t
    def __call__(self):
        return self.t
    def advance(self, dt):
        self.t += dt
        return self.t


def _make(sink_list, clock):
    return LatencyRecorder(symbol="EWY-USD", sink=sink_list.append, perf_counter=clock)


def test_single_tick_emits_one_record_with_boundary_deltas():
    out, clk = [], _Clock(100.0)
    rec = _make(out, clk)
    rec.tick_start(maker_freshness_ms=412.0, fair_freshness_ms=1180.5, two_sided=True, ts_wall=1750400000.0)
    clk.advance(0.00042); rec.mark("fair")
    clk.advance(0.00018); rec.mark("targets")
    clk.advance(0.00011); rec.mark("compute")
    clk.advance(0.00005); rec.mark("decision")
    clk.advance(0.00230); rec.mark("submit")
    rec.tick_end()
    assert len(out) == 1
    r = json.loads(out[0])
    assert r["symbol"] == "EWY-USD" and r["two_sided"] is True and r["repriced"] is True
    assert r["maker_freshness_ms"] == 412.0 and r["fair_freshness_ms"] == 1180.5
    assert round(r["fair_ms"], 3) == 0.42      # 100.00042 - 100.0
    assert round(r["targets_ms"], 3) == 0.18   # fair -> targets
    assert round(r["compute_ms"], 3) == 0.71   # tick start -> compute mark
    assert round(r["decision_ms"], 3) == 0.05
    assert round(r["submit_ms"], 3) == 2.30
    assert round(r["total_ms"], 3) == 3.06     # start -> submit
    assert r["loop_interval_ms"] is None       # first tick
    assert set(r.keys()) == set(MM_LATENCY_FIELDS)


def test_no_reprice_tick_has_null_submit_and_repriced_false():
    out, clk = [], _Clock(0.0)
    rec = _make(out, clk)
    rec.tick_start(maker_freshness_ms=1.0, fair_freshness_ms=2.0, two_sided=False, ts_wall=1.0)
    clk.advance(0.001); rec.mark("compute")
    clk.advance(0.0001); rec.mark("decision")
    rec.tick_end()                              # no submit, no fair/targets
    r = json.loads(out[0])
    assert r["repriced"] is False and r["submit_ms"] is None
    assert r["fair_ms"] is None and r["targets_ms"] is None   # base-only path
    assert r["compute_ms"] is not None and r["decision_ms"] is not None


def test_loop_interval_measured_across_ticks():
    out, clk = [], _Clock(0.0)
    rec = _make(out, clk)
    rec.tick_start(maker_freshness_ms=None, fair_freshness_ms=None, two_sided=False, ts_wall=1.0)
    rec.mark("compute"); rec.mark("decision"); rec.tick_end()
    clk.advance(1.0014)                          # next tick ~1.0014s later
    rec.tick_start(maker_freshness_ms=None, fair_freshness_ms=None, two_sided=False, ts_wall=2.0)
    rec.mark("compute"); rec.mark("decision"); rec.tick_end()
    r2 = json.loads(out[1])
    assert round(r2["loop_interval_ms"], 1) == 1001.4


def test_recorder_never_raises_even_if_sink_throws():
    def bad_sink(_line):
        raise RuntimeError("disk full")
    rec = LatencyRecorder(symbol="X", sink=bad_sink, perf_counter=_Clock(0.0))
    rec.tick_start(maker_freshness_ms=None, fair_freshness_ms=None, two_sided=False, ts_wall=1.0)
    rec.mark("compute")
    rec.tick_end()                               # must NOT raise


def test_default_file_sink_writes_jsonl(tmp_path):
    path = tmp_path / "mm_latency_EWY-USD.jsonl"
    rec = LatencyRecorder(symbol="EWY-USD", log_path=str(path), perf_counter=_Clock(0.0))
    rec.tick_start(maker_freshness_ms=None, fair_freshness_ms=None, two_sided=False, ts_wall=1.0)
    rec.mark("compute"); rec.mark("decision"); rec.tick_end()
    rec.close()
    lines = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    assert len(lines) == 1 and lines[0]["symbol"] == "EWY-USD"


def test_offloop_file_sink_never_raises_and_drains_on_close(tmp_path):
    # Exercises the real off-loop path (queue + daemon writer thread). The loop-side
    # put_nowait must never raise under load; close() drains the queue to disk.
    path = tmp_path / "mm_latency_FLOOD.jsonl"
    rec = LatencyRecorder(symbol="FLOOD", log_path=str(path), perf_counter=_Clock(0.0))
    for i in range(500):
        rec.tick_start(maker_freshness_ms=None, fair_freshness_ms=None, two_sided=False, ts_wall=float(i))
        rec.mark("compute"); rec.mark("decision"); rec.tick_end()   # must never raise
    rec.close()                                                      # drains + joins writer
    rec.close()                                                      # idempotent: no error
    lines = [l for l in path.read_text().splitlines() if l.strip()]
    assert 0 < len(lines) <= 500                                    # all, or drop-oldest subset


def test_offloop_close_drains_and_joins_under_saturation(tmp_path):
    # Peer-review B2 re-check: a TINY queue flooded past capacity must still shut down
    # cleanly — close() guarantees the sentinel is enqueued (drop-oldest), returns
    # promptly (no hang), and leaves no writer thread alive (no leak).
    import threading as _t
    path = tmp_path / "mm_latency_SAT.jsonl"
    rec = LatencyRecorder(symbol="SAT", log_path=str(path), perf_counter=_Clock(0.0), queue_maxsize=4)
    for i in range(200):
        rec.tick_start(maker_freshness_ms=None, fair_freshness_ms=None, two_sided=False, ts_wall=float(i))
        rec.mark("compute"); rec.mark("decision"); rec.tick_end()
    rec.close()                                                      # must not hang
    assert not any(t.name == "mm_latency_SAT" and t.is_alive() for t in _t.enumerate())
    assert path.exists()
