# latency_recorder.py
"""Per-tick latency recorder for cross-venue hedged executors (JEP-184).

Read-only, flag-gated, exception-safe profiling. Captures monotonic
``perf_counter()`` deltas at the decision-pipeline boundaries and emits ONE JSON
line per reconcile tick through a swappable ``sink``. The default sink is a
dedicated per-symbol log file; tests inject an in-memory list. Nothing here ever
raises into the control loop (every public method swallows its own errors).

Boundary marks (set by the executor; absent marks ⇒ null field):
  fair, targets   -- ladder sub-split inside _compute_targets (ladder-only)
  compute         -- end of _compute_targets (base)
  decision        -- end of _should_reprice (base)
  submit          -- end of order placement (base; only when repriced)
"""
import atexit
import json
import os
import queue
import threading
import time
from typing import Callable, Dict, List, Optional

_SINK_SENTINEL = object()  # stop signal for the writer thread

MM_LATENCY_FIELDS: List[str] = [
    "ts_wall", "symbol", "two_sided", "repriced",
    "maker_freshness_ms", "fair_freshness_ms",
    "fair_ms", "targets_ms", "compute_ms", "decision_ms", "submit_ms",
    "total_ms", "loop_interval_ms",
]

_MS = 1000.0


def _default_log_path(symbol: str) -> str:
    safe = symbol.replace("/", "_").replace(":", "_")
    try:
        from hummingbot import prefix_path  # hummingbot/__init__.py:39
        base = os.path.join(prefix_path(), "logs")
    except Exception:
        base = os.path.join(os.getcwd(), "logs")
    try:
        os.makedirs(base, exist_ok=True)
    except Exception:
        pass
    return os.path.join(base, f"mm_latency_{safe}.jsonl")


def _make_file_sink(symbol: str, log_path: str, queue_maxsize: int = 10000):
    """Off-loop per-symbol JSONL sink (JEP-184): a bounded queue drained by a daemon
    writer thread into a dedicated file. The control loop only does a non-blocking
    put_nowait (drop-oldest on backpressure), so a disk stall never blocks trading.

    NB (peer-review B2 re-check): we do NOT use logging.handlers.QueueListener — its
    stop() enqueues a sentinel via put_nowait and would raise queue.Full on a SATURATED
    bounded queue, leaking the listener thread. Our own writer owns shutdown: closer()
    GUARANTEES the sentinel is enqueued (drop-oldest until room) and joins the thread,
    so drain-on-stop is safe even under saturation. Returns (sink_callable, closer)."""
    q: "queue.Queue" = queue.Queue(maxsize=queue_maxsize)
    fh = open(log_path, "a", buffering=1)  # line-buffered text append

    def _writer() -> None:
        while True:
            item = q.get()
            if item is _SINK_SENTINEL:
                break
            try:
                fh.write(item + "\n")
            except Exception:
                pass

    thread = threading.Thread(target=_writer, name=f"mm_latency_{symbol}", daemon=True)
    thread.start()

    def sink(line: str) -> None:
        try:
            q.put_nowait(line)
        except queue.Full:
            try:
                q.get_nowait()          # drop oldest; profiling must never stall trading
            except queue.Empty:
                pass
            try:
                q.put_nowait(line)
            except queue.Full:
                pass

    def closer() -> None:
        # Guarantee the stop sentinel is enqueued even if the queue is saturated:
        # drop-oldest until there is room (terminates in <= 2 iterations — single
        # producer, stopped at shutdown). Then join the writer and close the file.
        for _ in range(queue_maxsize + 2):
            try:
                q.put_nowait(_SINK_SENTINEL)
                break
            except queue.Full:
                try:
                    q.get_nowait()
                except queue.Empty:
                    pass
        thread.join(timeout=2.0)
        try:
            fh.close()
        except Exception:
            pass

    return sink, closer


class LatencyRecorder:
    def __init__(
        self,
        symbol: str,
        sink: Optional[Callable[[str], None]] = None,
        log_path: Optional[str] = None,
        perf_counter: Callable[[], float] = time.perf_counter,
        queue_maxsize: int = 10000,
    ):
        self._symbol = symbol
        self._pc = perf_counter
        self._closer: Optional[Callable[[], None]] = None
        self._closed = False
        if sink is not None:
            self._sink = sink                       # test seam: synchronous, no thread
        else:
            path = log_path or _default_log_path(symbol)
            self._sink, self._closer = _make_file_sink(symbol, path, queue_maxsize)
            atexit.register(self.close)             # backstop drain if on_stop is missed
        self._prev_start: Optional[float] = None
        self._reset_tick()

    def _reset_tick(self) -> None:
        self._t0: Optional[float] = None
        self._marks: Dict[str, float] = {}
        self._ts_wall: Optional[float] = None
        self._maker_freshness_ms: Optional[float] = None
        self._fair_freshness_ms: Optional[float] = None
        self._two_sided: bool = False

    def tick_start(
        self,
        maker_freshness_ms: Optional[float],
        fair_freshness_ms: Optional[float],
        two_sided: bool,
        ts_wall: Optional[float] = None,
    ) -> None:
        try:
            self._reset_tick()
            self._t0 = self._pc()
            self._ts_wall = ts_wall if ts_wall is not None else time.time()
            self._maker_freshness_ms = maker_freshness_ms
            self._fair_freshness_ms = fair_freshness_ms
            self._two_sided = bool(two_sided)
        except Exception:
            self._t0 = None  # disable this tick; tick_end becomes a no-op

    def mark(self, stage: str) -> None:
        try:
            if self._t0 is not None:
                self._marks[stage] = self._pc()
        except Exception:
            pass

    def tick_end(self) -> None:
        try:
            if self._t0 is None:
                return
            t0, m = self._t0, self._marks

            def delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
                if a is None or b is None:
                    return None
                return (b - a) * _MS

            fair = m.get("fair")
            targets = m.get("targets")
            compute = m.get("compute")
            decision = m.get("decision")
            submit = m.get("submit")
            last = max(m.values()) if m else t0

            loop_interval = None if self._prev_start is None else (t0 - self._prev_start) * _MS
            record = {
                "ts_wall": self._ts_wall,
                "symbol": self._symbol,
                "two_sided": self._two_sided,
                "repriced": submit is not None,
                "maker_freshness_ms": self._maker_freshness_ms,
                "fair_freshness_ms": self._fair_freshness_ms,
                "fair_ms": delta(t0, fair),
                "targets_ms": delta(fair, targets),
                "compute_ms": delta(t0, compute),
                "decision_ms": delta(compute, decision),
                "submit_ms": delta(decision, submit),
                "total_ms": delta(t0, last),
                "loop_interval_ms": loop_interval,
            }
            self._prev_start = t0
            self._sink(json.dumps(record))
        except Exception:
            # Profiling must NEVER break the trading loop.
            try:
                self._prev_start = self._t0
            except Exception:
                pass
        finally:
            self._reset_tick()

    def close(self) -> None:
        if self._closed:                            # idempotent: on_stop + atexit may both call
            return
        self._closed = True
        try:
            if self._closer is not None:
                self._closer()
        except Exception:
            pass
