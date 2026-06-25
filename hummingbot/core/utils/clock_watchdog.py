"""Clock-stall watchdog (JEP-218): off-by-default, behavior-neutral diagnostic.

A separate OS daemon thread monitors a monotonic heartbeat updated on every
strategy clock tick (``StrategyV2Base.tick`` calls :meth:`beat`). If the heartbeat
stops advancing for longer than ``stall_threshold_s`` the event loop has wedged --
either a *synchronous* block on the loop thread (a blocking syscall / a
``threading`` primitive acquired on the loop thread) OR a *suspended coroutine*
that never resumes (the loop sits idle in ``select``). The single py-spy ``dump``
that diagnosed JEP-218 cannot disambiguate these, so on a stall the watchdog writes
a one-shot diagnostic bundle that captures BOTH:

  * ``faulthandler.dump_traceback(all_threads=True)`` -- the C-level Python stack of
    every thread, including the frozen event-loop thread and any blocked background
    thread (pins a synchronous block).
  * a snapshot of ``asyncio.all_tasks(loop)`` with each task's ``get_stack()`` frames
    (pins a suspended coroutine awaiting forever -- the case py-spy ``dump`` misses).

Design constraints (this is live-trading instrumentation):

  * Runs in its OWN OS thread, so it fires even while the event loop is fully frozen.
  * Reads only data structures; it schedules NOTHING on the loop and mutates no
    strategy state.
  * :meth:`beat` is a single timestamp store and never raises.
  * Every dump path is wrapped so a watchdog failure can never break trading.
  * Disabled unless ``HUMMINGBOT_CLOCK_WATCHDOG`` is truthy (or constructed directly).
"""
from __future__ import annotations

import asyncio
import faulthandler
import logging
import os
import threading
import time
from typing import Callable, Dict, Optional, TextIO

_TRUTHY = {"1", "true", "yes", "on", "y"}

# env knobs
ENV_ENABLE = "HUMMINGBOT_CLOCK_WATCHDOG"
ENV_STALL_S = "HUMMINGBOT_CLOCK_WATCHDOG_STALL_S"
ENV_POLL_S = "HUMMINGBOT_CLOCK_WATCHDOG_POLL_S"
ENV_COOLDOWN_S = "HUMMINGBOT_CLOCK_WATCHDOG_COOLDOWN_S"
ENV_DUMP_DIR = "HUMMINGBOT_CLOCK_WATCHDOG_DIR"
ENV_FAULTHANDLER = "HUMMINGBOT_CLOCK_WATCHDOG_FAULTHANDLER"

_logger: Optional[logging.Logger] = None


def _log() -> logging.Logger:
    global _logger
    if _logger is None:
        _logger = logging.getLogger(__name__)
    return _logger


def _truthy(value: Optional[str]) -> bool:
    return (value or "").strip().lower() in _TRUTHY


def _float_or(value: Optional[str], default: float) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


class ClockWatchdog:
    def __init__(
        self,
        *,
        stall_threshold_s: float = 10.0,
        poll_interval_s: float = 1.0,
        cooldown_s: float = 60.0,
        dump_dir: Optional[str] = None,
        arm_faulthandler: bool = True,
        time_fn: Callable[[], float] = time.monotonic,
        dump_fn: Optional[Callable[[str], None]] = None,
        faulthandler_mod=faulthandler,
    ):
        self.stall_threshold_s = float(stall_threshold_s)
        self.poll_interval_s = float(poll_interval_s)
        self.cooldown_s = float(cooldown_s)
        self.dump_dir = dump_dir
        # The OS-thread monitor (below) handles the realistic case: a suspended coroutine
        # OR a blocking syscall (both release the GIL, so the monitor thread runs and can
        # dump asyncio tasks). The faulthandler deadman is a GIL-INDEPENDENT C-timer backstop
        # for the one case the Python monitor cannot fire in -- a tight CPU loop holding the
        # GIL on the loop thread. Re-armed on every beat; fires all-thread stacks if beats stop.
        self.arm_faulthandler = bool(arm_faulthandler)
        self._faulthandler = faulthandler_mod
        self._fh_armed = False
        self._fh_file: Optional[TextIO] = None
        self._time_fn = time_fn
        # dump_fn override is a test seam; default writes the real diagnostic bundle.
        self._dump_fn = dump_fn or self._default_dump
        self._last_beat: Optional[float] = None
        self._last_tick_ts: Optional[float] = None
        self._last_dump_at: Optional[float] = None
        self._dump_count = 0
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------ construction
    @classmethod
    def from_env(cls, env: Optional[Dict[str, str]] = None) -> Optional["ClockWatchdog"]:
        """Return a configured watchdog iff the enable flag is truthy, else ``None``."""
        env = os.environ if env is None else env
        if not _truthy(env.get(ENV_ENABLE)):
            return None
        fh = env.get(ENV_FAULTHANDLER)
        return cls(
            stall_threshold_s=_float_or(env.get(ENV_STALL_S), 10.0),
            poll_interval_s=_float_or(env.get(ENV_POLL_S), 1.0),
            cooldown_s=_float_or(env.get(ENV_COOLDOWN_S), 60.0),
            dump_dir=env.get(ENV_DUMP_DIR) or None,
            # faulthandler deadman defaults ON when the watchdog is on; opt out with =0/false.
            arm_faulthandler=fh is None or _truthy(fh),
        )

    # ------------------------------------------------------------------ hot path
    def beat(self, tick_ts: Optional[float] = None) -> None:
        """Record a heartbeat. Called from the strategy clock tick (loop thread).

        Cheap (a couple of attribute stores) and exception-proof: instrumentation
        must never perturb the trading tick.
        """
        try:
            self._last_beat = self._time_fn()
            self._last_tick_ts = tick_ts
            if self._loop is None:
                # Capture the running loop the first time we beat from inside it, so the
                # watchdog thread can snapshot asyncio tasks even after the loop freezes.
                try:
                    self._loop = asyncio.get_running_loop()
                except RuntimeError:
                    self._loop = None
            self._rearm_faulthandler()
        except Exception:
            pass

    def _rearm_faulthandler(self) -> None:
        """Reset the GIL-independent deadman: it fires all-thread stacks iff beats stop
        for ``stall_threshold_s`` (catches a GIL-holding loop the OS thread can't preempt)."""
        if not self._fh_armed:
            return
        try:
            self._faulthandler.cancel_dump_traceback_later()
            self._faulthandler.dump_traceback_later(
                self.stall_threshold_s, repeat=False, file=self._fh_file
            )
        except Exception:
            pass

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._arm_faulthandler_deadman()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="clock_watchdog", daemon=True)
        self._thread.start()

    def _arm_faulthandler_deadman(self) -> None:
        if not self.arm_faulthandler or self._fh_armed:
            return
        try:
            self._fh_file = open(self._dump_path(), "a", buffering=1)
            self._faulthandler.dump_traceback_later(
                self.stall_threshold_s, repeat=False, file=self._fh_file
            )
            self._fh_armed = True
        except Exception:
            # faulthandler unavailable / no fd: fall back to the OS-thread monitor alone.
            self._fh_armed = False
            if self._fh_file is not None:
                try:
                    self._fh_file.close()
                except Exception:
                    pass
                self._fh_file = None

    def stop(self) -> None:
        self._stop.set()
        if self._fh_armed:
            try:
                self._faulthandler.cancel_dump_traceback_later()
            except Exception:
                pass
            self._fh_armed = False
            if self._fh_file is not None:
                try:
                    self._fh_file.close()
                except Exception:
                    pass
                self._fh_file = None
        t = self._thread
        if t is not None and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=2.0)
        self._thread = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        while not self._stop.wait(self.poll_interval_s):
            try:
                self._check_once()
            except Exception:
                # The watchdog must outlive its own bugs; never let the thread die.
                pass

    # ------------------------------------------------------------------ detection
    def _check_once(self) -> None:
        last = self._last_beat
        if last is None:
            return  # never beat yet (boot): a "stall" here is just not-started.
        now = self._time_fn()
        elapsed = now - last
        if elapsed < self.stall_threshold_s:
            return
        if self._last_dump_at is not None and (now - self._last_dump_at) < self.cooldown_s:
            return  # within cooldown: do not spam dumps while it stays frozen.
        self._last_dump_at = now
        self._dump_count += 1
        reason = (
            f"clock tick stalled {elapsed:.1f}s "
            f"(threshold={self.stall_threshold_s:.1f}s, last_tick_ts={self._last_tick_ts}, "
            f"dump #{self._dump_count})"
        )
        try:
            self._dump_fn(reason)
        except Exception:
            # A failed dump (disk full, etc.) must not crash the watchdog.
            try:
                _log().error("ClockWatchdog dump failed", exc_info=True)
            except Exception:
                pass

    # ------------------------------------------------------------------ dump bundle
    def _default_dump(self, reason: str) -> None:
        try:
            _log().error("JEP-218 ClockWatchdog: %s -- writing stall diagnostics", reason)
        except Exception:
            pass
        path = self._dump_path()
        fh: Optional[TextIO] = None
        try:
            fh = open(path, "a", buffering=1)
            fh.write(f"\n===== {reason} =====\n")
            fh.write(f"wall={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} pid={os.getpid()}\n\n")
            fh.write("----- all-thread tracebacks (faulthandler) -----\n")
            fh.flush()
            try:
                faulthandler.dump_traceback(file=fh, all_threads=True)
            except Exception as e:
                fh.write(f"<faulthandler dump failed: {e!r}>\n")
            fh.write("\n")
            fh.write(self._format_task_snapshot())
            fh.flush()
        finally:
            if fh is not None:
                try:
                    fh.close()
                except Exception:
                    pass
        try:
            _log().error("JEP-218 ClockWatchdog: stall diagnostics written to %s", path)
        except Exception:
            pass

    def _dump_path(self) -> str:
        base = self.dump_dir
        if not base:
            try:
                from hummingbot import prefix_path
                base = os.path.join(prefix_path(), "logs")
            except Exception:
                base = os.path.join(os.getcwd(), "logs")
        try:
            os.makedirs(base, exist_ok=True)
        except Exception:
            pass
        return os.path.join(base, f"clock_stall_{os.getpid()}.txt")

    def _format_task_snapshot(self) -> str:
        """Best-effort snapshot of suspended coroutines. Reads task/frame objects
        without scheduling on the loop, so it works even while the loop is frozen."""
        lines = ["----- asyncio tasks (suspended-coroutine stacks) -----\n"]
        loop = self._loop
        try:
            if loop is None:
                lines.append("<no event loop captured (watchdog never beat from inside the loop)>\n")
                return "".join(lines)
            try:
                tasks = list(asyncio.all_tasks(loop))
            except Exception as e:
                lines.append(f"<asyncio.all_tasks failed: {e!r}>\n")
                return "".join(lines)
            lines.append(f"{len(tasks)} task(s)\n")
            for i, task in enumerate(tasks):
                try:
                    coro = task.get_coro()
                    name = getattr(task, "get_name", lambda: "?")()
                    lines.append(f"\n[task {i}] name={name} done={task.done()} coro={coro!r}\n")
                    for frame in task.get_stack():
                        co = frame.f_code
                        lines.append(f"    {co.co_filename}:{frame.f_lineno} in {co.co_name}\n")
                except Exception as e:
                    lines.append(f"    <stack unavailable: {e!r}>\n")
        except Exception as e:
            lines.append(f"<task snapshot failed: {e!r}>\n")
        return "".join(lines)
