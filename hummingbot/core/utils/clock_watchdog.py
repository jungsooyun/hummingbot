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

Optional **self-heal** (off by default): when ``self_heal`` is enabled, the watchdog
terminates the process *after* writing the diagnostic bundle, so a process supervisor
(docker ``restart:`` / systemd ``Restart=``) relaunches it and adopt re-seeds the
positions. A frozen event loop cannot cancel orders or re-hedge itself, so a clean
restart is the only in-band recovery for an unattended long run. This recovers the
realistic JEP-218 freeze (a suspended coroutine or a blocking syscall -- both release
the GIL, so the OS watchdog thread runs and can ``os._exit`` the process). A pure
GIL-holding CPU loop on the event-loop thread is *not* an observed JEP-218 mode; it is
still dumped by the faulthandler deadman but is NOT self-healed (documented limitation).

Design constraints (this is live-trading instrumentation):

  * Runs in its OWN OS thread, so it fires even while the event loop is fully frozen.
  * Reads only data structures; it schedules NOTHING on the loop and mutates no
    strategy state.
  * :meth:`beat` is a single timestamp store and never raises.
  * Every dump path is wrapped so a watchdog failure can never break trading.
  * Disabled unless ``HUMMINGBOT_CLOCK_WATCHDOG`` is truthy (or constructed directly).
  * Self-heal is a SECOND opt-in flag (``HUMMINGBOT_CLOCK_WATCHDOG_SELF_HEAL``); the
    default watchdog stays diagnostic-only and never terminates the process.
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

# Default self-heal exit code: EX_TEMPFAIL ("transient failure, please restart me").
SELF_HEAL_EXIT_CODE = 75
# Require this many CONSECUTIVE stall observations (with the watchdog thread demonstrably
# scheduled between them) before self-heal exits -- rejects a whole-process deschedule
# (hypervisor live-migration / SIGSTOP / swap) that would otherwise look like one giant stall.
SELF_HEAL_MIN_CONSECUTIVE = 2
# Cross-restart governor: at most N self-heal exits within the wall-clock window, else park in
# dump-only mode (a freeze that survives restart must not crash-loop the live book forever).
SELF_HEAL_MAX_PER_WINDOW = 3
SELF_HEAL_WINDOW_S = 600.0

# env knobs
ENV_ENABLE = "HUMMINGBOT_CLOCK_WATCHDOG"
ENV_STALL_S = "HUMMINGBOT_CLOCK_WATCHDOG_STALL_S"
ENV_POLL_S = "HUMMINGBOT_CLOCK_WATCHDOG_POLL_S"
ENV_COOLDOWN_S = "HUMMINGBOT_CLOCK_WATCHDOG_COOLDOWN_S"
ENV_DUMP_DIR = "HUMMINGBOT_CLOCK_WATCHDOG_DIR"
ENV_FAULTHANDLER = "HUMMINGBOT_CLOCK_WATCHDOG_FAULTHANDLER"
ENV_SELF_HEAL = "HUMMINGBOT_CLOCK_WATCHDOG_SELF_HEAL"
ENV_SELF_HEAL_EXIT_CODE = "HUMMINGBOT_CLOCK_WATCHDOG_EXIT_CODE"
ENV_SELF_HEAL_MAX = "HUMMINGBOT_CLOCK_WATCHDOG_SELF_HEAL_MAX"
ENV_SELF_HEAL_WINDOW_S = "HUMMINGBOT_CLOCK_WATCHDOG_SELF_HEAL_WINDOW_S"

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


def _int_or(value: Optional[str], default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _boot_clock() -> float:
    """A clock for the self-heal governor that is continuous across a CONTAINER restart (host
    up) and immune to wall-clock (NTP) steps, so it bounds a fast restart-loop correctly.
    ``CLOCK_BOOTTIME`` advances through suspend and survives a container restart; it resets only
    on a host reboot, which the governor handles via a symmetric window prune. Falls back to wall
    time where ``CLOCK_BOOTTIME`` is unavailable (e.g. non-Linux dev hosts)."""
    try:
        return time.clock_gettime(time.CLOCK_BOOTTIME)
    except (AttributeError, OSError, ValueError):
        return time.time()


class ClockWatchdog:
    def __init__(
        self,
        *,
        stall_threshold_s: float = 10.0,
        poll_interval_s: float = 1.0,
        cooldown_s: float = 60.0,
        dump_dir: Optional[str] = None,
        arm_faulthandler: bool = True,
        self_heal: bool = False,
        self_heal_exit_code: int = SELF_HEAL_EXIT_CODE,
        self_heal_min_consecutive: int = SELF_HEAL_MIN_CONSECUTIVE,
        self_heal_max_per_window: int = SELF_HEAL_MAX_PER_WINDOW,
        self_heal_window_s: float = SELF_HEAL_WINDOW_S,
        governor_path: Optional[str] = None,
        time_fn: Callable[[], float] = time.monotonic,
        wall_time_fn: Callable[[], float] = _boot_clock,
        dump_fn: Optional[Callable[[str], None]] = None,
        exit_fn: Callable[[int], None] = os._exit,
        faulthandler_mod=faulthandler,
    ):
        self.stall_threshold_s = float(stall_threshold_s)
        self.poll_interval_s = float(poll_interval_s)
        self.cooldown_s = float(cooldown_s)
        self.dump_dir = dump_dir
        # Self-heal: off by default. When on, the watchdog terminates the process after the
        # diagnostic dump so the supervisor restarts it (the only in-band recovery for a
        # frozen loop). exit_fn is a test seam; default os._exit terminates from this thread.
        # Two guards (both verified necessary by adversarial review):
        #   * self_heal_min_consecutive: require N stall observations with the watchdog thread
        #     itself demonstrably scheduled between them -- rejects a whole-process deschedule
        #     (VM live-migration / SIGSTOP / swap) that looks like one giant stall on resume.
        #   * governor (max_per_window / window_s): a freeze that survives restart must not
        #     crash-loop; after N self-heals in the window the watchdog parks in dump-only.
        # wall_time_fn backs the governor (a boot-clock, NOT monotonic) so its history survives
        # container restarts and is immune to NTP steps.
        self.self_heal = bool(self_heal)
        self.self_heal_exit_code = int(self_heal_exit_code)
        self.self_heal_min_consecutive = max(1, int(self_heal_min_consecutive))
        self.self_heal_max_per_window = max(1, int(self_heal_max_per_window))
        self.self_heal_window_s = float(self_heal_window_s)
        self.governor_path = governor_path
        self._wall_time_fn = wall_time_fn
        self._exit_fn = exit_fn
        self._self_heal_fired = False
        self._last_poll_at: Optional[float] = None
        self._consecutive_stalls = 0
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
            # self-heal is a SECOND opt-in: diagnostic-only unless explicitly enabled.
            self_heal=_truthy(env.get(ENV_SELF_HEAL)),
            self_heal_exit_code=_int_or(env.get(ENV_SELF_HEAL_EXIT_CODE), SELF_HEAL_EXIT_CODE),
            self_heal_max_per_window=_int_or(env.get(ENV_SELF_HEAL_MAX), SELF_HEAL_MAX_PER_WINDOW),
            self_heal_window_s=_float_or(env.get(ENV_SELF_HEAL_WINDOW_S), SELF_HEAL_WINDOW_S),
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
    def _healthy_poll_gap_s(self) -> float:
        """Upper bound on a NORMAL gap between consecutive watchdog polls. A larger gap means
        the watchdog thread itself was descheduled -- evidence of a whole-process suspend
        rather than a wedged event loop."""
        return self.poll_interval_s * 2.0 + 1.0

    def _check_once(self) -> None:
        now = self._time_fn()
        prev_poll = self._last_poll_at
        self._last_poll_at = now  # update every poll, including boot/early-return, for the gate
        last = self._last_beat
        if last is None:
            return  # never beat yet (boot): a "stall" here is just not-started.
        elapsed = now - last
        if elapsed < self.stall_threshold_s:
            self._consecutive_stalls = 0
            return
        # --- a stall is observed ---
        # Self-liveness gate: if the watchdog thread's OWN inter-poll gap is also large, the
        # WHOLE process was descheduled (hypervisor live-migration / SIGSTOP / swap), not a
        # wedged loop. Never self-heal on that -- it would kill a healthy bot on resume.
        wd_scheduled = prev_poll is not None and (now - prev_poll) <= self._healthy_poll_gap_s()
        if wd_scheduled:
            self._consecutive_stalls += 1
        # else: the watchdog thread was itself descheduled this interval -> HOLD the count (do
        # NOT reset). A genuine wedge whose watchdog thread is only INTERMITTENTLY starved must
        # still accumulate scheduled observations; resetting here would let one bad poll between
        # good ones pin a real wedge below the threshold forever. A whole-process suspend still
        # cannot ARM self-heal from here (only a scheduled poll increments), and the count is
        # reset to 0 the instant a real beat resumes (the elapsed < threshold branch above).
        reason = (
            f"clock tick stalled {elapsed:.1f}s "
            f"(threshold={self.stall_threshold_s:.1f}s, last_tick_ts={self._last_tick_ts}, "
            f"consec_scheduled_stalls={self._consecutive_stalls}, wd_scheduled={wd_scheduled})"
        )
        # diagnostic dump (cooldown-gated; always informative, independent of self-heal)
        if self._last_dump_at is None or (now - self._last_dump_at) >= self.cooldown_s:
            self._last_dump_at = now
            self._dump_count += 1
            try:
                self._dump_fn(reason)
            except Exception:
                # A failed dump (disk full, etc.) must not crash the watchdog -- and must NOT
                # block self-heal: recovery still fires below even when the dump blew up.
                try:
                    _log().error("ClockWatchdog dump failed", exc_info=True)
                except Exception:
                    pass
        # self-heal (opt-in): only after N consecutive SCHEDULED stalls, and only if the
        # cross-restart governor still permits it. Latch once per process either way.
        if (self.self_heal and not self._self_heal_fired and wd_scheduled
                and self._consecutive_stalls >= self.self_heal_min_consecutive):
            self._self_heal_fired = True
            if self._self_heal_governor_allows():
                self._self_heal_restart(reason)
            else:
                self._park_self_heal(reason)

    def _self_heal_restart(self, reason: str) -> None:
        """Terminate the process so the supervisor (docker ``restart: unless-stopped`` / systemd
        ``Restart=``) relaunches it. Called from the OS watchdog thread AFTER the diagnostic
        bundle is written and AFTER the consecutive-stall + governor guards pass; ``os._exit``
        terminates immediately without waiting on the wedged event loop.

        NOTE: a clean restart only RECOVERS positions if the restarted controller has
        ``adopt_existing_inventory: true`` AND adopt reliably detects + re-hedges a single-leg
        (naked) state. Neither is guaranteed today (live SMSN has shipped ``adopt:false``, and
        adopt fail-closes on a balance miss -- see JEP-209/210/211). Self-heal restores a
        *ticking* bot; it does NOT by itself guarantee a stranded hedge leg is re-hedged."""
        try:
            _log().critical(
                "JEP-218 ClockWatchdog SELF-HEAL: %s -- terminating pid %s (exit code %s) for "
                "supervisor restart (stall diagnostics already written to %s)",
                reason, os.getpid(), self.self_heal_exit_code, self._dump_path(),
            )
        except Exception:
            pass
        self._exit_fn(self.self_heal_exit_code)

    def _park_self_heal(self, reason: str) -> None:
        """Governor circuit-open: too many self-heal exits within the window (a freeze that
        survives restart). Refuse to exit -- park in dump-only so it cannot crash-loop the live
        book; an operator must intervene."""
        try:
            _log().critical(
                "JEP-218 ClockWatchdog SELF-HEAL CIRCUIT OPEN: >=%s self-heal exits within %.0fs "
                "-- refusing to restart again (freeze likely survives restart). Parking in "
                "dump-only mode; operator intervention required. (%s)",
                self.self_heal_max_per_window, self.self_heal_window_s, reason,
            )
        except Exception:
            pass

    # --------------------------------------------------------------- self-heal governor
    def _governor_path(self) -> str:
        if self.governor_path:
            return self.governor_path
        return os.path.join(os.path.dirname(self._dump_path()), "clock_self_heal_history.json")

    def _load_governor_history(self, path: str) -> list:
        try:
            import json
            with open(path, "r") as fh:
                data = json.load(fh)
            return [t for t in data if isinstance(t, (int, float))] if isinstance(data, list) else []
        except Exception:
            return []  # missing/corrupt history -> treat as empty (allow self-heal)

    def _persist_governor_history(self, path: str, history: list) -> None:
        try:
            import json
            d = os.path.dirname(path)
            if d:  # bare filename -> dirname is "" -> os.makedirs("") would raise; skip it
                os.makedirs(d, exist_ok=True)
            with open(path, "w") as fh:
                json.dump(history, fh)
        except Exception:
            pass  # a write failure must not block recovery (best-effort persistence)

    def _self_heal_governor_allows(self) -> bool:
        """True iff fewer than ``self_heal_max_per_window`` self-heal exits occurred within the
        window. Records this exit on success. Backed by a boot-clock (``_boot_clock``) so the
        history survives a container restart and is NTP-step-immune.

        The window prune is SYMMETRIC -- ``-window <= (now - t) <= window`` -- so a clock
        anomaly cannot silently erase the cap:
          * modest future-dated entries (a small backward clock step) are RETAINED (conservative);
          * far-future entries (``t > now + window``, e.g. boot-clock reset after a HOST reboot)
            are dropped, which correctly re-allows self-heal on a genuine fresh host boot;
          * genuinely old entries (``now - t > window``) age out as intended."""
        path = self._governor_path()
        now = self._wall_time_fn()
        window = self.self_heal_window_s
        recent = [t for t in self._load_governor_history(path) if -window <= (now - t) <= window]
        if len(recent) >= self.self_heal_max_per_window:
            return False
        recent.append(now)
        self._persist_governor_history(path, recent)
        return True

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
