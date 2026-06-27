# heartbeat_pinger.py
"""External dead-man's switch heartbeat (JEP-259).

Mirrors the JEP-258 ``SafetyNotifier`` / JEP-184 ``LatencyRecorder`` daemon-sink pattern. The
control loop calls :meth:`HeartbeatPinger.record_beat` every RUNNING tick -- a cheap, lock-free
timestamp stamp that NEVER blocks and NEVER raises. A daemon thread, OFF the control loop, pings
a healthchecks.io-style URL OUTBOUND only: the base URL while the last beat is fresh, and
``<url>/fail`` once the beat has gone stale.

This is a *dead-man's switch*: the external watcher (healthchecks.io, or a self-hosted clone)
alerts on the ABSENCE of fresh pings, so it covers the exact failure modes an in-process pusher
(JEP-258) cannot -- a wedged event loop (beats stop -> we send ``/fail``), a SIGKILL'd container
or a dead box (the daemon thread dies -> no ping at all -> the watcher's grace lapses -> alert).

Crucially this opens NO inbound: the bot only makes an outbound HTTPS GET, so no port, firewall
rule, or security-group change is required. The ping URL embeds a secret token, so it is read
from the environment (``HEALTHCHECK_PING_URL``) like any credential, never logged. Behavior-
neutral: if the URL is absent the sink is disabled and ``record_beat`` is a silent no-op, so the
call site can wire it unconditionally.
"""
import atexit
import os
import threading
import time
import urllib.request
from typing import Callable, Optional, Tuple

_DEFAULT_INTERVAL_S = 30.0   # how often the daemon pings while the bot is healthy
_DEFAULT_FRESHNESS_S = 120.0  # a beat older than this is "stale" -> ping <url>/fail


def fail_url(url: str) -> str:
    """healthchecks.io failure signal: ``<ping-url>/fail`` (trailing slash tolerated)."""
    return url.rstrip("/") + "/fail"


def _http_get(url: str, *, timeout: float = 5.0) -> None:
    """One stdlib OUTBOUND GET. Refuses non-http(s) schemes; never raises (daemon-sink)."""
    try:
        if not (url.startswith("https://") or url.startswith("http://")):
            return  # only http(s) -- never file://, etc., even on a misconfigured URL
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310 - http(s) only
            resp.read()  # drain so the socket closes cleanly
    except Exception:
        pass


def _ping_transport_from_env(env=None) -> Tuple[Optional[Callable[[str], None]], bool]:
    """Build an outbound-GET transport from ``HEALTHCHECK_PING_URL``, or ``(None, False)``.

    The transport takes the FULL target URL to GET (base or ``/fail``); the freshness verdict is
    the pinger's, not the transport's. Absent/blank URL -> disabled (a half-wired sink must not
    half-work)."""
    env = os.environ if env is None else env
    url = env.get("HEALTHCHECK_PING_URL")
    if not url:
        return None, False
    return (lambda target: _http_get(target)), True


def _read_positive_float_env(env, key: str, default: float) -> float:
    """Read a positive float from ``env[key]``; any missing/garbage/non-positive value -> default
    (a bad tunable must degrade to a safe cadence, never crash the pinger or stop the heartbeat)."""
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return default
    return val if val > 0 else default


class HeartbeatPinger:
    _instance: Optional["HeartbeatPinger"] = None
    _instance_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "HeartbeatPinger":
        """Process-wide singleton (env-configured, atexit-registered). Disabled+silent if
        ``HEALTHCHECK_PING_URL`` is absent, so callers can stamp ``record_beat`` unconditionally."""
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(register_atexit=True)
            return cls._instance

    @classmethod
    def set_instance(cls, inst: Optional["HeartbeatPinger"]) -> None:
        """Test/bootstrap seam: install (or clear with ``None``) the process singleton. Closes the
        outgoing instance (best-effort) so swapping does not leak its daemon thread."""
        with cls._instance_lock:
            prev = cls._instance
            cls._instance = inst
        if prev is not None and prev is not inst:
            try:
                prev.close()
            except Exception:
                pass

    def __init__(
        self,
        transport: Optional[Callable[[str], None]] = None,
        *,
        env=None,
        ping_url: Optional[str] = None,
        interval_s: Optional[float] = None,
        freshness_s: Optional[float] = None,
        clock: Optional[Callable[[], float]] = None,
        register_atexit: bool = False,
        _start_worker: bool = True,
    ):
        env = os.environ if env is None else env
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._closed = False
        self._stop_event = threading.Event()
        # Seed last_beat to "now" so a freshly booted bot gets its boot grace before the first
        # staleness verdict; a bot that then NEVER ticks goes stale on its own -> /fail -> alert.
        self._last_beat = self._clock()
        self._interval = (
            interval_s if interval_s is not None
            else _read_positive_float_env(env, "HEALTHCHECK_INTERVAL_SECONDS", _DEFAULT_INTERVAL_S)
        )
        self._freshness = (
            freshness_s if freshness_s is not None
            else _read_positive_float_env(env, "HEALTHCHECK_FRESHNESS_SECONDS", _DEFAULT_FRESHNESS_S)
        )
        if transport is not None:
            self._transport, self._enabled = transport, True  # test/explicit seam
            self._url = ping_url or ""
        elif ping_url:
            self._url, self._enabled = ping_url, True
            self._transport = lambda target: _http_get(target)
        else:
            self._transport, self._enabled = _ping_transport_from_env(env)
            self._url = (env.get("HEALTHCHECK_PING_URL") or "")

        if self._enabled and _start_worker:
            self._thread: Optional[threading.Thread] = threading.Thread(
                target=self._worker, name="heartbeat_pinger", daemon=True
            )
            self._thread.start()
            if register_atexit:
                atexit.register(self.close)
        else:
            self._thread = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    def record_beat(self) -> None:
        """Stamp the dead-man heartbeat (called every RUNNING control tick). A plain float store
        under the GIL -- lock-free, never blocks, never raises."""
        try:
            if not self._enabled:
                return
            self._last_beat = self._clock()
        except Exception:
            pass

    def _is_fresh(self) -> bool:
        return (self._clock() - self._last_beat) <= self._freshness

    def _tick_once(self) -> None:
        """One daemon iteration: ping the base URL when the heartbeat is fresh, else ``<url>/fail``.
        Never raises (a delivery error must not kill the worker)."""
        if not self._enabled:
            return
        target = self._url if self._is_fresh() else fail_url(self._url)
        try:
            self._transport(target)
        except Exception:
            pass

    def _worker(self) -> None:
        # Event.wait returns True only when close() sets it -> interruptible sleep + clean exit.
        while not self._stop_event.wait(self._interval):
            self._tick_once()

    def close(self) -> None:
        with self._lock:  # idempotent under a race (on_stop + atexit may both call)
            if self._closed:
                return
            self._closed = True
        try:
            atexit.unregister(self.close)
        except Exception:
            pass
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
