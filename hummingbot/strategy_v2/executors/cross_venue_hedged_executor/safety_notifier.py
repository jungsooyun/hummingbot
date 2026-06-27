# safety_notifier.py
"""No-dependency Telegram safety-push sink (JEP-258).

Mirrors the JEP-184 ``LatencyRecorder`` daemon-sink pattern: a caller enqueues a
``(key, message)`` via :meth:`SafetyNotifier.notify`, which NEVER blocks and NEVER raises;
a daemon thread delivers each message to Telegram via the stdlib (``http.client``) -- no
external dependency. ``latch-once`` dedup means each ``key`` is delivered at most once per
process, so a safety latch that re-asserts on every control tick still produces exactly one
push. Behavior-neutral: if ``TELEGRAM_TOKEN`` / ``ADMIN_USER_ID`` are absent the sink is
disabled (``notify`` is a silent no-op); any delivery error is swallowed.

The alerter must NEVER be able to stop or slow trading: Telegram is a DELIVERY SINK only,
the trip logic stays at the call site (the executor's latch points). This is intentionally
NOT the command/pull bot (Condor) and NOT the asyncio ``NotifierBase`` -- it runs OFF the
control loop on its own daemon thread so a wedged event loop or a slow HTTPS round-trip can
never back-pressure the trading path.
"""
import atexit
import http.client
import json
import os
import queue
import sys
import threading
import time
from typing import Callable, Optional, Tuple

_SENTINEL = object()  # stop signal for the worker thread
_TELEGRAM_HOST = "api.telegram.org"


def telegram_path(token: str) -> str:
    return f"/bot{token}/sendMessage"


def telegram_payload(chat_id: str, message: str) -> str:
    return json.dumps({"chat_id": chat_id, "text": message})


def _post_telegram(token: str, chat_id: str, message: str, *, timeout: float = 5.0) -> None:
    """One stdlib HTTPS POST to Telegram ``sendMessage``. Never raises (daemon-sink)."""
    conn = None
    try:
        conn = http.client.HTTPSConnection(_TELEGRAM_HOST, timeout=timeout)
        conn.request(
            "POST", telegram_path(token), telegram_payload(chat_id, message),
            {"Content-Type": "application/json"},
        )
        resp = conn.getresponse()
        resp.read()  # drain the response so the socket closes cleanly
        if not (200 <= resp.status < 300):
            # A misconfigured token/chat (silent failure) is the worst mode for an alerter, so
            # surface it -- STATUS CODE ONLY (never the token or message), off the control loop.
            try:
                sys.stderr.write(
                    f"safety_notifier: Telegram sendMessage returned HTTP {resp.status} "
                    "(check TELEGRAM_TOKEN / ADMIN_USER_ID)\n"
                )
            except Exception:
                pass
    except Exception:
        pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _telegram_transport_from_env(env=None) -> Tuple[Optional[Callable[[str], None]], bool]:
    """Build a Telegram transport from the environment, or ``(None, False)`` if unconfigured.

    Reads ``TELEGRAM_TOKEN`` and ``ADMIN_USER_ID`` (the deploy ``.env`` convention). Both
    must be present, else the sink stays disabled (a half-configured sink must not half-work).
    """
    env = os.environ if env is None else env
    token = env.get("TELEGRAM_TOKEN")
    chat_id = env.get("ADMIN_USER_ID")
    if not token or not chat_id:
        return None, False

    def transport(message: str) -> None:
        _post_telegram(token, chat_id, message)

    return transport, True


class SafetyNotifier:
    _instance: Optional["SafetyNotifier"] = None
    _instance_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "SafetyNotifier":
        """Process-wide singleton (env-configured, atexit-registered). Disabled+silent if
        the Telegram env vars are absent, so callers can wire ``notify`` unconditionally."""
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(register_atexit=True)
            return cls._instance

    @classmethod
    def set_instance(cls, inst: Optional["SafetyNotifier"]) -> None:
        """Test/bootstrap seam: install (or clear with ``None``) the process singleton. Closes
        the outgoing instance (best-effort) so swapping does not leak its worker thread."""
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
        queue_maxsize: int = 256,
        register_atexit: bool = False,
    ):
        self._dedup = set()
        self._lock = threading.Lock()
        self._queue: "queue.Queue" = queue.Queue(maxsize=queue_maxsize)
        self._closed = False
        if transport is not None:
            self._transport, self._enabled = transport, True  # test/explicit seam
        else:
            self._transport, self._enabled = _telegram_transport_from_env(env)
        if self._enabled:
            self._thread: Optional[threading.Thread] = threading.Thread(
                target=self._worker, name="safety_notifier", daemon=True
            )
            self._thread.start()
            if register_atexit:
                atexit.register(self.close)
        else:
            self._thread = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    def notify(self, key: str, message: str) -> None:
        """Enqueue a one-shot safety push for ``key`` (deduped, latch-once). Never blocks,
        never raises. Disabled sink / repeat key / saturated queue -> silently skipped."""
        try:
            if not self._enabled:
                return
            with self._lock:
                if key in self._dedup:
                    return
                self._dedup.add(key)
            try:
                self._queue.put_nowait(message)
            except queue.Full:
                # Drop-oldest so a stalled delivery never back-pressures the caller.
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                except queue.Empty:
                    pass
                try:
                    self._queue.put_nowait(message)
                except queue.Full:
                    pass
        except Exception:
            pass  # the alerter must never raise into the trading path

    def _worker(self) -> None:
        while True:
            msg = self._queue.get()
            if msg is _SENTINEL:
                self._queue.task_done()
                break
            try:
                self._transport(msg)
            except Exception:
                pass  # a delivery failure must not kill the worker
            finally:
                self._queue.task_done()

    def reset(self, key: Optional[str] = None) -> None:
        """Re-arm: forget ``key`` (or all keys) so a later trip can push again."""
        with self._lock:
            if key is None:
                self._dedup.clear()
            else:
                self._dedup.discard(key)

    def flush(self, timeout: float = 2.0) -> bool:
        """Block until the queue drains (test / graceful-shutdown aid). ``queue.join`` has no
        timeout, so poll ``unfinished_tasks`` against a deadline. True if drained in time."""
        if self._thread is None:
            return True
        deadline = time.monotonic() + timeout
        while self._queue.unfinished_tasks > 0:
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.005)
        return True

    def close(self) -> None:
        with self._lock:  # idempotent under a race (on_stop + atexit may both call)
            if self._closed:
                return
            self._closed = True
        try:
            atexit.unregister(self.close)
        except Exception:
            pass
        if self._thread is None:
            return
        # Guarantee the sentinel is enqueued even if the queue is saturated (drop-oldest).
        try:
            self._queue.put_nowait(_SENTINEL)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(_SENTINEL)
            except queue.Full:
                pass
        self._thread.join(timeout=2.0)
