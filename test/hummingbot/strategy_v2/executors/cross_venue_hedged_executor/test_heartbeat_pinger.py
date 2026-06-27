"""JEP-259 — external dead-man's switch heartbeat pinger (engine).

Mirrors the JEP-258 ``SafetyNotifier`` / JEP-184 ``LatencyRecorder`` daemon-sink: the control
loop calls ``record_beat()`` every tick (cheap, non-blocking, never raises); a daemon thread
pings a healthchecks.io-style URL OUTBOUND only -- the base URL while the last beat is fresh,
``<url>/fail`` once the beat has gone stale. No inbound is ever opened. A wedged loop (beats
stop -> /fail), a dead process / dead box (thread dies -> the pings stop) all surface to the
external watcher. Behavior-neutral: absent ``HEALTHCHECK_PING_URL`` the sink is disabled and
``record_beat`` is a silent no-op.
"""
import threading

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.heartbeat_pinger import (
    HeartbeatPinger,
    _http_get,
    fail_url,
)


class _Clock:
    """Controllable monotonic clock so freshness verdicts are deterministic (no sleeps)."""

    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t


# --------------------------------------------------------------- enable / disable (behavior-neutral)

def test_disabled_without_url_is_noop():
    p = HeartbeatPinger(env={}, _start_worker=False)
    assert p.enabled is False
    p.record_beat()       # must not raise
    p._tick_once()        # disabled -> no delivery attempt
    p.close()


def test_enabled_with_url():
    p = HeartbeatPinger(ping_url="https://hc-ping.com/uuid", _start_worker=False)
    assert p.enabled is True
    p.close()


def test_partial_is_disabled_when_url_blank():
    assert HeartbeatPinger(env={"HEALTHCHECK_PING_URL": ""}, _start_worker=False).enabled is False


# --------------------------------------------------------------- fresh vs stale routing

def test_fresh_beat_pings_base_url():
    sent = []
    clk = _Clock(100.0)
    p = HeartbeatPinger(
        transport=sent.append, ping_url="https://hc/uuid",
        freshness_s=120, clock=clk, _start_worker=False,
    )
    p.record_beat()       # beat at t=100
    clk.t = 150.0         # 50s later -> still fresh (<=120)
    p._tick_once()
    assert sent == ["https://hc/uuid"]
    p.close()


def test_stale_beat_pings_fail_url():
    sent = []
    clk = _Clock(100.0)
    p = HeartbeatPinger(
        transport=sent.append, ping_url="https://hc/uuid",
        freshness_s=120, clock=clk, _start_worker=False,
    )
    p.record_beat()       # beat at t=100
    clk.t = 300.0         # 200s later -> stale (>120) -> /fail
    p._tick_once()
    assert sent == ["https://hc/uuid/fail"]
    p.close()


def test_boot_hang_never_beats_goes_stale():
    # last_beat seeds to construction time, so a bot that never ticks goes stale -> /fail -> alert.
    sent = []
    clk = _Clock(0.0)
    p = HeartbeatPinger(
        transport=sent.append, ping_url="https://hc/uuid",
        freshness_s=120, clock=clk, _start_worker=False,
    )
    clk.t = 500.0         # control loop never recorded a beat
    p._tick_once()
    assert sent == ["https://hc/uuid/fail"]
    p.close()


def test_record_beat_refreshes_after_stale():
    sent = []
    clk = _Clock(0.0)
    p = HeartbeatPinger(
        transport=sent.append, ping_url="https://hc/uuid",
        freshness_s=100, clock=clk, _start_worker=False,
    )
    clk.t = 200.0
    p._tick_once()        # stale -> /fail
    p.record_beat()       # loop recovers, beats again at t=200
    clk.t = 250.0
    p._tick_once()        # fresh again -> base
    assert sent == ["https://hc/uuid/fail", "https://hc/uuid"]
    p.close()


# --------------------------------------------------------------- never-raise / behavior-neutral

def test_transport_error_swallowed():
    def boom(_url):
        raise RuntimeError("ping endpoint down")

    p = HeartbeatPinger(transport=boom, ping_url="https://hc/uuid", _start_worker=False)
    p.record_beat()       # must not raise
    p._tick_once()        # a delivery error must never propagate into the loop
    p.close()


def test_record_beat_disabled_is_noop():
    p = HeartbeatPinger(env={}, _start_worker=False)
    p.record_beat()
    p.record_beat()
    assert p.enabled is False


# --------------------------------------------------------------- url construction

def test_fail_url_construction():
    assert fail_url("https://hc-ping.com/uuid") == "https://hc-ping.com/uuid/fail"
    assert fail_url("https://hc-ping.com/uuid/") == "https://hc-ping.com/uuid/fail"  # trailing slash


# --------------------------------------------------------------- env tunables

def test_interval_freshness_from_env():
    p = HeartbeatPinger(
        env={"HEALTHCHECK_PING_URL": "https://hc/uuid",
             "HEALTHCHECK_INTERVAL_SECONDS": "45",
             "HEALTHCHECK_FRESHNESS_SECONDS": "200"},
        _start_worker=False,
    )
    assert p._interval == 45.0
    assert p._freshness == 200.0
    p.close()


def test_bad_env_tunables_fall_back_to_defaults():
    p = HeartbeatPinger(
        env={"HEALTHCHECK_PING_URL": "https://hc/uuid",
             "HEALTHCHECK_INTERVAL_SECONDS": "not-a-number",
             "HEALTHCHECK_FRESHNESS_SECONDS": "-5"},
        _start_worker=False,
    )
    assert p._interval > 0     # garbage -> default, never a crash or a non-positive cadence
    assert p._freshness > 0
    p.close()


# --------------------------------------------------------------- singleton + lifecycle

def test_get_instance_is_singleton():
    HeartbeatPinger.set_instance(None)
    try:
        a = HeartbeatPinger.get_instance()
        b = HeartbeatPinger.get_instance()
        assert a is b
    finally:
        HeartbeatPinger.set_instance(None)


def test_close_is_idempotent():
    p = HeartbeatPinger(ping_url="https://hc/uuid", _start_worker=False)
    p.close()
    p.close()             # second close must be a clean no-op


def test_disabled_pinger_starts_no_thread():
    # Behavior-neutral: an unconfigured pinger must not leak a daemon thread even on the real
    # (worker-starting) constructor path.
    p = HeartbeatPinger(env={})
    assert p.enabled is False
    assert p._thread is None
    p.close()


# --------------------------------------------------------------- live daemon thread (the mechanism)

def test_worker_thread_loops_and_stops_on_close():
    # JEP-259's CORE is the off-loop daemon that pings on a cadence -- exercise the REAL thread:
    # it must start (daemon), LOOP (>=3 pings, not one-shot), send the base URL while fresh, and
    # stop gracefully on close(). This is the failure mode the switch itself must not have.
    calls = []
    got_three = threading.Event()

    def transport(url):
        calls.append(url)
        if len(calls) >= 3:
            got_three.set()

    p = HeartbeatPinger(
        transport=transport, ping_url="https://hc/uuid", interval_s=0.01, freshness_s=1000,
    )
    try:
        assert p._thread is not None
        assert p._thread.daemon is True
        assert p._thread.is_alive() is True
        assert got_three.wait(timeout=5.0) is True   # the daemon LOOPS on its interval
        assert calls[0] == "https://hc/uuid"          # fresh beat -> base url
    finally:
        p.close()
    assert p._thread.is_alive() is False              # close() joins the thread (graceful stop)


# --------------------------------------------------------------- outbound transport scheme guard

def test_http_get_rejects_non_http_scheme(monkeypatch):
    calls = []

    class _Resp:
        def read(self):
            return b""

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout=None):
        calls.append(getattr(req, "full_url", req))
        return _Resp()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    _http_get("file:///etc/passwd")          # non-http scheme must be refused (no fetch), no raise
    assert calls == []
    _http_get("https://hc-ping.com/uuid")    # http(s) is fetched
    assert calls == ["https://hc-ping.com/uuid"]
