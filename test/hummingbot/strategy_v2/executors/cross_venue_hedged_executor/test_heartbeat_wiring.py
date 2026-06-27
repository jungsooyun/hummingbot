"""JEP-259 — control_task -> HeartbeatPinger.record_beat wiring (executor side).

Proves the REAL control_task stamps the dead-man heartbeat on every RUNNING tick (incl. the
breaker early-return path, since the stamp is taken BEFORE the gates), swallows any pinger
error, and is behavior-neutral when the pinger is disabled. The pnl-breaker harness drives the
real control loop (its other collaborators are counting stubs).
"""
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.heartbeat_pinger import (
    HeartbeatPinger,
)
from test.hummingbot.strategy_v2.executors.cross_venue_hedged_executor.test_pnl_breaker import (
    _PnlHarness,
    _run,
)


class _RecordingPinger:
    def __init__(self, raises: bool = False):
        self.beats = 0
        self._raises = raises

    def record_beat(self):
        self.beats += 1
        if self._raises:
            raise RuntimeError("pinger boom")


def test_running_tick_records_heartbeat():
    rec = _RecordingPinger()
    HeartbeatPinger.set_instance(rec)
    try:
        h = _PnlHarness(enabled=False, pnl="0")   # no breaker -> full happy tick
        _run(h)
        assert rec.beats == 1
    finally:
        HeartbeatPinger.set_instance(None)


def test_heartbeat_recorded_even_when_breaker_trips_early_return():
    # The stamp is taken at the TOP of the RUNNING branch, BEFORE the gates / breaker, so a tick
    # that trips the breaker and early-returns STILL proves the loop is cycling.
    rec = _RecordingPinger()
    HeartbeatPinger.set_instance(rec)
    try:
        h = _PnlHarness(enabled=True, limit="100", action="hold", pnl="-150")
        _run(h)
        assert h._pnl_breaker_tripped is True
        assert rec.beats == 1
    finally:
        HeartbeatPinger.set_instance(None)


def test_heartbeat_recorded_each_tick():
    rec = _RecordingPinger()
    HeartbeatPinger.set_instance(rec)
    try:
        h = _PnlHarness(enabled=False, pnl="0")
        _run(h)
        _run(h)
        _run(h)
        assert rec.beats == 3
    finally:
        HeartbeatPinger.set_instance(None)


def test_record_swallows_pinger_errors():
    rec = _RecordingPinger(raises=True)
    HeartbeatPinger.set_instance(rec)
    try:
        h = _PnlHarness(enabled=False, pnl="0")
        _run(h)                                   # a raising pinger must NOT break the control loop
        assert rec.beats == 1
        assert h.reconcile_calls == 1             # the tick still completed normally
        assert h.hedge_calls == 1
    finally:
        HeartbeatPinger.set_instance(None)


def test_behavior_neutral_when_pinger_disabled():
    HeartbeatPinger.set_instance(HeartbeatPinger(env={}, _start_worker=False))
    try:
        h = _PnlHarness(enabled=False, pnl="0")
        _run(h)                                   # disabled pinger -> trading proceeds untouched
        assert h.reconcile_calls == 1
        assert h.hedge_calls == 1
    finally:
        HeartbeatPinger.set_instance(None)
