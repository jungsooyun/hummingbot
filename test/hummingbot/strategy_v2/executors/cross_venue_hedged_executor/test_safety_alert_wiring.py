"""JEP-258 — safety-latch -> SafetyNotifier wiring (executor side).

Proves the executor emits ONE no-dep push at a latch trip, exactly once across latched
ticks, swallows any notifier error, and is behavior-neutral when the notifier is disabled.
The pnl_breaker path stands in for all five wired latches (they share _emit_safety_alert).
"""
import asyncio

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.safety_notifier import (
    SafetyNotifier,
)
from test.hummingbot.strategy_v2.executors.cross_venue_hedged_executor.test_pnl_breaker import (
    _PnlHarness,
)


class _RecordingNotifier:
    def __init__(self, raises: bool = False):
        self.calls = []
        self._raises = raises

    def notify(self, key, message):
        self.calls.append((key, message))
        if self._raises:
            raise RuntimeError("notifier boom")


def test_pnl_breaker_trip_emits_safety_alert():
    rec = _RecordingNotifier()
    SafetyNotifier.set_instance(rec)
    try:
        h = _PnlHarness(enabled=True, limit="100", action="hold", pnl="-150")
        asyncio.run(h.control_task())
        assert len(rec.calls) == 1
        key, msg = rec.calls[0]
        assert key.endswith(":pnl_breaker")
        assert "net_pnl_quote=-150" in msg
    finally:
        SafetyNotifier.set_instance(None)


def test_pnl_breaker_emits_once_across_latched_ticks():
    rec = _RecordingNotifier()
    SafetyNotifier.set_instance(rec)
    try:
        h = _PnlHarness(enabled=True, limit="100", action="hold", pnl="-150")
        asyncio.run(h.control_task())   # trips + emits
        asyncio.run(h.control_task())   # already latched -> no re-trip -> no second emit
        assert len(rec.calls) == 1
    finally:
        SafetyNotifier.set_instance(None)


def test_emit_swallows_notifier_errors():
    rec = _RecordingNotifier(raises=True)
    SafetyNotifier.set_instance(rec)
    try:
        h = _PnlHarness(enabled=True, limit="100", action="hold", pnl="-150")
        # A raising notifier must NOT propagate into the control loop.
        asyncio.run(h.control_task())
        assert len(rec.calls) == 1
        assert h._pnl_breaker_tripped is True  # the trip still completed
    finally:
        SafetyNotifier.set_instance(None)


def test_no_alert_when_no_trip():
    rec = _RecordingNotifier()
    SafetyNotifier.set_instance(rec)
    try:
        h = _PnlHarness(enabled=True, limit="100", action="hold", pnl="-50")  # above the limit
        asyncio.run(h.control_task())
        assert rec.calls == []
    finally:
        SafetyNotifier.set_instance(None)


def test_behavior_neutral_when_notifier_disabled():
    # A disabled (env-unconfigured) notifier makes the trip emit a silent no-op; trading proceeds.
    SafetyNotifier.set_instance(SafetyNotifier(env={}))
    try:
        h = _PnlHarness(enabled=True, limit="100", action="hold", pnl="-150")
        asyncio.run(h.control_task())
        assert h._pnl_breaker_tripped is True
    finally:
        SafetyNotifier.set_instance(None)
