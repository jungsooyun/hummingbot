import pytest
from decimal import Decimal  # noqa: F401
from unittest.mock import MagicMock

import hummingbot.strategy_v2.controllers.ladder_hedge_controller_base as ladder_mod
from hummingbot.strategy_v2.controllers.ladder_hedge_controller_base import LadderHedgeControllerBase
from hummingbot.strategy_v2.models.executors import CloseType


@pytest.fixture(autouse=True)
def _stub_create_executor_action(monkeypatch):
    # Keep production CreateExecutorAction (pydantic-validating) untouched; in tests we only
    # care whether determine_executor_actions returns a create (len 1) or pauses ([]), so a
    # lightweight stub avoids validating the MagicMock executor_config.
    monkeypatch.setattr(ladder_mod, "CreateExecutorAction",
                        lambda **kwargs: {"create": True, **kwargs})


class _Ctl(LadderHedgeControllerBase):
    def _build_executor_config(self):
        return MagicMock(name="executor_config")


def _ctl():
    c = _Ctl.__new__(_Ctl)          # bypass framework __init__
    c.config = MagicMock()
    c.config.max_executors = 1
    c.config.id = "ctl-1"
    c.executors_info = []
    c._clock = {"t": 1_700_000_000.0}     # epoch-like clock (0.0 would mask the epoch bug)
    c._now = lambda: c._clock["t"]
    return c


def _done(eid, close_type, ts):
    e = MagicMock()
    e.id = eid; e.is_done = True; e.close_type = close_type; e.close_timestamp = ts; e.timestamp = ts
    return e


def test_breaker_latches_after_threshold_consecutive_ib():
    c = _ctl()
    for i in range(c._IB_BREAKER_THRESHOLD):
        c.executors_info = [_done(f"e{i}", CloseType.INSUFFICIENT_BALANCE, float(i))]
        c.determine_executor_actions()
    assert c._consecutive_ib >= c._IB_BREAKER_THRESHOLD
    c.executors_info = []
    assert c.determine_executor_actions() == []


def test_non_ib_done_resets_counter_and_resumes():
    c = _ctl()
    for i in range(c._IB_BREAKER_THRESHOLD):
        c.executors_info = [_done(f"e{i}", CloseType.INSUFFICIENT_BALANCE, float(i))]
        c.determine_executor_actions()
    c.executors_info = [_done("ok", CloseType.COMPLETED, 100.0)]
    c.determine_executor_actions()
    assert c._consecutive_ib == 0
    c.executors_info = []
    assert len(c.determine_executor_actions()) == 1


def test_first_latch_from_preexisting_dones_holds():
    c = _ctl()
    c.executors_info = [_done(f"pre{i}", CloseType.INSUFFICIENT_BALANCE, float(i))
                        for i in range(c._IB_BREAKER_THRESHOLD)]
    actions = c.determine_executor_actions()
    assert c._consecutive_ib >= c._IB_BREAKER_THRESHOLD
    assert actions == []


def test_latched_probe_allows_exactly_one_create_per_interval():
    c = _ctl()
    for i in range(c._IB_BREAKER_THRESHOLD):
        c.executors_info = [_done(f"e{i}", CloseType.INSUFFICIENT_BALANCE, float(i))]
        c.determine_executor_actions()
    c.executors_info = []
    c._clock["t"] += c._IB_BREAKER_PROBE_INTERVAL_S + 1.0
    assert len(c.determine_executor_actions()) == 1
    assert c.determine_executor_actions() == []
    c._clock["t"] += c._IB_BREAKER_PROBE_INTERVAL_S + 1.0
    assert len(c.determine_executor_actions()) == 1


def test_mixed_batch_evaluated_in_close_timestamp_order():
    c = _ctl()
    c.executors_info = [_done("ib", CloseType.INSUFFICIENT_BALANCE, 1.0),
                        _done("ok", CloseType.COMPLETED, 2.0)]
    c.determine_executor_actions()
    assert c._consecutive_ib == 0


def test_seen_ids_pruned_no_recount():
    c = _ctl()
    c.executors_info = [_done("e0", CloseType.INSUFFICIENT_BALANCE, 0.0)]
    c.determine_executor_actions()
    assert c._consecutive_ib == 1
    c.determine_executor_actions()
    assert c._consecutive_ib == 1
