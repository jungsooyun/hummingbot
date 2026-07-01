import pytest
from decimal import Decimal  # noqa: F401
from unittest.mock import MagicMock

import hummingbot.strategy_v2.controllers.ladder_hedge_controller_base as ladder_mod
from hummingbot.strategy_v2.controllers.ladder_hedge_controller_base import LadderHedgeControllerBase
from hummingbot.strategy_v2.models.base import RunnableStatus
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
    c = _Ctl.__new__(_Ctl)              # bypass framework __init__
    c.config = MagicMock()
    c.config.max_executors = 1
    c.config.id = "ctl-1"
    # A MagicMock attribute is truthy + un-coercible, so EVERY breaker field must be set explicitly.
    c.config.ib_breaker_enabled = True  # ARM for enforcement tests (ships OFF; see OFF-default test)
    c.config.ib_breaker_threshold = 10
    c.config.ib_breaker_window_s = 60.0
    c.config.ib_breaker_probe_base_s = 15.0
    c.config.ib_breaker_probe_max_s = 300.0
    c.executors_info = []
    c._clock = {"t": 1_700_000_000.0}   # epoch-like clock (0.0 would mask the epoch-init bug)
    c._now = lambda: c._clock["t"]
    return c


def _ib_done(c, eid):
    # IB-terminated executor stamped at the CURRENT clock (window prunes by now - window_s, so a
    # ~0 timestamp like the old float(i) would prune instantly and never trigger).
    e = MagicMock()
    e.id = eid; e.is_done = True; e.close_type = CloseType.INSUFFICIENT_BALANCE
    e.close_timestamp = c._now(); e.timestamp = c._now(); e.status = RunnableStatus.TERMINATED
    return e


def _done(eid, close_type, ts):
    e = MagicMock()
    e.id = eid; e.is_done = True; e.close_type = close_type
    e.close_timestamp = ts; e.timestamp = ts; e.status = RunnableStatus.TERMINATED
    return e


def _running(eid):
    e = MagicMock()
    e.id = eid; e.is_done = False; e.status = RunnableStatus.RUNNING
    return e


def _latch(c, n=None):
    # Drive n IB terminations at the current clock to trip the latch. Uses distinct ids each tick.
    n = c.config.ib_breaker_threshold if n is None else n
    for i in range(n):
        c.executors_info = [_ib_done(c, f"e{i}")]
        c.determine_executor_actions()


def test_config_fields_default_off_and_tunable():
    from hummingbot.strategy_v2.controllers.ladder_hedge_controller_base import (
        LadderHedgeControllerConfigBase,
    )
    f = LadderHedgeControllerConfigBase.model_fields
    assert f["ib_breaker_enabled"].default is False          # ships OFF (read the pydantic default)
    assert f["ib_breaker_threshold"].default == 10
    assert f["ib_breaker_window_s"].default == 60.0
    assert f["ib_breaker_probe_base_s"].default == 15.0
    assert f["ib_breaker_probe_max_s"].default == 300.0
    for name in ("ib_breaker_enabled", "ib_breaker_threshold", "ib_breaker_window_s",
                 "ib_breaker_probe_base_s", "ib_breaker_probe_max_s"):
        assert f[name].json_schema_extra == {"is_updatable": True}


def test_accessors_coerce_malformed_config_without_crashing():
    c = _ctl()
    c.config.ib_breaker_threshold = 0            # < 1 -> default 10
    c.config.ib_breaker_window_s = -5.0          # <= 0 -> default 60.0
    c.config.ib_breaker_probe_base_s = "oops"    # non-numeric -> default 15.0
    c.config.ib_breaker_probe_max_s = 5.0        # < base -> max(base, 300.0)
    assert c._ib_threshold() == 10
    assert c._ib_window_s() == 60.0
    assert c._ib_probe_base() == 15.0
    assert c._ib_probe_max() == 300.0
    c.config.ib_breaker_probe_base_s = 40.0      # valid base
    c.config.ib_breaker_probe_max_s = 20.0       # < base -> max(40, 300)
    assert c._ib_probe_max() == 300.0


def test_windowed_trigger_latches_then_holds():
    c = _ctl()
    _latch(c)                                   # threshold IBs at the current clock
    assert c._ib_latched is True
    assert len(c._ib_times) >= c.config.ib_breaker_threshold
    c.executors_info = []
    assert c.determine_executor_actions() == []  # held (first pause = base, clock not advanced)


def test_windowed_aging_below_threshold_does_not_latch():
    c = _ctl()
    _latch(c, n=c.config.ib_breaker_threshold - 1)   # threshold-1 IBs
    c._clock["t"] += c.config.ib_breaker_window_s + 1.0   # age them all out
    c.executors_info = [_ib_done(c, "late")]         # +1 IB, but the old ones pruned
    c.determine_executor_actions()
    assert c._ib_latched is False
    assert len(c._ib_times) == 1


def test_non_ib_death_is_ignored_not_reset():
    c = _ctl()
    c.executors_info = [_ib_done(c, "ib0")]
    c.determine_executor_actions()
    c.executors_info = [_done("ok", CloseType.COMPLETED, c._now())]  # non-IB: ignored, not a reset
    c.determine_executor_actions()
    assert len(c._ib_times) == 1                     # still 1 (COMPLETED neither added nor reset)


def test_seen_ids_pruned_no_recount():
    c = _ctl()
    c.executors_info = [_ib_done(c, "e0")]
    c.determine_executor_actions()
    assert len(c._ib_times) == 1
    c.determine_executor_actions()                   # same done still reported
    assert len(c._ib_times) == 1                     # not recounted


def test_first_latch_from_preexisting_dones_holds():
    c = _ctl()
    c.executors_info = [_ib_done(c, f"pre{i}") for i in range(c.config.ib_breaker_threshold)]
    actions = c.determine_executor_actions()         # first-ever call
    assert c._ib_latched is True
    assert actions == []                             # _ib_last_probe_ts lazy-inits to _now(), so it HOLDS


def test_transient_first_pause_is_base_not_max():
    c = _ctl()
    _latch(c)
    c.executors_info = []
    assert c.determine_executor_actions() == []      # base pause not yet elapsed
    c._clock["t"] += c.config.ib_breaker_probe_base_s + 1.0
    assert len(c.determine_executor_actions()) == 1  # exactly one probe at +base (NOT +max/300s)


def test_backoff_escalates_across_window_aging():
    # BLOCKING-1 regression: sustained failure where the original burst ages out of the window must
    # STILL hold (latch persists) and the pause must grow base -> 2x -> ... -> cap.
    c = _ctl()
    _latch(c)
    base = c.config.ib_breaker_probe_base_s
    cap = c.config.ib_breaker_probe_max_s
    seen_pauses = []
    for step in range(6):
        pause = min(base * 2 ** c._probe_fail_count, cap)
        seen_pauses.append(pause)
        c._clock["t"] += pause + 1.0                 # let this probe fire
        c.executors_info = []
        acted = c.determine_executor_actions()       # probe due -> one create, probe_fail++
        assert len(acted) == 1
        # the probe immediately IB-dies (sustained failure) before surviving two ticks:
        c.executors_info = [_ib_done(c, f"probe{step}")]
        c.determine_executor_actions()
    assert c._ib_latched is True                     # window aged out, but latch persists
    assert seen_pauses[0] == base and seen_pauses[1] == base * 2
    assert seen_pauses[-1] == cap                    # escalated to cap


def test_log_rate_limited_and_never_raises(monkeypatch):
    # BLOCKING-2 regression: many held ticks -> at most one warning per _IB_BREAKER_ALERT_INTERVAL_S,
    # and no crash from a missing config field for the alert interval. Install the capturing logger
    # BEFORE _latch: the latch trips on the threshold-th tick without the clock advancing, so that
    # tick is itself the first HELD tick and emits the single warning; the 20 further held ticks are
    # all inside the same 300s log window and add none.
    c = _ctl()
    warnings = []
    monkeypatch.setattr(c, "logger", lambda: MagicMock(warning=lambda *a, **k: warnings.append(a)))
    _latch(c)
    c.executors_info = []
    for _ in range(20):
        c._clock["t"] += 0.5                          # 20 * 0.5 = +10s < base 15s -> every tick HOLDs
        c.determine_executor_actions()
    assert len(warnings) == 1                          # one warning per 300s window despite 21 held ticks


def test_success_reset_survives_two_ticks():
    c = _ctl()
    _latch(c)
    assert c._ib_latched is True
    surv = _running("probe")
    c.executors_info = [surv]                          # tick 1: RUNNING (also snapshots prev_running)
    c.determine_executor_actions()
    c.executors_info = [surv]                          # tick 2: RUNNING on two consecutive ticks
    c.determine_executor_actions()
    assert c._ib_latched is False
    assert len(c._ib_times) == 0
    assert c._probe_fail_count == 0
    c.executors_info = [_ib_done(c, "again")]          # a later IB now counts from 1
    c.determine_executor_actions()
    assert len(c._ib_times) == 1


def test_zero_pnl_running_still_clears_latch():
    # The survivor is RUNNING + is_active but is_trading==False (no fill). It MUST still clear the
    # latch across two ticks — proving the signal is RUNNING-survival, not is_trading.
    c = _ctl()
    _latch(c)
    surv = _running("probe"); surv.is_trading = False; surv.is_active = True; surv.net_pnl_quote = 0
    c.executors_info = [surv]; c.determine_executor_actions()
    c.executors_info = [surv]; c.determine_executor_actions()
    assert c._ib_latched is False


def test_single_tick_running_then_ib_does_not_reset():
    # The doomed-probe race: RUNNING one tick, TERMINATED(IB) the next -> never RUNNING on two ticks.
    c = _ctl()
    _latch(c)
    c.executors_info = [_running("doomed")]            # tick 1: briefly RUNNING (start() pre-validate)
    c.determine_executor_actions()
    c.executors_info = [_ib_done(c, "doomed")]         # tick 2: same id IB-terminated
    c.determine_executor_actions()
    assert c._ib_latched is True                       # not cleared


def test_transient_false_latch_heals_within_base_pause():
    # Full false-latch defense: latch, one probe at +base, the probe survives two ticks -> un-latched,
    # and NO 300s halt ever occurred.
    c = _ctl()
    _latch(c)
    c._clock["t"] += c.config.ib_breaker_probe_base_s + 1.0
    c.executors_info = []
    acted = c.determine_executor_actions()             # probe emitted
    assert len(acted) == 1
    surv = _running("probe")
    c.executors_info = [surv]; c.determine_executor_actions()   # tick 1 RUNNING
    c.executors_info = [surv]; c.determine_executor_actions()   # tick 2 RUNNING -> reset
    assert c._ib_latched is False
    assert c._probe_fail_count == 0


def test_disable_releases_active_hold():
    c = _ctl()
    _latch(c)
    assert c._ib_latched is True
    c.config.ib_breaker_enabled = False                # operator disables mid-latch
    c.executors_info = []
    assert len(c.determine_executor_actions()) == 1    # hold released next tick, creation resumes
    assert c._ib_latched is False
    c.executors_info = [_ib_done(c, "still")]           # counting still updates while disabled
    c.determine_executor_actions()
    assert len(c._ib_times) >= 1


class _FakeNotifier:
    # Model SafetyNotifier's process-permanent key dedup so the test proves the CONTROLLER's key
    # contract (once-per-episode via the epoch key), not the fake's. `calls` = unique keys, in order.
    def __init__(self): self.calls = []; self._seen = set()
    def notify(self, key, message):
        if key in self._seen:
            return
        self._seen.add(key); self.calls.append(key)


@pytest.fixture(autouse=True)
def fake_notifier():
    # autouse: EVERY test in this module installs a silent fake, so a latch that fires _emit_ib_alert
    # can never construct the real (atexit-registered, network-capable) SafetyNotifier singleton.
    from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.safety_notifier import (
        SafetyNotifier,
    )
    fake = _FakeNotifier()
    SafetyNotifier.set_instance(fake)
    yield fake
    SafetyNotifier.set_instance(None)


def test_safetynotifier_latch_recovery_and_relatch(fake_notifier):
    c = _ctl()
    _latch(c)
    c.executors_info = []
    c.determine_executor_actions()                     # held -> latched notify (epoch 0), once
    c.determine_executor_actions()
    latched0 = [k for k in fake_notifier.calls if k == "ctl-1:ib_breaker_latched:0"]
    assert len(latched0) == 1                          # deduped within the episode
    # recover
    surv = _running("probe")
    c.executors_info = [surv]; c.determine_executor_actions()
    c.executors_info = [surv]; c.determine_executor_actions()
    assert "ctl-1:ib_breaker_recovered:0" in fake_notifier.calls
    assert c._ib_latch_epoch == 1
    # re-latch -> alerts again under the NEW epoch key
    _latch(c)
    c.executors_info = []
    c.determine_executor_actions()
    assert "ctl-1:ib_breaker_latched:1" in fake_notifier.calls


def test_safetynotifier_silent_while_disabled(fake_notifier):
    c = _ctl()
    c.config.ib_breaker_enabled = False
    for i in range(c.config.ib_breaker_threshold + 3):
        c.executors_info = [_ib_done(c, f"e{i}")]
        c.determine_executor_actions()
    assert fake_notifier.calls == []                   # counts only, never latches -> never notifies


def test_ships_off_by_default_observes_without_pausing():
    c = _ctl()
    c.config.ib_breaker_enabled = False                # class default is False (asserted in Task 2)
    for i in range(c.config.ib_breaker_threshold + 3):
        c.executors_info = [_ib_done(c, f"e{i}")]
        assert len(c.determine_executor_actions()) == 1  # never pauses while disabled
    assert len(c._ib_times) >= c.config.ib_breaker_threshold  # but still counts (observability)
    assert c._ib_latched is False


# ---- R1 hardening (Codex adversarial challenge) ---------------------------------------------------

def test_enable_coercion_false_like_values_stay_off():
    # Challenge finding 1: a naive bool() would arm the breaker on any truthy junk (a "ships OFF,
    # behavior-neutral" flag must default to OFF on ambiguous input). False-like strings, None, NaN,
    # and non-1 numbers stay OFF; only real True or a known true-string arms it.
    c = _ctl()
    for bad in ("false", "False", "0", "no", "off", "", None, float("nan"), -1, 0, 2):
        c.config.ib_breaker_enabled = bad
        assert c._ib_breaker_enabled() is False, bad
    for good in (True, "true", "True", "1", "yes", "on", 1):
        c.config.ib_breaker_enabled = good
        assert c._ib_breaker_enabled() is True, good
    # a malformed false-like enable must NOT latch/pause a churning maker (stays behavior-neutral)
    c.config.ib_breaker_enabled = "false"
    for i in range(c.config.ib_breaker_threshold + 2):
        c.executors_info = [_ib_done(c, f"e{i}")]
        assert len(c.determine_executor_actions()) == 1
    assert c._ib_latched is False


def test_backoff_saturates_without_overflow():
    # Challenge finding 2: base * 2**_probe_fail_count OverflowErrors (~fail>=1024) inside the per-tick
    # loop. A pathological sustained-failure count must saturate at max, never raise.
    c = _ctl()
    _latch(c)
    c._probe_fail_count = 5000                          # ~days of capped probes
    c.executors_info = []
    assert c.determine_executor_actions() == []         # held; pause computed without OverflowError
    assert c._ib_pause() == c.config.ib_breaker_probe_max_s


def test_accessors_reject_non_finite_and_bool():
    # Challenge finding 3: inf/NaN/bool slip through coercion -> degenerate live behavior (window=inf
    # never prunes; base=inf never probes; threshold=True==1 latches on a single IB).
    c = _ctl()
    c.config.ib_breaker_window_s = float("inf")
    assert c._ib_window_s() == 60.0
    c.config.ib_breaker_window_s = float("nan")
    assert c._ib_window_s() == 60.0
    c.config.ib_breaker_probe_base_s = float("inf")
    assert c._ib_probe_base() == 15.0
    c.config.ib_breaker_probe_max_s = float("inf")
    assert c._ib_probe_max() == 300.0
    c.config.ib_breaker_threshold = float("inf")        # int(inf) raises OverflowError
    assert c._ib_threshold() == 10
    c.config.ib_breaker_threshold = True                # bool -> default (avoid threshold==1)
    assert c._ib_threshold() == 10


def test_future_close_timestamp_does_not_block_pruning():
    # Challenge finding 5a: an out-of-order/future close_timestamp must not wedge left-edge pruning.
    # Counting at OBSERVATION time (now, monotonic across ticks) keeps the window prune-able.
    c = _ctl()
    fut = _ib_done(c, "future"); fut.close_timestamp = c._now() + 10_000.0  # bogus far-future close ts
    c.executors_info = [fut]
    c.determine_executor_actions()                      # observed at T
    assert len(c._ib_times) == 1
    c._clock["t"] += c.config.ib_breaker_window_s + 1.0  # advance past the window
    c.executors_info = []
    c.determine_executor_actions()
    assert len(c._ib_times) == 0                         # aged out by observation time, not blocked


def test_prune_open_left_at_exact_boundary():
    # Challenge finding 7: prune must match the documented open-left window (now - window_s, now] —
    # an entry exactly at now - window_s ages out.
    c = _ctl()
    c.executors_info = [_ib_done(c, "edge")]
    c.determine_executor_actions()                      # observed at T
    assert len(c._ib_times) == 1
    c._clock["t"] += c.config.ib_breaker_window_s       # now == T + window -> cutoff == T
    c.executors_info = []
    c.determine_executor_actions()
    assert len(c._ib_times) == 0                         # entry exactly at cutoff pruned (open-left)
