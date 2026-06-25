"""JEP-221 #2: HARD per-boot order-rate circuit breaker.

A general backstop that trips on placement RATE: more than ``max_placements_per_window``
real maker placements within ``placement_rate_window_s`` latches a per-boot HALT — cancel
all resting makers and suppress all further placement (maker + hedge) until the process
restarts. The JEP-218 clock watchdog only catches a FROZEN clock; this catches the opposite
failure (a too-fast placement storm) the watchdog cannot see.

NOTE (2026-06-26 correction): the 2026-06-25 NXT flip that motivated this was first read as a
"~435 placements/72s (~6/s) churn from dropped unknown-order fills", but live logs FALSIFIED
that — it was correct ``cap < Q`` close-only REDUCE at ~0.4/s (28 fills/~67s), not a bug, and
this breaker (default 80/30s = 2.67/s) would NOT have tripped on it. The breaker's real value
is a genuine multi-rung per-tick reprice storm (> 2.67/s), e.g. ``_should_reprice`` bypassing
``min_reprice_interval_s`` on an empty book at a sub-second tick. Default-on; the ~1/s tick
floor keeps normal 2-symbol operation far under the trip threshold (no false trips).
"""
from decimal import Decimal
from types import SimpleNamespace

from hummingbot.core.data_type.common import TradeType
from test.hummingbot.strategy_v2.executors.cross_venue_hedged_executor.test_hedge_residual import (
    _Cx,
    _Harness,
    _Tracked,
)


def _breaker_harness(enabled=True, window=20.0, cap=5):
    h = _Harness()
    h.config = SimpleNamespace(
        placement_rate_breaker_enabled=enabled,
        placement_rate_window_s=window,
        max_placements_per_window=cap,
    )
    h._strategy.current_timestamp = 1000.0
    return h


def _open_maker(h, oid):
    tr = _Tracked(oid)
    cx = _Cx(1)
    cx.is_open = True
    tr.order = cx
    h.maker_orders[oid] = tr
    return tr


def test_breaker_trips_on_burst_within_window():
    h = _breaker_harness(cap=5, window=20.0)
    _open_maker(h, "m0")  # a resting maker to cancel on trip
    for _ in range(6):  # 6 > cap 5
        h._note_maker_placement()
    assert h._rate_halted is True
    # trip cancels all resting makers
    h._strategy.cancel.assert_any_call(h.maker_connector, h.maker_trading_pair, "m0")


def test_breaker_not_tripped_at_or_under_cap():
    h = _breaker_harness(cap=5, window=20.0)
    for _ in range(5):  # == cap, not strictly over
        h._note_maker_placement()
    assert h._rate_halted is False


def test_breaker_window_prunes_old_placements():
    h = _breaker_harness(cap=5, window=20.0)
    for _ in range(5):
        h._note_maker_placement()
    h._strategy.current_timestamp += 25.0  # > window -> old placements age out
    for _ in range(5):
        h._note_maker_placement()
    assert h._rate_halted is False


def test_breaker_disabled_is_behavior_neutral():
    h = _breaker_harness(enabled=False, cap=5, window=20.0)
    _open_maker(h, "m0")
    for _ in range(100):
        h._note_maker_placement()
    assert h._rate_halted is False
    h._strategy.cancel.assert_not_called()


def test_breaker_defaults_on_when_config_absent():
    # No breaker attrs on config at all -> getattr defaults; must still trip on a clear burst.
    h = _Harness()
    h.config = SimpleNamespace()
    h._strategy.current_timestamp = 1000.0
    for _ in range(500):  # far past any sane default cap, all same timestamp (one window)
        h._note_maker_placement()
    assert h._rate_halted is True


def test_place_target_one_suppressed_after_halt():
    h = _breaker_harness(cap=5)
    h._rate_halt_latched = True
    target = SimpleNamespace(side=None, price=Decimal("1"), size=Decimal("1"), edge_bps=Decimal("1"))
    # Must early-return BEFORE _place_maker (which raises NotImplementedError in the base):
    # reaching it would raise, so a clean return proves placement is suppressed.
    h._place_target_one(target)


def test_process_hedges_suppressed_after_halt():
    h = _breaker_harness(cap=5)
    h._rate_halt_latched = True
    h._pending_hedge_signed = Decimal("-5")  # would normally fire a hedge
    h._process_hedges()
    assert h.hedge_orders == {}  # no hedge placed while halted


def test_breaker_counts_only_submitted_placements_via_place_target_one():
    # The wiring under test: _place_target_one counts a placement ONLY when _place_maker
    # returns an order id. observe / sub-min returns None -> must NOT count (else the breaker
    # would falsely trip a healthy observe soak, which submits zero orders).
    h = _breaker_harness(cap=5, window=20.0)
    h._is_two_sided = lambda: False
    h._record_placed_rung = lambda oid, t: None  # bypass connector quantize (not under test)
    target = SimpleNamespace(side=None, price=Decimal("1"), size=Decimal("1"), edge_bps=Decimal("1"))

    h._place_maker = lambda *a, **k: None  # observe: no order submitted
    for _ in range(50):  # 50 >> cap 5
        h._place_target_one(target)
    assert h._rate_halted is False
    assert getattr(h, "_maker_placement_ts", []) == []  # nothing counted

    ids = iter(f"m{i}" for i in range(100))
    h._place_maker = lambda *a, **k: next(ids)  # real submits now DO count
    for _ in range(6):  # 6 > cap 5
        h._place_target_one(target)
    assert h._rate_halted is True


def test_real_config_carries_breaker_defaults():
    from test.hummingbot.strategy_v2.executors.ladder_maker_executor.test_latency_config import _cfg

    cfg = _cfg()
    assert cfg.placement_rate_breaker_enabled is True
    assert cfg.placement_rate_window_s == 30.0
    assert cfg.max_placements_per_window == 80


def test_real_config_overrides_read_by_executor():
    from test.hummingbot.strategy_v2.executors.ladder_maker_executor.test_latency_config import _cfg

    h = _Harness()
    h.config = _cfg(
        placement_rate_breaker_enabled=True,
        placement_rate_window_s=10.0,
        max_placements_per_window=3,
    )
    h._strategy.current_timestamp = 1000.0
    for _ in range(4):  # 4 > cap 3
        h._note_maker_placement()
    assert h._rate_halted is True
