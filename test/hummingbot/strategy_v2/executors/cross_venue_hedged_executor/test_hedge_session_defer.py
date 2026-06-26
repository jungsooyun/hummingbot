"""JEP-226: the session/auction-aware hedge gate wired into _process_hedges.

Drives the REAL base _process_hedges through the _Harness (bypasses RunnableBase) with an
injected _session_halt_state, asserting place/defer/hold/force and that force composes with
the JEP-219 open-hedge reconcile + single-in-flight invariant. The pure decision is covered
by test_hedge_defer_policy; this locks the WIRING.
"""
from decimal import Decimal
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_halt_source import SessionHaltState
from test.hummingbot.strategy_v2.executors.cross_venue_hedged_executor.test_hedge_residual import (  # noqa: F401
    _Cx,
    _Harness,
    _Tracked,
    _patch_tracked,  # autouse fixture: patches TrackedOrder -> _Tracked
)


def _arm(h, *, halted, reason, cap=30.0, since=None, side=None, now=1000.0):
    """Configure the JEP-226 gate state on a harness needing a BUY hedge (pending=-1)."""
    h._session_halt_state = SessionHaltState(halted=halted, ready=True, reason=reason)
    h._hedge_session_defer_cap_s = cap
    h._hedge_defer_since_ts = since
    h._hedge_defer_side = side
    h._hedge_defer_logged_kind = None
    h._pending_hedge_signed = Decimal("-1")   # BUY hedge needed
    h._hedge_order_side = {}
    h._strategy.current_timestamp = now


def test_defer_in_auction_within_cap_suppresses_placement():
    h = _Harness()
    _arm(h, halted=True, reason="in_auction_window", since=995.0, side=TradeType.BUY, now=1000.0)  # naked 5 < 30
    h._process_hedges()
    assert h.placed == []                         # deferred — no order placed
    assert h._hedge_defer_logged_kind == "defer"


def test_force_in_auction_past_cap_places_once():
    h = _Harness()
    _arm(h, halted=True, reason="in_auction_window", since=970.0, side=TradeType.BUY, now=1000.0)  # naked 30 >= 30
    h._process_hedges()
    assert len(h.placed) == 1                      # forced — one hedge placed
    assert h._hedge_defer_logged_kind == "force"


def test_hold_on_vi_never_places_even_past_cap():
    h = _Harness()
    _arm(h, halted=True, reason="vi", since=0.0, side=TradeType.BUY, now=10_000.0)  # naked huge
    h._process_hedges()
    assert h.placed == []                          # genuine halt -> hold, never force
    assert h._hedge_defer_logged_kind == "hold"


def test_hold_on_hour_cls_auction():
    # the VI-ambiguous connector reason must HOLD, never force (R2-F1)
    h = _Harness()
    _arm(h, halted=True, reason="hour_cls_auction", since=0.0, side=TradeType.BUY, now=10_000.0)
    h._process_hedges()
    assert h.placed == [] and h._hedge_defer_logged_kind == "hold"


def test_cap_zero_killswitch_places_despite_halt():
    h = _Harness()
    _arm(h, halted=True, reason="in_auction_window", cap=0.0, now=1000.0)  # gate disabled
    h._process_hedges()
    assert len(h.placed) == 1                      # kill-switch -> place immediately


def test_not_halted_places():
    h = _Harness()
    _arm(h, halted=False, reason="", now=1000.0)
    h._process_hedges()
    assert len(h.placed) == 1


def test_force_then_reconcile_blocks_second_placement():
    """F3: force places one hedge; while it rests open past hedge_fill_timeout_s the JEP-219
    reconcile cancels it and single-in-flight blocks a second placement."""
    h = _Harness()
    h._hedge_fill_timeout_s = 5.0
    _arm(h, halted=True, reason="in_auction_window", since=970.0, side=TradeType.BUY, now=1000.0)
    h._process_hedges()
    assert len(h.placed) == 1
    oid = h.placed[0][0]
    # simulate the order-created event: the forced hedge now rests OPEN, created at t=1000
    cx = _Cx(1)
    cx.is_open = True
    cx.creation_timestamp = 1000.0
    h.hedge_orders[oid].order = cx
    # next tick: aged 10s > timeout 5s, still halted+in_auction, pending unchanged
    h._strategy.current_timestamp = 1010.0
    h._process_hedges()
    h._strategy.cancel.assert_called_once_with("kis", "005930-KRW", oid)  # JEP-219 cancel
    assert len(h.placed) == 1                      # single-in-flight blocked a 2nd placement


def test_log_kind_deduped_across_ticks():
    h = _Harness()
    mocklog = MagicMock()
    h.logger = lambda: mocklog
    _arm(h, halted=True, reason="in_auction_window", since=995.0, side=TradeType.BUY, now=1000.0)
    h._process_hedges()   # defer -> log once
    h._strategy.current_timestamp = 1001.0
    h._process_hedges()   # still defer, same kind -> NO second log
    assert mocklog.warning.call_count == 1
