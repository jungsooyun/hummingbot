"""JEP-219: a hedge order that rests OPEN (unfilled) past ``hedge_fill_timeout_s`` must be
cancelled so the next tick re-prices it.

Without this, a marketable hedge parked on a thin / momentarily-empty book (no contra) sits
open, ``_hedge_in_flight()`` blocks every re-hedge, and ``_pending_hedge`` only decrements on a
real fill -> the perp leg stays naked for the resting order's whole lifetime. The reconcile
path that already exists (``_reconcile_stuck_hedges``) handles only ``order is None``
(lost-created-event) hedges, NOT a tracked-and-open-but-unfilled one.

``_hedge_fill_timeout_s`` <= 0 (the default) must be fully behavior-neutral: no clock read, no
cancel, for every existing config.
"""
from decimal import Decimal

import pytest

import hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base as mod
from hummingbot.core.data_type.common import TradeType
from test.hummingbot.strategy_v2.executors.cross_venue_hedged_executor.test_hedge_residual import (
    _Cx,
    _Harness,
    _Tracked,
)


def _open_hedge(h, oid, created_ts):
    """Attach a tracked, OPEN (unfilled) hedge order created at ``created_ts``."""
    tr = _Tracked(oid)
    cx = _Cx(1)
    cx.is_open = True
    cx.creation_timestamp = created_ts
    tr.order = cx
    h.hedge_orders[oid] = tr
    return cx


def test_open_hedge_canceled_after_timeout():
    h = _Harness()
    h._hedge_fill_timeout_s = 5.0
    h._strategy.current_timestamp = 1000.0
    _open_hedge(h, "h0", created_ts=993.0)  # age 7.0 > 5.0
    h._reconcile_open_hedges()
    h._strategy.cancel.assert_called_once_with("kis", "005930-KRW", "h0")


def test_open_hedge_not_canceled_before_timeout():
    h = _Harness()
    h._hedge_fill_timeout_s = 5.0
    h._strategy.current_timestamp = 1000.0
    _open_hedge(h, "h0", created_ts=997.0)  # age 3.0 < 5.0
    h._reconcile_open_hedges()
    h._strategy.cancel.assert_not_called()


def test_filled_or_done_hedge_not_canceled():
    h = _Harness()
    h._hedge_fill_timeout_s = 5.0
    h._strategy.current_timestamp = 1000.0
    cx = _open_hedge(h, "h0", created_ts=900.0)  # very old
    cx.is_open = False  # terminal (filled/canceled) => is_done True
    h._reconcile_open_hedges()
    h._strategy.cancel.assert_not_called()


def test_disabled_when_timeout_zero_is_behavior_neutral():
    h = _Harness()
    # _hedge_fill_timeout_s unset (getattr default 0.0); current_timestamp deliberately NOT set,
    # so a regression that reads the clock before the disable-check would raise AttributeError.
    _open_hedge(h, "h0", created_ts=900.0)
    h._reconcile_open_hedges()
    h._strategy.cancel.assert_not_called()


def test_cancel_issued_once_within_retry_window():
    h = _Harness()
    h._hedge_fill_timeout_s = 5.0
    h._strategy.current_timestamp = 1000.0
    _open_hedge(h, "h0", created_ts=990.0)  # age 10 > 5
    h._reconcile_open_hedges()
    h._reconcile_open_hedges()  # same tick: within the retry window -> no second cancel
    h._strategy.cancel.assert_called_once_with("kis", "005930-KRW", "h0")


def test_cancel_retried_after_timeout_if_still_open():
    # HIGH-1 fix: a lost/failed cancel must NOT permanently suppress re-cancel. While the order
    # stays open, the cancel is re-issued every _hedge_fill_timeout_s so naked exposure stays bounded.
    h = _Harness()
    h._hedge_fill_timeout_s = 5.0
    h._strategy.current_timestamp = 1000.0
    _open_hedge(h, "h0", created_ts=990.0)  # age 10 > 5
    h._reconcile_open_hedges()                       # cancel #1
    h._strategy.current_timestamp = 1006.0           # +6 > timeout; cancel was "lost", order still open
    h._reconcile_open_hedges()                       # cancel #2 (retry)
    assert h._strategy.cancel.call_count == 2


@pytest.fixture
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


def test_wired_into_process_hedges(_patch_tracked):
    """End-to-end: _process_hedges -> _reconcile_open_hedges cancels an aged open hedge whose
    side still matches the pending need (so the opposite-side cancel loop does NOT fire it),
    then _hedge_in_flight() blocks placement -> exactly one cancel, from the timeout path."""
    h = _Harness()
    h._hedge_fill_timeout_s = 5.0
    h._strategy.current_timestamp = 1000.0
    h._pending_hedge_signed = Decimal("-1")  # BUY hedge still needed (unfilled)
    _open_hedge(h, "h0", created_ts=990.0)   # age 10 > 5
    h._hedge_order_side = {"h0": (TradeType.BUY, Decimal("1"))}  # matches needed side => opposite-loop skips it
    h._process_hedges()
    h._strategy.cancel.assert_called_once_with("kis", "005930-KRW", "h0")
