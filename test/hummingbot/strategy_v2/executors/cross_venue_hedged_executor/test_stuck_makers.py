"""JEP-177 Fix #2: maker-side stuck-order reconcile (`_reconcile_stuck_makers`).

The hedge leg has ``_reconcile_stuck_hedges`` (adopts a hedge stuck at ``order is None`` from
the connector tracker after a lost created event). The maker leg had no equivalent, so a
PERMANENTLY-lost maker created event on a genuinely-resting order could freeze that side until a
fill or restart (under-quote only, restart-recoverable). This adds the maker analogue:

  * connector tracks the order (``in_flight is not None``) -> ADOPT it (``tracked.order = in_flight``)
    so the order becomes visible to ``_open_maker_orders`` / the reprice diff and the side unfreezes.
  * connector has NO record (``in_flight is None``) -> do NOT reap/cancel. It may be a JUST-PLACED
    order whose ``start_tracking_order`` has not run yet; reaping/cancelling it is a live-money bug
    (the create task may still place on the exchange). Leave it for the next tick.

Adopt-only (no fill crediting, no grace-cancel) keeps the change behavior-neutral on the money
path. Gated behind ``reconcile_stuck_makers_enabled`` (default False = current behavior).
"""
from decimal import Decimal

import pytest

import hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base as mod
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.models.base import RunnableStatus


class _Tracked:
    def __init__(self, order_id):
        self.order_id = order_id
        self.order = None


class _InFlight:
    """Minimal InFlightOrder stand-in (only the fields the reconcile reads)."""

    def __init__(self, order_id, is_open=True, trade_type=TradeType.SELL):
        self.client_order_id = order_id
        self.is_open = is_open
        self.trade_type = trade_type


class _Harness(CrossVenueHedgedExecutorBase):
    def __init__(self, enabled=True):
        self.maker_orders = {}
        self.hedge_orders = {}
        self.maker_connector = "hl"
        self.maker_trading_pair = "X-USD"
        self.hedge_connector = "kis"
        self.hedge_trading_pair = "005930-KRW"
        self.entry_side = TradeType.SELL
        self._status = RunnableStatus.RUNNING
        self._tracker = {}  # oid -> _InFlight (the connector's truth)
        self._cancelled = []
        self.config = type("C", (), {"reconcile_stuck_makers_enabled": enabled})()
        self._strategy = type("S", (), {})()
        self._strategy.cancel = lambda *a, **k: self._cancelled.append(a)

    def get_in_flight_order(self, connector_name, order_id):
        return self._tracker.get(order_id)

    # Abstract-method stubs (unused by _reconcile_stuck_makers, required to instantiate).
    def _gates_open(self):
        return True

    def _compute_targets(self):
        return []

    def _should_reprice(self, targets):
        return False

    def _place_targets(self, targets):
        pass

    def _maker_balance_candidate(self):
        return None

    def _size_hedge(self, pending_base):
        return None


@pytest.fixture(autouse=True)
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


def _stuck_maker(h, oid, tracked_in_connector=None):
    """A maker order stuck at order is None; optionally present in the connector tracker."""
    h.maker_orders[oid] = _Tracked(oid)
    if tracked_in_connector is not None:
        h._tracker[oid] = tracked_in_connector


# ------------------------------------------------------------------ adopt path

def test_adopts_stuck_maker_tracked_by_connector():
    h = _Harness(enabled=True)
    inflight = _InFlight("m1", is_open=True)
    _stuck_maker(h, "m1", tracked_in_connector=inflight)

    h._reconcile_stuck_makers()

    # Adopted: the lost-created-event freeze is cleared; the order is now visible.
    assert h.maker_orders["m1"].order is inflight
    assert h._open_maker_orders() == [h.maker_orders["m1"]]
    assert h._cancelled == []  # adopt-only never cancels


def test_untracked_stuck_maker_not_reaped_or_cancelled():
    # in_flight is None -> may be a just-placed order (start_tracking not run). NEVER reap/cancel.
    h = _Harness(enabled=True)
    _stuck_maker(h, "m1", tracked_in_connector=None)

    h._reconcile_stuck_makers()

    assert "m1" in h.maker_orders  # still tracked, not popped
    assert h.maker_orders["m1"].order is None  # left stuck for next tick
    assert h._cancelled == []


def test_already_resolved_maker_left_alone():
    # A maker whose order is already set (not stuck) is never touched.
    h = _Harness(enabled=True)
    inflight = _InFlight("m1", is_open=True)
    h.maker_orders["m1"] = _Tracked("m1")
    h.maker_orders["m1"].order = inflight
    h._tracker["m1"] = _InFlight("m1", is_open=False)  # connector says closed; must be ignored

    h._reconcile_stuck_makers()

    assert h.maker_orders["m1"].order is inflight  # unchanged
    assert h._cancelled == []


# ------------------------------------------------------------------ gating

def test_disabled_is_noop():
    # Default off: a connector-tracked stuck maker is NOT adopted (behavior-neutral).
    h = _Harness(enabled=False)
    inflight = _InFlight("m1", is_open=True)
    _stuck_maker(h, "m1", tracked_in_connector=inflight)

    h._reconcile_stuck_makers()

    assert h.maker_orders["m1"].order is None  # untouched when disabled
    assert h._cancelled == []


def test_missing_config_flag_defaults_off():
    # A config lacking the attribute entirely defaults to OFF (getattr default).
    h = _Harness(enabled=True)
    h.config = type("C", (), {})()  # no reconcile_stuck_makers_enabled
    inflight = _InFlight("m1", is_open=True)
    _stuck_maker(h, "m1", tracked_in_connector=inflight)

    h._reconcile_stuck_makers()

    assert h.maker_orders["m1"].order is None  # not adopted -> defaulted off
