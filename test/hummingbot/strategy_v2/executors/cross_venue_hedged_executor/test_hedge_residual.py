"""JEP-162: hedge fractional-residual carry must be fill-truth based + single-in-flight.

Drives the REAL base event methods (process_order_filled_event /
process_order_failed_event / process_order_canceled_event / _process_hedges /
_unhedged_base / _hedge_in_flight) through a thin harness that bypasses only the
heavy RunnableBase wiring.

Faithful to production timing: a freshly placed TrackedOrder has ``order is None``
until the order-created event arrives, so _hedge_in_flight must treat the
just-placed window as in-flight (else a second tick double-hedges the same
residual now that pending is no longer zeroed at placement).
"""
from decimal import Decimal

import pytest

import hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base as mod
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)


class _Inner:
    def __init__(self):
        self.is_open = True


class _Cx:
    """Fake connector InFlightOrder: what ``fetch_order`` returns (active or 30s-cached).

    ``is_open`` flips False on a terminal transition; ``is_filled`` mirrors the FILLED
    *state*, which the connector can set with ``executed_amount_base`` still lagging
    behind ``amount`` (it only waits a short fill-window before completing). ``price``
    may be ``Decimal("NaN")`` (market hedge) and ``average_executed_price`` may be None.
    """

    def __init__(self, amount):
        self.amount = Decimal(str(amount))
        self.is_open = True
        self.is_filled = False
        self.executed_amount_base = Decimal("0")
        self.average_executed_price = Decimal("1")
        self.price = Decimal("1")
        self.base_asset = "X"
        self.quote_asset = "KRW"

    @property
    def is_done(self):
        return not self.is_open

    def cumulative_fee_paid(self, token, exchange=None):
        return Decimal("0")


class _Tracked:
    def __init__(self, order_id):
        self.order_id = order_id
        self.order = None  # faithful: None until the order-created event (matches production)

    @property
    def cum_fees_quote(self):
        return Decimal("0")


class _Harness(CrossVenueHedgedExecutorBase):
    def __init__(self):  # bypass RunnableBase wiring; set only what the methods touch
        self.maker_orders = {}
        self.hedge_orders = {}
        self._maker_executed_base = Decimal("0")
        self._maker_executed_quote = Decimal("0")
        self._hedge_executed_base = Decimal("0")
        self._hedge_executed_quote = Decimal("0")
        self._maker_fees_quote = Decimal("0")
        self._hedge_fees_quote = Decimal("0")
        self._pending_hedge_base = Decimal("0")
        self._current_retries = 0
        self._max_retries = 3
        self.maker_connector = "hl"
        self.maker_trading_pair = "X-USD"
        self.hedge_connector = "kis"
        self.hedge_trading_pair = "005930-KRW"
        self.entry_side = TradeType.SELL
        self.hedge_side = TradeType.BUY
        self.placed = []
        # The connector's order tracker (active + 30s cache) — the source of truth that
        # _reconcile_stuck_hedges consults via get_in_flight_order.
        self.connector_orders = {}

    def _update_tracked(self, *_):
        pass

    def get_in_flight_order(self, connector_name, order_id):
        return self.connector_orders.get(order_id)

    def evaluate_max_retries(self):
        pass

    # unused abstract hooks (needed only to make the ABC concrete)
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
        amt = pending_base.to_integral_value(rounding="ROUND_DOWN")  # KIS whole-share floor
        if amt <= 0:
            return None
        return {"amount": amt, "price": Decimal("1"), "order_type": None, "metadata": {}}

    def place_order(self, **kw):
        oid = f"h{len(self.placed)}"
        self.placed.append((oid, kw["amount"]))
        # Production: the connector synchronously starts tracking the order (PENDING_CREATE)
        # inside buy()/sell() before place_order returns, so it is immediately fetchable.
        self.connector_orders[oid] = _Cx(kw["amount"])
        return oid


class _Ev:
    def __init__(self, order_id, amount, price="1"):
        self.order_id = order_id
        self.amount = Decimal(str(amount))
        self.price = Decimal(str(price))


@pytest.fixture(autouse=True)
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


def _maker_fill(h, oid, amt):
    h.maker_orders[oid] = _Tracked(oid)
    h.process_order_filled_event(None, None, _Ev(oid, amt))


def _created(h, oid):  # simulate the order-created event populating .order
    h.hedge_orders[oid].order = _Inner()


def test_carry_residual_on_full_hedge_fill():
    h = _Harness()
    _maker_fill(h, "m0", "1.7")
    assert h._pending_hedge_base == Decimal("1.7")
    h._process_hedges()
    assert h.placed == [("h0", Decimal("1"))]  # floor
    _created(h, "h0")
    h.process_order_filled_event(None, None, _Ev("h0", "1"))  # hedge fill
    h.hedge_orders["h0"].order.is_open = False  # order completes
    assert h._pending_hedge_base == Decimal("0.7") == h._unhedged_base()
    h._process_hedges()
    assert len(h.placed) == 1  # floor(0.7)=0 -> no new order


def test_sub_one_share_not_hedged_and_kept():
    h = _Harness()
    _maker_fill(h, "m0", "0.3")
    h._process_hedges()
    assert h.placed == []
    assert h._pending_hedge_base == Decimal("0.3")


def test_no_double_hedge_before_created_event():
    # F1: pending is no longer zeroed at placement; the just-placed window (order is None)
    # must count as in-flight or the next tick double-hedges the same residual.
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()
    assert h.placed == [("h0", Decimal("2"))]
    assert h.hedge_orders["h0"].order is None  # order-created event not yet arrived
    h._process_hedges()
    assert h.placed == [("h0", Decimal("2"))]  # NO duplicate hedge


def test_single_in_flight_blocks_double_place_after_created():
    h = _Harness()
    _maker_fill(h, "m0", "3.0")
    h._process_hedges()
    _created(h, "h0")  # order open
    h._process_hedges()  # gated by in-flight (open)
    assert h.placed == [("h0", Decimal("3"))]


def test_partial_hedge_fill_then_fail_rehedges_remainder():
    h = _Harness()
    _maker_fill(h, "m0", "1.7")
    h._process_hedges()
    assert h.placed[-1] == ("h0", Decimal("1"))
    _created(h, "h0")
    h.process_order_filled_event(None, None, _Ev("h0", "0.5"))  # partial
    h.hedge_orders["h0"].order.is_open = False
    h.process_order_failed_event(None, None, _Ev("h0", "0"))  # fail -> pops, clears in-flight
    assert h._pending_hedge_base == Decimal("1.2")  # remainder NOT stranded
    h._process_hedges()
    assert h.placed[-1] == ("h1", Decimal("1"))  # re-hedge floor(1.2)


def test_fail_before_any_fill_rehedges_full():
    # failure at submission, before the created event (order stays None)
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()
    assert h.placed[-1] == ("h0", Decimal("2"))
    h.process_order_failed_event(None, None, _Ev("h0", "0"))  # pop while order is None
    assert h._pending_hedge_base == Decimal("2.0")
    h._process_hedges()
    assert h.placed[-1] == ("h1", Decimal("2"))


def test_hedge_cancel_keeps_pending_and_pops_order():
    # F4: a hedge cancellation must be popped (no leak/deadlock); pending stays (re-hedges).
    h = _Harness()
    _maker_fill(h, "m0", "1.7")
    h._process_hedges()
    _created(h, "h0")
    h.hedge_orders["h0"].order.is_open = False
    h.process_order_canceled_event(None, None, _Ev("h0", "0"))
    assert "h0" not in h.hedge_orders  # popped
    assert h._pending_hedge_base == Decimal("1.7")  # not stranded
    h._process_hedges()
    assert h.placed[-1] == ("h1", Decimal("1"))  # re-hedge


# ----------------------------------------------------------------- never-event reconcile
# A hedge order whose created/filled/canceled event is LOST would otherwise stay
# order-None forever and freeze ALL hedging (_hedge_in_flight). _process_hedges
# reconciles such orders against the connector's order tracker (active + 30s cache)
# every tick: it only ever ADOPTS orders the connector is actually tracking (crediting
# each pre-adoption fill once) and NEVER re-submits a hedge -> no double hedge. A hedge
# the tracker doesn't know yet (just-placed, or create task cancelled) is left for the
# next tick, never reaped — reaping it would double-hedge an order whose create task
# still runs (the exact failure of the reverted watchdog).


def test_stuck_none_hedge_healed_when_connector_live():
    # created event lost, but the order is genuinely live on the connector ->
    # adopt it (freeze clears) without re-hedging.
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); created event "lost"
    assert h.placed == [("h0", Decimal("2"))]
    assert h.hedge_orders["h0"].order is None
    h._process_hedges()  # reconcile -> heal
    assert h.hedge_orders["h0"].order is not None  # adopted from connector
    assert h.hedge_orders["h0"].order.is_open
    assert h._hedge_executed_base == Decimal("0")  # nothing filled yet -> no credit
    assert h.placed == [("h0", Decimal("2"))]  # NOT double-hedged


def test_stuck_none_hedge_terminal_filled_credits_without_double_hedge():
    # the order fully filled but ALL its events were lost: credit the full amount from
    # the connector record (FILLED state), never re-hedge it.
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); order None
    cx = h.connector_orders["h0"]
    cx.executed_amount_base = Decimal("2")
    cx.is_filled = True
    cx.is_open = False  # terminal FILLED
    h._process_hedges()  # reconcile: credit + drop
    assert "h0" not in h.hedge_orders
    assert h._hedge_executed_base == Decimal("2")
    assert h._pending_hedge_base == Decimal("0")
    assert h.placed == [("h0", Decimal("2"))]  # no re-hedge of filled base


def test_stuck_none_hedge_filled_with_lagging_executed_credits_full_amount():
    # FINDING #2: the connector can complete a FILLED order with executed_amount_base
    # still lagging behind amount. Trust the FILLED state -> credit the full amount so
    # the late fill update does not double up with a re-hedge.
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); order None
    cx = h.connector_orders["h0"]
    cx.is_filled = True  # FILLED state...
    cx.executed_amount_base = Decimal("0")  # ...but fills not accounted yet (lagging)
    cx.is_open = False
    h._process_hedges()  # reconcile credits the full amount, drops, no re-hedge
    assert h._hedge_executed_base == Decimal("2")
    assert h._pending_hedge_base == Decimal("0")
    assert h.placed == [("h0", Decimal("2"))]  # NOT re-hedged


def test_open_partial_adopt_credits_pre_adoption_fill():
    # FINDING #3: a still-open order with a partial fill whose events were lost must be
    # credited on adoption, else pending never decrements and the next hedge over-buys.
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); order None
    cx = h.connector_orders["h0"]
    cx.executed_amount_base = Decimal("1")  # 1 of 2 filled, still OPEN
    h._process_hedges()  # reconcile adopts + credits the partial
    assert h.hedge_orders["h0"].order is not None  # adopted, kept (still open)
    assert h._hedge_executed_base == Decimal("1")
    assert h._pending_hedge_base == Decimal("1")
    assert h.placed == [("h0", Decimal("2"))]  # in-flight -> no re-hedge this tick


def test_stuck_none_hedge_terminal_partial_credits_and_rehedges_remainder():
    # terminal after a PARTIAL fill (canceled/failed), events lost: credit the part that
    # filled, the unfilled remainder re-hedges; the lost failure counts toward retries.
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); order None
    cx = h.connector_orders["h0"]
    cx.executed_amount_base = Decimal("1")  # only 1 of 2 filled
    cx.is_open = False  # terminal, not is_filled
    h._process_hedges()  # reconcile: credit 1, drop, re-hedge remaining 1
    assert h._hedge_executed_base == Decimal("1")
    assert h._pending_hedge_base == Decimal("1")
    assert h.placed[-1] == ("h1", Decimal("1"))
    assert h._current_retries == 1  # lost failure counted


def test_stuck_none_hedge_terminal_unfilled_rehedges_and_counts_retry():
    # terminal with zero fill (canceled/failed) and events lost: drop without credit,
    # pending re-hedges, lost failure counts toward retries.
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); order None
    cx = h.connector_orders["h0"]
    cx.executed_amount_base = Decimal("0")
    cx.is_open = False  # terminal, nothing filled
    h._process_hedges()  # reconcile drops h0, re-hedges
    assert h._hedge_executed_base == Decimal("0")
    assert h._pending_hedge_base == Decimal("2")
    assert h.placed[-1] == ("h1", Decimal("2"))
    assert h._current_retries == 1


def test_unknown_to_connector_is_left_not_reaped_no_double_hedge():
    # FINDING #1: a hedge the tracker doesn't know (just-placed before start_tracking, or
    # cancelled create task) must NOT be reaped+re-hedged — its create task may still run
    # and place on the exchange. Leave it; the next tick adopts it once tracked.
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); order None
    del h.connector_orders["h0"]  # tracker has no record (model the pre-tracking window)
    h._process_hedges()  # reconcile must NOT re-hedge
    assert h._hedge_executed_base == Decimal("0")
    assert h.placed == [("h0", Decimal("2"))]  # NO second hedge (h0 not reaped)
    assert "h0" in h.hedge_orders and h.hedge_orders["h0"].order is None  # left in-flight


def test_terminal_filled_nan_price_does_not_poison_quote():
    # FINDING #5: a market hedge carries price=NaN; if average_executed_price is also
    # unavailable, crediting must not write NaN into the quote accumulator / PnL.
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); order None
    cx = h.connector_orders["h0"]
    cx.is_filled = True
    cx.is_open = False
    cx.average_executed_price = None
    cx.price = Decimal("NaN")
    h._process_hedges()  # reconcile credits base, skips the unusable quote
    assert h._hedge_executed_base == Decimal("2")
    assert not h._hedge_executed_quote.is_nan()
    assert h._hedge_executed_quote == Decimal("0")
