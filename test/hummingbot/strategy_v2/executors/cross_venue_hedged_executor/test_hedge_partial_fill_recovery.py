"""JEP-186: hedge partial-fill recovery must credit the MARGINAL fill price, not the
cumulative-average price, when the lost-lifecycle-event reconcile path adopts an order
that had already been partially credited at a different price.

Scenario (rare but real): a hedge order partially fills at price P1 (credited once via
the normal fill-event path). Its subsequent lifecycle events are then LOST, so the next
partial lands only on the connector record. ``_reconcile_stuck_hedges`` adopts the order
and passes ``executed_amount_base`` (cumulative base) + ``average_executed_price``
(cumulative-AVERAGE price) to ``_credit_hedge_fill``. ``_credit_hedge_fill`` credits only
the marginal delta, but multiplying that delta by the *cumulative average* price
mis-attributes quote whenever the two partials filled at different prices.

The fix reconstructs the marginal price for the delta from the cumulative average and the
already-credited native quote, so ``_hedge_executed_quote`` / ``_spot_cash`` equal the sum
of (marginal_base * marginal_price). Base/watermark accounting is unchanged (JEP-172 C3).
"""
from decimal import Decimal

import pytest

import hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base as mod
from hummingbot.core.data_type.common import TradeType
from test.hummingbot.strategy_v2.executors.cross_venue_hedged_executor.test_hedge_residual import (
    _Cx,
    _Ev,
    _Harness,
    _Tracked,
    _maker_fill,
)


@pytest.fixture(autouse=True)
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


def test_reconcile_partial_then_event_partial_uses_marginal_price():
    """First partial credited via event @100; second partial recovered via reconcile.

    The connector record carries cumulative base = 2 and average_executed_price = 150
    (the blended avg of 1@100 + 1@200). The recovered delta of 1 unit must be credited at
    its MARGINAL price 200 (not the cumulative average 150), so the BUY-hedge quote totals
    1*100 + 1*200 = 300, and _spot_cash (BUY => negative cash out) == -300.
    """
    h = _Harness()
    # Build a maker fill large enough to need a 2-unit BUY hedge.
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); created event "lost" -> order stays None

    # First partial fills @100 and is credited via the normal fill-event path.
    cx = h.connector_orders["h0"]
    cx.executed_amount_base = Decimal("1")
    h.hedge_orders["h0"].order = cx  # created event arrives for this partial
    h.process_order_filled_event(None, None, _Ev("h0", "1", "100"))
    assert h._hedge_executed_base == Decimal("1")
    assert h._hedge_executed_quote == Decimal("100")
    assert h._spot_cash == Decimal("-100")  # BUY hedge = cash out

    # Lifecycle events are now LOST: a second unit fills @200 but only the connector
    # record updates (cumulative base 2, blended average 150). Force the reconcile path
    # by resetting order to None (the lost-event freeze condition).
    h.hedge_orders["h0"].order = None
    cx.executed_amount_base = Decimal("2")
    cx.average_executed_price = Decimal("150")  # blended avg of 1@100 + 1@200
    cx.is_open = True  # still open partial
    h._process_hedges()  # reconcile adopts + credits the recovered partial

    # Base/watermark accounting unchanged.
    assert h._hedge_executed_base == Decimal("2")
    assert h._hedge_credited_base["h0"] == Decimal("2")
    # Quote must use the MARGINAL price (200) for the recovered unit, not the avg (150).
    assert h._hedge_executed_quote == Decimal("300")
    assert h._spot_cash == Decimal("-300")


def test_reconcile_full_recovery_no_prior_credit_uses_avg_unchanged():
    """No prior credit: the cumulative average IS the marginal price for the whole fill.

    Behavior-neutral guard — when ``already == 0`` the reconstruction must reduce to the
    cumulative average exactly (so a clean lost-then-fully-filled order is unaffected).
    """
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); order None
    cx = h.connector_orders["h0"]
    cx.executed_amount_base = Decimal("2")
    cx.average_executed_price = Decimal("123")
    cx.is_filled = True
    cx.is_open = False  # terminal FILLED, all events lost
    h._process_hedges()  # reconcile credits the full 2 @123

    assert h._hedge_executed_base == Decimal("2")
    assert h._hedge_executed_quote == Decimal("246")  # 2 * 123
    assert h._spot_cash == Decimal("-246")


def test_reconcile_partial_then_reconcile_partial_uses_marginal_price():
    """Two recovered partials at different prices, both via the reconcile path.

    First reconcile credits 1@100 (avg==marginal since already==0). Second reconcile sees
    cumulative base 2, average 150 and must back out the marginal 200 for the new unit.
    """
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()  # place h0(2); order None
    cx = h.connector_orders["h0"]

    # First recovered partial: 1 @100 (no prior credit -> avg == marginal).
    cx.executed_amount_base = Decimal("1")
    cx.average_executed_price = Decimal("100")
    cx.is_open = True
    h._process_hedges()
    assert h._hedge_executed_quote == Decimal("100")
    assert h._spot_cash == Decimal("-100")

    # Events lost again; second unit fills @200 -> cumulative 2, blended avg 150.
    h.hedge_orders["h0"].order = None  # re-trigger the lost-event freeze
    cx.executed_amount_base = Decimal("2")
    cx.average_executed_price = Decimal("150")
    cx.is_open = True
    h._process_hedges()

    assert h._hedge_executed_base == Decimal("2")
    assert h._hedge_executed_quote == Decimal("300")  # 1*100 + 1*200
    assert h._spot_cash == Decimal("-300")
