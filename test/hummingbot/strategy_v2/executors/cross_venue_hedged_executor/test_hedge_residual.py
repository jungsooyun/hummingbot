"""JEP-162: hedge fractional-residual carry must be fill-truth based.

Drives the REAL base event methods (process_order_filled_event /
process_order_failed_event / _process_hedges / _unhedged_base / _hedge_in_flight)
through a thin harness that bypasses only the heavy RunnableBase wiring. The
payloads are lightweight (the handlers read order_id/amount/price). Covers the
money-risk cases the placement-time-zeroing bug would strand: partial-fill-then-
fail and fail-before-fill.
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


class _Tracked:
    def __init__(self, order_id):
        self.order_id = order_id
        self.order = _Inner()

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

    def _update_tracked(self, *_):
        pass

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


def test_carry_residual_on_full_hedge_fill():
    h = _Harness()
    _maker_fill(h, "m0", "1.7")
    assert h._pending_hedge_base == Decimal("1.7")
    h._process_hedges()
    assert h.placed == [("h0", Decimal("1"))]  # floor
    h.hedge_orders["h0"].order.is_open = False
    h.process_order_filled_event(None, None, _Ev("h0", "1"))  # hedge fill
    assert h._pending_hedge_base == Decimal("0.7") == h._unhedged_base()
    h._process_hedges()
    assert len(h.placed) == 1  # floor(0.7)=0 -> no new order


def test_sub_one_share_not_hedged_and_kept():
    h = _Harness()
    _maker_fill(h, "m0", "0.3")
    h._process_hedges()
    assert h.placed == []
    assert h._pending_hedge_base == Decimal("0.3")


def test_partial_hedge_fill_then_fail_rehedges_remainder():
    h = _Harness()
    _maker_fill(h, "m0", "1.7")
    h._process_hedges()
    assert h.placed[-1] == ("h0", Decimal("1"))
    h.process_order_filled_event(None, None, _Ev("h0", "0.5"))  # partial
    h.hedge_orders["h0"].order.is_open = False
    h.process_order_failed_event(None, None, _Ev("h0", "0"))  # fail -> pops, clears in-flight
    assert h._pending_hedge_base == Decimal("1.2")  # remainder NOT stranded
    h._process_hedges()
    assert h.placed[-1] == ("h1", Decimal("1"))  # re-hedge floor(1.2)


def test_fail_before_any_fill_rehedges_full():
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()
    assert h.placed[-1] == ("h0", Decimal("2"))
    h.hedge_orders["h0"].order.is_open = False
    h.process_order_failed_event(None, None, _Ev("h0", "0"))
    assert h._pending_hedge_base == Decimal("2.0")
    h._process_hedges()
    assert h.placed[-1] == ("h1", Decimal("2"))


def test_single_in_flight_blocks_double_place():
    h = _Harness()
    _maker_fill(h, "m0", "3.0")
    h._process_hedges()
    h._process_hedges()  # 2nd call gated by in-flight
    assert h.placed == [("h0", Decimal("3"))]
