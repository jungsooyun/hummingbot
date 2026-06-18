"""JEP-166: direction-aware base accounting for two-sided maker fills."""
from decimal import Decimal

import pytest

import hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base as mod
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.models.base import RunnableStatus


class _Inner:
    def __init__(self):
        self.is_open = True


class _Cx:
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
        self.order = None

    @property
    def cum_fees_quote(self):
        return Decimal("0")


class _Harness(CrossVenueHedgedExecutorBase):
    def __init__(self):
        self.maker_orders = {}
        self.hedge_orders = {}
        self._maker_executed_base = Decimal("0")
        self._maker_executed_quote = Decimal("0")
        self._hedge_executed_base = Decimal("0")
        self._hedge_executed_quote = Decimal("0")
        self._maker_fees_quote = Decimal("0")
        self._hedge_fees_quote = Decimal("0")
        self._pending_hedge_signed = Decimal("0")
        self._current_retries = 0
        self._max_retries = 3
        self._hedge_kill_switch = False
        self._status = RunnableStatus.RUNNING
        self.maker_connector = "hl"
        self.maker_trading_pair = "X-USD"
        self.hedge_connector = "kis"
        self.hedge_trading_pair = "005930-KRW"
        self.entry_side = TradeType.SELL
        self.hedge_side = TradeType.BUY
        self.placed = []
        self.connector_orders = {}
        self._hedge_order_side = {}
        self._maker_placed_edge_bps = {}

    def _update_tracked(self, *_):
        pass

    def get_in_flight_order(self, connector_name, order_id):
        return self.connector_orders.get(order_id)

    def stop(self):
        self._status = RunnableStatus.TERMINATED

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
        amt = pending_base.to_integral_value(rounding="ROUND_DOWN")
        if amt <= 0:
            return None
        return {"amount": amt, "price": Decimal("1"), "order_type": None, "metadata": {}}

    def place_order(self, **kw):
        oid = f"h{len(self.placed)}"
        self.placed.append({"order_id": oid, "side": kw["side"], "amount": kw["amount"]})
        self.connector_orders[oid] = _Cx(kw["amount"])
        return oid


class _Ev:
    def __init__(self, order_id, amount, price="1", trade_type=None):
        self.order_id = order_id
        self.amount = Decimal(str(amount))
        self.price = Decimal(str(price))
        self.trade_type = trade_type


@pytest.fixture(autouse=True)
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


def _maker_fill(h, oid, amount, side):
    h.maker_orders[oid] = _Tracked(oid)
    h.process_order_filled_event(None, None, _Ev(oid, amount, trade_type=side))


def _created(h, oid):
    h.hedge_orders[oid].order = _Inner()


def test_signed_ledgers_pair_only_opposing_perp_and_spot_inventory():
    h = _Harness()

    _maker_fill(h, "m0", "2", TradeType.SELL)
    assert h._perp_net() == Decimal("-2")
    assert h._spot_net() == Decimal("0")
    assert h._paired_oi() == Decimal("0")
    assert h._unhedged_base_signed() == Decimal("-2")
    assert h._pending_hedge_base == Decimal("2")

    h.hedge_orders["orphan"] = _Tracked("orphan")
    h._hedge_order_side["orphan"] = TradeType.SELL
    h.process_order_filled_event(None, None, _Ev("orphan", "1"))
    assert h._perp_net() == Decimal("-2")
    assert h._spot_net() == Decimal("-1")
    assert h._paired_oi() == Decimal("0")
    assert h._unhedged_base_signed() == Decimal("-3")

    h.hedge_orders["pair"] = _Tracked("pair")
    h._hedge_order_side["pair"] = TradeType.BUY
    h.process_order_filled_event(None, None, _Ev("pair", "1.5"))
    assert h._spot_net() == Decimal("0.5")
    assert h._paired_oi() == Decimal("0.5")
    assert h._unhedged_base_signed() == Decimal("-1.5")


def test_signed_pending_nets_sell_and_buy_maker_fills():
    h = _Harness()

    _maker_fill(h, "m0", "3", TradeType.SELL)
    _maker_fill(h, "m1", "1", TradeType.BUY)
    assert h._pending_hedge_signed == Decimal("-2")
    assert h._pending_hedge_base == Decimal("2")

    _maker_fill(h, "m2", "5", TradeType.BUY)
    assert h._pending_hedge_signed == Decimal("3")
    assert h._pending_hedge_base == Decimal("3")


def test_in_flight_hedge_fill_uses_recorded_side_after_pending_sign_flip():
    h = _Harness()
    _maker_fill(h, "m0", "2", TradeType.SELL)
    h._process_hedges()
    assert h.placed[-1]["side"] == TradeType.BUY
    assert h._hedge_order_side["h0"] == (TradeType.BUY, Decimal("2"))

    _created(h, "h0")
    _maker_fill(h, "m1", "3", TradeType.BUY)
    assert h._pending_hedge_signed == Decimal("1")

    h.process_order_filled_event(None, None, _Ev("h0", "2"))
    assert h._spot_net() == Decimal("2")
    assert h._pending_hedge_signed == Decimal("3")
    assert h._unhedged_base_signed() == Decimal("3")


def test_reconcile_adopt_uses_recorded_side_after_pending_sign_flip():
    h = _Harness()
    _maker_fill(h, "m0", "2", TradeType.SELL)
    h._process_hedges()
    assert h._hedge_order_side["h0"] == (TradeType.BUY, Decimal("2"))

    _maker_fill(h, "m1", "3", TradeType.BUY)
    cx = h.connector_orders["h0"]
    cx.executed_amount_base = Decimal("2")
    h._reconcile_stuck_hedges()

    assert h._spot_net() == Decimal("2")
    assert h._pending_hedge_signed == Decimal("3")
    assert h._unhedged_base_signed() == Decimal("3")
