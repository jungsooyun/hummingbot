"""JEP-166 slice 3: two-sided cash-flow round-trip PnL."""
from decimal import Decimal
from types import SimpleNamespace

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
    def __init__(self, two_sided=True, config_has_two_sided=True):
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
        self.config = (
            SimpleNamespace(two_sided=two_sided)
            if config_has_two_sided
            else SimpleNamespace(type="legacy")
        )
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
        return {"amount": amt, "price": Decimal("8"), "order_type": None, "metadata": {}}

    def place_order(self, **kw):
        oid = f"h{len(self.placed)}"
        self.placed.append({**kw, "oid": oid})
        self.connector_orders[oid] = _Cx(kw["amount"])
        return oid

    def place_buy_order(self, **kw):
        return self.place_order(side=TradeType.BUY, **kw)

    def place_sell_order(self, **kw):
        return self.place_order(side=TradeType.SELL, **kw)


class _Ev:
    def __init__(self, order_id, amount, price="1", trade_type=None):
        self.order_id = order_id
        self.amount = Decimal(str(amount))
        self.price = Decimal(str(price))
        self.trade_type = trade_type


@pytest.fixture(autouse=True)
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


def _maker_fill(h, oid, amount, price, side):
    h.maker_orders[oid] = _Tracked(oid)
    h.process_order_filled_event(None, None, _Ev(oid, amount, price, trade_type=side))


def _hedge_fill(h, oid, amount, price, side):
    h.hedge_orders[oid] = _Tracked(oid)
    h._hedge_order_side[oid] = side
    h.process_order_filled_event(None, None, _Ev(oid, amount, price))


def test_roundtrip_realized_equals_captured_edge_minus_fees():
    h = _Harness(two_sided=True)

    _maker_fill(h, "m0", "1", "101", TradeType.SELL)
    _hedge_fill(h, "h0", "1", "100", TradeType.BUY)
    _maker_fill(h, "m1", "1", "100", TradeType.BUY)
    _hedge_fill(h, "h1", "1", "100.5", TradeType.SELL)

    assert h._unhedged_base_signed() == Decimal("0")
    assert h.get_net_pnl_quote() == Decimal("1.5")


def test_pnl_equivalence_full_hedge_matches_legacy():
    h = _Harness(two_sided=True)
    _maker_fill(h, "m0", "2", "10", TradeType.SELL)
    _hedge_fill(h, "h0", "2", "8", TradeType.BUY)
    assert h.get_net_pnl_quote() == Decimal("4")

    h = _Harness(two_sided=True)
    _maker_fill(h, "m0", "2", "10", TradeType.BUY)
    _hedge_fill(h, "h0", "2", "8", TradeType.SELL)
    assert h.get_net_pnl_quote() == Decimal("-4")


def test_pnl_equivalence_partial_and_reconcile_match_legacy():
    h = _Harness(two_sided=True)
    _maker_fill(h, "m0", "2", "10", TradeType.SELL)
    _hedge_fill(h, "h0", "0.75", "8", TradeType.BUY)
    assert h.get_net_pnl_quote() == Decimal("1.50")

    h = _Harness(two_sided=True)
    _maker_fill(h, "m0", "2", "10", TradeType.SELL)
    _hedge_fill(h, "h0", "1", "8", TradeType.BUY)
    assert h.get_net_pnl_quote() == Decimal("2")


def test_config_without_two_sided_uses_legacy_path():
    h = _Harness(config_has_two_sided=False)
    _maker_fill(h, "m0", "2", "10", TradeType.SELL)
    _hedge_fill(h, "h0", "2", "8", TradeType.BUY)

    assert h.get_net_pnl_quote() == Decimal("4")


def test_two_sided_open_position_marks_residual_at_maker_avg():
    h = _Harness(two_sided=True)

    _maker_fill(h, "m0", "2", "10", TradeType.SELL)
    assert h._unhedged_base_signed() == Decimal("-2")
    assert h.get_net_pnl_quote() == Decimal("0")

    _hedge_fill(h, "h0", "2", "8", TradeType.BUY)
    assert h.get_net_pnl_quote() == Decimal("4")
