"""JEP-166: average-cost gross placement edge for remaining open inventory."""
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
        self._hedge_order_side = {}
        self._maker_placed_edge_bps = {}

    def _update_tracked(self, *_):
        pass

    def get_in_flight_order(self, connector_name, order_id):
        return None

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


class _Ev:
    def __init__(self, order_id, amount, trade_type):
        self.order_id = order_id
        self.amount = Decimal(str(amount))
        self.price = Decimal("1")
        self.trade_type = trade_type


@pytest.fixture(autouse=True)
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


def _maker_fill(h, oid, amount, side, edge_bps):
    h.maker_orders[oid] = _Tracked(oid)
    h._maker_placed_edge_bps[oid] = Decimal(str(edge_bps))
    h.process_order_filled_event(None, None, _Ev(oid, amount, side))


def test_open_edge_vwap_accumulates_gross_sell_edges_by_volume():
    h = _Harness()

    _maker_fill(h, "m0", "2", TradeType.SELL, "10")
    _maker_fill(h, "m1", "3", TradeType.SELL, "20")

    assert h._open_edge_vwap == Decimal("16")


def test_partial_close_preserves_average_cost_and_ignores_close_edge():
    h = _Harness()

    _maker_fill(h, "m0", "2", TradeType.SELL, "10")
    _maker_fill(h, "m1", "1", TradeType.BUY, "99")

    assert h._open_edge_vwap == Decimal("10")


def test_full_close_resets_open_edge_vwap_to_zero():
    h = _Harness()

    _maker_fill(h, "m0", "2", TradeType.SELL, "10")
    _maker_fill(h, "m1", "2", TradeType.BUY, "99")

    assert h._open_edge_vwap == Decimal("0")


def test_close_at_zero_does_not_contaminate_reopen_average():
    h = _Harness()

    _maker_fill(h, "m0", "1", TradeType.BUY, "99")
    assert h._open_edge_vwap == Decimal("0")

    _maker_fill(h, "m1", "2", TradeType.SELL, "7")
    assert h._open_edge_vwap == Decimal("7")
