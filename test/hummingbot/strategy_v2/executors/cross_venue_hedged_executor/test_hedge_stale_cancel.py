from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base as mod
from hummingbot.core.data_type.common import TradeType
from test.hummingbot.strategy_v2.executors.cross_venue_hedged_executor.test_hedge_residual import (
    _Cx,
    _Ev,
    _Harness,
    _Tracked,
    _created,
)


@pytest.fixture(autouse=True)
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


class _H2Harness(_Harness):
    def __init__(self):
        super().__init__()
        self._strategy = SimpleNamespace(current_timestamp=1000.0, cancel=MagicMock())

    def place_order(self, **kw):
        oid = f"h{len(self.placed)}"
        self.placed.append((oid, kw["side"], kw["amount"]))
        self.connector_orders[oid] = _Cx(kw["amount"])
        return oid


def _maker_fill(h, oid, amount, trade_type):
    h.maker_orders[oid] = _Tracked(oid)
    event = _Ev(oid, amount)
    event.trade_type = trade_type
    h.process_order_filled_event(None, None, event)


def _hedge_fill(h, oid, amount):
    h.process_order_filled_event(None, None, _Ev(oid, amount))


def _cancelled_ids(h):
    return [c.args[2] for c in h._strategy.cancel.call_args_list]


def test_signflip_cancels_stale_hedge():
    h = _H2Harness()
    _maker_fill(h, "open-1", "2", TradeType.SELL)
    h._process_hedges()
    _created(h, "h0")
    assert h.placed == [("h0", TradeType.BUY, Decimal("2"))]

    _maker_fill(h, "close-1", "3", TradeType.BUY)
    before = list(h.placed)
    h._process_hedges()

    assert _cancelled_ids(h) == ["h0"]
    assert h.placed == before


def test_same_direction_hedge_not_cancelled():
    h = _H2Harness()
    _maker_fill(h, "open-1", "2", TradeType.SELL)
    h._process_hedges()
    _created(h, "h0")
    _maker_fill(h, "open-2", "1", TradeType.SELL)

    h._process_hedges()

    h._strategy.cancel.assert_not_called()
    assert "h0" in h.hedge_orders


def test_just_placed_hedge_not_cancelled():
    h = _H2Harness()
    _maker_fill(h, "open-1", "2", TradeType.SELL)
    h._process_hedges()
    h.connector_orders.pop("h0", None)
    assert h.hedge_orders["h0"].order is None
    _maker_fill(h, "close-1", "3", TradeType.BUY)

    h._process_hedges()

    h._strategy.cancel.assert_not_called()


def test_stale_cancel_then_rehedge_net():
    h = _H2Harness()
    _maker_fill(h, "open-1", "2", TradeType.SELL)
    h._process_hedges()
    _created(h, "h0")
    _maker_fill(h, "close-1", "3", TradeType.BUY)
    h._process_hedges()
    h.process_order_canceled_event(None, None, _Ev("h0", "0"))

    h._process_hedges()

    assert h.placed[-1] == ("h1", TradeType.SELL, Decimal("1"))
    _created(h, "h1")
    _hedge_fill(h, "h1", "1")
    assert h._perp_net() + h._spot_net() == Decimal("0")


def test_stale_partial_fill_credited_then_cancel():
    h = _H2Harness()
    _maker_fill(h, "open-1", "2", TradeType.SELL)
    h._process_hedges()
    _created(h, "h0")
    _hedge_fill(h, "h0", "0.6")
    _maker_fill(h, "close-1", "3", TradeType.BUY)

    h._process_hedges()

    assert h._hedge_executed_base == Decimal("0.6")
    assert _cancelled_ids(h) == ["h0"]


def test_single_sided_no_stale_cancel():
    h = _H2Harness()
    _maker_fill(h, "open-1", "2", TradeType.SELL)
    h._process_hedges()
    _created(h, "h0")
    _maker_fill(h, "open-2", "2", TradeType.SELL)

    h._process_hedges()
    h._process_hedges()

    h._strategy.cancel.assert_not_called()
    assert h.placed == [("h0", TradeType.BUY, Decimal("2"))]
