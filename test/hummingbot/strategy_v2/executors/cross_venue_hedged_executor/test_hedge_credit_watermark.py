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


def _adopt_filled(h, oid, amount, executed=None):
    cx = _Cx(amount)
    cx.is_filled = True
    cx.executed_amount_base = Decimal(str(amount if executed is None else executed))
    cx.is_open = False
    h.connector_orders[oid] = cx


def _hedge_fill(h, oid, cumulative, price="1"):
    tracked = h.hedge_orders.get(oid)
    if tracked is None:
        h.process_order_filled_event(None, None, _Ev(oid, cumulative, price))
        return
    cx = h.connector_orders.get(oid) or _Cx(cumulative)
    cx.executed_amount_base = Decimal(str(cumulative))
    h.connector_orders[oid] = cx
    tracked.order = cx
    h.process_order_filled_event(None, None, _Ev(oid, cumulative, price))


def _spot_net(h):
    return h._hedge_buy_base - h._hedge_sell_base


def test_adopt_then_late_fill_credits_once():
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()

    _adopt_filled(h, "h0", "2")
    h._process_hedges()
    _hedge_fill(h, "h0", "2")

    assert _spot_net(h) == Decimal("2")
    assert h._pending_hedge_signed == Decimal("0")
    assert h._hedge_executed_base == Decimal("2")


def test_fill_then_adopt_credits_once():
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()

    _hedge_fill(h, "h0", "2")
    _adopt_filled(h, "h0", "2")
    h._process_hedges()

    assert _spot_net(h) == Decimal("2")
    assert h._pending_hedge_signed == Decimal("0")
    assert h._hedge_executed_base == Decimal("2")


def test_partial_adopt_then_completion_event():
    h = _Harness()
    _maker_fill(h, "m0", "2.0")
    h._process_hedges()
    h.connector_orders["h0"].executed_amount_base = Decimal("0.6")
    h._process_hedges()

    _hedge_fill(h, "h0", "1.0")
    assert h._hedge_executed_base == Decimal("1.0")
    assert h._hedge_credited_base["h0"] == Decimal("1.0")

    before = h._hedge_executed_base
    h.hedge_orders["h1"] = _Tracked("h1")
    h._hedge_order_side["h1"] = (TradeType.BUY, Decimal("1"))
    _adopt_filled(h, "h1", "1", executed="0.4")
    h.connector_orders["h1"].is_open = True
    h._process_hedges()
    assert h._hedge_credited_base["h1"] == Decimal("1")

    _hedge_fill(h, "h1", "0.4")
    assert h._hedge_credited_base["h1"] == Decimal("1")
    assert h._hedge_executed_base == before + Decimal("1")


def test_duplicate_fill_event_idempotent():
    h = _Harness()
    _maker_fill(h, "m0", "1")
    h._process_hedges()

    _hedge_fill(h, "h0", "1")
    _hedge_fill(h, "h0", "1")

    assert h._hedge_executed_base == Decimal("1")
    assert h._pending_hedge_signed == Decimal("0")


def test_signflip_adopt_no_wrong_direction():
    h = _Harness()
    h.entry_side = TradeType.SELL
    _maker_fill(h, "m0", "2")
    h._process_hedges()
    h.maker_orders["m1"] = _Tracked("m1")
    close_event = _Ev("m1", "3")
    close_event.trade_type = TradeType.BUY
    h.process_order_filled_event(None, None, close_event)

    _adopt_filled(h, "h0", "2")
    h._process_hedges()
    _hedge_fill(h, "h0", "2")

    expected = (
        h._maker_buy_base
        - h._maker_sell_base
        + Decimal("2")  # h0 BUY hedge credited exactly once
    )
    assert h._pending_hedge_signed == expected


def test_watermark_retained_on_terminal_until_evicted():
    h = _Harness()
    h._hedge_credited_base = {"h0": Decimal("1")}
    h.hedge_orders["h0"] = _Tracked("h0")
    h.process_order_completed_event(None, None, _Ev("h0", "1"))
    assert "h0" in h._hedge_terminal_ids
    assert h._hedge_credited_base["h0"] == Decimal("1")

    h._hedge_credited_base["h0"] = Decimal("1")
    h.hedge_orders["h0"] = _Tracked("h0")
    h.process_order_canceled_event(None, None, _Ev("h0", "0"))
    assert "h0" in h._hedge_terminal_ids
    assert h._hedge_credited_base["h0"] == Decimal("1")

    h._hedge_credited_base["h0"] = Decimal("1")
    h.hedge_orders["h0"] = _Tracked("h0")
    h.process_order_failed_event(None, None, _Ev("h0", "0"))
    assert "h0" in h._hedge_terminal_ids
    assert h._hedge_credited_base["h0"] == Decimal("1")

    h.hedge_orders["h1"] = _Tracked("h1")
    h._hedge_order_side["h1"] = (TradeType.BUY, Decimal("1"))
    h._hedge_credited_base["h1"] = Decimal("1")
    _adopt_filled(h, "h1", "1")
    h._process_hedges()
    assert "h1" in h._hedge_terminal_ids
    assert h._hedge_credited_base["h1"] == Decimal("1")

    for i in range(h._HEDGE_TERMINAL_ID_CAP):
        oid = f"evict-{i}"
        h._hedge_order_side[oid] = (TradeType.BUY, Decimal("1"))
        h._hedge_credited_base[oid] = Decimal("1")
        h._remember_terminal_hedge_order(oid)

    assert "h0" not in h._hedge_terminal_ids
    assert "h0" not in h._hedge_order_side
    assert "h0" not in h._hedge_credited_base
