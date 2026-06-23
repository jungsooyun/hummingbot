"""Money-critical: a cancelled-then-filled MAKER order must still be ACCOUNTED.

Cancel/fill race (reprice cancels a rung; it fills on the venue before the cancel
confirms): the maker order is POPPED from ``self.maker_orders`` on the cancel event,
so the subsequent ``OrderFilledEvent`` formerly hit the ``else`` branch and was
DROPPED with "Ignoring fill for unknown order". The perp position moved on-exchange
but ``_maker_executed_base`` / ``_pending_hedge_signed`` were NOT updated, so the
hedge never fired -> naked directional exposure.

LIVE PROOF (2026-06-23): 2 cancelled CLOSE perp orders (1.0 + 2.0) filled after
cancel, each "Ignoring fill for unknown order"; 0 spot SELL hedges fired; result =
naked long 3.0 (short 19.15 perp vs long 22 spot).

Fix mirrors the proven hedge-terminal handling: ``_maker_terminal_ids`` (OrderedDict)
records a popped maker order; a fill that arrives afterwards is credited via the
``_maker_credited_base`` watermark + the connector's cumulative ``executed_amount_base``
(fallback watermark + event.amount) so a fill is never double-counted across the
maker_orders -> terminal transition.
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


def _adopt_maker_filled(h, oid, cumulative):
    """Make the connector report a cumulative executed_amount_base for a maker order.

    Mirrors the live path: after the cancel pops the tracker, the connector's order
    tracker (active or 30s cache) still carries the cumulative fill, fetched via
    get_in_flight_order in the terminal branch.
    """
    cx = _Cx(cumulative)
    cx.executed_amount_base = Decimal(str(cumulative))
    cx.is_open = False
    cx.is_filled = True
    h.connector_orders[oid] = cx


# --------------------------------------------------------------------------- (a) RED


def test_cancelled_maker_order_late_fill_is_accounted():
    """RED-first reproduction of the live bug.

    A maker order is placed and CANCELLED (popped from maker_orders). Its fill then
    arrives. Before the fix the fill is dropped (Ignoring unknown) so neither
    _maker_executed_base nor _pending_hedge_signed move -> no hedge. After the fix the
    fill is accounted and a hedge would fire.
    """
    h = _Harness()  # entry_side=SELL, hedge_side=BUY
    h.maker_orders["m0"] = _Tracked("m0")

    # reprice cancels the rung BEFORE it filled on-exchange
    h.process_order_canceled_event(None, None, _Ev("m0", "0"))
    assert "m0" not in h.maker_orders
    assert "m0" in h._maker_terminal_ids  # recorded as a terminal maker order

    # the cancelled rung fills on the venue after the cancel (cancel/fill race)
    _adopt_maker_filled(h, "m0", "1.0")
    fill = _Ev("m0", "1.0")
    fill.trade_type = TradeType.SELL  # an entry-side (open) SELL maker fill
    h.process_order_filled_event(None, None, fill)

    # the perp position moved -> base must be accounted and a hedge must be pending
    assert h._maker_executed_base == Decimal("1.0")
    assert h._maker_sell_base == Decimal("1.0")
    # entry SELL => signed pending moves negative (a BUY hedge would fire)
    assert h._pending_hedge_signed == Decimal("-1.0")
    needed_side = TradeType.BUY if h._pending_hedge_signed < 0 else TradeType.SELL
    assert needed_side == TradeType.BUY


# --------------------------------------------------------------------------- (b) no double-count


def test_no_double_count_tracked_fill_then_cancel_then_no_more_fills():
    """A fill counted via the maker_orders branch must NOT be re-counted on cancel.

    Fill arrives while tracked (counted once), THEN the order is cancelled (becomes
    terminal). No further fills arrive. The watermark must hold the already-credited
    base so the totals are unchanged by the terminal transition.
    """
    h = _Harness()
    h.maker_orders["m0"] = _Tracked("m0")

    # full fill while tracked -> counted once by the maker_orders branch
    fill = _Ev("m0", "1.0")
    fill.trade_type = TradeType.SELL
    h.process_order_filled_event(None, None, fill)
    assert h._maker_executed_base == Decimal("1.0")
    assert h._pending_hedge_signed == Decimal("-1.0")
    # the maker_orders branch must have seeded the watermark for a clean handoff
    assert h._maker_credited_base["m0"] == Decimal("1.0")

    # order then cancels (no remaining base) -> terminal, but nothing new to credit
    _adopt_maker_filled(h, "m0", "1.0")  # connector cumulative == already credited
    h.process_order_canceled_event(None, None, _Ev("m0", "0"))
    assert "m0" in h._maker_terminal_ids

    before_base = h._maker_executed_base
    before_pending = h._pending_hedge_signed
    # a duplicate/late fill event for the SAME cumulative must be idempotent
    dup = _Ev("m0", "1.0")
    dup.trade_type = TradeType.SELL
    h.process_order_filled_event(None, None, dup)
    assert h._maker_executed_base == before_base == Decimal("1.0")
    assert h._pending_hedge_signed == before_pending == Decimal("-1.0")


def test_partial_tracked_then_cancel_then_remainder_credits_only_delta():
    """Partial fill while tracked, cancel, then the remainder fills post-cancel.

    Only the NEW delta (remainder) is credited via the watermark, never the already-
    counted partial.
    """
    h = _Harness()
    h.maker_orders["m0"] = _Tracked("m0")

    # partial fill (0.6) while tracked
    p = _Ev("m0", "0.6")
    p.trade_type = TradeType.SELL
    h.process_order_filled_event(None, None, p)
    assert h._maker_executed_base == Decimal("0.6")
    assert h._maker_credited_base["m0"] == Decimal("0.6")

    # cancel pops it -> terminal
    h.process_order_canceled_event(None, None, _Ev("m0", "0"))
    assert "m0" in h._maker_terminal_ids

    # remainder fills post-cancel: connector cumulative is now 1.0
    _adopt_maker_filled(h, "m0", "1.0")
    r = _Ev("m0", "0.4")
    r.trade_type = TradeType.SELL
    h.process_order_filled_event(None, None, r)

    # only the 0.4 delta is added on top of the 0.6 already counted
    assert h._maker_executed_base == Decimal("1.0")
    assert h._maker_sell_base == Decimal("1.0")
    assert h._pending_hedge_signed == Decimal("-1.0")
    assert h._maker_credited_base["m0"] == Decimal("1.0")


# --------------------------------------------------------------------------- (c) CLOSE direction


def test_close_buy_maker_terminal_fill_drives_pending_toward_sell():
    """A CLOSE (BUY maker, against entry_side=SELL) terminal fill drives the hedge SELL.

    This is the exact live failure: cancelled CLOSE perp orders filled after cancel, so
    the spot SELL hedge must be queued (positive _pending_hedge_signed => SELL).
    """
    h = _Harness()  # entry_side = SELL
    # establish an open short so a BUY maker is a valid CLOSE
    _maker_fill(h, "open", "3.0")  # entry-side SELL open
    assert h._pending_hedge_signed == Decimal("-3.0")

    # a CLOSE rung (BUY) is placed then cancelled
    h.maker_orders["close0"] = _Tracked("close0")
    h.process_order_canceled_event(None, None, _Ev("close0", "0"))
    assert "close0" in h._maker_terminal_ids

    # the cancelled CLOSE fills post-cancel
    _adopt_maker_filled(h, "close0", "3.0")
    close = _Ev("close0", "3.0")
    close.trade_type = TradeType.BUY
    h.process_order_filled_event(None, None, close)

    # the BUY (close) fill drives pending toward SELL (net moves +3.0 from -3.0 to 0)
    assert h._maker_buy_base == Decimal("3.0")
    assert h._pending_hedge_signed == Decimal("0.0")
    # a fresh open BUY (no prior short) would itself drive a SELL hedge
    h.maker_orders["close1"] = _Tracked("close1")
    h.process_order_canceled_event(None, None, _Ev("close1", "0"))
    _adopt_maker_filled(h, "close1", "2.0")
    close1 = _Ev("close1", "2.0")
    close1.trade_type = TradeType.BUY
    h.process_order_filled_event(None, None, close1)
    assert h._pending_hedge_signed == Decimal("2.0")
    needed_side = TradeType.BUY if h._pending_hedge_signed < 0 else TradeType.SELL
    assert needed_side == TradeType.SELL


# --------------------------------------------------------------------------- failed-order parity


def test_failed_maker_order_late_fill_is_accounted():
    """A maker order popped on FAIL whose fill arrives must also be accounted."""
    h = _Harness()
    h.maker_orders["m0"] = _Tracked("m0")
    h.process_order_failed_event(None, None, _Ev("m0", "0"))
    assert "m0" not in h.maker_orders
    assert "m0" in h._maker_terminal_ids

    _adopt_maker_filled(h, "m0", "1.0")
    fill = _Ev("m0", "1.0")
    fill.trade_type = TradeType.SELL
    h.process_order_filled_event(None, None, fill)
    assert h._maker_executed_base == Decimal("1.0")
    assert h._pending_hedge_signed == Decimal("-1.0")


# --------------------------------------------------------------------------- fallback path


def test_terminal_maker_fill_without_connector_cumulative_uses_event_delta():
    """If the connector has no cumulative (no in-flight), fall back to watermark+event."""
    h = _Harness()
    h.maker_orders["m0"] = _Tracked("m0")
    h.process_order_canceled_event(None, None, _Ev("m0", "0"))
    # NOTE: no _adopt_maker_filled -> get_in_flight_order returns None
    assert h.get_in_flight_order(h.maker_connector, "m0") is None

    fill = _Ev("m0", "1.0")
    fill.trade_type = TradeType.SELL
    h.process_order_filled_event(None, None, fill)
    assert h._maker_executed_base == Decimal("1.0")
    assert h._pending_hedge_signed == Decimal("-1.0")


# --------------------------------------------------------------------------- eviction cap


def test_maker_terminal_cap_evicts_oldest_with_watermark():
    h = _Harness()
    h._ensure_direction_accounting()
    h._maker_credited_base["m0"] = Decimal("1")
    h._remember_terminal_maker_order("m0")
    assert "m0" in h._maker_terminal_ids

    for i in range(h._MAKER_TERMINAL_ID_CAP):
        oid = f"evict-{i}"
        h._maker_credited_base[oid] = Decimal("1")
        h._remember_terminal_maker_order(oid)

    assert "m0" not in h._maker_terminal_ids
    assert "m0" not in h._maker_credited_base
