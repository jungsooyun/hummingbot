"""JEP-166 base hardening — fixes from the adversarial review of the direction-aware base.

Covers three findings:
  * CRITICAL: hedge fills credited shares into a units-denominated signed pending queue
    (only safe when share_per_unit == 1; the old non-negative pending masked it by clamping).
  * HIGH: open-edge basis hardcoded SELL=open / BUY=close (wrong for a BUY-open executor).
  * HIGH: order completion popped the recorded hedge side but left the order tracked, so a
    stray post-completion fill credited at the default side (wrong after a sign flip).
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

    @property
    def cum_fees_quote(self):
        return Decimal("0")


class _Harness(CrossVenueHedgedExecutorBase):
    def __init__(self, entry_side=TradeType.SELL, share_per_unit=Decimal("1")):
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
        self.entry_side = entry_side
        self.hedge_side = TradeType.BUY if entry_side == TradeType.SELL else TradeType.SELL
        self._hedge_order_side = {}
        self._maker_placed_edge_bps = {}
        self._share_per_unit = share_per_unit

    def _hedge_base_to_maker_base(self, amount):
        return amount / self._share_per_unit

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
    def __init__(self, order_id, amount, price="1", trade_type=None):
        self.order_id = order_id
        self.amount = Decimal(str(amount))
        self.price = Decimal(str(price))
        self.trade_type = trade_type


@pytest.fixture(autouse=True)
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


def _maker_fill(h, oid, amount, side, price="1", edge_bps="0"):
    h.maker_orders[oid] = _Tracked(oid)
    h._maker_placed_edge_bps[oid] = Decimal(str(edge_bps))
    h.process_order_filled_event(None, None, _Ev(oid, amount, price, trade_type=side))


def _hedge_fill(h, oid, amount, side, price="1"):
    h.hedge_orders[oid] = _Tracked(oid)
    h._hedge_order_side[oid] = side
    h.process_order_filled_event(None, None, _Ev(oid, amount, price))


# ---- CRITICAL: share_per_unit unit conversion on the signed ledgers ----

def test_hedge_fill_credits_maker_units_not_raw_shares():
    # share_per_unit=2: 2 KIS shares == 1 perp unit. Without the conversion the signed
    # pending would go -1 + 2 = +1 (wrong sign -> spurious reverse hedge); with it, -1 + 1 = 0.
    h = _Harness(share_per_unit=Decimal("2"))
    _maker_fill(h, "m0", "1", TradeType.SELL)          # short 1 perp unit
    assert h._perp_net() == Decimal("-1")
    assert h._pending_hedge_signed == Decimal("-1")

    _hedge_fill(h, "h0", "2", TradeType.BUY)            # buy 2 shares == 1 unit hedge
    assert h._spot_net() == Decimal("1")               # maker units, not 2 raw shares
    assert h._pending_hedge_signed == Decimal("0")     # fully hedged, no sign flip
    assert h._unhedged_base_signed() == Decimal("0")   # delta neutral
    assert h._hedge_executed_base == Decimal("2")      # notional/legacy keeps raw shares


def test_identity_conversion_is_default():
    h = _Harness()  # share_per_unit == 1
    _maker_fill(h, "m0", "1", TradeType.SELL)
    _hedge_fill(h, "h0", "1", TradeType.BUY)
    assert h._spot_net() == Decimal("1")
    assert h._pending_hedge_signed == Decimal("0")


# ---- HIGH: open-edge basis keyed off entry_side ----

def test_open_edge_tracks_buy_open_for_buy_entry_executor():
    h = _Harness(entry_side=TradeType.BUY)
    _maker_fill(h, "m0", "2", TradeType.BUY, edge_bps="10")   # open long, basis 10
    assert h._open_edge_vwap == Decimal("10")

    _maker_fill(h, "m1", "1", TradeType.SELL, edge_bps="99")  # close 1 (avg-cost), edge ignored
    assert h._open_edge_vwap == Decimal("10")

    _maker_fill(h, "m2", "1", TradeType.SELL, edge_bps="99")  # full close -> reset
    assert h._open_edge_vwap == Decimal("0")


# ---- HIGH: completion drops the hedge from both books ----

def test_completion_pops_hedge_order_so_stray_fill_is_noop():
    h = _Harness()
    _maker_fill(h, "m0", "1", TradeType.SELL)
    _hedge_fill(h, "h0", "1", TradeType.BUY)
    assert h._spot_net() == Decimal("1")

    h.process_order_completed_event(None, None, _Ev("h0", "1"))
    assert "h0" not in h.hedge_orders
    assert "h0" not in h._hedge_order_side

    # A stray/duplicate post-completion fill must NOT credit again at the default side.
    h.process_order_filled_event(None, None, _Ev("h0", "1"))
    assert h._spot_net() == Decimal("1")               # unchanged
    assert h._pending_hedge_signed == Decimal("0")
