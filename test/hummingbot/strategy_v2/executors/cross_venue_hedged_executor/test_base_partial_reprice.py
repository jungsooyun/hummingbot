"""JEP-145 Task 2: the generic partial-reprice now lives in the base.

``CrossVenueHedgedExecutorBase._reconcile_maker`` reprices only the rungs that
actually changed (selective cancel/replace) instead of cancel-all + place-all, so
ANY hedge executor (incl. the single-sided ladder path, and a future XEMM) inherits
it. These tests drive the BASE method directly through a minimal harness that
subclasses the base — no ladder override, no Cython runtime beyond the base import.

Acceptance (from the JEP-145 spec): 0 rungs changed -> 0 cancel / 0 place; one rung
moved beyond the reprice threshold -> exactly 1 cancel + 1 place (the unchanged rung
is left untouched); a just-placed rung (``order is None``, inflight) is NOT
double-placed on the next tick.
"""
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import RungTarget, Side


class _Order:
    def __init__(self, order_id, price, amount, trade_type):
        self.order_id = order_id
        self.price = Decimal(str(price))
        self.amount = Decimal(str(amount))
        self.trade_type = trade_type
        self.is_open = True


class _Tracked:
    def __init__(self, order_id, order=None):
        self.order_id = order_id
        self.order = order


class _Harness(CrossVenueHedgedExecutorBase):
    """Minimal concrete subclass of the base exercising only the maker-reconcile path."""

    def __init__(self, targets, observe=False, two_sided=False):
        self.config = SimpleNamespace(
            observe=observe,
            two_sided=two_sided,
            maker_tick=Decimal("0.01"),
            min_reprice_delta_ticks=Decimal("2"),
        )
        self.maker_connector = "hl"
        self.maker_trading_pair = "X-USD"
        self.hedge_connector = "kis"
        self.hedge_trading_pair = "005930-KRW"
        self.entry_side = TradeType.SELL
        self.hedge_side = TradeType.BUY
        conn = MagicMock()
        conn.quantize_order_amount.side_effect = lambda pair, amt: amt
        conn.quantize_order_price.side_effect = lambda pair, price: price
        self.connectors = {self.maker_connector: conn, self.hedge_connector: conn}
        self._strategy = SimpleNamespace(current_timestamp=1000.0, cancel=MagicMock())
        self.maker_orders = {}
        self._maker_placed_edge_bps = {}
        self._last_reprice_ts = 0.0
        self._placed = []
        self._targets = targets
        self._reprice = True

    # ---- abstract hooks ----
    def _gates_open(self):
        return True

    def _compute_targets(self):
        return self._targets

    def _should_reprice(self, targets):
        return self._reprice

    def _place_targets(self, targets):
        for target in targets:
            self._place_target_one(target)

    def _size_hedge(self, pending_base):
        return None

    def _maker_balance_candidate(self):
        return None

    # ---- placement hook (base soft hook) ----
    def _place_maker(self, price, amount, edge_bps, side=None, position_action=None):
        # Mimic a MINIMAL subclass: a just-placed maker is inflight (order is None until its
        # created event). Deliberately does NOT record _maker_placed_rung — the base must
        # record it from the returned order id, so the inflight double-place guard holds for
        # any subclass that only returns its placed id (JEP-145 base-enforced contract).
        side = side if side is not None else self.entry_side
        oid = f"OID-{len(self.maker_orders)}"
        self.maker_orders[oid] = _Tracked(oid, order=None)
        self._placed.append({"oid": oid, "side": side, "price": Decimal(str(price)), "amount": Decimal(str(amount))})
        return oid


def _t(side, price, size="1", edge="100"):
    return RungTarget(side=side, price=Decimal(str(price)), size=Decimal(str(size)), edge_bps=Decimal(str(edge)))


def _live(h, oid, price, amount="1", trade_type=TradeType.SELL):
    h.maker_orders[oid] = _Tracked(oid, _Order(oid, price, amount, trade_type))


def _cancelled(h):
    return [c.args[2] for c in h._strategy.cancel.call_args_list]


def test_unchanged_targets_no_cancel_no_place():
    # Within the tick threshold -> no churn at all.
    h = _Harness([_t(Side.SELL, "50.10")])
    _live(h, "a", "50.10")

    h._reconcile_maker()

    assert _cancelled(h) == []
    assert h._placed == []


def test_one_rung_moved_one_cancel_one_place():
    # Best rung moved 30 ticks (> 2-tick threshold) -> exactly 1 cancel + 1 place;
    # the matched rung (50.20) is left untouched.
    h = _Harness([_t(Side.SELL, "50.40"), _t(Side.SELL, "50.20")])
    _live(h, "a", "50.10")
    _live(h, "b", "50.20")

    h._reconcile_maker()

    assert _cancelled(h) == ["a"]
    assert len(h._placed) == 1
    assert h._placed[0]["price"] == Decimal("50.40")


def test_inflight_rung_not_double_placed():
    # First tick places OID-0 inflight (order is None); the second tick must see it
    # via _maker_placed_rung and NOT re-place it.
    h = _Harness([_t(Side.SELL, "50.10")])

    h._reconcile_maker()
    h._reconcile_maker()

    assert _cancelled(h) == []
    assert len(h._placed) == 1
    assert list(h.maker_orders) == ["OID-0"]
    assert h.maker_orders["OID-0"].order is None


def test_no_live_orders_places_all():
    h = _Harness([_t(Side.SELL, "50.10"), _t(Side.SELL, "50.20")])

    h._reconcile_maker()

    assert _cancelled(h) == []
    assert len(h._placed) == 2


def test_base_records_rung_from_returned_id_for_inflight_safety():
    # Contract: a subclass that ONLY returns its placed id (no _maker_placed_rung recording)
    # is still inflight double-place safe — the base records the rung from the returned id.
    h = _Harness([_t(Side.SELL, "50.10")])

    h._reconcile_maker()

    # base lazily created _maker_placed_rung and recorded the inflight rung, quantized.
    assert h._maker_placed_rung == {"OID-0": (Side.SELL, Decimal("50.10"), Decimal("1"))}


def test_filled_maker_rung_pruned_to_bound_growth():
    # A filled maker order stays in maker_orders (pre-existing retention) but its rung is
    # never read again (only inflight rungs are), so the prune must drop it — bounding
    # _maker_placed_rung to the active (inflight + open) order count, not the maker_orders leak.
    h = _Harness([_t(Side.SELL, "50.10")])
    h._reconcile_maker()
    assert "OID-0" in h._maker_placed_rung

    # Simulate the maker filling: order populated, no longer open. Nothing new to place.
    filled = _Order("OID-0", "50.10", "1", TradeType.SELL)
    filled.is_open = False
    h.maker_orders["OID-0"].order = filled
    h._targets = []

    h._reconcile_maker()

    assert "OID-0" not in h._maker_placed_rung
