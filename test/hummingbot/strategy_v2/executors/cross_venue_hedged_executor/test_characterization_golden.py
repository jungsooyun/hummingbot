"""JEP-166 slice 0: byte-stable characterization of current single-side behavior."""
from decimal import Decimal
from types import SimpleNamespace

import pytest

import hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base as mod
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


class _Inner:
    def __init__(self):
        self.is_open = True


class _Cx:
    def __init__(self, amount):
        self.amount = Decimal(str(amount))
        self.is_open = True
        self.is_filled = False
        self.executed_amount_base = Decimal("0")
        self.average_executed_price = Decimal("8")
        self.price = Decimal("8")
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
    def __init__(self, entry_side):
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
        self._hedge_kill_switch = False
        self._status = RunnableStatus.RUNNING
        self.close_type = None
        self.config = SimpleNamespace(type="characterization_golden", execution_purpose="characterization_golden")
        self.maker_connector = "hl"
        self.maker_trading_pair = "X-USD"
        self.hedge_connector = "kis"
        self.hedge_trading_pair = "005930-KRW"
        self.entry_side = entry_side
        self.hedge_side = TradeType.BUY if entry_side == TradeType.SELL else TradeType.SELL
        self.placed = []
        self.connector_orders = {}

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
        self.placed.append(
            {
                "order_id": oid,
                "side": kw["side"],
                "amount": kw["amount"],
                "price": kw["price"],
                "metadata": kw["metadata"],
            }
        )
        self.connector_orders[oid] = _Cx(kw["amount"])
        return oid


class _Ev:
    def __init__(self, order_id, amount, price="10"):
        self.order_id = order_id
        self.amount = Decimal(str(amount))
        self.price = Decimal(str(price))


@pytest.fixture(autouse=True)
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


def _maker_fill(h, oid, amt, price="10"):
    h.maker_orders[oid] = _Tracked(oid)
    h.process_order_filled_event(None, None, _Ev(oid, amt, price))


def _created(h, oid):
    h.hedge_orders[oid].order = _Inner()


def _norm(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, TradeType):
        return value.name
    if isinstance(value, RunnableStatus):
        return value.name
    if isinstance(value, CloseType):
        return value.name
    if isinstance(value, dict):
        return {key: _norm(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_norm(item) for item in value]
    return value


def _snapshot(h, label):
    size = h._size_hedge(h._pending_hedge_base)
    return _norm(
        {
            "label": label,
            "_pending_hedge_base": h._pending_hedge_base,
            "_maker_executed_base": h._maker_executed_base,
            "_hedge_executed_base": h._hedge_executed_base,
            "_unhedged_base": h._unhedged_base(),
            "_unhedged_base_signed": h._unhedged_base_signed(),
            "get_net_pnl_quote": h.get_net_pnl_quote(),
            "get_custom_info": h.get_custom_info(),
            "_size_hedge": size,
            "_current_retries": h._current_retries,
            "_hedge_kill_switch": h._hedge_kill_switch,
            "status": h.status,
            "close_type": h.close_type,
            "placed": h.placed,
            "hedge_order_ids": sorted(h.hedge_orders),
        }
    )


def _normal(entry_side):
    h = _Harness(entry_side)
    out = []
    _maker_fill(h, "m0", "2", "10")
    out.append(_snapshot(h, "maker_fill"))
    h._process_hedges()
    out.append(_snapshot(h, "process_hedges"))
    _created(h, "h0")
    h.process_order_filled_event(None, None, _Ev("h0", "2", "8"))
    h.hedge_orders["h0"].order.is_open = False
    out.append(_snapshot(h, "hedge_full_fill"))
    return out


def _partial(entry_side):
    h = _Harness(entry_side)
    out = []
    _maker_fill(h, "m0", "2", "10")
    h._process_hedges()
    out.append(_snapshot(h, "process_hedges"))
    _created(h, "h0")
    h.process_order_filled_event(None, None, _Ev("h0", "0.75", "8"))
    out.append(_snapshot(h, "hedge_partial_fill"))
    return out


def _cancel(entry_side):
    h = _Harness(entry_side)
    out = []
    _maker_fill(h, "m0", "2", "10")
    h._process_hedges()
    out.append(_snapshot(h, "process_hedges"))
    _created(h, "h0")
    h.hedge_orders["h0"].order.is_open = False
    h.process_order_canceled_event(None, None, _Ev("h0", "0", "8"))
    out.append(_snapshot(h, "hedge_cancel"))
    return out


def _retries(entry_side):
    h = _Harness(entry_side)
    out = []
    _maker_fill(h, "m0", "2", "10")
    for i in range(4):
        h.hedge_orders[f"f{i}"] = _Tracked(f"f{i}")
        h.process_order_failed_event(None, None, _Ev(f"f{i}", "0", "8"))
        out.append(_snapshot(h, f"hedge_failure_{i + 1}"))
    return out


def _reconcile(entry_side):
    h = _Harness(entry_side)
    out = []
    _maker_fill(h, "m0", "2", "10")
    h._process_hedges()
    out.append(_snapshot(h, "stuck_before_reconcile"))
    cx = h.connector_orders["h0"]
    cx.executed_amount_base = Decimal("1")
    h._reconcile_stuck_hedges()
    out.append(_snapshot(h, "stuck_after_reconcile"))
    return out


def _placed(side):
    return [{"order_id": "h0", "side": side, "amount": "2", "price": "8", "metadata": {"order_role": "hedge"}}]


def _golden_snap(
    label,
    side,
    pending,
    maker,
    hedge,
    unhedged,
    signed,
    pnl,
    size,
    retries=0,
    kill=False,
    placed=None,
    hedge_ids=None,
    open_hedge_orders=0,
    status="RUNNING",
    close_type=None,
):
    placed = [] if placed is None else placed
    hedge_ids = [] if hedge_ids is None else hedge_ids
    return {
        "label": label,
        "_pending_hedge_base": pending,
        "_maker_executed_base": maker,
        "_hedge_executed_base": hedge,
        "_unhedged_base": unhedged,
        "_unhedged_base_signed": signed,
        "get_net_pnl_quote": pnl,
        "get_custom_info": {
            "side": side,
            "execution_purpose": "characterization_golden",
            "maker_connector": "hl",
            "maker_trading_pair": "X-USD",
            "hedge_connector": "kis",
            "hedge_trading_pair": "005930-KRW",
            "maker_executed_base": maker,
            "hedge_executed_base": hedge,
            "unhedged_base": unhedged,
            "pending_hedge_base": pending,
            "open_maker_orders": 0,
            "open_hedge_orders": open_hedge_orders,
            "hedge_kill_switch": kill,
        },
        "_size_hedge": size,
        "_current_retries": retries,
        "_hedge_kill_switch": kill,
        "status": status,
        "close_type": close_type,
        "placed": placed,
        "hedge_order_ids": hedge_ids,
    }


SIZE_2 = {"amount": "2", "price": "8", "order_type": None, "metadata": {}}
SIZE_1 = {"amount": "1", "price": "8", "order_type": None, "metadata": {}}


def _side_goldens(side, signed, hedge_side, pnl_sign):
    process = _golden_snap("process_hedges", side, "2", "2", "0", "2", signed("2"), "0", SIZE_2, placed=_placed(hedge_side), hedge_ids=["h0"])
    return {
        "normal": [
            _golden_snap("maker_fill", side, "2", "2", "0", "2", signed("2"), "0", SIZE_2),
            process,
            _golden_snap("hedge_full_fill", side, "0", "2", "2", "0", "0", pnl_sign("4"), None, placed=_placed(hedge_side), hedge_ids=["h0"]),
        ],
        "partial": [
            process,
            _golden_snap(
                "hedge_partial_fill",
                side,
                "1.25",
                "2",
                "0.75",
                "1.25",
                signed("1.25"),
                pnl_sign("1.50"),
                SIZE_1,
                placed=_placed(hedge_side),
                hedge_ids=["h0"],
                open_hedge_orders=1,
            ),
        ],
        "cancel": [
            process,
            _golden_snap("hedge_cancel", side, "2", "2", "0", "2", signed("2"), "0", SIZE_2, placed=_placed(hedge_side)),
        ],
        "retries": [
            _golden_snap(f"hedge_failure_{idx}", side, "2", "2", "0", "2", signed("2"), "0", SIZE_2, retries=idx, kill=(idx == 4))
            for idx in range(1, 5)
        ],
        "reconcile": [
            _golden_snap(
                "stuck_before_reconcile",
                side,
                "2",
                "2",
                "0",
                "2",
                signed("2"),
                "0",
                SIZE_2,
                placed=_placed(hedge_side),
                hedge_ids=["h0"],
            ),
            _golden_snap(
                "stuck_after_reconcile",
                side,
                "1",
                "2",
                "1",
                "1",
                signed("1"),
                pnl_sign("2"),
                SIZE_1,
                placed=_placed(hedge_side),
                hedge_ids=["h0"],
                open_hedge_orders=1,
            ),
        ],
    }


_SELL = _side_goldens("SELL", lambda value: f"-{value}", "BUY", lambda value: value)
_BUY = _side_goldens("BUY", lambda value: value, "SELL", lambda value: f"-{value}")
GOLDEN = {
    ("SELL", scenario): snapshots for scenario, snapshots in _SELL.items()
} | {
    ("BUY", scenario): snapshots for scenario, snapshots in _BUY.items()
}


@pytest.mark.parametrize("entry_side", [TradeType.SELL, TradeType.BUY])
@pytest.mark.parametrize(
    "scenario,driver",
    [
        ("normal", _normal),
        ("partial", _partial),
        ("cancel", _cancel),
        ("retries", _retries),
        ("reconcile", _reconcile),
    ],
)
def test_cross_venue_single_direction_characterization_golden(entry_side, scenario, driver):
    assert driver(entry_side) == GOLDEN[(entry_side.name, scenario)]
