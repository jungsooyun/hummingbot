from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from hummingbot.core.data_type.common import TradeType

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import RungTarget, Side
    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    RungTarget = None
    Side = None
    _EXECUTOR_IMPORTABLE = False


pytestmark = pytest.mark.skipif(
    not _EXECUTOR_IMPORTABLE,
    reason="ladder_maker_executor requires the V2 stack (paho) - run in Docker/CI",
)


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


def _target(side, price, size="1", edge="100"):
    return RungTarget(side=side, price=Decimal(str(price)), size=Decimal(str(size)), edge_bps=Decimal(str(edge)))


def _make_executor(targets, observe=False, two_sided=True):
    ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
    ex.config = SimpleNamespace(
        observe=observe,
        two_sided=two_sided,
        maker_tick=Decimal("0.01"),
        min_reprice_delta_ticks=Decimal("2"),
        min_reprice_interval_s=0,
    )
    ex.maker_connector = "hyperliquid_perpetual"
    ex.maker_trading_pair = "XYZ:SKHX-USD"
    ex.hedge_connector = "kis"
    ex.hedge_trading_pair = "000660-KRW"
    ex.entry_side = TradeType.SELL
    conn = MagicMock()
    conn.quantize_order_amount.side_effect = lambda pair, amt: amt
    conn.quantize_order_price.side_effect = lambda pair, price: price
    ex.connectors = {ex.maker_connector: conn, ex.hedge_connector: conn}
    ex._strategy = SimpleNamespace(current_timestamp=1000.0, cancel=MagicMock())
    ex.place_order = MagicMock(side_effect=lambda **kw: f"OID-{len(ex.maker_orders)}")
    ex._last_reprice_ts = 0.0
    ex.maker_orders = {}
    ex._maker_placed_edge_bps = {}
    ex._compute_targets = MagicMock(return_value=targets)
    ex._should_reprice = MagicMock(return_value=True)
    ex._last_observe = None
    ex._last_observe_log_ts = 0.0
    ex._build_observe = MagicMock(
        return_value={
            "side": "SELL",
            "fair": "50.00",
            "spot_pair": ex.hedge_trading_pair,
            "spot_bid": "50.00",
            "spot_ask": "50.01",
            "fx_bid": "1",
            "fx_ask": "1",
            "rungs": [{"price": str(t.price), "edge_bps": str(t.edge_bps)} for t in targets],
        }
    )
    ex.logger = MagicMock(return_value=MagicMock())
    return ex


def _live(ex, oid, price, amount="1", trade_type=TradeType.SELL):
    ex.maker_orders[oid] = _Tracked(oid, _Order(oid, price, amount, trade_type))


def _cancelled_ids(ex):
    return [c.args[2] for c in ex._strategy.cancel.call_args_list]


def test_unchanged_rung_not_cancelled():
    ex = _make_executor([_target(Side.SELL, "50.10")])
    _live(ex, "a", "50.10")

    ex._reconcile_maker()

    ex._strategy.cancel.assert_not_called()
    ex.place_order.assert_not_called()


def test_only_moved_rung_reprices():
    ex = _make_executor([_target(Side.SELL, "50.10"), _target(Side.SELL, "50.40")])
    _live(ex, "a", "50.10")
    _live(ex, "b", "50.20")

    ex._reconcile_maker()

    assert _cancelled_ids(ex) == ["b"]
    ex.place_order.assert_called_once()
    assert ex.place_order.call_args.kwargs["price"] == Decimal("50.40")


def test_no_live_orders_places_all():
    ex = _make_executor([_target(Side.SELL, "50.10"), _target(Side.SELL, "50.20")])

    ex._reconcile_maker()

    ex._strategy.cancel.assert_not_called()
    assert ex.place_order.call_count == 2


def test_inflight_maker_rung_not_double_placed():
    ex = _make_executor([_target(Side.BUY, "50.10")])

    ex._reconcile_maker()
    ex._reconcile_maker()

    ex._strategy.cancel.assert_not_called()
    ex.place_order.assert_called_once()
    assert list(ex.maker_orders) == ["OID-0"]
    assert ex.maker_orders["OID-0"].order is None


def test_untracked_maker_not_cancelled():
    ex = _make_executor([])
    ex.maker_orders["a"] = _Tracked("a", None)

    ex._reconcile_maker()

    assert "a" not in _cancelled_ids(ex)


def test_observe_mode_no_submit():
    ex = _make_executor([_target(Side.SELL, "50.40")], observe=True)
    _live(ex, "a", "50.10")

    ex._reconcile_maker()

    ex.place_order.assert_not_called()
    ex._strategy.cancel.assert_not_called()
    assert ex._last_observe is not None


def test_single_sided_reprice_set_unchanged():
    targets = [_target(Side.SELL, "50.40"), _target(Side.SELL, "50.50")]
    ex = _make_executor(targets, two_sided=False)
    _live(ex, "a", "50.10")
    _live(ex, "b", "50.20")

    ex._reconcile_maker()

    assert _cancelled_ids(ex) == ["a", "b"]
    assert ex.place_order.call_count == 2


def test_opposite_side_same_price_size_not_matched():
    ex = _make_executor([_target(Side.BUY, "50.10")])
    _live(ex, "sell-open", "50.10", trade_type=TradeType.SELL)

    ex._reconcile_maker()

    assert _cancelled_ids(ex) == ["sell-open"]
    ex.place_order.assert_called_once()
    assert ex.place_order.call_args.kwargs["side"] == TradeType.BUY
    assert ex.place_order.call_args.kwargs["price"] == Decimal("50.10")
