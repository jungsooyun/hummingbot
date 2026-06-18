"""JEP-166 slice 0: current ladder target-to-maker intent characterization."""
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor

    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    _EXECUTOR_IMPORTABLE = False


def _make_executor():
    ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
    ex.config = MagicMock(
        observe=False,
        rungs=[
            SimpleNamespace(edge_bps=Decimal("100"), size=Decimal("0.01"), min_edge_bps=Decimal("0"), enabled=True),
            SimpleNamespace(edge_bps=Decimal("600"), size=Decimal("0.02"), min_edge_bps=Decimal("0"), enabled=True),
        ],
        total_size_cap=Decimal("0.03"),
        maker_tick=Decimal("0.1"),
        buffer_ticks=Decimal("0"),
        max_inventory=Decimal("100"),
        round_trip_cost_bps=Decimal("25"),
    )
    ex.maker_connector = "hyperliquid_perpetual"
    ex.maker_trading_pair = "XYZ:SKHX-USD"
    ex.entry_side = TradeType.SELL
    conn = MagicMock()
    conn.quantize_order_amount.side_effect = lambda pair, amt: amt
    conn.quantize_order_price.side_effect = lambda pair, price: price
    ex.connectors = {"hyperliquid_perpetual": conn}
    ex._compute_fair = MagicMock(return_value=Decimal("1595.2"))
    ex._policy_side = MagicMock(return_value=None)
    ex._unhedged_base_signed = MagicMock(return_value=Decimal("0"))
    ex._maker_executed_base = Decimal("0")
    ex._strategy = SimpleNamespace(current_timestamp=1000.0)
    ex.maker_orders = {}
    ex.place_order = MagicMock(side_effect=["OID-1", "OID-2"])
    return ex


def _norm(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, TradeType):
        return value.name
    if isinstance(value, PositionAction):
        return value.name
    if isinstance(value, OrderType):
        return value.name
    if isinstance(value, dict):
        return {key: _norm(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_norm(item) for item in value]
    return value


GOLDEN = {
    "SELL": [
        {
            "connector_name": "hyperliquid_perpetual",
            "trading_pair": "XYZ:SKHX-USD",
            "order_type": "LIMIT_MAKER",
            "side": "SELL",
            "amount": "0.01",
            "position_action": "OPEN",
            "price": "1615.2",
            "metadata": {"order_role": "maker", "edge_bps": "100"},
        },
        {
            "connector_name": "hyperliquid_perpetual",
            "trading_pair": "XYZ:SKHX-USD",
            "order_type": "LIMIT_MAKER",
            "side": "SELL",
            "amount": "0.02",
            "position_action": "OPEN",
            "price": "1694.9",
            "metadata": {"order_role": "maker", "edge_bps": "600"},
        },
    ]
}


@pytest.mark.skipif(not _EXECUTOR_IMPORTABLE, reason="ladder_maker_executor requires the V2 stack")
def test_ladder_compute_targets_to_place_targets_intent_golden():
    ex = _make_executor()
    targets = ex._compute_targets()
    ex._place_targets(targets)
    placed = [_norm(call.kwargs) for call in ex.place_order.call_args_list]
    assert placed == GOLDEN["SELL"]
