"""JEP-187 keystone: real forward fair path (KRW book + side-aware FX -> fair -> ladder).

The pre-extraction guard for the FairPriceProvider seam. No existing executor-level
test drives the real `_compute_fair` (all mock/stub it), so this golden is the SOLE
forward-path pin. It mocks `_get_fx` (today on the executor; Task 3 repoints to
`ex._fair._get_fx`) to a SPREAD FX so side-aware selection is exercised, then asserts
the exact side-aware fair (BUY=kis_bid/fx_ask, SELL=kis_ask/fx_bid) AND the full
quantized ladder.
"""
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import Side

    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    Side = None
    _EXECUTOR_IMPORTABLE = False

KIS_BID = Decimal("1920000")
KIS_ASK = Decimal("2000000")
FX_BID = Decimal("1250")
FX_ASK = Decimal("1280")


def _make_executor():
    ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
    ex.config = MagicMock(
        observe=False,
        side_aware_fx=True,
        target_inventory=Decimal("0"),
        inventory_skew_bps_per_unit=Decimal("0"),
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
    ex.hedge_connector = "kis"
    ex.hedge_trading_pair = "000660-KRW"
    ex.maker_connector = "hyperliquid_perpetual"
    ex.maker_trading_pair = "XYZ:SKHX-USD"
    ex.entry_side = TradeType.SELL

    hedge_conn = MagicMock()

    def _price(pair, price_type):
        return KIS_BID if price_type == PriceType.BestBid else KIS_ASK

    hedge_conn.get_price_by_type.side_effect = _price
    maker_conn = MagicMock()
    maker_conn.quantize_order_amount.side_effect = lambda pair, amt: amt
    maker_conn.quantize_order_price.side_effect = lambda pair, price: price
    ex.connectors = {"kis": hedge_conn, "hyperliquid_perpetual": maker_conn}

    # Spread FX: mocked at the CURRENT _get_fx location (Task 3 repoints to ex._fair._get_fx).
    from hummingbot.strategy_v2.executors.ladder_maker_executor.fx_bridged_fair_source import FxBridgedFairSource

    ex._fair = FxBridgedFairSource(
        getattr(ex.config, "side_aware_fx", True),
        getattr(ex.config, "static_fx_rate", None),
        LadderMakerExecutor.logger(),
    )
    ex._fair._get_fx = MagicMock(return_value=(FX_BID, FX_ASK))
    ex._unhedged_base_signed = MagicMock(return_value=Decimal("0"))
    ex._policy_side = MagicMock(return_value=Side.SELL)
    ex._maker_executed_base = Decimal("0")
    ex._strategy = SimpleNamespace(current_timestamp=1000.0)
    ex.maker_orders = {}
    ex.place_order = MagicMock(side_effect=["OID-1", "OID-2"])
    return ex


def _norm(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (TradeType, PositionAction, OrderType)):
        return value.name
    if isinstance(value, dict):
        return {k: _norm(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_norm(v) for v in value]
    return value


GOLDEN_SELL = [
    {
        "connector_name": "hyperliquid_perpetual",
        "trading_pair": "XYZ:SKHX-USD",
        "order_type": "LIMIT_MAKER",
        "side": "SELL",
        "amount": "0.01",
        "position_action": "OPEN",
        "price": "1620.0",
        "metadata": {"order_role": "maker", "edge_bps": "100"},
    },
    {
        "connector_name": "hyperliquid_perpetual",
        "trading_pair": "XYZ:SKHX-USD",
        "order_type": "LIMIT_MAKER",
        "side": "SELL",
        "amount": "0.02",
        "position_action": "OPEN",
        "price": "1700.0",
        "metadata": {"order_role": "maker", "edge_bps": "600"},
    },
]


@pytest.mark.skipif(not _EXECUTOR_IMPORTABLE, reason="ladder_maker_executor requires the V2 stack")
def test_keystone_fair_side_aware_direction():
    ex = _make_executor()
    # Zero skew -> _compute_fair == compute_fair_price exactly. Pins the JEP-185 forward pairing.
    assert ex._compute_fair(Side.SELL) == Decimal("1600")  # kis_ask / fx_bid
    assert ex._compute_fair(Side.BUY) == Decimal("1500")   # kis_bid / fx_ask


@pytest.mark.skipif(not _EXECUTOR_IMPORTABLE, reason="ladder_maker_executor requires the V2 stack")
def test_keystone_fair_to_ladder_targets_golden():
    ex = _make_executor()
    targets = ex._compute_targets()
    ex._place_targets(targets)
    placed = [_norm(call.kwargs) for call in ex.place_order.call_args_list]
    assert placed == GOLDEN_SELL
