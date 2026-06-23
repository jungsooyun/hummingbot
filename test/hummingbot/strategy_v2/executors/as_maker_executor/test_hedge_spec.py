"""Hedge-spec sizing + observe hedge_preview tests (JEP-205 Phase 3c, Task 4).

Exercises ``AsMakerExecutor._hedge_spec_for`` / ``_size_hedge`` and the display-only
``hedge_preview`` block in the observe snapshot. Everything is observe-safe: these
methods only COMPUTE a spec dict; the Slice 1 observe guard blocks any hedge order-API
call. Pure-Python fakes (no Cython runtime, no real strategy).
"""
from decimal import Decimal

from hummingbot.core.data_type.common import OrderType
from hummingbot.strategy_v2.executors.as_maker_executor.as_maker_executor import AsMakerExecutor
from hummingbot.strategy_v2.executors.as_maker_executor.data_types import AsMakerExecutorConfig
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import RungTarget, Side

from test.hummingbot.strategy_v2.executors.as_maker_executor.test_as_maker_executor import (
    FakeVolatilitySource,
    _FakeConnector,
    _FakeStrategy,
    _valid_config_kwargs,
)

# OKX hedge book: bid 99.5 / ask 100.5, ticks matched to hedge_tick=0.1 so the two-step
# quantize (compute_hedge_order ceil/floor-to-hedge_tick, then connector quantize) is
# deterministic.
OKX_BID = Decimal("99.5")
OKX_ASK = Decimal("100.5")
HEDGE_TICK = Decimal("0.1")


def _two_venue_config(**overrides):
    base = _valid_config_kwargs(
        hedge_connector_name="okx_perpetual",
        hedge_trading_pair="BTC-USDT",
        hedge_max_slippage_bps=Decimal("5"),
        hedge_tick=HEDGE_TICK,
    )
    base.update(overrides)
    return AsMakerExecutorConfig(**base)


def _make_two_venue_exec(cfg=None, okx_bid=OKX_BID, okx_ask=OKX_ASK,
                         okx_price_tick=HEDGE_TICK, okx_amount_tick=Decimal("0.0001"),
                         include_okx=True, sigma=Decimal("2"), ready=True):
    cfg = cfg or _two_venue_config()
    connectors = {
        cfg.connector_name: _FakeConnector(Decimal("99.5"), Decimal("100.5"), price_tick=Decimal("0.5")),
    }
    if include_okx:
        connectors[cfg.hedge_connector_name] = _FakeConnector(
            okx_bid, okx_ask, price_tick=okx_price_tick, amount_tick=okx_amount_tick
        )
    strat = _FakeStrategy(connectors)
    return AsMakerExecutor(strategy=strat, config=cfg, volatility_source=FakeVolatilitySource(sigma, ready))


def test_buy_hedge_price_ceil_to_tick():
    # BUY hedge walks up from best ask: ceil(100.5 * (1 + 5bps), 0.1).
    # 100.5 * 1.0005 = 100.550250 -> ceil to 0.1 -> 100.6; connector floor-quantize @0.1 -> 100.6.
    ex = _make_two_venue_exec()
    spec = ex._hedge_spec_for(Side.BUY, Decimal("0.01"))
    assert spec is not None
    assert spec["price"] == Decimal("100.6")
    assert spec["order_type"] == OrderType.LIMIT
    assert spec["metadata"] == {"order_role": "hedge"}


def test_sell_hedge_price_floor_to_tick():
    # SELL hedge walks down from best bid: floor(99.5 * (1 - 5bps), 0.1).
    # 99.5 * 0.9995 = 99.450250 -> floor to 0.1 -> 99.4; connector floor-quantize @0.1 -> 99.4.
    ex = _make_two_venue_exec()
    spec = ex._hedge_spec_for(Side.SELL, Decimal("0.01"))
    assert spec is not None
    assert spec["price"] == Decimal("99.4")


def test_hedge_spec_amount_quantized():
    ex = _make_two_venue_exec(okx_amount_tick=Decimal("0.001"))
    spec = ex._hedge_spec_for(Side.BUY, Decimal("0.0123"))
    # floor 0.0123 to 0.001 -> 0.012
    assert spec is not None
    assert spec["amount"] == Decimal("0.012")


def test_hedge_spec_none_when_book_missing():
    ex = _make_two_venue_exec(include_okx=False)
    assert ex._hedge_spec_for(Side.BUY, Decimal("0.01")) is None


def test_hedge_spec_none_when_ref_nonpositive():
    ex = _make_two_venue_exec(okx_bid=Decimal("0"), okx_ask=Decimal("0"))
    assert ex._hedge_spec_for(Side.BUY, Decimal("0.01")) is None
    assert ex._hedge_spec_for(Side.SELL, Decimal("0.01")) is None


def test_hedge_spec_none_when_amount_zero():
    # amount_tick coarser than the requested qty -> floor-quantized to 0 -> None
    ex = _make_two_venue_exec(okx_amount_tick=Decimal("1"))
    assert ex._hedge_spec_for(Side.BUY, Decimal("0.01")) is None


def test_size_hedge_buy_when_short_pending():
    ex = _make_two_venue_exec()
    ex._pending_hedge_signed = Decimal("-0.01")   # short pending -> hedge BUYs
    spec = ex._size_hedge(Decimal("0.01"))
    assert spec is not None
    assert spec["price"] == Decimal("100.6")      # BUY hedge price


def test_size_hedge_sell_when_long_pending():
    ex = _make_two_venue_exec()
    ex._pending_hedge_signed = Decimal("0.01")    # long pending -> hedge SELLs
    spec = ex._size_hedge(Decimal("0.01"))
    assert spec is not None
    assert spec["price"] == Decimal("99.4")       # SELL hedge price


def test_observe_snapshot_hedge_preview():
    ex = _make_two_venue_exec()
    targets = [
        RungTarget(side=Side.BUY, price=Decimal("99.0"), size=Decimal("0.01"), edge_bps=Decimal("0")),
        RungTarget(side=Side.SELL, price=Decimal("101.0"), size=Decimal("0.01"), edge_bps=Decimal("0")),
    ]
    ex._emit_observe_snapshot(targets)
    preview = ex._last_observe["hedge_preview"]
    assert len(preview) == 2
    # maker BUY target -> hedge SELL; maker SELL target -> hedge BUY
    by_maker = {p["maker_side"]: p for p in preview}
    assert by_maker["BUY"]["hedge_side"] == "SELL"
    assert by_maker["SELL"]["hedge_side"] == "BUY"
    assert by_maker["BUY"]["spec"] is not None
    assert by_maker["SELL"]["spec"] is not None
