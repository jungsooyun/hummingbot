"""Two-venue (HL maker + OKX hedge) wiring tests (JEP-205 Phase 3c, Task 4).

Asserts the executor now wires DISTINCT maker/hedge markets (the degenerate
single-venue wiring is removed), constructs a ``PerpHedgeInventory`` seam, keeps A&S
pricing reading the MAKER connector only, and that the config round-trips through the
``AnyExecutorConfig`` discriminated union with the new hedge fields present.
"""
from decimal import Decimal

from hummingbot.strategy_v2.executors.as_maker_executor.as_maker_executor import AsMakerExecutor
from hummingbot.strategy_v2.executors.as_maker_executor.data_types import AsMakerExecutorConfig
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.inventory_adapter import PerpHedgeInventory
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

from test.hummingbot.strategy_v2.executors.as_maker_executor.test_as_maker_executor import (
    FakeVolatilitySource,
    _FakeConnector,
    _FakeStrategy,
    _valid_config_kwargs,
)


def _two_venue_config(**overrides):
    base = _valid_config_kwargs(
        hedge_connector_name="okx_perpetual",
        hedge_trading_pair="BTC-USDT",
    )
    base.update(overrides)
    return AsMakerExecutorConfig(**base)


def _make_two_venue_exec(cfg=None):
    cfg = cfg or _two_venue_config()
    connectors = {
        # Maker (HL): mid = 100.
        cfg.connector_name: _FakeConnector(Decimal("99.5"), Decimal("100.5"), price_tick=Decimal("0.5")),
        # Hedge (OKX): deliberately DIFFERENT prices so we can prove _mid_price ignores it.
        cfg.hedge_connector_name: _FakeConnector(Decimal("199.0"), Decimal("201.0"), price_tick=Decimal("0.1")),
    }
    strat = _FakeStrategy(connectors)
    return AsMakerExecutor(strategy=strat, config=cfg, volatility_source=FakeVolatilitySource(Decimal("2"), True))


def test_two_venue_markets_distinct():
    ex = _make_two_venue_exec()
    assert ex.maker_connector == "hyperliquid_perpetual"
    assert ex.hedge_connector == "okx_perpetual"
    assert ex.maker_connector != ex.hedge_connector
    assert ex.maker_trading_pair == "BTC-USD"
    assert ex.hedge_trading_pair == "BTC-USDT"
    assert ex.maker_trading_pair != ex.hedge_trading_pair


def test_inventory_adapter_constructed():
    ex = _make_two_venue_exec()
    assert isinstance(ex._inventory, PerpHedgeInventory)


def test_mid_price_reads_maker_only():
    ex = _make_two_venue_exec()
    # Maker mid = (99.5 + 100.5) / 2 = 100; OKX mid would be 200. Confirm maker is used.
    assert ex._mid_price() == Decimal("100")


def test_config_roundtrip_through_any_executor_config():
    cfg = _two_venue_config()
    info = ExecutorInfo(
        id="e1",
        timestamp=0.0,
        type="as_maker_executor",
        status=RunnableStatus.NOT_STARTED,
        config=cfg.dict(),
        net_pnl_pct=Decimal("0"),
        net_pnl_quote=Decimal("0"),
        cum_fees_quote=Decimal("0"),
        filled_amount_quote=Decimal("0"),
        is_active=False,
        is_trading=False,
        custom_info={},
    )
    assert isinstance(info.config, AsMakerExecutorConfig)
    assert info.config.type == "as_maker_executor"
    assert info.config.hedge_connector_name == "okx_perpetual"
    assert info.config.hedge_trading_pair == "BTC-USDT"
    assert info.config.hedge_max_slippage_bps == Decimal("5")
    assert info.config.hedge_tick == Decimal("0.1")
