from decimal import Decimal

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import (
    LadderMakerExecutorConfig,
    LadderRungConfig,
)


def _minimal_kwargs():
    # Reuse an existing fixture/builder if the repo already has one for this config;
    # otherwise construct the minimal required fields (maker_market, hedge_market,
    # entry_side, total_size_cap, rungs, maker_tick, hedge_tick). Keep it minimal —
    # this test only asserts the NEW defaults, not full construction semantics.
    return dict(
        timestamp=0.0,
        maker_market=ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="EWY-USD"),
        hedge_market=ConnectorPair(connector_name="kis", trading_pair="069500-KRW"),
        entry_side=TradeType.SELL,
        total_size_cap=Decimal("100"),
        rungs=[LadderRungConfig(edge_bps=Decimal("10"), size=Decimal("10"))],
        maker_tick=Decimal("0.001"),
        hedge_tick=Decimal("1"),
    )


def test_jep297_knobs_default_off_and_neutral():
    cfg = LadderMakerExecutorConfig(**_minimal_kwargs())
    assert cfg.batch_sweep_interval_s == 0.0            # sweep OFF by default
    assert cfg.batch_sweep_min_age_s == 5.0
    assert cfg.post_only_reject_backoff_count == 0      # backoff OFF by default
    assert cfg.post_only_reject_window_s == 10.0
    assert cfg.batch_disable_in_auction is True
    assert cfg.batch_margin_headroom_quote == Decimal("0")
    assert cfg.batch_max_generations_per_side == 1      # 0c cap; 1 = no over-commit
