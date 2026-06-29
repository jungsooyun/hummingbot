from decimal import Decimal
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import (
    LadderMakerExecutorConfig,
    LadderRungConfig,
)


def _cfg(**kw):
    base = dict(
        timestamp=0.0,
        maker_market=ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="EWY-USD"),
        hedge_market=ConnectorPair(connector_name="kis", trading_pair="069500-KRW"),
        entry_side=TradeType.SELL,
        total_size_cap=Decimal("100"),
        rungs=[LadderRungConfig(edge_bps=Decimal("10"), size=Decimal("10"))],
        maker_tick=Decimal("0.001"),
        hedge_tick=Decimal("1"),
    )
    base.update(kw)
    return LadderMakerExecutorConfig(**base)


def test_executor_config_has_latency_profiling_default_false():
    assert _cfg().latency_profiling is False


def test_executor_config_latency_profiling_can_be_enabled():
    assert _cfg(latency_profiling=True).latency_profiling is True


def test_account_truth_snapshot_flag_defaults_off():
    cfg = _cfg()
    assert cfg.account_truth_snapshot_enabled is False
    assert cfg.account_truth_snapshot_interval_s == 60.0
    assert cfg.account_truth_snapshot_path is None


def test_account_truth_snapshot_flags_parse_values():
    cfg = _cfg(
        account_truth_snapshot_enabled=True,
        account_truth_snapshot_interval_s=5.0,
        account_truth_snapshot_path="/tmp/account_truth.sqlite",
    )
    assert cfg.account_truth_snapshot_enabled is True
    assert cfg.account_truth_snapshot_interval_s == 5.0
    assert cfg.account_truth_snapshot_path == "/tmp/account_truth.sqlite"
