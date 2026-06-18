"""
Regression tests for the perpetual-connector allowlist patch (JEP-167).

Verifies that:
- kis_futures is recognized as perpetual by both executor_base and strategy_v2_base
- binance_perpetual continues to be recognized (regression guard)
- plain "kis" is NOT treated as perpetual
"""
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy.strategy_v2_base import StrategyV2Base


def test_executor_base_recognizes_kis_futures():
    assert ExecutorBase.is_perpetual_connector("kis_futures") is True
    assert ExecutorBase.is_perpetual_connector("binance_perpetual") is True
    assert ExecutorBase.is_perpetual_connector("kis") is False


def test_strategy_v2_base_recognizes_kis_futures():
    assert StrategyV2Base.is_perpetual("kis_futures") is True
    assert StrategyV2Base.is_perpetual("binance_perpetual") is True
    assert StrategyV2Base.is_perpetual("kis") is False
