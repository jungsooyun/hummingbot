import unittest
from decimal import Decimal

from hummingbot.strategy_v2.executors.as_maker_executor.as_maker_executor import AsMakerExecutor
from hummingbot.strategy_v2.executors.as_maker_executor.data_types import AsMakerExecutorConfig
from hummingbot.strategy_v2.executors.executor_orchestrator import ExecutorOrchestrator


def _cfg(**kw):
    base = dict(connector_name="hyperliquid_perpetual", trading_pair="BTC-USD",
                gamma=Decimal("0.1"), kappa=Decimal("1.5"), order_amount=Decimal("0.001"),
                max_inventory=Decimal("0.01"), maker_tick=Decimal("1"))
    base.update(kw)
    return AsMakerExecutorConfig(**base)


class AsMakerRegistrationTests(unittest.TestCase):
    def test_orchestrator_mapping_resolves_executor(self):
        self.assertIs(ExecutorOrchestrator._executor_mapping["as_maker_executor"], AsMakerExecutor)

    def test_executor_config_type_accepts_as_maker(self):
        cfg = _cfg()
        self.assertEqual(cfg.type, "as_maker_executor")

    def test_any_executor_config_union_includes_as_maker(self):
        # ExecutorInfo.config is a discriminated Union (AnyExecutorConfig, discriminator="type").
        # Test membership directly via TypeAdapter — avoids constructing a full ExecutorInfo
        # (which has many unrelated required fields). This is the load-bearing assertion.
        from pydantic import TypeAdapter

        from hummingbot.strategy_v2.models.executors_info import AnyExecutorConfig
        cfg = _cfg()
        parsed = TypeAdapter(AnyExecutorConfig).validate_python(cfg.model_dump())
        self.assertIsInstance(parsed, AsMakerExecutorConfig)
        self.assertEqual(parsed.type, "as_maker_executor")


if __name__ == "__main__":
    unittest.main()
