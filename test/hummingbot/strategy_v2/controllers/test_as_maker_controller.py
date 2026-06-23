import importlib.util
import os
import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import MarketDict
from hummingbot.strategy_v2.executors.as_maker_executor.data_types import AsMakerExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction

# AsMakerController lives in the meta repo (deploy/hummingbot-controllers/generic/), but its imports
# are submodule modules. Load it by file path so it runs under the same py312 harness as the rest.
# Depth: controllers -> strategy_v2 -> hummingbot -> test -> <submodule root> -> <meta root> = 5x "..".
_CTRL = os.path.join(os.path.dirname(__file__),
                     "../../../../../deploy/hummingbot-controllers/generic/as_maker_controller.py")


def _load():
    spec = importlib.util.spec_from_file_location("as_maker_controller", os.path.abspath(_CTRL))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class AsMakerControllerTests(unittest.TestCase):
    def setUp(self):
        self.mod = _load()
        self.cfg = self.mod.AsMakerControllerConfig(
            id="as_maker_btc_observe_v1", connector_name="hyperliquid_perpetual",
            trading_pair="BTC-USD", gamma=Decimal("0.1"), kappa=Decimal("1.5"),
            order_amount=Decimal("0.001"), max_inventory=Decimal("0.01"), maker_tick=Decimal("1"))
        mdp = MagicMock()
        mdp.time.return_value = 0.0   # _build_executor_config() feeds this into timestamp: Optional[float]
        self.ctrl = self.mod.AsMakerController(self.cfg, market_data_provider=mdp,
                                               actions_queue=MagicMock())

    def test_update_markets_registers_pair(self):
        markets = self.cfg.update_markets(MarketDict())
        self.assertIn("BTC-USD", markets["hyperliquid_perpetual"])

    def test_emits_one_create_action_when_idle(self):
        self.ctrl.executors_info = []
        actions = self.ctrl.determine_executor_actions()
        self.assertEqual(len(actions), 1)
        self.assertIsInstance(actions[0], CreateExecutorAction)
        self.assertIsInstance(actions[0].executor_config, AsMakerExecutorConfig)
        self.assertEqual(actions[0].executor_config.connector_name, "hyperliquid_perpetual")
        self.assertTrue(actions[0].executor_config.observe)   # observe default true

    def test_idempotent_when_active_executor_exists(self):
        alive = MagicMock()
        alive.is_done = False
        self.ctrl.executors_info = [alive]
        self.assertEqual(self.ctrl.determine_executor_actions(), [])


if __name__ == "__main__":
    unittest.main()
