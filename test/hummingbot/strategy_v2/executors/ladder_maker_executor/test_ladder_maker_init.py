import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.core.data_type.common import TradeType

try:
    # The executor module pulls the V2 strategy base (paho), absent in the local
    # py312 env -> these tests run in Docker/CI where the full stack is available.
    from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
        CrossVenueHedgedExecutorBase,
    )
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import (
        LadderMakerExecutor,
    )
    from hummingbot.strategy_v2.gates.gate_chain import InventoryGate
    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    CrossVenueHedgedExecutorBase = None
    InventoryGate = None
    _EXECUTOR_IMPORTABLE = False


def _mock_config(max_inventory):
    cfg = MagicMock()
    cfg.max_inventory = max_inventory
    # super().__init__ reads these off the LOCAL ``config`` param (not self.config).
    cfg.maker_market = MagicMock()
    cfg.hedge_market = MagicMock()
    cfg.entry_side = TradeType.SELL
    cfg.fx_connector = "upbit"
    cfg.session_halt_gate_enabled = False
    cfg.ws_staleness_kill_switch_enabled = False
    return cfg


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack (paho) — run in Docker/CI")
class LadderMakerInitTest(unittest.TestCase):
    """Regression for the __init__ ordering bug.

    ``__init__`` built the gate chain from ``self.config.max_inventory`` BEFORE
    ``super().__init__`` assigned ``self.config``, so constructing the executor
    raised ``AttributeError: 'LadderMakerExecutor' object has no attribute 'config'``
    on every controller action — the EC2 Samsung observe crashed once per second and
    never placed/observed a single rung. The fix reads the local ``config`` parameter.

    Every other executor test builds the object with ``LadderMakerExecutor.__new__`` +
    manual attribute injection, so NONE exercised the real ``__init__`` ordering. This
    test calls the real constructor (with the heavy base ``__init__`` stubbed) so the
    bug cannot silently return.
    """

    def _construct(self, max_inventory):
        captured = {}

        def fake_base_init(self, **kwargs):
            # The base assigns self.config — and it runs AFTER the gate-chain build,
            # so any pre-super ``self.config`` read still raises (RED on the old code).
            captured.update(kwargs)
            self.config = kwargs.get("config")

        with patch.object(CrossVenueHedgedExecutorBase, "__init__", fake_base_init):
            ex = LadderMakerExecutor(strategy=MagicMock(), config=_mock_config(max_inventory))
        return ex, captured

    def test_init_does_not_read_self_config_before_super(self):
        # Must NOT raise AttributeError; the composable gate chain must be built.
        ex, captured = self._construct(Decimal("8"))
        self.assertIsNotNone(ex._gate_chain)
        # config flowed into super().__init__ unchanged.
        self.assertEqual(captured["config"].max_inventory, Decimal("8"))

    def test_inventory_gate_cap_wired_from_config(self):
        ex, _ = self._construct(Decimal("8"))
        inv_gates = [g for g in ex._gate_chain._gates if isinstance(g, InventoryGate)]
        self.assertEqual(len(inv_gates), 1)
        self.assertEqual(inv_gates[0]._max_inventory, Decimal("8"))

    def test_nonpositive_max_inventory_disables_inventory_cap(self):
        ex, _ = self._construct(Decimal("0"))
        inv_gates = [g for g in ex._gate_chain._gates if isinstance(g, InventoryGate)]
        self.assertEqual(len(inv_gates), 1)
        self.assertIsNone(inv_gates[0]._max_inventory)

    def test_session_halt_requires_ws_staleness(self):
        cfg = _mock_config(Decimal("8"))
        cfg.session_halt_gate_enabled = True
        cfg.ws_staleness_kill_switch_enabled = False
        with self.assertRaises(ValueError):
            with patch.object(CrossVenueHedgedExecutorBase, "__init__", lambda self, **k: None):
                LadderMakerExecutor(strategy=MagicMock(), config=cfg)


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack (paho) — run in Docker/CI")
class LadderMakerBalanceValidationTest(unittest.TestCase):
    """Regression for the observe balance-validation bug.

    ``on_start()`` calls ``validate_sufficient_balance()``. The base implementation sizes
    a PerpetualOrderCandidate at the FULL ``total_size_cap`` and, on an underfunded maker
    account, stops the executor (``CloseType.INSUFFICIENT_BALANCE``) — before it ever
    quotes. In observe mode (zero real orders) that balance is irrelevant, so the ladder
    must SKIP the check; otherwise observe silently terminates every tick and never logs a
    rung (JEP-162 live regression: executor created then immediately TERMINATED, active=0).
    No prior test exercised the on_start balance path.
    """

    @staticmethod
    def _exec(observe):
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex.config = MagicMock(observe=observe)
        return ex

    def test_observe_skips_maker_balance_validation(self):
        ex = self._exec(observe=True)
        with patch.object(CrossVenueHedgedExecutorBase, "validate_sufficient_balance",
                          new_callable=AsyncMock) as base_check:
            asyncio.run(LadderMakerExecutor.validate_sufficient_balance(ex))
        base_check.assert_not_called()

    def test_live_mode_runs_maker_balance_validation(self):
        ex = self._exec(observe=False)
        with patch.object(CrossVenueHedgedExecutorBase, "validate_sufficient_balance",
                          new_callable=AsyncMock) as base_check:
            asyncio.run(LadderMakerExecutor.validate_sufficient_balance(ex))
        base_check.assert_called_once()
