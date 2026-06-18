import asyncio
import inspect
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from controllers.generic.hip3_kis_ladder_controller import (
    Hip3KisLadderController,
    Hip3KisLadderControllerConfig,
)
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import (
    LadderMakerExecutorConfig,
    LadderRungConfig,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import ExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class TestHip3KisLadderController(IsolatedAsyncioWrapperTestCase):
    """JEP-155: exercise determine_executor_actions() THROUGH the real
    ControllerBase.filter_executors (no mocking of filter_executors), which is
    the path that crashed live when the not-done lambda was passed positionally
    and landed in the wrong slot of
    filter_executors(self, executors=None, executor_filter=None, filter_func=None).
    """

    def setUp(self):
        super().setUp()
        self.config = Hip3KisLadderControllerConfig(
            id="test-hip3-kis-ladder",
            max_executors=1,
        )
        self.mock_market_data_provider = MagicMock()
        self.mock_market_data_provider.time.return_value = 1000.0
        self.mock_actions_queue = AsyncMock(spec=asyncio.Queue)
        self.controller = Hip3KisLadderController(
            config=self.config,
            market_data_provider=self.mock_market_data_provider,
            actions_queue=self.mock_actions_queue,
        )

    def _make_executor_info(self, executor_id: str, *, is_done: bool) -> ExecutorInfo:
        """Build a REAL ExecutorInfo (real discriminated config). is_done is a
        computed property: status == TERMINATED. We never set is_done directly."""
        status = RunnableStatus.TERMINATED if is_done else RunnableStatus.RUNNING
        config = LadderMakerExecutorConfig(
            timestamp=1000.0,
            maker_market=ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="EWY-USD"),
            hedge_market=ConnectorPair(connector_name="kis", trading_pair="069500-KRW"),
            entry_side=self.config.entry_side,
            total_size_cap=Decimal("1.0"),
            rungs=[LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("0.25"), min_edge_bps=Decimal("3"))],
            maker_tick=Decimal("0.001"),
            hedge_tick=Decimal("1"),
        )
        return ExecutorInfo(
            id=executor_id,
            timestamp=1000.0,
            type="ladder_maker_executor",
            status=status,
            config=config,
            net_pnl_pct=Decimal("0"),
            net_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            filled_amount_quote=Decimal("0"),
            is_active=not is_done,
            is_trading=not is_done,
            custom_info={},
        )

    def test_determine_executor_actions_empty_creates_one_executor(self):
        # Smoke: through the REAL filter_executors. No active executors -> create one.
        self.controller.executors_info = []

        actions = self.controller.determine_executor_actions()

        self.assertIsInstance(actions, list)
        self.assertTrue(all(isinstance(a, ExecutorAction) for a in actions))
        self.assertEqual(1, len(actions))

    def test_two_sided_fields_default_to_single_direction(self):
        self.controller.executors_info = []

        actions = self.controller.determine_executor_actions()
        executor_config = actions[0].executor_config

        self.assertIsInstance(executor_config, LadderMakerExecutorConfig)
        self.assertIs(executor_config.two_sided, False)
        self.assertEqual(Decimal("0"), executor_config.k_open_skew_bps)
        self.assertEqual(Decimal("0"), executor_config.k_close_skew_bps)
        self.assertEqual(Decimal("0"), executor_config.eod_close_skew_bps)
        self.assertEqual(0, executor_config.eod_wind_minutes)
        self.assertEqual(Decimal("0"), executor_config.max_close_cost_bps)
        self.assertIs(executor_config.wind_down, False)
        self.assertEqual(30.0, executor_config.flatten_timeout_s)

    def test_two_sided_fields_passthrough(self):
        self.config.two_sided = True
        self.config.k_open_skew_bps = Decimal("3.5")
        self.config.k_close_skew_bps = Decimal("4.5")
        self.config.eod_close_skew_bps = Decimal("7.5")
        self.config.eod_wind_minutes = 25
        self.config.max_close_cost_bps = Decimal("2.5")
        self.config.wind_down = True
        self.config.flatten_timeout_s = 45.5
        self.controller.executors_info = []

        actions = self.controller.determine_executor_actions()
        executor_config = actions[0].executor_config

        self.assertIs(executor_config.two_sided, True)
        self.assertEqual(Decimal("3.5"), executor_config.k_open_skew_bps)
        self.assertEqual(Decimal("4.5"), executor_config.k_close_skew_bps)
        self.assertEqual(Decimal("7.5"), executor_config.eod_close_skew_bps)
        self.assertEqual(25, executor_config.eod_wind_minutes)
        self.assertEqual(Decimal("2.5"), executor_config.max_close_cost_bps)
        self.assertIs(executor_config.wind_down, True)
        self.assertEqual(45.5, executor_config.flatten_timeout_s)

    def test_determine_executor_actions_filters_done_through_real_base(self):
        # One running (not-done) + one terminated (done). The not-done lambda is
        # applied through the REAL ControllerBase.filter_executors. The single
        # active executor reaches max_executors=1 -> no new CreateExecutorAction.
        self.controller.executors_info = [
            self._make_executor_info("running-1", is_done=False),
            self._make_executor_info("done-1", is_done=True),
        ]

        active = self.controller.filter_executors(
            self.controller.executors_info, filter_func=lambda e: not e.is_done
        )
        self.assertEqual(["running-1"], [e.id for e in active])

        actions = self.controller.determine_executor_actions()
        self.assertIsInstance(actions, list)
        self.assertTrue(all(isinstance(a, ExecutorAction) for a in actions))
        # 1 active >= max_executors(1) -> create nothing.
        self.assertEqual(0, len(actions))

    def test_determine_executor_actions_all_done_creates_new(self):
        # Both terminated -> 0 active -> below max_executors -> create one.
        self.controller.executors_info = [
            self._make_executor_info("done-1", is_done=True),
            self._make_executor_info("done-2", is_done=True),
        ]

        actions = self.controller.determine_executor_actions()
        self.assertEqual(1, len(actions))
        self.assertTrue(all(isinstance(a, ExecutorAction) for a in actions))

    def test_positional_lambda_would_crash_proving_keyword_is_required(self):
        # Non-vacuous proof of the JEP-155 bug. Reproduce the OLD behaviour
        # (lambda passed POSITIONALLY) and show it raises through the real base,
        # while the fixed keyword call succeeds. We do NOT edit prod code; we
        # call filter_executors directly the OLD vs NEW way.
        self.controller.executors_info = [
            self._make_executor_info("running-1", is_done=False),
            self._make_executor_info("done-1", is_done=True),
        ]
        bad_filter = lambda e: not e.is_done  # noqa: E731

        # OLD (buggy) call: lambda lands in the first positional slot 'executors'
        # (when self.executors_info default is used), then `.copy()` on a function
        # blows up. With executors_info present it lands in 'executor_filter' and
        # `_apply_executor_filter` hits `.executor_ids` on a function. Either way
        # it must raise -> this is what made every live tick crash.
        with self.assertRaises(AttributeError):
            self.controller.filter_executors(bad_filter)

        # NEW (fixed) call: keyword filter_func works.
        active = self.controller.filter_executors(filter_func=bad_filter)
        self.assertEqual(["running-1"], [e.id for e in active])

    def test_filter_executors_signature_still_accepts_filter_func_kwarg(self):
        # Signature-drift guard: goes red if a future upstream merge renames or
        # removes the 'filter_func' keyword that the controller relies on.
        sig = inspect.signature(ControllerBase.filter_executors)
        self.assertIn("filter_func", sig.parameters)
        self.assertEqual(
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            sig.parameters["filter_func"].kind,
        )


# --- JEP-162: total_size_cap = max accumulated position; fail-loud validator ---


def test_config_rejects_rungs_exceeding_total_size_cap():
    # one full ladder must fit within the position ceiling, else fail loud at load
    with pytest.raises(ValueError, match="total_size_cap"):
        Hip3KisLadderControllerConfig(
            id="t",
            total_size_cap=Decimal("3"),
            rungs=[LadderRungConfig(edge_bps=Decimal("20"), size=Decimal("4"))],  # 4 > 3
        )


def test_config_accepts_default_rungs_within_cap():
    cfg = Hip3KisLadderControllerConfig(id="t")  # defaults: rungs {1,2,4}=7, cap 100
    assert sum((r.size for r in cfg.rungs), Decimal("0")) <= cfg.total_size_cap


def test_config_default_round_trip_cost_is_positive():
    cfg = Hip3KisLadderControllerConfig(id="t")
    assert cfg.round_trip_cost_bps > Decimal("0")


def test_config_rejects_non_unit_share_per_unit():
    # cross-venue hedge accounting assumes 1 maker unit == 1 hedge share
    with pytest.raises(ValueError, match="share_per_unit"):
        Hip3KisLadderControllerConfig(id="t", share_per_unit=Decimal("2"))


def test_config_rejects_negative_round_trip_cost():
    with pytest.raises(ValueError, match="round_trip_cost_bps"):
        Hip3KisLadderControllerConfig(id="t", round_trip_cost_bps=Decimal("-1"))
