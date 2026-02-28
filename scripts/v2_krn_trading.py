# hummingbot/scripts/v2_krn_trading.py
"""
V2 Korean Exchange Trading Script
Entry point for running Korean exchange arbitrage and XEMM strategies.
"""
import os
from decimal import Decimal
from typing import Dict, List, Optional, Set

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class V2KrnTradingConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    candles_config: List[CandlesConfig] = []
    markets: Dict[str, Set[str]] = {}
    max_global_drawdown_quote: Optional[float] = None
    max_controller_drawdown_quote: Optional[float] = None


class V2KrnTrading(StrategyV2Base):
    @classmethod
    def init_markets(cls, config: V2KrnTradingConfig):
        cls.markets = config.markets

    def __init__(self, connectors: Dict[str, ConnectorBase], config: V2KrnTradingConfig):
        super().__init__(connectors, config)
        self.config = config
        self._max_pnl_per_controller: Dict[str, Decimal] = {}
        self._max_global_pnl = Decimal("0")
        self._is_stop_triggered = False

    def on_tick(self):
        super().on_tick()
        if not self._is_stop_triggered:
            self._check_drawdown()

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        return []

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []

    def _check_drawdown(self):
        if self.config.max_global_drawdown_quote is not None:
            global_pnl = self.executor_orchestrator.get_global_pnl_quote()
            self._max_global_pnl = max(self._max_global_pnl, global_pnl)
            if self._max_global_pnl - global_pnl > Decimal(str(self.config.max_global_drawdown_quote)):
                self.logger().warning("Global drawdown limit reached. Stopping all controllers.")
                self._stop_all_controllers()

        if self.config.max_controller_drawdown_quote is not None:
            for controller_id, controller in self.controllers.items():
                report = controller.performance_report
                if report is None:
                    continue
                pnl = report.global_pnl_quote
                max_pnl = self._max_pnl_per_controller.get(controller_id, Decimal("0"))
                self._max_pnl_per_controller[controller_id] = max(max_pnl, pnl)
                if self._max_pnl_per_controller[controller_id] - pnl > Decimal(str(self.config.max_controller_drawdown_quote)):
                    self.logger().warning(f"Controller {controller_id} drawdown limit reached. Stopping.")
                    controller.stop()

    def _stop_all_controllers(self):
        self._is_stop_triggered = True
        for controller in self.controllers.values():
            controller.stop()
