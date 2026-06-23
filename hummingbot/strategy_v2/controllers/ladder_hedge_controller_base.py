from decimal import Decimal
from typing import List, Optional

from pydantic import Field

from hummingbot.core.data_type.common import MarketDict
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


class LadderHedgeControllerConfigBase(ControllerConfigBase):
    maker_connector: str = Field(...)
    maker_trading_pair: str = Field(...)
    maker_tick: Decimal = Field(...)
    hedge_connector: str = Field(...)
    hedge_trading_pair: str = Field(...)
    hedge_tick: Decimal = Field(...)
    fx_connector: Optional[str] = None
    fx_trading_pair: Optional[str] = None
    static_fx_rate: Optional[Decimal] = None
    side_aware_fx: bool = True
    leverage: int = 1
    min_reprice_interval_s: float = 0.75
    min_reprice_delta_ticks: Decimal = Decimal("2")
    kill_switch: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    observe: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    ws_staleness_kill_switch_enabled: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    max_kis_ws_age_s: Optional[float] = Field(default=3.0, json_schema_extra={"is_updatable": True})
    max_hl_ws_age_s: Optional[float] = Field(default=12.0, json_schema_extra={"is_updatable": True})
    ws_staleness_grace_s: float = Field(default=90.0, json_schema_extra={"is_updatable": True})
    session_halt_gate_enabled: bool = Field(default=True, json_schema_extra={"is_updatable": False})
    session_halt_max_ws_age_s: float = Field(default=3.0, json_schema_extra={"is_updatable": True})
    session_halt_max_book_static_s: float = Field(default=15.0, json_schema_extra={"is_updatable": True})
    # JEP-198 interim auction-gap guard: once a freeze/CB is detected, hold the maker-quote
    # halt this many seconds past the freeze END (covers the ~10min CB single-price auction the
    # clock can't see). Default 1800s = full CB (20min freeze + 10min auction). 0 disables.
    session_halt_cooldown_s: float = Field(default=1800.0, json_schema_extra={"is_updatable": True})
    adopt_existing_inventory: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    latency_profiling: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    max_executors: int = Field(default=1, json_schema_extra={"is_updatable": True})

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets.add_or_update(self.maker_connector, self.maker_trading_pair)
        markets.add_or_update(self.hedge_connector, self.hedge_trading_pair)
        if self.fx_connector and self.fx_trading_pair:
            markets.add_or_update(self.fx_connector, self.fx_trading_pair)
        return markets


class LadderHedgeControllerBase(ControllerBase):
    async def update_processed_data(self):
        pass

    def determine_executor_actions(self) -> List[ExecutorAction]:
        active = self.filter_executors(self.executors_info, filter_func=lambda e: not e.is_done)
        if len(active) >= self.config.max_executors:
            return []
        return [CreateExecutorAction(
            executor_config=self._build_executor_config(),
            controller_id=self.config.id,
        )]

    def _build_executor_config(self):
        raise NotImplementedError
