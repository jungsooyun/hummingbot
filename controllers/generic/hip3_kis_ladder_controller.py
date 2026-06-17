# hummingbot/controllers/generic/hip3_kis_ladder_controller.py
"""
HIP3-KIS Ladder Market-Making Controller.

Maker: Hyperliquid perpetual (HIP-3 equity perp), posting a ladder of post-only
quotes around a conservative fair price derived from KIS spot.
Hedge: KIS spot (marketable limit) on each perp fill.

Ported from stratops korea_hip3. Pure pricing math lives in
``strategy_v2.executors.ladder_maker_executor.ladder_policy`` (unit-tested).
One controller instance per symbol; the V2 framework runs them concurrently.
"""
from decimal import Decimal
from typing import List, Optional

from pydantic import Field

from hummingbot.core.data_type.common import MarketDict, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import (
    LadderMakerExecutorConfig,
    LadderRungConfig,
)
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


def _default_rungs() -> List[LadderRungConfig]:
    # edge_bps, size, min_edge_bps — far rungs carry more size (cumulative cap)
    return [
        LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("0.25"), min_edge_bps=Decimal("3")),
        LadderRungConfig(edge_bps=Decimal("15"), size=Decimal("0.75"), min_edge_bps=Decimal("8")),
        LadderRungConfig(edge_bps=Decimal("35"), size=Decimal("1.00"), min_edge_bps=Decimal("20")),
    ]


class Hip3KisLadderControllerConfig(ControllerConfigBase):
    controller_name: str = "hip3_kis_ladder_controller"
    controller_type: str = "generic"
    candles_config: List[CandlesConfig] = []

    # Maker leg: Hyperliquid perp (HIP-3 equity perp)
    maker_connector: str = "hyperliquid_perpetual"
    maker_trading_pair: str = "EWY-USD"
    maker_tick: Decimal = Decimal("0.001")

    # Hedge leg: KIS spot
    hedge_connector: str = "kis"
    hedge_trading_pair: str = "069500-KRW"  # KODEX 200 ETF (example)
    hedge_tick: Decimal = Decimal("1")

    # Maker side on the perp (SELL perp -> BUY KIS hedge)
    entry_side: TradeType = TradeType.SELL

    # Ladder
    total_size_cap: Decimal = Decimal("1.0")
    rungs: List[LadderRungConfig] = Field(default_factory=_default_rungs)
    buffer_ticks: Decimal = Decimal("0")

    # FX (USD/KRW). Use an FX connector pair if available, else a static rate.
    fx_connector: Optional[str] = None
    fx_trading_pair: Optional[str] = None
    static_fx_rate: Optional[Decimal] = Field(default=Decimal("1380"))
    side_aware_fx: bool = True

    # Inventory skew / gate
    inventory_skew_bps_per_unit: Decimal = Field(default=Decimal("2"), json_schema_extra={"is_updatable": True})
    target_inventory: Decimal = Decimal("0")
    max_inventory: Optional[Decimal] = Field(default=Decimal("8"), json_schema_extra={"is_updatable": True})

    # Hedge
    share_per_unit: Decimal = Decimal("1")
    hedge_max_slippage_bps: Decimal = Field(default=Decimal("30"), json_schema_extra={"is_updatable": True})

    # Perp leverage (HIP-3: isolated only)
    leverage: int = 1

    # Reprice guards
    min_reprice_interval_s: float = 0.75
    min_reprice_delta_ticks: Decimal = Decimal("2")

    # Safety
    kill_switch: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    # No-submit verification: executor computes fair + logs intended quotes, no orders.
    observe: bool = Field(default=False, json_schema_extra={"is_updatable": True})

    # One executor per controller (single-direction ladder per symbol)
    max_executors: int = Field(default=1, json_schema_extra={"is_updatable": True})

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets.add_or_update(self.maker_connector, self.maker_trading_pair)
        markets.add_or_update(self.hedge_connector, self.hedge_trading_pair)
        if self.fx_connector and self.fx_trading_pair:
            markets.add_or_update(self.fx_connector, self.fx_trading_pair)
        return markets


class Hip3KisLadderController(ControllerBase):
    def __init__(self, config: Hip3KisLadderControllerConfig, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        pass

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        active = self.filter_executors(self.executors_info, filter_func=lambda e: not e.is_done)
        if len(active) >= self.config.max_executors:
            return actions

        actions.append(CreateExecutorAction(
            executor_config=LadderMakerExecutorConfig(
                timestamp=self.market_data_provider.time(),
                maker_market=ConnectorPair(
                    connector_name=self.config.maker_connector,
                    trading_pair=self.config.maker_trading_pair,
                ),
                hedge_market=ConnectorPair(
                    connector_name=self.config.hedge_connector,
                    trading_pair=self.config.hedge_trading_pair,
                ),
                entry_side=self.config.entry_side,
                total_size_cap=self.config.total_size_cap,
                rungs=self.config.rungs,
                maker_tick=self.config.maker_tick,
                hedge_tick=self.config.hedge_tick,
                buffer_ticks=self.config.buffer_ticks,
                fx_connector=self.config.fx_connector,
                fx_trading_pair=self.config.fx_trading_pair,
                static_fx_rate=self.config.static_fx_rate,
                side_aware_fx=self.config.side_aware_fx,
                inventory_skew_bps_per_unit=self.config.inventory_skew_bps_per_unit,
                target_inventory=self.config.target_inventory,
                max_inventory=self.config.max_inventory,
                share_per_unit=self.config.share_per_unit,
                hedge_max_slippage_bps=self.config.hedge_max_slippage_bps,
                min_reprice_interval_s=self.config.min_reprice_interval_s,
                min_reprice_delta_ticks=self.config.min_reprice_delta_ticks,
                leverage=self.config.leverage,
                kill_switch=self.config.kill_switch,
                observe=self.config.observe,
                controller_id=self.config.id,
            ),
            controller_id=self.config.id,
        ))
        return actions

    def to_format_status(self) -> List[str]:
        active = self.filter_executors(self.executors_info, filter_func=lambda e: not e.is_done)
        return [
            f"  Active ladder executors: {len(active)}",
            f"  Maker(perp): {self.config.maker_connector} {self.config.maker_trading_pair}",
            f"  Hedge(spot): {self.config.hedge_connector} {self.config.hedge_trading_pair}",
            f"  entry_side: {self.config.entry_side.name}  rungs: {len(self.config.rungs)}",
            f"  kill_switch: {self.config.kill_switch}",
        ]
