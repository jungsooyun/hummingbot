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

from pydantic import Field, model_validator

from hummingbot.core.data_type.common import TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.ladder_hedge_controller_base import (
    LadderHedgeControllerBase,
    LadderHedgeControllerConfigBase,
)
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import (
    LadderMakerExecutorConfig,
    LadderRungConfig,
)
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_cost import KisHlCostModel


def _default_rungs() -> List[LadderRungConfig]:
    # NET edge_bps (gross placement = net + round_trip_cost_bps); size in SHARES.
    return [
        LadderRungConfig(edge_bps=Decimal("20"), size=Decimal("1"), min_edge_bps=Decimal("10")),
        LadderRungConfig(edge_bps=Decimal("100"), size=Decimal("2"), min_edge_bps=Decimal("80")),
        LadderRungConfig(edge_bps=Decimal("150"), size=Decimal("4"), min_edge_bps=Decimal("120")),
    ]


class Hip3KisLadderControllerConfig(LadderHedgeControllerConfigBase):
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

    # Ladder. total_size_cap = MAX |accumulated net position| (shares); one full
    # ladder must fit (validated below). rung edge_bps are NET targets; placement
    # gross = net + round_trip_cost_bps.
    total_size_cap: Decimal = Decimal("100")
    rungs: List[LadderRungConfig] = Field(default_factory=_default_rungs)
    buffer_ticks: Decimal = Decimal("0")
    round_trip_cost_bps: Decimal = Field(
        default_factory=lambda: KisHlCostModel().round_trip_cost_bps(),
        json_schema_extra={"is_updatable": True},
    )

    # FX (USD/KRW). Use an FX connector pair if available, else a static rate.
    static_fx_rate: Optional[Decimal] = Field(default=Decimal("1380"))

    # Inventory skew / gate
    inventory_skew_bps_per_unit: Decimal = Field(default=Decimal("2"), json_schema_extra={"is_updatable": True})
    target_inventory: Decimal = Decimal("0")
    max_inventory: Optional[Decimal] = Field(default=Decimal("8"), json_schema_extra={"is_updatable": True})

    # Hedge
    share_per_unit: Decimal = Decimal("1")
    hedge_max_slippage_bps: Decimal = Field(default=Decimal("30"), json_schema_extra={"is_updatable": True})

    # Two-sided MM
    two_sided: bool = False
    k_open_skew_bps: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    k_close_skew_bps: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    eod_close_skew_bps: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    eod_wind_minutes: int = Field(default=0, json_schema_extra={"is_updatable": True})
    max_close_cost_bps: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    wind_down: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    flatten_timeout_s: float = Field(default=30.0, json_schema_extra={"is_updatable": True})

    @model_validator(mode="after")
    def _validate_config(self):
        total = sum((r.size for r in self.rungs), Decimal("0"))
        if total > self.total_size_cap:
            raise ValueError(
                f"sum(rung sizes)={total} exceeds total_size_cap (position ceiling)="
                f"{self.total_size_cap}; one full ladder must fit within the max accumulated position."
            )
        # The cross-venue hedge accounting assumes 1 maker unit == 1 hedge share;
        # pending / _unhedged_base mix units otherwise (JEP-162 adversarial finding #2).
        if self.share_per_unit != Decimal("1"):
            raise ValueError(
                f"share_per_unit={self.share_per_unit} is unsupported: hedge accounting "
                "assumes 1 maker unit == 1 hedge share (non-1 is phase 2)."
            )
        # A negative round_trip_cost_bps would invert the net-edge safety floor
        # (SELL inside fair / BUY above fair) and can disable the min-edge clamp.
        if self.round_trip_cost_bps < Decimal("0"):
            raise ValueError(f"round_trip_cost_bps={self.round_trip_cost_bps} must be >= 0")
        for r in self.rungs:
            if r.size <= Decimal("0"):
                raise ValueError(f"rung size must be > 0, got {r.size}")
        return self


class Hip3KisLadderController(LadderHedgeControllerBase):
    def __init__(self, config: Hip3KisLadderControllerConfig, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)

    def _build_executor_config(self):
        return LadderMakerExecutorConfig(
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
            round_trip_cost_bps=self.config.round_trip_cost_bps,
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
            maker_post_only=self.config.maker_post_only,
            leverage=self.config.leverage,
            kill_switch=self.config.kill_switch,
            ws_staleness_kill_switch_enabled=self.config.ws_staleness_kill_switch_enabled,
            max_kis_ws_age_s=self.config.max_kis_ws_age_s,
            max_hl_ws_age_s=self.config.max_hl_ws_age_s,
            ws_staleness_grace_s=self.config.ws_staleness_grace_s,
            session_halt_gate_enabled=self.config.session_halt_gate_enabled,
            session_halt_max_ws_age_s=self.config.session_halt_max_ws_age_s,
            session_halt_max_book_static_s=self.config.session_halt_max_book_static_s,
            observe=self.config.observe,
            adopt_existing_inventory=self.config.adopt_existing_inventory,
            latency_profiling=self.config.latency_profiling,
            two_sided=self.config.two_sided,
            k_open_skew_bps=self.config.k_open_skew_bps,
            k_close_skew_bps=self.config.k_close_skew_bps,
            eod_close_skew_bps=self.config.eod_close_skew_bps,
            eod_wind_minutes=self.config.eod_wind_minutes,
            max_close_cost_bps=self.config.max_close_cost_bps,
            wind_down=self.config.wind_down,
            flatten_timeout_s=self.config.flatten_timeout_s,
            controller_id=self.config.id,
        )

    def to_format_status(self) -> List[str]:
        active = self.filter_executors(self.executors_info, filter_func=lambda e: not e.is_done)
        lines = [
            f"  Active ladder executors: {len(active)}",
            f"  Maker(perp): {self.config.maker_connector} {self.config.maker_trading_pair}",
            f"  Hedge(spot): {self.config.hedge_connector} {self.config.hedge_trading_pair}",
            f"  entry_side: {self.config.entry_side.name}  rungs: {len(self.config.rungs)}",
            f"  kill_switch: {self.config.kill_switch}  observe: {self.config.observe}",
        ]
        # Surface each active executor's last intended quote (fair + spot + fx +
        # rungs) so the fair price and virtual ladder are visible in `status` and
        # on the dashboard, not only by grepping the executor log. Populated in
        # observe mode via the executor's get_custom_info["last_quote"].
        for executor in active:
            quote = (getattr(executor, "custom_info", None) or {}).get("last_quote")
            if not quote:
                continue
            lines.append(
                f"    fair={quote.get('fair')}  "
                f"spot[{quote.get('spot_pair')}] bid/ask={quote.get('spot_bid')}/{quote.get('spot_ask')}  "
                f"fx={quote.get('fx_bid')}/{quote.get('fx_ask')}"
            )
            rungs = "  ".join(
                f"{r['price']}@{r['edge_bps']}bps" for r in quote.get("rungs", [])
            )
            if rungs:
                lines.append(f"    rungs: {rungs}")
        return lines
