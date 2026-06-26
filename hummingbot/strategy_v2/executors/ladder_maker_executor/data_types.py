from decimal import Decimal
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class LadderRungConfig(BaseModel):
    """One ladder level. edge_bps = distance from fair, size = base order size,
    min_edge_bps = floor distance (never quote closer than this)."""

    edge_bps: Decimal
    size: Decimal
    min_edge_bps: Decimal = Decimal("0")
    enabled: bool = True


class LadderMakerExecutorConfig(ExecutorConfigBase):
    type: Literal["ladder_maker_executor"] = "ladder_maker_executor"

    # Markets: maker leg on a perp (e.g. hyperliquid_perpetual HIP-3), hedge leg on KIS spot
    maker_market: ConnectorPair
    hedge_market: ConnectorPair

    # Maker side on the perp. Fills are hedged on the opposite side on KIS spot.
    entry_side: TradeType

    # Ladder
    total_size_cap: Decimal
    rungs: List[LadderRungConfig]
    maker_tick: Decimal
    hedge_tick: Decimal
    buffer_ticks: Decimal = Decimal("0")
    # Round-trip friction (fees + 증권거래세) added to each rung's NET edge so the
    # maker quote rests at gross = net + cost. See ladder_cost.round_trip_cost_bps.
    round_trip_cost_bps: Decimal = Decimal("0")

    # Fair price inputs (KIS spot KRW -> USD via FX)
    fx_connector: Optional[str] = None
    fx_trading_pair: Optional[str] = None
    static_fx_rate: Optional[Decimal] = None
    side_aware_fx: bool = True

    # Inventory skew / gate
    inventory_skew_bps_per_unit: Decimal = Decimal("0")
    target_inventory: Decimal = Decimal("0")
    max_inventory: Optional[Decimal] = None

    # Hedge (KIS spot marketable limit)
    share_per_unit: Decimal = Decimal("1")
    hedge_max_slippage_bps: Decimal = Decimal("30")
    hedge_order_type: OrderType = OrderType.LIMIT
    # JEP-219: cancel + re-price a hedge order that rests OPEN (unfilled) longer than this many
    # seconds (a marketable hedge parked on a thin/empty book). 0 disables (behavior-neutral).
    hedge_fill_timeout_s: float = 0.0
    # Maker leg order discipline. True (default): LIMIT_MAKER (post-only) — a rung price that
    # crosses the fast maker book is rejected (no fill), pure-maker. False: plain LIMIT at the
    # same rung price (= fair + net + round_trip_cost, the profitability floor) so a crossing
    # rung fills as a taker at a price >= its edge instead of being rejected; resting orders are
    # still maker fills. Only profitable immediate fills happen (the limit price is the floor).
    maker_post_only: bool = True

    # Reprice guards
    min_reprice_interval_s: float = 0.75
    min_reprice_delta_ticks: Decimal = Decimal("2")
    # JEP-177 Fix #5: in-flight conflict-blocking granularity. When an unmatched (moved/resized)
    # in-flight maker rung exists, the reconcile defers same-side placements so a moved/resized
    # rung is not double-placed (over-close -> net-long, the round-2 JEP-172 #5 failure).
    # False (default) = whole-side block (defer EVERY same-side target while any in-flight rung is
    # unmatched; conservative, behavior-neutral, ~1-tick under-quote on a genuinely-new far rung).
    # True = block only targets within maker_inflight_block_radius_ticks of an unmatched in-flight
    # rung's price, so a genuinely-new far same-side rung is placed immediately. The radius must
    # cover the round-2 cases (same-price/size-shift = 0 distance; fair-move up to the radius).
    maker_inflight_block_rung_granular: bool = False
    maker_inflight_block_radius_ticks: Decimal = Decimal("4")
    # JEP-177 Fix #2: maker-side stuck-order reconcile. When a maker order's *created* event is
    # permanently lost, it stays at order is None and freezes that side (under-quote only,
    # restart-recoverable). True adopts the connector tracker's truth for such an order (adopt-only:
    # never reaps/cancels an order the connector does not track, so a just-placed order is never
    # orphaned). False (default) = current behavior (resolve only on a created/filled/cancelled
    # event). Mirrors the hedge leg's always-on _reconcile_stuck_hedges.
    reconcile_stuck_makers_enabled: bool = False

    # Perp leverage (HIP-3 markets: isolated only)
    leverage: int = 1

    # Safety
    kill_switch: bool = False
    # JEP-221 #2: HARD per-boot order-rate circuit breaker (backstop for the close-fill churn
    # that produced the 2026-06-25 NXT runaway). More than max_placements_per_window REAL maker
    # placements within placement_rate_window_s latches a per-boot HALT — cancel all resting
    # makers + suppress all further maker/hedge placement until restart. Default-on with generous
    # bounds: normal reprice is rate-limited by min_reprice_interval_s while orders rest, so it
    # stays far below the cap; the runaway sustained ~6 placements/s (435 in 72s), which the
    # JEP-218 clock watchdog cannot catch (the clock keeps advancing). Tune from live baseline.
    placement_rate_breaker_enabled: bool = True
    placement_rate_window_s: float = 30.0
    max_placements_per_window: int = 80
    # JEP-133 approval-envelope per-order cap (port of stratops max_qty_per_order). When set,
    # a maker order whose quantized size exceeds this many base units is REFUSED at the
    # placement boundary (logged + skipped), never clamped down and never submitted oversized.
    # None (default) = no cap = current behavior. Enforced in LadderMakerExecutor._place_maker.
    max_maker_order_size: Optional[Decimal] = None
    ws_staleness_kill_switch_enabled: bool = True
    max_kis_ws_age_s: Optional[float] = 3.0
    max_hl_ws_age_s: Optional[float] = 12.0
    ws_staleness_grace_s: float = 90.0
    session_halt_gate_enabled: bool = True
    session_halt_max_ws_age_s: float = 3.0
    session_halt_max_book_static_s: float = 15.0
    session_halt_cooldown_s: float = 1800.0
    # JEP-226: cap (s) of continuous same-side naked age the hedge is deferred during a
    # clock-scheduled single-price auction before force-hedging. <=0 disables the whole
    # session-aware hedge gate (behavior-neutral). Read by CrossVenueHedgedExecutorBase.
    hedge_session_defer_cap_s: float = 30.0
    # Observe / no-submit: compute fair + targets and LOG the intended maker quotes
    # but never call place_order (no real orders, so no fills and no hedges). For
    # safe live verification of the full decision path before enabling submission.
    observe: bool = False
    two_sided: bool = False
    k_open_skew_bps: Decimal = Decimal("0")
    k_close_skew_bps: Decimal = Decimal("0")
    eod_close_skew_bps: Decimal = Decimal("0")
    eod_wind_minutes: int = 0
    max_close_cost_bps: Decimal = Decimal("0")
    wind_down: bool = False
    flatten_timeout_s: float = 30.0

    # Display / bookkeeping
    adopt_existing_inventory: bool = False
    # JEP-184: per-tick decision-pipeline latency profiling (read-only, off by default).
    latency_profiling: bool = False
    execution_purpose: str = Field(default="ladder_market_making")
