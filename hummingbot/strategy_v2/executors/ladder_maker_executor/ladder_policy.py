"""Pure ladder market-making policy logic.

Ported from stratops ``korea_hip3`` policy/target.py + policy/ladder.py so the
core pricing/hedging math can be unit-tested locally WITHOUT the hummingbot
Cython runtime. No I/O and no hummingbot *runtime* (Cython) imports — only the
stdlib, plus the pure shared ``maker_reconcile`` value types relocated in JEP-145.

The executor layer adapts these pure results to hummingbot order placement
(TradeType, place_order, TrackedOrder).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import List, Optional

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import (
    LadderDiff,
    RestingOrder,
    RungTarget,
    Side,
    diff_ladder_targets,
)

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")


@dataclass(frozen=True)
class RungSpec:
    """One ladder level. edge_bps = distance from fair; size = base order size."""

    edge_bps: Decimal
    size: Decimal
    min_edge_bps: Decimal = ZERO
    enabled: bool = True


@dataclass(frozen=True)
class TwoSidedTargets:
    open: list[RungTarget]
    close: list[RungTarget]


@dataclass(frozen=True)
class HedgeOrder:
    side: Side
    price: Decimal
    size: Decimal


def floor_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    if tick <= ZERO:
        return price
    return (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick


def ceil_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    if tick <= ZERO:
        return price
    return (price / tick).to_integral_value(rounding=ROUND_UP) * tick


def compute_fair_price(
    kis_bid: Decimal,
    kis_ask: Decimal,
    fx_bid: Decimal,
    fx_ask: Decimal,
    side: Side,
    side_aware_fx: bool = True,
) -> Decimal:
    """KIS spot (KRW) -> fair USD with side-aware FX conservatism.

    BUY  uses kis_bid / fx_ask  (lower fair  -> more conservative buy quotes)
    SELL uses kis_ask / fx_bid  (higher fair -> more conservative sell quotes)
    """
    if side is Side.BUY:
        kis_px = kis_bid
        fx = fx_ask if side_aware_fx else (fx_bid + fx_ask) / Decimal("2")
    else:
        kis_px = kis_ask
        fx = fx_bid if side_aware_fx else (fx_bid + fx_ask) / Decimal("2")
    if fx <= ZERO:
        raise ValueError("FX rate must be positive")
    return kis_px / fx


def apply_inventory_skew(
    fair: Decimal,
    inventory: Decimal,
    target_inventory: Decimal,
    skew_bps_per_unit: Decimal,
) -> Decimal:
    """Long inventory (inventory > target) lowers fair -> encourages selling."""
    deviation = inventory - target_inventory
    skew_bps = skew_bps_per_unit * deviation
    return fair * (ONE - skew_bps / BPS)


def rung_price(
    fair: Decimal,
    edge_bps: Decimal,
    side: Side,
    tick: Decimal,
    buffer_ticks: Decimal = ZERO,
) -> Decimal:
    edge = edge_bps / BPS
    buffer = tick * buffer_ticks
    if side is Side.BUY:
        return floor_to_tick(fair * (ONE - edge) - buffer, tick)
    return ceil_to_tick(fair * (ONE + edge) + buffer, tick)


def _apply_min_edge(
    price: Decimal, fair: Decimal, min_edge_bps: Decimal, side: Side, tick: Decimal
) -> Decimal:
    """Clamp a rung price so it never sits closer to fair than min_edge_bps."""
    if min_edge_bps <= ZERO:
        return price
    min_edge = min_edge_bps / BPS
    if side is Side.BUY:
        return min(price, floor_to_tick(fair * (ONE - min_edge), tick))
    return max(price, ceil_to_tick(fair * (ONE + min_edge), tick))


def build_ladder_targets(
    fair: Decimal,
    rungs: List[RungSpec],
    total_size_cap: Decimal,
    side: Side,
    tick: Decimal,
    buffer_ticks: Decimal = ZERO,
    inventory: Decimal = ZERO,
    max_inventory: Optional[Decimal] = None,
    cost_bps: Decimal = ZERO,
    current_position: Decimal = ZERO,
) -> List[RungTarget]:
    """simultaneous_maker: one order per enabled rung, size accumulated under cap.

    ``total_size_cap`` is the **max |accumulated net position|** (shares):
    ``current_position`` (the magnitude already held) consumes that budget, so the
    quoted ladder shrinks toward the cap and halts once the cap is reached.

    Inventory gate (separate, unhedged-exposure safety pin): withhold BUY if
    inventory >= max_inventory, withhold SELL if inventory <= -max_inventory.

    ``cost_bps`` is the round-trip friction (fees + 증권거래세) added to each rung's
    NET edge so the *placement* sits at gross = net + cost. The reported
    ``RungTarget.edge_bps`` stays the NET target. ``cost_bps`` is also added to
    ``min_edge_bps`` so the floor is expressed in the same net terms.
    """
    if max_inventory is not None:
        if side is Side.BUY and inventory >= max_inventory:
            return []
        if side is Side.SELL and inventory <= -max_inventory:
            return []

    targets: List[RungTarget] = []
    remaining = total_size_cap - abs(current_position)  # position budget, not per-tick
    if remaining <= ZERO:
        return []
    for rung in rungs:
        if not rung.enabled or remaining <= ZERO:
            continue
        qty = min(rung.size, remaining)
        if qty <= ZERO:
            continue
        effective_edge = rung.edge_bps + cost_bps  # NET target -> gross placement
        price = rung_price(fair, effective_edge, side, tick, buffer_ticks)
        price = _apply_min_edge(price, fair, rung.min_edge_bps + cost_bps, side, tick)
        targets.append(RungTarget(side=side, price=price, size=qty, edge_bps=rung.edge_bps))
        remaining -= qty
    return targets


def build_two_sided_targets(
    *,
    fair_open: Decimal,
    fair_close: Decimal,
    rungs: List[RungSpec],
    total_size_cap: Decimal,
    net_position: Decimal,
    open_edge_vwap: Decimal,
    util: Decimal,
    eod_pressure: Decimal,
    cost_bps: Decimal,
    k_open_skew_bps: Decimal,
    k_close_skew_bps: Decimal,
    eod_close_skew_bps: Decimal,
    max_close_cost_bps: Decimal,
    tick: Decimal,
    buffer_ticks: Decimal,
    wind_down: bool,
) -> TwoSidedTargets:
    buffer = tick * buffer_ticks
    half_cost = cost_bps / Decimal("2")
    break_even_buy_edge = cost_bps - open_edge_vwap - max_close_cost_bps

    open_targets: List[RungTarget] = []
    open_remaining = total_size_cap - net_position
    if not wind_down and net_position < total_size_cap and open_remaining > ZERO:
        for rung in rungs:
            if not rung.enabled or open_remaining <= ZERO:
                continue
            qty = min(rung.size, open_remaining)
            if qty <= ZERO:
                continue
            edge_sell = half_cost + rung.edge_bps + k_open_skew_bps * util
            price_sell = ceil_to_tick(fair_open * (ONE + edge_sell / BPS) + buffer, tick)
            open_targets.append(RungTarget(side=Side.SELL, price=price_sell, size=qty, edge_bps=edge_sell))
            open_remaining -= qty

    close_targets: List[RungTarget] = []
    close_remaining = net_position
    if close_remaining > ZERO:
        for rung in rungs:
            if not rung.enabled or close_remaining <= ZERO:
                continue
            qty = min(rung.size, close_remaining)
            if qty <= ZERO:
                continue
            edge_buy = half_cost + rung.edge_bps - k_close_skew_bps * util - eod_close_skew_bps * eod_pressure
            edge_buy = max(edge_buy, break_even_buy_edge)
            price_buy = floor_to_tick(fair_close * (ONE - edge_buy / BPS) - buffer, tick)
            close_targets.append(RungTarget(side=Side.BUY, price=price_buy, size=qty, edge_bps=edge_buy))
            close_remaining -= qty

    return TwoSidedTargets(open=open_targets, close=close_targets)


def compute_eod_pressure(now_kst_min: int, krx_close_min: int, wind_minutes: int) -> Decimal:
    wind_minutes_dec = Decimal(wind_minutes)
    if wind_minutes_dec <= ZERO:
        return ZERO

    wind_start = Decimal(krx_close_min) - wind_minutes_dec
    pressure = (Decimal(now_kst_min) - wind_start) / (Decimal(krx_close_min) - wind_start)
    if pressure <= ZERO:
        return ZERO
    if pressure >= ONE:
        return ONE
    return pressure


def compute_hedge_order(
    fill_qty: Decimal,
    share_per_unit: Decimal,
    kis_price: Decimal,
    max_slippage_bps: Decimal,
    tick: Decimal,
    side: Side = Side.BUY,
) -> HedgeOrder:
    """Maker fill -> KIS spot marketable-limit hedge.

    BUY walks up from best ask + slippage; SELL walks down from best bid - slippage
    so either limit order crosses (takerable).
    """
    share_qty = fill_qty * share_per_unit
    slip = max_slippage_bps / BPS
    if side is Side.BUY:
        price = ceil_to_tick(kis_price * (ONE + slip), tick)
    else:
        price = floor_to_tick(kis_price * (ONE - slip), tick)
    return HedgeOrder(side=side, price=price, size=share_qty)
