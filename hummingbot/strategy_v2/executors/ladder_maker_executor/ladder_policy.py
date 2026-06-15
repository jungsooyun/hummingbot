"""Pure ladder market-making policy logic.

Ported from stratops ``korea_hip3`` policy/target.py + policy/ladder.py so the
core pricing/hedging math can be unit-tested locally WITHOUT the hummingbot
Cython runtime. No I/O, no hummingbot imports — only the stdlib.

The executor layer adapts these pure results to hummingbot order placement
(TradeType, place_order, TrackedOrder).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from enum import Enum
from typing import List, Optional

ZERO = Decimal("0")
ONE = Decimal("1")
BPS = Decimal("10000")


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class RungSpec:
    """One ladder level. edge_bps = distance from fair; size = base order size."""

    edge_bps: Decimal
    size: Decimal
    min_edge_bps: Decimal = ZERO
    enabled: bool = True


@dataclass(frozen=True)
class RungTarget:
    side: Side
    price: Decimal
    size: Decimal
    edge_bps: Decimal


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
) -> List[RungTarget]:
    """simultaneous_maker: one order per enabled rung, size accumulated under cap.

    Inventory gate: withhold BUY if inventory >= max_inventory,
    withhold SELL if inventory <= -max_inventory.
    """
    if max_inventory is not None:
        if side is Side.BUY and inventory >= max_inventory:
            return []
        if side is Side.SELL and inventory <= -max_inventory:
            return []

    targets: List[RungTarget] = []
    remaining = total_size_cap
    for rung in rungs:
        if not rung.enabled or remaining <= ZERO:
            continue
        qty = min(rung.size, remaining)
        if qty <= ZERO:
            continue
        price = rung_price(fair, rung.edge_bps, side, tick, buffer_ticks)
        price = _apply_min_edge(price, fair, rung.min_edge_bps, side, tick)
        targets.append(RungTarget(side=side, price=price, size=qty, edge_bps=rung.edge_bps))
        remaining -= qty
    return targets


def compute_hedge_order(
    fill_qty: Decimal,
    share_per_unit: Decimal,
    kis_best_ask: Decimal,
    max_slippage_bps: Decimal,
    tick: Decimal,
) -> HedgeOrder:
    """HL short maker fill -> KIS spot BUY marketable-limit hedge.

    Price walks up to best_ask + slippage so the limit order crosses (takerable).
    """
    share_qty = fill_qty * share_per_unit
    slip = max_slippage_bps / BPS
    price = ceil_to_tick(kis_best_ask * (ONE + slip), tick)
    return HedgeOrder(side=Side.BUY, price=price, size=share_qty)
