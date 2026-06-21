"""Pure fair-price math shared by ladder and base executor seams."""
from __future__ import annotations

from decimal import Decimal

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import Side

ZERO = Decimal("0")
ONE = Decimal("1")


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
