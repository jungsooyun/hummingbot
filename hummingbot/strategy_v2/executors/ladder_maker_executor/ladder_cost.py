"""Pure round-trip friction model for the HIP3-KIS ladder (stdlib only).

One round trip = the full life of one hedged unit::

    entry: HL perp maker fill (SELL) + KIS spot BUY  (no tax)
    exit:  HL perp close (BUY maker/taker) + KIS spot SELL (증권거래세 once)

The dominant term is the KIS 증권거래세 (20 bps), paid ONCE on the spot sell.
All numbers are user-confirmed (2026-06-18). This module is the single source of
truth for the cost the ladder folds into its NET edge (placement = net + cost).

Routing keys mirror ``kis_constants.MARKET_ROUTING_*`` values
(``krx`` / ``nxt`` / ``sor``); ``sor`` (통합) routes via NXT-class usage fees.
No I/O, no hummingbot imports — keep it pure so it stays unit-testable without the
Cython runtime.
"""
from decimal import Decimal

# KIS spot
TAX_SELL_BPS = Decimal("20")            # 증권거래세, SELL only (KOSPI 농특세, 2025~26)
KIS_BROKERAGE_BPS = Decimal("1.5")      # 위탁수수료 (DEFAULT_FEES 0.00015), each side
KRX_USAGE_BPS = Decimal("0.36396")      # 유관기관 0.0036396%, each side (KRX)
NXT_USAGE_BPS = Decimal("0.31833")      # 유관기관 0.0031833%, each side (NXT, cheaper)

# Hyperliquid HIP-3 (builder dex) — NOT the connector DEFAULT_FEES (maker 0 / taker 0.025%)
HL_HIP3_MAKER_BPS = Decimal("0.3")      # 0.003%
HL_HIP3_TAKER_BPS = Decimal("1")        # 0.01%

_USAGE_BY_ROUTING = {
    "krx": KRX_USAGE_BPS,
    "nxt": NXT_USAGE_BPS,
    "sor": NXT_USAGE_BPS,
}


def round_trip_cost_bps(routing: str = "krx", close_is_taker: bool = False) -> Decimal:
    """Total friction (bps) to open AND eventually flatten one hedged unit.

    Tax is included exactly once (on the spot sell at exit). ``routing`` selects the
    KIS 유관기관 rate; ``close_is_taker`` uses the HL taker fee for the perp close
    instead of the maker fee.

    Raises ``ValueError`` on an unknown routing so a config typo fails loud rather
    than silently mispricing the ladder.
    """
    key = routing.strip().lower()
    if key not in _USAGE_BY_ROUTING:
        raise ValueError(f"unknown KIS routing: {routing!r}")
    usage = _USAGE_BY_ROUTING[key]
    hl_close = HL_HIP3_TAKER_BPS if close_is_taker else HL_HIP3_MAKER_BPS
    entry = HL_HIP3_MAKER_BPS + (KIS_BROKERAGE_BPS + usage)          # buy: no tax
    exit_ = hl_close + (KIS_BROKERAGE_BPS + usage + TAX_SELL_BPS)    # sell: tax once
    return entry + exit_


class KisHlCostModel:
    """KIS spot + HL HIP-3 perp round-trip friction (cost-model impl #1).

    Thin class wrapper over the pure ``round_trip_cost_bps`` so the controller
    constructs cost via a named object. No Protocol (YAGNI — lane 2 folds cost into
    the A&S optimal spread, a different shape; see JEP-188 spec §3.2).
    """

    def __init__(self, routing: str = "krx", close_is_taker: bool = False):
        self._routing = routing
        self._close_is_taker = close_is_taker

    def round_trip_cost_bps(self) -> Decimal:
        return round_trip_cost_bps(self._routing, self._close_is_taker)
