"""
KRX domestic stock-futures tick-size (호가가격단위) schedule.

KRX individual stock futures use the same price-tiered tick schedule as the
underlying cash equities (2026 rule). The tick is a function of the order price,
NOT a per-contract constant.

Provenance:
  - KRX public rules (2026 domestic tick schedule), cross-checked against
    https://global.krx.co.kr/contents/GLB/06/0602/0602010201/GLB0602010201T3.jsp
  - Empirically validated by the live-verified nautilus-trader KIS gate5c
    Samsung (005930) stock-futures BUY/SELL runner, which floors every order
    price with this exact schedule and had orders accepted by KRX.

Schedule (price tier -> tick, KRW):
    < 2,000    -> 1
    < 5,000    -> 5
    < 20,000   -> 10
    < 50,000   -> 50
    < 200,000  -> 100
    < 500,000  -> 500
    >= 500,000 -> 1,000

The upper bound of each tier is EXCLUSIVE: a price exactly on a threshold falls
into the next (coarser) tier.
"""
from decimal import Decimal
from typing import Tuple

# (exclusive upper-bound threshold, tick) ordered ascending.
KRX_TICK_TIERS: Tuple[Tuple[Decimal, Decimal], ...] = (
    (Decimal(2000), Decimal(1)),
    (Decimal(5000), Decimal(5)),
    (Decimal(20000), Decimal(10)),
    (Decimal(50000), Decimal(50)),
    (Decimal(200000), Decimal(100)),
    (Decimal(500000), Decimal(500)),
)

# Tick for prices at or above the highest threshold (>= 500,000).
KRX_TICK_TOP: Decimal = Decimal(1000)


def tick_size_for_price(price: Decimal) -> Decimal:
    """Return the KRX tick size (호가가격단위) for ``price``.

    Fail-closed: raises ``ValueError`` for non-positive prices rather than
    returning a tick that could mis-align an order.
    """
    if price <= 0:
        raise ValueError(f"kis_futures price must be positive: {price}")
    for threshold, tier_tick in KRX_TICK_TIERS:
        if price < threshold:
            return tier_tick
    return KRX_TICK_TOP


def floor_to_tick(price: Decimal) -> Decimal:
    """Floor ``price`` down to the nearest valid KRX tick boundary.

    Mirrors the nautilus-trader gate5c ``floor_to_tick``: selects the tier from
    the input price itself, then floors. Guarantees a KRX-valid price.

    Fail-closed: raises ``ValueError`` for non-positive prices.
    """
    tick = tick_size_for_price(price)
    return (price // tick) * tick


def ceil_to_tick(price: Decimal) -> Decimal:
    """Round ``price`` UP to the nearest valid KRX tick boundary.

    Used for maker SELL/ask quantization so the order is never repriced
    *downward* (which could make a passive ask marketable). Every tier threshold
    is itself an exact multiple of the next tier's tick, so a ceil that lands on
    or crosses a tier boundary remains a KRX-valid price.

    Fail-closed: raises ``ValueError`` for non-positive prices.
    """
    floored = floor_to_tick(price)
    if floored == price:
        return floored
    return floored + tick_size_for_price(price)
