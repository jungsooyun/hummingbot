"""
Unit tests for KRX domestic stock-futures tick-size schedule.

Authoritative source: KRX domestic tick schedule (2026 rule), cross-checked
against the KRX public rules page and empirically validated by the live-verified
nautilus-trader gate5c Samsung (005930) stock-futures BUY/SELL runner, which
floors every order price with this exact schedule and had orders accepted by KRX.

Schedule (price tier -> tick):
  < 2,000   -> 1
  < 5,000   -> 5
  < 20,000  -> 10
  < 50,000  -> 50
  < 200,000 -> 100
  < 500,000 -> 500
  >= 500,000 -> 1,000
"""
from decimal import Decimal

import pytest

from hummingbot.connector.derivative.kis_futures.kis_futures_tick import (
    floor_to_tick,
    tick_size_for_price,
)


# (price, expected tick) covering every tier interior and exact boundaries.
# Boundaries are EXCLUSIVE upper bounds: price == threshold falls into the next tier.
@pytest.mark.parametrize(
    "price, tick",
    [
        ("1", "1"),            # interior of <2000
        ("1999", "1"),         # just below boundary
        ("2000", "5"),         # boundary -> next tier (<5000)
        ("4999", "5"),
        ("5000", "10"),        # boundary -> <20000
        ("19999", "10"),
        ("20000", "50"),       # boundary -> <50000
        ("49999", "50"),
        ("50000", "100"),      # boundary -> <200000
        ("199999", "100"),
        ("200000", "500"),     # boundary -> <500000
        ("363000", "500"),     # Samsung 005930 futures (observed live)
        ("499999", "500"),
        ("500000", "1000"),    # boundary -> top tier
        ("2500000", "1000"),   # SK hynix-scale price, top tier
    ],
)
def test_tick_size_for_price_tiers(price, tick):
    assert tick_size_for_price(Decimal(price)) == Decimal(tick)


@pytest.mark.parametrize("bad", ["0", "-1", "-500000"])
def test_tick_size_for_price_nonpositive_raises(bad):
    with pytest.raises(ValueError):
        tick_size_for_price(Decimal(bad))


@pytest.mark.parametrize(
    "price, floored",
    [
        ("363000", "363000"),   # already aligned to 500
        ("363005", "363000"),   # 200k-500k tier: tick 500 -> floor down
        ("363499", "363000"),
        ("363500", "363500"),   # next valid tick
        ("50000", "50000"),     # aligned to 100
        ("50050", "50000"),     # 50k-200k tier: tick 100
        ("50099", "50000"),
        ("50100", "50100"),
        ("1999", "1999"),       # <2000 tier: tick 1, integer already valid
        ("3003", "3000"),       # <5000 tier: tick 5
        ("500000", "500000"),   # top tier: tick 1000
        ("500999", "500000"),
    ],
)
def test_floor_to_tick(price, floored):
    assert floor_to_tick(Decimal(price)) == Decimal(floored)


@pytest.mark.parametrize("bad", ["0", "-5"])
def test_floor_to_tick_nonpositive_raises(bad):
    with pytest.raises(ValueError):
        floor_to_tick(Decimal(bad))


def test_floor_to_tick_returns_decimal():
    result = floor_to_tick(Decimal("363005"))
    assert isinstance(result, Decimal)
