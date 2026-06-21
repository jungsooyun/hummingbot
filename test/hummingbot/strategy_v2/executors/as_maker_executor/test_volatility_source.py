"""Real InstantVolatilitySource adapter — imports the Cython RingBuffer, so it is
skipped cleanly where the .so is unavailable (runs in Docker where it is compiled)."""
from decimal import Decimal

import pytest


def test_instant_volatility_source_contract():
    try:
        from hummingbot.strategy_v2.executors.as_maker_executor.volatility_source import (
            InstantVolatilitySource,
        )
        src = InstantVolatilitySource(sampling_length=5, processing_length=3)
    except ImportError as e:                       # Cython RingBuffer .so unavailable
        pytest.skip(f"InstantVolatilityIndicator/RingBuffer unavailable: {e}")
    assert src.is_ready is False
    for p in (100.0, 101.0, 100.5, 101.5, 102.0):  # fill the sampling buffer (len 5)
        src.add_sample(p)
    assert src.is_ready is True
    assert Decimal(str(src.current_value)) >= 0
