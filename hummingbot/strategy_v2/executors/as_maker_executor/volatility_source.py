"""Volatility seam for the Lane 2 A&S maker (Phase 3b).

Isolates the Cython dependency: InstantVolatilityIndicator -> BaseTrailingIndicator
-> RingBuffer (ring_buffer.pyx) needs a compiled .so. The Cython import lives inside
the adapter's __init__, so importing this module (Protocol + class object) imports no
Cython; only constructing InstantVolatilitySource does. The executor depends on the
Protocol; tests inject a pure-Python double.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class VolatilitySource(Protocol):
    def add_sample(self, value: float) -> None: ...
    @property
    def current_value(self) -> float: ...
    @property
    def is_ready(self) -> bool: ...


class InstantVolatilitySource:
    """Production adapter wrapping the Cython-backed InstantVolatilityIndicator."""

    def __init__(self, sampling_length: int, processing_length: int) -> None:
        from hummingbot.strategy.__utils__.trailing_indicators.instant_volatility import (
            InstantVolatilityIndicator,
        )
        self._indicator = InstantVolatilityIndicator(sampling_length, processing_length)

    def add_sample(self, value: float) -> None:
        self._indicator.add_sample(value)

    @property
    def current_value(self) -> float:
        return self._indicator.current_value

    @property
    def is_ready(self) -> bool:
        return self._indicator.is_sampling_buffer_full
