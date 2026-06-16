"""Process-wide live USD/KRW fair-FX source (JEP-148).

A ``NetworkBase`` singleton that owns the Toss bank-rate poll, reads the
USDT-KRW top-of-book via an injected getter, and blends them (clamp-then-blend,
``fx_policy.blend_fx_quote``). ``get_fx()`` is a cheap synchronous read for the
trading loop; the heavy bank poll runs in ``start_network()``'s background task,
so it can later move out of process without touching the executor.

Lifecycle (``start()``/``stop()``) is owned at the strategy/script level, mirroring
``TradingCore`` driving ``RateOracle`` — never the controller. The source holds NO
config/secrets: the script builds the ``TossFxClient`` and the ``usdt_price_getter``
and injects them via ``configure()``.

``get_fx()`` returns ``None`` whenever the bank rate is stale or never fetched →
callers (the ladder executor) treat that as invalid FX and gate. USDT-KRW
staleness is tolerated (degrades to bank-only).
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Callable, Optional, Tuple

from hummingbot.core.network_base import NetworkBase
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.strategy_v2.executors.ladder_maker_executor.fx_policy import blend_fx_quote

UsdtGetter = Callable[[], Tuple[Optional[Decimal], Optional[Decimal]]]


class FairFxSource(NetworkBase):
    _instance: Optional["FairFxSource"] = None

    @classmethod
    def get_instance(cls) -> "FairFxSource":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        super().__init__()
        self._toss_client = None
        self._usdt_price_getter: Optional[UsdtGetter] = None
        self._poll_interval_s: float = 30.0
        self._max_bank_age_s: float = 120.0
        self._bank: Optional[Decimal] = None
        self._bank_ts: float = 0.0
        self._poll_task: Optional[asyncio.Task] = None

    def _now(self) -> float:
        return time.time()

    def configure(
        self,
        toss_client,
        usdt_price_getter: UsdtGetter,
        *,
        poll_interval_s: float = 30.0,
        max_bank_age_s: float = 120.0,
    ) -> None:
        """Inject dependencies (no global reach-in). ``usdt_price_getter()`` returns
        ``(bid, ask)`` or ``(None, None)`` (reads ``connectors[fx_connector]``)."""
        self._toss_client = toss_client
        self._usdt_price_getter = usdt_price_getter
        self._poll_interval_s = float(poll_interval_s)
        self._max_bank_age_s = float(max_bank_age_s)

    async def _poll_once(self) -> None:
        bank = await self._toss_client.fetch_exchange_rate()
        self._bank = bank
        self._bank_ts = self._now()

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().warning(f"FairFxSource bank poll failed: {e}")
            await asyncio.sleep(self._poll_interval_s)

    async def check_network(self) -> NetworkStatus:
        if self._toss_client is None:
            return NetworkStatus.NOT_CONNECTED
        try:
            await self._poll_once()   # primes the bank rate too
            return NetworkStatus.CONNECTED
        except Exception:
            return NetworkStatus.NOT_CONNECTED

    async def start_network(self) -> None:
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = safe_ensure_future(self._poll_loop())

    async def stop_network(self) -> None:
        if self._poll_task is not None:
            self._poll_task.cancel()
            self._poll_task = None

    def get_fx(self) -> Optional[Tuple[Decimal, Decimal]]:
        if self._bank is None or self._now() - self._bank_ts > self._max_bank_age_s:
            return None
        usdt_bid: Optional[Decimal] = None
        usdt_ask: Optional[Decimal] = None
        if self._usdt_price_getter is not None:
            try:
                usdt_bid, usdt_ask = self._usdt_price_getter()
            except Exception:
                usdt_bid, usdt_ask = None, None
        return blend_fx_quote(self._bank, usdt_bid, usdt_ask)
