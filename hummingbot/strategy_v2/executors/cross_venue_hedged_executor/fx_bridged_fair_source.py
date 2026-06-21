"""KIS fair-price provider (JEP-187): KRW spot -> USD via side-aware FX (JEP-148/185).

Holds the moved ``_get_fx`` (process-wide FairFxSource + static fallback) and the
JEP-185 currency bridge in BOTH directions, relocated verbatim from
``LadderMakerExecutor`` so the forward/backward FX pairing is a single reviewable
invariant.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Optional, Tuple

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.fair_math import compute_fair_price
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import Side

ZERO = Decimal("0")


class FxBridgedFairSource:
    def __init__(self, side_aware_fx: bool, static_fx_rate: Optional[Decimal], logger: logging.Logger):
        self._side_aware_fx = side_aware_fx
        self._static_fx_rate = static_fx_rate
        self._logger = logger

    def _get_fx(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Live blended USD/KRW from the process-wide FairFxSource (JEP-148), with a
        guarded ``static_fx_rate`` offline/test fallback ONLY. ``None`` -> (None, None)
        so the fair gate closes."""
        from hummingbot.data_feed.fair_fx.fair_fx_source import FairFxSource

        quote = FairFxSource.get_instance().get_fx()
        if quote is not None:
            return quote
        if self._static_fx_rate and self._static_fx_rate > ZERO:
            rate = self._static_fx_rate
            return rate, rate
        return None, None

    def fair_from_book(self, ref_bid: Decimal, ref_ask: Decimal, side: Side) -> Optional[Decimal]:
        fx_bid, fx_ask = self._get_fx()
        if fx_bid is None or fx_ask is None:
            return None
        return compute_fair_price(ref_bid, ref_ask, fx_bid, fx_ask, side, self._side_aware_fx)

    def hedge_price_to_maker_quote(self, price: Decimal, side: TradeType) -> Decimal:
        """JEP-185 backward bridge: hedge BUY / fx_bid, hedge SELL / fx_ask (mid if not
        side-aware) so a round trip nets at fair. FX unavailable -> return 0 + ERROR
        (never book raw KRW as USD; inventory is still tracked by the caller)."""
        fx_bid, fx_ask = self._get_fx()
        if fx_bid is None or fx_ask is None or fx_bid <= ZERO or fx_ask <= ZERO:
            self._logger.error(
                "JEP-185: FX unavailable at hedge-fill credit; skipping _spot_cash/quote "
                "accrual for this fill (inventory still tracked) to avoid KRW-as-USD "
                "corruption. PnL under-reports this leg. Check FairFxSource/static_fx_rate."
            )
            return ZERO
        if self._side_aware_fx:
            fx = fx_bid if side == TradeType.BUY else fx_ask
        else:
            fx = (fx_bid + fx_ask) / Decimal("2")
        return price / fx

    def observe_fx_legs(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        return self._get_fx()
