"""Fair-price provider seam (JEP-187) — the currency bridge, both directions.

Owns the JEP-185 invariant in one tested object: forward (reference book ->
maker-quote-currency fair) and backward (hedge fill price -> maker quote). KIS impl is
``FxBridgedFairSource``; neutral default is ``DirectFairSource`` (same-currency venue
pair, identity bridge). Inventory skew is NOT here — it stays in the executor
(venue-agnostic policy).
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional, Protocol, Tuple

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import Side


class FairPriceProvider(Protocol):
    def fair_from_book(self, ref_bid: Decimal, ref_ask: Decimal, side: Side) -> Optional[Decimal]:
        """Reference (hedge-venue) book top -> maker-quote-currency fair (PRE-skew).
        None closes the data-readiness gate. Side selection mirrors compute_fair_price
        (BUY uses bid, SELL uses ask)."""
        ...

    def hedge_price_to_maker_quote(self, price: Decimal, side: TradeType) -> Decimal:
        """Hedge fill price -> maker-quote currency at fill time. Same-currency: identity."""
        ...

    def observe_fx_legs(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """(fx_bid, fx_ask) for observe/status. Same-currency venues: (None, None)."""
        ...


class DirectFairSource:
    """Neutral default: same-currency venue pair (e.g. HL perp <-> CEX perp, both USD)."""

    def fair_from_book(self, ref_bid: Decimal, ref_ask: Decimal, side: Side) -> Optional[Decimal]:
        return ref_bid if side is Side.BUY else ref_ask

    def hedge_price_to_maker_quote(self, price: Decimal, side: TradeType) -> Decimal:
        return price

    def observe_fx_legs(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        return (None, None)
