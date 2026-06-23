"""Read-seam for each hedge-leg's current signed position (JEP-205 3c).

Standalone + unit-tested; NO base wiring in 3c (the seed path that would consume it
is adopt-gated and lands in 3d — spec §6, peer-review F3). Mirrors JEP-187's
DirectFairSource: ship the seam ahead of its consumer.
"""
from __future__ import annotations
from decimal import Decimal
from typing import Protocol, Tuple

ZERO = Decimal("0")


def read_signed_perp_position(connector, trading_pair) -> Tuple[Decimal, Decimal]:
    """(signed_base, entry_price) from connector.account_positions. LONG +, SHORT -.
    Returns (0, 0) if flat/absent. HEDGE position_mode disambiguation is a 3d call-site concern."""
    positions = getattr(connector, "account_positions", {}) or {}
    items = positions.values() if hasattr(positions, "values") else positions
    for pos in items:
        if getattr(pos, "trading_pair", None) != trading_pair:
            continue
        amt = Decimal(str(getattr(pos, "amount", ZERO)))
        if amt == ZERO:
            return ZERO, ZERO
        side = getattr(pos, "position_side", None)
        signed = -abs(amt) if str(getattr(side, "name", side)) == "SHORT" else abs(amt)
        return signed, Decimal(str(getattr(pos, "entry_price", ZERO)))
    return ZERO, ZERO


class InventoryAdapter(Protocol):
    def read_maker_signed(self) -> Tuple[Decimal, Decimal]: ...
    def read_hedge_signed(self) -> Tuple[Decimal, Decimal]: ...


class SpotHedgeInventory:
    """Reproduces the current KIS spot read (for the 3d seed-path swap-in)."""
    def __init__(self, connectors, maker, maker_pair, hedge, hedge_pair, base_asset):
        self._connectors, self._maker, self._maker_pair = connectors, maker, maker_pair
        self._hedge, self._hedge_pair, self._base_asset = hedge, hedge_pair, base_asset

    def read_maker_signed(self) -> Tuple[Decimal, Decimal]:
        return read_signed_perp_position(self._connectors[self._maker], self._maker_pair)

    def read_hedge_signed(self) -> Tuple[Decimal, Decimal]:
        bal = self._connectors[self._hedge].get_balance(self._base_asset)  # always >= 0
        return max(Decimal(str(bal)), ZERO), ZERO


class PerpHedgeInventory:
    """OKX signed perp hedge (the 3c deliverable)."""
    def __init__(self, connectors, maker, maker_pair, hedge, hedge_pair):
        self._connectors, self._maker, self._maker_pair = connectors, maker, maker_pair
        self._hedge, self._hedge_pair = hedge, hedge_pair

    def read_maker_signed(self) -> Tuple[Decimal, Decimal]:
        return read_signed_perp_position(self._connectors[self._maker], self._maker_pair)

    def read_hedge_signed(self) -> Tuple[Decimal, Decimal]:
        return read_signed_perp_position(self._connectors[self._hedge], self._hedge_pair)
