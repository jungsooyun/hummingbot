"""Pure USD/KRW fair-FX blend. Stdlib only — mirrors ladder_policy / gate_chain.
blend = bank anchor (weight 4) + USDT-KRW crypto premium (weight 1), with the
USDT-KRW side CLAMPED into the bank band BEFORE blending (else the cap is a no-op).
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional, Tuple

W_BANK = Decimal("4")
W_USDT = Decimal("1")
BAND_LO = Decimal("0.98")   # -2%
BAND_HI = Decimal("1.03")   # +3%


def _blend_side(bank: Decimal, usdt_side: Decimal) -> Decimal:
    capped = min(max(usdt_side, bank * BAND_LO), bank * BAND_HI)
    return (bank * W_BANK + capped * W_USDT) / (W_BANK + W_USDT)


def blend_fx_quote(
    bank: Optional[Decimal],
    usdt_bid: Optional[Decimal],
    usdt_ask: Optional[Decimal],
) -> Tuple[Decimal, Decimal]:
    """Return (fx_bid, fx_ask). Bank is the trusted anchor.
    USDT-KRW missing -> bank-only degrade (fx_bid=fx_ask=bank).
    Bank missing/non-positive -> ValueError (FX invalid; caller gates).
    """
    if bank is None or bank <= 0:
        raise ValueError("bank rate must be positive")
    if usdt_bid is None or usdt_ask is None or usdt_bid <= 0 or usdt_ask <= 0:
        return bank, bank
    return _blend_side(bank, usdt_bid), _blend_side(bank, usdt_ask)
