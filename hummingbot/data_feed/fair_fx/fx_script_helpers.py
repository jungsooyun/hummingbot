"""Pure helpers for the FairFxSource-owning script (JEP-148).

Kept free of the V2 strategy-base import chain (which pulls heavy deps like
``paho``) so they unit-test locally; the script that wires lifecycle imports
these and is verified by the Docker boot smoke.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Callable, Dict, Iterable, Optional, Tuple

from hummingbot.core.data_type.common import PriceType

UsdtQuote = Tuple[Optional[Decimal], Optional[Decimal]]


def _clean(value) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if d.is_nan() or d <= 0:
        return None
    return d


def make_usdt_getter(connectors: Dict, fx_connector: str, fx_trading_pair: str) -> Callable[[], UsdtQuote]:
    """Build a sync getter reading USDT-KRW top-of-book from ``connectors[fx_connector]``.
    Returns ``(None, None)`` on missing connector / error / invalid price."""
    def getter() -> UsdtQuote:
        conn = connectors.get(fx_connector)
        if conn is None:
            return (None, None)
        try:
            bid = conn.get_price_by_type(fx_trading_pair, PriceType.BestBid)
            ask = conn.get_price_by_type(fx_trading_pair, PriceType.BestAsk)
        except Exception:
            return (None, None)
        return (_clean(bid), _clean(ask))
    return getter


def discover_fx_market(
    controller_configs: Iterable,
    default: Tuple[str, str] = ("upbit", "USDT-KRW"),
) -> Tuple[str, str]:
    """Return ``(fx_connector, fx_trading_pair)`` from the first controller config
    that declares both, else ``default``."""
    for cfg in controller_configs:
        conn = getattr(cfg, "fx_connector", None)
        pair = getattr(cfg, "fx_trading_pair", None)
        if conn and pair:
            return (conn, pair)
    return default
