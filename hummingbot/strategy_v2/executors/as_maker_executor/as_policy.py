"""Pure Avellaneda-Stoikov pricing policy for the Lane 2 (perp-perp) maker.

Stateless A&S formulas pinned to the upstream reference
``hummingbot/strategy/avellaneda_market_making/avellaneda_market_making.pyx``.
No I/O, no connector, no runtime state, no estimators (sigma/kappa are inputs).
Decimal throughout; ln/exp via Decimal methods. See
docs/superpowers/specs/2026-06-21-venue-pair-generalization-phase3a-as-policy-design.md
"""
from __future__ import annotations

from decimal import Decimal

ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
NEG_ONE = Decimal("-1")
ONE_HUNDRED = Decimal("100")


def normalize_inventory(net: Decimal, target: Decimal, max_inventory: Decimal) -> Decimal:
    """q = clamp((net - target) / max_inventory, -1, 1). Requires max_inventory > 0."""
    if max_inventory <= ZERO:
        raise ValueError(f"max_inventory must be > 0, got {max_inventory}")
    q = (net - target) / max_inventory
    if q > ONE:
        return ONE
    if q < NEG_ONE:
        return NEG_ONE
    return q


def reservation_price(
    mid: Decimal, q: Decimal, gamma: Decimal, sigma: Decimal, tau: Decimal
) -> Decimal:
    """r = mid - q*gamma*sigma*tau  (reference .pyx:755). sigma=tick std; q in [-1, 1]."""
    if mid <= ZERO:
        raise ValueError(f"mid must be > 0, got {mid}")
    if not (NEG_ONE <= q <= ONE):
        raise ValueError(f"q must be in [-1, 1], got {q}")
    if gamma < ZERO:
        raise ValueError(f"gamma must be >= 0, got {gamma}")
    if sigma < ZERO:
        raise ValueError(f"sigma must be >= 0, got {sigma}")
    if tau <= ZERO:
        raise ValueError(f"tau must be > 0, got {tau}")
    return mid - (q * gamma * sigma * tau)


def optimal_spread(
    gamma: Decimal, sigma: Decimal, tau: Decimal, kappa: Decimal
) -> Decimal:
    """delta = gamma*sigma*tau + 2*ln(1 + gamma/kappa)/gamma  (reference .pyx:757-758).

    NOTE: the second term uses the reference operation order ``2 * ln(...) / gamma``
    (NOT ``(2/gamma) * ln(...)``) so Decimal rounding matches the golden parity test.
    """
    if gamma <= ZERO:
        raise ValueError(f"gamma must be > 0, got {gamma}")
    if kappa <= ZERO:
        raise ValueError(f"kappa must be > 0, got {kappa}")
    if sigma < ZERO:
        raise ValueError(f"sigma must be >= 0, got {sigma}")
    if tau <= ZERO:
        raise ValueError(f"tau must be > 0, got {tau}")
    return gamma * sigma * tau + TWO * (ONE + gamma / kappa).ln() / gamma


def clamp_quotes(
    mid: Decimal, reservation: Decimal, spread: Decimal, min_spread_pct: Decimal
) -> tuple[Decimal, Decimal]:
    """Returns (bid, ask). Raw A&S quotes reservation -/+ spread/2, floored to a
    min-spread band measured from MID (reference .pyx:760-766)."""
    if mid <= ZERO:
        raise ValueError(f"mid must be > 0, got {mid}")
    if spread < ZERO:
        raise ValueError(f"spread must be >= 0, got {spread}")
    if min_spread_pct < ZERO:
        raise ValueError(f"min_spread_pct must be >= 0, got {min_spread_pct}")
    min_spread = mid / ONE_HUNDRED * min_spread_pct
    max_limit_bid = mid - min_spread / TWO
    min_limit_ask = mid + min_spread / TWO
    bid = min(reservation - spread / TWO, max_limit_bid)
    ask = max(reservation + spread / TWO, min_limit_ask)
    return bid, ask


def inventory_skew_sizes(
    order_amount: Decimal, q: Decimal, eta: Decimal
) -> tuple[Decimal, Decimal]:
    """Returns (bid_size, ask_size). Asymmetric eta damping (reference .pyx:1101-1112):
    q>0 shrinks buys by exp(-eta*q); q<0 shrinks sells by exp(eta*q)."""
    if order_amount <= ZERO:
        raise ValueError(f"order_amount must be > 0, got {order_amount}")
    if not (NEG_ONE <= q <= ONE):
        raise ValueError(f"q must be in [-1, 1], got {q}")
    if eta < ZERO:
        raise ValueError(f"eta must be >= 0, got {eta}")
    bid_size = order_amount * (-eta * q).exp() if q > ZERO else order_amount
    ask_size = order_amount * (eta * q).exp() if q < ZERO else order_amount
    return bid_size, ask_size
