from __future__ import annotations

import ast
import pathlib
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.as_maker_executor import as_policy as P

D = Decimal
TWO_REF = Decimal("2")
ONE_REF = Decimal("1")


# ---------- normalize_inventory ----------
def test_normalize_inventory_center_and_clamp():
    assert P.normalize_inventory(D("5"), D("5"), D("10")) == D("0")          # net==target -> 0
    assert P.normalize_inventory(D("10"), D("0"), D("10")) == D("1")         # at cap -> 1
    assert P.normalize_inventory(D("-10"), D("0"), D("10")) == D("-1")       # at -cap -> -1
    assert P.normalize_inventory(D("50"), D("0"), D("10")) == D("1")         # over cap clamps
    assert P.normalize_inventory(D("-50"), D("0"), D("10")) == D("-1")
    assert P.normalize_inventory(D("3"), D("1"), D("10")) == D("0.2")        # linear within band


def test_normalize_inventory_guard():
    with pytest.raises(ValueError):
        P.normalize_inventory(D("1"), D("0"), D("0"))
    with pytest.raises(ValueError):
        P.normalize_inventory(D("1"), D("0"), D("-1"))


# ---------- reservation_price ----------
def test_reservation_price_skew_direction():
    mid = D("100")
    # q == 0 -> r == mid
    assert P.reservation_price(mid, D("0"), D("0.5"), D("2"), D("1")) == mid
    # q > 0 (long) -> r < mid
    assert P.reservation_price(mid, D("0.5"), D("0.5"), D("2"), D("1")) < mid
    # q < 0 (short) -> r > mid
    assert P.reservation_price(mid, D("-0.5"), D("0.5"), D("2"), D("1")) > mid
    # gamma == 0 -> no skew (valid)
    assert P.reservation_price(mid, D("0.5"), D("0"), D("2"), D("1")) == mid


@pytest.mark.parametrize("kwargs", [
    dict(mid=D("0"), q=D("0"), gamma=D("1"), sigma=D("1"), tau=D("1")),     # mid<=0
    dict(mid=D("100"), q=D("1.5"), gamma=D("1"), sigma=D("1"), tau=D("1")), # q>1
    dict(mid=D("100"), q=D("-1.5"), gamma=D("1"), sigma=D("1"), tau=D("1")),# q<-1
    dict(mid=D("100"), q=D("0"), gamma=D("-1"), sigma=D("1"), tau=D("1")),  # gamma<0
    dict(mid=D("100"), q=D("0"), gamma=D("1"), sigma=D("-1"), tau=D("1")),  # sigma<0
    dict(mid=D("100"), q=D("0"), gamma=D("1"), sigma=D("1"), tau=D("0")),   # tau<=0
])
def test_reservation_price_guards(kwargs):
    with pytest.raises(ValueError):
        P.reservation_price(**kwargs)


# ---------- optimal_spread ----------
def test_optimal_spread_monotonicity():
    base = P.optimal_spread(D("0.5"), D("2"), D("1"), D("1.5"))
    assert P.optimal_spread(D("0.5"), D("3"), D("1"), D("1.5")) > base   # ↑ sigma (gamma,kappa fixed)
    assert P.optimal_spread(D("0.5"), D("2"), D("2"), D("1.5")) > base   # ↑ tau
    assert P.optimal_spread(D("0.5"), D("2"), D("1"), D("3")) < base     # ↑ kappa -> ↓ spread
    # sigma == 0 -> only adverse-selection term (well-defined, positive)
    assert P.optimal_spread(D("0.5"), D("0"), D("1"), D("1.5")) > D("0")


def test_optimal_spread_tiny_kappa_large_finite():
    # spec §4(c): very small kappa -> large but FINITE spread (kappa>0 guard holds)
    s = P.optimal_spread(D("0.5"), D("2"), D("1"), D("0.0001"))
    assert s.is_finite()
    assert s > P.optimal_spread(D("0.5"), D("2"), D("1"), D("1.5"))  # much larger than baseline


@pytest.mark.parametrize("kwargs", [
    dict(gamma=D("0"), sigma=D("1"), tau=D("1"), kappa=D("1")),    # gamma<=0
    dict(gamma=D("-1"), sigma=D("1"), tau=D("1"), kappa=D("1")),
    dict(gamma=D("1"), sigma=D("1"), tau=D("1"), kappa=D("0")),    # kappa<=0
    dict(gamma=D("1"), sigma=D("1"), tau=D("1"), kappa=D("-1")),
    dict(gamma=D("1"), sigma=D("-1"), tau=D("1"), kappa=D("1")),   # sigma<0
    dict(gamma=D("1"), sigma=D("1"), tau=D("0"), kappa=D("1")),    # tau<=0
])
def test_optimal_spread_guards(kwargs):
    with pytest.raises(ValueError):
        P.optimal_spread(**kwargs)


# ---------- clamp_quotes ----------
def test_clamp_quotes_band_and_floor():
    mid = D("100")
    bid, ask = P.clamp_quotes(mid, D("100"), D("2"), D("1"))   # r==mid, spread 2, min 1%
    assert bid <= ask
    assert ask - bid >= mid * D("1") / D("100")               # >= min_spread band
    # heavy skew: reservation far below mid -> ask floored to mid+min/2, bid free below
    bid2, ask2 = P.clamp_quotes(mid, D("90"), D("2"), D("1"))
    assert ask2 == mid + (mid / D("100") * D("1")) / D("2")
    assert bid2 == D("90") - D("2") / D("2")


@pytest.mark.parametrize("kwargs", [
    dict(mid=D("0"), reservation=D("100"), spread=D("2"), min_spread_pct=D("1")),    # mid<=0
    dict(mid=D("100"), reservation=D("100"), spread=D("-1"), min_spread_pct=D("1")), # spread<0
    dict(mid=D("100"), reservation=D("100"), spread=D("2"), min_spread_pct=D("-1")), # min<0
])
def test_clamp_quotes_guards(kwargs):
    with pytest.raises(ValueError):
        P.clamp_quotes(**kwargs)


# ---------- inventory_skew_sizes ----------
def test_inventory_skew_sizes_asymmetric_damping():
    amt = D("10")
    # q == 0 -> both full
    assert P.inventory_skew_sizes(amt, D("0"), D("1")) == (amt, amt)
    # q > 0 (long) -> bid shrinks, ask full
    b, a = P.inventory_skew_sizes(amt, D("0.5"), D("1"))
    assert b < amt and a == amt
    # q < 0 (short) -> ask shrinks, bid full
    b2, a2 = P.inventory_skew_sizes(amt, D("-0.5"), D("1"))
    assert a2 < amt and b2 == amt
    # eta == 0 -> no damping
    assert P.inventory_skew_sizes(amt, D("0.5"), D("0")) == (amt, amt)


@pytest.mark.parametrize("kwargs", [
    dict(order_amount=D("0"), q=D("0"), eta=D("1")),     # amount<=0
    dict(order_amount=D("10"), q=D("1.5"), eta=D("1")),  # q>1
    dict(order_amount=D("10"), q=D("-1.5"), eta=D("1")), # q<-1
    dict(order_amount=D("10"), q=D("0"), eta=D("-1")),   # eta<0
])
def test_inventory_skew_sizes_guards(kwargs):
    with pytest.raises(ValueError):
        P.inventory_skew_sizes(**kwargs)


# ---------- GOLDEN PARITY: formula-level pin against avellaneda_market_making.pyx ----------
# Each test recomputes the REFERENCE expression inline (reference operation order)
# and asserts as_policy equals it. This pins the extracted math to the proven source.

_GRID_RP = [
    # (mid, q, gamma, sigma, tau)
    (D("100"), D("0.3"), D("0.5"), D("2.5"), D("1")),
    (D("43000"), D("-0.7"), D("0.1"), D("15"), D("1")),
    (D("1.05"), D("1"), D("0.8"), D("0.002"), D("0.5")),    # q == +1 boundary
    (D("250"), D("-1"), D("0.5"), D("3"), D("1")),          # q == -1 boundary
    (D("250"), D("0"), D("0.5"), D("3"), D("1")),
]


@pytest.mark.parametrize("mid,q,gamma,sigma,tau", _GRID_RP)
def test_golden_reservation_price(mid, q, gamma, sigma, tau):
    # reference .pyx:755 : price - (q * gamma * vol * time_left_fraction)
    expected = mid - (q * gamma * sigma * tau)
    assert P.reservation_price(mid, q, gamma, sigma, tau) == expected


_GRID_SPREAD = [
    # (gamma, sigma, tau, kappa)
    (D("0.5"), D("2.5"), D("1"), D("1.5")),
    (D("0.1"), D("15"), D("1"), D("0.8")),
    (D("0.8"), D("0.002"), D("0.5"), D("3")),
    (D("0.5"), D("0"), D("1"), D("1.5")),       # sigma == 0
    (D("0.5"), D("2.5"), D("1"), D("0.001")),   # tiny kappa -> large finite spread
]


@pytest.mark.parametrize("gamma,sigma,tau,kappa", _GRID_SPREAD)
def test_golden_optimal_spread(gamma, sigma, tau, kappa):
    # reference .pyx:757-758 : gamma*vol*tau ; then += 2 * Decimal(1 + gamma/kappa).ln() / gamma
    expected = gamma * sigma * tau
    expected += TWO_REF * (ONE_REF + gamma / kappa).ln() / gamma
    assert P.optimal_spread(gamma, sigma, tau, kappa) == expected


_GRID_CLAMP = [
    # (mid, reservation, spread, min_spread_pct)
    (D("100"), D("100"), D("2"), D("1")),
    (D("100"), D("90"), D("2"), D("1")),    # reservation below mid
    (D("100"), D("110"), D("2"), D("1")),   # reservation above mid
    (D("100"), D("100"), D("0.1"), D("1")), # tight spread floored by min
]


@pytest.mark.parametrize("mid,reservation,spread,min_spread_pct", _GRID_CLAMP)
def test_golden_clamp_quotes(mid, reservation, spread, min_spread_pct):
    # reference .pyx:760-766 (floor measured from MID/price)
    min_spread = mid / D("100") * min_spread_pct
    max_limit_bid = mid - min_spread / D("2")
    min_limit_ask = mid + min_spread / D("2")
    exp_bid = min(reservation - spread / D("2"), max_limit_bid)
    exp_ask = max(reservation + spread / D("2"), min_limit_ask)
    assert P.clamp_quotes(mid, reservation, spread, min_spread_pct) == (exp_bid, exp_ask)


_GRID_ETA = [
    # (amount, q, eta)
    (D("10"), D("0.5"), D("1")),
    (D("10"), D("-0.5"), D("1")),
    (D("10"), D("0"), D("1")),
    (D("5"), D("1"), D("0.3")),
    (D("5"), D("-1"), D("0.3")),
]


@pytest.mark.parametrize("amount,q,eta", _GRID_ETA)
def test_golden_inventory_skew_sizes(amount, q, eta):
    # reference .pyx:1101-1112
    exp_bid = amount * (-eta * q).exp() if q > D("0") else amount
    exp_ask = amount * (eta * q).exp() if q < D("0") else amount
    assert P.inventory_skew_sizes(amount, q, eta) == (exp_bid, exp_ask)


def test_as_policy_is_pure_stdlib_decimal_only():
    """as_policy.py must import only __future__, decimal (+ typing). No numpy/scipy/
    pandas/connector/executors-runtime imports — the purity contract (spec §3.1).

    Scope: checks STATIC top-level import roots (the contract for this hand-written
    module). It does not chase dynamic __import__ or no-import I/O; the full source is
    code-reviewed to use only static imports, so this guard is sufficient here."""
    src = pathlib.Path(P.__file__).read_text()
    tree = ast.parse(src)
    roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                roots.add(node.module.split(".")[0])
    assert roots <= {"__future__", "decimal", "typing"}, f"forbidden imports: {roots}"
