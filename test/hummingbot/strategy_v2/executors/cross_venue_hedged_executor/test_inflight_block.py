"""Unit tests for the in-flight conflict-blocking primitive (JEP-177 Fix #5).

``blocked_targets`` decides which of this tick's ``to_place`` targets to suppress when
an unmatched in-flight maker rung exists on the same side. The conservative default
(``granular=False``) reproduces the round-2 whole-side block; the opt-in granular mode
(``granular=True``) suppresses ONLY targets that collide with an unmatched in-flight
rung within ``radius`` price, so a genuinely-new far-away same-side rung is no longer
deferred. Pure stdlib + pytest — no hummingbot runtime / Cython.
"""
from decimal import Decimal

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import (
    RungTarget,
    Side,
    blocked_targets,
)

D = Decimal


def _t(price, size="1", side=Side.SELL, edge="10"):
    return RungTarget(side=side, price=D(price), size=D(size), edge_bps=D(edge))


def _inflight(side, price):
    """An unmatched in-flight rung, as (side, quantized_price) — the shape the executor
    derives from ``_maker_placed_rung`` for orders still in ``to_cancel`` and inflight."""
    return (side, D(price))


# ------------------------------------------------------------------ default (whole-side)

def test_default_blocks_whole_side():
    # granular=False (default): ANY unmatched in-flight rung on a side blocks EVERY
    # same-side target — identical to the pre-JEP-177 round-2 whole-side block.
    inflight = [_inflight(Side.SELL, "50.10")]
    targets = [_t("50.10"), _t("50.90"), _t("55.00")]  # near + far same-side targets
    out = blocked_targets(targets, inflight, radius=D("0.04"), granular=False)
    assert out == []  # all SELL targets suppressed


def test_default_leaves_other_side_untouched():
    inflight = [_inflight(Side.SELL, "50.10")]
    targets = [_t("50.10", side=Side.SELL), _t("49.00", side=Side.BUY)]
    out = blocked_targets(targets, inflight, radius=D("0.04"), granular=False)
    assert out == [_t("49.00", side=Side.BUY)]  # BUY side unaffected


def test_default_no_inflight_places_all():
    targets = [_t("50.10"), _t("50.90")]
    out = blocked_targets(targets, [], radius=D("0.04"), granular=False)
    assert out == targets


# ------------------------------------------------------------------ granular mode

def test_granular_blocks_same_price_collision():
    # Size-shift / same-price round-2 case: a target at the in-flight rung's price still
    # collides and is suppressed even in granular mode.
    inflight = [_inflight(Side.SELL, "50.10")]
    targets = [_t("50.10")]
    out = blocked_targets(targets, inflight, radius=D("0.04"), granular=True)
    assert out == []


def test_granular_blocks_fair_move_within_radius():
    # Fair-move round-2 case: in-flight 50.10, target 50.13 (3 ticks). With a 4-tick radius
    # (0.04) the moved target still collides -> suppressed (round-2 fix preserved).
    inflight = [_inflight(Side.SELL, "50.10")]
    targets = [_t("50.13")]
    out = blocked_targets(targets, inflight, radius=D("0.04"), granular=True)
    assert out == []


def test_granular_places_genuinely_new_far_rung():
    # The whole point of Fix #5: a genuinely-new same-side rung far from the unmatched
    # in-flight rung is NOT deferred under granular mode.
    inflight = [_inflight(Side.SELL, "50.10")]
    targets = [_t("50.10"), _t("55.00")]  # one collides, one is far
    out = blocked_targets(targets, inflight, radius=D("0.04"), granular=True)
    assert out == [_t("55.00")]  # far rung placed; colliding rung suppressed


def test_granular_radius_boundary_is_inclusive_block():
    # A target exactly ``radius`` away is treated as a collision (block), matching the
    # conservative >= reprice-threshold semantics (never under-protect at the boundary).
    inflight = [_inflight(Side.SELL, "50.10")]
    targets = [_t("50.14")]  # exactly 0.04 away
    out = blocked_targets(targets, inflight, radius=D("0.04"), granular=True)
    assert out == []


def test_granular_collision_is_side_scoped():
    # An in-flight SELL rung must not block a BUY target at the same price.
    inflight = [_inflight(Side.SELL, "50.10")]
    targets = [_t("50.10", side=Side.BUY)]
    out = blocked_targets(targets, inflight, radius=D("0.04"), granular=True)
    assert out == [_t("50.10", side=Side.BUY)]
