"""Unit tests for the shared maker partial-reprice primitives (JEP-145).

``Side`` / ``RungTarget`` / ``RestingOrder`` / ``LadderDiff`` / ``diff_ladder_targets`` were
relocated from ``ladder_maker_executor.ladder_policy`` into the cross_venue base layer so any
hedge executor (not just the ladder) can use them without importing a subclass package.
Pure stdlib + pytest — no hummingbot runtime / Cython.
"""
from decimal import Decimal

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import (
    LadderDiff,
    RestingOrder,
    RungTarget,
    Side,
    diff_ladder_targets,
)

D = Decimal
TICK = D("0.01")
THRESH = D("2")  # 2 ticks


def _t(price, size, side=Side.SELL, edge="10"):
    return RungTarget(side=side, price=D(price), size=D(size), edge_bps=D(edge))


def _o(oid, price, size, side=Side.SELL):
    return RestingOrder(order_id=oid, side=side, price=D(price), size=D(size))


def test_primitives_importable_from_shared_module():
    assert Side.BUY.value == "BUY"
    assert RungTarget(side=Side.SELL, price=D("1"), size=D("1"), edge_bps=D("0")).side is Side.SELL


def test_unchanged_ladder_yields_zero_cancel_zero_place():
    # JEP-145 acceptance: within the tick threshold -> no churn.
    current = [_o("a", "50.10", "1"), _o("b", "50.20", "1")]
    targets = [_t("50.10", "1"), _t("50.20", "1")]
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff == LadderDiff(to_cancel=[], to_place=[])


def test_one_rung_moved_yields_one_cancel_one_place():
    # JEP-145 acceptance: best target moved beyond threshold -> exactly 1 cancel + 1 place;
    # the unchanged rung stays untouched.
    current = [_o("a", "50.10", "1"), _o("b", "50.20", "1")]
    targets = [_t("50.50", "1"), _t("50.20", "1")]  # first rung moved 40 ticks (> 2-tick thresh)
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff.to_cancel == ["a"]
    assert diff.to_place == [targets[0]]


def test_relocation_preserves_ladder_policy_reexport():
    # Backward compat: ladder_policy must RE-EXPORT (not redefine) the relocated objects, so
    # existing `from ...ladder_policy import diff_ladder_targets` keeps the same identity.
    from hummingbot.strategy_v2.executors.ladder_maker_executor import ladder_policy

    assert ladder_policy.diff_ladder_targets is diff_ladder_targets
    assert ladder_policy.RungTarget is RungTarget
    assert ladder_policy.Side is Side
    assert ladder_policy.RestingOrder is RestingOrder
    assert ladder_policy.LadderDiff is LadderDiff
