"""Unit tests for the partial-reprice diff (JEP-145).

Pure stdlib + pytest — no hummingbot runtime / Cython. Run with:
    cd hummingbot && PYENV_VERSION=py312 python -m pytest \
        test/hummingbot/strategy_v2/executors/ladder_maker_executor/test_ladder_diff.py -v
"""
from decimal import Decimal

from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import (
    LadderDiff,
    RestingOrder,
    RungTarget,
    Side,
    diff_ladder_targets,
)

D = Decimal
TICK = D("0.01")
THRESH = D("2")  # 2 ticks


def _t(price, size, edge="10"):
    return RungTarget(side=Side.SELL, price=D(price), size=D(size), edge_bps=D(edge))


def _o(oid, price, size, side=Side.SELL):
    return RestingOrder(order_id=oid, side=side, price=D(price), size=D(size))


def test_empty_current_places_all():
    targets = [_t("50.10", "1"), _t("50.20", "1")]
    diff = diff_ladder_targets([], targets, TICK, THRESH)
    assert diff.to_cancel == []
    assert diff.to_place == targets


def test_empty_targets_cancels_all():
    current = [_o("a", "50.10", "1"), _o("b", "50.20", "1")]
    diff = diff_ladder_targets(current, [], TICK, THRESH)
    assert sorted(diff.to_cancel) == ["a", "b"]
    assert diff.to_place == []


def test_exact_match_no_churn():
    current = [_o("a", "50.10", "1"), _o("b", "50.20", "1")]
    targets = [_t("50.10", "1"), _t("50.20", "1")]
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff == LadderDiff(to_cancel=[], to_place=[])


def test_price_drift_within_threshold_keeps():
    # 1 tick drift (< 2-tick threshold), same size -> untouched.
    current = [_o("a", "50.11", "1")]
    targets = [_t("50.10", "1")]
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff.to_cancel == []
    assert diff.to_place == []


def test_price_move_at_threshold_reprices():
    # exactly 2 ticks -> at threshold counts as a reprice (>= triggers).
    current = [_o("a", "50.12", "1")]
    targets = [_t("50.10", "1")]
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff.to_cancel == ["a"]
    assert diff.to_place == targets


def test_size_change_reprices_even_if_price_same():
    current = [_o("a", "50.10", "1")]
    targets = [_t("50.10", "2")]
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff.to_cancel == ["a"]
    assert diff.to_place == targets


def test_one_rung_moves_others_stay():
    current = [_o("a", "50.10", "1"), _o("b", "50.20", "1"), _o("c", "50.30", "1")]
    # middle rung moves 5 ticks; outer two unchanged.
    targets = [_t("50.10", "1"), _t("50.25", "1"), _t("50.30", "1")]
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff.to_cancel == ["b"]
    assert diff.to_place == [_t("50.25", "1")]


def test_extra_resting_order_is_cancelled():
    current = [_o("a", "50.10", "1"), _o("b", "50.20", "1")]
    targets = [_t("50.10", "1")]  # ladder shrank to one rung
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff.to_cancel == ["b"]
    assert diff.to_place == []


def test_new_rung_is_placed():
    current = [_o("a", "50.10", "1")]
    targets = [_t("50.10", "1"), _t("50.20", "1")]  # ladder grew
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff.to_cancel == []
    assert diff.to_place == [_t("50.20", "1")]


def test_match_is_deterministic_closest_then_id():
    # two equal-size orders could match one target; the closer price wins, and
    # the farther one is cancelled.
    current = [_o("a", "50.13", "1"), _o("b", "50.11", "1")]
    targets = [_t("50.10", "1")]
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    # b (drift 1 tick) matches; a (drift 3 ticks) is out of threshold anyway.
    assert diff.to_cancel == ["a"]
    assert diff.to_place == []


def test_tie_break_prefers_lowest_order_id():
    # both within threshold at equal distance -> lowest id matches, other cancels.
    current = [_o("z", "50.11", "1"), _o("a", "50.09", "1")]
    targets = [_t("50.10", "1")]
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff.to_cancel == ["z"]
    assert diff.to_place == []


def test_no_target_double_matches_same_order():
    # one live order, two same-size targets within threshold of it: it can match
    # only one; the other target must be placed.
    current = [_o("a", "50.10", "1")]
    targets = [_t("50.10", "1"), _t("50.11", "1")]
    diff = diff_ladder_targets(current, targets, TICK, THRESH)
    assert diff.to_cancel == []
    assert diff.to_place == [_t("50.11", "1")]


def test_opposite_side_same_price_size_not_matched():
    target = RungTarget(side=Side.BUY, price=D("50.10"), size=D("1"), edge_bps=D("10"))
    current = [_o("sell-open", "50.10", "1", side=Side.SELL)]
    diff = diff_ladder_targets(current, [target], TICK, THRESH)
    assert diff.to_cancel == ["sell-open"]
    assert diff.to_place == [target]
