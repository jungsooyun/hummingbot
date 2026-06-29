from decimal import Decimal
from unittest.mock import MagicMock
import pytest

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import LadderRungConfig
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import build_ladder_targets, Side, RungSpec
    from hummingbot.core.data_type.common import TradeType
except Exception:  # pragma: no cover - stale .so guard
    LadderMakerExecutor = None


def _fake_all_or_none_budget(available):
    """HL perp budget checker all-or-none approximation: zero the amount if amount*price > available."""
    def _adjust(connector, candidates):
        out = []
        for c in candidates:
            funded = c.amount if (c.amount * c.price) <= available else Decimal("0")
            adj = MagicMock(); adj.amount = funded
            out.append(adj)
        return out
    return _adjust


def _exec_with_rungs(rungs, cap, fair=Decimal("1705"), leverage=1):
    ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
    cfg = MagicMock()
    cfg.rungs = rungs
    cfg.total_size_cap = cap
    cfg.leverage = leverage
    cfg.observe = False
    ex.config = cfg
    ex.maker_trading_pair = "XYZ:SKHX-USD"
    ex.entry_side = TradeType.SELL
    ex._compute_fair = MagicMock(return_value=fair)
    ex._policy_side = MagicMock(return_value=MagicMock())
    return ex


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_maker_balance_candidate_amount_is_enabled_rung_sum():
    rungs = [LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("1")),
             LadderRungConfig(edge_bps=Decimal("10"), size=Decimal("1"))]
    ex = _exec_with_rungs(rungs, cap=Decimal("5"))
    cand = ex._maker_balance_candidate()
    assert cand.amount == Decimal("2")
    assert cand.leverage == Decimal("1")


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_disabled_rungs_excluded_from_gate_size():
    rungs = [LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("1")),
             LadderRungConfig(edge_bps=Decimal("10"), size=Decimal("100"), enabled=False)]
    ex = _exec_with_rungs(rungs, cap=Decimal("101"))
    cand = ex._maker_balance_candidate()
    assert cand.amount == Decimal("1")


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_all_disabled_or_empty_returns_none():
    # JEP-270 (challenge HIGH): no positive enabled rung => placement creates no targets
    # (build_ladder_targets skips disabled rungs), so there is nothing to fund. Return None so
    # the gate is skipped rather than over-demanding the full cap and terminating a no-op executor.
    rungs = [LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("3"), enabled=False)]
    ex = _exec_with_rungs(rungs, cap=Decimal("5"))
    assert ex._maker_balance_candidate() is None


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_gate_caps_at_total_size_cap_when_rung_sum_exceeds_cap():
    # JEP-270 (challenge BLOCKER): build_ladder_targets clips the running open sum to
    # total_size_cap - |position| (ladder_policy.py), so the maximum simultaneously-open maker
    # size is min(rung-sum, cap), NOT raw rung-sum. Gating at raw rung-sum over-demands when
    # rungs sum past the cap and re-triggers the very INSUFFICIENT_BALANCE churn this fixes.
    rungs = [LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("3")),
             LadderRungConfig(edge_bps=Decimal("10"), size=Decimal("3")),
             LadderRungConfig(edge_bps=Decimal("15"), size=Decimal("3"))]  # sum 9 > cap 5
    ex = _exec_with_rungs(rungs, cap=Decimal("5"))
    cand = ex._maker_balance_candidate()
    assert cand.amount == Decimal("5")  # clamped to cap, not 9


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_nonpositive_rung_sizes_excluded_from_gate_size():
    # JEP-270 (challenge HIGH): sum only positive enabled rung sizes so a stray zero/negative
    # size cannot deflate the gate below what placement will actually open.
    rungs = [LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("1")),
             LadderRungConfig(edge_bps=Decimal("10"), size=Decimal("-0.9"))]
    ex = _exec_with_rungs(rungs, cap=Decimal("5"))
    cand = ex._maker_balance_candidate()
    assert cand.amount == Decimal("1")  # negative size excluded, not summed to 0.1


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_no_fair_returns_none():
    ex = _exec_with_rungs([LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("1"))], cap=Decimal("5"))
    ex._compute_fair = MagicMock(return_value=None)
    assert ex._maker_balance_candidate() is None


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_gate_passes_at_rung_sum_where_full_cap_would_fail():
    import asyncio
    avail = Decimal("7899")
    ex = _exec_with_rungs([LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("1")),
                           LadderRungConfig(edge_bps=Decimal("10"), size=Decimal("1"))],
                          cap=Decimal("5"), fair=Decimal("1705"))
    ex.maker_connector = "hyperliquid_perpetual"; ex.stop = MagicMock(); ex.close_type = None
    ex.adjust_order_candidates = _fake_all_or_none_budget(avail)
    asyncio.run(ex.validate_sufficient_balance())
    ex.stop.assert_not_called()


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_gate_would_fail_at_full_cap_under_same_collateral():
    import asyncio
    avail = Decimal("7899")
    ex = _exec_with_rungs([LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("5"))],
                          cap=Decimal("5"), fair=Decimal("1705"))
    ex.maker_connector = "hyperliquid_perpetual"; ex.stop = MagicMock(); ex.close_type = None
    ex.adjust_order_candidates = _fake_all_or_none_budget(avail)
    asyncio.run(ex.validate_sufficient_balance())
    ex.stop.assert_called_once()


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_near_cap_placement_clips_to_remaining_le_rung_sum():
    rungs = [RungSpec(edge_bps=Decimal("5"), size=Decimal("1"), min_edge_bps=Decimal("0")),
             RungSpec(edge_bps=Decimal("15"), size=Decimal("1"), min_edge_bps=Decimal("0"))]
    targets = build_ladder_targets(Decimal("1705"), rungs, Decimal("5"), Side.SELL, Decimal("0.01"),
                                   current_position=Decimal("4"))
    total_open = sum((t.size for t in targets), Decimal("0"))
    assert total_open <= Decimal("1")
    assert total_open <= sum((r.size for r in rungs), Decimal("0"))


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_insufficient_balance_branch_logs_warning():
    # JEP-270 Fix 3a: the base validate_sufficient_balance emits ONE WARNING before stop() when the
    # adjusted candidate amount is ZERO. Exercised via the concrete subclass (base is abstract).
    import asyncio
    from unittest.mock import patch
    ex = _exec_with_rungs([LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("5"))],
                          cap=Decimal("5"), fair=Decimal("1705"))
    ex.maker_connector = "hyperliquid_perpetual"; ex.stop = MagicMock(); ex.close_type = None
    ex.adjust_order_candidates = _fake_all_or_none_budget(Decimal("100"))  # avail too low -> amount 0 -> IB
    with patch.object(LadderMakerExecutor, "logger") as logger_mock:
        asyncio.run(ex.validate_sufficient_balance())
    logger_mock.return_value.warning.assert_called_once()
    ex.stop.assert_called_once()
