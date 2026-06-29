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
def test_all_disabled_or_empty_falls_back_to_cap():
    rungs = [LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("3"), enabled=False)]
    ex = _exec_with_rungs(rungs, cap=Decimal("5"))
    cand = ex._maker_balance_candidate()
    assert cand.amount == Decimal("5")


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
    asyncio.get_event_loop().run_until_complete(ex.validate_sufficient_balance())
    ex.stop.assert_not_called()


@pytest.mark.skipif(LadderMakerExecutor is None, reason="stale .so")
def test_gate_would_fail_at_full_cap_under_same_collateral():
    import asyncio
    avail = Decimal("7899")
    ex = _exec_with_rungs([LadderRungConfig(edge_bps=Decimal("5"), size=Decimal("5"))],
                          cap=Decimal("5"), fair=Decimal("1705"))
    ex.maker_connector = "hyperliquid_perpetual"; ex.stop = MagicMock(); ex.close_type = None
    ex.adjust_order_candidates = _fake_all_or_none_budget(avail)
    asyncio.get_event_loop().run_until_complete(ex.validate_sufficient_balance())
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
        asyncio.get_event_loop().run_until_complete(ex.validate_sufficient_balance())
    logger_mock.return_value.warning.assert_called_once()
    ex.stop.assert_called_once()
