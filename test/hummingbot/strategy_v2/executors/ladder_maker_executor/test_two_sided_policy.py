from decimal import Decimal

from hummingbot.strategy_v2.executors.ladder_maker_executor import ladder_policy
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import RungSpec, Side

D = Decimal


def _targets(
    *,
    net_position=D("0"),
    total_size_cap=D("10"),
    open_edge_vwap=D("0"),
    util=D("0"),
    eod_pressure=D("0"),
    cost_bps=D("4"),
    k_open_skew_bps=D("5"),
    k_close_skew_bps=D("5"),
    eod_close_skew_bps=D("0"),
    max_close_cost_bps=D("0"),
    wind_down=False,
):
    assert hasattr(ladder_policy, "build_two_sided_targets")
    return ladder_policy.build_two_sided_targets(
        fair=D("100"),
        rungs=[RungSpec(edge_bps=D("10"), size=D("3")), RungSpec(edge_bps=D("20"), size=D("3"))],
        total_size_cap=total_size_cap,
        net_position=net_position,
        open_edge_vwap=open_edge_vwap,
        util=util,
        eod_pressure=eod_pressure,
        cost_bps=cost_bps,
        k_open_skew_bps=k_open_skew_bps,
        k_close_skew_bps=k_close_skew_bps,
        eod_close_skew_bps=eod_close_skew_bps,
        max_close_cost_bps=max_close_cost_bps,
        tick=D("0.01"),
        buffer_ticks=D("0"),
        wind_down=wind_down,
    )


def test_q_zero_open_only():
    targets = _targets(net_position=D("0"))

    assert targets.close == []
    assert len(targets.open) == 2
    assert all(target.side is Side.SELL for target in targets.open)
    assert sum(target.size for target in targets.open) == D("6")


def test_q_at_cap_close_only():
    targets = _targets(net_position=D("10"), total_size_cap=D("10"))

    assert targets.open == []
    assert len(targets.close) == 2
    assert all(target.side is Side.BUY for target in targets.close)
    assert sum(target.size for target in targets.close) == D("6")


def test_util_skew_moves_open_farther_and_close_nearer_fair():
    low_util = _targets(net_position=D("3"), util=D("0"))
    high_util = _targets(net_position=D("3"), util=D("1"))

    assert high_util.open[0].edge_bps > low_util.open[0].edge_bps
    assert high_util.open[0].price > low_util.open[0].price
    assert high_util.close[0].edge_bps < low_util.close[0].edge_bps
    assert high_util.close[0].price > low_util.close[0].price


def test_close_breakeven_floor_caps_basket_loss():
    targets = _targets(
        net_position=D("3"),
        open_edge_vwap=D("8"),
        util=D("1"),
        eod_pressure=D("1"),
        cost_bps=D("6"),
        k_close_skew_bps=D("50"),
        eod_close_skew_bps=D("50"),
        max_close_cost_bps=D("0"),
    )

    close_edge = targets.close[0].edge_bps
    assert close_edge >= D("-2")
    assert close_edge == D("-2")
    assert D("8") + close_edge - D("6") >= D("0")


def test_util_zero_gross_edges_cover_round_trip_cost():
    targets = _targets(net_position=D("3"), util=D("0"), cost_bps=D("6"))

    for open_target, close_target in zip(targets.open, targets.close):
        rung_net = open_target.edge_bps + close_target.edge_bps - D("6")
        assert rung_net >= D("0")


def test_eod_pressure_ramp():
    assert hasattr(ladder_policy, "compute_eod_pressure")
    assert ladder_policy.compute_eod_pressure(
        now_kst_min=15 * 60 + 20, krx_close_min=15 * 60 + 30, wind_minutes=20
    ) == D("0.5")
    assert ladder_policy.compute_eod_pressure(
        now_kst_min=15 * 60 + 10, krx_close_min=15 * 60 + 30, wind_minutes=10
    ) == D("0")
    assert ladder_policy.compute_eod_pressure(
        now_kst_min=15 * 60 + 20, krx_close_min=15 * 60 + 30, wind_minutes=0
    ) == D("0")
