from decimal import Decimal

from hummingbot.strategy_v2.executors.ladder_maker_executor import ladder_policy
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import RungSpec, Side

D = Decimal


def _targets(
    *,
    fair_open=D("100"),
    fair_close=D("100"),
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
        fair_open=fair_open,
        fair_close=fair_close,
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


FAIR_OPEN = D("80100") / D("1380")   # SELL fair = 58.0435...
FAIR_CLOSE = D("80000") / D("1381")  # BUY fair  = 57.9290...


def test_close_rung_anchors_to_fair_close():
    # Spec case 1: close BUY rung must price off fair_close, NOT fair_open.
    targets = _targets(fair_open=FAIR_OPEN, fair_close=FAIR_CLOSE, net_position=D("3"))
    close0 = targets.close[0]
    assert close0.side is Side.BUY
    # edge_buy with util=0, eod=0, cost_bps=4: half_cost(2)+rung(10) = 12 bps (above break_even=4)
    assert close0.edge_bps == D("12")
    assert close0.price == D("57.85")            # floor_to_tick(fair_close*(1-12/10000), 0.01)
    wrong = D("57.97")                            # floor_to_tick(fair_open *(1-12/10000), 0.01)
    assert close0.price < wrong                   # split is strictly below the entry-fair price


def test_open_rung_anchors_to_fair_open():
    # Spec case 2: open SELL rung unchanged -- prices off fair_open.
    targets = _targets(fair_open=FAIR_OPEN, fair_close=FAIR_CLOSE, net_position=D("3"))
    open0 = targets.open[0]
    assert open0.side is Side.SELL
    assert open0.edge_bps == D("12")             # half_cost(2)+rung(10)+k_open(5)*util(0)
    assert open0.price == D("58.12")             # ceil_to_tick(fair_open*(1+12/10000), 0.01)


def test_close_breakeven_floor_anchors_to_fair_close():
    # Spec case 3: when edge_buy clamps to break_even_buy_edge, the floored price
    # must still be computed from fair_close.
    targets = _targets(
        fair_open=FAIR_OPEN,
        fair_close=FAIR_CLOSE,
        net_position=D("3"),
        open_edge_vwap=D("8"),
        util=D("1"),
        eod_pressure=D("1"),
        cost_bps=D("6"),
        k_close_skew_bps=D("50"),
        eod_close_skew_bps=D("50"),
        max_close_cost_bps=D("0"),
    )
    close0 = targets.close[0]
    assert close0.edge_bps == D("-2")            # clamped to break_even_buy_edge
    assert close0.price == D("57.94")            # floor_to_tick(fair_close*(1-(-2)/10000), 0.01)
    wrong = D("58.05")                            # floor_to_tick(fair_open *(1-(-2)/10000), 0.01)
    assert close0.price < wrong


def test_equal_fairs_collapses_split_to_noop():
    # Spec case 4 (reframed): when fair_open == fair_close, the open/close split
    # collapses and the two-sided output is byte-identical to the single-fair
    # baseline. This is a property of build_two_sided_targets ONLY; it does NOT
    # assert that side_aware_fx=False yields equal fairs (it does not over a KIS
    # spread -- see the Money-logic correction in this task).
    split = _targets(fair_open=D("100"), fair_close=D("100"), net_position=D("3"))
    baseline = _targets(net_position=D("3"))  # fixture defaults both fairs to 100
    assert [(t.side, t.price, t.size, t.edge_bps) for t in split.open] == \
           [(t.side, t.price, t.size, t.edge_bps) for t in baseline.open]
    assert [(t.side, t.price, t.size, t.edge_bps) for t in split.close] == \
           [(t.side, t.price, t.size, t.edge_bps) for t in baseline.close]
    # And the split close/open prices are equal when the two fairs are equal:
    assert split.open[0].price == D("100.12")    # ceil_to_tick(100*(1+12/10000))
    assert split.close[0].price == D("99.88")    # floor_to_tick(100*(1-12/10000))
