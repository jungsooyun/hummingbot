from decimal import Decimal

from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import (
    RungSpec,
    Side,
    apply_inventory_skew,
    build_ladder_targets,
    ceil_to_tick,
    compute_fair_price,
    compute_hedge_order,
    floor_to_tick,
    rung_price,
)

D = Decimal


def test_floor_ceil_to_tick():
    assert floor_to_tick(D("49.978"), D("0.01")) == D("49.97")
    assert ceil_to_tick(D("50.071"), D("0.01")) == D("50.08")
    # tick <= 0 is a no-op
    assert floor_to_tick(D("12.34"), D("0")) == D("12.34")


def test_compute_fair_price_side_aware():
    buy = compute_fair_price(D("70000"), D("70100"), D("1380"), D("1382"), Side.BUY)
    assert buy == D("70000") / D("1382")
    sell = compute_fair_price(D("70000"), D("70100"), D("1380"), D("1382"), Side.SELL)
    assert sell == D("70100") / D("1380")
    # conservative spread: buy fair below sell fair
    assert buy < sell


def test_compute_fair_price_mid_fx():
    fair = compute_fair_price(
        D("70000"), D("70100"), D("1380"), D("1382"), Side.BUY, side_aware_fx=False
    )
    assert fair == D("70000") / D("1381")


def test_compute_fair_price_rejects_nonpositive_fx():
    import pytest

    with pytest.raises(ValueError):
        compute_fair_price(D("70000"), D("70100"), D("0"), D("0"), Side.SELL)


def test_apply_inventory_skew_long_lowers_fair():
    fair = D("50")
    skewed = apply_inventory_skew(fair, D("2"), D("0"), D("2"))
    assert skewed == D("50") * (D("1") - D("4") / D("10000"))
    assert skewed < fair


def test_apply_inventory_skew_short_raises_fair():
    skewed = apply_inventory_skew(D("50"), D("-3"), D("0"), D("2"))
    assert skewed > D("50")


def test_rung_price_buy_below_sell_above():
    fair = D("50")
    buy = rung_price(fair, D("15"), Side.BUY, D("0.01"))
    sell = rung_price(fair, D("15"), Side.SELL, D("0.01"))
    assert buy < fair < sell
    assert buy == D("49.92")  # 50*(1-0.0015)=49.925 -> floor 49.92
    assert sell == D("50.08")  # 50*1.0015=50.075 -> ceil 50.08


def test_build_ladder_size_cap_far_rung_gets_more():
    fair = D("50")
    rungs = [
        RungSpec(edge_bps=D("5"), size=D("0.25"), min_edge_bps=D("3")),
        RungSpec(edge_bps=D("15"), size=D("0.75"), min_edge_bps=D("8")),
        RungSpec(edge_bps=D("35"), size=D("1.00"), min_edge_bps=D("20")),
    ]
    targets = build_ladder_targets(fair, rungs, D("1.0"), Side.BUY, D("0.01"))
    assert len(targets) == 2  # third rung receives nothing (cap reached)
    assert targets[0].size == D("0.25")  # near rung: small
    assert targets[1].size == D("0.75")  # far rung: larger
    assert targets[1].price < targets[0].price  # far rung more aggressive (lower BUY)


def test_build_ladder_inventory_gate_buy_withheld():
    rungs = [RungSpec(edge_bps=D("5"), size=D("1"))]
    targets = build_ladder_targets(
        D("50"), rungs, D("1"), Side.BUY, D("0.01"), inventory=D("8"), max_inventory=D("8")
    )
    assert targets == []


def test_build_ladder_inventory_gate_sell_allowed_when_long():
    rungs = [RungSpec(edge_bps=D("5"), size=D("1"))]
    targets = build_ladder_targets(
        D("50"), rungs, D("1"), Side.SELL, D("0.01"), inventory=D("8"), max_inventory=D("8")
    )
    assert len(targets) == 1


def test_min_edge_floor_applied():
    fair = D("50")
    rungs = [RungSpec(edge_bps=D("1"), size=D("1"), min_edge_bps=D("10"))]
    targets = build_ladder_targets(fair, rungs, D("1"), Side.BUY, D("0.01"))
    # clamped to fair*(1-0.001)=49.95, not fair*(1-0.0001)=49.995
    assert targets[0].price <= D("49.95")


def test_compute_hedge_order_buy_marketable():
    hedge = compute_hedge_order(D("0.5"), D("1.0"), D("70100"), D("30"), D("1"))
    assert hedge.side == Side.BUY
    assert hedge.size == D("0.5")
    assert hedge.price == D("70311")  # 70100*1.003=70310.3 -> ceil tick(1) = 70311


# --- Task 2: net-edge placement (gross = net + cost_bps) ---


def test_build_ladder_cost_bps_widens_sell_edge():
    fair = D("1000")
    rungs = [RungSpec(edge_bps=D("20"), size=D("1"))]
    # gross = net(20) + cost(24) = 44 bps -> 1000 * 1.0044 = 1004.4, ceil tick 0.1
    targets = build_ladder_targets(fair, rungs, D("100"), Side.SELL, D("0.1"), cost_bps=D("24"))
    assert targets[0].price == D("1004.4")
    assert targets[0].edge_bps == D("20")  # reported edge stays the NET target


def test_build_ladder_cost_bps_widens_buy_edge_downward():
    fair = D("1000")
    rungs = [RungSpec(edge_bps=D("20"), size=D("1"))]
    # gross 44 bps below -> 1000 * 0.9956 = 995.6, floor tick 0.1
    targets = build_ladder_targets(fair, rungs, D("100"), Side.BUY, D("0.1"), cost_bps=D("24"))
    assert targets[0].price == D("995.6")


def test_build_ladder_cost_bps_default_zero_unchanged():
    fair = D("1000")
    rungs = [RungSpec(edge_bps=D("20"), size=D("1"))]
    a = build_ladder_targets(fair, rungs, D("100"), Side.SELL, D("0.1"))
    b = build_ladder_targets(fair, rungs, D("100"), Side.SELL, D("0.1"), cost_bps=D("0"))
    assert a[0].price == b[0].price == D("1002")  # 20 bps only


def test_build_ladder_cost_applies_to_min_edge_floor():
    fair = D("1000")
    rungs = [RungSpec(edge_bps=D("20"), size=D("1"), min_edge_bps=D("10"))]
    targets = build_ladder_targets(fair, rungs, D("100"), Side.SELL, D("0.1"), cost_bps=D("24"))
    assert targets[0].price == D("1004.4")


# --- Task 3: total_size_cap = max accumulated position (current_position budget) ---


def test_build_ladder_position_budget_trims_remaining_room():
    fair = D("1000")
    rungs = [RungSpec(edge_bps=D("20"), size=D("4")), RungSpec(edge_bps=D("100"), size=D("4"))]
    # cap 7, already hold 5 -> remaining 2 -> only 2 shares quoted total
    targets = build_ladder_targets(fair, rungs, D("7"), Side.SELL, D("0.1"), current_position=D("5"))
    assert sum(t.size for t in targets) == D("2")


def test_build_ladder_position_budget_halts_at_cap():
    fair = D("1000")
    rungs = [RungSpec(edge_bps=D("20"), size=D("1"))]
    assert build_ladder_targets(fair, rungs, D("7"), Side.SELL, D("0.1"), current_position=D("7")) == []


def test_build_ladder_position_budget_uses_abs_position():
    # short side stored negative; budget consumes magnitude
    fair = D("1000")
    rungs = [RungSpec(edge_bps=D("20"), size=D("1"))]
    assert build_ladder_targets(fair, rungs, D("7"), Side.SELL, D("0.1"), current_position=D("-7")) == []
