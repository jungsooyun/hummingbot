from decimal import Decimal

import pytest

from hummingbot.connector.exchange.kis.kis_constants import VALID_MARKET_ROUTINGS
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_cost import (
    HL_HIP3_MAKER_BPS,
    HL_HIP3_TAKER_BPS,
    KIS_BROKERAGE_BPS,
    KRX_USAGE_BPS,
    NXT_USAGE_BPS,
    TAX_SELL_BPS,
    round_trip_cost_bps,
)


def test_constants_match_user_confirmed_values():
    assert TAX_SELL_BPS == Decimal("20")
    assert KIS_BROKERAGE_BPS == Decimal("1.5")
    assert KRX_USAGE_BPS == Decimal("0.36396")
    assert NXT_USAGE_BPS == Decimal("0.31833")
    assert HL_HIP3_MAKER_BPS == Decimal("0.3")
    assert HL_HIP3_TAKER_BPS == Decimal("1")


def test_round_trip_cost_krx_maker_close():
    # entry HL maker 0.3 + KIS buy (1.5 + 0.36396); exit HL maker 0.3 + KIS sell (1.5 + 0.36396 + 20)
    assert round_trip_cost_bps("krx") == Decimal("24.32792")


def test_round_trip_cost_nxt_cheaper_than_krx():
    assert round_trip_cost_bps("nxt") < round_trip_cost_bps("krx")
    assert round_trip_cost_bps("nxt") == Decimal("24.23666")


def test_round_trip_cost_taker_close_more_expensive():
    assert round_trip_cost_bps("krx", close_is_taker=True) > round_trip_cost_bps("krx")


def test_round_trip_cost_case_insensitive():
    assert round_trip_cost_bps("KRX") == round_trip_cost_bps("krx")


def test_accepts_exactly_valid_market_routings():
    # drift guard vs kis_constants.VALID_MARKET_ROUTINGS
    for routing in VALID_MARKET_ROUTINGS:
        assert round_trip_cost_bps(routing) > Decimal("0")
    with pytest.raises(ValueError):
        round_trip_cost_bps("nasdaq")


def test_kis_hl_cost_model_matches_pure_fn():
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_cost import (
        KisHlCostModel,
        round_trip_cost_bps,
    )
    assert KisHlCostModel().round_trip_cost_bps() == round_trip_cost_bps("krx")
    assert KisHlCostModel("nxt").round_trip_cost_bps() == round_trip_cost_bps("nxt")
    assert KisHlCostModel("krx", close_is_taker=True).round_trip_cost_bps() == round_trip_cost_bps("krx", close_is_taker=True)
