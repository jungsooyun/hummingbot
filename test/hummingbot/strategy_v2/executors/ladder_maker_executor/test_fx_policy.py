from decimal import Decimal

import pytest

from hummingbot.strategy_v2.executors.ladder_maker_executor.fx_policy import (
    W_BANK,
    W_USDT,
    blend_fx_quote,
)

D = Decimal


def test_blend_weight_is_9_bank_to_1_usdt():
    # Toss bank : Upbit USDT-KRW = 9 : 1 (bank-heavy — the bank rate is the truer
    # USD/KRW; the USDT-KRW leg carries a crypto/kimchi premium, so it gets only 10%).
    assert (W_BANK, W_USDT) == (D("9"), D("1"))
    # numeric: bank 1000, usdt 1010 (in band) -> (1000*9 + 1010)/10 = 1001
    bid, ask = blend_fx_quote(D("1000"), D("1010"), D("1010"))
    assert bid == ask == D("1001")


def test_clamp_then_blend_not_a_noop():
    # usdt far above the +3% cap -> clamped to bank*1.03 BEFORE blending
    bank, usdt = D("1400"), D("1600")
    bid, ask = blend_fx_quote(bank, usdt, usdt)
    capped = bank * D("1.03")
    expected = (bank * 9 + capped) / 10
    assert bid == ask == expected


def test_low_tail_clamped():
    bank, usdt = D("1400"), D("1000")          # below -2%
    bid, _ = blend_fx_quote(bank, usdt, usdt)
    assert bid == (bank * 9 + bank * D("0.98")) / 10


def test_in_band_blends_raw():
    bank, usdt = D("1400"), D("1410")          # within band
    bid, _ = blend_fx_quote(bank, usdt, usdt)
    assert bid == (bank * 9 + usdt) / 10


def test_per_side_bid_ask():
    bank = D("1400")
    bid, ask = blend_fx_quote(bank, usdt_bid=D("1405"), usdt_ask=D("1415"))
    assert bid == (bank * 9 + D("1405")) / 10
    assert ask == (bank * 9 + D("1415")) / 10
    assert bid < ask


def test_usdt_missing_degrades_to_bank_only():
    bank = D("1400")
    assert blend_fx_quote(bank, None, None) == (bank, bank)


def test_bank_missing_or_nonpositive_raises():
    with pytest.raises(ValueError):
        blend_fx_quote(None, D("1400"), D("1400"))
    with pytest.raises(ValueError):
        blend_fx_quote(D("0"), D("1400"), D("1400"))
