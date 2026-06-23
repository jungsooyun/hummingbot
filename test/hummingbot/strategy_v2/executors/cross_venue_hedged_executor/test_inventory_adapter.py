from decimal import Decimal
from types import SimpleNamespace
import pytest
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.inventory_adapter import (
    read_signed_perp_position, PerpHedgeInventory, SpotHedgeInventory,
)

def _pos(pair, side, amt, entry="0"):
    return SimpleNamespace(trading_pair=pair, position_side=SimpleNamespace(name=side),
                           amount=Decimal(amt), entry_price=Decimal(entry))

def test_read_signed_long_is_positive():
    conn = SimpleNamespace(account_positions={"k": _pos("BTC-USDT", "LONG", "0.5", "65000")})
    signed, entry = read_signed_perp_position(conn, "BTC-USDT")
    assert signed == Decimal("0.5") and entry == Decimal("65000")

def test_read_signed_short_is_negative():
    conn = SimpleNamespace(account_positions={"k": _pos("BTC-USDT", "SHORT", "0.5")})
    signed, _ = read_signed_perp_position(conn, "BTC-USDT")
    assert signed == Decimal("-0.5")

def test_read_signed_flat_or_absent_is_zero():
    assert read_signed_perp_position(SimpleNamespace(account_positions={}), "BTC-USDT") == (Decimal("0"), Decimal("0"))
    conn = SimpleNamespace(account_positions={"k": _pos("BTC-USDT", "LONG", "0")})
    assert read_signed_perp_position(conn, "BTC-USDT") == (Decimal("0"), Decimal("0"))
    conn2 = SimpleNamespace(account_positions={"k": _pos("ETH-USDT", "LONG", "1")})
    assert read_signed_perp_position(conn2, "BTC-USDT") == (Decimal("0"), Decimal("0"))

def test_perp_hedge_reads_signed_both_legs():
    conns = {
        "hl": SimpleNamespace(account_positions={"m": _pos("BTC-USD", "LONG", "0.3")}),
        "okx": SimpleNamespace(account_positions={"h": _pos("BTC-USDT", "SHORT", "0.3")}),
    }
    inv = PerpHedgeInventory(conns, "hl", "BTC-USD", "okx", "BTC-USDT")
    assert inv.read_maker_signed()[0] == Decimal("0.3")
    assert inv.read_hedge_signed()[0] == Decimal("-0.3")

def test_spot_hedge_reads_nonnegative_balance():
    conns = {
        "hl": SimpleNamespace(account_positions={"m": _pos("BTC-USD", "LONG", "0.3")}),
        "kis": SimpleNamespace(get_balance=lambda asset: Decimal("4.0")),
    }
    inv = SpotHedgeInventory(conns, "hl", "BTC-USD", "kis", "005930", "005930")
    sig, entry = inv.read_hedge_signed()
    assert sig == Decimal("4.0") and entry == Decimal("0")   # always >= 0
