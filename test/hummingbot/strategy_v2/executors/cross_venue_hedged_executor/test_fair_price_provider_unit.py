"""JEP-187 FairPriceProvider units: JEP-185 bridge both directions + _get_fx fallbacks + neutral no-op."""
import logging
from decimal import Decimal
from unittest.mock import MagicMock, patch

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.fair_price_provider import DirectFairSource
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.fx_bridged_fair_source import FxBridgedFairSource
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import Side

LOG = logging.getLogger("test")


def _src(side_aware=True, static=None):
    return FxBridgedFairSource(side_aware_fx=side_aware, static_fx_rate=static, logger=LOG)


# ---- forward: fair_from_book (side-aware) ----
def test_fair_from_book_buy_uses_fx_ask():
    s = _src()
    s._get_fx = MagicMock(return_value=(Decimal("1250"), Decimal("1280")))
    assert s.fair_from_book(Decimal("1920000"), Decimal("2000000"), Side.BUY) == Decimal("1500")  # kis_bid/fx_ask


def test_fair_from_book_sell_uses_fx_bid():
    s = _src()
    s._get_fx = MagicMock(return_value=(Decimal("1250"), Decimal("1280")))
    assert s.fair_from_book(Decimal("1920000"), Decimal("2000000"), Side.SELL) == Decimal("1600")  # kis_ask/fx_bid


def test_fair_from_book_mid_when_not_side_aware():
    s = _src(side_aware=False)
    s._get_fx = MagicMock(return_value=(Decimal("1250"), Decimal("1270")))
    mid = (Decimal("1250") + Decimal("1270")) / Decimal("2")  # 1260
    assert s.fair_from_book(Decimal("2520000"), Decimal("2520000"), Side.BUY) == Decimal("2520000") / mid


def test_fair_from_book_none_when_fx_missing():
    s = _src()
    s._get_fx = MagicMock(return_value=(None, None))
    assert s.fair_from_book(Decimal("1920000"), Decimal("2000000"), Side.SELL) is None


# ---- backward: hedge_price_to_maker_quote (side-aware) ----
def test_hedge_quote_buy_divides_by_fx_bid():
    s = _src()
    s._get_fx = MagicMock(return_value=(Decimal("1250"), Decimal("1280")))
    assert s.hedge_price_to_maker_quote(Decimal("2000000"), TradeType.BUY) == Decimal("2000000") / Decimal("1250")


def test_hedge_quote_sell_divides_by_fx_ask():
    s = _src()
    s._get_fx = MagicMock(return_value=(Decimal("1250"), Decimal("1280")))
    assert s.hedge_price_to_maker_quote(Decimal("2000000"), TradeType.SELL) == Decimal("2000000") / Decimal("1280")


def test_hedge_quote_mid_when_not_side_aware():
    s = _src(side_aware=False)
    s._get_fx = MagicMock(return_value=(Decimal("1250"), Decimal("1270")))
    mid = (Decimal("1250") + Decimal("1270")) / Decimal("2")
    assert s.hedge_price_to_maker_quote(Decimal("2520000"), TradeType.BUY) == Decimal("2520000") / mid


def test_hedge_quote_zero_and_error_on_missing_fx():
    s = _src()
    s._get_fx = MagicMock(return_value=(None, None))
    s._logger = MagicMock()
    assert s.hedge_price_to_maker_quote(Decimal("2000000"), TradeType.BUY) == Decimal("0")
    s._logger.error.assert_called_once()


def test_hedge_quote_zero_on_nonpositive_fx():
    s = _src()
    s._get_fx = MagicMock(return_value=(Decimal("0"), Decimal("1280")))
    s._logger = MagicMock()
    assert s.hedge_price_to_maker_quote(Decimal("2000000"), TradeType.SELL) == Decimal("0")
    s._logger.error.assert_called_once()


# ---- _get_fx fallback paths (golden can't cover these) ----
def test_get_fx_uses_live_source_when_present():
    s = _src(static=Decimal("1300"))
    with patch(
        "hummingbot.data_feed.fair_fx.fair_fx_source.FairFxSource.get_instance"
    ) as gi:
        gi.return_value.get_fx.return_value = (Decimal("1251"), Decimal("1252"))
        assert s._get_fx() == (Decimal("1251"), Decimal("1252"))


def test_get_fx_falls_back_to_static_when_live_none():
    s = _src(static=Decimal("1300"))
    with patch(
        "hummingbot.data_feed.fair_fx.fair_fx_source.FairFxSource.get_instance"
    ) as gi:
        gi.return_value.get_fx.return_value = None
        assert s._get_fx() == (Decimal("1300"), Decimal("1300"))


def test_get_fx_none_when_live_none_and_no_static():
    s = _src(static=None)
    with patch(
        "hummingbot.data_feed.fair_fx.fair_fx_source.FairFxSource.get_instance"
    ) as gi:
        gi.return_value.get_fx.return_value = None
        assert s._get_fx() == (None, None)


def test_get_fx_ignores_nonpositive_static():
    s = _src(static=Decimal("0"))
    with patch(
        "hummingbot.data_feed.fair_fx.fair_fx_source.FairFxSource.get_instance"
    ) as gi:
        gi.return_value.get_fx.return_value = None
        assert s._get_fx() == (None, None)


# ---- DirectFairSource: verified no-op bridge (== base default) ----
def test_direct_fair_from_book_is_side_best():
    d = DirectFairSource()
    assert d.fair_from_book(Decimal("100"), Decimal("101"), Side.BUY) == Decimal("100")
    assert d.fair_from_book(Decimal("100"), Decimal("101"), Side.SELL) == Decimal("101")


def test_direct_hedge_quote_is_identity():
    assert DirectFairSource().hedge_price_to_maker_quote(Decimal("123.45"), TradeType.BUY) == Decimal("123.45")


def test_direct_observe_fx_legs_is_none():
    assert DirectFairSource().observe_fx_legs() == (None, None)
