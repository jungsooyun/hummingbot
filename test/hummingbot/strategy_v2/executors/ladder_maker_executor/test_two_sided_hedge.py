import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import OrderType, PriceType, TradeType

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import (
        LadderMakerExecutor,
    )
    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    _EXECUTOR_IMPORTABLE = False


class _Tracked:
    def __init__(self, order_id):
        self.order_id = order_id
        self.order = None

    @property
    def cum_fees_quote(self):
        return Decimal("0")


class _Ev:
    def __init__(self, order_id, amount, price="1", trade_type=None):
        self.order_id = order_id
        self.amount = Decimal(str(amount))
        self.price = Decimal(str(price))
        self.trade_type = trade_type


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack")
class LadderMakerTwoSidedHedgeTest(unittest.TestCase):
    def _make_executor(
        self,
        *,
        share_per_unit: Decimal = Decimal("1"),
        pending_signed: Decimal = Decimal("-1"),
    ) -> "LadderMakerExecutor":
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex.config = SimpleNamespace(
            share_per_unit=share_per_unit,
            side_aware_fx=True,
            static_fx_rate=None,
            hedge_max_slippage_bps=Decimal("30"),
            hedge_tick=Decimal("1"),
            hedge_order_type=OrderType.LIMIT,
        )
        ex.maker_connector = "hyperliquid_perpetual"
        ex.maker_trading_pair = "XYZ:SKHX-USD"
        ex.hedge_connector = "kis"
        ex.hedge_trading_pair = "000660-KRW"
        ex.entry_side = TradeType.SELL
        ex.hedge_side = TradeType.BUY
        ex.maker_orders = {}
        ex.hedge_orders = {}
        ex._maker_executed_base = Decimal("0")
        ex._maker_executed_quote = Decimal("0")
        ex._hedge_executed_base = Decimal("0")
        ex._hedge_executed_quote = Decimal("0")
        ex._maker_fees_quote = Decimal("0")
        ex._hedge_fees_quote = Decimal("0")
        ex._perp_cash = Decimal("0")
        ex._spot_cash = Decimal("0")
        ex._maker_buy_base = Decimal("0")
        ex._maker_sell_base = Decimal("0")
        ex._hedge_buy_base = Decimal("0")
        ex._hedge_sell_base = Decimal("0")
        ex._pending_hedge_signed = pending_signed
        ex._current_retries = 0
        ex._hedge_order_side = {}
        ex._maker_placed_edge_bps = {}
        ex._open_edge_base = Decimal("0")
        ex._open_edge_notional_bps = Decimal("0")
        ex._open_edge_vwap = Decimal("0")
        ex._update_tracked = MagicMock()
        from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.fx_bridged_fair_source import FxBridgedFairSource

        ex._fair = FxBridgedFairSource(
            getattr(ex.config, "side_aware_fx", True),
            getattr(ex.config, "static_fx_rate", None),
            LadderMakerExecutor.logger(),
        )

        kis = MagicMock()
        kis.quantize_order_amount.side_effect = lambda pair, amount: amount
        kis.quantize_order_price.side_effect = lambda pair, price: price
        kis.get_price_by_type.side_effect = lambda pair, price_type: {
            PriceType.BestBid: Decimal("70000"),
            PriceType.BestAsk: Decimal("70100"),
        }[price_type]
        ex.connectors = {"kis": kis}
        return ex

    def test_size_hedge_buy_uses_best_ask_unchanged(self):
        ex = self._make_executor(pending_signed=Decimal("-1"))

        spec = ex._size_hedge(Decimal("1"))

        self.assertEqual(Decimal("1"), spec["amount"])
        self.assertEqual(Decimal("70311"), spec["price"])
        ex.connectors["kis"].get_price_by_type.assert_called_once_with(
            "000660-KRW", PriceType.BestAsk
        )

    def test_size_hedge_sell_uses_best_bid(self):
        ex = self._make_executor(pending_signed=Decimal("1"))

        spec = ex._size_hedge(Decimal("1"))

        self.assertEqual(Decimal("1"), spec["amount"])
        self.assertEqual(Decimal("69790"), spec["price"])
        self.assertLess(spec["price"], Decimal("70000"))
        ex.connectors["kis"].get_price_by_type.assert_called_once_with(
            "000660-KRW", PriceType.BestBid
        )

    def test_hedge_base_to_maker_base_divides_by_share_per_unit(self):
        ex = self._make_executor(share_per_unit=Decimal("2"))
        self.assertEqual(Decimal("2"), ex._hedge_base_to_maker_base(Decimal("4")))

        ex.config.share_per_unit = Decimal("1")
        self.assertEqual(Decimal("4"), ex._hedge_base_to_maker_base(Decimal("4")))

    def test_share_per_unit_roundtrip_closes_to_neutral(self):
        ex = self._make_executor(share_per_unit=Decimal("2"), pending_signed=Decimal("0"))
        ex.maker_orders["m0"] = _Tracked("m0")
        ex.process_order_filled_event(
            None,
            None,
            _Ev("m0", "1", price="100", trade_type=TradeType.SELL),
        )
        self.assertEqual(Decimal("-1"), ex._pending_hedge_signed)

        spec = ex._size_hedge(Decimal("1"))
        self.assertEqual(Decimal("2"), spec["amount"])

        ex._hedge_order_side["h0"] = (TradeType.BUY, Decimal("2"))
        ex._credit_hedge_fill("h0", Decimal("2"), Decimal("70100"))

        self.assertEqual(Decimal("0"), ex._pending_hedge_signed)
        self.assertEqual(Decimal("0"), ex._unhedged_base_signed())


if __name__ == "__main__":
    unittest.main()
