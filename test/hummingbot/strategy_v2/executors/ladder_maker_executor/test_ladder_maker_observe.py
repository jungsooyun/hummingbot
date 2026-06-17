import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import TradeType

try:
    # The executor module pulls the V2 strategy base (paho), absent in the local
    # py312 env -> these tests run in Docker/CI where the full stack is available.
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    _EXECUTOR_IMPORTABLE = False


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack (paho) — run in Docker/CI")
class LadderMakerObserveTest(unittest.TestCase):
    """The observe (no-submit) gate: _place_maker must compute the (quantized) quote
    and LOG it but never call place_order, so a live verification run places zero
    real orders (no fills -> no KIS hedge)."""

    def _make_executor(self, observe: bool) -> LadderMakerExecutor:
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex.config = MagicMock(observe=observe)
        ex.maker_connector = "hyperliquid_perpetual"
        ex.maker_trading_pair = "XYZ:SKHX-USD"
        ex.entry_side = TradeType.SELL
        conn = MagicMock()
        conn.quantize_order_amount.side_effect = lambda pair, amt: amt
        conn.quantize_order_price.side_effect = lambda pair, price: price
        ex.connectors = {"hyperliquid_perpetual": conn}
        ex.place_order = MagicMock(return_value="OID-1")
        ex.maker_orders = {}
        ex.logger = MagicMock(return_value=MagicMock())
        return ex

    def test_observe_true_skips_place_order(self):
        ex = self._make_executor(observe=True)
        ex._place_maker(Decimal("100000"), Decimal("0.01"), Decimal("100"))
        ex.place_order.assert_not_called()
        self.assertEqual({}, ex.maker_orders)
        ex.logger().info.assert_called_once()

    def test_observe_false_places_and_tracks(self):
        ex = self._make_executor(observe=False)
        ex._place_maker(Decimal("100000"), Decimal("0.01"), Decimal("100"))
        ex.place_order.assert_called_once()
        self.assertIn("OID-1", ex.maker_orders)


if __name__ == "__main__":
    unittest.main()
