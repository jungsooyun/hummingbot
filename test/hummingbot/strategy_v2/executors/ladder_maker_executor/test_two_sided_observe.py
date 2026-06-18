import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import PositionAction, TradeType
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import Side

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import (
        LadderMakerExecutor,
    )
    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    _EXECUTOR_IMPORTABLE = False


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack")
class LadderMakerTwoSidedObserveTest(unittest.TestCase):
    def _make_executor(
        self,
        *,
        two_sided: bool,
        observe: bool,
        paired_oi: Decimal = Decimal("0.01"),
    ) -> "LadderMakerExecutor":
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex.config = SimpleNamespace(
            observe=observe,
            two_sided=two_sided,
            total_size_cap=Decimal("0.02"),
            rungs=[
                SimpleNamespace(
                    edge_bps=Decimal("100"),
                    size=Decimal("0.01"),
                    min_edge_bps=Decimal("0"),
                    enabled=True,
                )
            ],
            maker_tick=Decimal("0.1"),
            buffer_ticks=Decimal("0"),
            round_trip_cost_bps=Decimal("10"),
            k_open_skew_bps=Decimal("0"),
            k_close_skew_bps=Decimal("0"),
            eod_close_skew_bps=Decimal("0"),
            max_close_cost_bps=Decimal("0"),
            wind_down=False,
            max_inventory=None,
        )
        ex.maker_connector = "hyperliquid_perpetual"
        ex.maker_trading_pair = "XYZ:SKHX-USD"
        ex.hedge_connector = "kis"
        ex.hedge_trading_pair = "000660-KRW"
        ex.entry_side = TradeType.SELL
        conn = MagicMock()
        conn.quantize_order_amount.side_effect = lambda pair, amount: amount
        conn.quantize_order_price.side_effect = lambda pair, price: price
        conn.get_price_by_type.side_effect = lambda pair, price_type: Decimal("100")
        ex.connectors = {"hyperliquid_perpetual": conn, "kis": conn}
        ex.place_order = MagicMock(side_effect=["OID-OPEN", "OID-CLOSE"])
        ex.maker_orders = {}
        ex._maker_placed_edge_bps = {}
        ex.logger = MagicMock(return_value=MagicMock())
        ex._last_observe = None
        ex._last_observe_log_ts = 0.0
        ex._last_reprice_ts = 0.0
        ex._strategy = SimpleNamespace(current_timestamp=1000.0)
        ex._compute_fair = MagicMock(return_value=Decimal("100"))
        ex._policy_side = MagicMock(return_value=Side.SELL)
        ex._get_fx = MagicMock(return_value=(Decimal("1300"), Decimal("1301")))
        ex._paired_oi = MagicMock(return_value=paired_oi)
        ex._unhedged_base_signed = MagicMock(return_value=Decimal("0.002"))
        ex._open_edge_vwap = Decimal("0")
        ex._pending_hedge_signed = Decimal("-0.003")
        ex._maker_executed_base = Decimal("0")
        return ex

    def test_two_sided_observe_emits_open_and_close_zero_submit(self):
        ex = self._make_executor(two_sided=True, observe=True)

        ex._place_targets(ex._compute_targets())

        obs = ex._last_observe
        self.assertTrue(obs["two_sided"])
        self.assertEqual("SELL", obs["open"][0]["side"])
        self.assertEqual("OPEN", obs["open"][0]["position_action"])
        self.assertEqual("BUY", obs["close"][0]["side"])
        self.assertEqual("CLOSE", obs["close"][0]["position_action"])
        self.assertLessEqual({"Q", "U", "util", "eod", "pending_signed"}, set(obs))
        ex.place_order.assert_not_called()

    def test_two_sided_false_path_unchanged(self):
        ex = self._make_executor(two_sided=False, observe=True)

        targets = ex._compute_targets()
        ex._place_targets(targets)

        self.assertEqual(1, len(targets))
        self.assertIn("rungs", ex._last_observe)
        self.assertNotIn("open", ex._last_observe)
        self.assertNotIn("close", ex._last_observe)
        ex.place_order.assert_not_called()

    def test_two_sided_live_places_open_and_close_with_position_action(self):
        ex = self._make_executor(two_sided=True, observe=False)

        ex._place_targets(ex._compute_targets())

        calls = ex.place_order.call_args_list
        self.assertEqual(PositionAction.OPEN, calls[0].kwargs["position_action"])
        self.assertEqual(TradeType.SELL, calls[0].kwargs["side"])
        self.assertEqual(PositionAction.CLOSE, calls[1].kwargs["position_action"])
        self.assertEqual(TradeType.BUY, calls[1].kwargs["side"])
        self.assertEqual({"OID-OPEN": Decimal("105")}, ex._maker_placed_edge_bps)

    def test_residual_mark_uses_live_fair(self):
        ex = self._make_executor(two_sided=True, observe=False)
        ex._compute_fair = MagicMock(return_value=Decimal("101.25"))

        self.assertEqual(Decimal("101.25"), ex._residual_mark_price())


if __name__ == "__main__":
    unittest.main()
