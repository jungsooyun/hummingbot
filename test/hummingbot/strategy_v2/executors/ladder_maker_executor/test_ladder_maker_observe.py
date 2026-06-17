import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import TradeType

try:
    # The executor module pulls the V2 strategy base (paho), absent in the local
    # py312 env -> these tests run in Docker/CI where the full stack is available.
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import (
        LadderMakerExecutor,
        _fmt_num,
    )
    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    _fmt_num = None
    _EXECUTOR_IMPORTABLE = False


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack (paho) — run in Docker/CI")
class LadderMakerObserveTest(unittest.TestCase):
    """The observe (no-submit) gate: in observe mode the executor computes the full
    intended ladder and surfaces it (one throttled summary line + custom_info) but
    never calls place_order, so a live verification run places zero real orders
    (no fills -> no KIS hedge)."""

    def _make_executor(self, observe: bool) -> "LadderMakerExecutor":
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex.config = MagicMock(observe=observe)
        ex.maker_connector = "hyperliquid_perpetual"
        ex.maker_trading_pair = "XYZ:SKHX-USD"
        ex.hedge_connector = "kis"
        ex.hedge_trading_pair = "000660-KRW"
        ex.entry_side = TradeType.SELL
        conn = MagicMock()
        conn.quantize_order_amount.side_effect = lambda pair, amt: amt
        conn.quantize_order_price.side_effect = lambda pair, price: price
        ex.connectors = {"hyperliquid_perpetual": conn, "kis": conn}
        ex.place_order = MagicMock(return_value="OID-1")
        ex.maker_orders = {}
        ex.logger = MagicMock(return_value=MagicMock())
        # observe summary plumbing
        ex._last_observe = None
        ex._last_observe_log_ts = 0.0
        ex._strategy = SimpleNamespace(current_timestamp=1000.0)
        ex._compute_fair = MagicMock(return_value=Decimal("1595.2"))
        ex._policy_side = MagicMock(return_value=None)
        ex._get_fx = MagicMock(return_value=(Decimal("1514.1"), Decimal("1514.2")))
        conn.get_price_by_type.side_effect = lambda pair, ptype: Decimal("244000")
        return ex

    @staticmethod
    def _targets():
        return [
            SimpleNamespace(price=Decimal("1610.59999999999990905"), size=Decimal("0.01"), edge_bps=Decimal("100")),
            SimpleNamespace(price=Decimal("1690.40000000000009094"), size=Decimal("0.01"), edge_bps=Decimal("600")),
        ]

    # ------------------------------------------------------------------ _place_maker
    def test_observe_true_skips_place_order_and_is_silent(self):
        ex = self._make_executor(observe=True)
        ex._place_maker(Decimal("100000"), Decimal("0.01"), Decimal("100"))
        ex.place_order.assert_not_called()
        self.assertEqual({}, ex.maker_orders)
        # The per-rung log moved to the throttled _place_targets summary; _place_maker
        # itself is silent in observe mode (no log-per-rung spam).
        ex.logger().info.assert_not_called()

    def test_observe_false_places_and_tracks(self):
        ex = self._make_executor(observe=False)
        ex._place_maker(Decimal("100000"), Decimal("0.01"), Decimal("100"))
        ex.place_order.assert_called_once()
        self.assertIn("OID-1", ex.maker_orders)

    # ------------------------------------------------------------------ _place_targets
    def test_place_targets_observe_logs_once_then_throttles(self):
        ex = self._make_executor(observe=True)
        targets = self._targets()
        ex._place_targets(targets)
        self.assertEqual(1, ex.logger().info.call_count)
        ex.place_order.assert_not_called()
        # Second call within _OBSERVE_LOG_INTERVAL_S: still no order, no new log line.
        ex._place_targets(targets)
        self.assertEqual(1, ex.logger().info.call_count)
        # After the interval elapses, one more summary line is emitted.
        ex._strategy.current_timestamp += ex._OBSERVE_LOG_INTERVAL_S
        ex._place_targets(targets)
        self.assertEqual(2, ex.logger().info.call_count)

    def test_observe_summary_line_has_fair_spot_fx_and_rungs(self):
        ex = self._make_executor(observe=True)
        ex._place_targets(self._targets())
        line = ex.logger().info.call_args[0][0]
        self.assertIn("[OBSERVE]", line)
        self.assertIn("fair=1595.2", line)
        self.assertIn("bid/ask=244000/244000", line)
        self.assertIn("fx=1514.1/1514.2", line)
        self.assertIn("1610.6@100bps", line)
        self.assertIn("1690.4@600bps", line)
        self.assertIn("no submit", line)

    # ------------------------------------------------------------------ get_custom_info
    def test_custom_info_exposes_observe_and_last_quote(self):
        ex = self._make_executor(observe=True)
        # Patch the base get_custom_info via the MRO-independent route: seed the
        # parent's dict by calling _build_observe directly through _place_targets.
        ex._place_targets(self._targets())
        # super().get_custom_info() touches cross-venue state; stub it for the unit.
        import hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base as base_mod
        orig = base_mod.CrossVenueHedgedExecutorBase.get_custom_info
        base_mod.CrossVenueHedgedExecutorBase.get_custom_info = lambda self: {"side": self.entry_side}
        try:
            info = LadderMakerExecutor.get_custom_info(ex)
        finally:
            base_mod.CrossVenueHedgedExecutorBase.get_custom_info = orig
        self.assertTrue(info["observe"])
        self.assertEqual("1595.2", info["last_quote"]["fair"])
        self.assertEqual(2, len(info["last_quote"]["rungs"]))


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack (paho) — run in Docker/CI")
class FmtNumTest(unittest.TestCase):
    def test_trims_binary_float_noise(self):
        self.assertEqual("1690.4", _fmt_num(Decimal("1690.40000000000009094")))

    def test_integer_edge_stays_plain_not_scientific(self):
        # normalize() would turn 100 into "1E+2"; the fixed-point path must not.
        self.assertEqual("100", _fmt_num(Decimal("100")))

    def test_none_renders_as_dashes(self):
        self.assertEqual("--", _fmt_num(None))

    def test_accepts_float(self):
        self.assertEqual("244000", _fmt_num(244000.0))


if __name__ == "__main__":
    unittest.main()
