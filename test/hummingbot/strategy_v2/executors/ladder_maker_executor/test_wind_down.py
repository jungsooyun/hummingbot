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
class LadderMakerWindDownTest(unittest.TestCase):
    def _make_executor(
        self,
        *,
        wind_down: bool,
        observe: bool = True,
        paired_oi: Decimal = Decimal("0.01"),
    ) -> "LadderMakerExecutor":
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex.config = SimpleNamespace(
            observe=observe,
            two_sided=True,
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
            wind_down=wind_down,
            max_inventory=None,
            min_reprice_interval_s=0.0,
            min_reprice_delta_ticks=2,
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
        ex.connectors = {ex.maker_connector: conn, ex.hedge_connector: conn}

        ex.place_order = MagicMock(side_effect=["OID-1", "OID-2", "OID-3"])
        ex.maker_orders = {}
        ex._maker_placed_edge_bps = {}
        ex.logger = MagicMock(return_value=MagicMock())
        ex._last_observe = None
        ex._last_observe_log_ts = 0.0
        ex._last_reprice_ts = 0.0
        ex._strategy = SimpleNamespace(current_timestamp=1000.0, cancel=MagicMock())
        ex._compute_fair = MagicMock(return_value=Decimal("100"))
        ex._policy_side = MagicMock(return_value=Side.SELL)
        from hummingbot.strategy_v2.executors.ladder_maker_executor.fx_bridged_fair_source import FxBridgedFairSource

        ex._fair = FxBridgedFairSource(
            getattr(ex.config, "side_aware_fx", True),
            getattr(ex.config, "static_fx_rate", None),
            LadderMakerExecutor.logger(),
        )
        ex._fair._get_fx = MagicMock(return_value=(Decimal("1300"), Decimal("1301")))
        ex._paired_oi = MagicMock(return_value=paired_oi)
        ex._unhedged_base_signed = MagicMock(return_value=Decimal("0.002"))
        ex._open_edge_vwap = Decimal("0")
        ex._pending_hedge_signed = Decimal("-0.003")
        ex._maker_executed_base = Decimal("0")
        from hummingbot.strategy_v2.executors.ladder_maker_executor.session_calendar import KrxSessionCalendar

        ex._calendar = KrxSessionCalendar()
        return ex

    def _resting(self, *, order_id: str, side: TradeType, price: str, amount: str = "0.01"):
        order = SimpleNamespace(
            price=Decimal(price),
            trade_type=side,
            amount=Decimal(amount),
            is_open=True,
            order_id=order_id,
        )
        return SimpleNamespace(order_id=order_id, order=order)

    def _assert_close_only_targets(self, targets):
        open_targets = [target for target in targets if target.side == Side.SELL]
        close_targets = [target for target in targets if target.side == Side.BUY]
        self.assertEqual([], open_targets)
        self.assertGreaterEqual(len(close_targets), 1)

    def test_wind_down_compute_targets_close_only(self):
        ex = self._make_executor(wind_down=True, paired_oi=Decimal("0.01"))

        targets = ex._compute_targets()

        self._assert_close_only_targets(targets)

    def test_wind_down_reconcile_cancels_open_keeps_close(self):
        ex = self._make_executor(wind_down=True, observe=False, paired_oi=Decimal("0.01"))
        open_maker = self._resting(order_id="OID-OPEN", side=TradeType.SELL, price="101")
        close_maker = self._resting(order_id="OID-CLOSE", side=TradeType.BUY, price="99")
        ex._open_maker_orders = MagicMock(return_value=[open_maker, close_maker])

        ex._reconcile_maker()

        ex._strategy.cancel.assert_any_call(ex.maker_connector, ex.maker_trading_pair, "OID-OPEN")
        ex._strategy.cancel.assert_any_call(ex.maker_connector, ex.maker_trading_pair, "OID-CLOSE")
        self.assertGreaterEqual(ex.place_order.call_count, 1)
        for call in ex.place_order.call_args_list:
            self.assertEqual(TradeType.BUY, call.kwargs["side"])
            self.assertEqual(PositionAction.CLOSE, call.kwargs["position_action"])

    def test_wind_down_distinct_from_kill_switch(self):
        ex = self._make_executor(wind_down=True, paired_oi=Decimal("0.01"))

        targets = ex._compute_targets()

        self.assertFalse(getattr(ex, "_hedge_kill_switch", False))
        self._assert_close_only_targets(targets)

    def test_wind_down_false_emits_both_sides(self):
        ex = self._make_executor(wind_down=False, paired_oi=Decimal("0.01"))

        targets = ex._compute_targets()

        self.assertGreaterEqual(len([target for target in targets if target.side == Side.SELL]), 1)
        self.assertGreaterEqual(len([target for target in targets if target.side == Side.BUY]), 1)


if __name__ == "__main__":
    unittest.main()
