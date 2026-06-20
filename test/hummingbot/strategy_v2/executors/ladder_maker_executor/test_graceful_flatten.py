import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import Side
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import (
        LadderMakerExecutor,
    )

    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    _EXECUTOR_IMPORTABLE = False


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack")
class LadderMakerGracefulFlattenTest(unittest.TestCase):
    def _make_executor(
        self,
        *,
        wind_down: bool = False,
        observe: bool = True,
        perp_net: Decimal = Decimal("-0.01"),
        paired_oi: Decimal = Decimal("0.01"),
        timestamp: float = 100.0,
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
            flatten_timeout_s=30.0,
            fx_connector=None,
            fx_trading_pair=None,
            static_fx_rate=Decimal("1"),
            side_aware_fx=True,
            target_inventory=Decimal("0"),
            inventory_skew_bps_per_unit=Decimal("0"),
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
        ex.hedge_orders = {}
        ex._maker_placed_edge_bps = {}
        ex.logger = MagicMock(return_value=MagicMock())
        ex._last_observe = None
        ex._last_observe_log_ts = 0.0
        ex._observe_log_interval_s = 5.0
        ex._strategy = MagicMock()
        ex._strategy.current_timestamp = timestamp
        ex._status = RunnableStatus.RUNNING
        ex.close_type = None
        ex.stop = MagicMock()
        ex._flatten_on_stop = False
        ex._flatten_started_ts = None
        ex._hedge_kill_switch = False
        ex._pending_hedge_signed = Decimal("0")
        ex._maker_executed_base = Decimal("0")
        ex._open_edge_vwap = Decimal("100")
        ex._perp_net = MagicMock(return_value=perp_net)
        ex._paired_oi = MagicMock(return_value=paired_oi)
        ex._unhedged_base_signed = MagicMock(return_value=Decimal("0"))
        ex._process_hedges = MagicMock()
        ex._reconcile_maker = MagicMock()
        ex._cancel_all_maker = MagicMock()
        from hummingbot.strategy_v2.executors.ladder_maker_executor.session_calendar import KrxSessionCalendar

        ex._calendar = KrxSessionCalendar()
        return ex

    def _resting(self, order_id: str = "OID-OPEN", side: TradeType = TradeType.SELL):
        return SimpleNamespace(
            order_id=order_id,
            order=SimpleNamespace(trade_type=side, is_open=True, order_id=order_id),
        )

    def test_early_stop_default_unchanged(self):
        ex = self._make_executor()

        ex.early_stop()

        self.assertEqual(CloseType.EARLY_STOP, ex.close_type)
        self.assertEqual(RunnableStatus.SHUTTING_DOWN, ex._status)
        self.assertFalse(ex._flatten_on_stop)

        ex = self._make_executor()
        ex.early_stop(keep_position=True)

        self.assertEqual(CloseType.POSITION_HOLD, ex.close_type)
        ex.stop.assert_called_once()

    def test_early_stop_flatten_sets_flag(self):
        ex = self._make_executor()

        ex.early_stop(flatten=True)

        self.assertTrue(ex._flatten_on_stop)
        self.assertEqual(CloseType.EARLY_STOP, ex.close_type)
        self.assertEqual(RunnableStatus.SHUTTING_DOWN, ex._status)

    def test_flatten_before_timeout_runs_close_ladder(self):
        ex = self._make_executor(perp_net=Decimal("-0.01"), timestamp=110.0)
        ex._flatten_on_stop = True
        ex._flatten_started_ts = 100.0

        result = ex._flatten_unwind_step()

        self.assertTrue(result)
        ex._process_hedges.assert_called_once()
        ex._reconcile_maker.assert_called_once()
        ex.place_order.assert_not_called()

    def test_flatten_after_timeout_sends_one_market_close(self):
        ex = self._make_executor(perp_net=Decimal("-0.01"), timestamp=131.0)
        ex._flatten_on_stop = True
        ex._flatten_started_ts = 100.0
        ex._open_maker_orders = MagicMock(return_value=[])

        result = ex._flatten_unwind_step()

        self.assertTrue(result)
        ex.place_order.assert_called_once()
        call = ex.place_order.call_args
        self.assertEqual(OrderType.MARKET, call.kwargs["order_type"])
        self.assertEqual(PositionAction.CLOSE, call.kwargs["position_action"])
        self.assertEqual(TradeType.BUY, call.kwargs["side"])
        self.assertEqual(Decimal("0.01"), call.kwargs["amount"])
        self.assertTrue(call.kwargs["price"].is_nan())

        ex = self._make_executor(perp_net=Decimal("-0.01"), timestamp=131.0)
        ex._flatten_on_stop = True
        ex._flatten_started_ts = 100.0
        ex._open_maker_orders = MagicMock(return_value=[self._resting()])

        result = ex._flatten_unwind_step()

        self.assertTrue(result)
        ex._cancel_all_maker.assert_called_once()
        ex.place_order.assert_not_called()

    def test_no_duplicate_taker_during_created_window(self):
        ex = self._make_executor(perp_net=Decimal("-0.01"), timestamp=131.0)
        ex._flatten_on_stop = True
        ex._flatten_started_ts = 100.0

        self.assertTrue(ex._flatten_unwind_step())
        ex.place_order.reset_mock()

        self.assertTrue(ex._flatten_unwind_step())

        ex.place_order.assert_not_called()
        ex._cancel_all_maker.assert_not_called()

    def test_taker_not_canceled_once_open(self):
        ex = self._make_executor(perp_net=Decimal("-0.01"), timestamp=131.0)
        ex._flatten_on_stop = True
        ex._flatten_started_ts = 100.0

        self.assertTrue(ex._flatten_unwind_step())
        ex.maker_orders["OID-1"].order = SimpleNamespace(is_open=True)
        ex.place_order.reset_mock()

        self.assertTrue(ex._flatten_unwind_step())

        ex.place_order.assert_not_called()
        ex._cancel_all_maker.assert_not_called()

    def test_partial_fill_resends_remainder(self):
        ex = self._make_executor(perp_net=Decimal("-0.01"), timestamp=131.0)
        ex._flatten_on_stop = True
        ex._flatten_started_ts = 100.0

        self.assertTrue(ex._flatten_unwind_step())
        ex.maker_orders["OID-1"].order = SimpleNamespace(is_open=False)
        ex._perp_net.return_value = Decimal("-0.004")
        ex.place_order.reset_mock()

        self.assertTrue(ex._flatten_unwind_step())

        ex.place_order.assert_called_once()
        self.assertEqual(Decimal("0.004"), ex.place_order.call_args.kwargs["amount"])

    def test_kill_switch_holds_no_taker(self):
        ex = self._make_executor(perp_net=Decimal("-0.01"), timestamp=131.0)
        ex._flatten_on_stop = True
        ex._flatten_started_ts = 100.0
        ex._hedge_kill_switch = True

        result = ex._flatten_unwind_step()

        self.assertTrue(result)
        ex._process_hedges.assert_called_once()
        ex.place_order.assert_not_called()
        ex._cancel_all_maker.assert_not_called()

    def test_does_not_stop_while_taker_live(self):
        ex = self._make_executor(perp_net=Decimal("0"), timestamp=131.0)
        ex._flatten_on_stop = True
        ex._flatten_started_ts = 100.0
        ex._flatten_taker_oid = "OID-1"
        ex.maker_orders["OID-1"] = TrackedOrder(order_id="OID-1")

        result = ex._flatten_unwind_step()

        self.assertTrue(result)
        ex._process_hedges.assert_called_once()

    def test_flatten_perp_flat_returns_false(self):
        ex = self._make_executor(perp_net=Decimal("0"))
        ex._flatten_on_stop = True

        result = ex._flatten_unwind_step()

        self.assertFalse(result)
        ex._process_hedges.assert_called_once()

    def test_early_stop_flatten_anchors_started_ts(self):
        ex = self._make_executor(timestamp=123.0)

        ex.early_stop(flatten=True)

        self.assertEqual(123.0, ex._flatten_started_ts)

    def test_compute_targets_close_only_under_flatten(self):
        ex = self._make_executor(wind_down=False, paired_oi=Decimal("0.01"))
        ex._flatten_on_stop = True

        targets = ex._compute_targets()

        self.assertEqual([], [target for target in targets if target.side == Side.SELL])
        self.assertGreaterEqual(len([target for target in targets if target.side == Side.BUY]), 1)


if __name__ == "__main__":
    unittest.main()
