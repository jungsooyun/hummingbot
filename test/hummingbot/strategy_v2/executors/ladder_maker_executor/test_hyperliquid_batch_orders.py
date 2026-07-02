import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import RungTarget, Side
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor


class LadderMakerHyperliquidBatchOrderTests(TestCase):
    def _make_executor(self, *, enabled: bool = True, maker_post_only: bool = True):
        executor = LadderMakerExecutor.__new__(LadderMakerExecutor)
        executor.config = SimpleNamespace(
            enable_hyperliquid_batch_orders=enabled,
            maker_post_only=maker_post_only,
            two_sided=False,
            maker_tick=Decimal("0.1"),
            min_reprice_delta_ticks=Decimal("2"),
            placement_rate_breaker_enabled=False,
        )
        executor.maker_connector = "hyperliquid_perpetual"
        executor.maker_trading_pair = "XYZ:SKHX-USD"
        executor.entry_side = TradeType.SELL
        connector = MagicMock()
        connector.batch_place_orders.return_value = [
            "0x00000000000000000000000000000001",
            "0x00000000000000000000000000000002",
        ]
        connector.batch_cancel_by_cloid.return_value = ["OID-1", "OID-2"]
        connector.is_hyperliquid_batch_modify_rejected.return_value = False
        connector.is_hyperliquid_batch_modify_ambiguous.return_value = False
        connector.quantize_order_price.side_effect = lambda pair, price: price
        connector.quantize_order_amount.side_effect = lambda pair, amount: amount
        executor.connectors = {"hyperliquid_perpetual": connector}
        executor.maker_orders = {}
        executor._maker_placement_ts = []
        executor._rate_halt_latched = False
        executor._strategy = MagicMock()
        executor._strategy.current_timestamp = 1000.0
        executor.logger = MagicMock(return_value=MagicMock())
        return executor, connector

    def _resting(self, cloid, price="100.0", ts=970.0, coin="SKHX"):
        from hummingbot.connector.derivative.hyperliquid_perpetual.hyperliquid_perpetual_derivative import HyperliquidRestingOrder
        return HyperliquidRestingOrder(cloid=cloid, oid=1, coin=coin, side="A",
                                       price=Decimal(price), size=Decimal("1"), timestamp=ts)

    def _sell_targets(self, n, start=0):
        return [RungTarget(side=Side.SELL, price=Decimal("100") + Decimal(start + i), size=Decimal("1"),
                           edge_bps=Decimal("20")) for i in range(n)]

    def test_place_targets_subset_uses_hyperliquid_batch_when_enabled(self):
        executor, connector = self._make_executor(enabled=True)
        targets = [
            RungTarget(side=Side.SELL, price=Decimal("100"), size=Decimal("1"), edge_bps=Decimal("20")),
            RungTarget(side=Side.SELL, price=Decimal("101"), size=Decimal("2"), edge_bps=Decimal("30")),
        ]

        executor._place_targets_subset(targets)

        connector.batch_place_orders.assert_called_once()
        requests = connector.batch_place_orders.call_args.args[0]
        self.assertEqual(2, len(requests))
        self.assertEqual(Decimal("100"), requests[0].price)
        self.assertEqual(Decimal("2"), requests[1].amount)
        self.assertEqual(2, len(executor.maker_orders))
        self.assertEqual(Side.SELL, executor._maker_placed_rung["0x00000000000000000000000000000001"][0])
        self.assertEqual(Decimal("20"), executor._maker_placed_edge_bps["0x00000000000000000000000000000001"])

    def test_place_targets_subset_falls_back_when_not_post_only(self):
        executor, connector = self._make_executor(enabled=True, maker_post_only=False)
        executor._place_target_one = MagicMock()
        targets = [
            RungTarget(side=Side.SELL, price=Decimal("100"), size=Decimal("1"), edge_bps=Decimal("20")),
        ]

        executor._place_targets_subset(targets)

        connector.batch_place_orders.assert_not_called()
        executor._place_target_one.assert_called_once_with(targets[0])

    def test_place_targets_subset_falls_back_only_from_failed_chunk(self):
        executor, connector = self._make_executor(enabled=True)
        executor._place_target_one = MagicMock()
        first_chunk_order_ids = [
            f"0x{i + 1:032x}"
            for i in range(8)
        ]
        connector.batch_place_orders.side_effect = [first_chunk_order_ids, ValueError("bad second chunk")]
        targets = [
            RungTarget(
                side=Side.SELL,
                price=Decimal("100") + Decimal(i),
                size=Decimal("1"),
                edge_bps=Decimal("20"),
            )
            for i in range(9)
        ]

        executor._place_targets_subset(targets)

        self.assertEqual(2, connector.batch_place_orders.call_count)
        self.assertEqual(first_chunk_order_ids, list(executor.maker_orders.keys()))
        executor._place_target_one.assert_called_once_with(targets[8])

    def test_place_targets_subset_skips_fallback_on_order_id_count_mismatch(self):
        executor, connector = self._make_executor(enabled=True)
        executor._place_target_one = MagicMock()
        connector.batch_place_orders.return_value = ["0x00000000000000000000000000000001"]
        targets = [
            RungTarget(side=Side.SELL, price=Decimal("100"), size=Decimal("1"), edge_bps=Decimal("20")),
            RungTarget(side=Side.SELL, price=Decimal("101"), size=Decimal("1"), edge_bps=Decimal("21")),
        ]

        executor._place_targets_subset(targets)

        connector.batch_place_orders.assert_called_once()
        executor._place_target_one.assert_not_called()
        self.assertEqual({}, executor.maker_orders)
        self.assertFalse(hasattr(executor, "_last_reprice_ts"))

    def test_cancel_maker_order_ids_uses_hyperliquid_batch_when_enabled(self):
        executor, connector = self._make_executor(enabled=True)

        executor._cancel_maker_order_ids(["OID-1", "OID-2"])

        connector.batch_cancel_by_cloid.assert_called_once()
        cancels = connector.batch_cancel_by_cloid.call_args.args[0]
        self.assertEqual(["OID-1", "OID-2"], [cancel.client_order_id for cancel in cancels])
        executor._strategy.cancel.assert_not_called()

    def test_cancel_maker_order_ids_falls_back_when_disabled(self):
        executor, connector = self._make_executor(enabled=False)

        executor._cancel_maker_order_ids(["OID-1", "OID-2"])

        connector.batch_cancel_by_cloid.assert_not_called()
        self.assertEqual(2, executor._strategy.cancel.call_count)

    def test_stuck_maker_adoption_forced_on_when_batch_enabled(self):
        executor, _ = self._make_executor(enabled=True)
        executor.config.reconcile_stuck_makers_enabled = False
        self.assertTrue(executor._stuck_maker_adoption_enabled())

    def test_stuck_maker_adoption_respects_config_when_batch_disabled(self):
        executor, _ = self._make_executor(enabled=False)
        executor.config.reconcile_stuck_makers_enabled = False
        self.assertFalse(executor._stuck_maker_adoption_enabled())

    def test_select_orphans_flags_executor_owned_lost_aged_even_if_price_matches_target(self):
        executor, _ = self._make_executor(enabled=True)
        executor.config.batch_sweep_min_age_s = 5.0
        orphan = "0xorphan01"
        executor._submitted_maker_cloids = {orphan}
        executor.maker_orders = {}
        resting = [self._resting(orphan, ts=1000 - 30)]
        self.assertEqual([orphan], executor._select_orphans(resting, 1000.0))

    def test_select_orphans_spares_tracked_fresh_and_other_executor(self):
        executor, _ = self._make_executor(enabled=True)
        executor.config.batch_sweep_min_age_s = 5.0
        tracked = "0xtracked1"; fresh = "0xfresh1"; other_ctrl = "0xotherctrl1"
        executor._submitted_maker_cloids = {tracked, fresh}
        executor.maker_orders = {tracked: MagicMock()}
        resting = [
            self._resting(tracked,    ts=1000 - 30),
            self._resting(fresh,      ts=1000 - 2),
            self._resting(other_ctrl, ts=1000 - 30),
        ]
        self.assertEqual([], executor._select_orphans(resting, 1000.0))

    def test_orphan_sweep_enabled_is_independent_of_placement_gate(self):
        executor, _ = self._make_executor(enabled=True)
        executor.config.batch_sweep_interval_s = 15.0
        executor.config.batch_disable_in_auction = True
        executor._session_halt_state = SimpleNamespace(halted=True, reason="auction")
        executor._calendar = MagicMock(); executor._calendar.in_auction_window.return_value = True
        self.assertFalse(executor._hyperliquid_batch_orders_enabled())
        self.assertTrue(executor._hyperliquid_orphan_sweep_enabled())

    def test_orphan_sweep_disabled_when_interval_zero(self):
        executor, _ = self._make_executor(enabled=True)
        executor.config.batch_sweep_interval_s = 0.0
        self.assertFalse(executor._hyperliquid_orphan_sweep_enabled())

    def test_async_orphan_sweep_awaits_cancel_of_selected(self):
        executor, connector = self._make_executor(enabled=True)
        executor.config.batch_sweep_min_age_s = 5.0
        executor._strategy.current_timestamp = 1000.0
        orphan = "0xorphanA"
        executor._submitted_maker_cloids = {orphan}
        executor.maker_orders = {}
        connector.fetch_open_orders_by_cloid = AsyncMock(return_value=[self._resting(orphan, ts=1000 - 30)])
        connector.cancel_orders_by_cloid_unchecked = AsyncMock()
        asyncio.run(executor._async_orphan_sweep())
        connector.cancel_orders_by_cloid_unchecked.assert_awaited_once()
        self.assertEqual([orphan], connector.cancel_orders_by_cloid_unchecked.await_args.args[1])

    def test_batch_place_allows_a_full_generation_multi_rung(self):
        executor, connector = self._make_executor(enabled=True)
        executor.config.batch_max_generations_per_side = 1
        executor.config.batch_margin_headroom_quote = Decimal("0")
        executor.maker_orders = {}
        executor._maker_placed_rung = {}
        executor._target_count_by_side = {Side.SELL: 3}
        executor._place_targets_subset(self._sell_targets(3))
        connector.batch_place_orders.assert_called_once()
        self.assertEqual(3, len(connector.batch_place_orders.call_args.args[0]))

    def test_batch_place_allows_the_missing_rung_when_two_already_rest(self):
        executor, connector = self._make_executor(enabled=True)
        executor.config.batch_max_generations_per_side = 1
        executor.config.batch_margin_headroom_quote = Decimal("0")
        executor.maker_orders = {f"0x{i}": MagicMock(order=None) for i in range(2)}
        executor._maker_placed_rung = {f"0x{i}": (Side.SELL, Decimal("100") + Decimal(i), Decimal("1")) for i in range(2)}
        executor._target_count_by_side = {Side.SELL: 3}
        connector.batch_place_orders.return_value = ["0xnew1"]
        executor._place_targets_subset(self._sell_targets(1, start=2))
        connector.batch_place_orders.assert_called_once()
        self.assertEqual(1, len(connector.batch_place_orders.call_args.args[0]))

    def test_batch_place_blocks_a_second_generation(self):
        executor, connector = self._make_executor(enabled=True)
        executor.config.batch_max_generations_per_side = 1
        executor.config.batch_margin_headroom_quote = Decimal("0")
        executor.maker_orders = {f"0x{i}": MagicMock(order=None) for i in range(3)}
        executor._maker_placed_rung = {f"0x{i}": (Side.SELL, Decimal("100") + Decimal(i), Decimal("1")) for i in range(3)}
        executor._target_count_by_side = {Side.SELL: 3}
        executor._place_targets_subset(self._sell_targets(3, start=10))
        connector.batch_place_orders.assert_not_called()

    def test_batch_place_skips_when_below_margin_headroom(self):
        executor, connector = self._make_executor(enabled=True)
        executor.config.batch_max_generations_per_side = 99
        executor.config.batch_margin_headroom_quote = Decimal("500")
        executor.maker_orders = {}
        executor._maker_placed_rung = {}
        executor._target_count_by_side = {Side.SELL: 1}
        connector.get_available_balance = MagicMock(return_value=Decimal("100"))
        executor._place_targets_subset(self._sell_targets(1))
        connector.batch_place_orders.assert_not_called()

    def test_post_only_reject_backoff_skips_reprice_then_resumes(self):
        executor, _ = self._make_executor(enabled=True)
        executor.config.post_only_reject_backoff_count = 3
        executor.config.post_only_reject_window_s = 10.0
        executor._post_only_reject_ts = []
        executor._strategy.current_timestamp = 1000.0
        for _ in range(3):
            executor._note_post_only_reject()
        self.assertTrue(executor._in_post_only_backoff())
        executor._strategy.current_timestamp = 1011.0
        self.assertFalse(executor._in_post_only_backoff())

    def test_post_only_reject_backoff_off_by_default(self):
        executor, _ = self._make_executor(enabled=True)
        executor.config.post_only_reject_backoff_count = 0
        executor._post_only_reject_ts = []
        executor._strategy.current_timestamp = 1000.0
        for _ in range(50):
            executor._note_post_only_reject()
        self.assertFalse(executor._in_post_only_backoff())

    def test_batch_disabled_during_auction_or_halt(self):
        executor, _ = self._make_executor(enabled=True)
        executor.config.batch_disable_in_auction = True
        executor._session_halt_state = SimpleNamespace(halted=True, reason="auction")
        executor._calendar = MagicMock()
        executor._calendar.in_auction_window.return_value = True
        self.assertFalse(executor._hyperliquid_batch_orders_enabled())

    def test_batch_enabled_outside_auction(self):
        executor, _ = self._make_executor(enabled=True)
        executor.config.batch_disable_in_auction = True
        executor._session_halt_state = SimpleNamespace(halted=False, reason=None)
        executor._calendar = MagicMock()
        executor._calendar.in_auction_window.return_value = False
        self.assertTrue(executor._hyperliquid_batch_orders_enabled())

    def test_batch_modify_matches_subset_and_routes_only_delta(self):
        executor, connector = self._make_executor(enabled=True)
        matched_maker = SimpleNamespace(
            order_id="OID-MATCH",
            order=SimpleNamespace(
                price=Decimal("100"),
                amount=Decimal("1"),
                trade_type=TradeType.SELL,
                position=PositionAction.OPEN,
                order_type=OrderType.LIMIT_MAKER,
                is_done=False,
                executed_amount_base=Decimal("0"),
            ),
        )
        unmatched_maker = SimpleNamespace(
            order_id="OID-CANCEL",
            order=SimpleNamespace(
                price=Decimal("101"),
                amount=Decimal("2"),
                trade_type=TradeType.SELL,
                position=PositionAction.OPEN,
                order_type=OrderType.LIMIT_MAKER,
                is_done=False,
                executed_amount_base=Decimal("0"),
            ),
        )
        modify_target = RungTarget(side=Side.SELL, price=Decimal("102"), size=Decimal("1"), edge_bps=Decimal("20"))
        new_target = RungTarget(side=Side.SELL, price=Decimal("103"), size=Decimal("3"), edge_bps=Decimal("30"))
        executor._open_maker_orders = MagicMock(return_value=[matched_maker, unmatched_maker])
        executor._should_reprice = MagicMock(return_value=True)
        executor._cancel_maker_order_ids = MagicMock()
        executor._place_targets_subset = MagicMock()
        async def _noop(*_args, **_kwargs):
            return None
        executor._execute_hyperliquid_batch_modify = MagicMock(side_effect=_noop)

        with patch(
            "hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor.safe_ensure_future",
            side_effect=lambda coro: coro.close(),
        ):
            handled = executor._try_hyperliquid_batch_modify([modify_target, new_target])

        self.assertTrue(handled)
        executor._execute_hyperliquid_batch_modify.assert_called_once()
        requests = executor._execute_hyperliquid_batch_modify.call_args.args[0]
        self.assertEqual(1, len(requests))
        self.assertEqual("OID-MATCH", requests[0].client_order_id)
        self.assertEqual(Decimal("102"), requests[0].price)
        executor._place_targets_subset.assert_called_once_with([new_target])
        executor._cancel_maker_order_ids.assert_called_once_with(["OID-CANCEL"])
