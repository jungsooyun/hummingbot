from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import TradeType
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
