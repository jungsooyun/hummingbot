import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import TradeType
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
class LadderMakerCloseRepriceTest(unittest.TestCase):
    def _make_executor(self, *, two_sided: bool = True) -> "LadderMakerExecutor":
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex.config = SimpleNamespace(
            two_sided=two_sided,
            min_reprice_interval_s=0.0,
            min_reprice_delta_ticks=2,
            maker_tick=Decimal("0.1"),
        )
        ex.maker_connector = "hyperliquid_perpetual"
        ex.maker_trading_pair = "XYZ:SKHX-USD"
        ex.entry_side = TradeType.SELL
        ex._last_reprice_ts = 0.0
        ex._strategy = SimpleNamespace(current_timestamp=1000.0)

        conn = MagicMock()
        conn.quantize_order_price.side_effect = lambda pair, price: price
        conn.quantize_order_amount.side_effect = lambda pair, amount: amount
        ex.connectors = {ex.maker_connector: conn}
        ex._open_maker_orders = MagicMock(return_value=[])
        return ex

    def _target(self, side: Side, price: str, size: str = "1"):
        return SimpleNamespace(side=side, price=Decimal(price), size=Decimal(size))

    def _resting(self, side: TradeType, price: str, amount: str = "1"):
        order = SimpleNamespace(
            price=Decimal(price),
            trade_type=side,
            amount=Decimal(amount),
            is_open=True,
        )
        return SimpleNamespace(order=order)

    def _open_resting(self, price: str = "101", amount: str = "1"):
        return self._resting(TradeType.SELL, price, amount)

    def _close_resting(self, price: str = "99", amount: str = "1"):
        return self._resting(TradeType.BUY, price, amount)

    def _open_target(self, price: str = "101", size: str = "1"):
        return self._target(Side.SELL, price, size)

    def _close_target(self, price: str = "99", size: str = "1"):
        return self._target(Side.BUY, price, size)

    def test_close_full_unwind_Q_to_zero_reprices(self):
        ex = self._make_executor()
        ex._open_maker_orders.return_value = [self._open_resting(), self._close_resting()]
        targets = [self._open_target()]

        self.assertTrue(ex._should_reprice(targets))

    def test_eod_ramp_shifts_close_price_reprices(self):
        ex = self._make_executor()
        ex._open_maker_orders.return_value = [self._open_resting(), self._close_resting("99")]
        targets = [self._open_target(), self._close_target("98.7")]

        self.assertTrue(ex._should_reprice(targets))

    def test_wind_down_flip_cancels_open_reprices(self):
        ex = self._make_executor()
        ex._open_maker_orders.return_value = [self._open_resting(), self._close_resting()]
        targets = [self._close_target()]

        self.assertTrue(ex._should_reprice(targets))

    def test_partial_Q_resizes_close_reprices(self):
        ex = self._make_executor()
        ex._open_maker_orders.return_value = [
            self._open_resting(),
            self._close_resting("99", "2"),
        ]
        targets = [self._open_target(), self._close_target("99", "1")]

        self.assertTrue(ex._should_reprice(targets))

    def test_two_sided_no_change_holds(self):
        ex = self._make_executor()
        ex._open_maker_orders.return_value = [self._open_resting(), self._close_resting()]
        targets = [self._open_target(), self._close_target()]

        self.assertFalse(ex._should_reprice(targets))

    def test_single_direction_first_rung_gate_unchanged(self):
        ex = self._make_executor(two_sided=False)
        ex._open_maker_orders.return_value = [self._open_resting("100")]
        targets = [self._open_target("100.19")]

        self.assertFalse(ex._should_reprice(targets))
