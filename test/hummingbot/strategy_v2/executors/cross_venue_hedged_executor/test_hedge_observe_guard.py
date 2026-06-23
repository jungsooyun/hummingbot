from decimal import Decimal
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import TrackedOrder


class _GuardProbe(CrossVenueHedgedExecutorBase):
    """Concrete subclass stubbing the 6 abstract hooks so the class is instantiable."""
    def _gates_open(self): return True
    def _compute_targets(self): return []
    def _should_reprice(self, targets): return False
    def _place_targets(self, targets): pass
    def _size_hedge(self, pending_base): return None
    def _maker_balance_candidate(self): return None


def _make_executor(observe: bool):
    ex = _GuardProbe.__new__(_GuardProbe)
    ex.config = MagicMock(observe=observe)
    ex._strategy = MagicMock()
    ex.place_order = MagicMock(return_value="HEDGE-1")
    ex.hedge_connector = "okx_perpetual"
    ex.hedge_trading_pair = "BTC-USDT"
    ex._pending_hedge_signed = Decimal("0.01")  # > 0 => needed_side = SELL
    # _pending_hedge_base is a @property = abs(_pending_hedge_signed), no need to set

    stale = TrackedOrder(order_id="STALE-1")
    stale.order = MagicMock(is_done=False, is_open=True)
    ex.hedge_orders = {"STALE-1": stale}
    ex._hedge_order_side = {"STALE-1": (TradeType.BUY, Decimal("0.01"))}  # BUY != needed SELL => cancel fires

    ex._ensure_direction_accounting = MagicMock()
    ex._reconcile_stuck_hedges = MagicMock()
    ex._hedge_in_flight = MagicMock(return_value=False)
    ex._hedge_ws_stale = False  # _process_hedges reads this via getattr on live path
    ex._size_hedge = MagicMock(return_value={
        "amount": Decimal("0.01"),
        "price": Decimal("65000"),
        "order_type": OrderType.LIMIT,
        "metadata": {"order_role": "hedge"},
    })
    ex._status = RunnableStatus.RUNNING  # status is a read-only @property backed by _status
    return ex


def test_observe_blocks_cancel_and_place():
    ex = _make_executor(observe=True)
    ex._process_hedges()
    ex.place_order.assert_not_called()
    ex._strategy.cancel.assert_not_called()


def test_live_runs_cancel_and_place():
    ex = _make_executor(observe=False)
    ex._process_hedges()
    ex._strategy.cancel.assert_called_once()   # stale opposite-side order cancelled
    ex.place_order.assert_called_once()        # real hedge placed
