import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base as mod
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.models.base import RunnableStatus


class _Tracked:
    def __init__(self, order_id):
        self.order_id = order_id
        self.order = None

    @property
    def cum_fees_quote(self):
        return Decimal("0")


@pytest.fixture(autouse=True)
def _patch_tracked(monkeypatch):
    monkeypatch.setattr(mod, "TrackedOrder", _Tracked)


class _WsHarness(CrossVenueHedgedExecutorBase):
    def __init__(self, *, enabled=True, kis_age=None, hl_age=0.0, grace=90.0):
        self.maker_orders = {}
        self.hedge_orders = {}
        self._maker_executed_base = Decimal("0")
        self._maker_executed_quote = Decimal("0")
        self._hedge_executed_base = Decimal("0")
        self._hedge_executed_quote = Decimal("0")
        self._maker_fees_quote = Decimal("0")
        self._hedge_fees_quote = Decimal("0")
        self._pending_hedge_signed = Decimal("0")
        self._current_retries = 0
        self._max_retries = 3
        self._hedge_kill_switch = False
        self._status = RunnableStatus.RUNNING
        self.maker_connector = "hl"
        self.maker_trading_pair = "X-USD"
        self.hedge_connector = "kis"
        self.hedge_trading_pair = "005930-KRW"
        self.entry_side = TradeType.SELL
        self.hedge_side = TradeType.BUY
        self.placed = []
        self.cancelled_maker = False
        self.connector_orders = {}
        self._hedge_order_side = {}
        self._hedge_credited_base = {}
        self._hedge_terminal_ids = {}
        self.config = SimpleNamespace(
            ws_staleness_kill_switch_enabled=enabled,
            max_kis_ws_age_s=3.0,
            max_hl_ws_age_s=12.0,
            ws_staleness_grace_s=grace,
        )
        self._fake_ws = {
            ("kis", "005930-KRW"): kis_age,
            ("hl", "X-USD"): hl_age,
        }
        self._strategy = SimpleNamespace(
            current_timestamp=1000.0,
            cancel=MagicMock(),
            market_data_provider=SimpleNamespace(
                get_ws_freshness_sec=lambda connector, pair: self._fake_ws.get((connector, pair))
            ),
        )
        self._gate_result = True
        self._cancel_called = False
        self._init_ws_staleness_state()

    def _update_tracked(self, *_):
        pass

    def get_in_flight_order(self, connector_name, order_id):
        return self.connector_orders.get(order_id)

    def stop(self):
        self._status = RunnableStatus.TERMINATED

    def _gates_open(self):
        return self._gate_result

    def _compute_targets(self):
        return []

    def _should_reprice(self, targets):
        return False

    def _place_targets(self, targets):
        pass

    def _maker_balance_candidate(self):
        return None

    def _size_hedge(self, pending_base):
        return {
            "amount": pending_base.to_integral_value(rounding="ROUND_DOWN"),
            "order_type": MagicMock(),
            "price": Decimal("100"),
            "metadata": {},
        }

    def place_order(self, **kwargs):
        order_id = f"h{len(self.placed)}"
        self.placed.append(order_id)
        self.connector_orders[order_id] = SimpleNamespace(
            amount=kwargs["amount"],
            is_open=True,
            is_filled=False,
            executed_amount_base=Decimal("0"),
            average_executed_price=Decimal("100"),
            price=Decimal("100"),
        )
        return order_id

    async def _seed_inventory_from_connector(self):
        pass

    def _reconcile_maker(self):
        pass

    def _cancel_all_maker(self):
        self._cancel_called = True


def test_unproven_is_stale_and_latches_at_grace():
    h = _WsHarness(enabled=True, kis_age=None)

    h._evaluate_ws_staleness()
    assert h._hedge_ws_stale is True
    assert h._staleness_kill_switch is False

    h._strategy.current_timestamp = 1090.0
    h._evaluate_ws_staleness()

    assert h._staleness_kill_switch is True


def test_recovery_within_grace_does_not_latch_and_resets():
    h = _WsHarness(enabled=True, kis_age=10.0)

    h._evaluate_ws_staleness()
    h._strategy.current_timestamp = 1030.0
    h._fake_ws[("kis", "005930-KRW")] = 0.5
    h._evaluate_ws_staleness()

    assert h._staleness_kill_switch is False
    assert h._staleness_since_ts is None
    assert h._hedge_suppress_logged is False


def test_kis_stale_suppresses_hedge_pre_latch():
    h = _WsHarness(enabled=True, kis_age=None)
    h._pending_hedge_signed = Decimal("-2")

    h._evaluate_ws_staleness()
    h._process_hedges()

    assert h._hedge_ws_stale is True
    assert h._staleness_kill_switch is False
    assert h.placed == []
    assert h._pending_hedge_base == Decimal("2")


def test_only_maker_stale_allows_hedge():
    h = _WsHarness(enabled=True, kis_age=0.5, hl_age=None)
    h._pending_hedge_signed = Decimal("-2")

    h._evaluate_ws_staleness()
    h._process_hedges()

    assert h._maker_ws_stale is True
    assert h._hedge_ws_stale is False
    assert h.placed == ["h0"]


def test_disabled_is_noop():
    h = _WsHarness(enabled=False, kis_age=None, hl_age=None)
    h._pending_hedge_signed = Decimal("-2")

    h._evaluate_ws_staleness()
    h._process_hedges()

    assert h._maker_ws_stale is False
    assert h._hedge_ws_stale is False
    assert h._staleness_kill_switch is False
    assert h.placed == ["h0"]


def test_control_task_runs_evaluate_then_suppresses_without_manual_call():
    h = _WsHarness(enabled=True, kis_age=None)
    h._gate_result = False
    h._pending_hedge_signed = Decimal("-2")

    asyncio.run(h.control_task())

    assert h._hedge_ws_stale is True
    assert h._cancel_called is True
    assert h.placed == []
    assert h._pending_hedge_base == Decimal("2")


def test_base_change_is_noop_for_subclass_without_config():
    h = _WsHarness(enabled=True, kis_age=None)
    h.config = SimpleNamespace()
    h._pending_hedge_signed = Decimal("-2")

    h._evaluate_ws_staleness()
    h._process_hedges()

    assert h._hedge_ws_stale is False
    assert h._staleness_kill_switch is False
    assert h.placed == ["h0"]
