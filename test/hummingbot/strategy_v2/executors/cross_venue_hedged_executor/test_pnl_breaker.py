"""JEP-238 — latching P&L drawdown circuit breaker (per-executor).

No loss-based kill exists today: share-count / order-rate / staleness breakers can all be
green while money bleeds (bad fair / FX divergence / hedge-slippage cascade). This breaker
trips when the executor's FX-correct net PnL (JEP-254, incl. fees) falls below an absolute
loss limit, and reuses the JEP-221 latch pattern (separate latch state so the two breakers
compose independently). Account-aggregate cap is JEP-255 (Decision Plane).

Trip action is configurable; the default "hold" must NOT be a bare stop -- a bare stop would
abandon the very naked leg it is meant to protect. "hold" therefore cancels/suppresses makers
(stops NEW exposure) but stays RUNNING and KEEPS hedging the residual via _process_hedges
(no naked leg). "flatten" routes to early_stop(flatten=True) for an active reduce-only unwind.
"""
import asyncio
from decimal import Decimal
from types import SimpleNamespace

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType


class _PnlHarness(CrossVenueHedgedExecutorBase):
    """Drives the REAL control_task; only the P&L-breaker gate is the surface under test.

    get_net_pnl_quote() is set directly (its FX-correct computation is covered by JEP-254);
    every other control_task collaborator is a counting stub.
    """

    def __init__(self, *, enabled=True, limit="100", action="hold", pnl="0"):
        self._status = RunnableStatus.RUNNING
        self.config = SimpleNamespace(
            pnl_breaker_enabled=enabled,
            pnl_loss_limit_quote=Decimal(str(limit)),
            pnl_breach_action=action,
        )
        self._pnl = Decimal(str(pnl))
        self._strategy = SimpleNamespace(current_timestamp=1000.0)
        self.close_type = None
        self.hedge_calls = 0
        self.cancel_calls = 0
        self.reconcile_calls = 0

    def get_net_pnl_quote(self) -> Decimal:
        return self._pnl

    # --- control_task collaborators (stubbed) ---
    async def _seed_inventory_from_connector(self):
        pass

    def _evaluate_session_state(self):
        pass

    def _seed_pending(self):
        return False

    def _evaluate_ws_staleness(self):
        pass

    def _gates_open(self):
        return True

    def _reconcile_maker(self):
        self.reconcile_calls += 1

    def _cancel_all_maker(self):
        self.cancel_calls += 1

    def _process_hedges(self):
        self.hedge_calls += 1

    # --- abstract-method stubs (instantiation only) ---
    def _compute_targets(self):
        return []

    def _should_reprice(self, targets):
        return False

    def _place_targets(self, targets):
        pass

    def _size_hedge(self, pending_base):
        return None

    def _maker_balance_candidate(self):
        return None


def _run(h):
    asyncio.run(h.control_task())


# ---------------------------------------------------------------- _check_pnl_breaker (unit)

def test_check_trips_below_negative_limit():
    h = _PnlHarness(enabled=True, limit="100", pnl="-150")
    assert h._check_pnl_breaker() is True


def test_check_boundary_not_below_equal_limit():
    # -100 < -100 is False: the limit is a strict floor (no trip exactly at -limit).
    h = _PnlHarness(enabled=True, limit="100", pnl="-100")
    assert h._check_pnl_breaker() is False


def test_check_disabled_never_trips():
    h = _PnlHarness(enabled=False, limit="100", pnl="-9999")
    assert h._check_pnl_breaker() is False


def test_check_requires_positive_limit_to_arm():
    # limit=0 must NOT mean "trip on any loss"; the breaker is unarmed without a positive limit.
    h = _PnlHarness(enabled=True, limit="0", pnl="-50")
    assert h._check_pnl_breaker() is False


def test_check_trips_on_nan_pnl_conservatively():
    # A NaN PnL means the accounting is already corrupt; `NaN < -limit` is False (which would
    # silently DISARM the breaker), so it must trip conservatively instead of going blind.
    h = _PnlHarness(enabled=True, limit="100", pnl="NaN")
    assert h._check_pnl_breaker() is True


# ------------------------------------------------------------- control_task integration

def test_hold_trip_suppresses_makers_keeps_hedging_stays_running():
    h = _PnlHarness(enabled=True, limit="100", action="hold", pnl="-150")
    _run(h)
    assert h._pnl_breaker_tripped is True
    assert h.cancel_calls == 1
    assert h.reconcile_calls == 0                  # makers suppressed (no new exposure)
    assert h.hedge_calls == 1                       # residual still hedged -> no naked leg
    assert h.status == RunnableStatus.RUNNING       # held, NOT terminated


def test_hold_stays_latched_after_pnl_recovers():
    h = _PnlHarness(enabled=True, limit="100", action="hold", pnl="-150")
    _run(h)                                          # trip
    h._pnl = Decimal("500")                          # PnL recovers well above the limit
    _run(h)                                          # second tick
    assert h._pnl_breaker_tripped is True            # latched: does NOT self-clear
    assert h.reconcile_calls == 0                    # still suppressed
    assert h.hedge_calls == 2                         # hedging continues every tick


def test_flatten_trip_enters_shutdown():
    h = _PnlHarness(enabled=True, limit="100", action="flatten", pnl="-150")
    _run(h)
    assert h._pnl_breaker_tripped is True
    assert h.status == RunnableStatus.SHUTTING_DOWN
    assert getattr(h, "_flatten_on_stop", False) is True
    assert h.close_type == CloseType.EARLY_STOP


def test_no_trip_when_pnl_above_limit_quotes_normally():
    h = _PnlHarness(enabled=True, limit="100", action="hold", pnl="-50")
    _run(h)
    assert h._pnl_breaker_tripped is False
    assert h.reconcile_calls == 1
    assert h.cancel_calls == 0
    assert h.hedge_calls == 1


# --------------------------------------------------------------------- real config defaults

def test_real_config_carries_pnl_breaker_defaults():
    from test.hummingbot.strategy_v2.executors.ladder_maker_executor.test_latency_config import _cfg

    cfg = _cfg()
    assert cfg.pnl_breaker_enabled is False
    assert cfg.pnl_loss_limit_quote == Decimal("0")
    assert cfg.pnl_breach_action == "hold"
