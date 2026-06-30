"""JEP-284 — ``control_task`` fail-closed on a non-finite (NaN) Hummingbot clock.

Incident: a strategy-level stop (drawdown guard / manual kill-switch / cash-out)
calls ``HummingbotApplication.stop()``, which freezes the Hummingbot clock so
``strategy.current_timestamp`` goes NaN. This executor's control loop is an
INDEPENDENT asyncio task that keeps cycling AFTER the strategy stops -- it read the
NaN clock and crash-looped forever in ``SessionCalendar.now()``
(``datetime.fromtimestamp(NaN) -> ValueError``), placing 0 orders.

The calendar got a crash-guard (JEP-284 first cut), but a wall-clock fallback there
is the WRONG fail direction: it lets the orphaned loop keep making valid-looking
session/trading decisions on a dead lifecycle. The real fix is fail-closed at the
single control-loop chokepoint: a non-finite clock means the lifecycle is invalid ->
place NO new orders (no quote, no hedge, no reconcile, no heartbeat stamp). But the
orphaned loop's connectors stay LIVE, so a resting post-only maker can still fill and
strand a never-fired pending hedge -> an unbounded naked perp leg (adversarial finding,
HIGH). So the dead-clock path is not inert: it PULLS the resting ladder off the book
(``_cancel_all_maker`` is clock-free) before skipping, so no further naked fills occur.
The calendar crash-guard stays as a defense net behind this.

These tests drive the *real* ``control_task`` with every collaborator stubbed, so
only its control-flow is exercised (mirrors test_control_task_hedge_isolation).
"""
import asyncio
from decimal import Decimal
from typing import Dict, List, Optional

import pytest

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.models.base import RunnableStatus

_NONFINITE = (float("nan"), float("inf"), float("-inf"))


class _FakeStrategy:
    def __init__(self, ts):
        self.current_timestamp = ts


class _ClockHarness(CrossVenueHedgedExecutorBase):
    """Real-``control_task`` harness with an injectable strategy clock.

    Mirrors test_control_task_hedge_isolation._IsoHarness but also sets ``_strategy``
    so the JEP-284 clock guard sees a concrete ``current_timestamp``.
    """

    def __init__(self, *, clock, status: RunnableStatus = RunnableStatus.RUNNING):
        self._status = status
        self._strategy = _FakeStrategy(clock)
        self.hedge_calls = 0
        self.cancel_calls = 0
        self.reconcile_calls = 0

    # --- control_task collaborators ---
    async def _seed_inventory_from_connector(self) -> None:
        pass

    def _evaluate_session_state(self) -> None:
        pass

    def _seed_pending(self) -> bool:
        return False

    def _evaluate_ws_staleness(self) -> None:
        pass

    def _reconcile_maker(self) -> None:
        self.reconcile_calls += 1

    def _cancel_all_maker(self) -> None:
        self.cancel_calls += 1

    def _process_hedges(self) -> None:
        self.hedge_calls += 1

    # --- SHUTTING_DOWN collaborators (only reached if the guard does NOT skip) ---
    def _has_open_orders(self) -> bool:
        return False

    def _unhedged_base(self) -> Decimal:
        return Decimal("0")

    def stop(self) -> None:
        self._status = RunnableStatus.TERMINATED

    # --- abstract-method stubs (needed only to instantiate) ---
    def _gates_open(self) -> bool:
        return True

    def _compute_targets(self) -> List:
        return []

    def _should_reprice(self, targets: List) -> bool:
        return False

    def _place_targets(self, targets: List) -> None:
        pass

    def _size_hedge(self, pending_base) -> Optional[Dict]:
        return None

    def _maker_balance_candidate(self):
        return None


@pytest.mark.parametrize("clock", _NONFINITE)
def test_nonfinite_clock_pulls_makers_no_hedge_no_reconcile(clock):
    """A dead (NaN/inf) clock on a RUNNING executor must place NO new orders (no hedge,
    no reconcile) but MUST pull the resting ladder off the book so a still-live connector
    cannot keep filling resting makers into an unhedged naked leg (adversarial HIGH)."""
    h = _ClockHarness(clock=clock)
    asyncio.run(h.control_task())
    assert h.cancel_calls == 1, "fail-closed: must cancel resting makers to stop naked fills"
    assert h.hedge_calls == 0, "fail-closed: a dead clock must place NO hedge"
    assert h.reconcile_calls == 0, "fail-closed: no reconcile/new quotes on a dead clock"


@pytest.mark.parametrize("clock", _NONFINITE)
def test_nonfinite_clock_never_raises(clock):
    """The orphaned loop must skip cleanly, not crash (the original NaN loop)."""
    h = _ClockHarness(clock=clock)
    try:
        asyncio.run(h.control_task())
    except Exception as exc:  # noqa: BLE001 - the whole point is that it must NOT raise
        pytest.fail(f"control_task raised on a non-finite clock: {exc!r}")


@pytest.mark.parametrize("clock", _NONFINITE)
def test_nonfinite_clock_pulls_makers_in_shutting_down(clock):
    """SHUTTING_DOWN also reads current_timestamp (flatten timing) -> fail-closed there too:
    pull the resting ladder, fire no shutdown hedge. The residual flatten is abandoned (operator
    runbook: a dead-clock stop during flatten needs manual neutralization)."""
    h = _ClockHarness(clock=clock, status=RunnableStatus.SHUTTING_DOWN)
    asyncio.run(h.control_task())
    assert h.cancel_calls == 1, "fail-closed: pull resting makers even in SHUTTING_DOWN"
    assert h.hedge_calls == 0, "fail-closed: no shutdown hedge on a dead clock"


def test_finite_clock_runs_normal_running_path_and_hedges_once():
    """A live clock must NOT be skipped: the normal RUNNING path still hedges once."""
    h = _ClockHarness(clock=1_700_000_000.0)
    asyncio.run(h.control_task())
    assert h.hedge_calls == 1, "a live clock must run the normal RUNNING path"
    assert h.reconcile_calls == 1
    assert h.cancel_calls == 0


def test_missing_strategy_clock_proceeds_unchanged():
    """A harness with no clock info (current_timestamp -> None) must NOT be treated as
    dead -- otherwise the guard would break every existing control_task test that omits
    _strategy. Only a numeric non-finite value is fail-closed."""
    h = _ClockHarness(clock=None)
    asyncio.run(h.control_task())
    assert h.hedge_calls == 1
    assert h.reconcile_calls == 1
