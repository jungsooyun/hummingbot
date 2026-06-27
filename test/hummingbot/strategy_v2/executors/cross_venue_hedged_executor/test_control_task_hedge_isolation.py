"""JEP-233 — control_task hedge exception isolation.

Safety invariant: while the executor is RUNNING, ``_process_hedges()`` must run
every tick even if the maker-side path (inventory seed / session-state /
ws-staleness evaluation / ``_reconcile_maker``) raises. A persistent maker-side
exception that skipped hedging would silently leave maker fills unhedged -> a
naked perp leg, with no kill-switch covering an exception-driven stall (the
control loop merely logs and respins at 1 Hz).

These tests drive the *real* ``control_task`` with every collaborator stubbed, so
only the control-flow / exception-handling of ``control_task`` itself is exercised.
"""
import asyncio
from typing import Dict, List, Optional

import pytest

from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.models.base import RunnableStatus


class _IsoHarness(CrossVenueHedgedExecutorBase):
    def __init__(self, *, raise_in: Optional[str] = None, seed_pending: bool = False, gates_open: bool = True):
        self._status = RunnableStatus.RUNNING
        self._raise_in = raise_in
        self._seed_pending_flag = seed_pending
        self._gates_open_flag = gates_open
        self.hedge_calls = 0
        self.cancel_calls = 0

    def _maybe_raise(self, where: str) -> None:
        if self._raise_in == where:
            raise RuntimeError(f"boom-{where}")

    # --- control_task collaborators (the surface under test orchestrates these) ---
    async def _seed_inventory_from_connector(self) -> None:
        self._maybe_raise("seed")

    def _evaluate_session_state(self) -> None:
        self._maybe_raise("session")

    def _seed_pending(self) -> bool:
        return self._seed_pending_flag

    def _evaluate_ws_staleness(self) -> None:
        self._maybe_raise("staleness")

    def _reconcile_maker(self) -> None:
        self._maybe_raise("reconcile")

    def _cancel_all_maker(self) -> None:
        self.cancel_calls += 1

    def _process_hedges(self) -> None:
        self.hedge_calls += 1

    # --- abstract-method stubs (needed only to instantiate; not exercised here) ---
    def _gates_open(self) -> bool:
        return self._gates_open_flag

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


@pytest.mark.parametrize("where", ["seed", "session", "staleness", "reconcile"])
def test_hedge_runs_even_when_maker_side_raises(where):
    h = _IsoHarness(raise_in=where)
    try:
        asyncio.run(h.control_task())
    except Exception as exc:  # noqa: BLE001 - the whole point is that it must NOT escape
        pytest.fail(f"control_task propagated {exc!r}; maker-side exception must be isolated")
    assert h.hedge_calls == 1, f"hedge skipped when '{where}' raised -> naked perp"


def test_happy_path_hedges_exactly_once():
    h = _IsoHarness()
    asyncio.run(h.control_task())
    assert h.hedge_calls == 1
    assert h.cancel_calls == 0


def test_gates_closed_cancels_makers_and_still_hedges_once():
    h = _IsoHarness(gates_open=False)
    asyncio.run(h.control_task())
    assert h.cancel_calls == 1
    assert h.hedge_calls == 1


def test_seed_pending_cancels_makers_and_still_hedges_once():
    h = _IsoHarness(seed_pending=True)
    asyncio.run(h.control_task())
    assert h.cancel_calls == 1
    assert h.hedge_calls == 1
