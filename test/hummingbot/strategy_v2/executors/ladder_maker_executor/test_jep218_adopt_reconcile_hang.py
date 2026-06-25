"""JEP-218 repro: two cross_venue_hedged controllers sharing ONE KIS connector +
ONE AsyncThrottler in one event loop wedge the strategy clock the instant after
``JEP-210 adopt seed complete`` is logged -- but only when ``max_inventory`` is large
enough that the InventoryGate OPENS (gate-open reconcile path). With a small
``max_inventory`` (gate closed -> early return before reconcile) there is no hang.

This is the TDD "create the failing test" pass: REPRODUCE + LOCATE only, no fix.

Repro shape (adapted from test_inventory_seed.py's ``_SeedHarness``):
  * adopt_existing_inventory=True, two_sided=True
  * seed perp net -28.769 (HL short) + spot net +31 (KIS long)
    -> unhedged signed +2.231, paired_oi 28.769
  * a REAL shared ``AsyncThrottler`` built from KIS ``RATE_LIMITS`` backs the hedge
    connector; every KIS REST hop raises ``IOError(... EGW00215 ...)`` AFTER passing
    through the throttler's ``acquire()`` capacity spin
  * BOTH a SMSN and a SKHX executor share that one throttler + one hedge connector
  * gate-open variant  (max_inventory=6, |2.231| < 6 -> gate OPEN ) -> reconcile path
  * gate-closed variant (max_inventory=2, |2.231| > 2 -> gate CLOSED) -> early return

Assertion: ``asyncio.wait_for(executor.control_task(), timeout=3)`` must NOT raise
``TimeoutError``. On timeout we dump every parked task's stack to locate the wedge.
"""

import asyncio
import io
import traceback
from decimal import Decimal
from types import SimpleNamespace

import pytest

from hummingbot.connector.exchange.kis import kis_constants as KIS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import PositionMode, PositionSide, TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)

ZERO = Decimal("0")

# Live JEP-218 inventory: HL perp net short 28.769, KIS spot long 31 shares.
PERP_SHORT = Decimal("28.769")
SPOT_LONG = Decimal("31")
# share_per_unit converts spot shares -> maker-base units. With share_per_unit=1 the
# hedge-base is 31 and unhedged signed = maker_sell(28.769) vs hedge_buy(31)... we instead
# pick share_per_unit so the seeded |unhedged| lands at ~2.231 to mirror the live state.
SHARE_PER_UNIT = Decimal("1")


# --------------------------------------------------------------------------- throttler
def _kis_throttler() -> AsyncThrottler:
    """A REAL shared AsyncThrottler built from the KIS rate-limit table."""
    return AsyncThrottler(KIS.RATE_LIMITS)


class _StuckKisConnector:
    """Stub KIS hedge connector whose REST hops all pass through a REAL shared throttler.

    This is the leading-hypothesis surface: under sustained EGW00215 from BOTH executors,
    does the shared ``acquire()`` capacity spin + shared lock wedge the KIS REST tasks?

    The held inventory IS adoptable (31 shares, a poll confirms it) -- we WANT the seed to
    reach "adopt seed complete" so the gate-open reconcile path is genuinely exercised.
    EGW00215 pressure is modelled as a saturating background task (``saturate``) that keeps
    the ORDER/BALANCE buckets pinned full so any further ``acquire()`` spins.
    """

    def __init__(self, throttler: AsyncThrottler):
        self.ready = True
        self._throttler = throttler
        self.api_calls = 0
        self._held = SPOT_LONG  # 31 shares actually held
        self._confirmed = False

    # --- balance cache read (sync, used by _read_spot_balance_base) ---
    def get_balance(self, asset):
        # Reads the cache; reflects the held 31 (a prior/confirmed poll populated it).
        return self._held

    # --- the REST hop the seed actively drives (async). Routes through the REAL
    #     shared throttler exactly like KisExchange._update_balances -> _api_get.
    #     Succeeds (so the seed can adopt) but only after winning throttler capacity. ---
    async def _update_balances(self):
        async with self._throttler.execute_task(limit_id=KIS.DOMESTIC_STOCK_BALANCE_PATH):
            self.api_calls += 1
            self._confirmed = True
            return None  # success leaves the held cache in place -> adopt

    async def get_open_orders(self, pair):
        # No resting orders -> seed proceeds past the resting-order guard.
        return []

    # price accessors (used by some fair-price paths; harmless defaults)
    def get_price_by_type(self, pair, price_type):
        return Decimal("1000")

    async def saturate_balance_bucket(self, hold: "asyncio.Event"):
        """Pin the BALANCE bucket to its limit and HOLD it, modelling the sibling
        controller's sustained EGW00215 burst. While held, any sibling ``acquire()`` on
        the same bucket must spin in ``async_request_context_base.acquire``."""
        limit = self._throttler.get_related_limits(KIS.DOMESTIC_STOCK_BALANCE_PATH)[0].limit
        for _ in range(int(limit)):
            async with self._throttler.execute_task(limit_id=KIS.DOMESTIC_STOCK_BALANCE_PATH):
                pass
        # bucket now full for the time_interval window; keep it full until released
        await hold.wait()


class _HangHarness(CrossVenueHedgedExecutorBase):
    """Minimal real-control_task harness. Mirrors test_inventory_seed._SeedHarness but
    drives the REAL ``control_task`` + REAL ``_seed_inventory_from_connector`` and lets
    the gate-open/closed branch be toggled to model max_inventory=6 vs 2."""

    def __init__(self, throttler: AsyncThrottler, hedge_connector, gate_open: bool):
        self.config = SimpleNamespace(adopt_existing_inventory=True, two_sided=True)
        self.maker_connector = "hyperliquid_perpetual"
        self.maker_trading_pair = "SMSN-USD"
        self.hedge_connector = "kis"
        self.hedge_trading_pair = "005930-KRW"
        self.entry_side = TradeType.SELL
        self.hedge_side = TradeType.BUY
        self.maker_orders = {}
        self.hedge_orders = {}
        self._maker_buy_base = ZERO
        self._maker_sell_base = ZERO
        self._hedge_buy_base = ZERO
        self._hedge_sell_base = ZERO
        self._perp_cash = ZERO
        self._spot_cash = ZERO
        self._pending_hedge_signed = ZERO
        self._maker_executed_base = ZERO
        self._maker_executed_quote = ZERO
        self._hedge_executed_base = ZERO
        self._hedge_executed_quote = ZERO
        self._maker_fees_quote = ZERO
        self._hedge_fees_quote = ZERO
        self._open_edge_base = ZERO
        self._open_edge_notional_bps = ZERO
        self._open_edge_vwap = ZERO
        self._hedge_order_side = {}
        self._maker_placed_edge_bps = {}
        self.share_per_unit = SHARE_PER_UNIT
        self._gate_open = gate_open

        # RUNNING so control_task takes the live branch.
        from hummingbot.strategy_v2.models.base import RunnableStatus
        self._status = RunnableStatus.RUNNING

        # perp short 28.769 @ entry 100 -- the maker (HL) connector snapshot.
        maker = SimpleNamespace(
            ready=True,
            position_mode=PositionMode.ONEWAY,
            account_positions={
                "short": SimpleNamespace(
                    trading_pair=self.maker_trading_pair,
                    position_side=PositionSide.SHORT,
                    amount=PERP_SHORT,
                    entry_price=Decimal("100"),
                )
            },
        )
        self.connectors = {
            self.maker_connector: maker,
            self.hedge_connector: hedge_connector,
        }
        # KIS spot holding 31 shares -> drives the seed's spot read once a poll "confirms".
        # The stubbed _update_balances never confirms (EGW00215), so to reach the
        # "adopt seed complete" log line we let get_balance reflect the held 31 AND make a
        # poll succeed (separate fixture below). Here the default connector keeps it empty.

    # --- collaborators control_task / seed touch that the harness must satisfy ---
    @property
    def status(self):
        return self._status

    def _gates_open(self):
        if getattr(self, "_seed_fail_closed", False):
            return False
        return self._gate_open

    def _evaluate_ws_staleness(self):
        return None

    def _cancel_all_maker(self):
        return None

    def _reconcile_maker(self):
        # The gate-open path. In the real ladder this diffs/places maker orders; here we
        # exercise whether reaching this branch (after a successful adopt seed) wedges.
        return None

    def _process_hedges(self):
        return None

    # seed hooks (mirror _SeedHarness)
    def _compute_targets(self):
        return []

    def _should_reprice(self, targets):
        return False

    def _place_targets(self, targets):
        return None

    def _size_hedge(self, pending_base):
        return None

    def _maker_balance_candidate(self):
        return None

    def _hedge_base_to_maker_base(self, amount: Decimal) -> Decimal:
        return amount / getattr(self, "share_per_unit", Decimal("1"))


def _make_pair(gate_open: bool):
    """Build SMSN + SKHX executors sharing ONE throttler + ONE hedge connector, with a
    hedge connector that adopts (so we PASS the seed and reach the gate-open reconcile)."""
    throttler = _kis_throttler()
    hedge = _StuckKisConnector(throttler)

    smsn = _HangHarness(throttler, hedge, gate_open=gate_open)
    skhx = _HangHarness(throttler, hedge, gate_open=gate_open)
    skhx.maker_trading_pair = "SKHX-USD"
    skhx.hedge_trading_pair = "000660-KRW"
    # SKHX maker snapshot (its own short) on a SEPARATE maker connector key so both share
    # only the hedge (KIS) connector + throttler, as in production.
    skhx.maker_connector = "hyperliquid_perpetual_skhx"
    skhx.connectors = {
        skhx.maker_connector: smsn.connectors[smsn.maker_connector],
        skhx.hedge_connector: hedge,
    }

    # Seed knobs. op_timeout is the REAL 10s bound the seed places on each KIS REST hop
    # (so wait_for does not trivially short-circuit the throttler spin); readiness/grace are
    # collapsed so the only thing that can stall is the throttler ``acquire()`` spin itself.
    for h in (smsn, skhx):
        h._seed_readiness_timeout = 0
        h._seed_grace_seconds = 5
        h._seed_op_timeout = 10.0
        h._seed_balance_refresh_interval = 0.0  # always allowed to poll

    return smsn, skhx, hedge, throttler


def _dump_parked_tasks() -> str:
    buf = io.StringIO()
    try:
        tasks = asyncio.all_tasks()
    except RuntimeError:
        return "(no running loop to inspect)"
    for t in tasks:
        buf.write(f"\n=== task {t!r} ===\n")
        stack = t.get_stack()
        if not stack:
            buf.write("  (no python stack -- parked in C / awaiting)\n")
            continue
        buf.write("".join(traceback.format_list(traceback.extract_stack(stack[-1]))))
        for f in stack:
            buf.write(f"  {f.f_code.co_filename}:{f.f_lineno} in {f.f_code.co_name}\n")
    return buf.getvalue()


async def _drive(executor, other, hedge, *, n_ticks: int, timeout: float, saturate: bool):
    """Drive ``executor.control_task`` n_ticks times while ``other`` also ticks
    concurrently (both sharing the throttler), under a wall-clock guard.

    When ``saturate`` is set, a background task pins the shared BALANCE bucket full for the
    first part of the run (modelling the sibling controller's sustained EGW00215 burst), so
    the seed's hedge-balance poll must spin in ``acquire()``. The hold is released partway so
    we can observe whether the spin EVER frees (permanent wedge) or merely delays (recovers).
    """
    hold = asyncio.Event()
    sat_task = None
    if saturate:
        sat_task = asyncio.ensure_future(hedge.saturate_balance_bucket(hold))
        await asyncio.sleep(0)  # let the saturator pin the bucket before the controllers run

    async def _run():
        for i in range(n_ticks):
            if saturate and i == n_ticks // 2:
                hold.set()  # release the EGW00215 pressure mid-run
            await asyncio.gather(executor.control_task(), other.control_task())

    try:
        await asyncio.wait_for(_run(), timeout=timeout)
    finally:
        hold.set()
        if sat_task is not None:
            sat_task.cancel()


def _run_variant(gate_open: bool, *, n_ticks: int = 6, timeout: float = 3.0, saturate: bool = True):
    smsn, skhx, hedge, throttler = _make_pair(gate_open)
    result = {"timed_out": False, "dump": "", "smsn_adopted": None, "api_calls": None}

    async def _main():
        try:
            await _drive(smsn, skhx, hedge, n_ticks=n_ticks, timeout=timeout, saturate=saturate)
        except asyncio.TimeoutError:
            result["timed_out"] = True
            result["dump"] = _dump_parked_tasks()
        result["smsn_adopted"] = getattr(smsn, "_seed_adopted", None)
        result["api_calls"] = hedge.api_calls

    asyncio.run(_main())
    return result


# --------------------------------------------------------------------------- tests
def test_gate_closed_variant_completes_cleanly():
    """max_inventory=2 analogue: gate CLOSED -> control_task early-returns before
    reconcile. This MUST complete without timing out (the live no-hang baseline)."""
    res = _run_variant(gate_open=False)
    assert res["timed_out"] is False, (
        "gate-closed variant unexpectedly hung; dump:\n" + res["dump"]
    )


def test_gate_open_variant_does_not_hang():
    """max_inventory=6 analogue: gate OPEN -> control_task runs the reconcile path right
    after 'adopt seed complete'. JEP-218 hypothesis: this wedges under shared-throttler
    sustained EGW00215. If it reproduces, this test TIMES OUT (the failing test)."""
    res = _run_variant(gate_open=True)
    # Guard against a false negative: confirm the suspect path was actually exercised
    # (the seed adopted AND a KIS REST hop went through the real shared throttler).
    if not res["timed_out"]:
        assert res["smsn_adopted"] is True, (
            "test did NOT exercise the post-adopt gate-open path "
            f"(smsn_adopted={res['smsn_adopted']}, kis_api_calls={res['api_calls']}); "
            "the seed never completed, so a 'no hang' result is meaningless."
        )
        assert res["api_calls"] > 0, "KIS REST hop never fired through the shared throttler"
    assert res["timed_out"] is False, (
        "JEP-218 REPRODUCED: gate-open reconcile path hung after adopt seed complete "
        f"(smsn_adopted={res['smsn_adopted']}, kis_api_calls={res['api_calls']}).\n"
        "Parked task stacks:\n" + res["dump"]
    )
