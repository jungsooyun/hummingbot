import asyncio
from decimal import Decimal
from types import SimpleNamespace

from hummingbot.core.data_type.common import PositionMode, PositionSide, TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import LadderMakerExecutorConfig
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import LadderRungConfig


ZERO = Decimal("0")


class _SeedHarness(CrossVenueHedgedExecutorBase):
    def __init__(self, adopt: bool = False):
        self.config = SimpleNamespace(adopt_existing_inventory=adopt, two_sided=True)
        self.maker_connector = "hyperliquid_perpetual"
        self.maker_trading_pair = "EWY-USD"
        self.hedge_connector = "kis"
        self.hedge_trading_pair = "069500-KRW"
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
        self.connectors = {
            self.maker_connector: SimpleNamespace(ready=True, position_mode=PositionMode.ONEWAY, account_positions={}),
            self.hedge_connector: SimpleNamespace(ready=True),
        }

    def _gates_open(self):
        return True

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


def _ladder_config(**overrides):
    values = dict(
        timestamp=1.0,
        maker_market=ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="EWY-USD"),
        hedge_market=ConnectorPair(connector_name="kis", trading_pair="069500-KRW"),
        entry_side=TradeType.SELL,
        total_size_cap=Decimal("1"),
        rungs=[LadderRungConfig(edge_bps=Decimal("10"), size=Decimal("1"))],
        maker_tick=Decimal("0.01"),
        hedge_tick=Decimal("1"),
    )
    values.update(overrides)
    return LadderMakerExecutorConfig(**values)


def test_adopt_existing_inventory_defaults_false():
    assert _ladder_config().adopt_existing_inventory is False
    assert _ladder_config(adopt_existing_inventory=True).adopt_existing_inventory is True


def test_seed_flag_off_returns_before_touching_ledgers():
    h = _SeedHarness(adopt=False)

    asyncio.run(h._seed_inventory_from_connector())

    assert h._maker_buy_base == ZERO
    assert h._maker_sell_base == ZERO
    assert h._hedge_buy_base == ZERO
    assert h._hedge_sell_base == ZERO
    assert h._perp_cash == ZERO
    assert h._spot_cash == ZERO
    assert getattr(h, "_seed_adopted", False) is False
    assert getattr(h, "_seed_fail_closed", False) is False


def test_seed_flag_on_empty_after_fresh_fail_closes_without_mutating_ledgers():
    h = _SeedHarness(adopt=True)
    h.connectors[h.hedge_connector].get_balance = lambda asset: ZERO
    h._seed_readiness_timeout = 0
    h._seed_grace_seconds = 0  # JEP-210: collapse the retry grace so an empty seed fail-closes at once

    asyncio.run(h._seed_inventory_from_connector())

    assert h._maker_buy_base == ZERO
    assert h._maker_sell_base == ZERO
    assert h._hedge_buy_base == ZERO
    assert h._hedge_sell_base == ZERO
    assert h._perp_cash == ZERO
    assert h._spot_cash == ZERO
    assert getattr(h, "_seed_adopted", False) is False
    assert h._seed_fail_closed is True


def test_read_perp_position_signed_reads_oneway_short_long_and_flat():
    h = _SeedHarness(adopt=True)
    maker = h.connectors[h.maker_connector]

    maker.account_positions = {
        "short": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.SHORT,
            amount=Decimal("2"),
            entry_price=Decimal("101"),
        )
    }
    assert h._read_perp_position_signed() == (Decimal("-2"), Decimal("101"))

    maker.account_positions = {
        "long": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.LONG,
            amount=Decimal("3"),
            entry_price=Decimal("99"),
        )
    }
    assert h._read_perp_position_signed() == (Decimal("3"), Decimal("99"))

    maker.account_positions = {}
    assert h._read_perp_position_signed() == (ZERO, ZERO)


def test_read_perp_position_signed_uses_signed_amount_in_oneway():
    h = _SeedHarness(adopt=True)
    h.connectors[h.maker_connector].account_positions = {
        h.maker_trading_pair: SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.BOTH,
            amount=Decimal("-4"),
            entry_price=Decimal("102"),
        )
    }

    assert h._read_perp_position_signed() == (Decimal("-4"), Decimal("102"))


def test_read_perp_position_signed_fail_closes_on_hedge_mode():
    h = _SeedHarness(adopt=True)
    h.connectors[h.maker_connector].position_mode = PositionMode.HEDGE

    assert h._read_perp_position_signed() == (ZERO, ZERO)
    assert h._seed_fail_closed is True


def test_read_spot_balance_base_clamps_raw_shares_to_non_negative():
    h = _SeedHarness(adopt=True)
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("7") if asset == "069500" else ZERO
    assert h._read_spot_balance_base() == Decimal("7")

    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("-3")
    assert h._read_spot_balance_base() == ZERO


def test_apply_seed_writes_direction_ledgers_and_converts_spot_shares():
    h = _SeedHarness(adopt=True)
    h.share_per_unit = Decimal("10")

    h._apply_seed(Decimal("-1"), Decimal("10"), Decimal("101"))

    assert h._maker_sell_base == Decimal("1")
    assert h._maker_buy_base == ZERO
    assert h._hedge_buy_base == Decimal("1")
    assert h._hedge_sell_base == ZERO
    assert h._seed_perp_basis_quote == Decimal("101")
    assert h._perp_cash == ZERO
    assert h._spot_cash == ZERO
    assert h.get_net_pnl_quote() == ZERO


def test_apply_seed_handles_long_perp_and_audit_basis_without_cash_pnl():
    h = _SeedHarness(adopt=True)

    h._apply_seed(Decimal("2"), ZERO, Decimal("99"))

    assert h._maker_buy_base == Decimal("2")
    assert h._maker_sell_base == ZERO
    assert h._hedge_buy_base == ZERO
    assert h._seed_perp_basis_quote == Decimal("198")
    assert h._perp_cash == ZERO
    assert h._spot_cash == ZERO
    assert h.get_net_pnl_quote() == ZERO


def test_await_connector_readiness_requires_ready_connectors_and_fresh_snapshots():
    h = _SeedHarness(adopt=True)
    # Empty account_positions is the startup race: the accessor exists before the
    # perp snapshot has completed, so freshness requires a populated position set.
    h.connectors[h.maker_connector].account_positions = {
        "short": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.SHORT,
            amount=Decimal("1"),
            entry_price=Decimal("100"),
        )
    }
    h.connectors[h.hedge_connector].get_balance = lambda asset: ZERO

    assert asyncio.run(h._await_connector_readiness()) is True

    h.connectors[h.maker_connector].ready = False
    assert asyncio.run(h._await_connector_readiness(timeout_s=0, interval_s=0)) is False


def test_await_connector_readiness_requires_snapshot_accessors():
    h = _SeedHarness(adopt=True)

    assert asyncio.run(h._await_connector_readiness(timeout_s=0, interval_s=0)) is False

    # Empty account_positions is not fresh; populate it before expecting readiness.
    h.connectors[h.maker_connector].account_positions = {
        "short": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.SHORT,
            amount=Decimal("1"),
            entry_price=Decimal("100"),
        )
    }
    h.connectors[h.hedge_connector].get_balance = lambda asset: ZERO
    assert asyncio.run(h._await_connector_readiness(timeout_s=0, interval_s=0)) is True


def test_has_resting_orders_true_when_hedge_connector_reports_open_orders():
    h = _SeedHarness(adopt=True)

    async def _open_orders(pair):
        return [SimpleNamespace(client_order_id="resting")]

    h.connectors[h.hedge_connector].get_open_orders = _open_orders

    assert asyncio.run(h._has_resting_orders()) is True


def test_has_resting_orders_false_when_empty_or_connector_lacks_accessor():
    h = _SeedHarness(adopt=True)

    async def _none(pair):
        return []

    h.connectors[h.hedge_connector].get_open_orders = _none

    assert asyncio.run(h._has_resting_orders()) is False


def test_has_resting_orders_fail_closed_when_accessor_errors():
    h = _SeedHarness(adopt=True)

    async def _raises(pair):
        raise RuntimeError("tracker unavailable")

    h.connectors[h.hedge_connector].get_open_orders = _raises

    assert asyncio.run(h._has_resting_orders()) is True


def test_has_resting_orders_false_for_zombie_zero_remaining():
    # Defence in depth (2026-06-29): even if a connector mis-reports a terminal/rejected order
    # as open (e.g. a KIS 09:00-auction-rejected row the daily-ccld feed still lists), a record
    # with remaining_amount == 0 can never fill or shift inventory and must NOT block adopt-seed.
    h = _SeedHarness(adopt=True)

    async def _zombie(pair):
        return [SimpleNamespace(client_order_id="zombie", remaining_amount=Decimal("0"))]

    h.connectors[h.hedge_connector].get_open_orders = _zombie

    assert asyncio.run(h._has_resting_orders()) is False


def test_has_resting_orders_true_for_working_order_with_remaining():
    h = _SeedHarness(adopt=True)

    async def _working(pair):
        return [SimpleNamespace(client_order_id="working", remaining_amount=Decimal("3"))]

    h.connectors[h.hedge_connector].get_open_orders = _working

    assert asyncio.run(h._has_resting_orders()) is True


def test_has_resting_orders_false_for_dict_zombie_zero_remaining():
    # Codex finding 3: a connector/adapter returning mapping-shaped records (dicts) with
    # remaining_amount == 0 must also be treated as terminal, not fail-safe-resting.
    h = _SeedHarness(adopt=True)

    async def _dict_zombie(pair):
        return [{"client_order_id": "zombie", "remaining_amount": Decimal("0")}]

    h.connectors[h.hedge_connector].get_open_orders = _dict_zombie

    assert asyncio.run(h._has_resting_orders()) is False


def test_has_resting_orders_true_for_dict_working_order():
    h = _SeedHarness(adopt=True)

    async def _dict_working(pair):
        return [{"client_order_id": "working", "remaining_amount": Decimal("2")}]

    h.connectors[h.hedge_connector].get_open_orders = _dict_working

    assert asyncio.run(h._has_resting_orders()) is True


def test_seed_fail_close_records_retry_reason():
    # The fail-close log was a misleading catch-all ("perp/spot snapshot never both landed")
    # even when the real cause was a resting/zombie order. _note_seed_retry now records the
    # actual blocking reason so operators are not misdirected (2026-06-29 SMSN incident).
    h = _SeedHarness(adopt=True)
    h._seed_grace_seconds = 0.0  # expire the grace window immediately
    h._note_seed_retry("resting_orders")
    assert h._seed_fail_closed is True
    assert h._seed_last_retry_reason == "resting_orders"


def test_seed_full_flow_applies_snapshot_once_and_is_idempotent():
    h = _SeedHarness(adopt=True)
    maker = h.connectors[h.maker_connector]
    maker.account_positions = {
        "short": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.SHORT,
            amount=Decimal("1"),
            entry_price=Decimal("101"),
        )
    }
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("1")

    asyncio.run(h._seed_inventory_from_connector())

    assert h._seed_adopted is True
    assert h._seed_fail_closed is False
    assert h._maker_sell_base == Decimal("1")
    assert h._hedge_buy_base == Decimal("1")
    assert h._seed_perp_basis_quote == Decimal("101")

    maker.account_positions["short"].amount = Decimal("5")
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("5")
    asyncio.run(h._seed_inventory_from_connector())

    assert h._maker_sell_base == Decimal("1")
    assert h._hedge_buy_base == Decimal("1")


def test_seed_fail_closes_on_not_ready_timeout_and_resting_orders():
    h = _SeedHarness(adopt=True)
    h.connectors[h.maker_connector].ready = False
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("1")
    h._seed_readiness_timeout = 0
    h._seed_grace_seconds = 0  # JEP-210: no retry grace -> fail-close on the first not-ready miss

    asyncio.run(h._seed_inventory_from_connector())

    assert h._seed_fail_closed is True
    assert getattr(h, "_seed_adopted", False) is False
    assert h._maker_sell_base == ZERO

    h = _SeedHarness(adopt=True)
    h.connectors[h.maker_connector].account_positions = {  # fresh -> readiness passes, reach resting check
        "short": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.SHORT,
            amount=Decimal("1"),
            entry_price=Decimal("101"),
        )
    }
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("1")
    h._seed_readiness_timeout = 0
    h._seed_grace_seconds = 0

    async def _open_orders(pair):
        return [SimpleNamespace(client_order_id="resting")]

    h.connectors[h.hedge_connector].get_open_orders = _open_orders
    asyncio.run(h._seed_inventory_from_connector())

    assert h._seed_fail_closed is True
    assert getattr(h, "_seed_adopted", False) is False
    assert h._hedge_buy_base == ZERO


def test_seed_fail_closes_on_hedge_position_mode():
    h = _SeedHarness(adopt=True)
    h.connectors[h.maker_connector].position_mode = PositionMode.HEDGE
    h.connectors[h.maker_connector].account_positions = {  # fresh -> readiness passes, reach the HEDGE check
        "x": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.SHORT,
            amount=Decimal("1"),
            entry_price=Decimal("100"),
        )
    }
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("1")
    h._seed_readiness_timeout = 0
    # HEDGE mode is an unrecoverable config error -> immediate permanent fail-close (no retry grace).

    asyncio.run(h._seed_inventory_from_connector())

    assert h._seed_fail_closed is True
    assert getattr(h, "_seed_adopted", False) is False
    assert h._maker_sell_base == ZERO
    assert h._hedge_buy_base == ZERO


def test_seed_full_flow_is_race_safe_for_concurrent_calls():
    h = _SeedHarness(adopt=True)
    h.connectors[h.maker_connector].account_positions = {
        "short": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.SHORT,
            amount=Decimal("1"),
            entry_price=Decimal("101"),
        )
    }
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("1")

    async def _run_twice():
        await asyncio.gather(h._seed_inventory_from_connector(), h._seed_inventory_from_connector())

    asyncio.run(_run_twice())

    assert h._seed_adopted is True
    assert h._seed_fail_closed is False
    assert h._maker_sell_base == Decimal("1")
    assert h._hedge_buy_base == Decimal("1")


def test_ladder_gates_close_when_seed_fail_closed_without_other_state():
    h = LadderMakerExecutor.__new__(LadderMakerExecutor)
    h._seed_fail_closed = True

    assert h._gates_open() is False


def test_seed_pending_true_while_adopt_seed_in_progress():
    # JEP-210: control_task suppresses quoting (every subclass) while _seed_pending() is true,
    # so no opens are placed before the held inventory is recognized.
    h = _SeedHarness(adopt=True)
    h._seed_adopted = False
    h._seed_fail_closed = False

    assert h._seed_pending() is True


def test_seed_pending_false_when_adopted_failclosed_or_adopt_off():
    h = _SeedHarness(adopt=True)
    h._seed_adopted = True
    h._seed_fail_closed = False
    assert h._seed_pending() is False

    h._seed_adopted = False
    h._seed_fail_closed = True
    assert h._seed_pending() is False

    h = _SeedHarness(adopt=False)
    h._seed_adopted = False
    h._seed_fail_closed = False
    assert h._seed_pending() is False


def test_seed_hedge_mode_fail_closes_immediately_even_with_cold_snapshots():
    # JEP-210 (review F3): HEDGE position mode is unrecoverable -> fail-close on the FIRST tick,
    # before the readiness wait / retry grace, even when snapshots are still cold (empty positions).
    h = _SeedHarness(adopt=True)
    h.connectors[h.maker_connector].position_mode = PositionMode.HEDGE
    h.connectors[h.maker_connector].account_positions = {}
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("1")

    asyncio.run(h._seed_inventory_from_connector())

    assert h._seed_fail_closed is True
    assert getattr(h, "_seed_adopted", False) is False


def test_seed_retries_when_update_positions_hangs_without_wedging():
    # JEP-210 (review F2): a hung _update_positions REST call must time out and retry, not wedge
    # the seed (which would leave _seed_adopting stuck and block the control loop forever).
    h = _SeedHarness(adopt=True)
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("1")
    h._seed_op_timeout = 0.01

    async def _hang(*args, **kwargs):
        await asyncio.sleep(5)

    h.connectors[h.maker_connector]._update_positions = _hang

    asyncio.run(h._seed_inventory_from_connector())

    assert h._seed_fail_closed is False
    assert getattr(h, "_seed_adopted", False) is False
    assert h._seed_adopting is False


def test_seed_retries_on_transient_readiness_then_adopts():
    # JEP-210: a cold-boot snapshot race (connector not ready / positions not yet populated)
    # must NOT permanently fail-close adoption. The seed retries on the next control tick and
    # adopts once the snapshot becomes fresh.
    h = _SeedHarness(adopt=True)
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("1")
    h._seed_readiness_timeout = 0

    # Tick 1: maker not ready yet (startup race) -> retry, NOT a permanent fail-close.
    h.connectors[h.maker_connector].ready = False
    asyncio.run(h._seed_inventory_from_connector())
    assert h._seed_fail_closed is False
    assert getattr(h, "_seed_adopted", False) is False
    assert h._maker_sell_base == ZERO

    # Tick 2: snapshot now fresh -> adopt the held position.
    h.connectors[h.maker_connector].ready = True
    h.connectors[h.maker_connector].account_positions = {
        "short": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.SHORT,
            amount=Decimal("1"),
            entry_price=Decimal("101"),
        )
    }
    asyncio.run(h._seed_inventory_from_connector())
    assert h._seed_adopted is True
    assert h._seed_fail_closed is False
    assert h._maker_sell_base == Decimal("1")
    assert h._hedge_buy_base == Decimal("1")


def test_seed_fail_closes_after_grace_window_when_inventory_stays_empty():
    # JEP-210: a genuinely empty adopt:true position still fail-closes, but only after the grace
    # window expires -- not on the first transient miss (here: spot leg never lands).
    h = _SeedHarness(adopt=True)
    h.connectors[h.maker_connector].account_positions = {
        "short": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.SHORT,
            amount=Decimal("1"),
            entry_price=Decimal("101"),
        )
    }
    h.connectors[h.hedge_connector].get_balance = lambda asset: ZERO
    h._seed_readiness_timeout = 0

    # Within the grace window: retries, no permanent fail-close.
    asyncio.run(h._seed_inventory_from_connector())
    assert h._seed_fail_closed is False
    assert getattr(h, "_seed_adopted", False) is False

    # Grace exhausted -> fail-close, ledgers untouched.
    h._seed_grace_seconds = 0
    asyncio.run(h._seed_inventory_from_connector())
    assert h._seed_fail_closed is True
    assert h._maker_sell_base == ZERO
    assert h._hedge_buy_base == ZERO


def _seeded_short_harness():
    h = _SeedHarness(adopt=True)
    h.connectors[h.maker_connector].account_positions = {
        "short": SimpleNamespace(
            trading_pair=h.maker_trading_pair,
            position_side=PositionSide.SHORT,
            amount=Decimal("1"),
            entry_price=Decimal("101"),
        )
    }
    h._seed_readiness_timeout = 0
    return h


def test_seed_actively_refreshes_hedge_balance_so_throttled_cache_adopts():
    # JEP-210: the hedge (KIS) background balance poll is starved by per-second throttle
    # (EGW00215) under multi-symbol load, so get_balance() reads 0 from an unpopulated
    # cache and a REAL held hedge fail-closed. The seed must actively DRIVE the hedge
    # _update_balances() (mirroring the perp _update_positions() refresh) so a real
    # holding adopts once a poll lands -- not sit reading an empty cache until grace.
    h = _seeded_short_harness()
    hedge = h.connectors[h.hedge_connector]
    cache = {"qty": ZERO}  # starts empty (throttled background poll never populated it)
    hedge.get_balance = lambda asset: cache["qty"]

    async def _update_balances():
        cache["qty"] = Decimal("1")  # a fresh successful poll populates the holding

    hedge._update_balances = _update_balances

    asyncio.run(h._seed_inventory_from_connector())

    assert h._seed_adopted is True
    assert h._seed_fail_closed is False
    assert h._maker_sell_base == Decimal("1")
    assert h._hedge_buy_base == Decimal("1")


def test_seed_hedge_balance_refresh_is_rate_limited():
    # The active hedge poll must be rate-limited so it does not hammer the throttled KIS
    # balance endpoint every 1s control tick across the (up to 180s) seed window.
    h = _seeded_short_harness()
    hedge = h.connectors[h.hedge_connector]
    calls = {"n": 0}
    hedge.get_balance = lambda asset: ZERO  # stays empty -> keeps retrying, never adopts

    async def _update_balances():
        calls["n"] += 1

    hedge._update_balances = _update_balances
    h._seed_balance_refresh_interval = 1000.0  # large -> only the first attempt polls

    for _ in range(3):  # three back-to-back attempts within the interval
        asyncio.run(h._seed_inventory_from_connector())

    assert calls["n"] == 1
    assert getattr(h, "_seed_adopted", False) is False


def test_seed_hedge_balance_refresh_failure_is_non_fatal():
    # A throttled _update_balances (EGW00215) must NOT crash or immediately fail-close;
    # the existing spot==0 -> _note_seed_retry + grace path handles it.
    h = _seeded_short_harness()
    hedge = h.connectors[h.hedge_connector]
    hedge.get_balance = lambda asset: ZERO

    async def _raises():
        raise IOError("EGW00215 throttle")

    hedge._update_balances = _raises

    asyncio.run(h._seed_inventory_from_connector())  # within grace

    assert h._seed_fail_closed is False
    assert getattr(h, "_seed_adopted", False) is False


def test_seed_without_update_balances_hook_falls_back_to_passive_cache_read():
    # Backward-compat: a hedge connector without _update_balances (other venues / stubs)
    # still adopts from the cache exactly as before -- the active refresh is a no-op.
    h = _seeded_short_harness()
    h.connectors[h.hedge_connector].get_balance = lambda asset: Decimal("1")

    asyncio.run(h._seed_inventory_from_connector())

    assert h._seed_adopted is True
    assert h._hedge_buy_base == Decimal("1")


def test_seed_does_not_adopt_stale_positive_cache_when_refresh_fails():
    # Review F1 (the dangerous regression the active refresh could introduce): if the active
    # _update_balances() FAILS (throttle/auth) but get_balance() still returns a STALE positive
    # value (prior poll / wrong-account / partial state), the seed must NOT adopt it. A failed
    # refresh withholds trust -> retry, never fall through to an unverified positive balance.
    h = _seeded_short_harness()
    hedge = h.connectors[h.hedge_connector]
    hedge.get_balance = lambda asset: Decimal("31")  # stale-positive cache

    async def _raises():
        raise IOError("EGW00215 throttle")

    hedge._update_balances = _raises

    asyncio.run(h._seed_inventory_from_connector())

    assert getattr(h, "_seed_adopted", False) is False  # did NOT adopt the unverified 31
    assert h._seed_fail_closed is False                 # within grace -> retry, not permanent
    assert h._maker_sell_base == ZERO
    assert h._hedge_buy_base == ZERO


def test_seed_adopts_stale_cache_only_after_a_confirmed_poll():
    # Companion to F1: once a poll SUCCEEDS, the (now-confirmed) cache may be adopted.
    h = _seeded_short_harness()
    hedge = h.connectors[h.hedge_connector]
    hedge.get_balance = lambda asset: Decimal("31")

    async def _ok():
        return None  # success, leaves the (already-populated) cache in place

    hedge._update_balances = _ok

    asyncio.run(h._seed_inventory_from_connector())

    assert h._seed_adopted is True
    assert h._hedge_buy_base == Decimal("31")


def test_seed_shared_connector_gate_prevents_duplicate_polls_across_executors():
    # Review F2: the rate-limit + success state live on the hedge CONNECTOR, so two symbols
    # sharing one KIS connector do not each burst the throttled endpoint -- one poll serves both.
    h1 = _seeded_short_harness()
    h2 = _seeded_short_harness()
    shared = h1.connectors[h1.hedge_connector]
    h2.connectors[h2.hedge_connector] = shared  # both executors share the one hedge connector

    cache = {"qty": ZERO}
    calls = {"n": 0}
    shared.get_balance = lambda asset: cache["qty"]

    async def _update_balances():
        calls["n"] += 1
        cache["qty"] = Decimal("1")

    shared._update_balances = _update_balances
    for h in (h1, h2):
        h._seed_balance_refresh_interval = 1000.0

    asyncio.run(h1._seed_inventory_from_connector())
    asyncio.run(h2._seed_inventory_from_connector())

    assert calls["n"] == 1            # h1 polled; h2 reused the shared confirmed cache
    assert h1._seed_adopted is True
    assert h2._seed_adopted is True   # adopted from the sibling's successful poll


def test_seed_hedge_balance_refresh_is_single_flight_across_executors():
    # Review rev2 F1: while one executor's _update_balances() is in flight on the shared
    # connector, a sibling must NOT start a second overlapping poll -- it trusts a recent
    # confirmed success or retries. Prevents overlapping balance mutations / endpoint bursts
    # when op_timeout (10s) > rate-limit interval (5s).
    h1 = _seeded_short_harness()
    h2 = _seeded_short_harness()
    shared = h1.connectors[h1.hedge_connector]
    h2.connectors[h2.hedge_connector] = shared

    calls = {"n": 0}
    cache = {"qty": ZERO}
    shared.get_balance = lambda asset: cache["qty"]

    async def _run():
        started = asyncio.Event()
        release = asyncio.Event()

        async def _update_balances():
            calls["n"] += 1
            started.set()
            await release.wait()  # hold the poll in flight
            cache["qty"] = Decimal("1")

        shared._update_balances = _update_balances

        t1 = asyncio.ensure_future(h1._seed_inventory_from_connector())
        await started.wait()  # h1's poll is now in flight
        await h2._seed_inventory_from_connector()  # h2 attempts while h1 in flight
        assert calls["n"] == 1                      # h2 did NOT start a second poll
        assert getattr(h2, "_seed_adopted", False) is False  # no confirmed success yet -> retry
        release.set()
        await t1
        assert h1._seed_adopted is True

    asyncio.run(_run())
    assert calls["n"] == 1


def test_seed_fail_closes_after_grace_on_persistent_refresh_failure():
    # Test gap (Codex): a hedge whose _update_balances() never succeeds must fail-close after
    # the grace window (not adopt), and the per-attempt poll stays rate-limited meanwhile.
    h = _seeded_short_harness()
    hedge = h.connectors[h.hedge_connector]
    hedge.get_balance = lambda asset: Decimal("31")  # stale positive, but never confirmed
    calls = {"n": 0}

    async def _raises():
        calls["n"] += 1
        raise IOError("EGW00215 throttle")

    hedge._update_balances = _raises
    h._seed_balance_refresh_interval = 1000.0  # only the first attempt polls

    asyncio.run(h._seed_inventory_from_connector())  # attempt 1 (polls, fails) -> retry
    assert h._seed_fail_closed is False
    asyncio.run(h._seed_inventory_from_connector())  # attempt 2 (not due) -> retry
    assert calls["n"] == 1                            # rate-limited: only one poll

    h._seed_grace_seconds = 0  # grace exhausted
    asyncio.run(h._seed_inventory_from_connector())
    assert h._seed_fail_closed is True
    assert getattr(h, "_seed_adopted", False) is False
    assert h._maker_sell_base == ZERO
    assert h._hedge_buy_base == ZERO
