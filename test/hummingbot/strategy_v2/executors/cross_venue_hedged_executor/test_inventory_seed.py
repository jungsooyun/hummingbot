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
