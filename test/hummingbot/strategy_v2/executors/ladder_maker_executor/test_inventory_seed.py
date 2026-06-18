import asyncio
from decimal import Decimal
from types import SimpleNamespace

from hummingbot.core.data_type.common import PositionSide
from test.hummingbot.strategy_v2.executors.ladder_maker_executor.test_seed_observe import _make_observe_executor


def _short_position(ex, amount: str = "1", entry_price: str = "100"):
    return SimpleNamespace(
        trading_pair=ex.maker_trading_pair,
        position_side=PositionSide.SHORT,
        amount=Decimal(amount),
        entry_price=Decimal(entry_price),
    )


def test_seed_fail_closed_on_half_read_perp_empty_spot_present():
    ex = _make_observe_executor(adopt=True)
    maker = ex.connectors[ex.maker_connector]
    maker.account_positions = {}
    ex._seed_readiness_timeout = 0

    async def update_positions():
        return None

    maker._update_positions = update_positions

    asyncio.run(ex._seed_inventory_from_connector())

    assert ex._seed_fail_closed is True
    assert ex._seed_adopted is False
    assert ex._maker_sell_base == Decimal("0")
    assert ex._hedge_buy_base == Decimal("0")


def test_seed_refreshes_positions_then_adopts():
    ex = _make_observe_executor(adopt=True)
    maker = ex.connectors[ex.maker_connector]
    maker.account_positions = {}

    async def update_positions():
        maker.account_positions = {"short": _short_position(ex)}

    maker._update_positions = update_positions

    asyncio.run(ex._seed_inventory_from_connector())

    assert ex._seed_adopted is True
    assert ex._seed_fail_closed is False
    assert ex._maker_sell_base == Decimal("1")
    assert ex._hedge_buy_base == Decimal("1")
    assert ex._paired_oi() == Decimal("1")


def test_seed_fail_closed_on_half_read_perp_present_spot_zero():
    ex = _make_observe_executor(adopt=True)
    hedge = ex.connectors[ex.hedge_connector]
    hedge.get_balance = lambda asset: Decimal("0")

    asyncio.run(ex._seed_inventory_from_connector())

    assert ex._seed_fail_closed is True
    assert ex._seed_adopted is False
    assert ex._maker_sell_base == Decimal("0")
    assert ex._hedge_buy_base == Decimal("0")
