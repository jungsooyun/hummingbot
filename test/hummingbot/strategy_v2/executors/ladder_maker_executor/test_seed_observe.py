import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import PositionMode, PositionSide, TradeType
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import Side
from hummingbot.strategy_v2.models.base import RunnableStatus


def _make_observe_executor(*, adopt: bool) -> LadderMakerExecutor:
    ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
    ex.config = SimpleNamespace(
        adopt_existing_inventory=adopt,
        observe=True,
        two_sided=True,
        total_size_cap=Decimal("2"),
        rungs=[
            SimpleNamespace(
                edge_bps=Decimal("100"),
                size=Decimal("1"),
                min_edge_bps=Decimal("0"),
                enabled=True,
            )
        ],
        maker_tick=Decimal("0.1"),
        buffer_ticks=Decimal("0"),
        round_trip_cost_bps=Decimal("10"),
        k_open_skew_bps=Decimal("0"),
        k_close_skew_bps=Decimal("0"),
        eod_close_skew_bps=Decimal("0"),
        max_close_cost_bps=Decimal("0"),
        wind_down=False,
        max_inventory=None,
        share_per_unit=Decimal("1"),
    )
    ex.maker_connector = "hyperliquid_perpetual"
    ex.maker_trading_pair = "XYZ:SKHX-USD"
    ex.hedge_connector = "kis"
    ex.hedge_trading_pair = "000660-KRW"
    ex.entry_side = TradeType.SELL
    ex.hedge_side = TradeType.BUY
    maker = SimpleNamespace(
        ready=True,
        position_mode=PositionMode.ONEWAY,
        account_positions={
            "short": SimpleNamespace(
                trading_pair=ex.maker_trading_pair,
                position_side=PositionSide.SHORT,
                amount=Decimal("1"),
                entry_price=Decimal("100"),
            )
        },
        quantize_order_amount=lambda pair, amount: amount,
        quantize_order_price=lambda pair, price: price,
        get_price_by_type=lambda pair, price_type: Decimal("100"),
    )
    hedge = SimpleNamespace(
        ready=True,
        get_balance=lambda asset: Decimal("1"),
        get_price_by_type=lambda pair, price_type: Decimal("130000"),
    )
    ex.connectors = {ex.maker_connector: maker, ex.hedge_connector: hedge}
    ex.place_order = MagicMock(side_effect=["OID-OPEN", "OID-CLOSE"])
    ex.maker_orders = {}
    ex.hedge_orders = {}
    ex._maker_placed_edge_bps = {}
    ex._last_observe = None
    ex._last_observe_log_ts = 0.0
    ex._last_reprice_ts = 0.0
    ex._strategy = SimpleNamespace(current_timestamp=1000.0)
    ex._status = RunnableStatus.RUNNING
    ex._compute_fair = MagicMock(return_value=Decimal("100"))
    ex._policy_side = MagicMock(return_value=Side.SELL)
    ex._get_fx = MagicMock(return_value=(Decimal("1300"), Decimal("1301")))
    ex._unhedged_base_signed = LadderMakerExecutor._unhedged_base_signed.__get__(ex, LadderMakerExecutor)
    ex._paired_oi = LadderMakerExecutor._paired_oi.__get__(ex, LadderMakerExecutor)
    ex._open_edge_vwap = Decimal("0")
    ex._pending_hedge_signed = Decimal("0")
    ex._maker_executed_base = Decimal("0")
    ex._maker_executed_quote = Decimal("0")
    ex._hedge_executed_base = Decimal("0")
    ex._hedge_executed_quote = Decimal("0")
    ex._maker_fees_quote = Decimal("0")
    ex._hedge_fees_quote = Decimal("0")
    ex._perp_cash = Decimal("0")
    ex._spot_cash = Decimal("0")
    ex._maker_buy_base = Decimal("0")
    ex._maker_sell_base = Decimal("0")
    ex._hedge_buy_base = Decimal("0")
    ex._hedge_sell_base = Decimal("0")
    ex._open_edge_base = Decimal("0")
    ex._open_edge_notional_bps = Decimal("0")
    ex._hedge_order_side = {}
    ex._process_hedges = MagicMock()
    ex._gates_open = MagicMock(return_value=True)
    ex._should_reprice = MagicMock(return_value=True)
    ex.logger = MagicMock(return_value=MagicMock())
    from hummingbot.strategy_v2.executors.ladder_maker_executor.session_calendar import KrxSessionCalendar

    ex._calendar = KrxSessionCalendar()
    return ex


def test_observe_startup_adopts_q_one_and_emits_close_zero_submit():
    ex = _make_observe_executor(adopt=True)

    asyncio.run(ex.control_task())

    assert ex._paired_oi() == Decimal("1")
    obs = ex._last_observe
    assert obs["Q"] == "1"
    assert obs["close"][0]["side"] == "BUY"
    assert obs["close"][0]["position_action"] == "CLOSE"
    ex.place_order.assert_not_called()


def test_observe_startup_flag_off_seed_is_structural_noop():
    ex = _make_observe_executor(adopt=False)

    asyncio.run(ex.control_task())

    assert ex._paired_oi() == Decimal("0")
    assert ex._maker_sell_base == Decimal("0")
    assert ex._hedge_buy_base == Decimal("0")
    assert getattr(ex, "_seed_fail_closed", False) is False
    ex.place_order.assert_not_called()
