import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import Side

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import (
        LadderMakerExecutor,
    )

    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    _EXECUTOR_IMPORTABLE = False


KST = timezone(timedelta(hours=9))
FAIR_OPEN = Decimal("80100") / Decimal("1380")   # SELL fair 58.0435...
FAIR_CLOSE = Decimal("80000") / Decimal("1381")  # BUY fair  57.9290...


class _Tracked:
    def __init__(self, order_id):
        self.order_id = order_id
        self.order = None

    @property
    def cum_fees_quote(self):
        return Decimal("0")


class _Ev:
    def __init__(self, order_id, amount, price, trade_type):
        self.order_id = order_id
        self.amount = Decimal(str(amount))
        self.price = Decimal(str(price))
        self.trade_type = trade_type


@unittest.skipUnless(_EXECUTOR_IMPORTABLE, "ladder_maker_executor requires the V2 stack")
class LadderMakerTwoSidedIntegrationTest(unittest.TestCase):
    def _make_executor(
        self,
        *,
        wind_down: bool = False,
        eod_wind_minutes: int = 0,
        current_timestamp: float = 1000.0,
        round_trip_cost_bps: Decimal = Decimal("6"),
        eod_close_skew_bps: Decimal = Decimal("0"),
        max_close_cost_bps: Decimal = Decimal("0"),
        open_edge_vwap: Decimal = Decimal("8"),
        fx: tuple = (Decimal("1"), Decimal("1")),
        side_aware_fx: bool = True,
        two_sided: bool = True,
    ) -> "LadderMakerExecutor":
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex.config = SimpleNamespace(
            observe=False,
            two_sided=two_sided,
            total_size_cap=Decimal("2"),
            rungs=[
                SimpleNamespace(
                    edge_bps=Decimal("10"),
                    size=Decimal("1"),
                    min_edge_bps=Decimal("0"),
                    enabled=True,
                )
            ],
            maker_tick=Decimal("0.01"),
            hedge_tick=Decimal("0.01"),
            buffer_ticks=Decimal("0"),
            round_trip_cost_bps=round_trip_cost_bps,
            k_open_skew_bps=Decimal("0"),
            k_close_skew_bps=Decimal("0"),
            eod_close_skew_bps=eod_close_skew_bps,
            eod_wind_minutes=eod_wind_minutes,
            max_close_cost_bps=max_close_cost_bps,
            wind_down=wind_down,
            flatten_timeout_s=30.0,
            max_inventory=None,
            share_per_unit=Decimal("1"),
            side_aware_fx=side_aware_fx,
            static_fx_rate=None,
            hedge_max_slippage_bps=Decimal("30"),
            hedge_order_type=OrderType.LIMIT,
            min_reprice_interval_s=0.0,
            min_reprice_delta_ticks=Decimal("2"),
        )
        ex.maker_connector = "hyperliquid_perpetual"
        ex.maker_trading_pair = "XYZ:SKHX-USD"
        ex.hedge_connector = "kis"
        ex.hedge_trading_pair = "000660-KRW"
        ex.entry_side = TradeType.SELL
        ex.hedge_side = TradeType.BUY
        ex.maker_orders = {}
        ex.hedge_orders = {}
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
        ex._pending_hedge_signed = Decimal("0")
        ex._current_retries = 0
        ex._hedge_order_side = {}
        ex._maker_placed_edge_bps = {}
        ex._open_edge_base = Decimal("0")
        ex._open_edge_notional_bps = Decimal("0")
        ex._open_edge_vwap = open_edge_vwap
        ex._update_tracked = MagicMock()
        ex._compute_fair = MagicMock(
            side_effect=lambda side: FAIR_OPEN if side is Side.SELL else FAIR_CLOSE
        )
        ex._policy_side = MagicMock(return_value=Side.SELL)
        ex._get_fx = MagicMock(return_value=fx)
        ex._strategy = SimpleNamespace(current_timestamp=current_timestamp)
        return ex

    def test_open_close_reopen_roundtrip(self):
        ex = self._make_executor()
        size = Decimal("1")

        ex.maker_orders["open-1"] = _Tracked("open-1")
        ex._maker_placed_edge_bps["open-1"] = Decimal("10")
        ex.process_order_filled_event(None, None, _Ev("open-1", size, "101", TradeType.SELL))

        self.assertEqual(Decimal("-1"), ex._perp_net())
        self.assertEqual(Decimal("-1"), ex._pending_hedge_signed)

        ex._hedge_order_side["hedge-open-1"] = (TradeType.BUY, size)
        ex._credit_hedge_fill("hedge-open-1", size, Decimal("100"))

        self.assertEqual(Decimal("1"), ex._spot_net())
        self.assertEqual(Decimal("0"), ex._unhedged_base_signed())
        self.assertEqual(Decimal("1"), ex._paired_oi())

        ex.maker_orders["close-1"] = _Tracked("close-1")
        ex.process_order_filled_event(None, None, _Ev("close-1", size, "99", TradeType.BUY))

        self.assertEqual(Decimal("0"), ex._perp_net())
        self.assertEqual(Decimal("1"), ex._pending_hedge_signed)

        ex._hedge_order_side["hedge-close-1"] = (TradeType.SELL, size)
        ex._credit_hedge_fill("hedge-close-1", size, Decimal("100"))

        self.assertEqual(Decimal("0"), ex._spot_net())
        self.assertEqual(Decimal("0"), ex._paired_oi())
        self.assertEqual(Decimal("0"), ex._unhedged_base_signed())
        self.assertEqual(Decimal("2"), ex.get_net_pnl_quote())
        self.assertGreater(ex.get_net_pnl_quote(), Decimal("0"))

        ex.maker_orders["open-2"] = _Tracked("open-2")
        ex.process_order_filled_event(None, None, _Ev("open-2", size, "101", TradeType.SELL))

        self.assertEqual(Decimal("-1"), ex._perp_net())

    def test_pending_netting_partial(self):
        ex = self._make_executor()
        size = Decimal("1")
        half = Decimal("0.5")

        ex.maker_orders["open-1"] = _Tracked("open-1")
        ex.process_order_filled_event(None, None, _Ev("open-1", size, "101", TradeType.SELL))

        ex._hedge_order_side["hedge-open-1"] = (TradeType.BUY, size)
        ex._credit_hedge_fill("hedge-open-1", half, Decimal("100"))

        self.assertEqual(Decimal("-0.5"), ex._pending_hedge_signed)
        self.assertEqual(Decimal("-0.5"), ex._unhedged_base_signed())

        ex._credit_hedge_fill("hedge-open-1", half, Decimal("100"))

        self.assertEqual(Decimal("0.0"), ex._pending_hedge_signed)
        self.assertEqual(Decimal("0.0"), ex._unhedged_base_signed())

    def test_wind_down_close_only_integration(self):
        ex = self._make_executor(wind_down=True)
        ex._maker_sell_base = Decimal("1")
        ex._hedge_buy_base = Decimal("1")

        targets = ex._compute_targets()

        self.assertEqual([], [target for target in targets if target.side is Side.SELL])
        self.assertEqual([Side.BUY], [target.side for target in targets])
        self.assertEqual([Decimal("1")], [target.size for target in targets])

    def test_two_sided_close_fair_missing_returns_no_targets(self):
        # Spec case 5: fair_close is None -> fail-closed, no targets (not open-only).
        ex = self._make_executor()
        ex._maker_sell_base = Decimal("1")
        ex._hedge_buy_base = Decimal("1")
        ex._compute_fair = MagicMock(
            side_effect=lambda side: FAIR_OPEN if side is Side.SELL else None
        )
        self.assertEqual([], ex._compute_targets())

    def test_eod_ramp_shifts_close_under_budget(self):
        timestamp = datetime(2026, 6, 18, 15, 20, 0, tzinfo=KST).timestamp()
        baseline = self._make_executor(
            eod_wind_minutes=0,
            current_timestamp=timestamp,
            eod_close_skew_bps=Decimal("50"),
            max_close_cost_bps=Decimal("1"),
        )
        eod = self._make_executor(
            eod_wind_minutes=20,
            current_timestamp=timestamp,
            eod_close_skew_bps=Decimal("50"),
            max_close_cost_bps=Decimal("1"),
        )
        for ex in (baseline, eod):
            ex._maker_sell_base = Decimal("1")
            ex._hedge_buy_base = Decimal("1")

        baseline_close = [target for target in baseline._compute_targets() if target.side is Side.BUY][0]
        eod_close = [target for target in eod._compute_targets() if target.side is Side.BUY][0]
        budget_floor = (
            eod.config.round_trip_cost_bps
            - eod._open_edge_vwap
            - eod.config.max_close_cost_bps
        )

        self.assertEqual(Decimal("0.5"), eod._two_sided_state()["eod"])
        # JEP-169: close prices now anchor to FAIR_CLOSE (80000/1381), not the entry
        # SELL fair (80100/1380). The ~19.76 bps drop is the intended close-side fix.
        self.assertEqual(Decimal("57.85"), baseline_close.price)   # floor(FAIR_CLOSE*(1-13/1e4))
        self.assertEqual(Decimal("-3"), budget_floor)
        self.assertEqual(budget_floor, eod_close.edge_bps)
        self.assertEqual(Decimal("57.94"), eod_close.price)        # floor(FAIR_CLOSE*(1-(-3)/1e4))
        self.assertNotEqual(baseline_close.price, eod_close.price)
        self.assertEqual(
            -eod.config.max_close_cost_bps,
            eod._open_edge_vwap + eod_close.edge_bps - eod.config.round_trip_cost_bps,
        )

    def test_hedge_fill_fx_converts_krw_to_usd_in_spot_cash(self):
        # JEP-185: a KRW hedge fill must be FX-converted before accruing _spot_cash,
        # else _roundtrip PnL (USD _perp_cash + KRW _spot_cash) is off by ~fx (~1380x).
        # Flat fx=1380: maker SELL 1 @ 60 USD (= 82800/1380) hedged BUY 1 @ 82800 KRW
        # nets to exactly 0 round-trip PnL once converted.
        ex = self._make_executor(fx=(Decimal("1380"), Decimal("1380")))
        size = Decimal("1")

        ex.maker_orders["open-1"] = _Tracked("open-1")
        ex._maker_placed_edge_bps["open-1"] = Decimal("10")
        ex.process_order_filled_event(None, None, _Ev("open-1", size, "60", TradeType.SELL))

        ex._hedge_order_side["hedge-open-1"] = (TradeType.BUY, size)
        ex._credit_hedge_fill("hedge-open-1", size, Decimal("82800"))

        self.assertEqual(Decimal("0"), ex._unhedged_base_signed())
        self.assertEqual(Decimal("0"), ex.get_net_pnl_quote())

    def test_spot_cash_fx_is_side_aware(self):
        # JEP-185: side-aware FX must MIRROR compute_fair_price's pairing so the
        # round-trip nets at fair. maker SELL fair = kis_ask / fx_bid, so the paired
        # hedge BUY (fills at kis_ask) must also divide by fx_bid (NOT fx_ask). With a
        # bid/ask FX spread only the fx_bid choice nets to 0; fx_ask leaves a residual.
        ex = self._make_executor(fx=(Decimal("1380"), Decimal("1392")), side_aware_fx=True)
        size = Decimal("1")

        # maker SELL fair = kis_ask / fx_bid = 82800 / 1380 = 60 (USD)
        ex.maker_orders["open-1"] = _Tracked("open-1")
        ex._maker_placed_edge_bps["open-1"] = Decimal("10")
        ex.process_order_filled_event(None, None, _Ev("open-1", size, "60", TradeType.SELL))

        # hedge BUY fills at kis_ask = 82800 KRW; correct conversion -> /fx_bid (1380) = 60.
        ex._hedge_order_side["hedge-open-1"] = (TradeType.BUY, size)
        ex._credit_hedge_fill("hedge-open-1", size, Decimal("82800"))

        self.assertEqual(Decimal("0"), ex.get_net_pnl_quote())

    def test_hedge_price_to_maker_quote_side_aware_both_sides(self):
        # JEP-185 (F4): pin BOTH directions of the side-aware FX mapping.
        # hedge BUY pairs with maker SELL (fair kis_ask/fx_bid) -> divide by fx_bid;
        # hedge SELL pairs with maker BUY (fair kis_bid/fx_ask) -> divide by fx_ask.
        ex = self._make_executor(fx=(Decimal("1380"), Decimal("1392")), side_aware_fx=True)
        self.assertEqual(
            Decimal("82800") / Decimal("1380"),
            ex._hedge_price_to_maker_quote(Decimal("82800"), TradeType.BUY),
        )
        self.assertEqual(
            Decimal("82800") / Decimal("1392"),
            ex._hedge_price_to_maker_quote(Decimal("82800"), TradeType.SELL),
        )

    def test_hedge_price_to_maker_quote_mid_when_not_side_aware(self):
        # side_aware_fx=False mirrors compute_fair_price: use the mid rate for both sides.
        ex = self._make_executor(fx=(Decimal("1380"), Decimal("1392")), side_aware_fx=False)
        mid = (Decimal("1380") + Decimal("1392")) / Decimal("2")
        self.assertEqual(
            Decimal("82800") / mid,
            ex._hedge_price_to_maker_quote(Decimal("82800"), TradeType.BUY),
        )
        self.assertEqual(
            Decimal("82800") / mid,
            ex._hedge_price_to_maker_quote(Decimal("82800"), TradeType.SELL),
        )

    def test_hedge_price_to_maker_quote_skips_on_missing_fx(self):
        # JEP-185 (F1): with NO FX (live AND static both unavailable) the override must NOT
        # book raw KRW as USD; it returns 0 (skip the quote accrual) instead of identity.
        ex = self._make_executor()
        ex._get_fx = MagicMock(return_value=(None, None))
        self.assertEqual(
            Decimal("0"),
            ex._hedge_price_to_maker_quote(Decimal("82800"), TradeType.BUY),
        )

    def test_single_sided_matched_pnl_fx_converts(self):
        # JEP-185 (F2): the single-sided (two_sided=False) matched path derives hedge_avg
        # from _hedge_executed_quote / _hedge_executed_base. Without FX-converting
        # _hedge_executed_quote it carries KRW against the USD maker_avg (~1380x error).
        ex = self._make_executor(two_sided=False, fx=(Decimal("1380"), Decimal("1380")))
        size = Decimal("1")

        ex.maker_orders["open-1"] = _Tracked("open-1")
        ex._maker_placed_edge_bps["open-1"] = Decimal("10")
        ex.process_order_filled_event(None, None, _Ev("open-1", size, "60", TradeType.SELL))

        ex._hedge_order_side["hedge-open-1"] = (TradeType.BUY, size)
        ex._credit_hedge_fill("hedge-open-1", size, Decimal("82800"))

        # maker_avg = 60 (USD); hedge_avg must be 82800/1380 = 60 (USD), not 82800 (KRW).
        self.assertEqual(Decimal("0"), ex.get_net_pnl_quote())


if __name__ == "__main__":
    unittest.main()
