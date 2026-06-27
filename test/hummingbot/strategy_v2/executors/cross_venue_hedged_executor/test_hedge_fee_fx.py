"""JEP-254 — hedge-leg fees must convert to the maker quote (USD) before PnL subtraction.

The KIS hedge leg pays fees in its native quote (KRW). ``get_cum_fees_quote()`` sums
``_maker_fees_quote`` (HL / USD) + ``_hedge_fees_quote`` and the total is subtracted from a
USD-denominated net PnL. Crediting the RAW KRW hedge fee is the JEP-185 currency bug on
the fee term (~1380x), which would mis-calibrate every loss-based safety mechanism that
reads this number (drawdown breaker / risk lease / engine-vs-dashboard reconciliation).

Fees must route through ``_hedge_fee_to_maker_quote`` (identity in the base; KRW->USD
mid-FX in the ladder via ``FxBridgedFairSource``) on BOTH credit paths: the
order-completed event (``process_order_completed_event``) and the lost-event
``_reconcile_stuck_hedges`` adoption.
"""
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.fx_bridged_fair_source import (
    FxBridgedFairSource,
)
from hummingbot.strategy_v2.models.base import RunnableStatus

FX = Decimal("1380")  # KRW per USD


# ----------------------------------------------------------------- FxBridgedFairSource

def _fx_source(fx, side_aware=False, static=None):
    src = FxBridgedFairSource(side_aware, static, MagicMock())
    src._get_fx = MagicMock(return_value=fx)
    return src


def test_fx_source_fee_converts_krw_to_usd_at_mid():
    # 1380 KRW fee / 1380 (mid) = 1.0 USD.
    src = _fx_source((Decimal("1380"), Decimal("1380")))
    assert src.hedge_fee_to_maker_quote(Decimal("1380")) == Decimal("1")


def test_fx_source_fee_uses_mid_under_bid_ask_spread():
    # A fee is a NON-directional cost: convert at the mid rate (no side threading),
    # unlike the side-aware notional bridge whose bid/ask pairing makes a round trip
    # net at fair. mid = (1380 + 1392) / 2 = 1386.
    src = _fx_source((Decimal("1380"), Decimal("1392")), side_aware=True)
    mid = (Decimal("1380") + Decimal("1392")) / Decimal("2")
    assert src.hedge_fee_to_maker_quote(Decimal("1386")) == Decimal("1386") / mid


def test_fx_source_fee_zero_when_fx_unavailable():
    # FX unavailable -> 0 (never book raw KRW as USD), mirroring the notional bridge.
    # Fail-safe direction: a transient under-count of one fill's cost, never a ~1380x
    # phantom loss that would false-trip the drawdown breaker.
    src = _fx_source((None, None))
    assert src.hedge_fee_to_maker_quote(Decimal("1380")) == Decimal("0")


# ----------------------------------------------------------------------- base fee seam

class _Cx:
    """Connector in-flight order for the lost-event reconcile path.

    Terminal-but-unfilled (canceled/failed with a fee) so the reconcile credits ONLY the
    fee (line ~1059) without invoking the fill-credit machinery — isolates the fee path.
    """

    def __init__(self, fee_krw):
        self.amount = Decimal("1")
        self.is_open = False
        self.is_filled = False
        self.is_done = True
        self.executed_amount_base = Decimal("0")
        self.average_executed_price = Decimal("100")
        self.price = Decimal("100")
        self.base_asset = "X"
        self.quote_asset = "KRW"
        self._fee_krw = Decimal(str(fee_krw))

    def cumulative_fee_paid(self, token, exchange=None):
        return self._fee_krw


class _Tracked:
    def __init__(self, order_id, fee_quote=Decimal("0")):
        self.order_id = order_id
        self.order = None
        self._fee_quote = Decimal(str(fee_quote))

    @property
    def cum_fees_quote(self):
        return self._fee_quote


class _FeeSeamHarness(CrossVenueHedgedExecutorBase):
    """Base executor whose fee seam divides KRW by a known FX (the ladder's real behavior).

    With the base identity ``_hedge_fee_to_maker_quote`` the override below is what the
    production credit paths must call; if they credit the raw native fee instead, the
    assertions below fail (the RED state).
    """

    def __init__(self):
        self.maker_orders = {}
        self.hedge_orders = {}
        self._maker_fees_quote = Decimal("0")
        self._hedge_fees_quote = Decimal("0")
        self._current_retries = 0
        self._max_retries = 3
        self._status = RunnableStatus.RUNNING
        self.config = SimpleNamespace(two_sided=True)
        self.maker_connector = "hl"
        self.hedge_connector = "kis"
        self.entry_side = TradeType.SELL
        self.hedge_side = TradeType.BUY
        self.connector_orders = {}

    def _hedge_fee_to_maker_quote(self, fee: Decimal) -> Decimal:
        return fee / FX

    def _update_tracked(self, *_):
        pass

    def get_in_flight_order(self, connector_name, order_id):
        return self.connector_orders.get(order_id)

    def evaluate_max_retries(self):
        pass

    # abstract-method stubs (instantiation only)
    def _gates_open(self):
        return True

    def _compute_targets(self):
        return []

    def _should_reprice(self, targets):
        return False

    def _place_targets(self, targets):
        pass

    def _maker_balance_candidate(self):
        return None

    def _size_hedge(self, pending_base):
        return None


def test_completion_event_routes_hedge_fee_through_fx_seam():
    h = _FeeSeamHarness()
    h.hedge_orders["h0"] = _Tracked("h0", fee_quote=Decimal("1380"))  # KRW
    h.process_order_completed_event(None, None, SimpleNamespace(order_id="h0"))
    assert h._hedge_fees_quote == Decimal("1")  # 1380 KRW -> 1 USD
    assert h.get_cum_fees_quote() == Decimal("1")


def test_reconcile_lost_event_routes_hedge_fee_through_fx_seam():
    h = _FeeSeamHarness()
    h.hedge_orders["h1"] = _Tracked("h1")  # order is None -> reconcile adopts the connector truth
    h.connector_orders["h1"] = _Cx(fee_krw=Decimal("2760"))
    h._reconcile_stuck_hedges()
    assert h._hedge_fees_quote == Decimal("2")  # 2760 KRW -> 2 USD


def test_maker_fee_is_not_fx_converted():
    # Maker leg (HL) fees are already in the maker quote (USD): must NOT be divided by FX.
    h = _FeeSeamHarness()
    h.maker_orders["m0"] = _Tracked("m0", fee_quote=Decimal("5"))  # USD
    h.process_order_completed_event(None, None, SimpleNamespace(order_id="m0"))
    assert h._maker_fees_quote == Decimal("5")
    assert h._hedge_fees_quote == Decimal("0")
