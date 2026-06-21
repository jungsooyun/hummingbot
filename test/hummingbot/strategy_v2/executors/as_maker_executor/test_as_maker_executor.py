from decimal import Decimal

import pytest
from pydantic import ValidationError

from hummingbot.core.data_type.common import PositionAction, PriceType, TradeType
from hummingbot.strategy_v2.executors.as_maker_executor.as_maker_executor import AsMakerExecutor
from hummingbot.strategy_v2.executors.as_maker_executor.data_types import AsMakerExecutorConfig
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.maker_reconcile import (
    RestingOrder,
    RungTarget,
    Side,
)


def _valid_config_kwargs(**overrides):
    base = dict(
        connector_name="hyperliquid_perpetual",
        trading_pair="BTC-USD",
        gamma=Decimal("1"),
        kappa=Decimal("1.5"),
        order_amount=Decimal("0.01"),
        max_inventory=Decimal("0.1"),
        maker_tick=Decimal("0.5"),
    )
    base.update(overrides)
    return base


def test_valid_config_constructs():
    cfg = AsMakerExecutorConfig(**_valid_config_kwargs())
    assert cfg.type == "as_maker_executor"
    assert cfg.observe is True            # default observe
    assert cfg.tau == Decimal("1")


@pytest.mark.parametrize("field,bad", [
    ("gamma", Decimal("0")), ("kappa", Decimal("0")), ("order_amount", Decimal("0")),
    ("max_inventory", Decimal("0")), ("tau", Decimal("0")), ("maker_tick", Decimal("0")),
    ("eta", Decimal("-1")), ("min_spread_pct", Decimal("-1")),
    ("volatility_sampling_length", 1), ("volatility_sampling_length", 0),
    ("volatility_processing_length", 0),
])
def test_config_validator_rejects(field, bad):
    with pytest.raises(ValidationError):
        AsMakerExecutorConfig(**_valid_config_kwargs(**{field: bad}))


# ----- pure-Python σ double, reused by later executor tests -----
class FakeVolatilitySource:
    def __init__(self, value=Decimal("2"), ready=True):
        self._value = float(value)
        self._ready = ready
        self.samples = []           # records add_sample calls (for σ-feed test)

    def add_sample(self, value):
        self.samples.append(value)

    @property
    def current_value(self):
        return self._value

    @property
    def is_ready(self):
        return self._ready


def test_fake_volatility_source_contract():
    fv = FakeVolatilitySource(value=Decimal("3"), ready=False)
    fv.add_sample(100.0)
    assert fv.samples == [100.0]
    assert fv.current_value == 3.0
    assert fv.is_ready is False


class _FakeConnector:
    def __init__(self, best_bid, best_ask, price_tick=Decimal("0.5"), amount_tick=Decimal("0.0001")):
        self._bid, self._ask = Decimal(best_bid), Decimal(best_ask)
        self._ptick, self._atick = Decimal(price_tick), Decimal(amount_tick)

    def get_price_by_type(self, pair, price_type):
        if price_type == PriceType.BestBid:
            return self._bid
        if price_type == PriceType.BestAsk:
            return self._ask
        return (self._bid + self._ask) / Decimal("2")

    def quantize_order_price(self, pair, price):
        return (Decimal(price) // self._ptick) * self._ptick

    def quantize_order_amount(self, pair, amount):
        return (Decimal(amount) // self._atick) * self._atick


class _FakeStrategy:
    def __init__(self, connectors, ts=1000.0):
        self.connectors = connectors
        self.current_timestamp = ts
        self.cancelled = []

    def cancel(self, connector, pair, oid):
        self.cancelled.append((connector, pair, oid))


def _fake_strategy_with_book(connector_name, pair, best_bid, best_ask, **ticks):
    return _FakeStrategy({connector_name: _FakeConnector(best_bid, best_ask, **ticks)})


def _rung(side, price, size):
    return RungTarget(side=side, price=Decimal(price), size=Decimal(size), edge_bps=Decimal("0"))


MID = Decimal("100")


def _make_exec(cfg_overrides=None, sigma=Decimal("2"), ready=True, perp_net=Decimal("0"),
               price_tick=Decimal("0.5")):
    cfg = AsMakerExecutorConfig(**_valid_config_kwargs(**(cfg_overrides or {})))
    # NOTE: the executor quantizes via the CONNECTOR (not config.maker_tick), so to exercise
    # the post-quantization positivity gate the coarse tick must be injected on the fake book.
    strat = _fake_strategy_with_book(
        cfg.connector_name,
        cfg.trading_pair,
        best_bid=Decimal("99.5"),
        best_ask=Decimal("100.5"),
        price_tick=price_tick,
    )
    ex = AsMakerExecutor(strategy=strat, config=cfg, volatility_source=FakeVolatilitySource(sigma, ready))
    if perp_net > 0:
        ex._maker_buy_base = perp_net
    elif perp_net < 0:
        ex._maker_sell_base = -perp_net
    return ex


def test_compute_targets_quote_golden_q0():
    ex = _make_exec(perp_net=Decimal("0"))
    targets = {t.side: t for t in ex._compute_targets()}
    assert set(targets) == {Side.BUY, Side.SELL}
    assert targets[Side.BUY].price < MID < targets[Side.SELL].price
    assert targets[Side.BUY].size == Decimal("0.01") == targets[Side.SELL].size
    tick = Decimal("0.5")
    assert abs((MID - targets[Side.BUY].price) - (targets[Side.SELL].price - MID)) <= tick


def test_compute_targets_q_positive_skews_down_and_shrinks_bid():
    ex = _make_exec(cfg_overrides={"eta": Decimal("1")}, perp_net=Decimal("0.05"))
    targets = {t.side: t for t in ex._compute_targets()}
    assert targets[Side.BUY].price < MID
    assert targets[Side.BUY].size < Decimal("0.01")
    assert targets[Side.SELL].size == Decimal("0.01")


def test_positivity_nonfinite_suppresses_whole_tick(caplog):
    # Non-finite σ (NaN) must suppress the WHOLE tick (return []) and log at ERROR, never crash.
    # as_policy's 3a finite-hardening raises before the executor's own positivity branch; either
    # path satisfies the spec §3.6 / brainstorm rule (non-finite -> whole-tick suppress + ERROR).
    import logging
    caplog.set_level(logging.ERROR)
    ex = _make_exec(sigma=Decimal("nan"))
    assert ex._compute_targets() == []
    errs = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("finite" in r.message.lower() for r in errs)


def test_positivity_negative_bid_drops_bid_keeps_ask():
    ex = _make_exec(cfg_overrides={"gamma": Decimal("50"), "min_spread_pct": Decimal("0")}, sigma=Decimal("10"))
    sides = {t.side for t in ex._compute_targets()}
    assert Side.BUY not in sides and Side.SELL in sides


def test_positivity_post_quantization_floor_drops_side():
    # raw bid is POSITIVE (~98.49, mid=100 sigma=2) but a coarse connector price-tick (100)
    # floors it to 0. The positivity gate runs on QUANTIZED values (spec §3.6 — the spec
    # pass-1 BLOCKING fix), so it MUST drop BUY here while SELL (raw ~101.5 -> 100) survives.
    # This is the only case where raw-positivity would keep a side that the quantized gate drops.
    ex = _make_exec(price_tick=Decimal("100"), sigma=Decimal("2"))
    sides = {t.side for t in ex._compute_targets()}
    assert Side.BUY not in sides and Side.SELL in sides


def test_warmup_gate_blocks_until_ready():
    ex = _make_exec(ready=False)
    assert ex._gates_open() is False
    ex._vol._ready = True
    assert ex._gates_open() is True


def test_sigma_fed_even_when_gate_closed():
    ex = _make_exec()
    ex.config.kill_switch = True
    n0 = len(ex._vol.samples)
    ex._gates_open()
    assert len(ex._vol.samples) == n0 + 1
    assert ex._gates_open() is False


def test_size_hedge_is_noop():
    ex = _make_exec()
    assert ex._size_hedge(Decimal("5")) is None


def test_maker_balance_candidate_none_in_observe():
    ex = _make_exec()
    assert ex._maker_balance_candidate() is None


def test_observe_no_submit(monkeypatch):
    ex = _make_exec()
    calls = []
    monkeypatch.setattr(ex, "place_order", lambda **kw: calls.append(kw) or "oid")
    ex._reconcile_maker()
    assert calls == []
    assert ex.maker_orders == {}


def test_dispatch_plumbing_production(monkeypatch):
    ex = _make_exec(cfg_overrides={"observe": False})
    calls = []
    monkeypatch.setattr(ex, "place_order", lambda **kw: calls.append(kw) or f"oid-{kw['side']}")
    ex._place_target_one(_rung(Side.BUY, Decimal("99.5"), Decimal("0.01")))
    ex._place_target_one(_rung(Side.SELL, Decimal("100.5"), Decimal("0.01")))
    sides = [c["side"] for c in calls]
    assert TradeType.BUY in sides and TradeType.SELL in sides


def test_infer_position_action_by_net_sign():
    ex = _make_exec(perp_net=Decimal("0.05"))
    assert ex._infer_position_action(TradeType.SELL) == PositionAction.CLOSE
    assert ex._infer_position_action(TradeType.BUY) == PositionAction.OPEN


def test_should_reprice_on_side_drift(monkeypatch):
    ex = _make_exec()
    resting = [RestingOrder(order_id="b", side=Side.BUY, price=Decimal("99.5"), size=Decimal("0.01"))]
    monkeypatch.setattr(ex, "_resting_maker_orders", lambda: resting)
    ex._last_reprice_ts = ex._strategy.current_timestamp
    assert ex._should_reprice([_rung(Side.SELL, Decimal("100.5"), Decimal("0.01"))]) is True

    ex._last_reprice_ts = ex._strategy.current_timestamp
    assert ex._should_reprice([_rung(Side.BUY, Decimal("99.5"), Decimal("0.01"))]) is False
