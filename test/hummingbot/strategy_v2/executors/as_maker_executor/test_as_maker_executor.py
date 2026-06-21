from decimal import Decimal

import pytest
from pydantic import ValidationError

from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.as_maker_executor.data_types import AsMakerExecutorConfig


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
