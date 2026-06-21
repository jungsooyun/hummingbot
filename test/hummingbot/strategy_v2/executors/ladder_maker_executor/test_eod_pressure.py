from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import (
    LadderMakerExecutor,
    ZERO,
)


KST = timezone(timedelta(hours=9))


def _executor_at(hour: int, minute: int, eod_wind_minutes=None):
    ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
    ex._strategy = SimpleNamespace(
        current_timestamp=datetime(2026, 6, 18, hour, minute, 0, tzinfo=KST).timestamp()
    )
    if eod_wind_minutes is None:
        ex.config = SimpleNamespace()
    else:
        ex.config = SimpleNamespace(eod_wind_minutes=eod_wind_minutes)
    from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_calendar import KrxSessionCalendar

    ex._calendar = KrxSessionCalendar()
    return ex


def test_eod_pressure_ramps_halfway_inside_wind_window():
    ex = _executor_at(15, 20, eod_wind_minutes=20)

    assert LadderMakerExecutor._compute_eod_pressure(ex) == Decimal("0.5")


def test_eod_pressure_disabled_when_wind_is_zero():
    ex = _executor_at(15, 20, eod_wind_minutes=0)

    assert LadderMakerExecutor._compute_eod_pressure(ex) == ZERO


def test_eod_pressure_is_zero_before_wind_window():
    ex = _executor_at(15, 0, eod_wind_minutes=20)

    assert LadderMakerExecutor._compute_eod_pressure(ex) == ZERO


def test_eod_pressure_saturates_after_close():
    ex = _executor_at(16, 0, eod_wind_minutes=20)

    assert LadderMakerExecutor._compute_eod_pressure(ex) == Decimal("1")


def test_eod_pressure_defaults_to_disabled_when_config_field_missing():
    ex = _executor_at(15, 20)

    assert LadderMakerExecutor._compute_eod_pressure(ex) == ZERO
