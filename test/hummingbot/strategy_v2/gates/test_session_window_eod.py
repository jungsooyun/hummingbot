from hummingbot.strategy_v2.gates.gate_chain import SessionWindow


def test_minutes_and_seconds_to_end_inside_krx_window():
    window = SessionWindow(start=(9, 0), end=(15, 30))

    assert window.minutes_to_end(15, 20, 0) == 10.0
    assert window.seconds_to_end(15, 20, 0) == 600.0


def test_time_to_end_returns_none_outside_half_open_window():
    window = SessionWindow(start=(9, 0), end=(15, 30))

    assert window.minutes_to_end(8, 59, 59) is None
    assert window.minutes_to_end(15, 30, 0) is None


def test_seconds_to_end_includes_start_boundary():
    window = SessionWindow(start=(9, 0), end=(15, 30))

    assert window.seconds_to_end(9, 0, 0) == float((15 * 60 + 30 - 9 * 60) * 60)


def test_nxt_window_minutes_to_end():
    window = SessionWindow(start=(15, 30), end=(20, 0))

    assert window.minutes_to_end(19, 45, 0) == 15.0
