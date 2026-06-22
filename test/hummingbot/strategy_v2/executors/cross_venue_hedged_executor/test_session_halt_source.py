from types import SimpleNamespace
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_halt_source import (
    compute_session_halt, NoHaltSource, KisSessionHaltSource, SessionHaltState,
)

def _sig(**kw):
    base = dict(hour_cls_auction=False, book_age_sec=0.0, book_static_sec=0.0,
                trht_halted=False, cb_latched=False, vi_latched=False, market_status_ready=True)
    base.update(kw)
    return SimpleNamespace(**base)

def _eval(sig, in_auction=False, max_ws_age_s=3.0, max_book_static_s=8.0):
    return compute_session_halt(sig, in_auction=in_auction,
                                max_ws_age_s=max_ws_age_s, max_book_static_s=max_book_static_s)

def test_clear_book_is_open():
    st = _eval(_sig(book_age_sec=0.1, book_static_sec=0.1))
    assert st.halted is False and st.ready is True

def test_in_auction_halts():
    assert _eval(_sig(), in_auction=True).halted is True

def test_cold_start_no_frame_fail_closed():
    st = _eval(_sig(book_age_sec=None))
    assert st.halted is True and st.ready is False

def test_stale_book_fail_closed():
    assert _eval(_sig(book_age_sec=99.0)).halted is True

def test_book_frozen_when_static_exceeds_threshold():
    st = _eval(_sig(book_age_sec=0.1, book_static_sec=9.0))
    assert st.halted is True and st.reason == "book_frozen"

def test_book_frozen_suppressed_during_auction():
    # scheduled auction static book must NOT be mislabelled book_frozen
    st = _eval(_sig(book_age_sec=0.1, book_static_sec=9.0), in_auction=True)
    assert st.reason == "in_auction_window"

def test_hour_cls_auction_halts():
    assert _eval(_sig(hour_cls_auction=True, book_static_sec=0.1)).halted is True

def test_cb_latch_halts():
    assert _eval(_sig(cb_latched=True, book_static_sec=0.1)).reason == "market_wide_cb"

def test_status_unconfirmed_fail_closed():
    st = _eval(_sig(market_status_ready=False, book_static_sec=0.1))
    assert st.halted is True and st.ready is False

def test_nohalt_source_always_open():
    st = NoHaltSource().evaluate("X", in_auction=True, max_ws_age_s=3.0, max_book_static_s=8.0)
    assert st.halted is False and st.ready is True

def test_kis_source_delegates_to_connector():
    conn = SimpleNamespace(get_session_halt_signals=lambda p: _sig(cb_latched=True, book_static_sec=0.1))
    st = KisSessionHaltSource(conn).evaluate("X", in_auction=False, max_ws_age_s=3.0, max_book_static_s=8.0)
    assert st.halted is True
