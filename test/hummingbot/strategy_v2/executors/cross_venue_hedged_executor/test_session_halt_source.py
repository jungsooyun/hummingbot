from types import SimpleNamespace
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_halt_source import (
    compute_session_halt, NoHaltSource, KisSessionHaltSource, SessionHaltState,
    apply_post_halt_cooldown, COOLDOWN_ARM_REASONS,
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


# ----------------------------------------------------------- JEP-198 post-halt cooldown (D)

def _frozen():
    return SessionHaltState(True, True, "book_frozen")


def _open():
    return SessionHaltState(False, True, "")


def test_cooldown_passthrough_when_open_and_inactive():
    st, until, armed = apply_post_halt_cooldown(_open(), now=100.0, cooldown_until=0.0, cooldown_s=1800.0)
    assert st.halted is False and until == 0.0 and armed is False


def test_cooldown_arms_on_book_frozen():
    st, until, armed = apply_post_halt_cooldown(_frozen(), now=100.0, cooldown_until=0.0, cooldown_s=1800.0)
    assert st.halted is True and st.reason == "book_frozen"
    assert until == 1900.0 and armed is True


def test_cooldown_holds_halt_after_freeze_clears():
    # book un-froze (open) but still within the cooldown window -> forced halt
    st, until, armed = apply_post_halt_cooldown(_open(), now=500.0, cooldown_until=1900.0, cooldown_s=1800.0)
    assert st.halted is True and st.reason == "post_halt_cooldown"
    assert until == 1900.0 and armed is False


def test_cooldown_releases_after_expiry():
    st, until, armed = apply_post_halt_cooldown(_open(), now=2000.0, cooldown_until=1900.0, cooldown_s=1800.0)
    assert st.halted is False and until == 1900.0 and armed is False


def test_staleness_does_not_arm_cooldown():
    stale = SessionHaltState(True, False, "not_ready_book_stale")
    st, until, armed = apply_post_halt_cooldown(stale, now=100.0, cooldown_until=0.0, cooldown_s=1800.0)
    assert until == 0.0 and armed is False
    assert st.reason == "not_ready_book_stale"  # passthrough — halted by its own reason, not cooldown


def test_cooldown_extends_while_frozen_but_arms_once():
    st1, until1, armed1 = apply_post_halt_cooldown(_frozen(), now=100.0, cooldown_until=0.0, cooldown_s=1800.0)
    assert armed1 is True and until1 == 1900.0
    st2, until2, armed2 = apply_post_halt_cooldown(_frozen(), now=101.0, cooldown_until=until1, cooldown_s=1800.0)
    assert armed2 is False and until2 == 1901.0   # extended (covers from freeze END), no re-arm


def test_scheduled_auction_does_not_arm_cooldown():
    auc = SessionHaltState(True, True, "in_auction_window")
    st, until, armed = apply_post_halt_cooldown(auc, now=100.0, cooldown_until=0.0, cooldown_s=1800.0)
    assert until == 0.0 and armed is False   # scheduled auctions self-clear via the clock


def test_cb_latch_arms_cooldown():
    cb = SessionHaltState(True, True, "market_wide_cb")
    _, until, armed = apply_post_halt_cooldown(cb, now=100.0, cooldown_until=0.0, cooldown_s=1800.0)
    assert until == 1900.0 and armed is True


def test_cooldown_zero_disables():
    st, until, armed = apply_post_halt_cooldown(_frozen(), now=100.0, cooldown_until=0.0, cooldown_s=0.0)
    assert until == 0.0 and armed is False           # cooldown_s=0 never arms
    st2, _, _ = apply_post_halt_cooldown(_open(), now=100.0, cooldown_until=until, cooldown_s=0.0)
    assert st2.halted is False                        # and never holds


def test_arm_reasons_membership():
    assert "book_frozen" in COOLDOWN_ARM_REASONS
    assert "market_wide_cb" in COOLDOWN_ARM_REASONS
    assert "not_ready_book_stale" not in COOLDOWN_ARM_REASONS


# ----------------------------------------------------------- JEP-226 force-eligibility + Tuple import

def test_force_eligible_reasons_is_in_auction_only():
    from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_halt_source import (
        FORCE_ELIGIBLE_HALT_REASONS,
    )
    assert FORCE_ELIGIBLE_HALT_REASONS == frozenset({"in_auction_window"})
    # hour_cls_auction / vi must NOT be force-eligible (R2-F1): they only surface OUTSIDE a clock window
    assert "hour_cls_auction" not in FORCE_ELIGIBLE_HALT_REASONS
    assert "vi" not in FORCE_ELIGIBLE_HALT_REASONS
    assert FORCE_ELIGIBLE_HALT_REASONS.isdisjoint(COOLDOWN_ARM_REASONS)


def test_vi_and_hourcls_cooccur_folds_to_hourcls_not_force():
    # vi co-occurring with hour_cls_auction folds to "hour_cls_auction" (priority), NOT force-eligible
    from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.session_halt_source import (
        FORCE_ELIGIBLE_HALT_REASONS,
    )
    st = _eval(_sig(hour_cls_auction=True, vi_latched=True, book_static_sec=0.1))
    assert st.reason == "hour_cls_auction"
    assert st.reason not in FORCE_ELIGIBLE_HALT_REASONS


def test_apply_post_halt_cooldown_type_hints_resolve():
    import typing
    from hummingbot.strategy_v2.executors.cross_venue_hedged_executor import session_halt_source as m
    typing.get_type_hints(m.apply_post_halt_cooldown)  # NameError if Tuple unimported
    assert "in_auction_window" not in COOLDOWN_ARM_REASONS
