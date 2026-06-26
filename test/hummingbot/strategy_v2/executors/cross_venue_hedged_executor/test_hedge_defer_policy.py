"""JEP-226 pure defer/hold/force decision for the session-aware hedge taker."""
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.hedge_defer_policy import (
    decide_hedge_defer,
    HedgeDeferDecision,
)

FE = frozenset({"in_auction_window"})


def _d(**kw):
    base = dict(cap=30.0, halted=True, reason="in_auction_window", hard_halt=False,
                defer_since=None, defer_side="SELL", needed_side="SELL",
                now=1000.0, force_eligible_reasons=FE)
    base.update(kw)
    return decide_hedge_defer(**base)


def test_cap_zero_is_killswitch_always_place():
    d = _d(cap=0.0, halted=True, reason="vi")
    assert d == HedgeDeferDecision(place=True, since=None, side=None, kind="place")


def test_negative_cap_is_killswitch():
    d = _d(cap=-1.0, halted=True, reason="in_auction_window")
    assert d.place is True and d.kind == "place" and d.since is None


def test_not_halted_places_and_resets_timer():
    d = _d(halted=False, defer_since=900.0)
    assert d.place is True and d.kind == "place" and d.since is None and d.side is None


def test_auction_within_cap_defers():
    d = _d(defer_since=990.0, now=1000.0)        # naked_age=10 < 30
    assert d.place is False and d.kind == "defer" and d.since == 990.0 and d.side == "SELL"


def test_auction_first_tick_starts_timer_and_defers():
    d = _d(defer_since=None, now=1000.0)
    assert d.place is False and d.kind == "defer" and d.since == 1000.0 and d.side == "SELL"


def test_auction_at_cap_forces():
    d = _d(defer_since=970.0, now=1000.0)        # naked_age=30 >= 30
    assert d.place is True and d.kind == "force" and d.since == 970.0


def test_auction_past_cap_forces():
    d = _d(defer_since=900.0, now=1000.0)        # naked_age=100 >= 30
    assert d.place is True and d.kind == "force" and d.since == 900.0


def test_auction_with_hard_halt_overlap_holds_never_forces():
    # JEP-226 (challenge F1): a real halt (CB/거래정지/VI/stale) overlapping a scheduled auction
    # arrives as reason="in_auction_window" + hard_halt=True -> HOLD, never force into a non-fillable book.
    d = _d(reason="in_auction_window", hard_halt=True, defer_since=0.0, now=10_000.0)  # naked huge
    assert d.place is False and d.kind == "hold"


def test_auction_no_hard_halt_still_forces():
    d = _d(reason="in_auction_window", hard_halt=False, defer_since=900.0, now=1000.0)
    assert d.place is True and d.kind == "force"


def test_halt_reason_never_forces_even_past_cap():
    for r in ("vi", "hour_cls_auction", "market_wide_cb", "trht_halt",
              "book_frozen", "not_ready_book_stale", "not_ready_status_unconfirmed",
              "post_halt_cooldown"):
        d = _d(reason=r, defer_since=0.0, now=10_000.0)   # naked_age huge
        assert d.place is False and d.kind == "hold", r


def test_side_flip_restarts_timer_no_premature_force():
    # was deferring SELL since t=900 (past cap); pending flipped to BUY at t=1000
    d = _d(defer_side="SELL", needed_side="BUY", defer_since=900.0, now=1000.0)
    assert d.since == 1000.0 and d.side == "BUY" and d.place is False and d.kind == "defer"


def test_same_side_keeps_timer():
    d = _d(defer_side="SELL", needed_side="SELL", defer_since=995.0, now=1000.0)
    assert d.since == 995.0 and d.side == "SELL"


def test_executor_config_exposes_defer_cap_default_30():
    # JEP-226 F4: ExecutorConfigBase is pydantic v2 extra=ignore — a pass-through kwarg the
    # config class does not declare is SILENTLY DROPPED. The field must exist on the executor
    # config so getattr(config, "hedge_session_defer_cap_s") is real, not the 30.0 fallback.
    # (The controller-config is_updatable + construction pass-through are Docker-verified in Task 7.)
    from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import LadderMakerExecutorConfig
    fields = LadderMakerExecutorConfig.model_fields
    assert "hedge_session_defer_cap_s" in fields
    assert fields["hedge_session_defer_cap_s"].default == 30.0
