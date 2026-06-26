"""Session-halt seam for the cross-venue hedged executor (JEP-198).

PURE-POLICY MODULE. Mirrors the JEP-187 SessionCalendar / FairPriceProvider seam:
the KIS connector PARSES raw WS halt signals; this module FOLDS them (with the
strategy clock + freshness thresholds) into a single halted/ready decision the
executor passes into GateContext.kis_session_halted.

Fail-safe by construction: under-detect (gate wrongly open) = naked exposure is the
failure designed out; over-pause = lost MM uptime is acceptable. Any missing /
stale / unknown / unconfirmed signal resolves to halted=True.

Stdlib-only (the signals object is duck-typed), so it is unit-testable under plain
py312 without the Cython / V2 stack, and introduces no connector -> strategy import.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, Tuple, runtime_checkable


@dataclass(frozen=True)
class SessionHaltState:
    """Folded decision the executor consumes (-> GateContext.kis_session_halted)."""
    halted: bool
    ready: bool
    reason: str
    # JEP-226: True when a genuine non-fillable halt (CB / 거래정지 / VI / stale-or-unconfirmed
    # book) is present. ``in_auction`` is checked FIRST in compute_session_halt (so a scheduled
    # auction overlapping a real halt is still reasoned as "in_auction_window" for the maker
    # book_frozen suppression), but the hedge force-eligibility must NOT force into such an
    # overlap -> it reads this flag. Defaults False; only the in_auction return needs it accurate.
    hard_halt: bool = False


def compute_session_halt(
    sig,
    *,
    in_auction: bool,
    max_ws_age_s: float,
    max_book_static_s: float,
) -> SessionHaltState:
    """Fold raw KIS halt signals + clock + thresholds into a halted/ready decision.

    ``sig`` is any object exposing: hour_cls_auction(bool), book_age_sec(Optional[float]),
    book_static_sec(Optional[float]), trht_halted(bool), cb_latched(bool),
    vi_latched(bool), market_status_ready(bool). Clocks are monotonic seconds.

    halted = in_auction OR (not ready) OR cb OR trht OR hour_cls_auction OR vi OR book_frozen
    ready  = book_fresh AND market_status_ready
    book_frozen = (not in_auction) AND book_fresh AND book_static_sec > max_book_static_s
                  # clock-gated so a scheduled-auction static book is not mislabelled;
                  # only fires for UNSCHEDULED freezes (CB / 거래정지)
    """
    book_fresh = sig.book_age_sec is not None and sig.book_age_sec <= max_ws_age_s
    book_frozen = (
        not in_auction
        and book_fresh
        and sig.book_static_sec is not None
        and sig.book_static_sec > max_book_static_s
    )
    ready = book_fresh and sig.market_status_ready
    # JEP-226: a genuine non-fillable halt present regardless of the scheduled-auction clock.
    # book_frozen is excluded here: it is (not in_auction)-gated and a static book DURING a
    # scheduled auction is normal, not a freeze (see test_book_frozen_suppressed_during_auction).
    hard_halt = bool(sig.cb_latched or sig.trht_halted or sig.vi_latched or not ready)

    if in_auction:
        return SessionHaltState(True, ready, "in_auction_window", hard_halt=hard_halt)
    if not ready:
        reason = "not_ready_book_stale" if not book_fresh else "not_ready_status_unconfirmed"
        return SessionHaltState(True, ready, reason)
    if sig.cb_latched:
        return SessionHaltState(True, ready, "market_wide_cb")
    if sig.trht_halted:
        return SessionHaltState(True, ready, "trht_halt")
    if sig.hour_cls_auction:
        return SessionHaltState(True, ready, "hour_cls_auction")
    if sig.vi_latched:
        return SessionHaltState(True, ready, "vi")
    if book_frozen:
        return SessionHaltState(True, ready, "book_frozen")
    return SessionHaltState(False, ready, "")


# JEP-198 interim post-halt cooldown (until JEP-201 H0STMKO0 decode is verified + enabled).
# Reasons that signal an UNSCHEDULED trading halt whose taker-unavailable window can persist
# into a single-price (단일가) auction the clock-based calendar can't see — a market-wide CB
# enters a ~10min 예상체결 auction whose UPDATING book would otherwise un-freeze the gate and
# re-open quoting while the KIS taker hedge is still impossible. NOT armed by:
#   * not_ready_* (WS staleness) — recovers in seconds, would over-pause 30min on every blip;
#   * in_auction_window / hour_cls_auction — SCHEDULED auctions self-clear via the calendar.
# In Phase 1 (H0STMKO0 off) only ``book_frozen`` actually fires; cb/trht/vi arm too once Phase 2
# is enabled (belt-and-suspenders alongside the explicit latch).
COOLDOWN_ARM_REASONS = frozenset({"book_frozen", "market_wide_cb", "trht_halt", "vi"})

# JEP-226: the ONLY reason a forced taker may be placed into — the deterministic
# clock-scheduled single-price auction, where the match price is well-defined and
# time-invariant within the window. compute_session_halt checks `in_auction` FIRST,
# so every clock window (open/close/시간외단일가) surfaces as exactly this reason.
# Every OTHER halted reason is a connector-flagged signal that only appears OUTSIDE a
# clock window (hour_cls_auction=='C' 예상가·VI단일가, checked before vi_latched; vi;
# market_wide_cb; trht_halt; book_frozen; not_ready_*; post_halt_cooldown) -> hold,
# never force (cannot fill into a real halt, or risks a VI/예상가 single-price match).
FORCE_ELIGIBLE_HALT_REASONS = frozenset({"in_auction_window"})


def apply_post_halt_cooldown(
    state: SessionHaltState,
    *,
    now: float,
    cooldown_until: float,
    cooldown_s: float,
) -> Tuple[SessionHaltState, float, bool]:
    """Fold a sticky post-halt cooldown onto a per-tick ``compute_session_halt`` decision.

    Once an unscheduled freeze/halt is seen (``state.reason`` in ``COOLDOWN_ARM_REASONS``),
    hold ``halted=True`` for ``cooldown_s`` seconds AFTER the book stops looking frozen, so the
    CB single-price-auction phase cannot re-open the gate while the taker hedge is unavailable.
    The window is EXTENDED on every frozen tick, so it always covers ``cooldown_s`` past the
    freeze's END (conservative — safe for halts longer than ``cooldown_s``).

    Pure: ``now``/``cooldown_until`` are passed in (no wall-clock read), so the caller owns the
    state across ticks. ``cooldown_s <= 0`` disables it (behavior-neutral). Returns
    ``(effective_state, new_cooldown_until, armed_now)``; ``armed_now`` is True only on the
    inactive→active transition, for one-shot logging.
    """
    new_until = cooldown_until
    armed_now = False
    if state.halted and cooldown_s > 0 and state.reason in COOLDOWN_ARM_REASONS:
        candidate = now + cooldown_s
        if candidate > new_until:
            armed_now = new_until <= now   # arming from an inactive (expired) cooldown
            new_until = candidate
    if not state.halted and now < new_until:
        return SessionHaltState(True, state.ready, "post_halt_cooldown"), new_until, armed_now
    return state, new_until, armed_now


@runtime_checkable
class SessionHaltSource(Protocol):
    def evaluate(
        self, trading_pair: str, *, in_auction: bool,
        max_ws_age_s: float, max_book_static_s: float,
    ) -> SessionHaltState:
        ...


class NoHaltSource:
    """Behavior-neutral default: never halts, always ready (disabled gate / non-KIS venue)."""

    def evaluate(self, trading_pair, *, in_auction, max_ws_age_s, max_book_static_s) -> SessionHaltState:
        return SessionHaltState(False, True, "")


class KisSessionHaltSource:
    """Reads raw per-pair signals from a KIS connector and folds them via compute_session_halt."""

    def __init__(self, connector) -> None:
        self._connector = connector

    def evaluate(self, trading_pair, *, in_auction, max_ws_age_s, max_book_static_s) -> SessionHaltState:
        sig = self._connector.get_session_halt_signals(trading_pair)
        return compute_session_halt(
            sig, in_auction=in_auction,
            max_ws_age_s=max_ws_age_s, max_book_static_s=max_book_static_s,
        )
