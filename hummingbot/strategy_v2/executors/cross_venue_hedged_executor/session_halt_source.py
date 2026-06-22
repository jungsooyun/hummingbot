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
from typing import Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class SessionHaltState:
    """Folded decision the executor consumes (-> GateContext.kis_session_halted)."""
    halted: bool
    ready: bool
    reason: str


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

    if in_auction:
        return SessionHaltState(True, ready, "in_auction_window")
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
