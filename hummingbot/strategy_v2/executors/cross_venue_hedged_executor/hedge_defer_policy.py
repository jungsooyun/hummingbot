"""JEP-226 pure decision for session/auction-aware hedge gating.

Stdlib-only and type-agnostic for the side values (compared by equality only), so it
is unit-testable under plain py312 without the Cython / V2 stack. The executor owns the
timer STATE (``_hedge_defer_since_ts`` / ``_hedge_defer_side``) and the logging; this
module is the pure decision over (halt state, timer, cap).

Policy (see spec 2026-06-26-jep226-session-aware-hedge-taker-design):
  * ``cap <= 0`` disables the gate entirely (always place; behavior-neutral kill-switch).
  * Otherwise, while ``halted``:
      - a clock-scheduled auction (``reason in force_eligible_reasons``, ==
        ``{"in_auction_window"}``) is DEFERRED up to ``cap`` seconds of CONTINUOUS
        same-side naked age, then FORCED (accept the auction / 단일가 match);
      - any other halted reason is HELD (never forced — a taker cannot fill into a real
        halt, and forcing a VI / 예상가 single-price state risks a violent-move match).
The timer restarts on a side flip so an opposite-side pending never inherits the
previous side's elapsed naked age.
"""
from __future__ import annotations

from typing import Any, FrozenSet, NamedTuple, Optional


class HedgeDeferDecision(NamedTuple):
    place: bool             # True -> proceed to size + place; False -> hold pending
    since: Optional[float]  # updated _hedge_defer_since_ts (None == reset/unblocked)
    side: Any               # updated _hedge_defer_side (None == reset)
    kind: str               # "place" | "defer" | "force" | "hold" (for one-shot logging)


def decide_hedge_defer(
    *,
    cap: float,
    halted: bool,
    reason: str,
    hard_halt: bool,
    defer_since: Optional[float],
    defer_side: Any,
    needed_side: Any,
    now: float,
    force_eligible_reasons: FrozenSet[str],
) -> HedgeDeferDecision:
    if cap <= 0 or not halted:
        return HedgeDeferDecision(place=True, since=None, side=None, kind="place")
    # blocked: maintain the same-side naked-age timer (restart on side flip / first entry)
    since, side = defer_since, defer_side
    if since is None or side != needed_side:
        since, side = now, needed_side
    naked_age = now - since
    # Force ONLY into a clock-scheduled auction with NO overlapping hard halt: in_auction is
    # checked before cb/trht/vi/not-ready in compute_session_halt, so a real halt overlapping a
    # scheduled window arrives as reason="in_auction_window" with hard_halt=True -> hold, never
    # force into a non-fillable book.
    if reason in force_eligible_reasons and not hard_halt:
        if naked_age >= cap:
            return HedgeDeferDecision(place=True, since=since, side=side, kind="force")
        return HedgeDeferDecision(place=False, since=since, side=side, kind="defer")
    # non-auction halt, OR a scheduled auction overlapping a hard halt: hold, never force
    return HedgeDeferDecision(place=False, since=since, side=side, kind="hold")
