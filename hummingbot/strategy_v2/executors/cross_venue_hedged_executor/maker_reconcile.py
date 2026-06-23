"""Generic maker-order partial-reprice primitives, shared by hedge executors.

Pure value types + the diff function used to reprice only the rungs that actually changed
(selective cancel/replace instead of cancel-all + place-all). No I/O, no hummingbot runtime
imports — stdlib only — so it lives in the executor base layer and is unit-testable without
the Cython runtime.

Relocated from ``ladder_maker_executor.ladder_policy`` (JEP-145) so
``CrossVenueHedgedExecutorBase`` can use it WITHOUT importing a subclass package; ladder_policy
re-exports these names for backward compatibility.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import List, Tuple


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class RungTarget:
    side: Side
    price: Decimal
    size: Decimal
    edge_bps: Decimal


@dataclass(frozen=True)
class RestingOrder:
    """Snapshot of one live maker order, for the partial-reprice diff."""

    order_id: str
    side: Side
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class LadderDiff:
    """Result of diffing live maker orders against this tick's targets.

    ``to_cancel`` holds the order ids to cancel; ``to_place`` holds the targets
    with no acceptable live match. Orders that match a target within the reprice
    threshold (same size, price drift < threshold) are left untouched.
    """

    to_cancel: List[str]
    to_place: List[RungTarget]


def diff_ladder_targets(
    current: List[RestingOrder],
    targets: List[RungTarget],
    tick: Decimal,
    reprice_threshold_ticks: Decimal,
) -> LadderDiff:
    """Partial reprice: cancel/replace only the rungs that actually changed.

    A live order *matches* a target when it has the SAME size and its price is
    within ``reprice_threshold_ticks * tick`` of the target price (strictly less
    than the threshold — at/over the threshold is a reprice, mirroring the
    executor's ``min_reprice_delta_ticks`` guard). Matching is greedy per target,
    preferring the closest price then the lowest order id, so the result is
    deterministic.

    Unmatched targets are placed; unmatched live orders are cancelled. With no
    live orders this places everything; with empty targets it cancels everything.
    """
    threshold = reprice_threshold_ticks * tick
    unmatched = list(current)
    to_place: List[RungTarget] = []
    matched_ids: set[str] = set()

    for target in targets:
        candidates = [
            o
            for o in unmatched
            if o.side == target.side and o.size == target.size and abs(o.price - target.price) < threshold
        ]
        if not candidates:
            to_place.append(target)
            continue
        best = min(candidates, key=lambda o: (abs(o.price - target.price), o.order_id))
        matched_ids.add(best.order_id)
        unmatched = [o for o in unmatched if o.order_id != best.order_id]

    to_cancel = [o.order_id for o in current if o.order_id not in matched_ids]
    return LadderDiff(to_cancel=to_cancel, to_place=to_place)


def blocked_targets(
    to_place: List[RungTarget],
    inflight_rungs: List[Tuple[Side, Decimal]],
    radius: Decimal,
    granular: bool,
) -> List[RungTarget]:
    """Drop the ``to_place`` targets that conflict with an unmatched in-flight maker rung.

    A maker reconcile injects each unmatched (still-cancelling) in-flight rung as
    ``(side, quantized_price)``. The round-2 double-place (JEP-172 #5) happened when the
    fair moved beyond the reprice threshold OR a partial fill resized a rung: the in-flight
    rung matched no target, so its cancel was *skipped* (an inflight order is never cancelled —
    its create task may still be racing the exchange), yet the moved/resized target was placed
    -> two live orders ~one rung apart on a single side -> over-close past flat -> net-long.

    Two suppression modes:

    * ``granular=False`` (conservative default, behavior-neutral): block EVERY same-side
      target while any unmatched in-flight rung exists on that side — identical to the
      whole-side ``blocked_sides`` set that shipped in JEP-172 #5. The cost is a genuinely-new
      non-conflicting same-side rung is deferred ~1 tick (under-quote only, self-heals when the
      in-flight order's created event lands).

    * ``granular=True`` (JEP-177 Fix #5 opt-in): block only the targets that actually COLLIDE
      with an unmatched in-flight rung — ``|target.price - inflight_price| <= radius`` on the
      SAME side. ``radius`` is the price window (the executor passes
      ``maker_inflight_block_radius_ticks * maker_tick``, default 4 ticks, which still covers
      both round-2 cases: same-price/size-shift -> 0 distance, and a fair-move up to the radius).
      A genuinely-new same-side rung farther than ``radius`` from every unmatched in-flight rung
      is placed this tick instead of being deferred.

    Suppression is always side-scoped: an in-flight SELL rung never blocks a BUY target. The
    boundary is inclusive (``<=``) so a target exactly ``radius`` away is treated as a collision
    and blocked — never under-protect at the edge (mirrors the >= reprice-threshold guard).
    """
    if not inflight_rungs:
        return list(to_place)
    blocked_side_set = {side for side, _ in inflight_rungs}
    kept: List[RungTarget] = []
    for target in to_place:
        if target.side not in blocked_side_set:
            kept.append(target)
            continue
        if not granular:
            # Whole-side block: any same-side in-flight rung suppresses this target.
            continue
        collides = any(
            side == target.side and abs(price - target.price) <= radius
            for side, price in inflight_rungs
        )
        if collides:
            continue
        kept.append(target)
    return kept
