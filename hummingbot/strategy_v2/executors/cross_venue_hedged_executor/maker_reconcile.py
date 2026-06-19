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
from typing import List


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
