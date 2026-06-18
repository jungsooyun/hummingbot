"""Composable safety-gate module for hummingbot V2 strategies.

Mirrors the purity of ``ladder_policy.py`` — STDLIB ONLY (dataclasses, enum,
datetime, decimal, typing).  No hummingbot imports, no I/O.  Unit-testable
without the Cython runtime.

Public API
----------
GateResult          — immutable result of one gate evaluation.
GateContext         — immutable snapshot of runtime inputs passed to gates.
Gate                — Protocol every gate must satisfy.
SessionWindow       — (start_hour, start_min) / (end_hour, end_min) window.
KillSwitchGate      — closes when ctx.kill_switch is True.
TradingHoursGate    — closes outside all configured SessionWindow(s).
StalenessGate       — closes when any feed's age exceeds its max threshold.
OrderCapGate        — closes when open-order count or pending notional hits cap.
GateChain           — ordered AND-chain; evaluate() short-circuits on first
                      closed gate; diagnose() runs all gates for logging.

Time convention
---------------
``GateContext.now_kst`` is a ``datetime`` object that **must carry timezone
info** (e.g. ``tzinfo=timezone(timedelta(hours=9))``).  The ``TradingHoursGate``
extracts (hour, minute, second) directly from it — no further conversion is
done inside the gate, so callers are responsible for supplying KST time when
checking KRX / NXT session windows.

Threshold semantics
-------------------
StalenessGate: age > max_age_s → closed (age == max_age_s is still open).
OrderCapGate:  count >= max OR notional >= max → closed (just-below is open).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Protocol, Tuple, runtime_checkable


# ---------------------------------------------------------------------------
# Core value types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GateResult:
    """Immutable result of one gate evaluation.

    Invariant: ``open is True`` implies ``reason == ""``.
    """

    open: bool
    reason: str


_OPEN = GateResult(open=True, reason="")


@dataclass(frozen=True)
class GateContext:
    """Immutable snapshot of all inputs a gate may inspect.

    Parameters
    ----------
    now_kst:
        Current wall-clock time in KST (must be timezone-aware).
    kis_age_s:
        Seconds since the last KIS best-bid/ask update.
    hl_age_s:
        Seconds since the last Hyperliquid best-bid/ask update.
    fx_age_s:
        Seconds since the last FX rate update.
    inventory:
        Current net inventory (positive = long).
    open_order_count:
        Number of resting orders currently tracked.
    pending_notional:
        Sum of (price × qty) for all open orders (quote currency).
    kill_switch:
        When True all trading must halt immediately.
    """

    now_kst: datetime
    kis_age_s: float
    hl_age_s: float
    fx_age_s: float
    inventory: Decimal
    open_order_count: int
    pending_notional: Decimal
    kill_switch: bool


# ---------------------------------------------------------------------------
# Gate Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Gate(Protocol):
    """A single safety predicate.  Must be stateless — all inputs via ctx."""

    def evaluate(self, ctx: GateContext) -> GateResult:
        ...


# ---------------------------------------------------------------------------
# Session window helper
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SessionWindow:
    """Half-open [start, end) time window expressed as (hour, minute) tuples.

    Example::

        SessionWindow(start=(9, 0), end=(15, 30))  # KRX: 09:00 ≤ t < 15:30 KST
        SessionWindow(start=(15, 30), end=(20, 0)) # NXT: 15:30 ≤ t < 20:00 KST
    """

    start: Tuple[int, int]  # (hour, minute)
    end: Tuple[int, int]    # (hour, minute)

    def contains(self, hour: int, minute: int, second: int) -> bool:
        """Return True if (hour, minute, second) falls in [start, end)."""
        # Convert to comparable integer seconds-since-midnight
        t = hour * 3600 + minute * 60 + second
        s = self.start[0] * 3600 + self.start[1] * 60
        e = self.end[0] * 3600 + self.end[1] * 60
        return s <= t < e

    def seconds_to_end(self, hour: int, minute: int, second: int) -> Optional[float]:
        """Remaining seconds until the window's end, or None if outside [start, end)."""
        if not self.contains(hour, minute, second):
            return None
        t = hour * 3600 + minute * 60 + second
        e = self.end[0] * 3600 + self.end[1] * 60
        return float(e - t)

    def minutes_to_end(self, hour: int, minute: int, second: int) -> Optional[float]:
        """Remaining minutes until the window's end, or None if outside [start, end)."""
        secs = self.seconds_to_end(hour, minute, second)
        return None if secs is None else secs / 60.0


# ---------------------------------------------------------------------------
# Concrete gates
# ---------------------------------------------------------------------------


class KillSwitchGate:
    """Closes immediately when ``ctx.kill_switch`` is True."""

    def evaluate(self, ctx: GateContext) -> GateResult:
        if ctx.kill_switch:
            return GateResult(open=False, reason="kill_switch=True")
        return _OPEN


class TradingHoursGate:
    """Opens only when ``ctx.now_kst`` falls inside at least one SessionWindow.

    An empty sessions list means never-open (always closed).

    Parameters
    ----------
    sessions:
        Ordered list of ``SessionWindow`` objects.  The gate is open when
        ``ctx.now_kst`` falls inside ANY window (OR across windows).
    """

    def __init__(self, sessions: List[SessionWindow]) -> None:
        self._sessions: List[SessionWindow] = list(sessions)

    def evaluate(self, ctx: GateContext) -> GateResult:
        dt = ctx.now_kst
        h, m, s = dt.hour, dt.minute, dt.second
        for window in self._sessions:
            if window.contains(h, m, s):
                return _OPEN
        if self._sessions:
            windows_str = ", ".join(
                f"{w.start[0]:02d}:{w.start[1]:02d}-{w.end[0]:02d}:{w.end[1]:02d}"
                for w in self._sessions
            )
            reason = f"outside_trading_hours: {h:02d}:{m:02d}:{s:02d} KST not in [{windows_str}]"
        else:
            reason = "outside_trading_hours: no sessions configured"
        return GateResult(open=False, reason=reason)


class StalenessGate:
    """Closes when any monitored feed's data age exceeds its threshold.

    Parameters
    ----------
    max_kis_age_s:
        Maximum acceptable KIS data age in seconds.  ``None`` disables.
    max_hl_age_s:
        Maximum acceptable Hyperliquid data age in seconds.  ``None`` disables.
    max_fx_age_s:
        Maximum acceptable FX data age in seconds.  ``None`` disables.

    Threshold semantics: age > max → closed; age == max → open.
    """

    def __init__(
        self,
        max_kis_age_s: Optional[float],
        max_hl_age_s: Optional[float],
        max_fx_age_s: Optional[float],
    ) -> None:
        self._max_kis = max_kis_age_s
        self._max_hl = max_hl_age_s
        self._max_fx = max_fx_age_s

    def evaluate(self, ctx: GateContext) -> GateResult:
        checks: List[Tuple[Optional[float], float, str]] = [
            (self._max_kis, ctx.kis_age_s, "kis"),
            (self._max_hl, ctx.hl_age_s, "hl"),
            (self._max_fx, ctx.fx_age_s, "fx"),
        ]
        for max_age, age, label in checks:
            if max_age is not None and age > max_age:
                return GateResult(
                    open=False,
                    reason=f"stale_{label}: age={age:.3f}s > max={max_age:.3f}s",
                )
        return _OPEN


class OrderCapGate:
    """Closes when open-order count or pending notional reaches its cap.

    Parameters
    ----------
    max_open_orders:
        Maximum number of resting orders allowed (inclusive cap: >= closes).
        ``None`` disables this check.
    max_pending_notional:
        Maximum sum of (price × qty) across open orders (inclusive cap: >= closes).
        ``None`` disables this check.
    """

    def __init__(
        self,
        max_open_orders: Optional[int],
        max_pending_notional: Optional[Decimal],
    ) -> None:
        self._max_orders = max_open_orders
        self._max_notional = max_pending_notional

    def evaluate(self, ctx: GateContext) -> GateResult:
        if self._max_orders is not None and ctx.open_order_count >= self._max_orders:
            return GateResult(
                open=False,
                reason=(
                    f"order_cap: open_order_count={ctx.open_order_count}"
                    f" >= max={self._max_orders}"
                ),
            )
        if self._max_notional is not None and ctx.pending_notional >= self._max_notional:
            return GateResult(
                open=False,
                reason=(
                    f"notional_cap: pending_notional={ctx.pending_notional}"
                    f" >= max={self._max_notional}"
                ),
            )
        return _OPEN


class InventoryGate:
    """Closes when the absolute net inventory reaches its cap (inclusive: >= closes).

    A HARD unhedged-exposure safety pin. With a single-direction maker ladder, every
    maker fill grows the unhedged (naked) inventory until it is hedged; nothing else
    hard-stops quoting on inventory (inventory only drives price skew). This gate halts
    quoting once ``|inventory|`` reaches ``max_inventory``, so naked exposure cannot grow
    past it — independent of, and faster than, the hedge kill-switch (which only trips
    after a streak of hedge failures). As the position is hedged back below the cap the
    gate reopens. ``None`` disables the check.

    Parameters
    ----------
    max_inventory:
        Maximum absolute net inventory allowed (inclusive cap: >= closes). ``None``
        disables this check.
    """

    def __init__(self, max_inventory: Optional[Decimal]) -> None:
        self._max_inventory = max_inventory

    def evaluate(self, ctx: GateContext) -> GateResult:
        if self._max_inventory is not None and abs(ctx.inventory) >= self._max_inventory:
            return GateResult(
                open=False,
                reason=(
                    f"inventory_cap: |inventory|={abs(ctx.inventory)}"
                    f" >= max={self._max_inventory}"
                ),
            )
        return _OPEN


# ---------------------------------------------------------------------------
# GateChain
# ---------------------------------------------------------------------------


class GateChain:
    """Ordered AND-chain of gates.

    ``evaluate()`` returns the first closed ``GateResult`` encountered
    (short-circuit).  If all gates pass, returns an open result.

    ``diagnose()`` evaluates **all** gates without short-circuiting and
    returns a per-gate list for logging / dashboards.

    Parameters
    ----------
    gates:
        Ordered sequence of gate instances.  Evaluated left-to-right.
    """

    def __init__(self, gates: List[Gate]) -> None:
        self._gates: List[Gate] = list(gates)

    def evaluate(self, ctx: GateContext) -> GateResult:
        for gate in self._gates:
            result = gate.evaluate(ctx)
            if not result.open:
                return result
        return _OPEN

    def diagnose(self, ctx: GateContext) -> List[GateResult]:
        """Evaluate every gate and return one GateResult per gate."""
        return [gate.evaluate(ctx) for gate in self._gates]
