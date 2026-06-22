"""Unit tests for hummingbot.strategy_v2.gates.gate_chain.

Pure stdlib + pytest — no hummingbot imports, no Cython required.
Run with:
    cd hummingbot && PYENV_VERSION=py312 python -m pytest test/hummingbot/strategy_v2/gates/ -v
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from hummingbot.strategy_v2.gates.gate_chain import (
    GateContext,
    GateResult,
    GateChain,
    KillSwitchGate,
    SessionHaltGate,
    TradingHoursGate,
    StalenessGate,
    WsStalenessGate,
    OrderCapGate,
    InventoryGate,
    SessionWindow,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

KST = timezone(timedelta(hours=9))


def _kst(hour: int, minute: int = 0, second: int = 0) -> datetime:
    """Return a naive-looking but KST-aware datetime for today."""
    return datetime(2026, 6, 16, hour, minute, second, tzinfo=KST)


def _ctx(
    now_kst: datetime = None,
    kis_age_s: float = 0.0,
    hl_age_s: float = 0.0,
    fx_age_s: float = 0.0,
    kis_ws_age_s: float = 0.0,
    hl_ws_age_s: float = 0.0,
    kis_session_halted: bool = False,
    inventory: Decimal = Decimal("0"),
    open_order_count: int = 0,
    pending_notional: Decimal = Decimal("0"),
    kill_switch: bool = False,
) -> GateContext:
    if now_kst is None:
        now_kst = _kst(10, 0, 0)
    return GateContext(
        now_kst=now_kst,
        kis_age_s=kis_age_s,
        hl_age_s=hl_age_s,
        fx_age_s=fx_age_s,
        kis_ws_age_s=kis_ws_age_s,
        hl_ws_age_s=hl_ws_age_s,
        kis_session_halted=kis_session_halted,
        inventory=inventory,
        open_order_count=open_order_count,
        pending_notional=pending_notional,
        kill_switch=kill_switch,
    )


# ---------------------------------------------------------------------------
# GateResult
# ---------------------------------------------------------------------------


class TestGateResult:
    def test_open_result(self):
        r = GateResult(open=True, reason="")
        assert r.open is True
        assert r.reason == ""

    def test_closed_result(self):
        r = GateResult(open=False, reason="kill_switch")
        assert r.open is False
        assert r.reason == "kill_switch"

    def test_frozen(self):
        r = GateResult(open=True, reason="")
        with pytest.raises((AttributeError, TypeError)):
            r.open = False  # type: ignore[misc]

    def test_open_has_empty_reason(self):
        """Convention: open gates must carry empty reason string."""
        r = GateResult(open=True, reason="")
        assert r.reason == ""


# ---------------------------------------------------------------------------
# GateContext
# ---------------------------------------------------------------------------


class TestGateContext:
    def test_frozen(self):
        ctx = _ctx()
        with pytest.raises((AttributeError, TypeError)):
            ctx.kill_switch = True  # type: ignore[misc]

    def test_defaults(self):
        ctx = _ctx()
        assert ctx.kill_switch is False
        assert ctx.open_order_count == 0
        assert ctx.pending_notional == Decimal("0")


# ---------------------------------------------------------------------------
# KillSwitchGate
# ---------------------------------------------------------------------------


class TestKillSwitchGate:
    gate = KillSwitchGate()

    def test_open_when_not_triggered(self):
        r = self.gate.evaluate(_ctx(kill_switch=False))
        assert r.open is True
        assert r.reason == ""

    def test_closed_when_triggered(self):
        r = self.gate.evaluate(_ctx(kill_switch=True))
        assert r.open is False
        assert "kill_switch" in r.reason.lower()

    def test_reason_non_empty_when_closed(self):
        r = self.gate.evaluate(_ctx(kill_switch=True))
        assert r.reason != ""


# ---------------------------------------------------------------------------
# SessionHaltGate
# ---------------------------------------------------------------------------


class TestSessionHaltGate:
    gate = SessionHaltGate()

    def test_open_when_not_halted(self):
        assert self.gate.evaluate(_ctx(kis_session_halted=False)).open is True

    def test_closed_when_halted(self):
        res = self.gate.evaluate(_ctx(kis_session_halted=True))
        assert res.open is False
        assert res.reason  # non-empty

    def test_gatecontext_defaults_kis_session_halted_false(self):
        # behavior-neutral: a GateContext built without the field is not halted
        assert _ctx().kis_session_halted is False


# ---------------------------------------------------------------------------
# TradingHoursGate
# ---------------------------------------------------------------------------


class TestTradingHoursGate:
    """KRX session: 09:00–15:30 KST (inclusive start, exclusive end)."""

    krx_gate = TradingHoursGate(
        sessions=[SessionWindow(start=(9, 0), end=(15, 30))]
    )

    def test_open_during_session(self):
        r = self.krx_gate.evaluate(_ctx(now_kst=_kst(10, 0, 0)))
        assert r.open is True

    def test_open_at_session_start(self):
        r = self.krx_gate.evaluate(_ctx(now_kst=_kst(9, 0, 0)))
        assert r.open is True

    def test_closed_one_second_before_session(self):
        r = self.krx_gate.evaluate(_ctx(now_kst=_kst(8, 59, 59)))
        assert r.open is False

    def test_open_at_last_second_of_session(self):
        # 15:29:59 is still inside [09:00, 15:30)
        r = self.krx_gate.evaluate(_ctx(now_kst=_kst(15, 29, 59)))
        assert r.open is True

    def test_closed_at_session_end_minute(self):
        # 15:30:00 is at/after the end — closed
        r = self.krx_gate.evaluate(_ctx(now_kst=_kst(15, 30, 0)))
        assert r.open is False

    def test_closed_after_session(self):
        r = self.krx_gate.evaluate(_ctx(now_kst=_kst(16, 0, 0)))
        assert r.open is False

    def test_closed_reason_non_empty(self):
        r = self.krx_gate.evaluate(_ctx(now_kst=_kst(20, 0, 0)))
        assert r.open is False
        assert r.reason != ""

    def test_multiple_sessions_open_in_second(self):
        """NXT adds a second session 15:30–20:00."""
        dual_gate = TradingHoursGate(
            sessions=[
                SessionWindow(start=(9, 0), end=(15, 30)),
                SessionWindow(start=(15, 30), end=(20, 0)),
            ]
        )
        r = dual_gate.evaluate(_ctx(now_kst=_kst(17, 0, 0)))
        assert r.open is True

    def test_multiple_sessions_open_at_boundary(self):
        """15:30:00 is closed for KRX but open for NXT — gate should be open."""
        dual_gate = TradingHoursGate(
            sessions=[
                SessionWindow(start=(9, 0), end=(15, 30)),
                SessionWindow(start=(15, 30), end=(20, 0)),
            ]
        )
        r = dual_gate.evaluate(_ctx(now_kst=_kst(15, 30, 0)))
        assert r.open is True

    def test_multiple_sessions_closed_outside_all(self):
        dual_gate = TradingHoursGate(
            sessions=[
                SessionWindow(start=(9, 0), end=(15, 30)),
                SessionWindow(start=(15, 30), end=(20, 0)),
            ]
        )
        r = dual_gate.evaluate(_ctx(now_kst=_kst(20, 0, 0)))
        assert r.open is False

    def test_no_sessions_always_closed(self):
        gate = TradingHoursGate(sessions=[])
        r = gate.evaluate(_ctx(now_kst=_kst(12, 0, 0)))
        assert r.open is False


# ---------------------------------------------------------------------------
# StalenessGate
# ---------------------------------------------------------------------------


class TestStalenessGate:
    gate = StalenessGate(max_kis_age_s=5.0, max_hl_age_s=5.0, max_fx_age_s=10.0)

    def test_open_when_all_fresh(self):
        r = self.gate.evaluate(_ctx(kis_age_s=0.0, hl_age_s=0.0, fx_age_s=0.0))
        assert r.open is True

    def test_open_at_exact_threshold(self):
        # Exactly at threshold — still open (< threshold would be OK,
        # but = threshold we treat as still valid; closed only when > threshold)
        r = self.gate.evaluate(_ctx(kis_age_s=5.0, hl_age_s=5.0, fx_age_s=10.0))
        assert r.open is True

    def test_closed_kis_stale(self):
        r = self.gate.evaluate(_ctx(kis_age_s=5.001, hl_age_s=0.0, fx_age_s=0.0))
        assert r.open is False
        assert "kis" in r.reason.lower()

    def test_closed_hl_stale(self):
        r = self.gate.evaluate(_ctx(kis_age_s=0.0, hl_age_s=5.001, fx_age_s=0.0))
        assert r.open is False
        assert "hl" in r.reason.lower()

    def test_closed_fx_stale(self):
        r = self.gate.evaluate(_ctx(kis_age_s=0.0, hl_age_s=0.0, fx_age_s=10.001))
        assert r.open is False
        assert "fx" in r.reason.lower()

    def test_closed_multiple_stale_reports_first(self):
        """When multiple feeds are stale the reason mentions at least one."""
        r = self.gate.evaluate(_ctx(kis_age_s=99.0, hl_age_s=99.0, fx_age_s=99.0))
        assert r.open is False
        assert r.reason != ""

    def test_optional_feeds_skipped_with_none_max(self):
        """Setting a max to None disables that feed check."""
        gate = StalenessGate(max_kis_age_s=5.0, max_hl_age_s=None, max_fx_age_s=None)
        # hl and fx are wildly stale but should be ignored
        r = gate.evaluate(_ctx(kis_age_s=0.0, hl_age_s=9999.0, fx_age_s=9999.0))
        assert r.open is True


# ---------------------------------------------------------------------------
# WsStalenessGate
# ---------------------------------------------------------------------------


class TestWsStalenessGate:
    gate = WsStalenessGate(max_kis_ws_age_s=3.0, max_hl_ws_age_s=12.0)

    def test_open_when_all_fresh(self):
        assert self.gate.evaluate(_ctx(kis_ws_age_s=0.0, hl_ws_age_s=0.0)).open is True

    def test_open_at_exact_threshold(self):
        assert self.gate.evaluate(_ctx(kis_ws_age_s=3.0, hl_ws_age_s=12.0)).open is True

    def test_closed_kis_ws_stale(self):
        r = self.gate.evaluate(_ctx(kis_ws_age_s=3.001, hl_ws_age_s=0.0))
        assert r.open is False and "kis" in r.reason.lower()

    def test_unproven_inf_age_closes(self):
        assert self.gate.evaluate(_ctx(kis_ws_age_s=float("inf"))).open is False

    def test_none_max_disables(self):
        g = WsStalenessGate(max_kis_ws_age_s=None, max_hl_ws_age_s=None)
        assert g.evaluate(_ctx(kis_ws_age_s=9999.0, hl_ws_age_s=9999.0)).open is True


# ---------------------------------------------------------------------------
# OrderCapGate
# ---------------------------------------------------------------------------


class TestOrderCapGate:
    gate = OrderCapGate(max_open_orders=4, max_pending_notional=Decimal("1000"))

    def test_open_below_both_caps(self):
        r = self.gate.evaluate(_ctx(open_order_count=3, pending_notional=Decimal("999")))
        assert r.open is True

    def test_open_at_count_just_below_cap(self):
        r = self.gate.evaluate(_ctx(open_order_count=3, pending_notional=Decimal("0")))
        assert r.open is True

    def test_closed_at_count_cap(self):
        r = self.gate.evaluate(_ctx(open_order_count=4, pending_notional=Decimal("0")))
        assert r.open is False
        assert r.reason != ""

    def test_closed_over_count_cap(self):
        r = self.gate.evaluate(_ctx(open_order_count=10, pending_notional=Decimal("0")))
        assert r.open is False

    def test_open_at_notional_just_below_cap(self):
        r = self.gate.evaluate(_ctx(open_order_count=0, pending_notional=Decimal("999.99")))
        assert r.open is True

    def test_closed_at_notional_cap(self):
        r = self.gate.evaluate(_ctx(open_order_count=0, pending_notional=Decimal("1000")))
        assert r.open is False
        assert r.reason != ""

    def test_closed_over_notional_cap(self):
        r = self.gate.evaluate(_ctx(open_order_count=0, pending_notional=Decimal("1001")))
        assert r.open is False

class TestInventoryGate:
    gate = InventoryGate(max_inventory=Decimal("8"))

    def test_open_below_cap(self):
        assert self.gate.evaluate(_ctx(inventory=Decimal("7"))).open is True

    def test_open_below_cap_short(self):
        assert self.gate.evaluate(_ctx(inventory=Decimal("-7"))).open is True

    def test_closed_at_cap_long(self):
        r = self.gate.evaluate(_ctx(inventory=Decimal("8")))
        assert r.open is False and r.reason != ""

    def test_closed_at_cap_short(self):
        r = self.gate.evaluate(_ctx(inventory=Decimal("-8")))
        assert r.open is False and r.reason != ""

    def test_closed_over_cap(self):
        assert self.gate.evaluate(_ctx(inventory=Decimal("12"))).open is False

    def test_none_disables(self):
        gate = InventoryGate(max_inventory=None)
        assert gate.evaluate(_ctx(inventory=Decimal("999999"))).open is True


class TestOrderCapGateExtra:
    def test_count_only_cap(self):
        gate = OrderCapGate(max_open_orders=2, max_pending_notional=None)
        r = gate.evaluate(_ctx(open_order_count=2, pending_notional=Decimal("999999")))
        assert r.open is False

    def test_notional_only_cap(self):
        gate = OrderCapGate(max_open_orders=None, max_pending_notional=Decimal("500"))
        r = gate.evaluate(_ctx(open_order_count=999, pending_notional=Decimal("499")))
        assert r.open is True

    def test_both_none_always_open(self):
        gate = OrderCapGate(max_open_orders=None, max_pending_notional=None)
        r = gate.evaluate(_ctx(open_order_count=999, pending_notional=Decimal("999999")))
        assert r.open is True


# ---------------------------------------------------------------------------
# GateChain — AND-composition, first-closed-reason
# ---------------------------------------------------------------------------


class TestGateChain:
    def test_empty_chain_open(self):
        chain = GateChain(gates=[])
        r = chain.evaluate(_ctx())
        assert r.open is True

    def test_single_open_gate(self):
        chain = GateChain(gates=[KillSwitchGate()])
        r = chain.evaluate(_ctx(kill_switch=False))
        assert r.open is True

    def test_single_closed_gate(self):
        chain = GateChain(gates=[KillSwitchGate()])
        r = chain.evaluate(_ctx(kill_switch=True))
        assert r.open is False

    def test_all_open_gates(self):
        krx = TradingHoursGate(sessions=[SessionWindow(start=(9, 0), end=(15, 30))])
        chain = GateChain(gates=[KillSwitchGate(), krx])
        r = chain.evaluate(_ctx(now_kst=_kst(10, 0), kill_switch=False))
        assert r.open is True

    def test_first_gate_closed_returns_its_reason(self):
        krx = TradingHoursGate(sessions=[SessionWindow(start=(9, 0), end=(15, 30))])
        chain = GateChain(gates=[KillSwitchGate(), krx])
        r = chain.evaluate(_ctx(now_kst=_kst(10, 0), kill_switch=True))
        assert r.open is False
        assert "kill_switch" in r.reason.lower()

    def test_second_gate_closed_returns_its_reason(self):
        krx = TradingHoursGate(sessions=[SessionWindow(start=(9, 0), end=(15, 30))])
        chain = GateChain(gates=[KillSwitchGate(), krx])
        r = chain.evaluate(_ctx(now_kst=_kst(20, 0), kill_switch=False))
        assert r.open is False
        # Reason should NOT mention kill_switch (that gate passed)
        assert "kill_switch" not in r.reason.lower()

    def test_first_closed_wins_over_later_closed(self):
        """Chain stops at first closed gate — first-closed-reason ordering."""
        krx = TradingHoursGate(sessions=[SessionWindow(start=(9, 0), end=(15, 30))])
        chain = GateChain(gates=[KillSwitchGate(), krx])
        # Both gates closed: kill_switch is first — its reason must be reported
        r = chain.evaluate(_ctx(now_kst=_kst(20, 0), kill_switch=True))
        assert r.open is False
        assert "kill_switch" in r.reason.lower()

    def test_full_chain_open(self):
        krx = TradingHoursGate(sessions=[SessionWindow(start=(9, 0), end=(15, 30))])
        staleness = StalenessGate(max_kis_age_s=5.0, max_hl_age_s=5.0, max_fx_age_s=10.0)
        cap = OrderCapGate(max_open_orders=4, max_pending_notional=Decimal("1000"))
        chain = GateChain(gates=[KillSwitchGate(), krx, staleness, cap])
        r = chain.evaluate(
            _ctx(
                now_kst=_kst(10, 0),
                kill_switch=False,
                kis_age_s=1.0,
                hl_age_s=1.0,
                fx_age_s=1.0,
                open_order_count=2,
                pending_notional=Decimal("500"),
            )
        )
        assert r.open is True


# ---------------------------------------------------------------------------
# GateChain.diagnose — per-gate results
# ---------------------------------------------------------------------------


class TestGateChainDiagnose:
    def test_diagnose_returns_one_result_per_gate(self):
        chain = GateChain(gates=[KillSwitchGate(), KillSwitchGate()])
        results = chain.diagnose(_ctx(kill_switch=False))
        assert len(results) == 2

    def test_diagnose_all_open(self):
        chain = GateChain(gates=[KillSwitchGate()])
        results = chain.diagnose(_ctx(kill_switch=False))
        assert all(r.open for r in results)

    def test_diagnose_mixed(self):
        krx = TradingHoursGate(sessions=[SessionWindow(start=(9, 0), end=(15, 30))])
        chain = GateChain(gates=[KillSwitchGate(), krx])
        results = chain.diagnose(_ctx(now_kst=_kst(20, 0), kill_switch=True))
        # KillSwitchGate → closed, TradingHoursGate → closed
        assert results[0].open is False
        assert results[1].open is False

    def test_diagnose_does_not_short_circuit(self):
        """diagnose() evaluates ALL gates even when an early one is closed."""
        krx = TradingHoursGate(sessions=[SessionWindow(start=(9, 0), end=(15, 30))])
        chain = GateChain(gates=[KillSwitchGate(), krx])
        results = chain.diagnose(_ctx(now_kst=_kst(20, 0), kill_switch=True))
        assert len(results) == 2  # both gates evaluated

    def test_diagnose_independent_of_evaluate(self):
        """diagnose() and evaluate() agree on open/closed for same context."""
        chain = GateChain(gates=[KillSwitchGate()])
        ctx_open = _ctx(kill_switch=False)
        ctx_closed = _ctx(kill_switch=True)
        assert chain.evaluate(ctx_open).open == chain.diagnose(ctx_open)[0].open
        assert chain.evaluate(ctx_closed).open == chain.diagnose(ctx_closed)[0].open
