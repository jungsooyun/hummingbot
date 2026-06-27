"""JEP-231 Task 6: out-of-session hedge submission suppression.

New hedge placements must be suppressed when _session_in_session=False.
Pending residual must be preserved (익일 in-session에 재개).
"""
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    _IMPORTABLE = True
except Exception:  # pragma: no cover
    LadderMakerExecutor = None
    _IMPORTABLE = False

ZERO = Decimal("0")


def _make_executor(session_in_session=True):
    """Minimal executor for _process_hedges session-guard tests."""
    ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
    ex.config = SimpleNamespace(
        kill_switch=False,
        ws_staleness_kill_switch_enabled=False,
        session_halt_gate_enabled=False,
        trading_hours_gate_enabled=True,
    )
    ex.maker_connector = "hyperliquid_perpetual"
    ex.maker_trading_pair = "X-USD"
    ex.hedge_connector = "kis"
    ex.hedge_trading_pair = "005930-KRW"
    ex._session_in_session = session_in_session
    ex._pending_hedge_signed = ZERO
    ex._rate_halt_latched = False    # _rate_halted is a property reading this
    ex._hedge_ws_stale = False
    ex._hedge_ws_age_s = 0.0
    ex._hedge_suppress_logged = False
    ex._hedge_order_side = {}
    ex._hedge_defer_logged_kind = None
    ex._hedge_defer_since_ts = None
    ex._hedge_defer_side = None
    ex._hedge_session_defer_cap_s = 0.0   # JEP-226 disabled
    ex._session_halt_state = None
    ex._strategy = SimpleNamespace(
        current_timestamp=1_000_000.0,
        cancel=MagicMock(),
    )
    # Stub reconcile methods to no-ops
    ex._ensure_direction_accounting = MagicMock()
    ex._reconcile_stuck_hedges = MagicMock()
    ex._reconcile_open_hedges = MagicMock()
    ex._hedge_in_flight = MagicMock(return_value=False)
    ex.hedge_orders = {}
    # Stub place_order to track calls
    ex.place_order = MagicMock(return_value="oid-001")
    # Stub _size_hedge
    ex._size_hedge = MagicMock(return_value={
        "order_type": None,
        "amount": Decimal("3"),
        "price": Decimal("100"),
        "metadata": {},
    })
    # status is a property reading _status; set _status directly
    from hummingbot.strategy_v2.models.base import RunnableStatus
    ex._status = RunnableStatus.RUNNING
    return ex


@unittest.skipUnless(_IMPORTABLE, "requires V2 stack (paho) — run in Docker/CI")
class HedgeOutOfSessionTest(unittest.TestCase):

    def test_out_of_session_holds_hedge_no_submit(self):
        """out-of-session + pending → place_order 미호출, pending 보존."""
        ex = _make_executor(session_in_session=False)
        ex._pending_hedge_signed = Decimal("-3")   # 미헤지 잔차
        ex._process_hedges()
        ex.place_order.assert_not_called()
        self.assertEqual(ex._pending_hedge_signed, Decimal("-3"))  # 잔차 보존

    def test_in_session_submits_hedge(self):
        """in-session + pending → place_order 호출(대조군)."""
        ex = _make_executor(session_in_session=True)
        ex._pending_hedge_signed = Decimal("-3")
        ex._process_hedges()
        ex.place_order.assert_called_once()

    def test_out_of_session_zero_pending_does_not_log_unnecessarily(self):
        """pending==ZERO이면 out-of-session 가드에 도달하기 전에 return."""
        ex = _make_executor(session_in_session=False)
        ex._pending_hedge_signed = ZERO
        ex._process_hedges()
        ex.place_order.assert_not_called()
        # _hedge_defer_logged_kind은 건드리지 않음
        self.assertIsNone(ex._hedge_defer_logged_kind)
