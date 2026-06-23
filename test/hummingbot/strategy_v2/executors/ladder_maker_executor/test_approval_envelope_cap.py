"""JEP-133: approval-envelope per-order maker quantity cap.

The stratops live runtime gates every order against an approval envelope; the
single most safety-critical field is ``max_qty_per_order`` (the per-order base
quantity cap). This ports that as a runtime guard at the ladder maker placement
boundary: a maker order whose quantized size exceeds the configured cap is
REFUSED (logged + skipped), never silently submitted oversized and never
clamped down. ``max_maker_order_size=None`` (default) = no cap = current
behavior (behavior-neutral).
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from hummingbot.core.data_type.common import OrderType, TradeType

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    _EXECUTOR_IMPORTABLE = True
except Exception:  # pragma: no cover - env-dependent
    LadderMakerExecutor = None
    _EXECUTOR_IMPORTABLE = False


pytestmark = pytest.mark.skipif(
    not _EXECUTOR_IMPORTABLE,
    reason="ladder_maker_executor requires the V2 stack (paho) - run in Docker/CI",
)


def _make_executor(observe=False, max_maker_order_size=None, quantize_floor=None):
    ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
    cfg = dict(
        observe=observe,
        maker_post_only=True,
    )
    if max_maker_order_size is not None:
        cfg["max_maker_order_size"] = max_maker_order_size
    ex.config = SimpleNamespace(**cfg)
    ex.maker_connector = "hyperliquid_perpetual"
    ex.maker_trading_pair = "XYZ:SKHX-USD"
    ex.hedge_connector = "kis"
    ex.hedge_trading_pair = "000660-KRW"
    ex.entry_side = TradeType.SELL
    conn = MagicMock()
    # quantize_order_amount can shrink the requested size to the venue lot; the cap
    # must be enforced against the resulting quantized amount, not the raw request.
    if quantize_floor is not None:
        conn.quantize_order_amount.side_effect = lambda pair, amt: Decimal(str(quantize_floor))
    else:
        conn.quantize_order_amount.side_effect = lambda pair, amt: amt
    conn.quantize_order_price.side_effect = lambda pair, price: price
    ex.connectors = {ex.maker_connector: conn, ex.hedge_connector: conn}
    ex.place_order = MagicMock(side_effect=lambda **kw: "OID-0")
    ex.maker_orders = {}
    ex._maker_placed_edge_bps = {}
    ex.logger = MagicMock(return_value=MagicMock())
    return ex


def test_no_cap_default_places_any_size():
    # Behavior-neutral: no max_maker_order_size attribute at all -> no cap.
    ex = _make_executor()
    oid = ex._place_maker(Decimal("50.40"), Decimal("100"), Decimal("100"))
    assert oid == "OID-0"
    ex.place_order.assert_called_once()
    assert ex.place_order.call_args.kwargs["amount"] == Decimal("100")


def test_cap_none_places_any_size():
    ex = _make_executor(max_maker_order_size=None)
    oid = ex._place_maker(Decimal("50.40"), Decimal("100"), Decimal("100"))
    assert oid == "OID-0"
    ex.place_order.assert_called_once()


def test_under_cap_places():
    ex = _make_executor(max_maker_order_size=Decimal("2"))
    oid = ex._place_maker(Decimal("50.40"), Decimal("2"), Decimal("100"))
    assert oid == "OID-0"
    ex.place_order.assert_called_once()
    assert ex.place_order.call_args.kwargs["amount"] == Decimal("2")


def test_over_cap_refused_not_clamped():
    # Oversized order is REFUSED (no place_order), not silently clamped to the cap.
    ex = _make_executor(max_maker_order_size=Decimal("2"))
    oid = ex._place_maker(Decimal("50.40"), Decimal("3"), Decimal("100"))
    assert oid is None
    ex.place_order.assert_not_called()
    # Nothing tracked: an oversized rung must not pollute _maker_orders.
    assert ex.maker_orders == {}


def test_cap_enforced_against_quantized_amount():
    # The raw request (1.9) is under the cap (2) but the venue lot rounds UP to 3
    # via quantize -> the SUBMITTED size exceeds the cap -> refuse.
    ex = _make_executor(max_maker_order_size=Decimal("2"), quantize_floor="3")
    oid = ex._place_maker(Decimal("50.40"), Decimal("1.9"), Decimal("100"))
    assert oid is None
    ex.place_order.assert_not_called()


def test_at_cap_boundary_places():
    # Exactly at the cap is allowed (<=).
    ex = _make_executor(max_maker_order_size=Decimal("2"))
    oid = ex._place_maker(Decimal("50.40"), Decimal("2"), Decimal("100"))
    assert oid == "OID-0"
    ex.place_order.assert_called_once()


def test_observe_takes_precedence_over_cap():
    # Observe is no-submit regardless of cap; an over-cap rung in observe still no-ops.
    ex = _make_executor(observe=True, max_maker_order_size=Decimal("2"))
    oid = ex._place_maker(Decimal("50.40"), Decimal("100"), Decimal("100"))
    assert oid is None
    ex.place_order.assert_not_called()
