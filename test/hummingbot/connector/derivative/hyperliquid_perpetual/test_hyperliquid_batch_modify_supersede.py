import asyncio
from decimal import Decimal
from unittest import TestCase
from unittest.mock import AsyncMock, patch

from hummingbot.connector.derivative.hyperliquid_perpetual.hyperliquid_perpetual_derivative import (
    HyperliquidPerpetualDerivative,
)
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent

CLOID = "0x882c0177c0504eaa713a7329eb3d7c5c"
OLD_OID = 485395618469
OLD_OID_STR = str(OLD_OID)
NEW_OID = 485395700000

MODIFY_ACCEPTED_RESPONSE = {
    "status": "ok",
    "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": NEW_OID, "cloid": CLOID}}]}},
}

MODIFY_REJECTED_RESPONSE = {
    "status": "ok",
    "response": {"type": "order", "data": {"statuses": [
        {"error": "Post only order would have immediately matched"}]}},
}


def ws_canceled_msg(oid: int, cloid: str) -> dict:
    # inner element of orderUpdates 'data' — what _process_order_message receives
    return {
        "order": {"coin": "SKHX", "side": "A", "limitPx": "1528.3", "sz": "1.0",
                  "oid": oid, "timestamp": 1782965271000, "origSz": "1.0", "cloid": cloid},
        "status": "canceled",
        "statusTimestamp": 1782965273000,
    }


class HyperliquidBatchModifySupersedeTests(TestCase):
    trading_pair = "SKHX-USD"
    api_secret = "13e56ca9cceebf1f33065c2c5376ab38570a114bc1b003b60d838f92be9d7930"  # noqa: mock

    def setUp(self) -> None:
        super().setUp()
        self._build_exchange()

    def tearDown(self) -> None:
        self.local_event_loop.close()
        super().tearDown()

    def _build_exchange(self) -> None:
        self.local_event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.local_event_loop)
        self.exchange = HyperliquidPerpetualDerivative(
            hyperliquid_perpetual_secret_key=self.api_secret,
            hyperliquid_perpetual_address="0x1111111111111111111111111111111111111111",
            trading_pairs=[self.trading_pair],
            trading_required=False,
        )
        self.exchange._set_current_timestamp(1640780000)
        self.exchange._trading_rules = {
            self.trading_pair: TradingRule(
                trading_pair=self.trading_pair,
                min_order_size=Decimal("0.01"),
                min_price_increment=Decimal("0.0001"),
                min_base_amount_increment=Decimal("0.000001"),
            )
        }
        self.exchange.coin_to_asset = {"SKHX": 1}
        self.order_cancelled_logger = EventLogger()
        self.exchange.add_listener(MarketEvent.OrderCancelled, self.order_cancelled_logger)

    def _reset_exchange(self) -> None:
        self.local_event_loop.close()
        self._build_exchange()

    def _run(self, coro, timeout: float = 1.0):
        return self.local_event_loop.run_until_complete(asyncio.wait_for(coro, timeout))

    def _run_until(self, predicate, timeout_s: float = 1.0) -> None:
        async def _spin():
            deadline = self.local_event_loop.time() + timeout_s
            while not predicate():
                if self.local_event_loop.time() > deadline:
                    raise AssertionError("condition not met before timeout")
                await asyncio.sleep(0)
        self._run(_spin(), timeout=timeout_s + 1.0)

    def _track_open_order(self, cloid: str, oid: int) -> None:
        # start_tracking_order(order_id=cloid, exchange_order_id=None, LIMIT_MAKER, OPEN, ...)
        # then process an OrderUpdate(new_state=OrderState.OPEN, exchange_order_id=str(oid))
        # so the tracked order is OPEN with exchange_order_id == str(oid) — mirrors the live
        # batch-place flow (_execute_batch_place_orders resting branch).
        self.exchange.start_tracking_order(
            order_id=cloid,
            exchange_order_id=None,
            trading_pair=self.trading_pair,
            trade_type=TradeType.SELL,
            price=Decimal("1528.3"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT_MAKER,
            position_action=PositionAction.OPEN,
        )
        order_update = OrderUpdate(
            trading_pair=self.trading_pair,
            update_timestamp=self.exchange.current_timestamp,
            new_state=OrderState.OPEN,
            client_order_id=cloid,
            exchange_order_id=str(oid),
        )
        self.exchange._order_tracker.process_order_update(order_update=order_update)

    def _validated_modifies_for(self, cloid: str) -> list:
        # the dict shape _validate_batch_modify_requests produces
        return [{
            "client_order_id": cloid,
            "trading_pair": self.trading_pair,
            "coin": "SKHX",
            "price": Decimal("1529.0"),
            "amount": Decimal("1"),
            "trade_type": TradeType.SELL,
            "order_type": OrderType.LIMIT_MAKER,
            "position_action": PositionAction.OPEN,
        }]

    def test_ws_canceled_for_superseded_oid_is_ignored_after_accepted_modify(self):
        # tracked OPEN order: cloid=CLOID, exchange_order_id=str(OLD_OID)
        self._track_open_order(CLOID, OLD_OID)
        with patch.object(self.exchange, "_api_post", new=AsyncMock(return_value=MODIFY_ACCEPTED_RESPONSE)):
            self._run(self.exchange._execute_batch_modify_orders(self._validated_modifies_for(CLOID)))
        # accepted modify updated tracked oid -> NEW_OID (existing behavior, assert as precondition)
        tracked = self.exchange._order_tracker.fetch_tracked_order(CLOID)
        self.assertEqual(str(NEW_OID), tracked.exchange_order_id)

        self.exchange._process_order_message(ws_canceled_msg(OLD_OID, CLOID))

        tracked = self.exchange._order_tracker.fetch_tracked_order(CLOID)
        self.assertIsNotNone(tracked)                      # still tracked
        self.assertFalse(tracked.is_done)                  # NOT cancelled
        self.assertEqual(str(NEW_OID), tracked.exchange_order_id)  # oid NOT rewound
        self.assertEqual(0, len(self.order_cancelled_logger.event_log))  # no OrderCancelledEvent

    def test_ws_canceled_for_superseded_oid_ignored_even_before_modify_response(self):
        self._track_open_order(CLOID, OLD_OID)
        release = asyncio.Event()

        async def delayed_post(*args, **kwargs):
            await release.wait()
            return MODIFY_ACCEPTED_RESPONSE

        with patch.object(self.exchange, "_api_post", side_effect=delayed_post):
            task = self.local_event_loop.create_task(
                self.exchange._execute_batch_modify_orders(self._validated_modifies_for(CLOID)))
            self._run_until(lambda: OLD_OID_STR in self.exchange._hyperliquid_superseded_oids)  # recorded pre-POST
            self.exchange._process_order_message(ws_canceled_msg(OLD_OID, CLOID))               # WS first
            release.set()
            self._run(task)                                                                     # REST second

        tracked = self.exchange._order_tracker.fetch_tracked_order(CLOID)
        self.assertFalse(tracked.is_done)
        self.assertEqual(str(NEW_OID), tracked.exchange_order_id)
        self.assertEqual(0, len(self.order_cancelled_logger.event_log))

    def test_ws_canceled_for_current_oid_still_cancels(self):
        # no modify — a genuine cancel of the live oid flows through unchanged
        self._track_open_order(CLOID, OLD_OID)
        self.exchange._process_order_message(ws_canceled_msg(OLD_OID, CLOID))
        self._run(asyncio.sleep(0))  # flush ClientOrderTracker.process_order_update's scheduled task
        tracked = self.exchange._order_tracker.fetch_tracked_order(CLOID)
        self.assertTrue(tracked is None or tracked.is_done)
        self.assertEqual(1, len(self.order_cancelled_logger.event_log))

    def test_rejected_modify_slot_unrecords_superseded_oid(self):
        # modify slot returns {"error": ...} -> supersession did not happen -> a later WS
        # canceled(OLD_OID) is genuine and must cancel
        self._track_open_order(CLOID, OLD_OID)
        with patch.object(self.exchange, "_api_post", new=AsyncMock(return_value=MODIFY_REJECTED_RESPONSE)):
            self._run(self.exchange._execute_batch_modify_orders(self._validated_modifies_for(CLOID)))
        self.assertNotIn(OLD_OID_STR, self.exchange._hyperliquid_superseded_oids)
        self.exchange._process_order_message(ws_canceled_msg(OLD_OID, CLOID))
        self._run(asyncio.sleep(0))  # flush ClientOrderTracker.process_order_update's scheduled task
        self.assertEqual(1, len(self.order_cancelled_logger.event_log))

    def test_ws_canceled_variant_states_for_superseded_oid_also_ignored(self):
        # finding #3: every HL status mapping to OrderState.CANCELED must be guarded —
        # for a superseded OLD oid they all refer to the dead pre-modify order
        for status in ("canceled", "siblingFilledCanceled"):
            with self.subTest(status=status):
                self._reset_exchange()
                self._track_open_order(CLOID, OLD_OID)
                self.exchange._record_superseded_oid(OLD_OID_STR)
                msg = ws_canceled_msg(OLD_OID, CLOID)
                msg["status"] = status
                self.exchange._process_order_message(msg)
                tracked = self.exchange._order_tracker.fetch_tracked_order(CLOID)
                self.assertFalse(tracked.is_done)

    def test_superseded_ledger_ttl_purges_stale_entries(self):
        with patch("time.monotonic", side_effect=[0.0, 1000.0]):
            self.exchange._record_superseded_oid("111")
            self.exchange._record_superseded_oid("222")   # second insert purges the first (Δ > TTL)
        self.assertNotIn("111", self.exchange._hyperliquid_superseded_oids)
        self.assertIn("222", self.exchange._hyperliquid_superseded_oids)

    def test_superseded_cancel_is_consumed_from_ledger_on_first_message(self):
        self._track_open_order(CLOID, OLD_OID)
        self.exchange._record_superseded_oid(OLD_OID_STR)
        self.exchange._process_order_message(ws_canceled_msg(OLD_OID, CLOID))
        self.assertNotIn(OLD_OID_STR, self.exchange._hyperliquid_superseded_oids)  # consumed
        self.assertEqual(0, len(self.order_cancelled_logger.event_log))            # and ignored

    def test_request_order_status_repolls_by_cloid_for_superseded_oid(self):
        self._track_open_order(CLOID, OLD_OID)
        self.exchange._record_superseded_oid(str(OLD_OID))

        def order_status_response(oid_or_cloid):
            if oid_or_cloid == int(OLD_OID) or oid_or_cloid == str(OLD_OID):
                return {"order": {"status": "canceled",
                                  "order": {"oid": OLD_OID, "cloid": CLOID, "timestamp": 1782965273000}}}
            return {"order": {"status": "open",
                              "order": {"oid": NEW_OID, "cloid": CLOID, "timestamp": 1782965273500}}}

        async def api_post_side_effect(*args, **kwargs):
            return order_status_response(kwargs["data"]["oid"])

        self.exchange._api_post = AsyncMock(side_effect=api_post_side_effect)

        update = self._run(self.exchange._request_order_status(
            self.exchange._order_tracker.fetch_tracked_order(CLOID)))

        self.assertEqual(OrderState.OPEN, update.new_state)
        self.assertEqual(str(NEW_OID), update.exchange_order_id)
        # finding #1: the ledger entry must SURVIVE the re-poll (a late WS canceled(old)
        # can still arrive; only the WS guard or the TTL may remove it)
        self.assertIn(OLD_OID_STR, self.exchange._hyperliquid_superseded_oids)

    def test_request_order_status_applies_genuine_cancel_when_no_successor_exists(self):
        # stale ledger entry (e.g. modify transport failure, nothing superseded) must not
        # strand tracking: re-poll by cloid returns the SAME canceled order -> CANCELED applies
        self._track_open_order(CLOID, OLD_OID)
        self.exchange._record_superseded_oid(OLD_OID_STR)
        # _api_post: BOTH the oid-poll and the cloid-re-poll return canceled with oid=OLD_OID
        self.exchange._api_post = AsyncMock(return_value={
            "order": {"status": "canceled",
                      "order": {"oid": OLD_OID, "cloid": CLOID, "timestamp": 1782965273000}}})

        update = self._run(self.exchange._request_order_status(
            self.exchange._order_tracker.fetch_tracked_order(CLOID)))

        self.assertEqual(OrderState.CANCELED, update.new_state)

    def test_request_order_status_genuine_cancel_still_cancels(self):
        # no supersession recorded -> normal single-poll canceled path, no re-poll
        self._track_open_order(CLOID, OLD_OID)
        api_post_mock = AsyncMock(return_value={
            "order": {"status": "canceled",
                      "order": {"oid": OLD_OID, "cloid": CLOID, "timestamp": 1782965273000}}})
        self.exchange._api_post = api_post_mock

        update = self._run(self.exchange._request_order_status(
            self.exchange._order_tracker.fetch_tracked_order(CLOID)))

        self.assertEqual(OrderState.CANCELED, update.new_state)
        api_post_mock.assert_awaited_once()  # no re-poll needed
