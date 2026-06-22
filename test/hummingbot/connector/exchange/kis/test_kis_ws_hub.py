import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from hummingbot.connector.exchange.kis.kis_ws_hub import KisWsHub


def _text(data: str):
    m = MagicMock()
    m.type = aiohttp.WSMsgType.TEXT
    m.data = data
    return m


def _closed():
    m = MagicMock()
    m.type = aiohttp.WSMsgType.CLOSED
    m.data = None
    return m


class _FakeWS:
    """Fake aiohttp ClientWebSocketResponse driven by a scripted message list.

    After the scripted messages are exhausted it BLOCKS (the socket stays open) until the
    hub run-task is cancelled by stop(). To force a server close / reconnect, include an
    explicit _closed() in the message list. This avoids the auto-close->reconnect->IndexError
    trap for single-connection tests whose _FakeSession only scripts one WS.
    """
    def __init__(self, messages):
        self._messages = list(messages)
        self._idle = asyncio.Event()
        self.send_json = AsyncMock()
        self.send_str = AsyncMock()
        self.close = AsyncMock(side_effect=self._mark_closed)
        self.closed = False

    async def _mark_closed(self):
        self.closed = True

    async def receive(self):
        if self._messages:
            return self._messages.pop(0)
        await self._idle.wait()   # stay open until the run-task is cancelled


class _FakeSession:
    """Fake aiohttp.ClientSession returning scripted fake WS objects in order."""
    def __init__(self, ws_list):
        self._ws_list = list(ws_list)
        self.ws_connect = AsyncMock(side_effect=self._next_ws)
        self.close = AsyncMock()
        self.closed = False

    async def _next_ws(self, *a, **k):
        return self._ws_list.pop(0)


def _make_auth():
    auth = MagicMock()
    auth.get_ws_approval_key = AsyncMock(return_value="approval123")
    return auth


class KisWsHubTests(IsolatedAsyncioWrapperTestCase):

    async def _settle(self, ticks=20):
        """Yield control repeatedly so the hub run-task advances (connect->subscribe->dispatch)."""
        for _ in range(ticks):
            await asyncio.sleep(0)

    async def _drain(self, hub, ticks=20):
        """Let the hub run-task make progress, then stop it."""
        await self._settle(ticks)
        await hub.stop()

    async def test_register_updates_registry_and_guards_overwrite(self):
        hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
        h1 = AsyncMock()
        await hub.register("H0UNASP0", "005930", h1)
        await hub.register("H0UNASP0", "005930", h1)  # idempotent no-op
        self.assertIn(("H0UNASP0", "005930"), hub._subs)
        self.assertIs(hub._dispatch["H0UNASP0"], h1)
        with self.assertRaises(ValueError):
            await hub.register("H0UNASP0", "005930", AsyncMock())  # different handler
        await hub.stop()

    async def test_unregister_drops_handler_only_when_last_key_gone(self):
        hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
        h = AsyncMock()
        await hub.register("H0UNASP0", "005930", h)
        await hub.register("H0UNASP0", "000660", h)
        await hub.unregister("H0UNASP0", "005930")
        self.assertIn("H0UNASP0", hub._dispatch)            # one key remains
        await hub.unregister("H0UNASP0", "000660")
        self.assertNotIn("H0UNASP0", hub._dispatch)         # last key gone
        await hub.stop()

    async def test_single_connection_subscribes_all_and_dispatches(self):
        ob_handler, exec_handler = AsyncMock(), AsyncMock()
        ob_frame = "0|H0UNASP0|005930|field^field"
        exec_frame = "0|H0STCNI0|005930|cipher"
        fake_ws = _FakeWS([_text(ob_frame), _text(exec_frame)])
        fake_session = _FakeSession([fake_ws])
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=fake_session):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
            await hub.register("H0UNASP0", "005930", ob_handler)
            await hub.register("H0STCNI0", "005930", exec_handler)
            await self._drain(hub)
        # exactly one socket opened
        self.assertEqual(1, fake_session.ws_connect.call_count)
        # both TRs subscribed on it
        sent = [c.args[0]["body"]["input"]["tr_id"] for c in fake_ws.send_json.call_args_list]
        self.assertIn("H0UNASP0", sent)
        self.assertIn("H0STCNI0", sent)
        # frames dispatched to the right handlers by tr_id
        ob_handler.assert_awaited_with(ob_frame)
        exec_handler.assert_awaited_with(exec_frame)

    async def test_pingpong_echoed_centrally_not_dispatched(self):
        handler = AsyncMock()
        ping = json.dumps({"header": {"tr_id": "PINGPONG"}})
        fake_ws = _FakeWS([_text(ping)])
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=_FakeSession([fake_ws])):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
            await hub.register("H0UNASP0", "005930", handler)
            await self._drain(hub)
        fake_ws.send_str.assert_awaited_with(ping)   # echoed
        handler.assert_not_awaited()                  # never dispatched

    async def test_unknown_tr_id_dropped_and_handler_isolation(self):
        good = AsyncMock()
        bad = AsyncMock(side_effect=RuntimeError("boom"))
        frames = [_text("0|H0UNCNT0|005930|x"),         # no handler -> dropped
                  _text("0|H0STCNI0|005930|y"),         # bad handler raises -> isolated
                  _text("0|H0UNASP0|005930|z")]         # good handler still runs
        fake_ws = _FakeWS(frames)
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=_FakeSession([fake_ws])):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
            await hub.register("H0STCNI0", "005930", bad)
            await hub.register("H0UNASP0", "005930", good)
            await self._drain(hub)
        good.assert_awaited_with("0|H0UNASP0|005930|z")  # survived the bad frame

    async def test_reconnect_resubscribes_full_registry(self):
        handler = AsyncMock()
        ws1 = _FakeWS([_text("0|H0UNASP0|005930|a"), _closed()])   # closes after 1 frame
        ws2 = _FakeWS([_text("0|H0UNASP0|005930|b")])
        fake_session = _FakeSession([ws1, ws2])
        sleep = AsyncMock()
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=fake_session):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=sleep)
            await hub.register("H0UNASP0", "005930", handler)
            await hub.register("H0STCNI0", "005930", handler)
            await self._drain(hub, ticks=20)
        # second connection re-subscribed BOTH TRs
        sent2 = [c.args[0]["body"]["input"]["tr_id"] for c in ws2.send_json.call_args_list]
        self.assertIn("H0UNASP0", sent2)
        self.assertIn("H0STCNI0", sent2)
        self.assertGreaterEqual(fake_session.ws_connect.call_count, 2)

    async def test_clean_close_takes_backoff(self):
        # A connection that closes WITHOUT raising still backs off (the storm bug:
        # the old clean-close path skipped the except block -> no backoff -> ~15/s).
        ws1 = _FakeWS([_text("0|H0UNASP0|005930|a"), _closed()])   # frame then clean close
        ws2 = _FakeWS([])   # reconnect target; blocks (stays open) until _drain stops the hub
        sleep = AsyncMock()
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=_FakeSession([ws1, ws2])):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=sleep)
            await hub.register("H0UNASP0", "005930", AsyncMock())
            await self._drain(hub, ticks=20)
        sleep.assert_awaited()   # backoff applied on the clean close

    async def test_failures_reset_only_after_a_frame(self):
        # connects that CLOSE before any frame keep escalating backoff (no reset).
        ws1 = _FakeWS([_closed()])     # closes immediately, no frame
        ws2 = _FakeWS([_closed()])
        ws3 = _FakeWS([])              # blocks, so the loop parks after 2 failures
        sleep = AsyncMock()
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=_FakeSession([ws1, ws2, ws3])):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=sleep)
            await hub.register("H0UNASP0", "005930", AsyncMock())
            await self._drain(hub, ticks=30)
        delays = [c.args[0] for c in sleep.await_args_list]
        self.assertGreaterEqual(len(delays), 2)
        self.assertGreater(delays[1], delays[0])   # backoff escalated (failures NOT reset)

    async def test_no_frame_watchdog_recycles_socket(self):
        # receive() blocks forever (no frame, no close) -> watchdog must time out.
        class _SilentWS(_FakeWS):
            async def receive(self):
                await asyncio.sleep(3600)   # never returns within the test
        silent = _SilentWS([])
        next_ws = _FakeWS([_text("0|H0UNASP0|005930|a")])
        sleep = AsyncMock()
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=_FakeSession([silent, next_ws])), \
             patch("hummingbot.connector.exchange.kis.kis_ws_hub.asyncio.wait_for",
                   new=AsyncMock(side_effect=[asyncio.TimeoutError(), *[
                       _text("0|H0UNASP0|005930|a"), _closed()]])):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=sleep)
            await hub.register("H0UNASP0", "005930", AsyncMock())
            await self._drain(hub, ticks=20)
        sleep.assert_awaited()              # took the backoff path after the timeout
        silent.close.assert_awaited()       # recycled the silent socket

    async def test_already_in_use_recycles_with_backoff(self):
        # ALREADY IN USE arrives as a control frame with rt_cd != "0". The hub must recycle
        # (close + backoff) and must NOT reset _ws_failures or idle forever - even if KIS
        # keeps the socket open after the rejection. (spec section 4.5)
        rejected = json.dumps({"header": {"tr_id": "H0UNASP0"},
                               "body": {"rt_cd": "1", "msg1": "ALREADY IN USE appkey"}})
        ws1 = _FakeWS([_text(rejected)])     # rejection frame, then drain emulates close
        ws2 = _FakeWS([_text("0|H0UNASP0|005930|a")])
        sleep = AsyncMock()
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=_FakeSession([ws1, ws2])):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=sleep)
            await hub.register("H0UNASP0", "005930", AsyncMock())
            await self._drain(hub, ticks=20)
        ws1.close.assert_awaited()           # rejected socket recycled
        sleep.assert_awaited()               # backoff applied (not a tight retry, not idle)

    async def test_one_runtask_regardless_of_registration_order(self):
        for order in (("H0UNASP0", "H0STCNI0"), ("H0STCNI0", "H0UNASP0")):
            fake_ws = _FakeWS([_text("0|H0UNASP0|005930|a")])
            with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                       return_value=_FakeSession([fake_ws])):
                hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
                await hub.register(order[0], "005930", AsyncMock())
                first_task = hub._run_task
                await hub.register(order[1], "005930", AsyncMock())
                self.assertIs(hub._run_task, first_task)   # second register did NOT start a 2nd task
                await self._drain(hub)

    async def test_stop_closes_live_socket_exactly_once_and_idempotent(self):
        fake_ws = _FakeWS([_text("0|H0UNASP0|005930|a")])   # dispatches then blocks (stays open)
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=_FakeSession([fake_ws])):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
            await hub.register("H0UNASP0", "005930", AsyncMock())
            await self._settle()
            await hub.stop()
            await hub.stop()   # idempotent second stop
        self.assertIsNone(hub._run_task)
        self.assertEqual(1, fake_ws.close.await_count)   # the live socket is closed EXACTLY once

    async def test_unregister_does_not_close_socket(self):
        fake_ws = _FakeWS([_text("0|H0UNASP0|005930|a")])
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=_FakeSession([fake_ws])):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
            await hub.register("H0UNASP0", "005930", AsyncMock())
            await self._settle()
            live_ws = hub._ws
            await hub.unregister("H0UNASP0", "005930")
            self.assertEqual(0, live_ws.close.await_count)   # unregister NEVER closes the socket
            await hub.stop()

    async def test_unregister_after_stop_is_safe(self):
        # OrderBookTracker.stop() cancels-not-awaits listen_for_subscriptions, so its
        # finally-unregister can run AFTER KisExchange.stop_network() already called
        # hub.stop(). That late unregister must be a harmless no-op (no error, no close).
        fake_ws = _FakeWS([_text("0|H0UNASP0|005930|a")])
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=_FakeSession([fake_ws])):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
            await hub.register("H0UNASP0", "005930", AsyncMock())
            await self._settle()
            await hub.stop()
            await hub.unregister("H0UNASP0", "005930")   # after stop: no exception, no socket op
        self.assertEqual(1, fake_ws.close.await_count)   # unregister added no extra close

    async def test_hub_restarts_after_stop(self):
        # Mirrors start_network()->stop_network()(hub.stop)->register(): a register AFTER
        # stop must clear _stopped and start a FRESH run-task (else the hub is permanently
        # dead and never reconnects on the restart that immediately follows). (BLOCKING-1)
        ws1 = _FakeWS([_text("0|H0UNASP0|005930|a")])
        ws2 = _FakeWS([_text("0|H0UNASP0|005930|b")])
        fake_session = _FakeSession([ws1, ws2])
        # Same handler object across the restart, mirroring production: the DS instance
        # (and its bound _on_ws_frame) persists across start_network/stop_network cycles.
        handler = AsyncMock()
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=fake_session):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
            await hub.register("H0UNASP0", "005930", handler)
            await self._settle()
            await hub.stop()
            self.assertTrue(hub._stopped)
            await hub.register("H0UNASP0", "005930", handler)   # restart (same handler)
            self.assertFalse(hub._stopped)
            self.assertIsNotNone(hub._run_task)
            await self._drain(hub)
        self.assertEqual(2, fake_session.ws_connect.call_count)     # connected before AND after restart

    async def test_multi_symbol_registry_resubscribes_every_pair(self):
        handler = AsyncMock()
        ws1 = _FakeWS([_text("0|H0UNASP0|005930|a"), _closed()])
        ws2 = _FakeWS([_text("0|H0UNASP0|000660|b")])
        fake_session = _FakeSession([ws1, ws2])
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=fake_session):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
            await hub.register("H0UNASP0", "005930", handler)
            await hub.register("H0UNASP0", "000660", handler)   # same tr_id, 2nd symbol
            self.assertEqual(1, len(hub._dispatch))              # one handler for the tr_id
            self.assertEqual(2, len(hub._subs))                  # two (tr_id, tr_key) subs
            await self._drain(hub, ticks=20)
        keys2 = {c.args[0]["body"]["input"]["tr_key"] for c in ws2.send_json.call_args_list}
        self.assertEqual({"005930", "000660"}, keys2)            # reconnect re-subbed both

    async def test_end_to_end_single_connection_three_trs(self):
        # both data sources registering through one hub => one socket, three TRs
        ob_h, ex_h = AsyncMock(), AsyncMock()
        fake_ws = _FakeWS([_text("0|H0UNASP0|005930|a")])
        fake_session = _FakeSession([fake_ws])
        with patch("hummingbot.connector.exchange.kis.kis_ws_hub.aiohttp.ClientSession",
                   return_value=fake_session):
            hub = KisWsHub(auth=_make_auth(), domain="real", ws_enabled=True, sleep=AsyncMock())
            await hub.register("H0UNASP0", "005930", ob_h)
            await hub.register("H0UNCNT0", "005930", ob_h)
            await hub.register("H0STCNI0", "005930", ex_h)
            await self._drain(hub)
        self.assertEqual(1, fake_session.ws_connect.call_count)
        subbed = {c.args[0]["body"]["input"]["tr_id"] for c in fake_ws.send_json.call_args_list}
        self.assertEqual({"H0UNASP0", "H0UNCNT0", "H0STCNI0"}, subbed)
