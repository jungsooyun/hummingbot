"""KIS single-connection WebSocket transport multiplexer.

KIS permits exactly ONE realtime WS connection per approval_key (appkey),
multiplexing up to ~40 TR subscriptions demuxed by ``tr_id``. This hub owns that
single connection so multiple data sources (orderbook + trade + execution-notice)
share it instead of each opening its own socket (which collides with
``ALREADY IN USE appkey``).

It is PURE TRANSPORT: connect, subscribe the registry, read frames, route each raw
frame to the handler registered for its ``tr_id``. No parsing, no AES, no freshness
logic - those stay in the data-source handlers.
"""

import asyncio
import inspect
import json
import logging
from typing import Awaitable, Callable, Dict, Optional, Set, Tuple, TYPE_CHECKING

import aiohttp

from hummingbot.connector.exchange.kis import kis_web_utils as web_utils
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.kis.kis_auth import KisAuth

Handler = Callable[[str], Awaitable[None]]


def _same_handler(a: Handler, b: Handler) -> bool:
    """Whether two handlers are the SAME logical handler for the overwrite guard.

    A bound method (e.g. a data source's ``self._on_ws_frame``) is a FRESH object on every
    attribute access — ``a.m is a.m`` is False in CPython — yet it represents one logical
    handler. Compare real bound methods by their (``__self__``, ``__func__``) identity so a
    source re-registering a tr_id (one DS sharing a tr_id across symbols, or a stop/start
    restart) is recognised as the same handler. For anything that is NOT a bound method, fall
    back to STRICT identity: we deliberately do NOT delegate to arbitrary callable ``__eq__``,
    so a callable with broad/custom equality cannot be mistaken for an already-registered
    handler and silently steal its tr_id (JEP-207 review)."""
    if inspect.ismethod(a) and inspect.ismethod(b):
        return a.__self__ is b.__self__ and a.__func__ is b.__func__
    return a is b


class KisWsHub:

    _logger: Optional[HummingbotLogger] = None

    # No-first-frame watchdog: if a (re)connect is accepted but KIS sends NO frame
    # (no subscribe ack, no data, no error) within this window, recycle the socket
    # via the backoff/reconnect path instead of blocking on receive() forever.
    # Only the PRE-first-frame window is guarded; a healthy-but-quiet stream after
    # the subscribe ack (e.g. exec-notice off-hours) is never killed.
    FIRST_FRAME_TIMEOUT = 10.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        auth: "KisAuth",
        domain: str,
        *,
        ws_enabled: bool = True,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self._auth = auth
        self._domain = domain
        self._ws_enabled = ws_enabled
        self._sleep = sleep

        self._subs: Set[Tuple[str, str]] = set()     # (tr_id, tr_key) to (re)subscribe
        self._dispatch: Dict[str, Handler] = {}       # tr_id -> handler (handler demuxes by tr_key)
        self._lock = asyncio.Lock()
        self._run_task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_failures = 0
        self._stopped = False

    # ------------------------------------------------------------------ #
    # Public surface
    # ------------------------------------------------------------------ #

    async def register(self, tr_id: str, tr_key: str, handler: Handler) -> None:
        if not self._ws_enabled:
            return
        async with self._lock:
            # A register() means "(re)start". ExchangePyBase.start_network() calls
            # stop_network() FIRST (exchange_py_base.py:676), which runs hub.stop() and
            # sets _stopped=True; without this reset the hub would be permanently dead and
            # never reconnect on the (re)start that immediately follows. Clear it BEFORE the
            # overwrite guard so the guard stays armed across restarts. The guard uses
            # _same_handler (bound-method __self__/__func__ identity, strict identity otherwise):
            # a bound method (e.g. a data source's self._on_ws_frame) is a FRESH object on every
            # attribute access (`a.m is a.m` is False in CPython), so the same source
            # re-registering a tr_id — one DS subscribing one tr_id for several symbols
            # (H0UNASP0 demuxes by tr_key), or a stop/start restart — MUST be a no-op. A
            # genuinely different source (different __self__) still raises (JEP-207: identity
            # here wrongly rejected the 2nd symbol and kill-switched the 2-symbol live run).
            self._stopped = False
            existing = self._dispatch.get(tr_id)
            if existing is not None and not _same_handler(existing, handler):
                raise ValueError(
                    f"KisWsHub: tr_id {tr_id} is already registered to a different handler; "
                    f"refusing to overwrite (would silently drop frames)."
                )
            self._dispatch[tr_id] = handler
            # Track whether this (tr_id, tr_key) is genuinely new so a same-key re-register is a
            # no-op ON THE WIRE too, not just in _dispatch. Re-sending tr_type="1" for an already
            # subscribed key risks a KIS duplicate-subscribe rejection, which _is_failed_subscribe_ack
            # would treat as a failed ack and recycle the whole shared socket (JEP-207 review).
            newly_added = (tr_id, tr_key) not in self._subs
            self._subs.add((tr_id, tr_key))
            live = self._ws is not None and not self._ws.closed
            self._ensure_running()
        if live and newly_added:
            await self._safe_send_sub(tr_id, tr_key, "1")

    async def unregister(self, tr_id: str, tr_key: str) -> None:
        async with self._lock:
            self._subs.discard((tr_id, tr_key))
            if not any(t == tr_id for (t, _k) in self._subs):
                self._dispatch.pop(tr_id, None)
            live = self._ws is not None and not self._ws.closed
        if live:
            await self._safe_send_sub(tr_id, tr_key, "2")
        # NEVER closes the shared socket (would orphan the other feeds).

    async def stop(self) -> None:
        # Serialize the _stopped / _run_task mutation against register()/unregister()
        # (which also hold _lock) so a concurrent register cannot start a run-task while
        # stop is tearing one down. The cancel-await + socket close happen OUTSIDE the
        # lock so a DS finally-unregister (which needs _lock) cannot deadlock against it.
        async with self._lock:
            self._stopped = True
            task, self._run_task = self._run_task, None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                self.logger().exception("KisWsHub: run-task raised during stop (ignored).")
        await self._close_ws()

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _ensure_running(self) -> None:
        if self._stopped:
            return
        if self._run_task is None or self._run_task.done():
            self._run_task = safe_ensure_future(self._run())

    async def _run(self) -> None:
        # Fresh run-task = fresh backoff state. The hub is a per-connector singleton reused
        # across stop_network()/start_network() cycles; without this a flappy prior session
        # would carry a high _ws_failures into the restart and start at the 60s backoff cap.
        self._ws_failures = 0
        while not self._stopped:
            try:
                ws_url = web_utils.ws_url(self._domain)
                self._session = aiohttp.ClientSession()
                self._ws = await self._session.ws_connect(ws_url)
                await self._subscribe_all()
                got_first_frame = False
                while not self._stopped:
                    try:
                        msg = await asyncio.wait_for(
                            self._ws.receive(),
                            timeout=None if got_first_frame else self.FIRST_FRAME_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        # no-frame watchdog: connection accepted but silent -> recycle
                        self.logger().warning(
                            "KisWsHub: no frame within first-frame timeout; recycling socket."
                        )
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    got_first_frame = True              # any frame disarms the no-frame watchdog
                    raw = msg.data
                    tr_id = self._extract_tr_id(raw)
                    if tr_id == "PINGPONG":
                        await self._echo_pingpong(raw)
                        continue
                    if self._is_failed_subscribe_ack(raw):
                        # e.g. "ALREADY IN USE" / rejected subscription. Recycle WITHOUT
                        # resetting _ws_failures so backoff escalates; covers the case where
                        # KIS rejects the subscription but does not immediately close the socket.
                        self.logger().warning(
                            f"KisWsHub: subscription rejected; recycling. ({raw[:120]})"
                        )
                        break
                    self._ws_failures = 0              # data frame or successful ack: confirmed healthy
                    await self._dispatch_to_handler(tr_id, raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Full traceback on the first failure of a streak; concise repr thereafter so a
                # persistently-unavailable WS still surfaces its root cause (401/DNS/TLS) without
                # flooding the log every few seconds.
                if self._ws_failures == 0:
                    self.logger().exception("KisWsHub: connection failed; retrying with backoff.")
                else:
                    self.logger().warning(f"KisWsHub: connection error ({exc!r}); backing off.")
            finally:
                await self._close_ws()
            if self._stopped:
                break
            # SINGLE reconnect path - clean close, error, normal completion, and the
            # watchdog ALL apply backoff. Fixes the no-backoff ALREADY-IN-USE storm.
            self._ws_failures += 1
            delay = web_utils.reconnect_backoff(self._ws_failures)
            if self._ws_failures > 1:
                self.logger().warning(
                    f"KisWsHub: unavailable (attempt {self._ws_failures}); retrying in {delay:.0f}s."
                )
            await self._sleep(delay)
            await asyncio.sleep(0)

    async def _subscribe_all(self) -> None:
        approval_key = await self._auth.get_ws_approval_key()
        for (tr_id, tr_key) in list(self._subs):
            await self._ws.send_json(self._build_sub(approval_key, tr_id, tr_key, "1"))

    async def _safe_send_sub(self, tr_id: str, tr_key: str, tr_type: str) -> None:
        try:
            approval_key = await self._auth.get_ws_approval_key()
            await self._ws.send_json(self._build_sub(approval_key, tr_id, tr_key, tr_type))
        except Exception:
            self.logger().warning(
                f"KisWsHub: live (un)subscribe failed for {tr_id}/{tr_key} "
                f"(tr_type={tr_type}); will resync on next reconnect."
            )

    async def _echo_pingpong(self, raw: str) -> None:
        try:
            await self._ws.send_str(raw)
        except Exception:
            self.logger().debug("KisWsHub: PINGPONG echo failed (ignored).")

    @staticmethod
    def _is_failed_subscribe_ack(raw: str) -> bool:
        """True for a JSON control frame whose body.rt_cd signals a rejected subscription
        (e.g. ALREADY IN USE). Data frames and successful acks (rt_cd == '0') return False.
        Subscription-lifecycle health is a transport concern, so the hub may inspect it."""
        if not raw or raw[0] in ("0", "1"):
            return False
        try:
            body = json.loads(raw).get("body", {})
        except (json.JSONDecodeError, AttributeError):
            return False
        rt_cd = body.get("rt_cd")
        return rt_cd is not None and rt_cd != "0"

    async def _dispatch_to_handler(self, tr_id: Optional[str], raw: str) -> None:
        handler = self._dispatch.get(tr_id) if tr_id else None
        if handler is None:
            self.logger().debug(f"KisWsHub: no handler for tr_id={tr_id!r}; frame dropped.")
            return
        try:
            await handler(raw)
        except Exception:
            self.logger().exception(
                f"KisWsHub: handler for tr_id={tr_id} raised; frame dropped (socket kept)."
            )

    async def _close_ws(self) -> None:
        ws, self._ws = self._ws, None
        session, self._session = self._session, None
        if ws is not None and not ws.closed:
            try:
                await ws.close()
            except Exception:
                pass
        if session is not None and not session.closed:
            try:
                await session.close()
            except Exception:
                pass

    @staticmethod
    def _extract_tr_id(raw: str) -> Optional[str]:
        if not raw:
            return None
        if raw[0] in ("0", "1"):
            parts = raw.split("|")
            return parts[1] if len(parts) >= 2 else None
        try:
            header = json.loads(raw).get("header", {})
        except (json.JSONDecodeError, AttributeError):
            return None
        return header.get("tr_id")

    @staticmethod
    def _build_sub(approval_key: str, tr_id: str, tr_key: str, tr_type: str) -> dict:
        return {
            "header": {
                "approval_key": approval_key,
                "custtype": "P",
                "tr_type": tr_type,
                "content-type": "utf-8",
            },
            "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}},
        }
