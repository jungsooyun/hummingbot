"""JEP-231 fix (MED): in SHUTTING_DOWN the executor must refresh session state BEFORE the
WS-staleness eval. Otherwise a graceful stop landing just after the 20:00 KST close (with an
accumulated stale timer from the prior in-session tick) can latch the JEP-134 kill-switch,
which defeats next-session auto-resume."""
import asyncio
import unittest

from hummingbot.strategy_v2.models.base import RunnableStatus

try:
    from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_maker_executor import LadderMakerExecutor
    _IMPORTABLE = True
except Exception:  # pragma: no cover
    LadderMakerExecutor = None
    _IMPORTABLE = False


@unittest.skipUnless(_IMPORTABLE, "requires V2 stack (paho) — run in Docker/CI")
class ShutdownOrderingTest(unittest.TestCase):
    def test_shutdown_evaluates_session_before_staleness(self):
        ex = LadderMakerExecutor.__new__(LadderMakerExecutor)
        ex._status = RunnableStatus.SHUTTING_DOWN  # status is a read-only property
        calls = []
        ex._evaluate_session_state = lambda: calls.append("session")
        ex._evaluate_ws_staleness = lambda: calls.append("staleness")

        async def _shutdown():
            calls.append("shutdown")

        ex._control_shutdown = _shutdown
        asyncio.run(ex.control_task())
        self.assertEqual(calls, ["session", "staleness", "shutdown"])


if __name__ == "__main__":
    unittest.main()
