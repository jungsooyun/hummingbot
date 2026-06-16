import unittest
from decimal import Decimal

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.data_feed.fair_fx.fair_fx_source import FairFxSource
from hummingbot.strategy_v2.executors.ladder_maker_executor.fx_policy import blend_fx_quote

D = Decimal


class FakeClient:
    def __init__(self, rate=D("1400"), raises=False):
        self.rate = rate
        self.raises = raises
        self.calls = 0

    async def fetch_exchange_rate(self):
        self.calls += 1
        if self.raises:
            raise RuntimeError("boom")
        return self.rate


def _fresh_source(usdt=(D("1410"), D("1420")), rate=D("1400"), raises=False):
    FairFxSource._instance = None
    src = FairFxSource()
    src.configure(FakeClient(rate=rate, raises=raises), lambda: usdt,
                  poll_interval_s=30.0, max_bank_age_s=120.0)
    return src


class FairFxSourceTest(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_returns_blended(self):
        usdt = (D("1410"), D("1420"))
        src = _fresh_source(usdt=usdt, rate=D("1400"))
        await src._poll_once()
        self.assertEqual(src.get_fx(), blend_fx_quote(D("1400"), *usdt))

    async def test_first_poll_logs_activation_once(self):
        # Observability lesson (JEP-148 live-verify): the first successful bank
        # fetch must emit an INFO line so operators can confirm FX activation;
        # subsequent polls must NOT re-log (no per-poll spam).
        src = _fresh_source(rate=D("1400"))
        with self.assertLogs(src.logger().name, level="INFO") as cm:
            await src._poll_once()   # None -> value : activation, logs
            await src._poll_once()   # value -> value : no re-log
        activations = [m for m in cm.output if "activated" in m.lower()]
        self.assertEqual(len(activations), 1)
        self.assertIn("1400", activations[0])

    async def test_stale_bank_returns_none(self):
        src = _fresh_source()
        clock = [1000.0]
        src._now = lambda: clock[0]
        await src._poll_once()
        clock[0] = 1000.0 + 121   # > max_bank_age_s (120)
        self.assertIsNone(src.get_fx())

    async def test_never_polled_returns_none(self):
        src = _fresh_source()
        self.assertIsNone(src.get_fx())

    async def test_usdt_missing_bank_only(self):
        src = _fresh_source(usdt=(None, None), rate=D("1400"))
        await src._poll_once()
        self.assertEqual(src.get_fx(), (D("1400"), D("1400")))

    async def test_idempotent_start_stop_network(self):
        src = _fresh_source()
        await src.start_network()
        task1 = src._poll_task
        await src.start_network()
        self.assertIs(src._poll_task, task1)   # no duplicate task
        await src.stop_network()
        self.assertIsNone(src._poll_task)
        await src.stop_network()               # no-op, no error
        await src.start_network()              # start after stop works
        self.assertIsNotNone(src._poll_task)
        await src.stop_network()

    async def test_check_network_connected(self):
        src = _fresh_source(raises=False)
        self.assertEqual(await src.check_network(), NetworkStatus.CONNECTED)

    async def test_check_network_not_connected(self):
        src = _fresh_source(raises=True)
        self.assertEqual(await src.check_network(), NetworkStatus.NOT_CONNECTED)

    async def test_get_instance_singleton(self):
        FairFxSource._instance = None
        self.assertIs(FairFxSource.get_instance(), FairFxSource.get_instance())


if __name__ == "__main__":
    unittest.main()
