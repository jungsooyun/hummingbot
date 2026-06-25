"""Tests for the JEP-218 clock-stall watchdog (off-by-default diagnostic)."""
import asyncio
import tempfile
import time
import unittest

from hummingbot.core.utils.clock_watchdog import ClockWatchdog


class _FakeFaulthandler:
    """Records dump_traceback_later / cancel calls for the deadman tests."""

    def __init__(self, fail: bool = False):
        self.fail = fail
        self.armed = 0
        self.cancelled = 0
        self.last_timeout = None

    def dump_traceback_later(self, timeout, repeat=False, file=None):
        if self.fail:
            raise RuntimeError("faulthandler unavailable")
        self.armed += 1
        self.last_timeout = timeout

    def cancel_dump_traceback_later(self):
        self.cancelled += 1

    def dump_traceback(self, file=None, all_threads=True):
        pass


class ClockWatchdogTest(unittest.TestCase):
    def test_from_env_disabled_by_default(self):
        # No env flag -> watchdog is not constructed (behavior-neutral default).
        self.assertIsNone(ClockWatchdog.from_env(env={}))

    def test_from_env_enabled_with_flag(self):
        wd = ClockWatchdog.from_env(env={"HUMMINGBOT_CLOCK_WATCHDOG": "1"})
        self.assertIsNotNone(wd)
        self.assertFalse(wd.is_running())

    def test_from_env_reads_overrides(self):
        wd = ClockWatchdog.from_env(env={
            "HUMMINGBOT_CLOCK_WATCHDOG": "true",
            "HUMMINGBOT_CLOCK_WATCHDOG_STALL_S": "3.5",
            "HUMMINGBOT_CLOCK_WATCHDOG_COOLDOWN_S": "12",
        })
        self.assertEqual(wd.stall_threshold_s, 3.5)
        self.assertEqual(wd.cooldown_s, 12.0)

    def test_no_dump_before_first_beat(self):
        # Until the strategy beats at least once, a "stall" is just not-started:
        # the watchdog must not fire (avoids a spurious dump during boot).
        dumps = []
        clk = [0.0]
        wd = ClockWatchdog(stall_threshold_s=5.0, time_fn=lambda: clk[0],
                           dump_fn=lambda reason: dumps.append(reason))
        clk[0] = 100.0  # huge elapsed, but no beat yet
        wd._check_once()
        self.assertEqual(dumps, [])

    def test_fires_on_stall_and_respects_cooldown(self):
        dumps = []
        clk = [1000.0]
        wd = ClockWatchdog(stall_threshold_s=5.0, cooldown_s=30.0,
                           time_fn=lambda: clk[0],
                           dump_fn=lambda reason: dumps.append((reason, clk[0])))
        wd.beat(tick_ts=1.0)            # first beat at t=1000
        clk[0] = 1004.0                 # 4s elapsed < 5s -> no fire
        wd._check_once()
        self.assertEqual(len(dumps), 0)
        clk[0] = 1006.0                 # 6s elapsed >= 5s -> fire once
        wd._check_once()
        self.assertEqual(len(dumps), 1)
        clk[0] = 1010.0                 # still stalled but within cooldown -> no re-fire
        wd._check_once()
        self.assertEqual(len(dumps), 1)
        clk[0] = 1040.0                 # cooldown elapsed, still stalled -> fire again
        wd._check_once()
        self.assertEqual(len(dumps), 2)

    def test_beat_clears_stall(self):
        dumps = []
        clk = [0.0]
        wd = ClockWatchdog(stall_threshold_s=5.0, time_fn=lambda: clk[0],
                           dump_fn=lambda reason: dumps.append(reason))
        wd.beat(tick_ts=1.0)
        clk[0] = 10.0
        wd.beat(tick_ts=2.0)            # fresh beat right before the check
        wd._check_once()
        self.assertEqual(dumps, [])

    def test_beat_is_cheap_and_never_raises(self):
        wd = ClockWatchdog(stall_threshold_s=5.0)
        # Even with a broken time_fn, beat must swallow and never propagate.
        wd._time_fn = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
        wd.beat(tick_ts=1.0)  # must not raise

    def test_dump_failure_is_swallowed(self):
        clk = [1000.0]

        def broken_dump(reason):
            raise RuntimeError("disk full")

        wd = ClockWatchdog(stall_threshold_s=1.0, time_fn=lambda: clk[0], dump_fn=broken_dump)
        wd.beat(tick_ts=1.0)
        clk[0] = 1005.0
        wd._check_once()  # must not raise even though dump_fn blew up

    def test_start_stop_thread_lifecycle(self):
        wd = ClockWatchdog(stall_threshold_s=100.0, poll_interval_s=0.01)
        wd.start()
        self.assertTrue(wd.is_running())
        wd.beat(tick_ts=1.0)
        time.sleep(0.05)
        wd.stop()
        self.assertFalse(wd.is_running())

    def test_faulthandler_deadman_armed_and_rearmed_and_cancelled(self):
        fake = _FakeFaulthandler()
        with tempfile.TemporaryDirectory() as d:
            wd = ClockWatchdog(stall_threshold_s=7.0, poll_interval_s=100.0,
                               dump_dir=d, arm_faulthandler=True, faulthandler_mod=fake)
            wd.start()
            self.assertTrue(wd._fh_armed)
            self.assertEqual(fake.armed, 1)          # armed once at start
            self.assertEqual(fake.last_timeout, 7.0)
            wd.beat(tick_ts=1.0)                      # re-arm = cancel + dump_traceback_later
            self.assertEqual(fake.cancelled, 1)
            self.assertEqual(fake.armed, 2)
            wd.stop()
            self.assertFalse(wd._fh_armed)
            self.assertEqual(fake.cancelled, 2)       # cancelled on stop

    def test_faulthandler_disabled_makes_no_calls(self):
        fake = _FakeFaulthandler()
        with tempfile.TemporaryDirectory() as d:
            wd = ClockWatchdog(stall_threshold_s=7.0, poll_interval_s=100.0,
                               dump_dir=d, arm_faulthandler=False, faulthandler_mod=fake)
            wd.start()
            wd.beat(tick_ts=1.0)
            wd.stop()
            self.assertEqual(fake.armed, 0)
            self.assertEqual(fake.cancelled, 0)

    def test_faulthandler_arm_failure_is_swallowed(self):
        fake = _FakeFaulthandler(fail=True)
        with tempfile.TemporaryDirectory() as d:
            wd = ClockWatchdog(stall_threshold_s=7.0, poll_interval_s=100.0,
                               dump_dir=d, arm_faulthandler=True, faulthandler_mod=fake)
            wd.start()                  # dump_traceback_later raises -> must not crash
            self.assertFalse(wd._fh_armed)
            wd.beat(tick_ts=1.0)        # _rearm is a no-op when not armed
            wd.stop()

    def test_from_env_faulthandler_default_on_opt_out(self):
        on = ClockWatchdog.from_env(env={"HUMMINGBOT_CLOCK_WATCHDOG": "1"})
        self.assertTrue(on.arm_faulthandler)
        off = ClockWatchdog.from_env(env={
            "HUMMINGBOT_CLOCK_WATCHDOG": "1",
            "HUMMINGBOT_CLOCK_WATCHDOG_FAULTHANDLER": "0",
        })
        self.assertFalse(off.arm_faulthandler)

    def test_loop_capture_and_task_snapshot(self):
        async def _run():
            wd = ClockWatchdog(stall_threshold_s=5.0)
            wd.beat(tick_ts=1.0)  # captures the running loop
            snap = wd._format_task_snapshot()
            return snap

        snap = asyncio.run(_run())
        self.assertIsInstance(snap, str)
        self.assertIn("asyncio tasks", snap)


if __name__ == "__main__":
    unittest.main()
