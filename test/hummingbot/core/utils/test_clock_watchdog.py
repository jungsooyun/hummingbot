"""Tests for the JEP-218 clock-stall watchdog (off-by-default diagnostic)."""
import asyncio
import json
import os
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

    # ------------------------------------------------------ JEP-218 self-heal (opt-in)
    @staticmethod
    def _drive_scheduled_stall(wd, clk, base, polls, gap=1.0):
        """beat once at `base`, then poll `polls` times at +gap each. The small, uniform
        inter-poll gaps mark every poll as 'scheduled' (watchdog thread healthy) while beats
        stop -> exercises the consecutive-scheduled-stall path. With the default
        min_consecutive=2 an exit fires on the 3rd poll (poll0 has no prior baseline so
        consec=0, poll1->consec=1, poll2->consec=2)."""
        clk[0] = base
        wd.beat(tick_ts=1.0)
        for i in range(polls):
            clk[0] = base + 20.0 + i * gap
            wd._check_once()

    def test_self_heal_off_by_default_does_not_exit(self):
        # Default behavior is diagnostic-only: a stall dumps but NEVER terminates the process.
        events = []
        clk = [1000.0]
        wd = ClockWatchdog(stall_threshold_s=5.0, time_fn=lambda: clk[0],
                           dump_fn=lambda reason: events.append("dump"),
                           exit_fn=lambda code: events.append(("exit", code)))
        self.assertFalse(wd.self_heal)
        self._drive_scheduled_stall(wd, clk, 1000.0, polls=4)
        self.assertNotIn(("exit", 75), events)  # dumped, never exited
        self.assertIn("dump", events)

    def test_self_heal_exits_after_consecutive_scheduled_stalls(self):
        # When enabled, the diagnostic bundle is written FIRST, then the process is killed --
        # but only after N consecutive stalls observed with the watchdog thread scheduled.
        events = []
        clk = [1000.0]
        with tempfile.TemporaryDirectory() as d:
            wd = ClockWatchdog(stall_threshold_s=5.0, self_heal=True, self_heal_exit_code=75,
                               governor_path=os.path.join(d, "gov.json"),
                               time_fn=lambda: clk[0], wall_time_fn=lambda: 0.0,
                               dump_fn=lambda reason: events.append("dump"),
                               exit_fn=lambda code: events.append(("exit", code)))
            self._drive_scheduled_stall(wd, clk, 1000.0, polls=3)
        self.assertEqual(events, ["dump", ("exit", 75)])  # dump strictly before exit

    def test_self_heal_exits_even_if_dump_fails(self):
        # Recovery must fire even if the diagnostic dump itself blows up (disk full, etc.).
        events = []
        clk = [1000.0]

        def broken_dump(reason):
            raise RuntimeError("disk full")

        with tempfile.TemporaryDirectory() as d:
            wd = ClockWatchdog(stall_threshold_s=5.0, self_heal=True,
                               governor_path=os.path.join(d, "gov.json"),
                               time_fn=lambda: clk[0], wall_time_fn=lambda: 0.0,
                               dump_fn=broken_dump,
                               exit_fn=lambda code: events.append(("exit", code)))
            self._drive_scheduled_stall(wd, clk, 1000.0, polls=3)
        self.assertEqual(events, [("exit", 75)])

    def test_self_heal_fires_only_once(self):
        # cooldown_s=0 keeps the dump path eligible every poll so we isolate the self-heal
        # latch (NOT the cooldown) as what prevents a double exit.
        exits = []
        clk = [1000.0]
        with tempfile.TemporaryDirectory() as d:
            wd = ClockWatchdog(stall_threshold_s=5.0, cooldown_s=0.0, self_heal=True,
                               governor_path=os.path.join(d, "gov.json"),
                               time_fn=lambda: clk[0], wall_time_fn=lambda: 0.0,
                               dump_fn=lambda reason: None,
                               exit_fn=lambda code: exits.append(code))
            self._drive_scheduled_stall(wd, clk, 1000.0, polls=5)
        self.assertEqual(exits, [75])  # exactly one exit despite repeated stalls

    def test_self_heal_skipped_on_whole_process_deschedule(self):
        # A whole-process suspend (hypervisor live-migration / SIGSTOP / swap) freezes BOTH the
        # loop AND the watchdog thread. On resume the watchdog sees one giant elapsed -- it must
        # NOT kill a healthy bot. Only after the loop is shown genuinely wedged (scheduled polls
        # keep observing the stall) may self-heal fire.
        exits = []
        clk = [1000.0]
        with tempfile.TemporaryDirectory() as d:
            wd = ClockWatchdog(stall_threshold_s=5.0, self_heal=True,
                               governor_path=os.path.join(d, "gov.json"),
                               time_fn=lambda: clk[0], wall_time_fn=lambda: 0.0,
                               dump_fn=lambda reason: None,
                               exit_fn=lambda code: exits.append(code))
            clk[0] = 1000.0
            wd.beat(tick_ts=1.0)            # last_beat = 1000
            clk[0] = 1001.0
            wd._check_once()                # healthy baseline poll
            clk[0] = 1101.0                 # ~100s whole-process suspend: watchdog gap also ~100s
            wd._check_once()
            self.assertEqual(exits, [])     # must NOT exit on resume (process suspend != wedge)
            clk[0] = 1102.0                 # loop still not beating; watchdog now polling normally
            wd._check_once()                # scheduled stall -> consec=1
            clk[0] = 1103.0
            wd._check_once()                # scheduled stall -> consec=2 -> genuine wedge -> exit
            self.assertEqual(exits, [75])

    # ----- cross-restart governor -----
    def test_governor_allows_up_to_max_then_blocks(self):
        with tempfile.TemporaryDirectory() as d:
            wd = ClockWatchdog(self_heal=True, self_heal_max_per_window=2, self_heal_window_s=600.0,
                               governor_path=os.path.join(d, "gov.json"), wall_time_fn=lambda: 5000.0)
            self.assertTrue(wd._self_heal_governor_allows())   # 1st
            self.assertTrue(wd._self_heal_governor_allows())   # 2nd
            self.assertFalse(wd._self_heal_governor_allows())  # 3rd within window -> blocked

    def test_governor_forgets_history_outside_window(self):
        t = [5000.0]
        with tempfile.TemporaryDirectory() as d:
            wd = ClockWatchdog(self_heal=True, self_heal_max_per_window=1, self_heal_window_s=100.0,
                               governor_path=os.path.join(d, "gov.json"), wall_time_fn=lambda: t[0])
            self.assertTrue(wd._self_heal_governor_allows())   # records t=5000
            self.assertFalse(wd._self_heal_governor_allows())  # 1 in window -> blocked
            t[0] = 5101.0                                      # window elapsed
            self.assertTrue(wd._self_heal_governor_allows())   # old entry pruned -> allowed

    def test_governor_persists_across_instances(self):
        # The history file survives a process restart -> a fresh watchdog sees prior self-heals.
        with tempfile.TemporaryDirectory() as d:
            gp = os.path.join(d, "gov.json")
            wd1 = ClockWatchdog(self_heal=True, self_heal_max_per_window=1, self_heal_window_s=600.0,
                                governor_path=gp, wall_time_fn=lambda: 7000.0)
            self.assertTrue(wd1._self_heal_governor_allows())
            wd2 = ClockWatchdog(self_heal=True, self_heal_max_per_window=1, self_heal_window_s=600.0,
                                governor_path=gp, wall_time_fn=lambda: 7001.0)
            self.assertFalse(wd2._self_heal_governor_allows())  # sees wd1's exit -> blocked

    def test_self_heal_blocked_by_governor_does_not_exit(self):
        # Governor circuit-open (freeze survives restart) -> park in dump-only, never exit.
        exits = []
        clk = [1000.0]
        with tempfile.TemporaryDirectory() as d:
            gp = os.path.join(d, "gov.json")
            with open(gp, "w") as fh:
                json.dump([0.0, 0.0], fh)  # pre-seed at max within window
            wd = ClockWatchdog(stall_threshold_s=5.0, self_heal=True,
                               self_heal_max_per_window=2, self_heal_window_s=600.0,
                               governor_path=gp, time_fn=lambda: clk[0], wall_time_fn=lambda: 1.0,
                               dump_fn=lambda reason: None,
                               exit_fn=lambda code: exits.append(code))
            self._drive_scheduled_stall(wd, clk, 1000.0, polls=3)
        self.assertEqual(exits, [])  # parked, no exit

    def test_self_heal_arms_through_intermittent_watchdog_starvation(self):
        # A GENUINE wedge (beats stopped) whose watchdog thread is INTERMITTENTLY starved
        # (alternating small/large inter-poll gaps) must still eventually self-heal: a
        # descheduled poll HOLDS the consecutive count instead of resetting it to 0.
        exits = []
        clk = [1000.0]
        with tempfile.TemporaryDirectory() as d:
            wd = ClockWatchdog(stall_threshold_s=5.0, self_heal=True,
                               governor_path=os.path.join(d, "gov.json"),
                               time_fn=lambda: clk[0], wall_time_fn=lambda: 0.0,
                               dump_fn=lambda reason: None, exit_fn=lambda code: exits.append(code))
            clk[0] = 1000.0; wd.beat(tick_ts=1.0)   # last_beat = 1000
            clk[0] = 1001.0; wd._check_once()        # healthy baseline poll (elapsed 1 < 5)
            clk[0] = 1011.0; wd._check_once()        # gap 10 > 3 -> descheduled, stalled -> HOLD (0)
            clk[0] = 1012.0; wd._check_once()        # gap 1 -> scheduled, stalled -> consec 1
            clk[0] = 1017.0; wd._check_once()        # gap 5 > 3 -> descheduled, stalled -> HOLD (1)
            clk[0] = 1018.0; wd._check_once()        # gap 1 -> scheduled -> consec 2 -> ARM -> exit
        self.assertEqual(exits, [75])

    def test_governor_reallows_after_boot_clock_reset(self):
        # CLOCK_BOOTTIME resets on a HOST reboot: prior entries are far-FUTURE vs a small
        # post-reboot clock -> dropped by the symmetric prune -> governor re-allows (a fresh
        # host boot is not a restart-loop and should not stay parked).
        with tempfile.TemporaryDirectory() as d:
            gp = os.path.join(d, "gov.json")
            with open(gp, "w") as fh:
                json.dump([50000.0, 50000.0], fh)   # boot-clock values from before the reboot
            wd = ClockWatchdog(self_heal=True, self_heal_max_per_window=2, self_heal_window_s=600.0,
                               governor_path=gp, wall_time_fn=lambda: 30.0)
            self.assertTrue(wd._self_heal_governor_allows())

    def test_governor_keeps_modest_future_dated_entries(self):
        # A small backward clock step makes a prior entry look slightly future-dated; the
        # governor RETAINS it (conservative -- never erase history on a minor clock wobble).
        with tempfile.TemporaryDirectory() as d:
            gp = os.path.join(d, "gov.json")
            with open(gp, "w") as fh:
                json.dump([1050.0], fh)             # 50s "future" vs now=1000, within window
            wd = ClockWatchdog(self_heal=True, self_heal_max_per_window=1, self_heal_window_s=600.0,
                               governor_path=gp, wall_time_fn=lambda: 1000.0)
            self.assertFalse(wd._self_heal_governor_allows())  # retained -> at cap -> blocked

    def test_governor_bare_filename_path_persists(self):
        # A bare-filename governor_path (dirname == "") must still persist -- os.makedirs("")
        # would raise and silently drop every write without the empty-dirname guard.
        cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as d:
            os.chdir(d)
            try:
                wd1 = ClockWatchdog(self_heal=True, self_heal_max_per_window=1,
                                    self_heal_window_s=600.0, governor_path="gov.json",
                                    wall_time_fn=lambda: 100.0)
                self.assertTrue(wd1._self_heal_governor_allows())   # records, persists
                wd2 = ClockWatchdog(self_heal=True, self_heal_max_per_window=1,
                                    self_heal_window_s=600.0, governor_path="gov.json",
                                    wall_time_fn=lambda: 101.0)
                self.assertFalse(wd2._self_heal_governor_allows())  # read back -> at cap -> blocked
            finally:
                os.chdir(cwd)

    def test_from_env_self_heal_off_by_default(self):
        wd = ClockWatchdog.from_env(env={"HUMMINGBOT_CLOCK_WATCHDOG": "1"})
        self.assertFalse(wd.self_heal)

    def test_from_env_self_heal_enabled_with_exit_code(self):
        wd = ClockWatchdog.from_env(env={
            "HUMMINGBOT_CLOCK_WATCHDOG": "1",
            "HUMMINGBOT_CLOCK_WATCHDOG_SELF_HEAL": "yes",
            "HUMMINGBOT_CLOCK_WATCHDOG_EXIT_CODE": "42",
        })
        self.assertTrue(wd.self_heal)
        self.assertEqual(wd.self_heal_exit_code, 42)

    def test_from_env_self_heal_governor_knobs(self):
        wd = ClockWatchdog.from_env(env={
            "HUMMINGBOT_CLOCK_WATCHDOG": "1",
            "HUMMINGBOT_CLOCK_WATCHDOG_SELF_HEAL": "1",
            "HUMMINGBOT_CLOCK_WATCHDOG_SELF_HEAL_MAX": "5",
            "HUMMINGBOT_CLOCK_WATCHDOG_SELF_HEAL_WINDOW_S": "1200",
        })
        self.assertEqual(wd.self_heal_max_per_window, 5)
        self.assertEqual(wd.self_heal_window_s, 1200.0)


if __name__ == "__main__":
    unittest.main()
