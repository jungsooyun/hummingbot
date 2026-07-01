import time
from collections import deque
from decimal import Decimal
from typing import List, Optional

from pydantic import Field

from hummingbot.core.data_type.common import MarketDict
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType


class LadderHedgeControllerConfigBase(ControllerConfigBase):
    maker_connector: str = Field(...)
    maker_trading_pair: str = Field(...)
    maker_tick: Decimal = Field(...)
    hedge_connector: str = Field(...)
    hedge_trading_pair: str = Field(...)
    hedge_tick: Decimal = Field(...)
    fx_connector: Optional[str] = None
    fx_trading_pair: Optional[str] = None
    static_fx_rate: Optional[Decimal] = None
    side_aware_fx: bool = True
    leverage: int = 1
    min_reprice_interval_s: float = 0.75
    min_reprice_delta_ticks: Decimal = Decimal("2")
    # Maker-leg order discipline. True (default): LIMIT_MAKER (post-only) — a rung price that
    # crosses the fast maker book is venue-rejected (no fill), pure-maker. False: plain LIMIT at
    # the same rung price (= fair + net + round_trip_cost, the profitability floor), so a
    # crossing rung fills as a taker at a price >= its edge instead of being rejected; resting
    # orders are still maker fills. The limit price is the floor -> only profitable immediate
    # (taker) fills occur. Runtime-togglable so post-only discipline can be restored without a redeploy.
    maker_post_only: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    kill_switch: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    observe: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    ws_staleness_kill_switch_enabled: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    max_kis_ws_age_s: Optional[float] = Field(default=3.0, json_schema_extra={"is_updatable": True})
    max_hl_ws_age_s: Optional[float] = Field(default=12.0, json_schema_extra={"is_updatable": True})
    ws_staleness_grace_s: float = Field(default=90.0, json_schema_extra={"is_updatable": True})
    session_halt_gate_enabled: bool = Field(default=True, json_schema_extra={"is_updatable": False})
    trading_hours_gate_enabled: bool = Field(default=True, json_schema_extra={"is_updatable": False})
    session_halt_max_ws_age_s: float = Field(default=3.0, json_schema_extra={"is_updatable": True})
    session_halt_max_book_static_s: float = Field(default=15.0, json_schema_extra={"is_updatable": True})
    # JEP-198 interim auction-gap guard: once a freeze/CB is detected, hold the maker-quote
    # halt this many seconds past the freeze END (covers the ~10min CB single-price auction the
    # clock can't see). Default 1800s = full CB (20min freeze + 10min auction). 0 disables.
    session_halt_cooldown_s: float = Field(default=1800.0, json_schema_extra={"is_updatable": True})
    # JEP-226: see LadderMakerExecutorConfig.hedge_session_defer_cap_s. Cap (s) the hedge taker
    # defers during a clock-scheduled single-price auction before force-hedging; <=0 disables the
    # session-aware hedge gate (behavior-neutral). Hot-tunable; the gate behavior itself needs a cold-boot.
    hedge_session_defer_cap_s: float = Field(default=30.0, json_schema_extra={"is_updatable": True})
    adopt_existing_inventory: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    latency_profiling: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    max_executors: int = Field(default=1, json_schema_extra={"is_updatable": True})
    # JEP-272 IB circuit breaker (defense-in-depth vs the JEP-270 silent maker churn). Ships OFF
    # (ib_breaker_enabled=False): _update_ib_breaker still COUNTS for observability but never pauses
    # creation. Staged-enable = flip ib_breaker_enabled True live (is_updatable), observe for
    # false-latches, tune window/backoff. All is_updatable so no cold boot. Read via the coercing
    # accessors on the controller (a bad hot-edit must not crash the control loop — JEP-283 lesson).
    ib_breaker_enabled: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    ib_breaker_threshold: int = Field(default=10, json_schema_extra={"is_updatable": True})
    ib_breaker_window_s: float = Field(default=60.0, json_schema_extra={"is_updatable": True})
    ib_breaker_probe_base_s: float = Field(default=15.0, json_schema_extra={"is_updatable": True})
    ib_breaker_probe_max_s: float = Field(default=300.0, json_schema_extra={"is_updatable": True})

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets.add_or_update(self.maker_connector, self.maker_trading_pair)
        markets.add_or_update(self.hedge_connector, self.hedge_trading_pair)
        if self.fx_connector and self.fx_trading_pair:
            markets.add_or_update(self.fx_connector, self.fx_trading_pair)
        return markets


class LadderHedgeControllerBase(ControllerBase):
    # JEP-270/JEP-272 IB circuit breaker. Defense-in-depth against the JEP-270 silent maker churn
    # (maker executor created + IB-terminated ~1/s). The real churn fix is the JEP-270 gate sizing;
    # this breaker pauses (re)creation + alerts if IB terminations recur at a churn RATE.
    #
    # Ships OFF (config.ib_breaker_enabled=False): _update_ib_breaker still COUNTS for observability
    # but never latches, so behavior is identical to "always create". Hardened per JEP-272 so it can
    # be enabled live (separate gated step): success-reset, windowed-rate + escalating backoff probe,
    # is_updatable tunables, SafetyNotifier wiring, defensive coercion.
    #
    # Operator log-alert cadence is a fixed private constant (never tuned — YAGNI, and keeping it
    # fixed avoids adding a coercion surface). The enable flag + tunables live on the config.
    _IB_BREAKER_ALERT_INTERVAL_S = 300.0

    def _now(self) -> float:
        return time.time()

    async def update_processed_data(self):
        pass

    # ---- is_updatable config accessors with defensive coercion (a malformed hot-edit falls back to
    # ---- the field default rather than raising inside the control loop — JEP-283 robust-coercion). ----
    def _ib_breaker_enabled(self) -> bool:
        return bool(getattr(self.config, "ib_breaker_enabled", False))

    def _ib_threshold(self) -> int:
        try:
            v = int(getattr(self.config, "ib_breaker_threshold", 10))
        except (TypeError, ValueError):
            return 10
        return v if v >= 1 else 10

    def _ib_window_s(self) -> float:
        try:
            v = float(getattr(self.config, "ib_breaker_window_s", 60.0))
        except (TypeError, ValueError):
            return 60.0
        return v if v > 0 else 60.0

    def _ib_probe_base(self) -> float:
        try:
            v = float(getattr(self.config, "ib_breaker_probe_base_s", 15.0))
        except (TypeError, ValueError):
            return 15.0
        return v if v > 0 else 15.0

    def _ib_probe_max(self) -> float:
        base = self._ib_probe_base()
        try:
            v = float(getattr(self.config, "ib_breaker_probe_max_s", 300.0))
        except (TypeError, ValueError):
            return max(base, 300.0)
        return v if v >= base else max(base, 300.0)

    def _update_ib_breaker(self) -> None:
        # lazy-init (avoid touching ControllerBase.__init__ signature; codebase getattr pattern).
        times = getattr(self, "_ib_times", None)
        if times is None:
            times = self._ib_times = deque()
            self._ib_latched = False
            self._probe_fail_count = 0
            self._ib_latch_epoch = 0
            self._ib_last_alert_ts = 0.0
            self._seen_done_ids = set()
            self._prev_running_ids = set()
            # _now() is epoch seconds; a 0.0 init would make a first latch from pre-existing dones see
            # now-0 >> pause and immediately probe (defeating the very first hold). Init to _now().
            self._ib_last_probe_ts = self._now()

        now = self._now()
        current_running_ids = {e.id for e in self.executors_info
                               if getattr(e, "status", None) == RunnableStatus.RUNNING}

        # Disable = temporary enforcement OVERRIDE (not a recovery): release the hold and keep
        # counting, but do NOT notify / bump epoch / reset backoff — so a disable->re-enable within
        # the same churn window resumes the SAME incident rather than starting a fresh episode.
        if not self._ib_breaker_enabled():
            self._ib_latched = False

        # Success-reset (item 1): an id RUNNING on THIS tick AND the previous one passed its on_start
        # pre-funding gate (a doomed IB executor is only briefly RUNNING before it IB-terminates;
        # RunnableBase.start() sets RUNNING synchronously before validate). RUNNING-across-two-ticks is
        # the robust controller-only "quote-capable" signal (NOT is_trading, which needs a fill).
        survived = current_running_ids & self._prev_running_ids
        if survived:
            was_latched = self._ib_latched
            self._ib_times.clear()
            self._probe_fail_count = 0
            self._ib_latched = False
            if was_latched:
                self._emit_ib_alert("recovered", "a probe executor survived two consecutive ticks")
                self._ib_latch_epoch += 1
                self._ib_last_alert_ts = 0.0  # let the next episode log/alert immediately

        # Count newly-done IB terminations into the window (close-timestamp order). Non-IB deaths are
        # not churn: they are ignored here and simply age out of the window.
        new_done = [e for e in self.executors_info if e.is_done and e.id not in self._seen_done_ids]
        new_done.sort(key=lambda e: (e.close_timestamp if e.close_timestamp is not None else e.timestamp))
        for e in new_done:
            if e.close_type == CloseType.INSUFFICIENT_BALANCE:
                ts = e.close_timestamp if e.close_timestamp is not None else e.timestamp
                self._ib_times.append(ts)
            self._seen_done_ids.add(e.id)

        # Prune the window to (now - window_s, now].
        cutoff = now - self._ib_window_s()
        while self._ib_times and self._ib_times[0] < cutoff:
            self._ib_times.popleft()

        # Trigger (window -> latch): churn RATE reached and not already latched -> new latch episode.
        if self._ib_breaker_enabled() and not self._ib_latched and len(self._ib_times) >= self._ib_threshold():
            self._ib_latched = True

        # Bound growth; no recount (gone == gone). Snapshot running ids for next tick's success signal.
        self._seen_done_ids &= {e.id for e in self.executors_info}
        self._prev_running_ids = current_running_ids

    def _emit_ib_alert(self, event: str, detail: str) -> None:
        # Route latch/recovery to the JEP-258/259 SafetyNotifier. Lazy-import + fully guarded: a test
        # env without the module (or any notify failure) must never crash the control loop. The key
        # carries the latch epoch so a re-latch after a recovery alerts again (SafetyNotifier dedup is
        # process-permanent, so a fixed key would alert only once per process).
        try:
            from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.safety_notifier import (
                SafetyNotifier,
            )
            cid = getattr(self.config, "id", "?")
            key = f"{cid}:ib_breaker_{event}:{self._ib_latch_epoch}"
            if event == "latched":
                msg = (f"IB circuit breaker LATCHED on {cid}: {detail}. Maker quoting paused "
                       f"(delta-neutral; existing hedge/position unchanged). Check maker-leg "
                       f"collateral / JEP-270 gate sizing.")
            else:
                msg = f"IB circuit breaker RECOVERED on {cid}: {detail}; maker quoting resumes."
            SafetyNotifier.get_instance().notify(key, msg)
        except Exception:
            pass

    def _maybe_alert_ib_breaker(self, now: float) -> None:
        # SafetyNotifier push: attempted every held tick, deduped once-per-episode by the epoch key
        # (independent of the log rate-limit below).
        self._emit_ib_alert("latched", f"{len(self._ib_times)} IB terminations within "
                                       f"{self._ib_window_s():.0f}s")
        # Operator LOG line: rate-limited by the fixed _IB_BREAKER_ALERT_INTERVAL_S.
        if (now - getattr(self, "_ib_last_alert_ts", 0.0)) >= self._IB_BREAKER_ALERT_INTERVAL_S:
            self._ib_last_alert_ts = now
            pause = min(self._ib_probe_base() * 2 ** self._probe_fail_count, self._ib_probe_max())
            self.logger().warning(
                "IB circuit breaker LATCHED: %s INSUFFICIENT_BALANCE terminations within %ss on "
                "controller %s -- pausing executor (re)creation (backoff probe, current pause %.0fs). "
                "Maker quoting stopped; existing hedge/position unchanged (delta-neutral). "
                "Check maker-leg collateral / JEP-270 gate sizing.",
                len(self._ib_times), self._ib_window_s(), getattr(self.config, "id", "?"), pause,
            )

    def determine_executor_actions(self) -> List[ExecutorAction]:
        self._update_ib_breaker()
        active = self.filter_executors(self.executors_info, filter_func=lambda e: not e.is_done)
        if len(active) >= self.config.max_executors:
            return []
        now = self._now()
        # ENFORCEMENT: _ib_latched is the single source of truth (it already implies enabled — the
        # disable path in _update_ib_breaker force-clears it). A transient burst latches with
        # _probe_fail_count=0, so the first pause is only probe_base; the pause escalates only on
        # SUSTAINED failure (each probe re-fails without surviving two ticks).
        if self._ib_latched:
            pause = min(self._ib_probe_base() * 2 ** self._probe_fail_count, self._ib_probe_max())
            if (now - self._ib_last_probe_ts) < pause:
                self._maybe_alert_ib_breaker(now)
                return []
            self._probe_fail_count += 1  # probe due -> advance the backoff step
        self._ib_last_probe_ts = now
        return [CreateExecutorAction(
            executor_config=self._build_executor_config(),
            controller_id=self.config.id,
        )]

    def _build_executor_config(self):
        raise NotImplementedError
