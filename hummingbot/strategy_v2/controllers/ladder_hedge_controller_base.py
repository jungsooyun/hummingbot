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
    # JEP-270 IB circuit breaker. The real bug fix is the gate sizing (ladder_maker_executor
    # min(rung_sum, cap)) + the per-IB WARNING (cross_venue base): with a correct gate, healthy
    # operation produces zero IB, and any residual IB is now LOUD instead of silent.
    #
    # This breaker is pure defense-in-depth and ships ARMED-OFF (_IB_BREAKER_ENABLED = False),
    # matching the JEP-238 OFF-by-default breaker convention. Adversarial review (JEP-270, 2
    # independent engines) found that ENFORCING it as-is is a net regression on a LIVE MM bot:
    # a transient JEP-209 /info-429 collateral starve momentarily reads HL collateral as 0 and
    # trips ~10 consecutive pre-quote IB terminations in ~10s, which would FALSE-LATCH a healthy
    # maker into a ~300s quoting halt (pre-breaker that glitch self-healed in ~1 tick). It also
    # lacks success-reset (hair-trigger re-latch), is not is_updatable, isn't wired to
    # SafetyNotifier, and is memoryless across restart. Until those are fixed (follow-up issue),
    # we keep the COUNTING (observability) but never act. When disabled the breaker only tracks
    # _consecutive_ib; it never pauses creation.
    _IB_BREAKER_ENABLED = False
    _IB_BREAKER_THRESHOLD = 10
    _IB_BREAKER_PROBE_INTERVAL_S = 300.0
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
        seen = getattr(self, "_seen_done_ids", None)
        if seen is None:
            seen = self._seen_done_ids = set()
            self._consecutive_ib = 0
            # _now() is epoch seconds; 0.0 init would make a first latch from preexisting dones see
            # now-0 >> interval and immediately probe (defeating the latch). Init to _now().
            self._ib_last_probe_ts = self._now()
            self._ib_last_alert_ts = 0.0
        new_done = [e for e in self.executors_info if e.is_done and e.id not in seen]
        new_done.sort(key=lambda e: (e.close_timestamp if e.close_timestamp is not None else e.timestamp))
        for e in new_done:
            if e.close_type == CloseType.INSUFFICIENT_BALANCE:
                self._consecutive_ib += 1
            else:
                self._consecutive_ib = 0
            seen.add(e.id)
        # prune ids the orchestrator no longer reports (bound growth; no recount since gone == gone).
        self._seen_done_ids = seen & {e.id for e in self.executors_info}

    def _maybe_alert_ib_breaker(self, now: float) -> None:
        if (now - getattr(self, "_ib_last_alert_ts", 0.0)) >= self._IB_BREAKER_ALERT_INTERVAL_S:
            self._ib_last_alert_ts = now
            self.logger().warning(
                "IB circuit breaker LATCHED: %s consecutive INSUFFICIENT_BALANCE executor "
                "terminations on controller %s -- pausing executor (re)creation (probe every %ss). "
                "Maker quoting stopped; existing hedge/position unchanged (delta-neutral). "
                "Check maker-leg collateral / JEP-270 gate sizing.",
                self._consecutive_ib, getattr(self.config, "id", "?"), self._IB_BREAKER_PROBE_INTERVAL_S,
            )

    def determine_executor_actions(self) -> List[ExecutorAction]:
        self._update_ib_breaker()
        active = self.filter_executors(self.executors_info, filter_func=lambda e: not e.is_done)
        if len(active) >= self.config.max_executors:
            return []
        now = self._now()
        # ENFORCEMENT is gated OFF by default (see class docstring): _update_ib_breaker above still
        # COUNTS consecutive IB for observability, but we only pause/probe when explicitly armed.
        if self._IB_BREAKER_ENABLED and self._consecutive_ib >= self._IB_BREAKER_THRESHOLD:
            if (now - self._ib_last_probe_ts) < self._IB_BREAKER_PROBE_INTERVAL_S:
                self._maybe_alert_ib_breaker(now)
                return []
            # else: probe due -> fall through to create exactly one
        self._ib_last_probe_ts = now
        return [CreateExecutorAction(
            executor_config=self._build_executor_config(),
            controller_id=self.config.id,
        )]

    def _build_executor_config(self):
        raise NotImplementedError
