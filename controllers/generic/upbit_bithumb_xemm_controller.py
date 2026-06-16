import math
from collections import deque
from decimal import Decimal
from typing import Deque, Dict, List, Literal, Optional, Set, Tuple

from pydantic import Field, field_validator, model_validator

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import MarketDict, OrderType, PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.inventory_rebalance_executor.data_types import InventoryRebalanceExecutorConfig
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


class _MidPriceBuffer:
    def __init__(self, maxlen: int = 500):
        self._samples: Deque[Tuple[float, float]] = deque(maxlen=maxlen)

    def add(self, timestamp: float, price: Decimal):
        if price is None or price <= Decimal("0"):
            return
        self._samples.append((float(timestamp), float(price)))

    def rv_bps(self, window_sec: float) -> Optional[Decimal]:
        if len(self._samples) < 3:
            return None
        latest_ts = self._samples[-1][0]
        cutoff = latest_ts - float(window_sec)
        prices = [price for ts, price in self._samples if ts >= cutoff and price > 0]
        if len(prices) < 3:
            return None
        returns = [math.log(prices[idx] / prices[idx - 1]) for idx in range(1, len(prices)) if prices[idx - 1] > 0]
        if len(returns) < 2:
            return None
        mean_ret = sum(returns) / len(returns)
        variance = sum((ret - mean_ret) ** 2 for ret in returns) / (len(returns) - 1)
        std = math.sqrt(max(variance, 0.0))
        return Decimal(str(std * 10000))


class UpbitBithumbXemmControllerConfig(ControllerConfigBase):
    controller_name: str = "upbit_bithumb_xemm_controller"
    controller_type: str = "generic"
    candles_config: List[CandlesConfig] = []

    maker_connector: str = Field(
        default="bithumb",
        json_schema_extra={"prompt": "Maker connector: ", "prompt_on_new": True},
    )
    maker_trading_pair: str = Field(
        default="BTC-KRW",
        json_schema_extra={"prompt": "Maker trading pair: ", "prompt_on_new": True},
    )
    taker_connector: str = Field(
        default="upbit",
        json_schema_extra={"prompt": "Taker connector: ", "prompt_on_new": True},
    )
    taker_trading_pair: str = Field(
        default="BTC-KRW",
        json_schema_extra={"prompt": "Taker trading pair: ", "prompt_on_new": True},
    )

    buy_levels_targets_amount: List[List[Decimal]] = Field(
        default="0.0008,1-0.0015,2-0.0025,4",
        json_schema_extra={
            "prompt": (
                "Buy levels (target_profitability,weight-target_profitability,weight). "
                "Near level should have smaller weight; far level larger weight: "
            ),
            "prompt_on_new": True,
        },
    )
    sell_levels_targets_amount: List[List[Decimal]] = Field(
        default="0.0008,1-0.0015,2-0.0025,4",
        json_schema_extra={
            "prompt": (
                "Sell levels (target_profitability,weight-target_profitability,weight). "
                "Near level should have smaller weight; far level larger weight: "
            ),
            "prompt_on_new": True,
        },
    )

    min_profitability_delta: Decimal = Field(
        default=Decimal("0.0004"),
        json_schema_extra={"prompt": "Min profitability delta below target: ", "prompt_on_new": True, "is_updatable": True},
    )
    max_profitability_delta: Decimal = Field(
        default=Decimal("0.006"),
        json_schema_extra={"prompt": "Max profitability delta above target: ", "prompt_on_new": True, "is_updatable": True},
    )

    max_executors_per_side: int = Field(default=3, json_schema_extra={"is_updatable": True})
    delay_between_creations: int = Field(default=2, json_schema_extra={"is_updatable": True})

    inventory_target_base: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    max_inventory_skew_base: Decimal = Field(
        default=Decimal("0.01"),
        json_schema_extra={"prompt": "Max maker-taker base imbalance: ", "prompt_on_new": True, "is_updatable": True},
    )
    inventory_skew_side_gating_enabled: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Block new entries when maker-taker base skew exceeds max_inventory_skew_base (True/False): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    maker_price_refresh_pct: Decimal = Field(
        default=Decimal("0.0002"),
        json_schema_extra={
            "prompt": "Refresh maker order when target drift exceeds this pct (e.g. 0.0002=0.02%): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    maker_order_max_age_seconds: float = Field(
        default=30.0,
        json_schema_extra={"prompt": "Max maker order age before refresh (seconds): ", "prompt_on_new": True, "is_updatable": True},
    )
    latency_diagnostics_enabled: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": "Enable XEMM latency diagnostics logs (True/False): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    min_profitability_guard: Decimal = Field(
        default=Decimal("0"),
        json_schema_extra={"prompt": "Minimum profitability required to place hedge: ", "prompt_on_new": True, "is_updatable": True},
    )
    allow_loss_hedge: bool = Field(
        default=False,
        json_schema_extra={"prompt": "Allow hedging even if profitability below guard (True/False): ", "prompt_on_new": True, "is_updatable": True},
    )
    close_out_loss_cap_bps: Decimal = Field(
        default=Decimal("0"),
        json_schema_extra={
            "prompt": "Allow close-out hedges within this loss cap (bps, 0 to disable): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    hedge_aggregation_window_sec: float = Field(
        default=1.0,
        json_schema_extra={"prompt": "Hedge aggregation window (seconds): ", "prompt_on_new": True, "is_updatable": True},
    )
    max_unhedged_notional_quote: Decimal = Field(
        default=Decimal("0"),
        json_schema_extra={"prompt": "Force hedge when unhedged notional exceeds this quote amount (0 to disable): ", "prompt_on_new": True, "is_updatable": True},
    )
    rate_limit_backoff_factor: float = Field(
        default=1.0,
        json_schema_extra={"prompt": "Backoff multiplier when rate limit nearing (1=no backoff): ", "prompt_on_new": True, "is_updatable": True},
    )
    market_data_stale_timeout_sec: float = Field(
        default=3.0,
        json_schema_extra={
            "prompt": "Order book stale timeout in seconds (fail-closed): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    market_data_recovery_grace_sec: float = Field(
        default=2.0,
        json_schema_extra={
            "prompt": "Grace period after market data recovery before creating new executors (seconds): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    cancel_open_orders_on_stale: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": "Cancel open executors when data stale is detected (True/False): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    stale_fill_hedge_mode: str = Field(
        default="pause",
        json_schema_extra={
            "prompt": "Stale fill hedge mode (pause/market): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    allow_one_sided_inventory_mode: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": "Allow maker-only execution when taker balance is insufficient (True/False): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    opportunity_gate_enabled: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    opportunity_tick_mode: Literal["fixed", "dynamic"] = Field(default="dynamic", json_schema_extra={"is_updatable": True})
    opportunity_min_ticks: int = Field(default=1, json_schema_extra={"is_updatable": True})
    opportunity_max_ticks: int = Field(default=2, json_schema_extra={"is_updatable": True})
    opportunity_vol_window_sec: float = Field(default=45.0, json_schema_extra={"is_updatable": True})
    opportunity_vol_low_bps: Decimal = Field(default=Decimal("4"), json_schema_extra={"is_updatable": True})
    opportunity_vol_high_bps: Decimal = Field(default=Decimal("6"), json_schema_extra={"is_updatable": True})
    opportunity_fee_buffer_bps_sell: Decimal = Field(default=Decimal("6"), json_schema_extra={"is_updatable": True})
    opportunity_fee_buffer_bps_buy: Decimal = Field(default=Decimal("9"), json_schema_extra={"is_updatable": True})
    opportunity_gate_stop_on_fail: bool = Field(default=True, json_schema_extra={"is_updatable": True})
    opportunity_debug_log_interval_sec: float = Field(default=5.0, json_schema_extra={"is_updatable": True})
    risk_session_loss_cut_quote: Decimal = Field(default=Decimal("20000"), json_schema_extra={"is_updatable": True})
    risk_unhedged_notional_cut_quote: Decimal = Field(default=Decimal("120000"), json_schema_extra={"is_updatable": True})
    risk_pause_cooldown_sec: float = Field(default=120.0, json_schema_extra={"is_updatable": True})
    risk_forced_flatten_cost_bps: Decimal = Field(default=Decimal("15"), json_schema_extra={"is_updatable": True})
    maker_price_source: Literal["formula", "best"] = Field(default="formula", json_schema_extra={"is_updatable": True})

    taker_order_type: OrderType = Field(
        default=OrderType.LIMIT,
        json_schema_extra={"prompt": "Taker order type (LIMIT/MARKET): ", "prompt_on_new": True, "is_updatable": True},
    )
    taker_slippage_buffer_bps: Decimal = Field(
        default=Decimal("8"),
        json_schema_extra={"prompt": "Taker slippage buffer (bps): ", "prompt_on_new": True, "is_updatable": True},
    )
    taker_order_max_age_seconds: float = Field(
        default=4.0,
        json_schema_extra={"prompt": "Taker limit order max age (sec): ", "prompt_on_new": True, "is_updatable": True},
    )
    taker_max_retries: int = Field(default=3, json_schema_extra={"is_updatable": True})
    taker_fallback_to_market: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": "Fallback to market after taker retries exhausted (True/False): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    upbit_shadow_maker_enabled: bool = Field(
        default=False,
        json_schema_extra={"prompt": "Enable Upbit shadow maker layer (True/False): ", "prompt_on_new": True, "is_updatable": True},
    )
    upbit_shadow_global_notional_cap_quote: Decimal = Field(
        default=Decimal("0"),
        json_schema_extra={"prompt": "Global shadow notional cap in quote (0 to disable): ", "prompt_on_new": True, "is_updatable": True},
    )
    upbit_shadow_price_refresh_pct: Decimal = Field(
        default=Decimal("0.0002"),
        json_schema_extra={"prompt": "Shadow maker refresh pct: ", "prompt_on_new": True, "is_updatable": True},
    )
    upbit_shadow_order_max_age_seconds: float = Field(
        default=20.0,
        json_schema_extra={"prompt": "Shadow maker max order age (seconds): ", "prompt_on_new": True, "is_updatable": True},
    )
    upbit_shadow_extra_buffer_bps: Decimal = Field(
        default=Decimal("0"),
        json_schema_extra={"prompt": "Shadow maker extra safety buffer (bps): ", "prompt_on_new": True, "is_updatable": True},
    )
    upbit_shadow_prefill_cross_timeout_sec: float = Field(
        default=1.0,
        json_schema_extra={"prompt": "Shadow prefill cross timeout (seconds): ", "prompt_on_new": True, "is_updatable": True},
    )
    upbit_shadow_prefill_unwind_timeout_sec: float = Field(
        default=1.0,
        json_schema_extra={"prompt": "Shadow prefill unwind timeout (seconds): ", "prompt_on_new": True, "is_updatable": True},
    )
    upbit_shadow_prefill_max_retries: int = Field(
        default=1,
        json_schema_extra={"prompt": "Shadow prefill unwind max retries: ", "prompt_on_new": True, "is_updatable": True},
    )
    bithumb_stp_prevention_enabled: bool = Field(
        default=True,
        json_schema_extra={"prompt": "Enable Bithumb self-trade prevention guard (True/False): ", "prompt_on_new": True, "is_updatable": True},
    )
    bithumb_stp_base_offset_ticks: int = Field(
        default=1,
        json_schema_extra={"prompt": "Bithumb STP base offset ticks: ", "prompt_on_new": True, "is_updatable": True},
    )
    bithumb_stp_max_offset_ticks: int = Field(
        default=3,
        json_schema_extra={"prompt": "Bithumb STP max offset ticks: ", "prompt_on_new": True, "is_updatable": True},
    )
    bithumb_stp_retry_cooldown_sec: float = Field(
        default=1.0,
        json_schema_extra={"prompt": "Bithumb STP retry cooldown (seconds): ", "prompt_on_new": True, "is_updatable": True},
    )
    bithumb_stp_pause_after_rejects: int = Field(
        default=3,
        json_schema_extra={"prompt": "Pause side after this many STP rejects: ", "prompt_on_new": True, "is_updatable": True},
    )
    bithumb_stp_pause_duration_sec: float = Field(
        default=30.0,
        json_schema_extra={"prompt": "Bithumb STP side pause duration (seconds): ", "prompt_on_new": True, "is_updatable": True},
    )
    bithumb_stp_consider_pending_cancel_as_conflict: bool = Field(
        default=True,
        json_schema_extra={"prompt": "Treat pending-cancel orders as STP conflicts (True/False): ", "prompt_on_new": True, "is_updatable": True},
    )

    inventory_rebalance_enabled: bool = Field(default=False, json_schema_extra={"is_updatable": True})
    inventory_rebalance_soft_band_base: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    inventory_rebalance_hard_band_base: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    inventory_rebalance_min_slice_base: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    inventory_rebalance_max_slice_base: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    inventory_rebalance_min_expected_pnl_quote: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    inventory_rebalance_delay_between_creations: float = Field(default=10.0, json_schema_extra={"is_updatable": True})
    inventory_rebalance_passive_order_max_age_seconds: float = Field(default=30.0, json_schema_extra={"is_updatable": True})
    inventory_rebalance_passive_price_refresh_pct: Decimal = Field(default=Decimal("0.0002"), json_schema_extra={"is_updatable": True})
    inventory_rebalance_entry_slippage_buffer_bps: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    inventory_rebalance_hedge_slippage_buffer_bps: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})
    inventory_rebalance_bithumb_rebate_bps: Decimal = Field(default=Decimal("0"), json_schema_extra={"is_updatable": True})

    @field_validator("buy_levels_targets_amount", "sell_levels_targets_amount", mode="before")
    @classmethod
    def validate_levels_targets_amount(cls, value):
        if isinstance(value, str):
            value = [level.strip() for level in value.split("-") if level.strip()]
            parsed = []
            for level in value:
                target_str, weight_str = [element.strip() for element in level.split(",")]
                parsed.append([Decimal(target_str), Decimal(weight_str)])
            return parsed
        if isinstance(value, list):
            return [[Decimal(str(level[0])), Decimal(str(level[1]))] for level in value]
        raise ValueError("Invalid levels format. Expected string or list.")

    @field_validator("taker_order_type", mode="before")
    @classmethod
    def validate_taker_order_type(cls, value):
        if isinstance(value, OrderType):
            return value
        if isinstance(value, int):
            return OrderType(value)
        if isinstance(value, str):
            normalized = value.strip().upper()
            if normalized.isdigit():
                return OrderType(int(normalized))
            try:
                return OrderType[normalized]
            except KeyError as e:
                raise ValueError(f"Invalid taker_order_type: {value}") from e
        raise ValueError(f"Invalid taker_order_type type: {type(value)}")

    @field_validator("stale_fill_hedge_mode", mode="before")
    @classmethod
    def validate_stale_fill_hedge_mode(cls, value):
        normalized = str(value).strip().lower()
        valid_modes = {"pause", "market"}
        if normalized not in valid_modes:
            raise ValueError(f"Invalid stale_fill_hedge_mode: {value}. Expected one of {sorted(valid_modes)}")
        return normalized

    @model_validator(mode="after")
    def post_validations(self):
        maker_base, maker_quote = split_hb_trading_pair(self.maker_trading_pair)
        taker_base, taker_quote = split_hb_trading_pair(self.taker_trading_pair)
        if maker_base != taker_base:
            raise ValueError("Maker and taker base assets must match for XEMM.")
        if maker_quote != taker_quote:
            raise ValueError("Maker and taker quote assets must match for this controller.")
        if self.max_executors_per_side < 1:
            raise ValueError("max_executors_per_side must be >= 1")
        if self.close_out_loss_cap_bps < Decimal("0"):
            raise ValueError("close_out_loss_cap_bps must be >= 0")
        if self.upbit_shadow_maker_enabled and not (self.maker_connector == "bithumb" and self.taker_connector == "upbit"):
            raise ValueError("upbit_shadow_maker_enabled is supported only for maker=bithumb and taker=upbit.")
        if self.bithumb_stp_base_offset_ticks < 1:
            raise ValueError("bithumb_stp_base_offset_ticks must be >= 1")
        if self.bithumb_stp_max_offset_ticks < self.bithumb_stp_base_offset_ticks:
            raise ValueError("bithumb_stp_max_offset_ticks must be >= bithumb_stp_base_offset_ticks")
        if self.bithumb_stp_retry_cooldown_sec <= 0:
            raise ValueError("bithumb_stp_retry_cooldown_sec must be > 0")
        if self.bithumb_stp_pause_after_rejects < 1:
            raise ValueError("bithumb_stp_pause_after_rejects must be >= 1")
        if self.bithumb_stp_pause_duration_sec <= 0:
            raise ValueError("bithumb_stp_pause_duration_sec must be > 0")
        if self.opportunity_min_ticks < 1:
            raise ValueError("opportunity_min_ticks must be >= 1")
        if self.opportunity_max_ticks < self.opportunity_min_ticks:
            raise ValueError("opportunity_max_ticks must be >= opportunity_min_ticks")
        if self.opportunity_vol_low_bps >= self.opportunity_vol_high_bps:
            raise ValueError("opportunity_vol_low_bps must be < opportunity_vol_high_bps")
        if self.opportunity_gate_enabled:
            if len(self.buy_levels_targets_amount) != 1 or len(self.sell_levels_targets_amount) != 1:
                raise ValueError("opportunity_gate_enabled requires single buy/sell level configuration.")
            if self.max_executors_per_side > 1:
                raise ValueError("opportunity_gate_enabled requires max_executors_per_side <= 1.")
        if self.inventory_rebalance_hard_band_base < self.inventory_rebalance_soft_band_base:
            raise ValueError("inventory_rebalance_hard_band_base must be >= inventory_rebalance_soft_band_base")
        if self.inventory_rebalance_min_slice_base < Decimal("0"):
            raise ValueError("inventory_rebalance_min_slice_base must be >= 0")
        if (
            self.inventory_rebalance_max_slice_base > Decimal("0")
            and self.inventory_rebalance_max_slice_base < self.inventory_rebalance_min_slice_base
        ):
            raise ValueError("inventory_rebalance_max_slice_base must be >= inventory_rebalance_min_slice_base")
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets.add_or_update(self.maker_connector, self.maker_trading_pair)
        markets.add_or_update(self.taker_connector, self.taker_trading_pair)
        return markets


class UpbitBithumbXemmController(ControllerBase):
    def __init__(self, config: UpbitBithumbXemmControllerConfig, *args, **kwargs):
        self.config = config
        self._last_creation_timestamp: Dict[TradeType, float] = {TradeType.BUY: 0.0, TradeType.SELL: 0.0}
        self._market_data_stale: bool = False
        self._market_data_stale_since_ts: Optional[float] = None
        self._market_data_recovery_grace_until_ts: Optional[float] = None
        self._stale_stop_sent_executor_ids: Set[str] = set()
        self._last_maker_freshness_sec: Optional[float] = None
        self._last_taker_freshness_sec: Optional[float] = None
        self._opportunity_stop_sent_executor_ids: Set[str] = set()
        self._opportunity_last_gate_reason: Dict[TradeType, str] = {TradeType.BUY: "", TradeType.SELL: ""}
        self._opportunity_edge_ticks: Dict[TradeType, Optional[int]] = {TradeType.BUY: None, TradeType.SELL: None}
        self._opportunity_edge_net_bps: Dict[TradeType, Optional[Decimal]] = {TradeType.BUY: None, TradeType.SELL: None}
        self._opportunity_required_ticks: int = int(self.config.opportunity_min_ticks)
        self._opportunity_last_debug_log_ts: float = 0.0
        self._opportunity_last_debug_log_ts_by_side: Dict[TradeType, float] = {
            TradeType.BUY: 0.0,
            TradeType.SELL: 0.0,
        }
        self._rv_maker = _MidPriceBuffer(maxlen=500)
        self._rv_taker = _MidPriceBuffer(maxlen=500)
        self._opportunity_rv_bps_maker: Optional[Decimal] = None
        self._opportunity_rv_bps_taker: Optional[Decimal] = None
        self._risk_pause_until_ts: float = 0.0
        self._risk_pause_reason: str = ""
        self._risk_effective_session_pnl_quote: Decimal = Decimal("0")
        self._risk_unhedged_notional_quote: Decimal = Decimal("0")
        self._last_inventory_rebalance_creation_ts: float = 0.0
        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        self._update_opportunity_buffers()

    def determine_executor_actions(self) -> List[ExecutorAction]:
        if not self.market_data_provider.ready:
            return []

        now = self.market_data_provider.time()
        is_stale = self._refresh_market_data_health(now=now)
        active_executors = self.filter_executors(self.executors_info, lambda executor: not executor.is_done)
        if is_stale:
            return self._stale_stop_actions(active_executors=active_executors)

        if self._market_data_recovery_grace_until_ts is not None and now < self._market_data_recovery_grace_until_ts:
            return []
        if self._market_data_recovery_grace_until_ts is not None and now >= self._market_data_recovery_grace_until_ts:
            self._market_data_recovery_grace_until_ts = None

        mid_price = self.market_data_provider.get_price_by_type(
            self.config.maker_connector,
            self.config.maker_trading_pair,
            PriceType.MidPrice,
        )
        if mid_price is None or mid_price <= Decimal("0"):
            return []

        inventory_delta = self._inventory_delta()
        allow_buy, allow_sell = self._allowed_sides(inventory_delta)
        if self._has_active_inventory_rebalance(active_executors=active_executors):
            allow_buy, allow_sell = self._inventory_rebalance_allowed_sides(
                inventory_delta=inventory_delta,
                allow_buy=allow_buy,
                allow_sell=allow_sell,
            )
        risk_allow_buy, risk_allow_sell = self._risk_allowed_sides(
            now=now,
            mid_price=mid_price,
            inventory_delta=inventory_delta,
            active_executors=active_executors,
        )
        allow_buy = allow_buy and risk_allow_buy
        allow_sell = allow_sell and risk_allow_sell

        active_xemm_executors = [
            executor
            for executor in active_executors
            if getattr(getattr(executor, "config", None), "type", None) != "inventory_rebalance_executor"
        ]

        active_buy_targets = {
            executor.config.target_profitability
            for executor in active_xemm_executors
            if executor.side == TradeType.BUY and hasattr(executor.config, "target_profitability")
        }
        active_sell_targets = {
            executor.config.target_profitability
            for executor in active_xemm_executors
            if executor.side == TradeType.SELL and hasattr(executor.config, "target_profitability")
        }

        active_buy_count = len([executor for executor in active_xemm_executors if executor.side == TradeType.BUY])
        active_sell_count = len([executor for executor in active_xemm_executors if executor.side == TradeType.SELL])

        actions: List[ExecutorAction] = []
        inventory_rebalance_action = self._create_inventory_rebalance_action(
            now=now,
            mid_price=mid_price,
            inventory_delta=inventory_delta,
            active_executors=active_executors,
        )
        if inventory_rebalance_action is not None:
            actions.append(inventory_rebalance_action)

        if self.config.opportunity_gate_enabled:
            buy_gate_pass, _ = self._opportunity_gate_check(TradeType.BUY)
            sell_gate_pass, _ = self._opportunity_gate_check(TradeType.SELL)
            if not buy_gate_pass:
                allow_buy = False
                self._log_opportunity_gate_decision(
                    side=TradeType.BUY,
                    passed=False,
                    reason=self._opportunity_last_gate_reason[TradeType.BUY],
                    now=now,
                    active_executor_count=active_buy_count,
                    action="stop_active_and_suppress_new_entries" if self.config.opportunity_gate_stop_on_fail else "suppress_new_entries",
                )
                if self.config.opportunity_gate_stop_on_fail:
                    actions.extend(self._opportunity_stop_actions(active_executors=active_executors, side=TradeType.BUY))
            if not sell_gate_pass:
                allow_sell = False
                self._log_opportunity_gate_decision(
                    side=TradeType.SELL,
                    passed=False,
                    reason=self._opportunity_last_gate_reason[TradeType.SELL],
                    now=now,
                    active_executor_count=active_sell_count,
                    action="stop_active_and_suppress_new_entries" if self.config.opportunity_gate_stop_on_fail else "suppress_new_entries",
                )
                if self.config.opportunity_gate_stop_on_fail:
                    actions.extend(self._opportunity_stop_actions(active_executors=active_executors, side=TradeType.SELL))
            if buy_gate_pass:
                self._opportunity_stop_sent_executor_ids = {
                    key for key in self._opportunity_stop_sent_executor_ids if not key.startswith("BUY:")
                }
            if sell_gate_pass:
                self._opportunity_stop_sent_executor_ids = {
                    key for key in self._opportunity_stop_sent_executor_ids if not key.startswith("SELL:")
                }

        if allow_buy:
            actions.extend(
                self._create_level_actions(
                    side=TradeType.BUY,
                    levels=self.config.buy_levels_targets_amount,
                    active_targets=active_buy_targets,
                    active_count=active_buy_count,
                    mid_price=mid_price,
                    active_executors=active_executors,
                )
            )
        if allow_sell:
            actions.extend(
                self._create_level_actions(
                    side=TradeType.SELL,
                    levels=self.config.sell_levels_targets_amount,
                    active_targets=active_sell_targets,
                    active_count=active_sell_count,
                    mid_price=mid_price,
                    active_executors=active_executors,
                )
            )
        if actions and any(isinstance(action, StopExecutorAction) and action.executor_id for action in actions):
            buy_executor_ids = {getattr(executor, "id", None) for executor in active_executors if executor.side == TradeType.BUY}
            sell_executor_ids = {getattr(executor, "id", None) for executor in active_executors if executor.side == TradeType.SELL}
            # Avoid mixing stop/create in the same cycle for deterministic lifecycle.
            has_buy_stop = any(
                isinstance(action, StopExecutorAction) and action.executor_id in buy_executor_ids
                for action in actions
            )
            has_sell_stop = any(
                isinstance(action, StopExecutorAction) and action.executor_id in sell_executor_ids
                for action in actions
            )
            if has_buy_stop or has_sell_stop:
                actions = [
                    action
                    for action in actions
                    if not (
                        isinstance(action, CreateExecutorAction)
                        and (
                            (has_buy_stop and action.executor_config.maker_side == TradeType.BUY)
                            or (has_sell_stop and action.executor_config.maker_side == TradeType.SELL)
                        )
                    )
                ]
        return actions

    def _has_active_inventory_rebalance(self, active_executors: List) -> bool:
        return any(
            getattr(getattr(executor, "config", None), "type", None) == "inventory_rebalance_executor"
            or getattr(executor, "type", None) == "inventory_rebalance_executor"
            for executor in active_executors
        )

    @staticmethod
    def _worsening_side_for_delta(inventory_delta: Decimal) -> Optional[TradeType]:
        if inventory_delta > Decimal("0"):
            return TradeType.BUY
        if inventory_delta < Decimal("0"):
            return TradeType.SELL
        return None

    def _inventory_rebalance_allowed_sides(
        self,
        inventory_delta: Decimal,
        allow_buy: bool,
        allow_sell: bool,
    ) -> Tuple[bool, bool]:
        worsening_side = self._worsening_side_for_delta(inventory_delta)
        if worsening_side == TradeType.BUY:
            allow_buy = False
        elif worsening_side == TradeType.SELL:
            allow_sell = False
        return allow_buy, allow_sell

    def _inventory_rebalance_creation_ready(self, now: float) -> bool:
        return now >= self._last_inventory_rebalance_creation_ts + float(self.config.inventory_rebalance_delay_between_creations)

    def _create_inventory_rebalance_action(
        self,
        now: float,
        mid_price: Decimal,
        inventory_delta: Decimal,
        active_executors: List,
    ) -> Optional[CreateExecutorAction]:
        if not self.config.inventory_rebalance_enabled:
            return None
        if not self._inventory_rebalance_creation_ready(now):
            return None
        if self._has_active_inventory_rebalance(active_executors=active_executors):
            return None

        candidate = self._select_inventory_rebalance_candidate(
            inventory_delta=inventory_delta,
            mid_price=mid_price,
        )
        if candidate is None:
            return None

        self._last_inventory_rebalance_creation_ts = now
        return CreateExecutorAction(controller_id=self._controller_id(), executor_config=candidate)

    def _select_inventory_rebalance_candidate(
        self,
        inventory_delta: Decimal,
        mid_price: Decimal,
    ) -> Optional[InventoryRebalanceExecutorConfig]:
        abs_delta = abs(inventory_delta)
        soft_band = self.config.inventory_rebalance_soft_band_base
        hard_band = self.config.inventory_rebalance_hard_band_base
        if abs_delta <= soft_band:
            return None

        slice_amount = max(Decimal("0"), (abs_delta - soft_band) / Decimal("2"))
        slice_amount = max(slice_amount, self.config.inventory_rebalance_min_slice_base)
        if self.config.inventory_rebalance_max_slice_base > Decimal("0"):
            slice_amount = min(slice_amount, self.config.inventory_rebalance_max_slice_base)
        if slice_amount <= Decimal("0"):
            return None

        controller_id = self._controller_id()
        maker_market = ConnectorPair(connector_name=self.config.maker_connector, trading_pair=self.config.maker_trading_pair)
        taker_market = ConnectorPair(connector_name=self.config.taker_connector, trading_pair=self.config.taker_trading_pair)
        candidates: List[InventoryRebalanceExecutorConfig] = []

        if inventory_delta > Decimal("0"):
            candidates.extend(self._rebalance_candidate_variants(
                entry_market=maker_market,
                hedge_market=taker_market,
                entry_side=TradeType.SELL,
                order_amount=slice_amount,
                inventory_delta=inventory_delta,
                controller_id=controller_id,
                allow_aggressive=abs_delta > hard_band,
            ))
            candidates.extend(self._rebalance_candidate_variants(
                entry_market=taker_market,
                hedge_market=maker_market,
                entry_side=TradeType.BUY,
                order_amount=slice_amount,
                inventory_delta=inventory_delta,
                controller_id=controller_id,
                allow_aggressive=abs_delta > hard_band,
            ))
        elif inventory_delta < Decimal("0"):
            candidates.extend(self._rebalance_candidate_variants(
                entry_market=maker_market,
                hedge_market=taker_market,
                entry_side=TradeType.BUY,
                order_amount=slice_amount,
                inventory_delta=inventory_delta,
                controller_id=controller_id,
                allow_aggressive=abs_delta > hard_band,
            ))
            candidates.extend(self._rebalance_candidate_variants(
                entry_market=taker_market,
                hedge_market=maker_market,
                entry_side=TradeType.SELL,
                order_amount=slice_amount,
                inventory_delta=inventory_delta,
                controller_id=controller_id,
                allow_aggressive=abs_delta > hard_band,
            ))
        else:
            return None

        eligible = [
            candidate for candidate in candidates
            if candidate.expected_pnl_quote > self.config.inventory_rebalance_min_expected_pnl_quote
        ]
        if not eligible:
            return None
        return max(eligible, key=lambda candidate: candidate.expected_pnl_quote)

    def _rebalance_candidate_variants(
        self,
        entry_market: ConnectorPair,
        hedge_market: ConnectorPair,
        entry_side: TradeType,
        order_amount: Decimal,
        inventory_delta: Decimal,
        controller_id: str,
        allow_aggressive: bool,
    ) -> List[InventoryRebalanceExecutorConfig]:
        variants = [
            self._build_inventory_rebalance_candidate(
                entry_market=entry_market,
                hedge_market=hedge_market,
                entry_side=entry_side,
                entry_style="passive",
                order_amount=order_amount,
                inventory_delta=inventory_delta,
                controller_id=controller_id,
            )
        ]
        if allow_aggressive:
            variants.append(
                self._build_inventory_rebalance_candidate(
                    entry_market=entry_market,
                    hedge_market=hedge_market,
                    entry_side=entry_side,
                    entry_style="aggressive",
                    order_amount=order_amount,
                    inventory_delta=inventory_delta,
                    controller_id=controller_id,
                )
            )
        return [variant for variant in variants if variant is not None]

    def _build_inventory_rebalance_candidate(
        self,
        entry_market: ConnectorPair,
        hedge_market: ConnectorPair,
        entry_side: TradeType,
        entry_style: str,
        order_amount: Decimal,
        inventory_delta: Decimal,
        controller_id: str,
    ) -> Optional[InventoryRebalanceExecutorConfig]:
        quantized_amount = self.market_data_provider.quantize_order_amount(
            entry_market.connector_name,
            entry_market.trading_pair,
            order_amount,
        )
        quantized_amount = self.market_data_provider.quantize_order_amount(
            hedge_market.connector_name,
            hedge_market.trading_pair,
            quantized_amount,
        )
        if quantized_amount <= Decimal("0"):
            return None

        hedge_side = TradeType.SELL if entry_side == TradeType.BUY else TradeType.BUY
        entry_price = self._rebalance_entry_price(entry_market, entry_side, entry_style)
        hedge_price = self._rebalance_hedge_price(hedge_market, hedge_side)
        if entry_price is None or hedge_price is None or entry_price <= Decimal("0") or hedge_price <= Decimal("0"):
            return None

        expected_pnl_quote = self._expected_inventory_rebalance_pnl_quote(
            entry_market=entry_market,
            hedge_market=hedge_market,
            entry_side=entry_side,
            entry_style=entry_style,
            order_amount=quantized_amount,
            entry_price=entry_price,
            hedge_price=hedge_price,
        )
        if expected_pnl_quote <= Decimal("0"):
            return None

        delta_change = quantized_amount * Decimal("2")
        target_delta_after = (
            inventory_delta - delta_change if inventory_delta > Decimal("0") else inventory_delta + delta_change
        )

        return InventoryRebalanceExecutorConfig(
            timestamp=self.market_data_provider.time(),
            controller_id=controller_id,
            entry_market=entry_market,
            hedge_market=hedge_market,
            entry_side=entry_side,
            entry_style=entry_style,
            order_amount=quantized_amount,
            inventory_delta_before=inventory_delta,
            target_inventory_delta_after=target_delta_after,
            expected_pnl_quote=expected_pnl_quote,
            entry_price_refresh_pct=self.config.inventory_rebalance_passive_price_refresh_pct,
            entry_order_max_age_seconds=self.config.inventory_rebalance_passive_order_max_age_seconds,
            hedge_order_type=self.config.taker_order_type,
            hedge_slippage_buffer_bps=self.config.inventory_rebalance_hedge_slippage_buffer_bps,
            hedge_max_retries=self.config.taker_max_retries,
            hedge_fallback_to_market=True,
        )

    def _rebalance_entry_price(
        self,
        market: ConnectorPair,
        side: TradeType,
        entry_style: str,
    ) -> Optional[Decimal]:
        if entry_style == "passive":
            price_type = PriceType.BestBid if side == TradeType.BUY else PriceType.BestAsk
            return self.market_data_provider.get_price_by_type(market.connector_name, market.trading_pair, price_type)

        book_price_type = PriceType.BestAsk if side == TradeType.BUY else PriceType.BestBid
        reference_price = self.market_data_provider.get_price_by_type(market.connector_name, market.trading_pair, book_price_type)
        if reference_price is None or reference_price <= Decimal("0"):
            return None
        slippage = self.config.inventory_rebalance_entry_slippage_buffer_bps / Decimal("10000")
        if side == TradeType.BUY:
            return reference_price * (Decimal("1") + slippage)
        return reference_price * (Decimal("1") - slippage)

    def _rebalance_hedge_price(self, market: ConnectorPair, side: TradeType) -> Optional[Decimal]:
        price_type = PriceType.BestAsk if side == TradeType.BUY else PriceType.BestBid
        reference_price = self.market_data_provider.get_price_by_type(market.connector_name, market.trading_pair, price_type)
        if reference_price is None or reference_price <= Decimal("0"):
            return None
        slippage = self.config.inventory_rebalance_hedge_slippage_buffer_bps / Decimal("10000")
        if side == TradeType.BUY:
            return reference_price * (Decimal("1") + slippage)
        return reference_price * (Decimal("1") - slippage)

    def _expected_inventory_rebalance_pnl_quote(
        self,
        entry_market: ConnectorPair,
        hedge_market: ConnectorPair,
        entry_side: TradeType,
        entry_style: str,
        order_amount: Decimal,
        entry_price: Decimal,
        hedge_price: Decimal,
    ) -> Decimal:
        if entry_side == TradeType.SELL:
            sell_notional = entry_price * order_amount
            buy_notional = hedge_price * order_amount
        else:
            sell_notional = hedge_price * order_amount
            buy_notional = entry_price * order_amount

        gross_quote = sell_notional - buy_notional
        entry_is_maker = entry_style == "passive"
        hedge_is_maker = False
        entry_fee = self._estimate_fee_quote(
            connector_name=entry_market.connector_name,
            trading_pair=entry_market.trading_pair,
            side=entry_side,
            amount=order_amount,
            price=entry_price,
            is_maker=entry_is_maker,
        )
        hedge_side = TradeType.SELL if entry_side == TradeType.BUY else TradeType.BUY
        hedge_fee = self._estimate_fee_quote(
            connector_name=hedge_market.connector_name,
            trading_pair=hedge_market.trading_pair,
            side=hedge_side,
            amount=order_amount,
            price=hedge_price,
            is_maker=hedge_is_maker,
        )
        rebate_quote = Decimal("0")
        if entry_is_maker and entry_market.connector_name == self.config.maker_connector:
            rebate_quote += sell_notional * (self.config.inventory_rebalance_bithumb_rebate_bps / Decimal("10000"))
        if hedge_is_maker and hedge_market.connector_name == self.config.maker_connector:
            hedge_notional = hedge_price * order_amount
            rebate_quote += hedge_notional * (self.config.inventory_rebalance_bithumb_rebate_bps / Decimal("10000"))
        return gross_quote - entry_fee - hedge_fee + rebate_quote

    def _estimate_fee_quote(
        self,
        connector_name: str,
        trading_pair: str,
        side: TradeType,
        amount: Decimal,
        price: Decimal,
        is_maker: bool,
    ) -> Decimal:
        connector = self.market_data_provider.connectors.get(connector_name)
        if connector is None or not hasattr(connector, "get_fee"):
            return Decimal("0")
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        try:
            fee = connector.get_fee(
                base_currency=base_asset,
                quote_currency=quote_asset,
                order_type=OrderType.LIMIT if is_maker else OrderType.MARKET,
                order_side=side,
                amount=amount,
                price=price,
                is_maker=is_maker,
            )
            fee_in_quote = fee.fee_amount_in_token(
                trading_pair=trading_pair,
                price=price,
                order_amount=amount,
                token=quote_asset,
                exchange=connector,
            )
            return self._as_decimal(fee_in_quote)
        except Exception:
            return Decimal("0")

    def _create_level_actions(
        self,
        side: TradeType,
        levels: List[List[Decimal]],
        active_targets: Set[Decimal],
        active_count: int,
        mid_price: Decimal,
        active_executors: List,
    ) -> List[ExecutorAction]:
        now = self.market_data_provider.time()
        if now < self._last_creation_timestamp[side] + self.config.delay_between_creations:
            return []

        remaining_slots = max(0, self.config.max_executors_per_side - active_count)
        if remaining_slots <= 0:
            return []

        total_weight = sum(weight for _, weight in levels)
        if total_weight <= Decimal("0"):
            return []

        side_budget_quote = self.config.total_amount_quote / Decimal("2")
        actions: List[ExecutorAction] = []
        controller_id = self._controller_id()
        shadow_notional_used = self._current_shadow_notional_quote(active_executors=active_executors, mid_price=mid_price)
        shadow_supported_path = self._shadow_supported_path()

        for target_profitability, weight in levels:
            if remaining_slots <= 0:
                break
            if target_profitability in active_targets:
                continue

            quote_amount = side_budget_quote * (weight / total_weight)
            base_amount = self.market_data_provider.quantize_order_amount(
                self.config.maker_connector,
                self.config.maker_trading_pair,
                quote_amount / mid_price,
            )
            if base_amount <= Decimal("0"):
                continue

            min_profitability = max(Decimal("0"), target_profitability - self.config.min_profitability_delta)
            max_profitability = target_profitability + self.config.max_profitability_delta

            buying_market, selling_market = self._market_pairs_for_side(side)
            shadow_enabled_for_executor = self.config.upbit_shadow_maker_enabled and shadow_supported_path
            if shadow_enabled_for_executor and self.config.upbit_shadow_global_notional_cap_quote > Decimal("0"):
                projected = shadow_notional_used + quote_amount
                if projected > self.config.upbit_shadow_global_notional_cap_quote:
                    shadow_enabled_for_executor = False
                else:
                    shadow_notional_used = projected

            action = CreateExecutorAction(
                executor_config=XEMMExecutorConfig(
                    timestamp=now,
                    latency_diagnostics_enabled=self.config.latency_diagnostics_enabled,
                    buying_market=buying_market,
                    selling_market=selling_market,
                    maker_side=side,
                    order_amount=base_amount,
                    min_profitability=min_profitability,
                    target_profitability=target_profitability,
                    max_profitability=max_profitability,
                    maker_price_source=self.config.maker_price_source,
                    maker_price_refresh_pct=self.config.maker_price_refresh_pct,
                    maker_order_max_age_seconds=self.config.maker_order_max_age_seconds,
                    taker_order_type=self.config.taker_order_type,
                    taker_slippage_buffer_bps=self.config.taker_slippage_buffer_bps,
                    taker_order_max_age_seconds=self.config.taker_order_max_age_seconds,
                    taker_max_retries=self.config.taker_max_retries,
                    taker_fallback_to_market=self.config.taker_fallback_to_market,
                    min_profitability_guard=self.config.min_profitability_guard,
                    allow_loss_hedge=self.config.allow_loss_hedge,
                    hedge_aggregation_window_sec=self.config.hedge_aggregation_window_sec,
                    max_unhedged_notional_quote=self.config.max_unhedged_notional_quote,
                    rate_limit_backoff_factor=self.config.rate_limit_backoff_factor,
                    close_out_loss_cap_bps=self.config.close_out_loss_cap_bps,
                    stale_fill_hedge_mode=self.config.stale_fill_hedge_mode,
                    allow_one_sided_inventory_mode=self.config.allow_one_sided_inventory_mode,
                    shadow_maker_enabled=shadow_enabled_for_executor,
                    shadow_maker_min_profitability=target_profitability,
                    shadow_maker_price_refresh_pct=self.config.upbit_shadow_price_refresh_pct,
                    shadow_maker_order_max_age_seconds=self.config.upbit_shadow_order_max_age_seconds,
                    shadow_maker_extra_buffer_bps=self.config.upbit_shadow_extra_buffer_bps,
                    shadow_prefill_cross_timeout_sec=self.config.upbit_shadow_prefill_cross_timeout_sec,
                    shadow_prefill_unwind_timeout_sec=self.config.upbit_shadow_prefill_unwind_timeout_sec,
                    shadow_prefill_max_retries=self.config.upbit_shadow_prefill_max_retries,
                    bithumb_stp_prevention_enabled=self.config.bithumb_stp_prevention_enabled,
                    bithumb_stp_base_offset_ticks=self.config.bithumb_stp_base_offset_ticks,
                    bithumb_stp_max_offset_ticks=self.config.bithumb_stp_max_offset_ticks,
                    bithumb_stp_retry_cooldown_sec=self.config.bithumb_stp_retry_cooldown_sec,
                    bithumb_stp_pause_after_rejects=self.config.bithumb_stp_pause_after_rejects,
                    bithumb_stp_pause_duration_sec=self.config.bithumb_stp_pause_duration_sec,
                    bithumb_stp_consider_pending_cancel_as_conflict=self.config.bithumb_stp_consider_pending_cancel_as_conflict,
                    controller_id=controller_id,
                ),
                controller_id=controller_id,
            )
            actions.append(action)
            remaining_slots -= 1

        if actions:
            self._last_creation_timestamp[side] = now

        return actions

    def _market_pairs_for_side(self, side: TradeType) -> Tuple[ConnectorPair, ConnectorPair]:
        maker = ConnectorPair(connector_name=self.config.maker_connector, trading_pair=self.config.maker_trading_pair)
        taker = ConnectorPair(connector_name=self.config.taker_connector, trading_pair=self.config.taker_trading_pair)
        if side == TradeType.BUY:
            return maker, taker
        return taker, maker

    def _should_log_opportunity_gate(self, side: TradeType, now: float) -> bool:
        interval = float(self.config.opportunity_debug_log_interval_sec)
        last_ts = self._opportunity_last_debug_log_ts_by_side.get(side, 0.0)
        if now - last_ts < interval:
            return False
        self._opportunity_last_debug_log_ts_by_side[side] = now
        self._opportunity_last_debug_log_ts = now
        return True

    def _log_opportunity_gate_decision(
        self,
        side: TradeType,
        passed: bool,
        reason: str,
        now: float,
        active_executor_count: Optional[int] = None,
        action: Optional[str] = None,
    ) -> None:
        if not self._should_log_opportunity_gate(side=side, now=now):
            return
        self.logger().info(
            f"Opportunity gate {'PASS' if passed else 'FAIL'} ({side.name}): "
            f"reason={reason}, required_ticks={self._opportunity_required_ticks}, "
            f"edge_ticks={self._opportunity_edge_ticks[side]}, est_net_bps={self._opportunity_edge_net_bps[side]}, "
            f"active_executor_count={active_executor_count if active_executor_count is not None else 0}, "
            f"action={action or 'none'}"
        )

    def _controller_id(self) -> str:
        return self.config.id or self.config.controller_name or "main"

    def _shadow_supported_path(self) -> bool:
        return self.config.maker_connector == "bithumb" and self.config.taker_connector == "upbit"

    def _current_shadow_notional_quote(self, active_executors: List, mid_price: Decimal) -> Decimal:
        total = Decimal("0")
        for executor in active_executors:
            config = getattr(executor, "config", None)
            if config is None:
                continue
            if not getattr(config, "shadow_maker_enabled", False):
                continue
            order_amount = getattr(config, "order_amount", Decimal("0"))
            if order_amount is None:
                continue
            total += Decimal(order_amount) * mid_price
        return total

    def _refresh_market_data_health(self, now: float) -> bool:
        maker_freshness = self.market_data_provider.get_order_book_freshness_sec(
            self.config.maker_connector,
            self.config.maker_trading_pair,
        )
        taker_freshness = self.market_data_provider.get_order_book_freshness_sec(
            self.config.taker_connector,
            self.config.taker_trading_pair,
        )
        self._last_maker_freshness_sec = maker_freshness
        self._last_taker_freshness_sec = taker_freshness

        timeout = self.config.market_data_stale_timeout_sec
        stale = any(freshness is None or freshness > timeout for freshness in [maker_freshness, taker_freshness])

        if stale:
            if not self._market_data_stale:
                self.logger().warning(
                    f"Market data stale detected. maker_freshness={maker_freshness}, "
                    f"taker_freshness={taker_freshness}, timeout={timeout}s"
                )
                self._market_data_stale_since_ts = now
            self._market_data_stale = True
            self._market_data_recovery_grace_until_ts = None
            return True

        if self._market_data_stale:
            self.logger().info(
                f"Market data recovered. maker_freshness={maker_freshness}, "
                f"taker_freshness={taker_freshness}"
            )
            self._market_data_stale = False
            self._market_data_stale_since_ts = None
            self._stale_stop_sent_executor_ids.clear()
            if self.config.market_data_recovery_grace_sec > 0:
                self._market_data_recovery_grace_until_ts = now + self.config.market_data_recovery_grace_sec

        return False

    def _stale_stop_actions(self, active_executors: List) -> List[ExecutorAction]:
        if not self.config.cancel_open_orders_on_stale:
            return []
        actions: List[ExecutorAction] = []
        controller_id = self._controller_id()
        for executor in active_executors:
            if executor.id in self._stale_stop_sent_executor_ids:
                continue
            actions.append(
                StopExecutorAction(
                    executor_id=executor.id,
                    controller_id=controller_id,
                    keep_position=False,
                )
            )
            self._stale_stop_sent_executor_ids.add(executor.id)
        return actions

    def _inventory_delta(self) -> Decimal:
        base_asset, _ = split_hb_trading_pair(self.config.maker_trading_pair)
        maker_base = self.market_data_provider.connectors[self.config.maker_connector].get_available_balance(base_asset)
        taker_base = self.market_data_provider.connectors[self.config.taker_connector].get_available_balance(base_asset)
        maker_base = maker_base if maker_base is not None else Decimal("0")
        taker_base = taker_base if taker_base is not None else Decimal("0")
        return maker_base - taker_base - self.config.inventory_target_base

    def _available_base_balance(self, connector_name: str) -> Decimal:
        base_asset, _ = split_hb_trading_pair(self.config.maker_trading_pair)
        balance = self.market_data_provider.connectors[connector_name].get_available_balance(base_asset)
        return balance if balance is not None else Decimal("0")

    def _allowed_sides(self, inventory_delta: Decimal) -> Tuple[bool, bool]:
        if not self.config.inventory_skew_side_gating_enabled:
            return True, True
        max_skew = self.config.max_inventory_skew_base
        if inventory_delta > max_skew:
            return False, True
        if inventory_delta < -max_skew:
            return True, False
        return True, True

    @staticmethod
    def _as_decimal(value) -> Decimal:
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal("0")

    def _update_opportunity_buffers(self):
        if not self.config.opportunity_gate_enabled or not self.market_data_provider.ready:
            return
        now = self.market_data_provider.time()
        maker_mid = self.market_data_provider.get_price_by_type(
            self.config.maker_connector, self.config.maker_trading_pair, PriceType.MidPrice
        )
        taker_mid = self.market_data_provider.get_price_by_type(
            self.config.taker_connector, self.config.taker_trading_pair, PriceType.MidPrice
        )
        if maker_mid is not None and maker_mid > Decimal("0"):
            self._rv_maker.add(now, maker_mid)
        if taker_mid is not None and taker_mid > Decimal("0"):
            self._rv_taker.add(now, taker_mid)

    def _get_required_ticks(self) -> int:
        min_ticks = int(self.config.opportunity_min_ticks)
        max_ticks = int(self.config.opportunity_max_ticks)
        if self.config.opportunity_tick_mode == "fixed":
            self._opportunity_required_ticks = min_ticks
            return self._opportunity_required_ticks

        maker_rv = self._rv_maker.rv_bps(self.config.opportunity_vol_window_sec)
        taker_rv = self._rv_taker.rv_bps(self.config.opportunity_vol_window_sec)
        self._opportunity_rv_bps_maker = maker_rv
        self._opportunity_rv_bps_taker = taker_rv

        if maker_rv is None or taker_rv is None:
            self._opportunity_required_ticks = max_ticks
            return self._opportunity_required_ticks

        rv_bps = max(maker_rv, taker_rv)
        if rv_bps >= self.config.opportunity_vol_high_bps:
            self._opportunity_required_ticks = max_ticks
        elif rv_bps <= self.config.opportunity_vol_low_bps:
            self._opportunity_required_ticks = min_ticks
        else:
            self._opportunity_required_ticks = min(max(self._opportunity_required_ticks, min_ticks), max_ticks)
        return self._opportunity_required_ticks

    def _opportunity_gate_check(self, side: TradeType) -> Tuple[bool, str]:
        if not self.config.opportunity_gate_enabled:
            self._opportunity_last_gate_reason[side] = "disabled"
            return True, "disabled"

        required_ticks = self._get_required_ticks()
        maker_best_ask = self.market_data_provider.get_price_by_type(
            self.config.maker_connector, self.config.maker_trading_pair, PriceType.BestAsk
        )
        maker_best_bid = self.market_data_provider.get_price_by_type(
            self.config.maker_connector, self.config.maker_trading_pair, PriceType.BestBid
        )
        taker_best_ask = self.market_data_provider.get_price_by_type(
            self.config.taker_connector, self.config.taker_trading_pair, PriceType.BestAsk
        )
        taker_best_bid = self.market_data_provider.get_price_by_type(
            self.config.taker_connector, self.config.taker_trading_pair, PriceType.BestBid
        )
        prices = [maker_best_ask, maker_best_bid, taker_best_ask, taker_best_bid]
        if any(p is None or p <= Decimal("0") for p in prices):
            self._opportunity_edge_ticks[side] = None
            self._opportunity_edge_net_bps[side] = None
            self._opportunity_last_gate_reason[side] = "missing_prices"
            return False, "missing_prices"

        maker_ref_price = maker_best_ask if side == TradeType.SELL else maker_best_bid
        maker_connector = self.market_data_provider.connectors.get(self.config.maker_connector)
        if maker_connector is None:
            self._opportunity_last_gate_reason[side] = "missing_maker_connector"
            return False, "missing_maker_connector"
        maker_tick = self._as_decimal(maker_connector.get_order_price_quantum(self.config.maker_trading_pair, maker_ref_price))
        if maker_tick <= Decimal("0"):
            self._opportunity_last_gate_reason[side] = "invalid_tick_size"
            return False, "invalid_tick_size"

        if side == TradeType.SELL:
            edge_price = maker_best_ask - taker_best_ask
            reference_price = maker_best_ask
            fee_buffer = self.config.opportunity_fee_buffer_bps_sell
            level_target = self.config.sell_levels_targets_amount[0][0]
        else:
            edge_price = taker_best_bid - maker_best_bid
            reference_price = maker_best_bid
            fee_buffer = self.config.opportunity_fee_buffer_bps_buy
            level_target = self.config.buy_levels_targets_amount[0][0]

        if edge_price <= Decimal("0"):
            self._opportunity_edge_ticks[side] = 0
            self._opportunity_edge_net_bps[side] = Decimal("0")
            self._opportunity_last_gate_reason[side] = "negative_edge"
            return False, "negative_edge"

        edge_ticks = int(edge_price / maker_tick)
        gross_pct = edge_price / reference_price
        est_net_pct = gross_pct - (fee_buffer / Decimal("10000"))
        self._opportunity_edge_ticks[side] = edge_ticks
        self._opportunity_edge_net_bps[side] = est_net_pct * Decimal("10000")

        if edge_ticks < required_ticks:
            self._opportunity_last_gate_reason[side] = f"edge_ticks<{required_ticks}"
            return False, "insufficient_ticks"

        min_profitability = max(Decimal("0"), level_target - self.config.min_profitability_delta)
        if est_net_pct < min_profitability:
            self._opportunity_last_gate_reason[side] = "net_below_min_profitability"
            return False, "net_below_min_profitability"

        now = self.market_data_provider.time()
        self._log_opportunity_gate_decision(
            side=side,
            passed=True,
            reason="pass",
            now=now,
            action="allow_new_entries",
        )
        self._opportunity_last_gate_reason[side] = "pass"
        return True, "pass"

    def _opportunity_stop_actions(self, active_executors: List, side: TradeType) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        controller_id = self._controller_id()
        for executor in active_executors:
            if executor.side != side:
                continue
            executor_id = getattr(executor, "id", None)
            if executor_id is None:
                continue
            dedupe_key = f"{side.name}:{executor_id}"
            if dedupe_key in self._opportunity_stop_sent_executor_ids:
                continue
            actions.append(
                StopExecutorAction(
                    executor_id=executor_id,
                    controller_id=controller_id,
                    keep_position=False,
                )
            )
            self._opportunity_stop_sent_executor_ids.add(dedupe_key)
        return actions

    def _calculate_effective_session_pnl_quote(self, mid_price: Decimal, inventory_delta: Decimal) -> Decimal:
        realized = Decimal("0")
        for executor in self.executors_info:
            if not getattr(executor, "is_done", False):
                continue
            realized += self._as_decimal(getattr(executor, "net_pnl_quote", Decimal("0")))
        inventory_skew_quote = abs(inventory_delta) * mid_price
        forced_flatten_cost = inventory_skew_quote * (self.config.risk_forced_flatten_cost_bps / Decimal("10000"))
        effective = realized - forced_flatten_cost
        self._risk_effective_session_pnl_quote = effective
        return effective

    def _aggregate_unhedged_notional_quote(self, active_executors: List) -> Decimal:
        signed_total = Decimal("0")
        for executor in active_executors:
            custom_info = getattr(executor, "custom_info", {}) or {}
            signed_total += self._as_decimal(custom_info.get("unhedged_quote", Decimal("0")))
        self._risk_unhedged_notional_quote = abs(signed_total)
        return self._risk_unhedged_notional_quote

    def _risk_allowed_sides(
        self, now: float, mid_price: Decimal, inventory_delta: Decimal, active_executors: List
    ) -> Tuple[bool, bool]:
        effective_pnl = self._calculate_effective_session_pnl_quote(mid_price=mid_price, inventory_delta=inventory_delta)
        unhedged_notional = self._aggregate_unhedged_notional_quote(active_executors=active_executors)

        is_loss_cut_hit = (
            self.config.risk_session_loss_cut_quote > Decimal("0")
            and effective_pnl <= -self.config.risk_session_loss_cut_quote
        )
        is_unhedged_cut_hit = (
            self.config.risk_unhedged_notional_cut_quote > Decimal("0")
            and unhedged_notional >= self.config.risk_unhedged_notional_cut_quote
        )

        if is_loss_cut_hit:
            self._risk_pause_until_ts = now + self.config.risk_pause_cooldown_sec
            self._risk_pause_reason = f"session_loss_cut({effective_pnl})"
        elif is_unhedged_cut_hit:
            self._risk_pause_until_ts = now + self.config.risk_pause_cooldown_sec
            self._risk_pause_reason = f"unhedged_notional_cut({unhedged_notional})"

        if now < self._risk_pause_until_ts:
            return False, False

        self._risk_pause_reason = ""
        return True, True

    def to_format_status(self) -> List[str]:
        inventory_delta = self._inventory_delta()
        allow_buy, allow_sell = self._allowed_sides(inventory_delta)
        recovery_grace_remaining = Decimal("0")
        if self._market_data_recovery_grace_until_ts is not None:
            recovery_grace_remaining = Decimal(
                max(0.0, self._market_data_recovery_grace_until_ts - self.market_data_provider.time())
            )
        return [
            f"  Pair: maker={self.config.maker_connector} {self.config.maker_trading_pair} | taker={self.config.taker_connector} {self.config.taker_trading_pair}",
            f"  Taker order type: {self.config.taker_order_type.name}, slippage_buffer_bps={self.config.taker_slippage_buffer_bps}",
            f"  Upbit shadow maker: enabled={self.config.upbit_shadow_maker_enabled}, supported_path={self._shadow_supported_path()}, cap_quote={self.config.upbit_shadow_global_notional_cap_quote}",
            f"  Maker refresh: price_pct={self.config.maker_price_refresh_pct}, max_age={self.config.maker_order_max_age_seconds}s",
            f"  Taker fallback to market: {self.config.taker_fallback_to_market}",
            f"  Market data stale: {self._market_data_stale} | maker_freshness={self._last_maker_freshness_sec} | taker_freshness={self._last_taker_freshness_sec}",
            f"  Stale controls: timeout={self.config.market_data_stale_timeout_sec}s, grace={self.config.market_data_recovery_grace_sec}s, grace_remaining={recovery_grace_remaining}s, cancel_on_stale={self.config.cancel_open_orders_on_stale}, stale_fill_hedge_mode={self.config.stale_fill_hedge_mode}",
            f"  Loop metrics: {self.format_loop_metrics()}",
            f"  Inventory delta (maker-taker-target): {inventory_delta}",
            f"  Side gating -> inventory BUY:{allow_buy} SELL:{allow_sell}",
            f"  Opportunity gate: enabled={self.config.opportunity_gate_enabled}, required_ticks={self._opportunity_required_ticks}, "
            f"edge_ticks_buy={self._opportunity_edge_ticks[TradeType.BUY]}, edge_ticks_sell={self._opportunity_edge_ticks[TradeType.SELL]}, "
            f"net_bps_buy={self._opportunity_edge_net_bps[TradeType.BUY]}, net_bps_sell={self._opportunity_edge_net_bps[TradeType.SELL]}",
            f"  Risk cutoff: effective_session_pnl={self._risk_effective_session_pnl_quote}, "
            f"unhedged_notional={self._risk_unhedged_notional_quote}, pause_remaining={max(0.0, self._risk_pause_until_ts - self.market_data_provider.time()):.2f}s, reason={self._risk_pause_reason}",
        ]

    def get_custom_info(self) -> dict:
        inventory_delta = self._inventory_delta()
        allow_buy, allow_sell = self._allowed_sides(inventory_delta)
        active_executors = self.filter_executors(self.executors_info, lambda executor: not executor.is_done)
        maker_available_base = self._available_base_balance(self.config.maker_connector)
        taker_available_base = self._available_base_balance(self.config.taker_connector)
        active_buy = len([executor for executor in active_executors if executor.side == TradeType.BUY])
        active_sell = len([executor for executor in active_executors if executor.side == TradeType.SELL])
        recovery_grace_remaining = 0.0
        if self._market_data_recovery_grace_until_ts is not None:
            recovery_grace_remaining = max(0.0, self._market_data_recovery_grace_until_ts - self.market_data_provider.time())
        return {
            "loop_metrics": self.get_loop_metrics(),
            "inventory_delta": str(inventory_delta),
            "maker_available_base": str(maker_available_base),
            "taker_available_base": str(taker_available_base),
            "allow_buy": allow_buy,
            "allow_sell": allow_sell,
            "inventory_skew_side_gating_enabled": self.config.inventory_skew_side_gating_enabled,
            "active_executors_total": len(active_executors),
            "active_buy_executors": active_buy,
            "active_sell_executors": active_sell,
            "market_data_stale": self._market_data_stale,
            "market_data_stale_since_ts": self._market_data_stale_since_ts,
            "market_data_recovery_grace_remaining_sec": recovery_grace_remaining,
            "maker_order_book_freshness_sec": self._last_maker_freshness_sec,
            "taker_order_book_freshness_sec": self._last_taker_freshness_sec,
            "market_data_stale_timeout_sec": self.config.market_data_stale_timeout_sec,
            "cancel_open_orders_on_stale": self.config.cancel_open_orders_on_stale,
            "stale_fill_hedge_mode": self.config.stale_fill_hedge_mode,
            "latency_diagnostics_enabled": self.config.latency_diagnostics_enabled,
            "close_out_loss_cap_bps": str(self.config.close_out_loss_cap_bps),
            "allow_one_sided_inventory_mode": self.config.allow_one_sided_inventory_mode,
            "maker_price_source": self.config.maker_price_source,
            "opportunity_gate_enabled": self.config.opportunity_gate_enabled,
            "opportunity_required_ticks": self._opportunity_required_ticks,
            "opportunity_rv_bps_maker": str(self._opportunity_rv_bps_maker) if self._opportunity_rv_bps_maker is not None else None,
            "opportunity_rv_bps_taker": str(self._opportunity_rv_bps_taker) if self._opportunity_rv_bps_taker is not None else None,
            "opportunity_edge_ticks_buy": self._opportunity_edge_ticks[TradeType.BUY],
            "opportunity_edge_ticks_sell": self._opportunity_edge_ticks[TradeType.SELL],
            "opportunity_edge_net_bps_buy": str(self._opportunity_edge_net_bps[TradeType.BUY]) if self._opportunity_edge_net_bps[TradeType.BUY] is not None else None,
            "opportunity_edge_net_bps_sell": str(self._opportunity_edge_net_bps[TradeType.SELL]) if self._opportunity_edge_net_bps[TradeType.SELL] is not None else None,
            "opportunity_gate_last_reason_buy": self._opportunity_last_gate_reason[TradeType.BUY],
            "opportunity_gate_last_reason_sell": self._opportunity_last_gate_reason[TradeType.SELL],
            "risk_effective_session_pnl_quote": str(self._risk_effective_session_pnl_quote),
            "risk_unhedged_notional_quote": str(self._risk_unhedged_notional_quote),
            "risk_pause_active": self.market_data_provider.time() < self._risk_pause_until_ts,
            "risk_pause_reason": self._risk_pause_reason,
            "risk_pause_remaining_sec": max(0.0, self._risk_pause_until_ts - self.market_data_provider.time()),
            "upbit_shadow_maker_enabled": self.config.upbit_shadow_maker_enabled,
            "upbit_shadow_supported_path": self._shadow_supported_path(),
            "upbit_shadow_global_notional_cap_quote": str(self.config.upbit_shadow_global_notional_cap_quote),
            "bithumb_stp_prevention_enabled": self.config.bithumb_stp_prevention_enabled,
            "bithumb_stp_base_offset_ticks": self.config.bithumb_stp_base_offset_ticks,
            "bithumb_stp_max_offset_ticks": self.config.bithumb_stp_max_offset_ticks,
            "bithumb_stp_retry_cooldown_sec": self.config.bithumb_stp_retry_cooldown_sec,
            "bithumb_stp_pause_after_rejects": self.config.bithumb_stp_pause_after_rejects,
            "bithumb_stp_pause_duration_sec": self.config.bithumb_stp_pause_duration_sec,
        }
