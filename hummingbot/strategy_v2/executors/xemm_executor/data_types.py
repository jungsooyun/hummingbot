from decimal import Decimal
from typing import Literal

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair, ExecutorConfigBase


class XEMMExecutorConfig(ExecutorConfigBase):
    type: Literal["xemm_executor"] = "xemm_executor"
    buying_market: ConnectorPair
    selling_market: ConnectorPair
    maker_side: TradeType
    order_amount: Decimal
    min_profitability: Decimal
    target_profitability: Decimal
    max_profitability: Decimal
    maker_price_source: Literal["formula", "best"] = "formula"
    # Maker maintenance
    maker_price_refresh_pct: Decimal = Decimal("0.0002")
    maker_order_max_age_seconds: float = 60.0
    # Taker settings
    taker_order_type: OrderType = OrderType.MARKET
    taker_slippage_buffer_bps: Decimal = Decimal("0")
    taker_order_max_age_seconds: float = 4.0
    taker_max_retries: int = 3
    taker_fallback_to_market: bool = False
    # Safety / hedging guards
    min_profitability_guard: Decimal = Decimal("0")
    allow_loss_hedge: bool = False
    hedge_aggregation_window_sec: float = 1.0
    max_unhedged_notional_quote: Decimal = Decimal("0")
    rate_limit_backoff_factor: float = 1.0
    stale_fill_hedge_mode: Literal["pause", "market"] = "pause"
    allow_one_sided_inventory_mode: bool = False
    # Optional shadow maker on taker venue (for Upbit-Bithumb specialization)
    shadow_maker_enabled: bool = False
    shadow_maker_min_profitability: Decimal = Decimal("0")
    shadow_maker_price_refresh_pct: Decimal = Decimal("0.0002")
    shadow_maker_order_max_age_seconds: float = 20.0
    shadow_maker_extra_buffer_bps: Decimal = Decimal("0")
    shadow_prefill_cross_timeout_sec: float = 1.0
    shadow_prefill_unwind_timeout_sec: float = 1.0
    shadow_prefill_max_retries: int = 1
    # Bithumb self-trade prevention safeguards (cross_trading)
    bithumb_stp_prevention_enabled: bool = True
    bithumb_stp_base_offset_ticks: int = 1
    bithumb_stp_max_offset_ticks: int = 3
    bithumb_stp_retry_cooldown_sec: float = 1.0
    bithumb_stp_pause_after_rejects: int = 3
    bithumb_stp_pause_duration_sec: float = 30.0
    bithumb_stp_consider_pending_cancel_as_conflict: bool = True
