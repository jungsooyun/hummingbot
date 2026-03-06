import asyncio
import logging
from decimal import Decimal
from typing import Dict, Optional, Tuple

from hummingbot.connector.connector_base import ConnectorBase, Union
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class XEMMExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @staticmethod
    def _are_tokens_interchangeable(first_token: str, second_token: str):
        interchangeable_tokens = [
            {"WETH", "ETH"},
            {"WBTC", "BTC"},
            {"WBNB", "BNB"},
            {"WPOL", "POL"},
            {"WAVAX", "AVAX"},
            {"WONE", "ONE"},
            {"USDC", "USDC.E"},
            {"WBTC", "BTC"},
            {"USOL", "SOL"},
            {"UETH", "ETH"},
            {"UBTC", "BTC"}
        ]
        same_token_condition = first_token == second_token
        tokens_interchangeable_condition = any(({first_token, second_token} <= interchangeable_pair
                                                for interchangeable_pair
                                                in interchangeable_tokens))
        # for now, we will consider all the stablecoins interchangeable
        stable_coins_condition = "USD" in first_token and "USD" in second_token
        return same_token_condition or tokens_interchangeable_condition or stable_coins_condition

    def is_arbitrage_valid(self, pair1, pair2):
        base_asset1, _ = split_hb_trading_pair(pair1)
        base_asset2, _ = split_hb_trading_pair(pair2)
        return self._are_tokens_interchangeable(base_asset1, base_asset2)

    def __init__(self, strategy: ScriptStrategyBase, config: XEMMExecutorConfig, update_interval: float = 1.0,
                 max_retries: int = 10):
        if not self.is_arbitrage_valid(pair1=config.buying_market.trading_pair,
                                       pair2=config.selling_market.trading_pair):
            raise Exception("XEMM is not valid since the trading pairs are not interchangeable.")
        self.config = config
        self.rate_oracle = RateOracle.get_instance()
        if config.maker_side == TradeType.BUY:
            self.maker_connector = config.buying_market.connector_name
            self.maker_trading_pair = config.buying_market.trading_pair
            self.maker_order_side = TradeType.BUY
            self.taker_connector = config.selling_market.connector_name
            self.taker_trading_pair = config.selling_market.trading_pair
            self.taker_order_side = TradeType.SELL
        else:
            self.maker_connector = config.selling_market.connector_name
            self.maker_trading_pair = config.selling_market.trading_pair
            self.maker_order_side = TradeType.SELL
            self.taker_connector = config.buying_market.connector_name
            self.taker_trading_pair = config.buying_market.trading_pair
            self.taker_order_side = TradeType.BUY

        # Set up quote conversion pair
        _, maker_quote = split_hb_trading_pair(self.maker_trading_pair)
        _, taker_quote = split_hb_trading_pair(self.taker_trading_pair)
        self.quote_conversion_pair = f"{taker_quote}-{maker_quote}"

        taker_connector = strategy.connectors[self.taker_connector]
        if not self.is_amm_connector(exchange=self.taker_connector):
            supported_taker_order_types = taker_connector.supported_order_types()
            if self.config.taker_order_type not in supported_taker_order_types:
                raise ValueError(
                    f"{self.taker_connector} does not support {self.config.taker_order_type.name} orders."
                )
            if self.config.taker_fallback_to_market and OrderType.MARKET not in supported_taker_order_types:
                raise ValueError(
                    f"{self.taker_connector} does not support MARKET orders required by taker_fallback_to_market."
                )
            if self._shadow_maker_active() and OrderType.LIMIT_MAKER not in supported_taker_order_types:
                raise ValueError(
                    f"{self.taker_connector} does not support LIMIT_MAKER orders required by shadow_maker_enabled."
                )
        self._taker_result_price = Decimal("1")
        self._maker_target_price = Decimal("1")
        self._tx_cost = Decimal("1")
        self._tx_cost_pct = Decimal("1")
        self._current_trade_profitability = Decimal("0")
        self.maker_order = None
        self.taker_order = None
        self.failed_orders = []
        self._current_retries = 0
        self._max_retries = config.taker_max_retries if config.taker_max_retries is not None else max_retries
        self._force_market_taker = False
        self._maker_cancel_in_flight = False
        self._maker_cancel_requested_timestamp = 0.0
        self._maker_cancel_retry_interval = 3.0
        self._taker_cancel_in_flight = False
        self._taker_cancel_requested_timestamp = 0.0
        self._taker_cancel_retry_interval = 3.0
        # Optional shadow maker (on taker connector) state
        self.shadow_order: Optional[TrackedOrder] = None
        self._shadow_cancel_in_flight = False
        self._shadow_cancel_requested_timestamp = 0.0
        self._shadow_cancel_retry_interval = 3.0
        # Shadow prefill protection mode state
        self._shadow_prefill_mode = False
        self._shadow_prefill_stage: Optional[str] = None  # cancel_maker | cross | unwind
        self._shadow_prefill_stage_started_ts = 0.0
        self._shadow_prefill_remaining_base = Decimal("0")
        self._shadow_prefill_remaining_quote = Decimal("0")
        self._shadow_prefill_recorded_fills: Dict[str, Decimal] = {}
        self._shadow_prefill_cross_order_active = False
        self._shadow_prefill_unwind_order: Optional[TrackedOrder] = None
        self._shadow_prefill_unwind_cancel_in_flight = False
        self._shadow_prefill_unwind_cancel_requested_timestamp = 0.0
        self._shadow_prefill_unwind_cancel_retry_interval = 3.0
        self._shadow_prefill_unwind_retries = 0
        self._shadow_prefill_last_guard_block_ts: float = 0.0
        self._last_one_sided_warning_ts = 0.0
        # Hedging guard state
        self._unhedged_base: Decimal = Decimal("0")
        self._unhedged_quote: Decimal = Decimal("0")
        self._last_fill_timestamp: float = 0.0
        self._hedge_block_reason: str = ""
        self._early_stop_hedge_initiated: bool = False
        # Bithumb self-trade prevention (STP) guard state
        self._stp_reject_streak: int = 0
        self._stp_dynamic_offset_ticks: int = max(1, int(self.config.bithumb_stp_base_offset_ticks))
        self._stp_cooldown_until_ts: float = 0.0
        self._stp_pause_until_ts: float = 0.0
        self._stp_last_error_message: str = ""
        self._stp_last_blocker_order_id: Optional[str] = None
        self._stp_last_blocker_price: Optional[Decimal] = None
        super().__init__(strategy=strategy,
                         connectors=[config.buying_market.connector_name, config.selling_market.connector_name],
                         config=config, update_interval=update_interval)

    async def validate_sufficient_balance(self):
        mid_price = self.get_price(self.maker_connector, self.maker_trading_pair,
                                   price_type=PriceType.MidPrice)
        maker_order_candidate = OrderCandidate(
            trading_pair=self.maker_trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=self.maker_order_side,
            amount=self.config.order_amount,
            price=mid_price,)
        taker_order_candidate = OrderCandidate(
            trading_pair=self.taker_trading_pair,
            is_maker=False,
            order_type=self._effective_taker_order_type(),
            order_side=self.taker_order_side,
            amount=self.config.order_amount,
            price=mid_price,)
        maker_adjusted_candidate = self.adjust_order_candidates(self.maker_connector, [maker_order_candidate])[0]
        taker_adjusted_candidate = self.adjust_order_candidates(self.taker_connector, [taker_order_candidate])[0]
        maker_has_budget = maker_adjusted_candidate.amount > Decimal("0")
        taker_has_budget = taker_adjusted_candidate.amount > Decimal("0")
        if not maker_has_budget:
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough maker-side budget to open position.")
            self.stop()
            return
        if not taker_has_budget:
            if self.config.allow_one_sided_inventory_mode:
                self._log_one_sided_mode_warning(
                    "Taker-side budget is insufficient at start. Continuing in one-sided inventory mode."
                )
                return
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough taker-side budget to open position.")
            self.stop()

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            await self.update_prices_and_tx_costs()
            if self._shadow_prefill_mode:
                await self.control_shadow_prefill_mode()
                return
            await self.control_maker_order()
            await self.control_shadow_order()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()

    async def control_maker_order(self):
        if self.maker_order is None:
            now = self._strategy.current_timestamp
            if self._is_stp_pause_active(now):
                return
            if self._is_stp_cooldown_active(now):
                return
            await self.create_maker_order()
        elif self._maker_cancel_in_flight:
            await self.control_pending_maker_cancel()
        else:
            await self.control_update_maker_order()

    def _shadow_maker_active(self) -> bool:
        return (
            self.config.shadow_maker_enabled
            and self.maker_connector == "bithumb"
            and self.taker_connector == "upbit"
        )

    def _log_one_sided_mode_warning(self, message: str):
        now = self._strategy.current_timestamp
        if now - self._last_one_sided_warning_ts >= 10.0:
            self.logger().warning(message)
            self._last_one_sided_warning_ts = now

    def _has_order_budget(
        self,
        connector_name: str,
        trading_pair: str,
        side: TradeType,
        amount: Decimal,
        price: Decimal,
        order_type: OrderType,
        is_maker: bool,
    ) -> bool:
        try:
            candidate = OrderCandidate(
                trading_pair=trading_pair,
                is_maker=is_maker,
                order_type=order_type,
                order_side=side,
                amount=amount,
                price=price,
            )
            adjusted_candidate = self.adjust_order_candidates(connector_name, [candidate])[0]
            return adjusted_candidate.amount > Decimal("0")
        except Exception:
            return False

    def _bithumb_stp_guard_active(self) -> bool:
        return (
            self.config.bithumb_stp_prevention_enabled
            and self.maker_connector == "bithumb"
            and not self.is_amm_connector(exchange=self.maker_connector)
        )

    def _is_stp_pause_active(self, now: float) -> bool:
        return self._bithumb_stp_guard_active() and now < self._stp_pause_until_ts

    def _is_stp_cooldown_active(self, now: float) -> bool:
        return self._bithumb_stp_guard_active() and now < self._stp_cooldown_until_ts

    def _reset_stp_state_on_success(self):
        self._stp_reject_streak = 0
        self._stp_dynamic_offset_ticks = max(1, int(self.config.bithumb_stp_base_offset_ticks))
        self._stp_cooldown_until_ts = 0.0
        self._stp_pause_until_ts = 0.0
        self._stp_last_error_message = ""
        self._stp_last_blocker_order_id = None
        self._stp_last_blocker_price = None

    def _mark_stp_rejection(self, error_message: str):
        now = self._strategy.current_timestamp
        self._stp_reject_streak += 1
        self._stp_last_error_message = error_message
        self._stp_dynamic_offset_ticks = min(
            int(self.config.bithumb_stp_max_offset_ticks),
            max(self._stp_dynamic_offset_ticks + 1, int(self.config.bithumb_stp_base_offset_ticks)),
        )
        self._stp_cooldown_until_ts = now + float(self.config.bithumb_stp_retry_cooldown_sec)
        if self._stp_reject_streak >= int(self.config.bithumb_stp_pause_after_rejects):
            self._stp_pause_until_ts = now + float(self.config.bithumb_stp_pause_duration_sec)
            self._stp_cooldown_until_ts = self._stp_pause_until_ts

    def _is_stp_failure_event(self, event: MarketOrderFailureEvent) -> bool:
        error_type = (event.error_type or "").lower()
        error_message = (event.error_message or "").lower()
        return (
            "selftradeprevention" in error_type
            or "cross_trading" in error_type
            or "cross_trading" in error_message
            or "cross trading" in error_message
            or "자전거래" in error_message
            or "자전 거래" in error_message
            or "자전거래 위험" in error_message
        )

    def _find_bithumb_stp_conflicting_order(self, candidate_price: Decimal):
        if not self._bithumb_stp_guard_active():
            return None
        connector = self.connectors[self.maker_connector]
        for in_flight_order in connector.in_flight_orders.values():
            if in_flight_order.trading_pair != self.maker_trading_pair:
                continue
            if in_flight_order.is_done:
                continue
            if in_flight_order.trade_type == self.maker_order_side:
                continue
            if (
                not self.config.bithumb_stp_consider_pending_cancel_as_conflict
                and in_flight_order.is_pending_cancel_confirmation
            ):
                continue
            if self.maker_order is not None and in_flight_order.client_order_id == self.maker_order.order_id:
                continue
            if in_flight_order.price is None:
                continue

            order_price = Decimal(str(in_flight_order.price))
            if order_price <= Decimal("0"):
                continue

            if self.maker_order_side == TradeType.BUY and order_price <= candidate_price:
                return in_flight_order
            if self.maker_order_side == TradeType.SELL and order_price >= candidate_price:
                return in_flight_order
        return None

    def _safe_maker_price_with_stp(self, target_price: Decimal) -> Optional[Decimal]:
        if not self._bithumb_stp_guard_active():
            return target_price

        connector = self.connectors[self.maker_connector]
        quantized_target = connector.quantize_order_price(self.maker_trading_pair, target_price)
        if quantized_target <= Decimal("0"):
            return None

        initial_blocker = self._find_bithumb_stp_conflicting_order(quantized_target)
        force_offset = self._stp_reject_streak > 0
        if not force_offset and initial_blocker is None:
            self._stp_last_blocker_order_id = None
            self._stp_last_blocker_price = None
            return quantized_target

        if initial_blocker is not None:
            self._stp_last_blocker_order_id = initial_blocker.client_order_id
            self._stp_last_blocker_price = initial_blocker.price

        quantum = connector.get_order_price_quantum(self.maker_trading_pair, quantized_target)
        if quantum <= Decimal("0"):
            quantum = Decimal("1")
        max_ticks = max(1, int(self.config.bithumb_stp_max_offset_ticks))
        start_ticks = max(1, int(self.config.bithumb_stp_base_offset_ticks), int(self._stp_dynamic_offset_ticks))
        for ticks in range(start_ticks, max_ticks + 1):
            if self.maker_order_side == TradeType.BUY:
                candidate_price = quantized_target - (quantum * ticks)
            else:
                candidate_price = quantized_target + (quantum * ticks)
            candidate_price = connector.quantize_order_price(self.maker_trading_pair, candidate_price)
            if candidate_price <= Decimal("0"):
                continue
            blocker = self._find_bithumb_stp_conflicting_order(candidate_price)
            if blocker is None:
                return candidate_price
            self._stp_last_blocker_order_id = blocker.client_order_id
            self._stp_last_blocker_price = blocker.price

        return None

    async def control_shadow_order(self):
        if not self._shadow_maker_active():
            return
        if self._shadow_prefill_mode:
            if self._tracked_order_is_open(self.shadow_order):
                self.request_shadow_cancel()
            elif self._shadow_cancel_in_flight:
                await self.control_pending_shadow_cancel()
            return

        if self.maker_order is None or self._maker_cancel_in_flight:
            if self._tracked_order_is_open(self.shadow_order):
                self.request_shadow_cancel()
            elif self._shadow_cancel_in_flight:
                await self.control_pending_shadow_cancel()
            return

        if self.shadow_order is None:
            await self.create_shadow_order()
        elif self._shadow_cancel_in_flight:
            await self.control_pending_shadow_cancel()
        else:
            await self.control_update_shadow_order()

    def _record_maker_fill(
        self,
        executed_base: Decimal = None,
        executed_quote: Decimal = None,
    ):
        if executed_base is None or executed_quote is None:
            if self.maker_order is None or not self.maker_order.is_done:
                return
            executed_base = self.maker_order.executed_amount_base
            executed_quote = self.maker_order.average_executed_price * executed_base
        if executed_base.is_nan() or executed_quote.is_nan():
            return
        # maker_side BUY => positive base, negative quote (spent)
        sign = Decimal("1") if self.maker_order_side == TradeType.BUY else Decimal("-1")
        self._unhedged_base += executed_base * sign
        self._unhedged_quote += executed_quote * sign
        self._last_fill_timestamp = self._strategy.current_timestamp

    def _use_market_hedge_on_early_stop(self) -> bool:
        return str(self.config.stale_fill_hedge_mode).lower() == "market"

    def _unhedged_abs_notional(self) -> Decimal:
        if self._unhedged_base == 0:
            return Decimal("0")
        avg_price = abs(self._unhedged_quote) / abs(self._unhedged_base)
        return abs(self._unhedged_base) * avg_price

    def _unhedged_avg_price(self) -> Decimal:
        if self._unhedged_base == 0:
            return Decimal("0")
        return abs(self._unhedged_quote) / abs(self._unhedged_base)

    async def control_pending_maker_cancel(self):
        if self.maker_order is None:
            self._maker_cancel_in_flight = False
            return

        maker_order = self.maker_order.order
        if maker_order is not None and not maker_order.is_open:
            # Terminal event will clear state. Keep waiting to avoid immediate re-placement race.
            return

        now = self._strategy.current_timestamp
        if now - self._maker_cancel_requested_timestamp >= self._maker_cancel_retry_interval:
            self.logger().warning(
                f"Maker cancel pending for {self.maker_order.order_id} after "
                f"{now - self._maker_cancel_requested_timestamp:.2f}s. Retrying cancel."
            )
            self._strategy.cancel(self.maker_connector, self.maker_trading_pair, self.maker_order.order_id)
            self._maker_cancel_requested_timestamp = now

    def request_shadow_cancel(self):
        if self.shadow_order is None:
            return
        if self._shadow_cancel_in_flight:
            return
        self._strategy.cancel(self.taker_connector, self.taker_trading_pair, self.shadow_order.order_id)
        self._shadow_cancel_in_flight = True
        self._shadow_cancel_requested_timestamp = self._strategy.current_timestamp

    async def control_pending_shadow_cancel(self):
        if self.shadow_order is None:
            self._shadow_cancel_in_flight = False
            return
        shadow_order = self.shadow_order.order
        if shadow_order is not None and not shadow_order.is_open:
            return
        now = self._strategy.current_timestamp
        if now - self._shadow_cancel_requested_timestamp >= self._shadow_cancel_retry_interval:
            self.logger().warning(
                f"Shadow cancel pending for {self.shadow_order.order_id} after "
                f"{now - self._shadow_cancel_requested_timestamp:.2f}s. Retrying cancel."
            )
            self._strategy.cancel(self.taker_connector, self.taker_trading_pair, self.shadow_order.order_id)
            self._shadow_cancel_requested_timestamp = now

    def _maker_remaining_base(self) -> Decimal:
        if self.maker_order is None:
            return Decimal("0")
        executed = self.maker_order.executed_amount_base if self.maker_order.executed_amount_base else Decimal("0")
        remaining = self.config.order_amount - executed
        return remaining if remaining > 0 else Decimal("0")

    def _shadow_profitability_floor(self) -> Decimal:
        return max(self.config.shadow_maker_min_profitability, self.config.target_profitability)

    def _compute_shadow_order_spec(self) -> Optional[Tuple[TradeType, Decimal, Decimal]]:
        if self.maker_order is None:
            return None
        maker_price = self.maker_order.price if self.maker_order.price and self.maker_order.price > 0 else self._maker_target_price
        if maker_price <= 0:
            return None
        remaining_base = self._maker_remaining_base()
        if remaining_base <= 0:
            return None
        shadow_amount = self.connectors[self.taker_connector].quantize_order_amount(self.taker_trading_pair, remaining_base)
        if shadow_amount <= 0:
            return None

        shadow_side = self.taker_order_side
        extra_buffer = self.config.shadow_maker_extra_buffer_bps / Decimal("10000")
        required_profit = self._shadow_profitability_floor() + self._tx_cost_pct + extra_buffer

        if shadow_side == TradeType.SELL:
            min_safe_price = maker_price * (Decimal("1") + required_profit)
            best_ask = self.get_price(self.taker_connector, self.taker_trading_pair, PriceType.BestAsk) or self._taker_result_price
            candidate_price = max(min_safe_price, best_ask)
        else:
            max_safe_price = maker_price * (Decimal("1") - required_profit)
            if max_safe_price <= 0:
                return None
            best_bid = self.get_price(self.taker_connector, self.taker_trading_pair, PriceType.BestBid) or self._taker_result_price
            candidate_price = min(max_safe_price, best_bid)
        shadow_price = self.connectors[self.taker_connector].quantize_order_price(self.taker_trading_pair, candidate_price)
        if shadow_price <= 0:
            return None
        return shadow_side, shadow_amount, shadow_price

    async def create_shadow_order(self):
        order_spec = self._compute_shadow_order_spec()
        if order_spec is None:
            return
        shadow_side, shadow_amount, shadow_price = order_spec
        if self.config.allow_one_sided_inventory_mode:
            has_budget = self._has_order_budget(
                connector_name=self.taker_connector,
                trading_pair=self.taker_trading_pair,
                side=shadow_side,
                amount=shadow_amount,
                price=shadow_price,
                order_type=OrderType.LIMIT_MAKER,
                is_maker=True,
            )
            if not has_budget:
                self._log_one_sided_mode_warning(
                    f"Skipping shadow maker order due to insufficient {self.taker_connector} balance "
                    f"(side={shadow_side.name}, amount={shadow_amount})."
                )
                return
        shadow_order_id = self.place_order(
            connector_name=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            order_type=OrderType.LIMIT_MAKER,
            side=shadow_side,
            amount=shadow_amount,
            price=shadow_price,
        )
        self.shadow_order = TrackedOrder(order_id=shadow_order_id)
        self.logger().info(
            f"Created shadow maker order {shadow_order_id} on {self.taker_connector} "
            f"side={shadow_side.name} amount={shadow_amount} price={shadow_price}."
        )

    async def control_update_shadow_order(self):
        should_refresh, refresh_reason = self._should_refresh_shadow_order()
        if should_refresh:
            self.logger().info(f"Refreshing shadow maker order {self.shadow_order.order_id}: {refresh_reason}")
            self.request_shadow_cancel()

    def _amount_refresh_tolerance(self, connector: str, trading_pair: str) -> Decimal:
        try:
            trading_rule = self.get_trading_rules(connector, trading_pair)
            return trading_rule.min_base_amount_increment or Decimal("0")
        except Exception:
            return Decimal("0")

    def _should_refresh_shadow_order(self) -> Tuple[bool, str]:
        if self.shadow_order is None or self.shadow_order.order is None:
            return False, ""
        if not self.shadow_order.order.is_open:
            return False, ""
        desired_spec = self._compute_shadow_order_spec()
        if desired_spec is None:
            return True, "shadow order no longer valid"
        _, desired_amount, desired_price = desired_spec
        current_price = self.shadow_order.order.price
        current_amount = self.shadow_order.order.amount
        if current_price is None or current_price <= 0:
            return True, "missing current shadow price"
        price_deviation = abs(desired_price - current_price) / current_price
        if price_deviation >= self.config.shadow_maker_price_refresh_pct:
            return True, (
                f"price_deviation={price_deviation:.6f} >= "
                f"shadow_maker_price_refresh_pct={self.config.shadow_maker_price_refresh_pct}"
            )
        amount_tolerance = self._amount_refresh_tolerance(self.taker_connector, self.taker_trading_pair)
        amount_diff = abs(desired_amount - current_amount)
        if amount_diff > max(amount_tolerance, Decimal("0")):
            return True, f"shadow amount changed by {amount_diff} (> tolerance={amount_tolerance})"
        order_age = self._strategy.current_timestamp - self.shadow_order.order.creation_timestamp
        if order_age >= self.config.shadow_maker_order_max_age_seconds and price_deviation > Decimal("0"):
            return True, (
                f"shadow order_age={order_age:.2f}s with stale price deviation={price_deviation:.6f} "
                f"exceeds shadow_maker_order_max_age_seconds={self.config.shadow_maker_order_max_age_seconds}"
            )
        return False, ""

    def _record_prefill_fill(self, order_id: str, amount: Decimal):
        previous = self._shadow_prefill_recorded_fills.get(order_id, Decimal("0"))
        self._shadow_prefill_recorded_fills[order_id] = previous + amount

    def _record_prefill_completion_delta(self, order_id: str, completed_base: Decimal) -> Decimal:
        already = self._shadow_prefill_recorded_fills.get(order_id, Decimal("0"))
        delta = completed_base - already
        self._shadow_prefill_recorded_fills.pop(order_id, None)
        return delta if delta > 0 else Decimal("0")

    def _start_shadow_prefill_mode(self):
        if not self._shadow_prefill_mode:
            self.logger().warning(
                f"Shadow prefill detected. Entering protection mode with remaining_base={self._shadow_prefill_remaining_base}."
            )
            self._shadow_prefill_mode = True
            self._shadow_prefill_stage = "cancel_maker"
            self._shadow_prefill_stage_started_ts = self._strategy.current_timestamp
            self._shadow_prefill_cross_order_active = False
            self._shadow_prefill_unwind_retries = 0
        if self._tracked_order_is_open(self.shadow_order):
            self.request_shadow_cancel()
        if self._tracked_order_is_open(self.maker_order):
            self.request_maker_cancel()

    def _consume_shadow_prefill_remaining(self, consumed: Decimal):
        if consumed is None or consumed <= 0:
            return
        current_base = self._shadow_prefill_remaining_base
        if current_base > 0 and self._shadow_prefill_remaining_quote > 0:
            avg_entry_price = self._shadow_prefill_remaining_quote / current_base
            quote_consumed = avg_entry_price * consumed
            self._shadow_prefill_remaining_quote -= quote_consumed
            if self._shadow_prefill_remaining_quote < 0:
                self._shadow_prefill_remaining_quote = Decimal("0")
        self._shadow_prefill_remaining_base -= consumed
        if self._shadow_prefill_remaining_base < 0:
            self._shadow_prefill_remaining_base = Decimal("0")
        if self._shadow_prefill_remaining_base == 0:
            self._shadow_prefill_remaining_quote = Decimal("0")

    def _add_shadow_prefill_exposure(
        self,
        base_amount: Decimal,
        quote_amount: Decimal = Decimal("0"),
        price_hint: Decimal = Decimal("0"),
    ):
        if base_amount is None or base_amount <= 0:
            return
        self._shadow_prefill_remaining_base += base_amount
        if quote_amount is not None and quote_amount > 0:
            self._shadow_prefill_remaining_quote += quote_amount
            return
        if price_hint is not None and price_hint > 0:
            self._shadow_prefill_remaining_quote += base_amount * price_hint

    def _shadow_prefill_avg_entry_price(self) -> Decimal:
        if self._shadow_prefill_remaining_base <= 0 or self._shadow_prefill_remaining_quote <= 0:
            return Decimal("0")
        return self._shadow_prefill_remaining_quote / self._shadow_prefill_remaining_base

    def _shadow_prefill_close_profitability(self, close_price: Decimal) -> Decimal:
        entry_price = self._shadow_prefill_avg_entry_price()
        if entry_price <= 0 or close_price is None or close_price <= 0:
            return Decimal("0")
        if self.maker_order_side == TradeType.BUY:
            # prefill happened as taker SELL, flatten with BUY
            return (entry_price - close_price) / entry_price
        # prefill happened as taker BUY, flatten with SELL
        return (close_price - entry_price) / entry_price

    def _can_place_shadow_prefill_close_order(self, stage: str, close_price: Decimal) -> bool:
        if self.config.allow_loss_hedge:
            return True
        if close_price is None or close_price <= 0:
            return True
        profitability = self._shadow_prefill_close_profitability(close_price)
        if profitability >= self.config.min_profitability_guard:
            return True
        now = self._strategy.current_timestamp
        if now - self._shadow_prefill_last_guard_block_ts >= 2.0:
            self.logger().warning(
                f"Shadow prefill {stage} blocked by profitability guard: "
                f"estimated_profitability={profitability:.6f} < min_guard={self.config.min_profitability_guard}, "
                f"entry_avg={self._shadow_prefill_avg_entry_price():.6f}, close_price={close_price:.6f}"
            )
            self._shadow_prefill_last_guard_block_ts = now
        return False

    async def control_shadow_prefill_mode(self):
        if self._shadow_prefill_remaining_base <= 0:
            self.logger().info("Shadow prefill fully flattened. Terminating executor.")
            self.close_type = CloseType.COMPLETED
            self.stop()
            return

        if self._shadow_prefill_stage == "cancel_maker":
            if self._tracked_order_is_open(self.shadow_order):
                self.request_shadow_cancel()
            elif self._shadow_cancel_in_flight:
                await self.control_pending_shadow_cancel()

            if self._tracked_order_is_open(self.maker_order):
                self.request_maker_cancel()
                return
            if self._maker_cancel_in_flight:
                await self.control_pending_maker_cancel()
                return

            self._shadow_prefill_stage = "cross"
            self._shadow_prefill_stage_started_ts = self._strategy.current_timestamp
            return

        if self._shadow_prefill_stage == "cross":
            if self.maker_order is None and not self._shadow_prefill_cross_order_active:
                await self._place_shadow_prefill_cross_order()
                return
            if self._tracked_order_is_open(self.maker_order):
                order_age = self._strategy.current_timestamp - self.maker_order.order.creation_timestamp
                if order_age >= self.config.shadow_prefill_cross_timeout_sec:
                    self.request_maker_cancel()
                return
            if self._maker_cancel_in_flight:
                await self.control_pending_maker_cancel()
                return
            if self._shadow_prefill_cross_order_active and self.maker_order is None:
                if self._shadow_prefill_remaining_base > 0:
                    self._shadow_prefill_stage = "unwind"
                    self._shadow_prefill_stage_started_ts = self._strategy.current_timestamp
                return

        if self._shadow_prefill_stage == "unwind":
            if self._shadow_prefill_remaining_base <= 0:
                self.close_type = CloseType.COMPLETED
                self.stop()
                return
            if self._shadow_prefill_unwind_retries > self.config.shadow_prefill_max_retries:
                self.close_type = CloseType.FAILED
                self.logger().error(
                    f"Shadow prefill unwind retries exceeded with remaining_base={self._shadow_prefill_remaining_base}"
                )
                self.stop()
                return
            if self._shadow_prefill_unwind_order is None:
                await self._place_shadow_prefill_unwind_order()
                return
            if self._tracked_order_is_open(self._shadow_prefill_unwind_order):
                order_age = self._strategy.current_timestamp - self._shadow_prefill_unwind_order.order.creation_timestamp
                if order_age >= self.config.shadow_prefill_unwind_timeout_sec:
                    self.request_shadow_prefill_unwind_cancel()
                return
            if self._shadow_prefill_unwind_cancel_in_flight:
                await self.control_pending_shadow_prefill_unwind_cancel()
                return

    def _get_aggressive_limit_price(
        self,
        connector_name: str,
        trading_pair: str,
        side: TradeType,
        amount: Decimal,
        slippage_buffer_bps: Decimal,
    ) -> Decimal:
        connector = self.connectors[connector_name]
        price_result = connector.get_price_for_volume(trading_pair, side == TradeType.BUY, amount)
        if side == TradeType.BUY:
            fallback_price = self.get_price(connector_name, trading_pair, PriceType.BestAsk)
        else:
            fallback_price = self.get_price(connector_name, trading_pair, PriceType.BestBid)
        reference_price = price_result.result_price if price_result.result_price is not None else fallback_price
        buffer = slippage_buffer_bps / Decimal("10000")
        if side == TradeType.BUY:
            price = reference_price * (Decimal("1") + buffer)
        else:
            price = reference_price * (Decimal("1") - buffer)
        return connector.quantize_order_price(trading_pair, price)

    async def _place_shadow_prefill_cross_order(self):
        amount = self.connectors[self.maker_connector].quantize_order_amount(
            self.maker_trading_pair, self._shadow_prefill_remaining_base
        )
        if amount <= 0:
            self.close_type = CloseType.COMPLETED
            self.stop()
            return
        price = self._get_aggressive_limit_price(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            side=self.maker_order_side,
            amount=amount,
            slippage_buffer_bps=self.config.taker_slippage_buffer_bps,
        )
        if not self._can_place_shadow_prefill_close_order(stage="cross", close_price=price):
            return
        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT,
            side=self.maker_order_side,
            amount=amount,
            price=price,
        )
        self.maker_order = TrackedOrder(order_id=order_id)
        self._shadow_prefill_cross_order_active = True
        self._shadow_prefill_stage_started_ts = self._strategy.current_timestamp
        self.logger().warning(
            f"Shadow prefill cross attempt: order_id={order_id}, connector={self.maker_connector}, "
            f"side={self.maker_order_side.name}, amount={amount}, price={price}"
        )

    async def _place_shadow_prefill_unwind_order(self):
        amount = self.connectors[self.taker_connector].quantize_order_amount(
            self.taker_trading_pair, self._shadow_prefill_remaining_base
        )
        if amount <= 0:
            self.close_type = CloseType.COMPLETED
            self.stop()
            return
        unwind_side = self.maker_order_side
        order_type = self._effective_taker_order_type()
        price = Decimal("NaN")
        guard_price = Decimal("0")
        if order_type == OrderType.LIMIT:
            price = self._get_aggressive_limit_price(
                connector_name=self.taker_connector,
                trading_pair=self.taker_trading_pair,
                side=unwind_side,
                amount=amount,
                slippage_buffer_bps=self.config.taker_slippage_buffer_bps,
            )
            guard_price = price
        else:
            price_result = self.connectors[self.taker_connector].get_price_for_volume(
                self.taker_trading_pair, unwind_side == TradeType.BUY, amount
            )
            if price_result is not None and price_result.result_price is not None:
                guard_price = Decimal(str(price_result.result_price))
        if not self._can_place_shadow_prefill_close_order(stage="unwind", close_price=guard_price):
            return
        order_id = self.place_order(
            connector_name=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            order_type=order_type,
            side=unwind_side,
            amount=amount,
            price=price,
        )
        self._shadow_prefill_unwind_order = TrackedOrder(order_id=order_id)
        self._shadow_prefill_stage_started_ts = self._strategy.current_timestamp
        self.logger().warning(
            f"Shadow prefill unwind attempt: order_id={order_id}, connector={self.taker_connector}, "
            f"side={unwind_side.name}, amount={amount}, order_type={order_type.name}"
        )

    def request_shadow_prefill_unwind_cancel(self):
        if self._shadow_prefill_unwind_order is None:
            return
        if self._shadow_prefill_unwind_cancel_in_flight:
            return
        self._strategy.cancel(self.taker_connector, self.taker_trading_pair, self._shadow_prefill_unwind_order.order_id)
        self._shadow_prefill_unwind_cancel_in_flight = True
        self._shadow_prefill_unwind_cancel_requested_timestamp = self._strategy.current_timestamp

    async def control_pending_shadow_prefill_unwind_cancel(self):
        if self._shadow_prefill_unwind_order is None:
            self._shadow_prefill_unwind_cancel_in_flight = False
            return
        unwind_order = self._shadow_prefill_unwind_order.order
        if unwind_order is not None and not unwind_order.is_open:
            return
        now = self._strategy.current_timestamp
        if now - self._shadow_prefill_unwind_cancel_requested_timestamp >= self._shadow_prefill_unwind_cancel_retry_interval:
            self.logger().warning(
                f"Shadow prefill unwind cancel pending for {self._shadow_prefill_unwind_order.order_id}. Retrying."
            )
            self._strategy.cancel(self.taker_connector, self.taker_trading_pair, self._shadow_prefill_unwind_order.order_id)
            self._shadow_prefill_unwind_cancel_requested_timestamp = now

    async def update_prices_and_tx_costs(self, order_amount: Decimal = None):
        hedge_amount = order_amount if order_amount is not None else self.config.order_amount
        self._taker_result_price = await self.get_resulting_price_for_amount(
            connector=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            is_buy=self.taker_order_side == TradeType.BUY,
            order_amount=hedge_amount)
        await self.update_tx_costs(order_amount=hedge_amount)
        if getattr(self.config, "maker_price_source", "formula") == "best":
            connector = self.connectors[self.maker_connector]
            # is_buy=True -> best ask, is_buy=False -> best bid
            best_price = connector.get_price(self.maker_trading_pair, self.maker_order_side == TradeType.SELL)
            if best_price is not None and best_price > Decimal("0") and not best_price.is_nan():
                self._maker_target_price = connector.quantize_order_price(self.maker_trading_pair, best_price)
                return
            self.logger().warning(
                f"maker_price_source=best but maker best price unavailable for "
                f"{self.maker_connector} {self.maker_trading_pair}. Falling back to formula pricing."
            )

        if self.taker_order_side == TradeType.BUY:
            # Maker is SELL: profitability = (maker_price - taker_price) / maker_price
            # To achieve target: maker_price = taker_price / (1 - target_profitability - tx_cost_pct)
            self._maker_target_price = self._taker_result_price / (Decimal("1") - self.config.target_profitability - self._tx_cost_pct)
        else:
            # Maker is BUY: profitability = (taker_price - maker_price) / maker_price
            # To achieve target: maker_price = taker_price / (1 + target_profitability + tx_cost_pct)
            self._maker_target_price = self._taker_result_price / (Decimal("1") + self.config.target_profitability + self._tx_cost_pct)

    async def update_tx_costs(self, order_amount: Decimal = None):
        base, quote = split_hb_trading_pair(trading_pair=self.config.buying_market.trading_pair)
        base_without_wrapped = base[1:] if base.startswith("W") else base
        taker_fee_task = asyncio.create_task(self.get_tx_cost_in_asset(
            exchange=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            order_type=self._effective_taker_order_type(),
            is_buy=self.taker_order_side == TradeType.BUY,
            order_amount=order_amount if order_amount is not None else self.config.order_amount,
            asset=base_without_wrapped
        ))
        maker_fee_task = asyncio.create_task(self.get_tx_cost_in_asset(
            exchange=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT,
            is_buy=self.maker_order_side == TradeType.BUY,
            order_amount=order_amount if order_amount is not None else self.config.order_amount,
            asset=base_without_wrapped
        ))

        taker_fee, maker_fee = await asyncio.gather(taker_fee_task, maker_fee_task)
        self._tx_cost = taker_fee + maker_fee
        self._tx_cost_pct = self._tx_cost / self.config.order_amount

    async def get_tx_cost_in_asset(self, exchange: str, trading_pair: str, is_buy: bool, order_amount: Decimal,
                                   asset: str, order_type: OrderType = OrderType.MARKET):
        connector = self.connectors[exchange]
        if self.is_amm_connector(exchange=exchange):
            gas_cost = connector.network_transaction_fee
            conversion_price = RateOracle.get_instance().get_pair_rate(f"{asset}-{gas_cost.token}")
            if conversion_price is None:
                self.logger().warning(f"Could not get conversion rate for {asset}-{gas_cost.token}")
                return Decimal("0")
            return gas_cost.amount / conversion_price
        else:
            fee = connector.get_fee(
                base_currency=asset,
                quote_currency=trading_pair.split("-")[1],
                order_type=order_type,
                order_side=TradeType.BUY if is_buy else TradeType.SELL,
                amount=order_amount,
                price=self._taker_result_price,
                is_maker=order_type.is_limit_type(),
            )
            return fee.fee_amount_in_token(
                trading_pair=trading_pair,
                price=self._taker_result_price,
                order_amount=order_amount,
                token=asset,
                exchange=connector,
            )

    async def get_resulting_price_for_amount(self, connector: str, trading_pair: str, is_buy: bool,
                                             order_amount: Decimal):
        return await self.connectors[connector].get_quote_price(trading_pair, is_buy, order_amount)

    async def create_maker_order(self):
        maker_price = self._safe_maker_price_with_stp(self._maker_target_price)
        if maker_price is None:
            self._stp_last_error_message = (
                "Bithumb STP guard could not find a safe maker price within configured offset ticks."
            )
            self._stp_cooldown_until_ts = self._strategy.current_timestamp + float(self.config.bithumb_stp_retry_cooldown_sec)
            self.logger().warning(self._stp_last_error_message)
            return

        estimated_profitability = await self._estimate_profitability_for_maker_price(maker_price)
        if estimated_profitability is None:
            self.logger().warning("Could not estimate profitability for maker order. Skipping maker placement.")
            return
        if estimated_profitability < self.config.min_profitability:
            self.logger().info(
                f"Skipping maker placement at price={maker_price}: estimated profitability "
                f"{estimated_profitability} < min_profitability {self.config.min_profitability}."
            )
            return
        if estimated_profitability > self.config.max_profitability:
            self.logger().info(
                f"Skipping maker placement at price={maker_price}: estimated profitability "
                f"{estimated_profitability} > max_profitability {self.config.max_profitability}."
            )
            return

        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT,
            side=self.maker_order_side,
            amount=self.config.order_amount,
            price=maker_price)
        self.maker_order = TrackedOrder(order_id=order_id)
        self.logger().info(f"Created maker order {order_id} at price {maker_price}.")

    async def _estimate_profitability_for_maker_price(self, maker_price: Decimal) -> Optional[Decimal]:
        if maker_price <= Decimal("0") or self._taker_result_price <= Decimal("0"):
            return None
        try:
            conversion_rate = await self.get_quote_asset_conversion_rate()
            normalized_taker_price = self._taker_result_price * conversion_rate
            if self.maker_order_side == TradeType.BUY:
                trade_profitability = (normalized_taker_price - maker_price) / maker_price
            else:
                trade_profitability = (maker_price - normalized_taker_price) / maker_price
            return trade_profitability - self._tx_cost_pct
        except Exception as e:
            self.logger().error(f"Error estimating profitability for maker price {maker_price}: {e}")
            return None

    async def control_shutdown_process(self):
        if self.close_type == CloseType.EARLY_STOP:
            await self.control_early_stop_shutdown()
            return

        if self._tracked_order_is_open(self.shadow_order):
            if not self._shadow_cancel_in_flight:
                self.request_shadow_cancel()
            else:
                await self.control_pending_shadow_cancel()
        if self._tracked_order_is_open(self._shadow_prefill_unwind_order):
            if not self._shadow_prefill_unwind_cancel_in_flight:
                self.request_shadow_prefill_unwind_cancel()
            else:
                await self.control_pending_shadow_prefill_unwind_cancel()

        if self._should_refresh_taker_limit_order():
            await self._refresh_taker_limit_order()
            return

        if self.maker_order is not None and self.maker_order.is_done and (self.taker_order is None):
            await self._maybe_place_taker_after_fill()

        maker_done = self.maker_order is not None and self.maker_order.is_done
        taker_done = self.taker_order is not None and self.taker_order.is_done
        shadow_closed = not self._tracked_order_is_open(self.shadow_order) and not self._shadow_cancel_in_flight
        unwind_closed = (
            not self._tracked_order_is_open(self._shadow_prefill_unwind_order)
            and not self._shadow_prefill_unwind_cancel_in_flight
        )
        if maker_done and taker_done and shadow_closed and unwind_closed:
            self.logger().info("Both orders are done, executor terminated.")
            self.stop()

    async def control_early_stop_shutdown(self):
        maker_open = self._tracked_order_is_open(self.maker_order)
        taker_open = self._tracked_order_is_open(self.taker_order)

        if maker_open:
            if not self._maker_cancel_in_flight:
                self.logger().info(f"Early stop: cancelling maker order {self.maker_order.order_id}.")
                self.request_maker_cancel()
            else:
                await self.control_pending_maker_cancel()

        if taker_open:
            if self._early_stop_hedge_initiated:
                # Emergency hedge is in-flight; allow it to complete.
                pass
            else:
                if not self._taker_cancel_in_flight:
                    self.logger().info(f"Early stop: cancelling taker order {self.taker_order.order_id}.")
                    self.request_taker_cancel()
                else:
                    await self.control_pending_taker_cancel()

        shadow_open = self._tracked_order_is_open(self.shadow_order)
        if shadow_open:
            if not self._shadow_cancel_in_flight:
                self.logger().info(f"Early stop: cancelling shadow order {self.shadow_order.order_id}.")
                self.request_shadow_cancel()
            else:
                await self.control_pending_shadow_cancel()
        unwind_open = self._tracked_order_is_open(self._shadow_prefill_unwind_order)
        if unwind_open:
            if not self._shadow_prefill_unwind_cancel_in_flight:
                self.logger().info(
                    f"Early stop: cancelling shadow prefill unwind order {self._shadow_prefill_unwind_order.order_id}."
                )
                self.request_shadow_prefill_unwind_cancel()
            else:
                await self.control_pending_shadow_prefill_unwind_cancel()

        ready_to_stop = (
            not self._tracked_order_is_open(self.maker_order)
            and not self._maker_cancel_in_flight
            and not self._taker_cancel_in_flight
            and not self._shadow_cancel_in_flight
            and not self._shadow_prefill_unwind_cancel_in_flight
        )
        if self._early_stop_hedge_initiated:
            taker_done = self.taker_order is not None and self.taker_order.is_done
            ready_to_stop = ready_to_stop and taker_done
        else:
            ready_to_stop = ready_to_stop and (not self._tracked_order_is_open(self.taker_order))
        ready_to_stop = ready_to_stop and (not self._tracked_order_is_open(self.shadow_order))
        ready_to_stop = ready_to_stop and (not self._tracked_order_is_open(self._shadow_prefill_unwind_order))

        if ready_to_stop:
            self.logger().info("Early stop cleanup completed. Executor terminated.")
            self.stop()

    @staticmethod
    def _tracked_order_is_open(order: TrackedOrder) -> bool:
        if order is None:
            return False
        if order.order is None:
            return True
        return order.order.is_open

    async def _refresh_taker_limit_order(self):
        if self.taker_order is None or self.taker_order.order is None:
            self._replace_taker_order_after_cancel()
            return

        if not self.taker_order.order.is_open:
            return

        if not self._taker_cancel_in_flight:
            self.logger().info(f"Refreshing taker order {self.taker_order.order_id}: requesting cancel before replace.")
            self.request_taker_cancel()
            return

        await self.control_pending_taker_cancel()

    async def control_pending_taker_cancel(self):
        if self.taker_order is None:
            self._taker_cancel_in_flight = False
            return

        taker_order = self.taker_order.order
        if taker_order is not None and not taker_order.is_open:
            # Terminal event will clear state. Keep waiting to avoid immediate re-placement race.
            return

        now = self._strategy.current_timestamp
        if now - self._taker_cancel_requested_timestamp >= self._taker_cancel_retry_interval:
            self.logger().warning(
                f"Taker cancel pending for {self.taker_order.order_id} after "
                f"{now - self._taker_cancel_requested_timestamp:.2f}s. Retrying cancel."
            )
            self._strategy.cancel(self.taker_connector, self.taker_trading_pair, self.taker_order.order_id)
            self._taker_cancel_requested_timestamp = now

    def _replace_taker_order_after_cancel(self):
        self._current_retries += 1
        if self._current_retries > self._max_retries:
            if self.config.taker_fallback_to_market and not self._force_market_taker:
                self.logger().warning(
                    f"Taker {self.config.taker_order_type.name} retries exceeded. Fallback to MARKET for {self.taker_connector}."
                )
                self._force_market_taker = True
                self._current_retries = 0
            else:
                self.close_type = CloseType.FAILED
                self.logger().error("Taker order retries exceeded while already using MARKET. Stopping executor.")
                self.stop()
                return
        self.place_taker_order()

    async def control_update_maker_order(self):
        await self.update_current_trade_profitability()
        should_refresh, refresh_reason = self._should_refresh_maker_order()
        if should_refresh:
            self.logger().info(
                f"Refreshing maker order {self.maker_order.order_id}: {refresh_reason}. "
                f"old_price={self.maker_order.order.price if self.maker_order and self.maker_order.order else 'n/a'} "
                f"target_price={self._maker_target_price}"
            )
            self.request_maker_cancel()
            return

        if self._current_trade_profitability - self._tx_cost_pct < self.config.min_profitability:
            self.logger().info(f"Order {self.maker_order.order_id} profitability {self._current_trade_profitability - self._tx_cost_pct} is below minimum profitability {self.config.min_profitability}. Cancelling order.")
            self.request_maker_cancel()
        elif self._current_trade_profitability - self._tx_cost_pct > self.config.max_profitability:
            self.logger().info(f"Order {self.maker_order.order_id} profitability {self._current_trade_profitability - self._tx_cost_pct} is above maximum profitability {self.config.max_profitability}. Cancelling order.")
            self.request_maker_cancel()

    def request_maker_cancel(self):
        if self.maker_order is None:
            return
        if self._maker_cancel_in_flight:
            return
        self._strategy.cancel(self.maker_connector, self.maker_trading_pair, self.maker_order.order_id)
        self._maker_cancel_in_flight = True
        self._maker_cancel_requested_timestamp = self._strategy.current_timestamp

    async def _maybe_place_taker_after_fill(self):
        # Guard against placing hedges below profitability unless allowed
        hedge_amount = abs(self._unhedged_base) if self._unhedged_base != 0 else self.config.order_amount
        await self.update_prices_and_tx_costs(order_amount=hedge_amount)

        if self._unhedged_base == 0:
            self.logger().warning("Maker fill recorded but unhedged_base is zero. Skipping taker.")
            return

        avg_price = self._unhedged_avg_price()
        # profitability vs current taker price
        if avg_price == 0:
            self.logger().warning("Cannot compute average price for hedge. Skipping taker.")
            return

        # Compute profitability based on side
        if self.maker_order_side == TradeType.BUY:
            profitability = (self._taker_result_price - avg_price) / avg_price - self._tx_cost_pct
        else:
            profitability = (avg_price - self._taker_result_price) / avg_price - self._tx_cost_pct

        force_by_notional = (
            self.config.max_unhedged_notional_quote > 0 and
            self._unhedged_abs_notional() >= self.config.max_unhedged_notional_quote
        )

        if (not self.config.allow_loss_hedge and
                profitability < self.config.min_profitability_guard and
                not force_by_notional):
            self._hedge_block_reason = (
                f"Guard block: profitability={profitability:.6f} < min_guard={self.config.min_profitability_guard}"
            )
            self.logger().info(self._hedge_block_reason)
            return

        taker_order_type = self._effective_taker_order_type()
        taker_price = self._taker_result_price
        if taker_order_type == OrderType.LIMIT:
            taker_price = self._get_taker_limit_price(hedge_amount)
        has_taker_budget = self._has_order_budget(
            connector_name=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            side=self.taker_order_side,
            amount=hedge_amount,
            price=taker_price,
            order_type=taker_order_type,
            is_maker=False,
        )
        if not has_taker_budget:
            if self.config.allow_one_sided_inventory_mode:
                self._hedge_block_reason = "One-sided mode: taker hedge skipped due to insufficient balance."
                self._log_one_sided_mode_warning(self._hedge_block_reason)
                self.close_type = CloseType.INSUFFICIENT_BALANCE
                self.stop()
                return
            self._hedge_block_reason = "Taker hedge blocked: insufficient balance."
            self.logger().warning(self._hedge_block_reason)
            return

        # Place taker hedge for unhedged amount
        self._hedge_block_reason = ""
        self.place_taker_order(order_amount=hedge_amount)

    def request_taker_cancel(self):
        if self.taker_order is None:
            return
        if self._taker_cancel_in_flight:
            return
        self._strategy.cancel(self.taker_connector, self.taker_trading_pair, self.taker_order.order_id)
        self._taker_cancel_in_flight = True
        self._taker_cancel_requested_timestamp = self._strategy.current_timestamp

    def _should_refresh_maker_order(self) -> tuple[bool, str]:
        if self.maker_order is None or self.maker_order.order is None:
            return False, ""
        if not self.maker_order.order.is_open:
            return False, ""

        maker_price = self.maker_order.order.price
        if maker_price is None or maker_price <= Decimal("0") or self._maker_target_price <= Decimal("0"):
            return False, ""

        price_deviation = abs(self._maker_target_price - maker_price) / maker_price
        if price_deviation >= self.config.maker_price_refresh_pct:
            return True, (
                f"price_deviation={price_deviation:.6f} >= maker_price_refresh_pct="
                f"{self.config.maker_price_refresh_pct}"
            )
        order_age = self._strategy.current_timestamp - self.maker_order.order.creation_timestamp
        # 나의 주문가격이 목표가와 동일/유사할 때는 age만으로는 취소하지 않는다.
        if order_age >= self.config.maker_order_max_age_seconds and price_deviation > Decimal("0"):
            return True, (
                f"order_age={order_age:.2f}s with stale price (deviation={price_deviation:.6f}) exceeds "
                f"maker_order_max_age_seconds={self.config.maker_order_max_age_seconds}"
            )
        return False, ""

    async def update_current_trade_profitability(self):
        trade_profitability = Decimal("0")
        if self.maker_order and self.maker_order.order and self.maker_order.order.is_open:
            maker_price = self.maker_order.order.price
            # Get the conversion rate to normalize prices to the same quote asset
            try:
                conversion_rate = await self.get_quote_asset_conversion_rate()
                if self.maker_order_side == TradeType.BUY:
                    # If maker is buying, normalize taker (sell) price to maker quote asset
                    normalized_taker_price = self._taker_result_price * conversion_rate
                    trade_profitability = (normalized_taker_price - maker_price) / maker_price
                else:
                    # If maker is selling, normalize taker (buy) price to maker quote asset
                    normalized_taker_price = self._taker_result_price * conversion_rate
                    trade_profitability = (maker_price - normalized_taker_price) / maker_price
            except Exception as e:
                self.logger().error(f"Error calculating trade profitability: {e}")
                return Decimal("0")
        self._current_trade_profitability = trade_profitability
        return trade_profitability

    def process_order_created_event(self,
                                    event_tag: int,
                                    market: ConnectorBase,
                                    event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if self.maker_order and event.order_id == self.maker_order.order_id:
            self.logger().info(f"Maker order {event.order_id} created.")
            self.maker_order.order = self.get_in_flight_order(self.maker_connector, event.order_id)
            self._reset_stp_state_on_success()
        elif self.taker_order and event.order_id == self.taker_order.order_id:
            self.logger().info(f"Taker order {event.order_id} created.")
            self.taker_order.order = self.get_in_flight_order(self.taker_connector, event.order_id)
        elif self.shadow_order and event.order_id == self.shadow_order.order_id:
            self.logger().info(f"Shadow maker order {event.order_id} created.")
            self.shadow_order.order = self.get_in_flight_order(self.taker_connector, event.order_id)
        elif self._shadow_prefill_unwind_order and event.order_id == self._shadow_prefill_unwind_order.order_id:
            self.logger().info(f"Shadow prefill unwind order {event.order_id} created.")
            self._shadow_prefill_unwind_order.order = self.get_in_flight_order(self.taker_connector, event.order_id)

    def process_order_filled_event(self,
                                   event_tag: int,
                                   market: ConnectorBase,
                                   event: OrderFilledEvent):
        if self.shadow_order and event.order_id == self.shadow_order.order_id:
            self._record_prefill_fill(order_id=event.order_id, amount=event.amount)
            self._add_shadow_prefill_exposure(
                base_amount=event.amount,
                quote_amount=event.amount * event.price if event.price is not None else Decimal("0"),
            )
            if not self._shadow_prefill_mode and not (self.maker_order and self.maker_order.is_done):
                self._start_shadow_prefill_mode()
        elif self._shadow_prefill_mode and self.maker_order and event.order_id == self.maker_order.order_id:
            self._record_prefill_fill(order_id=event.order_id, amount=event.amount)
            self._consume_shadow_prefill_remaining(event.amount)
        elif self._shadow_prefill_unwind_order and event.order_id == self._shadow_prefill_unwind_order.order_id:
            self._record_prefill_fill(order_id=event.order_id, amount=event.amount)
            self._consume_shadow_prefill_remaining(event.amount)

    def process_order_completed_event(self,
                                      event_tag: int,
                                      market: ConnectorBase,
                                      event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        if self.maker_order and event.order_id == self.maker_order.order_id:
            event_executed_base = getattr(event, "base_asset_amount", Decimal("0"))
            event_executed_quote = getattr(event, "quote_asset_amount", Decimal("0"))
            if self._shadow_prefill_mode:
                delta = self._record_prefill_completion_delta(event.order_id, event_executed_base)
                self._consume_shadow_prefill_remaining(delta)
                self.logger().warning(
                    f"Shadow prefill cross order {event.order_id} completed. "
                    f"remaining_prefill_base={self._shadow_prefill_remaining_base}"
                )
                self._maker_cancel_in_flight = False
                self._shadow_prefill_cross_order_active = False
                self.maker_order = None
                if self._shadow_prefill_remaining_base <= 0:
                    self.close_type = CloseType.COMPLETED
                    self.stop()
                else:
                    self._shadow_prefill_stage = "unwind"
                    self._shadow_prefill_stage_started_ts = self._strategy.current_timestamp
                return
            if self.close_type == CloseType.EARLY_STOP:
                self._maker_cancel_in_flight = False
                self._record_maker_fill(executed_base=event_executed_base, executed_quote=event_executed_quote)
                if self._use_market_hedge_on_early_stop():
                    hedge_amount = abs(self._unhedged_base)
                    if hedge_amount > 0 and self.taker_order is None:
                        self.logger().warning(
                            f"Maker order {event.order_id} completed during early stop. "
                            f"Placing emergency MARKET taker hedge amount={hedge_amount}."
                        )
                        self._force_market_taker = True
                        self.place_taker_order(order_amount=hedge_amount)
                        self._early_stop_hedge_initiated = True
                    else:
                        self.logger().warning(
                            f"Maker order {event.order_id} completed during early stop, but no hedge sent "
                            f"(hedge_amount={hedge_amount}, taker_order_exists={self.taker_order is not None})."
                        )
                else:
                    self.logger().info(f"Maker order {event.order_id} completed during early stop. Skipping taker hedge.")
                return
            self.logger().info(f"Maker order {event.order_id} completed. Executing taker order.")
            self._maker_cancel_in_flight = False
            self._current_retries = 0
            self._record_maker_fill(executed_base=event_executed_base, executed_quote=event_executed_quote)
            # Taker will be placed in shutdown control with guard
            self._status = RunnableStatus.SHUTTING_DOWN
        elif self.taker_order and event.order_id == self.taker_order.order_id:
            self.logger().info(f"Taker order {event.order_id} completed.")
            self._taker_cancel_in_flight = False
            self._current_retries = 0
            executed_base = self.taker_order.executed_amount_base
            if executed_base == Decimal("0"):
                executed_base = getattr(event, "base_asset_amount", Decimal("0"))
            if executed_base and not executed_base.is_nan():
                sign = Decimal("-1") if self.maker_order_side == TradeType.BUY else Decimal("1")
                self._unhedged_base += executed_base * sign
                # approximate quote using average executed price if available
                if self.taker_order.average_executed_price and not self.taker_order.average_executed_price.is_nan():
                    self._unhedged_quote += executed_base * self.taker_order.average_executed_price * sign
                else:
                    event_executed_quote = getattr(event, "quote_asset_amount", Decimal("0"))
                    if event_executed_quote and not event_executed_quote.is_nan():
                        self._unhedged_quote += event_executed_quote * sign
            self._early_stop_hedge_initiated = False
        elif self.shadow_order and event.order_id == self.shadow_order.order_id:
            completed_base = getattr(event, "base_asset_amount", Decimal("0"))
            completed_quote = getattr(event, "quote_asset_amount", Decimal("0"))
            delta = self._record_prefill_completion_delta(event.order_id, completed_base)
            if delta > 0:
                quote_delta = Decimal("0")
                if completed_base > 0 and completed_quote > 0:
                    quote_delta = (completed_quote / completed_base) * delta
                elif self.shadow_order.average_executed_price and self.shadow_order.average_executed_price > 0:
                    quote_delta = self.shadow_order.average_executed_price * delta
                elif self.shadow_order.price and self.shadow_order.price > 0:
                    quote_delta = self.shadow_order.price * delta
                self._add_shadow_prefill_exposure(base_amount=delta, quote_amount=quote_delta)
            self.logger().warning(
                f"Shadow maker order {event.order_id} completed before maker. "
                f"prefill_remaining_base={self._shadow_prefill_remaining_base}"
            )
            self.shadow_order = None
            self._shadow_cancel_in_flight = False
            if self._shadow_prefill_remaining_base > 0:
                self._start_shadow_prefill_mode()
        elif self._shadow_prefill_unwind_order and event.order_id == self._shadow_prefill_unwind_order.order_id:
            completed_base = getattr(event, "base_asset_amount", Decimal("0"))
            delta = self._record_prefill_completion_delta(event.order_id, completed_base)
            self._consume_shadow_prefill_remaining(delta)
            self.logger().warning(
                f"Shadow prefill unwind order {event.order_id} completed. "
                f"remaining_prefill_base={self._shadow_prefill_remaining_base}"
            )
            self._shadow_prefill_unwind_order = None
            self._shadow_prefill_unwind_cancel_in_flight = False
            if self._shadow_prefill_remaining_base <= 0:
                self.close_type = CloseType.COMPLETED
                self.stop()
            else:
                self._shadow_prefill_unwind_retries += 1
                if self._shadow_prefill_unwind_retries > self.config.shadow_prefill_max_retries:
                    self.close_type = CloseType.FAILED
                    self.logger().error(
                        f"Shadow prefill unwind retries exceeded with remaining_base={self._shadow_prefill_remaining_base}"
                    )
                    self.stop()

    def process_order_canceled_event(self,
                                     event_tag: int,
                                     market: ConnectorBase,
                                     event: OrderCancelledEvent):
        if self.maker_order and event.order_id == self.maker_order.order_id:
            self.logger().info(f"Maker order {event.order_id} cancelled.")
            executed_base = self.maker_order.executed_amount_base if self.maker_order.executed_amount_base else Decimal("0")
            if self._shadow_prefill_mode and executed_base > 0:
                delta = self._record_prefill_completion_delta(event.order_id, executed_base)
                self._consume_shadow_prefill_remaining(delta)
            self.maker_order = None
            self._maker_cancel_in_flight = False
            if self._shadow_prefill_mode and self._shadow_prefill_stage == "cross":
                self._shadow_prefill_cross_order_active = False
                if self._shadow_prefill_remaining_base > 0:
                    self._shadow_prefill_stage = "unwind"
                    self._shadow_prefill_stage_started_ts = self._strategy.current_timestamp
        elif self.taker_order and event.order_id == self.taker_order.order_id:
            self.logger().info(f"Taker order {event.order_id} cancelled.")
            self.taker_order = None
            self._taker_cancel_in_flight = False
            if self.close_type == CloseType.EARLY_STOP and self._early_stop_hedge_initiated:
                hedge_amount = abs(self._unhedged_base)
                if hedge_amount > 0:
                    self.logger().warning(
                        f"Early-stop hedge taker was cancelled. Replacing MARKET hedge order amount={hedge_amount}."
                    )
                    self._force_market_taker = True
                    self.place_taker_order(order_amount=hedge_amount)
                    return
                self._early_stop_hedge_initiated = False
            if self.status == RunnableStatus.SHUTTING_DOWN and self.close_type != CloseType.EARLY_STOP:
                self._replace_taker_order_after_cancel()
        elif self.shadow_order and event.order_id == self.shadow_order.order_id:
            self.logger().info(f"Shadow maker order {event.order_id} cancelled.")
            executed_base = self.shadow_order.executed_amount_base if self.shadow_order.executed_amount_base else Decimal("0")
            if executed_base > 0:
                delta = self._record_prefill_completion_delta(event.order_id, executed_base)
                quote_delta = Decimal("0")
                if self.shadow_order.average_executed_price and self.shadow_order.average_executed_price > 0:
                    quote_delta = self.shadow_order.average_executed_price * delta
                elif self.shadow_order.price and self.shadow_order.price > 0:
                    quote_delta = self.shadow_order.price * delta
                self._add_shadow_prefill_exposure(base_amount=delta, quote_amount=quote_delta)
                if self._shadow_prefill_remaining_base > 0:
                    self._start_shadow_prefill_mode()
            self.shadow_order = None
            self._shadow_cancel_in_flight = False
        elif self._shadow_prefill_unwind_order and event.order_id == self._shadow_prefill_unwind_order.order_id:
            self.logger().info(f"Shadow prefill unwind order {event.order_id} cancelled.")
            executed_base = (
                self._shadow_prefill_unwind_order.executed_amount_base
                if self._shadow_prefill_unwind_order.executed_amount_base else Decimal("0")
            )
            if executed_base > 0:
                delta = self._record_prefill_completion_delta(event.order_id, executed_base)
                self._consume_shadow_prefill_remaining(delta)
            self._shadow_prefill_unwind_order = None
            self._shadow_prefill_unwind_cancel_in_flight = False
            self._shadow_prefill_unwind_retries += 1
            if self._shadow_prefill_unwind_retries > self.config.shadow_prefill_max_retries and self._shadow_prefill_remaining_base > 0:
                self.close_type = CloseType.FAILED
                self.logger().error(
                    f"Shadow prefill unwind retries exceeded after cancel with remaining_base={self._shadow_prefill_remaining_base}"
                )
                self.stop()

    def place_taker_order(self, order_amount: Decimal = None, side: TradeType = None):
        taker_order_type = self._effective_taker_order_type()
        taker_price = Decimal("NaN")
        amount = order_amount if order_amount is not None else self.config.order_amount
        effective_side = side if side is not None else self.taker_order_side
        if taker_order_type == OrderType.LIMIT:
            taker_price = self._get_taker_limit_price(amount, side=effective_side)
        taker_order_id = self.place_order(
            connector_name=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            order_type=taker_order_type,
            side=effective_side,
            amount=amount,
            price=taker_price)
        self.taker_order = TrackedOrder(order_id=taker_order_id)

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        if self.maker_order and self.maker_order.order_id == event.order_id:
            self.failed_orders.append(self.maker_order)
            self.maker_order = None
            self._maker_cancel_in_flight = False
            is_stp_reject = self._is_stp_failure_event(event)
            if is_stp_reject:
                error_message = event.error_message or "Bithumb self-trade prevention rejection."
                self._mark_stp_rejection(error_message)
                self.logger().warning(
                    f"Bithumb STP rejection on maker order {event.order_id}. "
                    f"streak={self._stp_reject_streak}, offset_ticks={self._stp_dynamic_offset_ticks}, "
                    f"cooldown_until={self._stp_cooldown_until_ts}, pause_until={self._stp_pause_until_ts}. "
                    f"error={error_message}"
                )
            if self._shadow_prefill_mode:
                self._shadow_prefill_cross_order_active = False
                self._shadow_prefill_stage = "unwind"
                self._shadow_prefill_stage_started_ts = self._strategy.current_timestamp
                return
            if self.close_type == CloseType.EARLY_STOP:
                return
            if is_stp_reject:
                return
            self._current_retries += 1
        elif self.taker_order and self.taker_order.order_id == event.order_id:
            self.failed_orders.append(self.taker_order)
            self._taker_cancel_in_flight = False
            if self.close_type == CloseType.EARLY_STOP:
                if self._early_stop_hedge_initiated:
                    hedge_amount = abs(self._unhedged_base)
                    if hedge_amount > 0:
                        self.logger().warning(
                            f"Emergency taker hedge failed during early stop. Retrying MARKET hedge amount={hedge_amount}."
                        )
                        self.taker_order = None
                        self._force_market_taker = True
                        self.place_taker_order(order_amount=hedge_amount)
                        return
                    self._early_stop_hedge_initiated = False
                self.taker_order = None
                return
            self._current_retries += 1
            if self._current_retries > self._max_retries:
                if self.config.taker_fallback_to_market and not self._force_market_taker:
                    self.logger().warning(
                        f"Taker {self.config.taker_order_type.name} failed repeatedly. Fallback to MARKET."
                    )
                    self._force_market_taker = True
                    self._current_retries = 0
                else:
                    self.close_type = CloseType.FAILED
                    self.logger().error("Taker order retries exceeded while using MARKET. Stopping executor.")
                    self.stop()
                    return
            self.place_taker_order()
        elif self.shadow_order and self.shadow_order.order_id == event.order_id:
            self.failed_orders.append(self.shadow_order)
            self.shadow_order = None
            self._shadow_cancel_in_flight = False
            if self._shadow_prefill_remaining_base > 0:
                self._start_shadow_prefill_mode()
        elif self._shadow_prefill_unwind_order and self._shadow_prefill_unwind_order.order_id == event.order_id:
            self.failed_orders.append(self._shadow_prefill_unwind_order)
            self._shadow_prefill_unwind_order = None
            self._shadow_prefill_unwind_cancel_in_flight = False
            self._shadow_prefill_unwind_retries += 1
            if self._shadow_prefill_unwind_retries > self.config.shadow_prefill_max_retries:
                self.close_type = CloseType.FAILED
                self.logger().error(
                    f"Shadow prefill unwind failed repeatedly with remaining_base={self._shadow_prefill_remaining_base}."
                )
                self.stop()

    def get_custom_info(self) -> Dict:
        # Since we can't make this method async, we'll skip the profitability calculation
        # The profitability will still be shown in the status message which is async
        return {
            "side": self.config.maker_side,
            "maker_connector": self.maker_connector,
            "maker_trading_pair": self.maker_trading_pair,
            "taker_connector": self.taker_connector,
            "taker_trading_pair": self.taker_trading_pair,
            "min_profitability": self.config.min_profitability,
            "target_profitability_pct": self.config.target_profitability,
            "max_profitability": self.config.max_profitability,
            "maker_price_refresh_pct": self.config.maker_price_refresh_pct,
            "maker_order_max_age_seconds": self.config.maker_order_max_age_seconds,
            "trade_profitability": self._current_trade_profitability,
            "tx_cost": self._tx_cost,
            "tx_cost_pct": self._tx_cost_pct,
            "taker_price": self._taker_result_price,
            "maker_target_price": self._maker_target_price,
            "net_profitability": self._current_trade_profitability - self._tx_cost_pct,
            "order_amount": self.config.order_amount,
            "taker_order_type": self._effective_taker_order_type().name,
            "taker_slippage_buffer_bps": self.config.taker_slippage_buffer_bps,
            "taker_order_max_age_seconds": self.config.taker_order_max_age_seconds,
            "taker_max_retries": self._max_retries,
            "taker_retries": self._current_retries,
            "taker_fallback_to_market": self.config.taker_fallback_to_market,
            "allow_one_sided_inventory_mode": self.config.allow_one_sided_inventory_mode,
            "stale_fill_hedge_mode": self.config.stale_fill_hedge_mode,
            "maker_cancel_in_flight": self._maker_cancel_in_flight,
            "taker_cancel_in_flight": self._taker_cancel_in_flight,
            "shadow_maker_enabled": self.config.shadow_maker_enabled,
            "shadow_order_id": self.shadow_order.order_id if self.shadow_order else None,
            "shadow_cancel_in_flight": self._shadow_cancel_in_flight,
            "shadow_prefill_mode": self._shadow_prefill_mode,
            "shadow_prefill_stage": self._shadow_prefill_stage,
            "shadow_prefill_remaining_base": self._shadow_prefill_remaining_base,
            "shadow_prefill_remaining_quote": self._shadow_prefill_remaining_quote,
            "shadow_prefill_entry_avg_price": self._shadow_prefill_avg_entry_price(),
            "shadow_prefill_unwind_order_id": (
                self._shadow_prefill_unwind_order.order_id if self._shadow_prefill_unwind_order else None
            ),
            "shadow_prefill_unwind_retries": self._shadow_prefill_unwind_retries,
            "early_stop_hedge_initiated": self._early_stop_hedge_initiated,
            "unhedged_base": self._unhedged_base,
            "unhedged_quote": self._unhedged_quote,
            "hedge_block_reason": self._hedge_block_reason,
            "stp_reject_streak": self._stp_reject_streak,
            "stp_dynamic_offset_ticks": self._stp_dynamic_offset_ticks,
            "stp_cooldown_until_ts": self._stp_cooldown_until_ts,
            "stp_pause_until_ts": self._stp_pause_until_ts,
            "stp_pause_remaining_sec": max(0.0, self._stp_pause_until_ts - self._strategy.current_timestamp),
            "stp_last_error_message": self._stp_last_error_message,
            "stp_last_blocker_order_id": self._stp_last_blocker_order_id,
            "stp_last_blocker_price": self._stp_last_blocker_price,
            "loop_metrics": self.get_loop_metrics(),
        }

    def early_stop(self, keep_position: bool = False):
        if keep_position:
            self.close_type = CloseType.POSITION_HOLD
            self.stop()
            return

        self.close_type = CloseType.EARLY_STOP
        should_wait_cancels = False

        if self._tracked_order_is_open(self.maker_order):
            self.logger().info(f"Cancelling maker order {self.maker_order.order_id}.")
            self.request_maker_cancel()
            should_wait_cancels = True
        if self._tracked_order_is_open(self.taker_order):
            self.logger().info(f"Cancelling taker order {self.taker_order.order_id}.")
            self.request_taker_cancel()
            should_wait_cancels = True
        if self._tracked_order_is_open(self.shadow_order):
            self.logger().info(f"Cancelling shadow order {self.shadow_order.order_id}.")
            self.request_shadow_cancel()
            should_wait_cancels = True
        if self._tracked_order_is_open(self._shadow_prefill_unwind_order):
            self.logger().info(
                f"Cancelling shadow prefill unwind order {self._shadow_prefill_unwind_order.order_id}."
            )
            self.request_shadow_prefill_unwind_cancel()
            should_wait_cancels = True

        if should_wait_cancels:
            self._status = RunnableStatus.SHUTTING_DOWN
        else:
            self.stop()

    def get_cum_fees_quote(self) -> Decimal:
        if self.is_closed and self.maker_order and self.taker_order:
            return self.maker_order.cum_fees_quote + self.taker_order.cum_fees_quote
        else:
            return Decimal("0")

    def get_net_pnl_quote(self) -> Decimal:
        if self.is_closed and self.maker_order and self.taker_order and self.maker_order.is_done and self.taker_order.is_done:
            maker_pnl = self.maker_order.executed_amount_base * self.maker_order.average_executed_price
            taker_pnl = self.taker_order.executed_amount_base * self.taker_order.average_executed_price
            return taker_pnl - maker_pnl - self.get_cum_fees_quote()
        else:
            return Decimal("0")

    def get_net_pnl_pct(self) -> Decimal:
        pnl_quote = self.get_net_pnl_quote()
        return pnl_quote / self.config.order_amount

    async def get_quote_asset_conversion_rate(self) -> Decimal:
        """
        Fetch the conversion rate between the quote assets of the buying and selling markets.
        Example: For M3M3/USDT and M3M3/USDC, fetch the USDC/USDT rate.
        """
        try:
            conversion_rate = self.rate_oracle.get_pair_rate(self.quote_conversion_pair)
            if conversion_rate is None:
                self.logger().error(f"Could not fetch conversion rate for {self.quote_conversion_pair}")
                raise ValueError(f"Could not fetch conversion rate for {self.quote_conversion_pair}")
            return conversion_rate
        except Exception as e:
            self.logger().error(f"Error fetching conversion rate for {self.quote_conversion_pair}: {e}")
            raise

    def to_format_status(self):
        now = self._strategy.current_timestamp
        stp_cooldown_remaining = max(0.0, self._stp_cooldown_until_ts - now)
        stp_pause_remaining = max(0.0, self._stp_pause_until_ts - now)
        return f"""
Maker Side: {self.maker_order_side}
-----------------------------------------------------------------------------------------------------------------------
    - Maker: {self.maker_connector} {self.maker_trading_pair} | Taker: {self.taker_connector} {self.taker_trading_pair}
    - Taker order type: {self._effective_taker_order_type().name} | Slippage buffer (bps): {self.config.taker_slippage_buffer_bps} | Taker retries: {self._current_retries}/{self._max_retries}
    - Taker fallback to market: {self.config.taker_fallback_to_market}
    - Shadow maker: enabled={self.config.shadow_maker_enabled} | order_id={self.shadow_order.order_id if self.shadow_order else 'n/a'} | prefill_mode={self._shadow_prefill_mode} | prefill_stage={self._shadow_prefill_stage} | prefill_remaining={self._shadow_prefill_remaining_base}
    - Stale fill hedge mode: {self.config.stale_fill_hedge_mode} | Early-stop hedge in-flight: {self._early_stop_hedge_initiated}
    - Maker refresh: price_pct={self.config.maker_price_refresh_pct * 100:.4f}% | max_age={self.config.maker_order_max_age_seconds}s
    - Bithumb STP: enabled={self.config.bithumb_stp_prevention_enabled} | streak={self._stp_reject_streak} | dynamic_offset_ticks={self._stp_dynamic_offset_ticks} | cooldown_remaining={stp_cooldown_remaining:.2f}s | pause_remaining={stp_pause_remaining:.2f}s
    - Maker cancel in-flight: {self._maker_cancel_in_flight}
    - Taker cancel in-flight: {self._taker_cancel_in_flight}
    - Loop metrics: {self.format_loop_metrics()}
    - Min profitability: {self.config.min_profitability * 100:.2f}% | Target profitability: {self.config.target_profitability * 100:.2f}% | Max profitability: {self.config.max_profitability * 100:.2f}% | Current profitability: {(self._current_trade_profitability - self._tx_cost_pct) * 100:.2f}%
    - Trade profitability: {self._current_trade_profitability * 100:.2f}% | Tx cost: {self._tx_cost_pct * 100:.2f}%
    - Taker result price: {self._taker_result_price:.3f} | Tx cost: {self._tx_cost:.3f} {self.maker_trading_pair.split('-')[-1]} | Order amount (Base): {self.config.order_amount:.2f}
-----------------------------------------------------------------------------------------------------------------------
"""

    def _effective_taker_order_type(self) -> OrderType:
        return OrderType.MARKET if self._force_market_taker else self.config.taker_order_type

    def _get_taker_limit_price(self, amount: Decimal, side: TradeType = None) -> Decimal:
        effective_side = side if side is not None else self.taker_order_side
        price_result = self.connectors[self.taker_connector].get_price_for_volume(
            self.taker_trading_pair,
            effective_side == TradeType.BUY,
            amount,
        )
        reference_price = price_result.result_price if price_result.result_price is not None else self._taker_result_price
        slippage_multiplier = self.config.taker_slippage_buffer_bps / Decimal("10000")
        if effective_side == TradeType.BUY:
            limit_price = reference_price * (Decimal("1") + slippage_multiplier)
        else:
            limit_price = reference_price * (Decimal("1") - slippage_multiplier)
        return self.connectors[self.taker_connector].quantize_order_price(self.taker_trading_pair, limit_price)

    def _should_refresh_taker_limit_order(self) -> bool:
        if self._effective_taker_order_type() != OrderType.LIMIT:
            return False
        if self.taker_order is None or self.taker_order.order is None:
            return False
        taker_in_flight_order = self.taker_order.order
        if not taker_in_flight_order.is_open:
            return False
        order_age = self._strategy.current_timestamp - taker_in_flight_order.creation_timestamp
        return order_age >= self.config.taker_order_max_age_seconds
