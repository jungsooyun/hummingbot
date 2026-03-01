import asyncio
import logging
from decimal import Decimal
from typing import Dict

from hummingbot.connector.connector_base import ConnectorBase, Union
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
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
        # Hedging guard state
        self._unhedged_base: Decimal = Decimal("0")
        self._unhedged_quote: Decimal = Decimal("0")
        self._last_fill_timestamp: float = 0.0
        self._hedge_block_reason: str = ""
        self._early_stop_hedge_initiated: bool = False
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
        if maker_adjusted_candidate.amount == Decimal("0") or taker_adjusted_candidate.amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough budget to open position.")
            self.stop()

    async def control_task(self):
        if self.status == RunnableStatus.RUNNING:
            await self.update_prices_and_tx_costs()
            await self.control_maker_order()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()

    async def control_maker_order(self):
        if self.maker_order is None:
            await self.create_maker_order()
        elif self._maker_cancel_in_flight:
            await self.control_pending_maker_cancel()
        else:
            await self.control_update_maker_order()

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

    async def update_prices_and_tx_costs(self, order_amount: Decimal = None):
        hedge_amount = order_amount if order_amount is not None else self.config.order_amount
        self._taker_result_price = await self.get_resulting_price_for_amount(
            connector=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            is_buy=self.taker_order_side == TradeType.BUY,
            order_amount=hedge_amount)
        await self.update_tx_costs(order_amount=hedge_amount)
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
        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT,
            side=self.maker_order_side,
            amount=self.config.order_amount,
            price=self._maker_target_price)
        self.maker_order = TrackedOrder(order_id=order_id)
        self.logger().info(f"Created maker order {order_id} at price {self._maker_target_price}.")

    async def control_shutdown_process(self):
        if self.close_type == CloseType.EARLY_STOP:
            await self.control_early_stop_shutdown()
            return

        if self._should_refresh_taker_limit_order():
            await self._refresh_taker_limit_order()
            return

        if self.maker_order is not None and self.maker_order.is_done and (self.taker_order is None):
            await self._maybe_place_taker_after_fill()

        maker_done = self.maker_order is not None and self.maker_order.is_done
        taker_done = self.taker_order is not None and self.taker_order.is_done
        if maker_done and taker_done:
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

        ready_to_stop = (
            not self._tracked_order_is_open(self.maker_order)
            and not self._maker_cancel_in_flight
            and not self._taker_cancel_in_flight
        )
        if self._early_stop_hedge_initiated:
            taker_done = self.taker_order is not None and self.taker_order.is_done
            ready_to_stop = ready_to_stop and taker_done
        else:
            ready_to_stop = ready_to_stop and (not self._tracked_order_is_open(self.taker_order))

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
        elif self.taker_order and event.order_id == self.taker_order.order_id:
            self.logger().info(f"Taker order {event.order_id} created.")
            self.taker_order.order = self.get_in_flight_order(self.taker_connector, event.order_id)

    def process_order_completed_event(self,
                                      event_tag: int,
                                      market: ConnectorBase,
                                      event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        if self.maker_order and event.order_id == self.maker_order.order_id:
            event_executed_base = getattr(event, "base_asset_amount", Decimal("0"))
            event_executed_quote = getattr(event, "quote_asset_amount", Decimal("0"))
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

    def process_order_canceled_event(self,
                                     event_tag: int,
                                     market: ConnectorBase,
                                     event: OrderCancelledEvent):
        if self.maker_order and event.order_id == self.maker_order.order_id:
            self.logger().info(f"Maker order {event.order_id} cancelled.")
            self.maker_order = None
            self._maker_cancel_in_flight = False
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

    def place_taker_order(self, order_amount: Decimal = None):
        taker_order_type = self._effective_taker_order_type()
        taker_price = Decimal("NaN")
        amount = order_amount if order_amount is not None else self.config.order_amount
        if taker_order_type == OrderType.LIMIT:
            taker_price = self._get_taker_limit_price(amount)
        taker_order_id = self.place_order(
            connector_name=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            order_type=taker_order_type,
            side=self.taker_order_side,
            amount=amount,
            price=taker_price)
        self.taker_order = TrackedOrder(order_id=taker_order_id)

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        if self.maker_order and self.maker_order.order_id == event.order_id:
            self.failed_orders.append(self.maker_order)
            self.maker_order = None
            self._maker_cancel_in_flight = False
            if self.close_type == CloseType.EARLY_STOP:
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
            "stale_fill_hedge_mode": self.config.stale_fill_hedge_mode,
            "maker_cancel_in_flight": self._maker_cancel_in_flight,
            "taker_cancel_in_flight": self._taker_cancel_in_flight,
            "early_stop_hedge_initiated": self._early_stop_hedge_initiated,
            "unhedged_base": self._unhedged_base,
            "unhedged_quote": self._unhedged_quote,
            "hedge_block_reason": self._hedge_block_reason,
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
        return f"""
Maker Side: {self.maker_order_side}
-----------------------------------------------------------------------------------------------------------------------
    - Maker: {self.maker_connector} {self.maker_trading_pair} | Taker: {self.taker_connector} {self.taker_trading_pair}
    - Taker order type: {self._effective_taker_order_type().name} | Slippage buffer (bps): {self.config.taker_slippage_buffer_bps} | Taker retries: {self._current_retries}/{self._max_retries}
    - Taker fallback to market: {self.config.taker_fallback_to_market}
    - Stale fill hedge mode: {self.config.stale_fill_hedge_mode} | Early-stop hedge in-flight: {self._early_stop_hedge_initiated}
    - Maker refresh: price_pct={self.config.maker_price_refresh_pct * 100:.4f}% | max_age={self.config.maker_order_max_age_seconds}s
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

    def _get_taker_limit_price(self, amount: Decimal) -> Decimal:
        price_result = self.connectors[self.taker_connector].get_price_for_volume(
            self.taker_trading_pair,
            self.taker_order_side == TradeType.BUY,
            amount,
        )
        reference_price = price_result.result_price if price_result.result_price is not None else self._taker_result_price
        slippage_multiplier = self.config.taker_slippage_buffer_bps / Decimal("10000")
        if self.taker_order_side == TradeType.BUY:
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
