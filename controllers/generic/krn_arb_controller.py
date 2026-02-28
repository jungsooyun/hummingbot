# hummingbot/controllers/generic/krn_arb_controller.py
"""
Korean Exchange Arbitrage Controller
Binance <-> Upbit (or other KRW exchange) arbitrage with kimchi premium tracking.
"""
from decimal import Decimal
from typing import List, Optional

from pydantic import Field

from hummingbot.core.data_type.common import MarketDict
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


class KrnArbControllerConfig(ControllerConfigBase):
    controller_name: str = "krn_arb_controller"
    controller_type: str = "generic"
    candles_config: List[CandlesConfig] = []

    # Exchange pairs
    global_exchange: ConnectorPair = ConnectorPair(
        connector_name="binance", trading_pair="BTC-USDT"
    )
    krn_exchange: ConnectorPair = ConnectorPair(
        connector_name="upbit", trading_pair="BTC-KRW"
    )

    # Strategy parameters
    min_profitability: Decimal = Field(
        default=Decimal("0.005"),
        json_schema_extra={
            "prompt": "Minimum profitability (e.g., 0.005 = 0.5%): ",
            "prompt_on_new": True,
            "is_updatable": True,
        },
    )
    max_executors_imbalance: int = Field(
        default=1,
        json_schema_extra={"is_updatable": True},
    )
    delay_between_executors: int = Field(
        default=10,
        json_schema_extra={"prompt": "Delay between executors (seconds): ", "prompt_on_new": True},
    )

    # Rate conversion
    rate_connector: str = "binance"
    quote_conversion_asset: str = "USDT"

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets.add_or_update(self.global_exchange.connector_name, self.global_exchange.trading_pair)
        markets.add_or_update(self.krn_exchange.connector_name, self.krn_exchange.trading_pair)
        return markets


class KrnArbController(ControllerBase):
    def __init__(self, config: KrnArbControllerConfig, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)
        self._imbalance = 0
        self._last_buy_closed_ts = 0.0
        self._last_sell_closed_ts = 0.0
        self._active_buy_count = 0
        self._active_sell_count = 0
        self.base_asset = self.config.global_exchange.trading_pair.split("-")[0]

    async def update_processed_data(self):
        pass

    def determine_executor_actions(self) -> List[ExecutorAction]:
        self._update_stats()
        actions: List[ExecutorAction] = []
        now = self.market_data_provider.time()

        # Guard: imbalance and cooldown
        if abs(self._imbalance) >= self.config.max_executors_imbalance:
            return actions
        if (self._last_buy_closed_ts + self.config.delay_between_executors > now
                or self._last_sell_closed_ts + self.config.delay_between_executors > now):
            return actions

        # Buy on global, sell on KRN (positive kimchi premium)
        if self._active_buy_count == 0:
            action = self._create_arb_action(
                buying=self.config.global_exchange,
                selling=self.config.krn_exchange,
            )
            if action:
                actions.append(action)

        # Buy on KRN, sell on global (negative kimchi premium)
        if self._active_sell_count == 0:
            action = self._create_arb_action(
                buying=self.config.krn_exchange,
                selling=self.config.global_exchange,
            )
            if action:
                actions.append(action)

        return actions

    def _create_arb_action(
        self, buying: ConnectorPair, selling: ConnectorPair
    ) -> Optional[CreateExecutorAction]:
        rate = self.market_data_provider.get_rate(
            f"{self.base_asset}-{self.config.quote_conversion_asset}"
        )
        if rate is None or rate <= Decimal("0"):
            return None
        amount = self.market_data_provider.quantize_order_amount(
            buying.connector_name,
            buying.trading_pair,
            self.config.total_amount_quote / rate,
        )
        if amount == Decimal("0"):
            return None
        return CreateExecutorAction(
            executor_config=ArbitrageExecutorConfig(
                timestamp=self.market_data_provider.time(),
                buying_market=buying,
                selling_market=selling,
                order_amount=amount,
                min_profitability=self.config.min_profitability,
                controller_id=self.config.id,
            ),
            controller_id=self.config.id,
        )

    def _update_stats(self):
        closed = [e for e in self.executors_info if e.is_done]
        active = [e for e in self.executors_info if not e.is_done]
        buy_closed = [e for e in closed if e.config.buying_market == self.config.global_exchange]
        sell_closed = [e for e in closed if e.config.buying_market == self.config.krn_exchange]
        self._imbalance = len(buy_closed) - len(sell_closed)
        self._active_buy_count = len([e for e in active if e.config.buying_market == self.config.global_exchange])
        self._active_sell_count = len([e for e in active if e.config.buying_market == self.config.krn_exchange])
        if buy_closed:
            self._last_buy_closed_ts = max(e.close_timestamp or 0 for e in buy_closed)
        if sell_closed:
            self._last_sell_closed_ts = max(e.close_timestamp or 0 for e in sell_closed)

    def to_format_status(self) -> List[str]:
        return [
            f"  Imbalance: {self._imbalance}",
            f"  Active buy: {self._active_buy_count}, sell: {self._active_sell_count}",
        ]
