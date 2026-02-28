# hummingbot/controllers/generic/krn_xemm_controller.py
"""
Korean Exchange XEMM Controller
Maker: Upbit (or other KRW exchange), Taker: Binance (hedge).
Simplified single-level XEMM for mid-frequency trading.
"""
from decimal import Decimal
from typing import List

from pydantic import Field

from hummingbot.core.data_type.common import MarketDict, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction, ExecutorAction


class KrnXemmControllerConfig(ControllerConfigBase):
    controller_name: str = "krn_xemm_controller"
    controller_type: str = "generic"
    candles_config: List[CandlesConfig] = []

    # Maker: Korean exchange (where we place limit orders)
    maker_connector: str = "upbit"
    maker_trading_pair: str = "BTC-KRW"

    # Taker: Global exchange (where we hedge)
    taker_connector: str = "binance"
    taker_trading_pair: str = "BTC-USDT"

    # Profitability
    min_profitability: Decimal = Field(
        default=Decimal("0.002"),
        json_schema_extra={"prompt": "Min profitability: ", "prompt_on_new": True, "is_updatable": True},
    )
    target_profitability: Decimal = Field(
        default=Decimal("0.005"),
        json_schema_extra={"prompt": "Target profitability: ", "prompt_on_new": True, "is_updatable": True},
    )
    max_profitability: Decimal = Field(
        default=Decimal("0.01"),
        json_schema_extra={"is_updatable": True},
    )

    # Risk controls
    max_executors_per_side: int = Field(
        default=1,
        json_schema_extra={"is_updatable": True},
    )

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets.add_or_update(self.maker_connector, self.maker_trading_pair)
        markets.add_or_update(self.taker_connector, self.taker_trading_pair)
        return markets


class KrnXemmController(ControllerBase):
    def __init__(self, config: KrnXemmControllerConfig, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        pass

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []
        active = self.filter_executors(self.executors_info, lambda e: not e.is_done)
        active_buys = [e for e in active if e.side == TradeType.BUY]
        active_sells = [e for e in active if e.side == TradeType.SELL]

        maker = ConnectorPair(
            connector_name=self.config.maker_connector,
            trading_pair=self.config.maker_trading_pair,
        )
        taker = ConnectorPair(
            connector_name=self.config.taker_connector,
            trading_pair=self.config.taker_trading_pair,
        )

        half_quote = self.config.total_amount_quote / Decimal("2")

        # Buy side: maker buys, taker sells (hedge)
        if len(active_buys) < self.config.max_executors_per_side:
            actions.append(CreateExecutorAction(
                executor_config=XEMMExecutorConfig(
                    timestamp=self.market_data_provider.time(),
                    buying_market=maker,
                    selling_market=taker,
                    maker_side=TradeType.BUY,
                    order_amount=half_quote,
                    min_profitability=self.config.min_profitability,
                    target_profitability=self.config.target_profitability,
                    max_profitability=self.config.max_profitability,
                    controller_id=self.config.id,
                ),
                controller_id=self.config.id,
            ))

        # Sell side: maker sells, taker buys (hedge)
        if len(active_sells) < self.config.max_executors_per_side:
            actions.append(CreateExecutorAction(
                executor_config=XEMMExecutorConfig(
                    timestamp=self.market_data_provider.time(),
                    buying_market=taker,
                    selling_market=maker,
                    maker_side=TradeType.SELL,
                    order_amount=half_quote,
                    min_profitability=self.config.min_profitability,
                    target_profitability=self.config.target_profitability,
                    max_profitability=self.config.max_profitability,
                    controller_id=self.config.id,
                ),
                controller_id=self.config.id,
            ))

        return actions

    def to_format_status(self) -> List[str]:
        active = self.filter_executors(self.executors_info, lambda e: not e.is_done)
        return [
            f"  Active executors: {len(active)}",
            f"  Maker: {self.config.maker_connector} {self.config.maker_trading_pair}",
            f"  Taker: {self.config.taker_connector} {self.config.taker_trading_pair}",
        ]
