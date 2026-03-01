from decimal import Decimal
from typing import Dict, List, Set, Tuple

from pydantic import Field, field_validator, model_validator

from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import MarketDict, OrderType, PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction


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
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets.add_or_update(self.maker_connector, self.maker_trading_pair)
        markets.add_or_update(self.taker_connector, self.taker_trading_pair)
        return markets


class UpbitBithumbXemmController(ControllerBase):
    def __init__(self, config: UpbitBithumbXemmControllerConfig, *args, **kwargs):
        self.config = config
        self._last_creation_timestamp: Dict[TradeType, float] = {TradeType.BUY: 0.0, TradeType.SELL: 0.0}
        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        pass

    def determine_executor_actions(self) -> List[ExecutorAction]:
        if not self.market_data_provider.ready:
            return []

        mid_price = self.market_data_provider.get_price_by_type(
            self.config.maker_connector,
            self.config.maker_trading_pair,
            PriceType.MidPrice,
        )
        if mid_price is None or mid_price <= Decimal("0"):
            return []

        inventory_delta = self._inventory_delta()
        allow_buy, allow_sell = self._allowed_sides(inventory_delta)

        active_executors = self.filter_executors(self.executors_info, lambda executor: not executor.is_done)
        active_buy_targets = {
            executor.config.target_profitability
            for executor in active_executors
            if executor.side == TradeType.BUY and hasattr(executor.config, "target_profitability")
        }
        active_sell_targets = {
            executor.config.target_profitability
            for executor in active_executors
            if executor.side == TradeType.SELL and hasattr(executor.config, "target_profitability")
        }

        active_buy_count = len([executor for executor in active_executors if executor.side == TradeType.BUY])
        active_sell_count = len([executor for executor in active_executors if executor.side == TradeType.SELL])

        actions: List[ExecutorAction] = []
        if allow_buy:
            actions.extend(
                self._create_level_actions(
                    side=TradeType.BUY,
                    levels=self.config.buy_levels_targets_amount,
                    active_targets=active_buy_targets,
                    active_count=active_buy_count,
                    mid_price=mid_price,
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
                )
            )

        return actions

    def _create_level_actions(
        self,
        side: TradeType,
        levels: List[List[Decimal]],
        active_targets: Set[Decimal],
        active_count: int,
        mid_price: Decimal,
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

            action = CreateExecutorAction(
                executor_config=XEMMExecutorConfig(
                    timestamp=now,
                    buying_market=buying_market,
                    selling_market=selling_market,
                    maker_side=side,
                    order_amount=base_amount,
                    min_profitability=min_profitability,
                    target_profitability=target_profitability,
                    max_profitability=max_profitability,
                    taker_order_type=self.config.taker_order_type,
                    taker_slippage_buffer_bps=self.config.taker_slippage_buffer_bps,
                    taker_order_max_age_seconds=self.config.taker_order_max_age_seconds,
                    taker_max_retries=self.config.taker_max_retries,
                    taker_fallback_to_market=self.config.taker_fallback_to_market,
                    controller_id=self.config.id,
                ),
                controller_id=self.config.id,
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

    def _inventory_delta(self) -> Decimal:
        base_asset, _ = split_hb_trading_pair(self.config.maker_trading_pair)
        maker_base = self.market_data_provider.connectors[self.config.maker_connector].get_available_balance(base_asset)
        taker_base = self.market_data_provider.connectors[self.config.taker_connector].get_available_balance(base_asset)
        maker_base = maker_base if maker_base is not None else Decimal("0")
        taker_base = taker_base if taker_base is not None else Decimal("0")
        return maker_base - taker_base - self.config.inventory_target_base

    def _allowed_sides(self, inventory_delta: Decimal) -> Tuple[bool, bool]:
        max_skew = self.config.max_inventory_skew_base
        if inventory_delta > max_skew:
            return False, True
        if inventory_delta < -max_skew:
            return True, False
        return True, True

    def to_format_status(self) -> List[str]:
        inventory_delta = self._inventory_delta()
        allow_buy, allow_sell = self._allowed_sides(inventory_delta)
        return [
            f"  Pair: maker={self.config.maker_connector} {self.config.maker_trading_pair} | taker={self.config.taker_connector} {self.config.taker_trading_pair}",
            f"  Taker order type: {self.config.taker_order_type.name}, slippage_buffer_bps={self.config.taker_slippage_buffer_bps}",
            f"  Taker fallback to market: {self.config.taker_fallback_to_market}",
            f"  Inventory delta (maker-taker-target): {inventory_delta}",
            f"  Side gating -> BUY:{allow_buy} SELL:{allow_sell}",
        ]
