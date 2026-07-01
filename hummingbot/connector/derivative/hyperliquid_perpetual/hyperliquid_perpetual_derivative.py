import asyncio
import hashlib
import time
from decimal import Decimal
from typing import Any, AsyncIterable, Dict, List, Literal, Mapping, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.hyperliquid_perpetual import (
    hyperliquid_perpetual_constants as CONSTANTS,
    hyperliquid_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.hyperliquid_perpetual.hyperliquid_perpetual_api_order_book_data_source import (
    HyperliquidPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.hyperliquid_perpetual.hyperliquid_perpetual_auth import HyperliquidPerpetualAuth
from hummingbot.connector.derivative.hyperliquid_perpetual.hyperliquid_perpetual_batch import (
    HyperliquidBatchCancelRequest,
    HyperliquidBatchOrderRequest,
)
from hummingbot.connector.derivative.hyperliquid_perpetual.hyperliquid_perpetual_user_stream_data_source import (
    HyperliquidPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, get_new_client_order_id
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

bpm_logger = None


class HyperliquidPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils

    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 12.0

    def __init__(
            self,
            balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
            rate_limits_share_pct: Decimal = Decimal("100"),
            hyperliquid_perpetual_secret_key: str = None,
            hyperliquid_perpetual_address: str = None,
            use_vault: bool = False,
            hyperliquid_perpetual_mode: Literal["arb_wallet", "api_wallet"] = "arb_wallet",
            trading_pairs: Optional[List[str]] = None,
            trading_required: bool = True,
            domain: str = CONSTANTS.DOMAIN,
            enable_hip3_markets: bool = True,
    ):
        self.hyperliquid_perpetual_address = hyperliquid_perpetual_address
        self.hyperliquid_perpetual_secret_key = hyperliquid_perpetual_secret_key
        self._use_vault = use_vault
        self._connection_mode = hyperliquid_perpetual_mode
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._enable_hip3_markets = enable_hip3_markets
        self._position_mode = None
        self._last_trade_history_timestamp = None
        self.coin_to_asset: Dict[str, int] = {}  # Maps coin name to asset ID for ALL markets
        self._exchange_info_dex_to_symbol = bidict({})
        self._dex_markets: List[Dict] = []  # Store HIP-3 DEX market info separately
        self._is_hip3_market: Dict[str, bool] = {}  # Track which coins are HIP-3
        self._trading_pair_to_exchange_symbol: Dict[str, str] = {}
        self._user_abstraction_mode: Optional[str] = None
        self._user_abstraction_mode_ts: Optional[float] = None
        # JEP-209: latch so the unified-account-safe balance fallback warns once per degraded
        # episode (abstraction mode unresolved) instead of every balance poll.
        self._abstraction_fallback_warned: bool = False
        self._hyperliquid_batch_orders_disabled: bool = False
        self._hyperliquid_batch_reconcile_order_ids = set()
        # Builder code (HGP-87). Fee starts at 0 and is resolved at startup (_initialize_builder_fee).
        self._builder_address: str = CONSTANTS.FOUNDATION_BUILDER_ADDRESS.lower()
        self._builder_fee_tenths_bps: int = 0
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def name(self) -> str:
        # Note: domain here refers to the entire exchange name. i.e. hyperliquid_perpetual or hyperliquid_perpetual_testnet
        return self._domain

    @property
    def authenticator(self) -> Optional[HyperliquidPerpetualAuth]:
        if self._trading_required:
            return HyperliquidPerpetualAuth(
                self.hyperliquid_perpetual_address,
                self.hyperliquid_perpetual_secret_key,
                self._use_vault
            )
        return None

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    def _set_trading_pair_symbol_map(self, trading_pair_and_symbol_map: Optional[Mapping[str, str]]):
        mapping = trading_pair_and_symbol_map or {}
        self._trading_pair_to_exchange_symbol = {
            trading_pair: exchange_symbol for exchange_symbol, trading_pair in mapping.items()
        }
        super()._set_trading_pair_symbol_map(trading_pair_and_symbol_map)

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PING_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 120

    async def _make_network_check_request(self):
        await self._api_post(path_url=self.check_network_request_path, data={"type": CONSTANTS.META_INFO})

    async def start_network(self):
        await super().start_network()
        if self._trading_required:
            await self._initialize_builder_fee()

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector
        """
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def supported_position_modes(self):
        """
        This method needs to be overridden to provide the accurate information depending on the exchange.
        """
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        return False

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth)

    async def _make_trading_rules_request(self) -> Any:
        exchange_info = await self._api_post(path_url=self.trading_rules_request_path,
                                             data={"type": CONSTANTS.ASSET_CONTEXT_TYPE})
        return exchange_info

    async def _make_trading_pairs_request(self) -> Any:
        exchange_info = await self._api_post(path_url=self.trading_pairs_request_path,
                                             data={"type": CONSTANTS.ASSET_CONTEXT_TYPE})
        return exchange_info

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    def quantize_order_price(self, trading_pair: str, price: Decimal) -> Decimal:
        """
        Applies trading rule to quantize order price.
        """
        d_price = Decimal(round(float(f"{price:.5g}"), 6))
        return d_price

    @staticmethod
    def _is_all_perp_metas_response(exchange_info_dex: Any) -> bool:
        if not isinstance(exchange_info_dex, list) or len(exchange_info_dex) == 0:
            return False
        first_non_null = next((entry for entry in exchange_info_dex if entry is not None), None)
        return (
            (
                isinstance(first_non_null, list)
                and len(first_non_null) >= 1
                and isinstance(first_non_null[0], dict)
                and "universe" in first_non_null[0]
            )
            or (
                isinstance(first_non_null, dict)
                and "universe" in first_non_null
            )
        )

    def _infer_hip3_dex_name(self, perp_meta_list: List[Dict[str, Any]]) -> Optional[str]:
        dex_names = set()
        for perp_meta in perp_meta_list:
            if not isinstance(perp_meta, dict):
                continue
            coin_name = str(perp_meta.get("name", ""))
            if ":" in coin_name:
                dex_names.add(coin_name.split(":", 1)[0])

        if len(dex_names) > 1:
            self.logger().warning(f"Unexpected multi-prefix allPerpMetas entry: {sorted(dex_names)}")
            return None
        return next(iter(dex_names)) if dex_names else None

    def _parse_all_perp_metas_response(self, all_perp_metas: List[Any]) -> List[Dict[str, Any]]:
        dex_markets: List[Dict[str, Any]] = []

        for dex_entry in all_perp_metas:
            if isinstance(dex_entry, dict):
                meta_payload = dex_entry
                asset_ctx_list = []
            elif isinstance(dex_entry, list) and len(dex_entry) >= 1:
                meta_payload = dex_entry[0] if isinstance(dex_entry[0], dict) else {}
                asset_ctx_list = dex_entry[1] if len(dex_entry) > 1 and isinstance(dex_entry[1], list) else []
            else:
                continue
            perp_meta_list = meta_payload.get("universe", []) if isinstance(meta_payload, dict) else []

            if not perp_meta_list:
                continue

            dex_name = self._infer_hip3_dex_name(perp_meta_list)
            if dex_name is None:
                # allPerpMetas includes the base perp dex (no "dex:COIN" names). Base markets are fetched separately.
                continue

            if len(perp_meta_list) != len(asset_ctx_list):
                if len(asset_ctx_list) > 0:
                    self.logger().warning(f"WARN: perpMeta and assetCtxs length mismatch for dex={dex_name}")

            dex_info = dict(meta_payload)
            dex_info["name"] = dex_name
            dex_info["perpMeta"] = perp_meta_list
            dex_info["assetCtxs"] = asset_ctx_list
            dex_markets.append(dex_info)

        return dex_markets

    @staticmethod
    def _has_complete_asset_ctxs(dex_info: Dict[str, Any]) -> bool:
        perp_meta_list = dex_info.get("perpMeta", []) or []
        asset_ctx_list = dex_info.get("assetCtxs", []) or []
        return len(perp_meta_list) > 0 and len(perp_meta_list) == len(asset_ctx_list)

    @staticmethod
    def _extract_asset_ctxs_from_meta_and_ctxs_response(response: Any) -> Optional[List[Dict[str, Any]]]:
        if (
            isinstance(response, list)
            and len(response) >= 2
            and isinstance(response[0], dict)
            and "universe" in response[0]
            and isinstance(response[1], list)
        ):
            return response[1]
        return None

    async def _hydrate_dex_markets_asset_ctxs(self, dex_markets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        hydrated_markets: List[Dict[str, Any]] = []

        for dex_info in dex_markets:
            if not isinstance(dex_info, dict):
                continue
            if self._has_complete_asset_ctxs(dex_info):
                hydrated_markets.append(dex_info)
                continue

            dex_name = dex_info.get("name", "")
            if not dex_name:
                hydrated_markets.append(dex_info)
                continue

            try:
                dex_meta_and_ctxs = await self._api_post(
                    path_url=self.trading_pairs_request_path,
                    data={"type": CONSTANTS.ASSET_CONTEXT_TYPE, "dex": dex_name},
                )
                asset_ctxs = self._extract_asset_ctxs_from_meta_and_ctxs_response(dex_meta_and_ctxs)
                if asset_ctxs is None:
                    self.logger().warning(
                        f"Unexpected metaAndAssetCtxs response shape for dex={dex_name}; skipping HIP-3 asset contexts."
                    )
                    hydrated_markets.append(dex_info)
                    continue
                updated_dex_info = dict(dex_info)
                updated_dex_info["assetCtxs"] = asset_ctxs
                if not self._has_complete_asset_ctxs(updated_dex_info):
                    self.logger().warning(f"WARN: perpMeta and assetCtxs length mismatch for dex={dex_name}")
                hydrated_markets.append(updated_dex_info)
            except Exception:
                self.logger().warning(
                    f"Error fetching metaAndAssetCtxs for dex={dex_name}; skipping HIP-3 asset contexts.",
                    exc_info=True,
                )
                hydrated_markets.append(dex_info)

        return hydrated_markets

    def _iter_hip3_merged_markets(self, dex_markets: Optional[List[Dict[str, Any]]] = None):
        source_dex_markets = dex_markets if dex_markets is not None else (self._dex_markets or [])
        for dex_info in source_dex_markets:
            if not isinstance(dex_info, dict):
                continue

            perp_meta_list = dex_info.get("perpMeta", []) or []
            asset_ctx_list = dex_info.get("assetCtxs", []) or []

            for perp_meta, asset_ctx in zip(perp_meta_list, asset_ctx_list):
                if not isinstance(perp_meta, dict):
                    continue
                if ":" not in str(perp_meta.get("name", "")):
                    continue
                if not isinstance(asset_ctx, dict):
                    continue
                yield {**perp_meta, **asset_ctx}

    def _configured_hip3_dex_names(self) -> set:
        """HIP-3 builder DEX names referenced by configured trading pairs (the prefix
        before ':' in e.g. 'XYZ:SMSN-USD', lowercased). Empty if no HIP-3 pairs are
        configured (then the original all-DEX behavior is preserved)."""
        names = set()
        for tp in (getattr(self, "_trading_pairs", None) or []):
            base = str(tp).split("-")[0]
            if ":" in base:
                names.add(base.split(":", 1)[0].lower())
        return names

    def _filter_dex_markets_to_configured(self, dex_markets):
        """Restrict HIP-3 asset-context hydration to the DEX(es) of configured trading
        pairs. Polling every builder DEX (xyz + km/abcd/cash/para/…) is a request-volume
        blowup that saturates a rate-limited RPC and starves balance/position calls; we
        only need our own DEX's contexts. No-op when no HIP-3 pairs are configured."""
        needed = self._configured_hip3_dex_names()
        if not needed:
            return dex_markets
        return [d for d in dex_markets if str(d.get("name", "")).lower() in needed]

    async def _fetch_and_cache_hip3_market_data(self):
        self._dex_markets = []

        if not self._enable_hip3_markets:
            return []

        exchange_info_dex = await self._api_post(
            path_url=self.trading_pairs_request_path,
            data={"type": CONSTANTS.DEX_ASSET_CONTEXT_TYPE},
        )

        if not isinstance(exchange_info_dex, list):
            return []

        exchange_info_dex = [info for info in exchange_info_dex if info is not None]

        # allPerpMetas may return either meta-only entries or [[meta, assetCtxs], ...] entries.
        if self._is_all_perp_metas_response(exchange_info_dex):
            dex_markets = self._parse_all_perp_metas_response(exchange_info_dex)
            # Only hydrate asset contexts for the DEX(es) we actually trade — avoids
            # polling/retrying every builder DEX, which saturates a rate-limited RPC.
            dex_markets = self._filter_dex_markets_to_configured(dex_markets)
            dex_markets = await self._hydrate_dex_markets_asset_ctxs(dex_markets)
            self._dex_markets = dex_markets
            return dex_markets
        self.logger().warning(
            "Unexpected allPerpMetas response shape for HIP-3 markets; expected list of dex meta payloads."
        )
        return []

    async def _update_trading_rules(self):
        exchange_info = await self._api_post(path_url=self.trading_rules_request_path,
                                             data={"type": CONSTANTS.ASSET_CONTEXT_TYPE})

        # Only fetch HIP-3/DEX markets if enabled
        exchange_info_dex = []
        if self._enable_hip3_markets:
            exchange_info_dex = await self._fetch_and_cache_hip3_market_data()

        # Store DEX info separately for reference, don't extend universe
        self._dex_markets = exchange_info_dex
        # Initialize symbol map BEFORE formatting trading rules (needed for symbol lookup)
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        # Keep base universe unchanged - only use validated perpetual indices
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule

    async def _initialize_trading_pair_symbol_map(self):
        try:
            exchange_info = await self._api_post(
                path_url=self.trading_pairs_request_path,
                data={"type": CONSTANTS.ASSET_CONTEXT_TYPE})

            # Only fetch HIP-3/DEX markets if enabled
            exchange_info_dex = []
            if self._enable_hip3_markets:
                exchange_info_dex = await self._fetch_and_cache_hip3_market_data()

            # Store DEX info separately for reference
            self._dex_markets = exchange_info_dex
            # Initialize trading pairs from both sources
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return HyperliquidPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return HyperliquidPerpetualUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        res: List[Dict[str, str]] = []

        # ===== Fetch main perp info =====
        exchange_info = await self._api_post(
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_URL,
            data={"type": CONSTANTS.ASSET_CONTEXT_TYPE},
        )

        perp_universe = exchange_info[0].get("universe", [])
        perp_asset_ctxs = exchange_info[1]

        if len(perp_universe) != len(perp_asset_ctxs):
            self.logger().info("WARN: perpMeta and assetCtxs length mismatch")

        # Merge perpetual markets
        for meta, ctx in zip(perp_asset_ctxs, perp_universe):
            merged = {**meta, **ctx}
            res.append({
                "symbol": merged.get("name"),
                "price": merged.get("markPx"),
            })

        # ===== Fetch DEX / HIP-3 markets (only if enabled) =====
        if self._enable_hip3_markets:
            dex_markets = await self._fetch_and_cache_hip3_market_data()
            for market in self._iter_hip3_merged_markets(dex_markets=dex_markets):
                res.append({
                    "symbol": market.get("name"),
                    "price": market.get("markPx"),
                })

        return res

    async def _status_polling_loop_fetch_updates(self):
        await safe_gather(
            self._update_trade_history(),
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )

    async def _update_order_status(self):
        await self._update_orders()

    async def _update_lost_orders_status(self):
        await self._update_lost_orders()

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 position_action: PositionAction,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = is_maker or False
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        coin = symbol

        api_params = {
            "type": "cancel",
            "cancels": {
                "asset": self.coin_to_asset[coin],
                "cloid": order_id
            },
        }
        cancel_result = await self._api_post(
            path_url=CONSTANTS.CANCEL_ORDER_URL,
            data=api_params,
            is_auth_required=True)

        if cancel_result.get("status") == "err" or "error" in cancel_result["response"]["data"]["statuses"][0]:
            self.logger().debug(f"The order {order_id} does not exist on Hyperliquid Perpetuals. "
                                f"No cancelation needed.")
            await self._order_tracker.process_order_not_found(order_id)
            raise IOError(f'{cancel_result["response"]["data"]["statuses"][0]["error"]}')
        if "success" in cancel_result["response"]["data"]["statuses"][0]:
            return True
        return False

    # === Orders placing ===

    def _new_hyperliquid_client_order_id(self, is_buy: bool, trading_pair: str) -> str:
        order_id = get_new_client_order_id(
            is_buy=is_buy,
            trading_pair=trading_pair,
            hbot_order_id_prefix=self.client_order_id_prefix,
            max_id_len=self.client_order_id_max_length
        )
        md5 = hashlib.md5()
        md5.update(order_id.encode('utf-8'))
        return f"0x{md5.hexdigest()}"

    def _coin_from_trading_pair(self, trading_pair: str) -> str:
        exchange_symbol = self._trading_pair_to_exchange_symbol.get(trading_pair)
        if exchange_symbol is not None:
            return exchange_symbol
        return trading_pair.rsplit("-", 1)[0]

    def _validate_batch_order_requests(self, orders: List[HyperliquidBatchOrderRequest]) -> List[Dict[str, Any]]:
        if self._hyperliquid_batch_orders_disabled:
            raise ValueError("Hyperliquid batch order placement is disabled after an unsafe batch response.")
        if not orders:
            raise ValueError("Batch order request must contain at least one order.")
        if len(orders) > 8:
            raise ValueError(f"Hyperliquid batch order request too large: {len(orders)} > 8.")

        validated_orders: List[Dict[str, Any]] = []
        generated_order_ids = set()
        for order in orders:
            if order.order_type is not OrderType.LIMIT_MAKER:
                raise ValueError("Hyperliquid batch placement only supports LIMIT_MAKER orders.")
            trading_rule = self._trading_rules.get(order.trading_pair)
            if trading_rule is None:
                raise ValueError(f"Trading rules are not initialized for {order.trading_pair}.")
            coin = self._coin_from_trading_pair(order.trading_pair)
            if coin not in self.coin_to_asset:
                raise ValueError(f"Coin {coin} not found in coin_to_asset mapping.")

            quantized_amount = self.quantize_order_amount(order.trading_pair, order.amount)
            quantized_price = self.quantize_order_price(order.trading_pair, order.price)
            if quantized_amount < trading_rule.min_order_size:
                raise ValueError(
                    f"Order amount {order.amount} is lower than minimum order size "
                    f"{trading_rule.min_order_size} for the pair {order.trading_pair}."
                )
            notional_size = quantized_amount * quantized_price
            if notional_size < trading_rule.min_notional_size:
                raise ValueError(
                    f"Order notional {notional_size} is lower than minimum notional size "
                    f"{trading_rule.min_notional_size} for the pair {order.trading_pair}."
                )

            order_id = self._new_hyperliquid_client_order_id(
                is_buy=order.trade_type is TradeType.BUY,
                trading_pair=order.trading_pair,
            )
            if order_id in generated_order_ids:
                raise ValueError(f"Duplicate generated Hyperliquid cloid {order_id} in batch.")
            generated_order_ids.add(order_id)
            validated_orders.append({
                "client_order_id": order_id,
                "trading_pair": order.trading_pair,
                "coin": coin,
                "amount": quantized_amount,
                "price": quantized_price,
                "trade_type": order.trade_type,
                "order_type": order.order_type,
                "position_action": order.position_action,
                "metadata": order.metadata,
            })
        return validated_orders

    def batch_place_orders(self, orders: List[HyperliquidBatchOrderRequest]) -> List[str]:
        validated_orders = self._validate_batch_order_requests(orders)
        order_ids = [order["client_order_id"] for order in validated_orders]
        safe_ensure_future(self._execute_batch_place_orders(validated_orders))
        return order_ids

    def _validate_batch_cancel_requests(self, cancels: List[HyperliquidBatchCancelRequest]) -> List[Dict[str, Any]]:
        if not cancels:
            raise ValueError("Batch cancel request must contain at least one order.")
        if len(cancels) > 8:
            raise ValueError(f"Hyperliquid batch cancel request too large: {len(cancels)} > 8.")
        validated_cancels = []
        for cancel in cancels:
            tracked_order = self._order_tracker.fetch_tracked_order(cancel.client_order_id)
            if tracked_order is None:
                raise ValueError(f"Cannot batch cancel untracked order {cancel.client_order_id}.")
            coin = self._coin_from_trading_pair(cancel.trading_pair)
            if coin not in self.coin_to_asset:
                raise ValueError(f"Coin {coin} not found in coin_to_asset mapping.")
            validated_cancels.append({
                "client_order_id": cancel.client_order_id,
                "trading_pair": cancel.trading_pair,
                "coin": coin,
            })
        return validated_cancels

    def batch_cancel_by_cloid(self, cancels: List[HyperliquidBatchCancelRequest]) -> List[str]:
        validated_cancels = self._validate_batch_cancel_requests(cancels)
        order_ids = [cancel["client_order_id"] for cancel in validated_cancels]
        safe_ensure_future(self._execute_batch_cancel_by_cloid(validated_cancels))
        return order_ids

    async def _execute_batch_cancel_by_cloid(self, validated_cancels: List[Dict[str, Any]]) -> None:
        api_cancels = [
            {
                "asset": self.coin_to_asset[cancel["coin"]],
                "cloid": cancel["client_order_id"],
            }
            for cancel in validated_cancels
        ]
        try:
            cancel_result = await self._api_post(
                path_url=CONSTANTS.CANCEL_ORDER_URL,
                data={"type": "cancel", "cancels": api_cancels},
                is_auth_required=True,
            )
        except Exception:
            self.logger().exception(
                "Hyperliquid batch cancel transport failed; leaving orders tracked for bounded reconciliation."
            )
            await self._bounded_reconcile_client_order_ids(
                [cancel["client_order_id"] for cancel in validated_cancels]
            )
            return
        try:
            statuses = cancel_result["response"]["data"]["statuses"]
        except Exception:
            self.logger().exception(
                "Hyperliquid batch cancel returned malformed response; leaving orders tracked for bounded reconciliation."
            )
            await self._bounded_reconcile_client_order_ids(
                [cancel["client_order_id"] for cancel in validated_cancels]
            )
            return
        if len(statuses) != len(validated_cancels):
            self.logger().error(
                "Hyperliquid batch cancel returned %s statuses for %s submitted cancels; "
                "leaving orders tracked for normal reconciliation.",
                len(statuses),
                len(validated_cancels),
            )
            await self._bounded_reconcile_client_order_ids(
                [cancel["client_order_id"] for cancel in validated_cancels]
            )
            return
        # SAFETY: Hyperliquid documents batched order/cancel errors as a vector with the same
        # length as the batched request and exposes per-child statuses as an array. Keep positional
        # mapping isolated here; any shape drift above is diverted to bounded reconciliation.
        # https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses
        accepted_count = 0
        for cancel, status in zip(validated_cancels, statuses):
            client_order_id = cancel["client_order_id"]
            if status == "success" or (isinstance(status, dict) and status.get("success") is True):
                order_update = OrderUpdate(
                    client_order_id=client_order_id,
                    trading_pair=cancel["trading_pair"],
                    update_timestamp=self.current_timestamp,
                    new_state=OrderState.CANCELED,
                )
                self._order_tracker.process_order_update(order_update)
                accepted_count += 1
            elif isinstance(status, dict) and "error" in status and CONSTANTS.UNKNOWN_ORDER_MESSAGE in status["error"]:
                await self._order_tracker.process_order_not_found(client_order_id)
            else:
                self.logger().warning(
                    "Hyperliquid batch cancel for %s returned uncertain status %s; "
                    "leaving order tracked for bounded reconciliation.",
                    client_order_id,
                    status,
                )
                await self._bounded_reconcile_client_order_ids([client_order_id])
        self.logger().info(
            "Hyperliquid batch cancel accepted %s/%s orders",
            accepted_count,
            len(validated_cancels),
        )

    async def _execute_batch_place_orders(self, validated_orders: List[Dict[str, Any]]) -> None:
        api_orders = []
        for order in validated_orders:
            self.start_tracking_order(
                order_id=order["client_order_id"],
                exchange_order_id=None,
                trading_pair=order["trading_pair"],
                trade_type=order["trade_type"],
                price=order["price"],
                amount=order["amount"],
                order_type=order["order_type"],
                position_action=order["position_action"],
                metadata=order["metadata"],
            )
            api_orders.append({
                "asset": self.coin_to_asset[order["coin"]],
                "isBuy": order["trade_type"] is TradeType.BUY,
                "limitPx": float(order["price"]),
                "sz": float(order["amount"]),
                "reduceOnly": order["position_action"] == PositionAction.CLOSE,
                "orderType": {"limit": {"tif": "Alo"}},
                "cloid": order["client_order_id"],
            })

        api_params = {
            "type": "order",
            "grouping": "na",
            "orders": api_orders,
        }
        builder_field = self._build_builder_field()
        if builder_field is not None:
            api_params["builder"] = builder_field
        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.CREATE_ORDER_URL,
                data=api_params,
                is_auth_required=True,
            )
        except Exception:
            self.logger().exception(
                "Hyperliquid batch place transport failed; leaving orders tracked for bounded reconciliation."
            )
            await self._bounded_reconcile_client_order_ids(
                [order["client_order_id"] for order in validated_orders]
            )
            return
        try:
            statuses = order_result["response"]["data"]["statuses"]
        except Exception:
            self.logger().exception(
                "Hyperliquid batch place returned malformed response; leaving orders tracked for bounded reconciliation."
            )
            await self._bounded_reconcile_client_order_ids(
                [order["client_order_id"] for order in validated_orders]
            )
            return
        if len(statuses) != len(validated_orders):
            self.logger().error(
                "Hyperliquid batch place returned %s statuses for %s submitted orders; "
                "leaving orders tracked for normal reconciliation.",
                len(statuses),
                len(validated_orders),
            )
            await self._bounded_reconcile_client_order_ids(
                [order["client_order_id"] for order in validated_orders]
            )
            return

        # SAFETY: Hyperliquid documents batched order/cancel errors as a vector with the same
        # length as the batched request and exposes per-child statuses as an array. Keep positional
        # mapping isolated here; any shape drift above is diverted to bounded reconciliation.
        # https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses
        accepted_count = 0
        for order, status in zip(validated_orders, statuses):
            client_order_id = order["client_order_id"]
            if "error" in status:
                self._update_order_after_failure(
                    order_id=client_order_id,
                    trading_pair=order["trading_pair"],
                    exception=IOError(f"Error submitting order {client_order_id}: {status['error']}"),
                )
                continue
            if "filled" in status:
                self._hyperliquid_batch_orders_disabled = True
                self.logger().error(
                    "Hyperliquid batch LIMIT_MAKER order %s returned filled status %s; "
                    "disabling batch placement and reconciling the order.",
                    client_order_id,
                    status,
                )
                await self._bounded_reconcile_client_order_ids([client_order_id])
                continue
            order_data = status.get("resting")
            if order_data is None or "oid" not in order_data:
                self.logger().error(
                    "Unexpected Hyperliquid batch LIMIT_MAKER status for %s: %s; "
                    "leaving order tracked for bounded reconciliation.",
                    client_order_id,
                    status,
                )
                await self._bounded_reconcile_client_order_ids([client_order_id])
                continue
            order_update = OrderUpdate(
                client_order_id=client_order_id,
                exchange_order_id=str(order_data["oid"]),
                trading_pair=order["trading_pair"],
                update_timestamp=self.current_timestamp,
                new_state=OrderState.OPEN,
            )
            self._order_tracker.process_order_update(order_update)
            accepted_count += 1
        self.logger().info(
            "Hyperliquid batch place accepted %s/%s LIMIT_MAKER orders",
            accepted_count,
            len(validated_orders),
        )

    async def _bounded_reconcile_client_order_ids(self, client_order_ids: List[str]) -> None:
        unresolved_order_ids = [
            client_order_id
            for client_order_id in dict.fromkeys(client_order_ids)
            if client_order_id not in self._hyperliquid_batch_reconcile_order_ids
        ]
        if not unresolved_order_ids:
            return
        self._hyperliquid_batch_reconcile_order_ids.update(unresolved_order_ids)
        try:
            for attempt in range(3):
                next_unresolved_order_ids = []
                for client_order_id in unresolved_order_ids:
                    tracked_order = self._order_tracker.fetch_tracked_order(client_order_id)
                    if tracked_order is None or tracked_order.is_done:
                        continue
                    try:
                        order_update = await self._request_order_status(tracked_order)
                        self._order_tracker.process_order_update(order_update)
                    except Exception:
                        self.logger().warning(
                            "Hyperliquid bounded reconciliation attempt %s failed for %s.",
                            attempt + 1,
                            client_order_id,
                            exc_info=True,
                        )
                        next_unresolved_order_ids.append(client_order_id)
                unresolved_order_ids = next_unresolved_order_ids
                if not unresolved_order_ids:
                    return
                if attempt < 2:
                    await self._sleep(10.0)
            self.logger().warning(
                "Hyperliquid bounded reconciliation exhausted for %s; leaving orders for normal polling.",
                unresolved_order_ids,
            )
        finally:
            self._hyperliquid_batch_reconcile_order_ids.difference_update(client_order_ids)

    def buy(self,
            trading_pair: str,
            amount: Decimal,
            order_type=OrderType.LIMIT,
            price: Decimal = s_decimal_NaN,
            **kwargs) -> str:
        """
        Creates a promise to create a buy order using the parameters

        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price

        :return: the id assigned by the connector to the order (the client id)
        """
        hex_order_id = self._new_hyperliquid_client_order_id(is_buy=True, trading_pair=trading_pair)
        if order_type is OrderType.MARKET:
            reference_price = self.get_mid_price(trading_pair) if price.is_nan() else price
            price = self.quantize_order_price(trading_pair, reference_price * Decimal(1 + CONSTANTS.MARKET_ORDER_SLIPPAGE))

        safe_ensure_future(self._create_order(
            trade_type=TradeType.BUY,
            order_id=hex_order_id,
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            price=price,
            **kwargs))
        return hex_order_id

    def sell(self,
             trading_pair: str,
             amount: Decimal,
             order_type: OrderType = OrderType.LIMIT,
             price: Decimal = s_decimal_NaN,
             **kwargs) -> str:
        """
        Creates a promise to create a sell order using the parameters.
        :param trading_pair: the token pair to operate with
        :param amount: the order amount
        :param order_type: the type of order to create (MARKET, LIMIT, LIMIT_MAKER)
        :param price: the order price
        :return: the id assigned by the connector to the order (the client id)
        """
        hex_order_id = self._new_hyperliquid_client_order_id(is_buy=False, trading_pair=trading_pair)
        if order_type is OrderType.MARKET:
            reference_price = self.get_mid_price(trading_pair) if price.is_nan() else price
            price = self.quantize_order_price(trading_pair, reference_price * Decimal(1 - CONSTANTS.MARKET_ORDER_SLIPPAGE))

        safe_ensure_future(self._create_order(
            trade_type=TradeType.SELL,
            order_id=hex_order_id,
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            price=price,
            **kwargs))
        return hex_order_id

    async def _place_order(
            self,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            trade_type: TradeType,
            order_type: OrderType,
            price: Decimal,
            position_action: PositionAction = PositionAction.NIL,
            **kwargs,
    ) -> Tuple[str, float]:

        coin = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        param_order_type = {"limit": {"tif": "Gtc"}}
        if order_type is OrderType.LIMIT_MAKER:
            param_order_type = {"limit": {"tif": "Alo"}}
        if order_type is OrderType.MARKET:
            param_order_type = {"limit": {"tif": "Ioc"}}

        api_params = {
            "type": "order",
            "grouping": "na",
            "orders": {
                "asset": self.coin_to_asset[coin],
                "isBuy": True if trade_type is TradeType.BUY else False,
                "limitPx": float(price),
                "sz": float(amount),
                "reduceOnly": position_action == PositionAction.CLOSE,
                "orderType": param_order_type,
                "cloid": order_id,
            }
        }
        # Builder code (HGP-87): part of the signed action dict.
        builder_field = self._build_builder_field()
        if builder_field is not None:
            api_params["builder"] = builder_field
        order_result = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_URL,
            data=api_params,
            is_auth_required=True)
        if order_result.get("status") == "err":
            raise IOError(f"Error submitting order {order_id}: {order_result['response']}")
        else:
            o_order_result = order_result['response']["data"]["statuses"][0]
        if "error" in o_order_result:
            raise IOError(f"Error submitting order {order_id}: {o_order_result['error']}")
        o_data = o_order_result.get("resting") or o_order_result.get("filled")
        o_id = str(o_data["oid"])
        return (o_id, self.current_timestamp)

    # === Builder code support (HGP-87) ===

    @property
    def _is_testnet(self) -> bool:
        return self._domain == CONSTANTS.TESTNET_DOMAIN

    def _should_inject_builder(self) -> bool:
        """Builder attribution applies only on mainnet, non-vault orders — the venue rejects the
        builder field on vault and testnet orders."""
        if not CONSTANTS.BUILDER_SUPPORTED:
            return False
        if self._use_vault or self._is_testnet:
            return False
        return True

    def _build_builder_field(self) -> Optional[Dict[str, Any]]:
        """The ``{"b": <address>, "f": <tenths_of_bps>}`` order field, or None when omitted. Address
        is lowercased (the venue rejects mixed-case)."""
        if not self._should_inject_builder():
            return None
        return {"b": self._builder_address.lower(), "f": self._builder_fee_tenths_bps}

    async def _initialize_builder_fee(self) -> None:
        """Resolve the per-order builder fee once at startup as min(on-chain approved, hardcoded fee):
        the hardcoded fee if the user has approved this builder in Condor, 0 if not (or if the lookup
        fails)."""
        if not self._should_inject_builder():
            return
        try:
            approved_max_tenths_bps = int(await self._api_post(
                path_url=CONSTANTS.EXCHANGE_INFO_URL,
                data={
                    "type": CONSTANTS.MAX_BUILDER_FEE_TYPE,
                    "user": self.hyperliquid_perpetual_address,
                    "builder": self._builder_address,
                },
            ))
        except Exception:
            self.logger().exception(
                "Could not query the approved Hyperliquid builder fee; charging 0 bps this session."
            )
            self._builder_fee_tenths_bps = 0
            return
        self._builder_fee_tenths_bps = min(approved_max_tenths_bps, CONSTANTS.FOUNDATION_BUILDER_FEE_TENTHS_BPS)

    async def _update_trade_history(self):
        orders = list(self._order_tracker.all_fillable_orders.values())
        all_fillable_orders = self._order_tracker.all_fillable_orders_by_exchange_order_id
        all_fills_response = []
        if len(orders) > 0:
            try:
                all_fills_response = await self._api_post(
                    path_url=CONSTANTS.ACCOUNT_TRADE_LIST_URL,
                    data={
                        "type": CONSTANTS.TRADES_TYPE,
                        "user": self.hyperliquid_perpetual_address,
                    })
            except asyncio.CancelledError:
                raise
            except Exception as request_error:
                self.logger().warning(
                    f"Failed to fetch trade updates. Error: {request_error}",
                    exc_info=request_error,
                )
            for trade_fill in all_fills_response:
                self._process_trade_rs_event_message(order_fill=trade_fill, all_fillable_order=all_fillable_orders)

    def _process_trade_rs_event_message(self, order_fill: Dict[str, Any], all_fillable_order):
        exchange_order_id = str(order_fill.get("oid"))
        fillable_order = all_fillable_order.get(exchange_order_id)
        if fillable_order is not None:
            fee_asset = fillable_order.quote_asset

            position_action = PositionAction.OPEN if order_fill["dir"].split(" ")[0] == "Open" else PositionAction.CLOSE
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=position_action,
                percent_token=fee_asset,
                flat_fees=[TokenAmount(amount=Decimal(order_fill["fee"]), token=fee_asset)]
            )

            trade_update = TradeUpdate(
                trade_id=str(order_fill["tid"]),
                client_order_id=fillable_order.client_order_id,
                exchange_order_id=str(order_fill["oid"]),
                trading_pair=fillable_order.trading_pair,
                fee=fee,
                fill_base_amount=Decimal(order_fill["sz"]),
                fill_quote_amount=Decimal(order_fill["px"]) * Decimal(order_fill["sz"]),
                fill_price=Decimal(order_fill["px"]),
                fill_timestamp=order_fill["time"] * 1e-3,
            )

            self._order_tracker.process_trade_update(trade_update)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        # Use _update_trade_history instead
        pass

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        try:
            raise error
        except (asyncio.TimeoutError, KeyError):
            self.logger().debug(
                f"Tracked order {order.client_order_id} does not have an exchange id. "
                f"Attempting fetch in next polling interval."
            )
            await self._order_tracker.process_order_not_found(order.client_order_id)
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            self.logger().warning(
                f"Error fetching status update for the active order {order.client_order_id}: {request_error}.",
            )
            self.logger().debug(
                f"Order {order.client_order_id} not found counter: {self._order_tracker._order_not_found_records.get(order.client_order_id, 0)}")
            await self._order_tracker.process_order_not_found(order.client_order_id)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        client_order_id = tracked_order.client_order_id
        try:
            if tracked_order.exchange_order_id:
                exchange_order_id = tracked_order.exchange_order_id
            else:
                exchange_order_id = await tracked_order.get_exchange_order_id()
        except asyncio.TimeoutError:
            exchange_order_id = None
        order_update = await self._api_post(
            path_url=CONSTANTS.ORDER_URL,
            data={
                "type": CONSTANTS.ORDER_STATUS_TYPE,
                "user": self.hyperliquid_perpetual_address,
                "oid": int(exchange_order_id) if exchange_order_id else client_order_id
            })
        current_state = order_update["order"]["status"]
        _exchange_order_id = str(tracked_order.exchange_order_id) if tracked_order.exchange_order_id else str(
            order_update["order"]["order"]["oid"])
        _order_update: OrderUpdate = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=order_update["order"]["order"]["timestamp"] * 1e-3,
            new_state=CONSTANTS.ORDER_STATE[current_state],
            client_order_id=order_update["order"]["order"]["cloid"] or client_order_id,
            exchange_order_id=_exchange_order_id,
        )
        return _order_update

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Hyperliquid. Check API key and network connection.",
                )
                await self._sleep(1.0)

    async def _user_stream_event_listener(self):
        """
        Listens to messages from _user_stream_tracker.user_stream queue.
        Traders, Orders, and Balance updates from the WS.
        """
        user_channels = [
            CONSTANTS.USER_ORDERS_ENDPOINT_NAME,
            CONSTANTS.USEREVENT_ENDPOINT_NAME,
        ]
        async for event_message in self._iter_user_event_queue():
            try:
                if isinstance(event_message, dict):
                    channel: str = event_message.get("channel", None)
                    results = event_message.get("data", None)
                elif event_message is asyncio.CancelledError:
                    raise asyncio.CancelledError
                else:
                    raise Exception(event_message)
                if channel not in user_channels:
                    self.logger().error(
                        f"Unexpected message in user stream: {event_message}.", exc_info=True)
                    continue
                if channel == CONSTANTS.USER_ORDERS_ENDPOINT_NAME:
                    for order_msg in results:
                        self._process_order_message(order_msg)
                elif channel == CONSTANTS.USEREVENT_ENDPOINT_NAME:
                    if "fills" in results:
                        for trade_msg in results["fills"]:
                            await self._process_trade_message(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _process_trade_message(self, trade: Dict[str, Any], client_order_id: Optional[str] = None):
        """
        Updates in-flight order and trigger order filled event for trade message received. Triggers order completed
        event if the total executed amount equals to the specified order amount.
        Example Trade:
        """
        exchange_order_id = str(trade.get("oid", ""))
        tracked_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)

        if tracked_order is None:
            all_orders = self._order_tracker.all_fillable_orders
            for k, v in all_orders.items():
                await v.get_exchange_order_id()
            _cli_tracked_orders = [o for o in all_orders.values() if exchange_order_id == o.exchange_order_id]
            if not _cli_tracked_orders:
                self.logger().debug(f"Ignoring trade message with id {client_order_id}: not in in_flight_orders.")
                return
            tracked_order = _cli_tracked_orders[0]
        trading_pair_base_coin = tracked_order.base_asset
        base = trade["coin"]
        if base.upper() == trading_pair_base_coin:
            position_action = PositionAction.OPEN if trade["dir"].split(" ")[0] == "Open" else PositionAction.CLOSE
            fee_asset = tracked_order.quote_asset
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=position_action,
                percent_token=fee_asset,
                flat_fees=[TokenAmount(amount=Decimal(trade["fee"]), token=fee_asset)]
            )
            trade_update: TradeUpdate = TradeUpdate(
                trade_id=str(trade["tid"]),
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=str(trade["oid"]),
                trading_pair=tracked_order.trading_pair,
                fill_timestamp=trade["time"] * 1e-3,
                fill_price=Decimal(trade["px"]),
                fill_base_amount=Decimal(trade["sz"]),
                fill_quote_amount=Decimal(trade["px"]) * Decimal(trade["sz"]),
                fee=fee,
            )
            self._order_tracker.process_trade_update(trade_update)

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Updates in-flight order and triggers cancelation or failure event if needed.

        :param order_msg: The order response from either REST or web socket API (they are of the same format)

        Example Order:
        """
        client_order_id = str(order_msg["order"].get("cloid", ""))
        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
        if not tracked_order:
            self.logger().debug(f"Ignoring order message with id {client_order_id}: not in in_flight_orders.")
            return
        current_state = order_msg["status"]
        tracked_order.update_exchange_order_id(str(order_msg["order"]["oid"]))
        order_update: OrderUpdate = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=order_msg["statusTimestamp"] * 1e-3,
            new_state=CONSTANTS.ORDER_STATE[current_state],
            client_order_id=order_msg["order"]["cloid"],
            exchange_order_id=str(order_msg["order"]["oid"]),
        )
        self._order_tracker.process_order_update(order_update=order_update)

    async def _format_trading_rules(self, exchange_info_dict: List) -> List[TradingRule]:
        """
        Queries the necessary API endpoint and initialize the TradingRule object for each trading pair being traded.

        Parameters
        ----------
        exchange_info_dict:
            Trading rules dictionary response from the exchange
        """
        # Build coin_to_asset mapping ONLY for base perpetuals (not DEX markets)
        self.coin_to_asset = {asset_info["name"]: asset for (asset, asset_info) in
                              enumerate(exchange_info_dict[0]["universe"])}
        self._is_hip3_market = {}

        # Map base perpetual markets only (indices match universe array)
        for asset_index, asset_info in enumerate(exchange_info_dict[0]["universe"]):
            is_perpetual = "szDecimals" in asset_info
            if is_perpetual and not asset_info.get("isDelisted", False):
                self.coin_to_asset[asset_info["name"]] = asset_index
                self._is_hip3_market[asset_info["name"]] = False

        # Map HIP-3 DEX markets with their actual asset IDs for order placement
        # According to Hyperliquid SDK: builder-deployed perp dexs start at 110000
        # Each DEX gets an offset of 10000 (first=110000, second=120000, etc.)
        perp_dex_to_offset = {"": 0}
        perp_dexs = self._dex_markets if self._dex_markets is not None else []
        for i, perp_dex in enumerate(perp_dexs):
            if perp_dex is not None:
                # builder-deployed perp dexs start at 110000
                perp_dex_to_offset[perp_dex["name"]] = 110000 + i * 10000

        for dex_info in perp_dexs:
            if dex_info is None:
                continue
            dex_name = dex_info.get("name", "")
            base_asset_id = perp_dex_to_offset.get(dex_name, 0)

            # Use perpMeta (universe from meta endpoint) with enumerate for correct indices
            # The position in the array IS the index (no explicit index field in API response)
            perp_meta_list = dex_info.get("perpMeta", []) or []
            for asset_index, perp_meta in enumerate(perp_meta_list):
                if isinstance(perp_meta, dict):
                    if ':' in perp_meta.get("name", ""):  # e.g., 'xyz:AAPL'
                        coin_name = perp_meta.get("name", "")
                        # Calculate actual asset ID using offset + array position
                        asset_id = base_asset_id + asset_index

                        self._is_hip3_market[coin_name] = True
                        self.coin_to_asset[coin_name] = asset_id  # Store asset ID for order placement
                        self.logger().debug(f"Mapped HIP-3 {coin_name} -> asset_id {asset_id} (base={base_asset_id}, idx={asset_index}, API name: {coin_name})")

        coin_infos: list = exchange_info_dict[0]['universe']
        price_infos: list = exchange_info_dict[1]
        return_val: list = []
        min_notional_size = Decimal(str(CONSTANTS.MIN_NOTIONAL_SIZE))
        for coin_info, price_info in zip(coin_infos, price_infos):
            try:
                ex_symbol = f'{coin_info["name"]}'
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=ex_symbol)
                step_size = Decimal(str(10 ** -coin_info.get("szDecimals")))

                price_size = Decimal(str(10 ** -len(price_info.get("markPx").split('.')[1])))
                min_order_size = step_size
                collateral_token = CONSTANTS.CURRENCY
                return_val.append(
                    TradingRule(
                        trading_pair,
                        min_base_amount_increment=step_size,
                        min_price_increment=price_size,
                        min_order_size=min_order_size,
                        min_notional_size=min_notional_size,
                        buy_order_collateral_token=collateral_token,
                        sell_order_collateral_token=collateral_token,
                    )
                )
            except Exception:
                self.logger().error(f"Error parsing the trading pair rule {coin_info}. Skipping.",
                                    exc_info=True)

        # Process HIP-3/DEX markets derived from cached _dex_markets
        for dex_info in self._iter_hip3_merged_markets():
            try:
                coin_name = dex_info.get("name", "")  # e.g., 'xyz:AAPL'
                self._is_hip3_market[coin_name] = True
                quote = "USD"
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=coin_name)

                step_size = Decimal(str(10 ** -dex_info.get("szDecimals")))
                price_size = Decimal(str(10 ** -len(dex_info.get("markPx").split('.')[1])))
                min_order_size = step_size
                collateral_token = quote

                return_val.append(
                    TradingRule(
                        trading_pair,
                        min_base_amount_increment=step_size,
                        min_price_increment=price_size,
                        min_order_size=min_order_size,
                        min_notional_size=min_notional_size,
                        buy_order_collateral_token=collateral_token,
                        sell_order_collateral_token=collateral_token,
                    )
                )
            except Exception:
                self.logger().error(f"Error parsing HIP-3 trading pair rule {dex_info}. Skipping.",
                                    exc_info=True)

        return return_val

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List):
        mapping = bidict()
        for symbol_data in filter(web_utils.is_exchange_information_valid, exchange_info[0].get("universe", [])):
            symbol = symbol_data["name"]
            base = symbol_data["name"]
            quote = CONSTANTS.CURRENCY
            trading_pair = combine_to_hb_trading_pair(base, quote)
            if trading_pair in mapping.inverse:
                self._resolve_trading_pair_symbols_duplicate(mapping, symbol, base, quote)
            else:
                mapping[symbol] = trading_pair

        # Process HIP-3/DEX markets from separate _dex_markets list
        for dex_info in self._dex_markets:
            if dex_info is None:
                continue
            perp_meta_list = dex_info.get("perpMeta", [])
            for _, perp_meta in enumerate(perp_meta_list):
                if isinstance(perp_meta, dict):
                    full_symbol = perp_meta.get("name", "")  # e.g., 'xyz:AAPL'
                    if ':' in full_symbol:
                        self._is_hip3_market[full_symbol] = True
                        deployer, base = full_symbol.split(':')
                        quote = CONSTANTS.CURRENCY
                        symbol = f'{deployer.upper()}_{base}'
                        # quote = "USD" if deployer == "xyz" else 'USDH'
                        trading_pair = combine_to_hb_trading_pair(full_symbol, quote)
                        if trading_pair in mapping.inverse:
                            self._resolve_trading_pair_symbols_duplicate(mapping, full_symbol, full_symbol.upper(), quote)
                        else:
                            mapping[full_symbol] = trading_pair.upper()

        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        if ":" in trading_pair:
            # HIP-3 trading pair - extract base (e.g., "xyz:XYZ100" from "xyz:XYZ100-USD")
            parts = trading_pair.split("-")
            if len(parts) >= 2:
                exchange_symbol = trading_pair.rsplit("-", 1)[0]
                # Convert to lowercase for the dex name part
                dex_name, coin = exchange_symbol.split(":")
                exchange_symbol = f"{dex_name.lower()}:{coin}"
        else:
            try:
                exchange_symbol = await self.exchange_symbol_associated_to_pair(
                    trading_pair=trading_pair
                )
            except KeyError as e:
                self.logger().error(f"Trading pair {trading_pair} not found in symbol map: {e}")
                # Trading pair not in symbol map yet, try to extract from trading pair directly
                exchange_symbol = trading_pair.split("-")[0]

        params = {"type": CONSTANTS.ASSET_CONTEXT_TYPE}
        # Detect HIP-3 market by dict lookup OR by ":" in symbol (fallback for early calls)
        is_hip3 = self._is_hip3_market.get(exchange_symbol, False) or ":" in exchange_symbol
        if is_hip3:
            # For HIP-3 markets, need to use different type with dex parameter
            dex_name = exchange_symbol.split(':')[0]
            params = {"type": "metaAndAssetCtxs", "dex": dex_name}
        try:
            response = await safe_ensure_future(
                self._api_post(
                    path_url=CONSTANTS.TICKER_PRICE_CHANGE_URL,
                    data=params
                )
            )

            universe = response[0]["universe"]
            asset_ctxs = response[1]

            for meta, ctx in zip(universe, asset_ctxs):
                if meta.get("name") == exchange_symbol:
                    return float(ctx["markPx"])
        except Exception as e:
            self.logger().error(f"Error fetching last traded price for {trading_pair} ({exchange_symbol}): {e}")

        raise RuntimeError(
            f"Price not found for trading_pair={trading_pair}, "
            f"exchange_symbol={exchange_symbol}"
        )

    def _resolve_trading_pair_symbols_duplicate(self, mapping: bidict, new_exchange_symbol: str, base: str, quote: str):
        """Resolves name conflicts provoked by futures contracts.

        If the expected BASEQUOTE combination matches one of the exchange symbols, it is the one taken, otherwise,
        the trading pair is removed from the map and an error is logged.
        """
        expected_exchange_symbol = f"{base}{quote}"
        trading_pair = combine_to_hb_trading_pair(base, quote)
        current_exchange_symbol = mapping.inverse[trading_pair]
        if current_exchange_symbol == expected_exchange_symbol:
            pass
        elif new_exchange_symbol == expected_exchange_symbol:
            mapping.pop(current_exchange_symbol)
            mapping[new_exchange_symbol] = trading_pair
        else:
            self.logger().error(
                f"Could not resolve the exchange symbols {new_exchange_symbol} and {current_exchange_symbol}")
            mapping.pop(current_exchange_symbol)

    async def _update_balances(self):
        """
        Calls the REST API to update total and available balances.
        Under unified account or portfolio margin, use spot balances endpoint instead for trading account balance across spot and perps.
        """

        quote = CONSTANTS.CURRENCY
        account_info = await self._api_post(path_url=CONSTANTS.ACCOUNT_INFO_URL,
                                            data={"type": CONSTANTS.USER_STATE_TYPE,
                                                  "user": self.hyperliquid_perpetual_address},
                                            )

        local_asset_names = set(self._account_balances.keys()) | set(self._account_available_balances.keys())
        for asset_name in local_asset_names:
            if asset_name != quote:
                self._account_balances.pop(asset_name, None)
                self._account_available_balances.pop(asset_name, None)

        abstraction_mode = await self._get_user_abstraction_mode()
        use_spot_balances = abstraction_mode in CONSTANTS.SPOT_BALANCE_ABSTRACTION_MODES
        abstraction_unresolved = abstraction_mode is None
        perp_account_value = Decimal((account_info.get("crossMarginSummary") or {}).get("accountValue", "0"))

        spot_usdc_balance = None
        if use_spot_balances or abstraction_unresolved:
            # JEP-209: wrap the spot fetch+parse so a failure (e.g. the same /info 429 that
            # starved the abstraction lookup, or a malformed response) degrades to the legacy
            # perp `withdrawable` path below instead of raising out of the balance poll.
            try:
                spot_account_info = await self._api_post(path_url=CONSTANTS.ACCOUNT_INFO_URL,
                                                         data={"type": CONSTANTS.SPOT_USER_STATE_TYPE,
                                                               "user": self.hyperliquid_perpetual_address},
                                                         )
                spot_usdc_balance = next(
                    (balance_entry for balance_entry in spot_account_info["balances"]
                     if balance_entry["coin"].upper() == "USDC"),
                    None,
                )
            except Exception:
                self.logger().debug("Hyperliquid spot balance fetch failed.", exc_info=True)
                spot_usdc_balance = None

        spot_total = None
        spot_free = None
        if spot_usdc_balance is not None:
            try:
                spot_total = Decimal(spot_usdc_balance["total"])
                spot_free = spot_total - Decimal(spot_usdc_balance.get("hold", "0"))
            except Exception:
                spot_total = None
                spot_free = None
        spot_usdc_present = spot_total is not None and spot_total > Decimal("0")

        if use_spot_balances:
            # Confirmed unified / portfolio-margin account: spot USDC is the authoritative
            # cross-spot-and-perp trading balance.
            if spot_total is None:
                self._account_balances.pop(quote, None)
                self._account_available_balances.pop(quote, None)
            else:
                self._account_balances[quote] = spot_total
                self._account_available_balances[quote] = spot_free
        elif abstraction_unresolved and spot_usdc_present and perp_account_value <= Decimal("0"):
            # JEP-209 unified-account-safe fallback: the abstraction mode could not be resolved
            # (e.g. the userAbstraction lookup is starved under multi-symbol /info load). Only
            # override the legacy perp source when the perp account is EMPTY (accountValue <= 0,
            # so `withdrawable` is ~0 anyway) AND spot USDC is present — i.e. the collateral lives
            # in the spot wallet. A genuine legacy account funds the perp wallet (accountValue > 0)
            # and therefore keeps the `withdrawable` path; this fallback can only raise a 0 to the
            # real spot balance, never over-report a funded perp account.
            self._account_balances[quote] = spot_total
            self._account_available_balances[quote] = spot_free
            if not self._abstraction_fallback_warned:
                self.logger().warning(
                    "Hyperliquid user abstraction mode unresolved; using spot USDC balance "
                    "(unified-account-safe) until it resolves. See JEP-209."
                )
                self._abstraction_fallback_warned = True
        else:
            self._account_balances[quote] = perp_account_value
            self._account_available_balances[quote] = Decimal(account_info.get("withdrawable", "0"))

        if abstraction_mode is not None:
            # Mode resolved (unified or legacy) — clear the degraded-fallback warning latch so a
            # later unresolved episode warns once again.
            self._abstraction_fallback_warned = False

    _ABSTRACTION_MODE_TTL_S = 60.0  # JEP-270: re-resolve cadence for the stable wallet abstraction mode

    def _now(self) -> float:
        return time.time()

    async def _get_user_abstraction_mode(self) -> Optional[str]:
        # JEP-270: the abstraction mode is a stable wallet property -> serve a cached value within a
        # TTL (re-fetching it on every balance poll wastes /info budget and worsens JEP-209
        # starvation). Not sticky-forever: after the TTL it re-resolves, so a rare unified<->legacy
        # transition is still picked up without a restart. No blocking sleep.
        now = self._now()
        if (self._user_abstraction_mode is not None
                and self._user_abstraction_mode_ts is not None
                and (now - self._user_abstraction_mode_ts) < self._ABSTRACTION_MODE_TTL_S):
            return self._user_abstraction_mode
        try:
            abstraction_mode = await self._api_post(
                path_url=CONSTANTS.ACCOUNT_INFO_URL,
                data={"type": CONSTANTS.USER_ABSTRACTION_TYPE,
                      "user": self.hyperliquid_perpetual_address},
            )
        except Exception:
            self.logger().debug("Failed to fetch Hyperliquid user abstraction mode.", exc_info=True)
            abstraction_mode = None

        if isinstance(abstraction_mode, str):
            self._user_abstraction_mode = abstraction_mode
            self._user_abstraction_mode_ts = now
        return self._user_abstraction_mode

    async def _update_positions(self):
        all_positions = []

        # Fetch base perpetual positions (no dex param)
        base_positions = await self._api_post(path_url=CONSTANTS.POSITION_INFORMATION_URL,
                                              data={"type": CONSTANTS.USER_STATE_TYPE,
                                                    "user": self.hyperliquid_perpetual_address}
                                              )
        all_positions.extend(base_positions.get("assetPositions", []))

        # Fetch HIP-3 positions for each DEX market (only if enabled)
        if self._enable_hip3_markets:
            for dex_info in (self._dex_markets or []):
                if dex_info is None:
                    continue
                dex_name = dex_info.get("name", "")
                if not dex_name:
                    continue
                try:
                    dex_positions = await self._api_post(path_url=CONSTANTS.POSITION_INFORMATION_URL,
                                                         data={"type": CONSTANTS.USER_STATE_TYPE,
                                                               "user": self.hyperliquid_perpetual_address,
                                                               "dex": dex_name}
                                                         )
                    all_positions.extend(dex_positions.get("assetPositions", []))
                except Exception as e:
                    self.logger().debug(f"Error fetching positions for DEX {dex_name}: {e}")

        # Process all positions
        processed_coins = set()  # Track processed coins to avoid duplicates
        seen_keys = set()
        for position in all_positions:
            position = position.get("position")
            ex_trading_pair = position.get("coin")

            # Skip if we already processed this coin (avoid duplicates)
            if ex_trading_pair in processed_coins:
                continue
            processed_coins.add(ex_trading_pair)

            try:
                hb_trading_pair = await self.trading_pair_associated_to_exchange_symbol(ex_trading_pair)
            except KeyError:
                self.logger().debug(f"Skipping position for unmapped coin: {ex_trading_pair}")
                continue

            position_side = PositionSide.LONG if Decimal(position.get("szi")) > 0 else PositionSide.SHORT
            unrealized_pnl = Decimal(position.get("unrealizedPnl"))
            entry_price = Decimal(position.get("entryPx"))
            amount = Decimal(position.get("szi", 0))
            leverage = Decimal(position.get("leverage").get("value"))
            pos_key = self._perpetual_trading.position_key(hb_trading_pair, position_side)
            seen_keys.add(pos_key)
            if amount != 0:
                _position = Position(
                    trading_pair=hb_trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=leverage
                )
                self._perpetual_trading.set_position(pos_key, _position)
            else:
                self._perpetual_trading.remove_position(pos_key)

        # Remove any cached position that the exchange no longer reports.
        # Hyperliquid omits closed positions from assetPositions entirely, so the old
        # "only clean up when list is empty" guard left stale entries in the cache forever.
        for key in list(self._perpetual_trading.account_positions.keys()):
            if key not in seen_keys:
                self._perpetual_trading.remove_position(key)

    async def _get_position_mode(self) -> Optional[PositionMode]:
        return PositionMode.ONEWAY

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        msg = ""
        success = True
        initial_mode = await self._get_position_mode()
        if initial_mode != mode:
            msg = "hyperliquid only supports the ONEWAY position mode."
            success = False
        return success, msg

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        if not self.coin_to_asset:
            await self._update_trading_rules()
        is_cross = True  # Default to cross margin

        # Check if this is a HIP-3 market (doesn't support leverage API)
        if exchange_symbol in self._is_hip3_market and self._is_hip3_market[exchange_symbol]:
            is_cross = False  # HIP-3 markets use isolated margin by default
            msg = f"HIP-3 market {trading_pair} does not support leverage setting for cross margin. Defaulting to isolated margin."
            self.logger().debug(msg)

        # Check if coin exists in mapping
        if exchange_symbol not in self.coin_to_asset:
            msg = f"Coin {exchange_symbol} not found in coin_to_asset mapping. Available coins: {list(self.coin_to_asset.keys())[:20]}"
            self.logger().error(msg)
            return False, msg

        asset_id = self.coin_to_asset[exchange_symbol]
        self.logger().debug(f"Setting leverage for {trading_pair}: coin={exchange_symbol}, asset_id={asset_id}")

        params = {
            "type": "updateLeverage",
            "asset": asset_id,
            "isCross": is_cross,
            "leverage": leverage,
        }
        try:
            set_leverage = await self._api_post(
                path_url=CONSTANTS.SET_LEVERAGE_URL,
                data=params,
                is_auth_required=True)
            success = False
            msg = ""
            if set_leverage.get("status") == "err":
                raise IOError(f"{set_leverage}")
            if set_leverage["status"] == 'ok':
                success = True
            else:
                msg = 'Unable to set leverage'
            return success, msg
        except Exception as exception:
            success = False
            msg = f"There was an error setting the leverage for {trading_pair} ({exception})"

        return success, msg

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        exchange_symbol = await self.exchange_symbol_associated_to_pair(trading_pair)

        # HIP-3 markets may not have funding info available
        if exchange_symbol in self._is_hip3_market and self._is_hip3_market[exchange_symbol]:
            self.logger().debug(f"Skipping funding info fetch for HIP-3 market {exchange_symbol}")
            return 0, Decimal("-1"), Decimal("-1")

        funding_info_response = await self._api_post(path_url=CONSTANTS.GET_LAST_FUNDING_RATE_PATH_URL,
                                                     data={
                                                         "type": "userFunding",
                                                         "user": self.hyperliquid_perpetual_address,
                                                         "startTime": self._last_funding_time(),
                                                     }
                                                     )
        sorted_payment_response = [i for i in funding_info_response if i["delta"]["coin"] == exchange_symbol]
        if len(sorted_payment_response) < 1:
            timestamp, funding_rate, payment = 0, Decimal("-1"), Decimal("-1")
            return timestamp, funding_rate, payment
        funding_payment = sorted_payment_response[0]
        _payment = Decimal(str(funding_payment["delta"]["usdc"]))
        funding_rate = Decimal(funding_payment["delta"]["fundingRate"])
        timestamp = funding_payment["time"] * 1e-3
        if _payment != Decimal("0"):
            payment = _payment
        else:
            timestamp, funding_rate, payment = 0, Decimal("-1"), Decimal("-1")
        return timestamp, funding_rate, payment

    def _last_funding_time(self) -> int:
        """
        Funding settlement occurs every 1 hours as mentioned in https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding
        """
        return int(((time.time() // 3600) - 1) * 3600 * 1e3)
