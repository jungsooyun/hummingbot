"""Ladder maker executor, rewired onto the shared cross-venue hedged machine (JEP-147).

``LadderMakerExecutor`` now inherits ``CrossVenueHedgedExecutorBase`` (JEP-143):
all inventory / hedge-queue / fill-accounting / PnL / fee / retry / event plumbing
lives in the base, identical to ``XEMMExecutor``. This subclass supplies only the
ladder-specific policy via the base's abstract hooks:

  * fair-price derivation (KIS spot KRW->USD via side-aware FX) + inventory skew,
  * ladder target construction + reprice guard,
  * marketable-limit hedge sizing on KIS spot,
  * the pre-quote gate.

Pure pricing math stays in ``ladder_policy`` (unit-tested without the runtime).

Gate wiring (JEP-147): the kill-switch now flows through a composable ``GateChain``
(JEP-142). The chain is the extension seam JEP-133 fills with the staleness /
trading-hours / order-cap gates (those need feed-age + session inputs that are not
plumbed yet); for now the chain holds ``KillSwitchGate`` and the fair-price
readiness check is kept as an explicit data gate to preserve current behavior.
"""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.cross_venue_hedged_executor.cross_venue_hedged_executor_base import (
    CrossVenueHedgedExecutorBase,
)
from hummingbot.strategy_v2.executors.ladder_maker_executor.data_types import LadderMakerExecutorConfig
from hummingbot.strategy_v2.executors.ladder_maker_executor.ladder_policy import (
    RungSpec,
    Side,
    apply_inventory_skew,
    build_ladder_targets,
    compute_fair_price,
    compute_hedge_order,
)
from hummingbot.strategy_v2.gates.gate_chain import GateChain, GateContext, KillSwitchGate
from hummingbot.strategy_v2.models.executors import TrackedOrder

ZERO = Decimal("0")
_KST = timezone(timedelta(hours=9))


class LadderMakerExecutor(CrossVenueHedgedExecutorBase):
    """Ladder market-making on a perp (maker, post-only) hedged on KIS spot.

    Each tick: derive a conservative fair price from the KIS spot orderbook (KRW->USD
    via side-aware FX), apply inventory skew, build a simultaneous-maker ladder, and
    keep the perp quotes in sync (subject to reprice guards). When a perp maker fills,
    the base enqueues a marketable-limit hedge on KIS spot sized by ``_size_hedge``.
    """

    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        strategy: StrategyV2Base,
        config: LadderMakerExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 10,
    ):
        self._last_reprice_ts = 0.0
        # Kill-switch flows through the composable chain; JEP-133 appends the
        # staleness / trading-hours / order-cap gates to this same chain.
        self._gate_chain = GateChain([KillSwitchGate()])
        super().__init__(
            strategy=strategy,
            config=config,
            maker_market=config.maker_market,
            hedge_market=config.hedge_market,
            entry_side=config.entry_side,
            connectors=[config.fx_connector] if config.fx_connector else None,
            update_interval=update_interval,
            max_retries=max_retries,
        )

    # ------------------------------------------------------------------ gates

    def _policy_side(self) -> Side:
        return Side.BUY if self.entry_side == TradeType.BUY else Side.SELL

    def _pending_maker_notional(self) -> Decimal:
        total = ZERO
        for o in self._open_maker_orders():
            order = o.order
            if order is not None and order.price is not None and order.amount is not None:
                total += Decimal(order.price) * Decimal(order.amount)
        return total

    def _gates_open(self) -> bool:
        ctx = GateContext(
            now_kst=datetime.fromtimestamp(self._strategy.current_timestamp, tz=_KST),
            kis_age_s=0.0,  # feed-age gates land in JEP-133
            hl_age_s=0.0,
            fx_age_s=0.0,
            inventory=self._unhedged_base_signed(),
            open_order_count=len(self._open_maker_orders()),
            pending_notional=self._pending_maker_notional(),
            kill_switch=bool(self.config.kill_switch),
        )
        if not self._gate_chain.evaluate(ctx).open:
            return False
        # Data-readiness gate: a fair price must be computable. Promoting this to a
        # StalenessGate with real KIS/FX ages is JEP-133.
        return self._compute_fair(self._policy_side()) is not None

    # ------------------------------------------------------------------ fair price

    def _get_fx(self):
        """Live blended USD/KRW from the process-wide FairFxSource (JEP-148).

        Preserves the ``(Optional[Decimal], Optional[Decimal])`` contract that
        ``_compute_fair`` unpacks: the singleton's ``None`` (stale/never-fetched
        bank) maps to ``(None, None)`` so the fair gate closes. ``fx_connector`` /
        ``fx_trading_pair`` still register the USDT-KRW market for subscription
        (the source reads it via the script's getter). ``static_fx_rate`` remains a
        guarded offline/test fallback ONLY — never the live path.
        """
        from hummingbot.data_feed.fair_fx.fair_fx_source import FairFxSource

        quote = FairFxSource.get_instance().get_fx()
        if quote is not None:
            return quote
        if self.config.static_fx_rate and self.config.static_fx_rate > ZERO:
            rate = self.config.static_fx_rate
            return rate, rate
        return None, None

    def _compute_fair(self, side: Side) -> Optional[Decimal]:
        kis = self.connectors[self.hedge_connector]
        bid = kis.get_price_by_type(self.hedge_trading_pair, PriceType.BestBid)
        ask = kis.get_price_by_type(self.hedge_trading_pair, PriceType.BestAsk)
        if not bid or not ask or bid <= ZERO or ask <= ZERO:
            return None
        fx_bid, fx_ask = self._get_fx()
        if fx_bid is None or fx_ask is None:
            return None
        fair = compute_fair_price(
            Decimal(str(bid)), Decimal(str(ask)), fx_bid, fx_ask, side, self.config.side_aware_fx
        )
        return apply_inventory_skew(
            fair,
            self._unhedged_base_signed(),
            self.config.target_inventory,
            self.config.inventory_skew_bps_per_unit,
        )

    # ------------------------------------------------------------------ ladder hooks

    def _compute_targets(self) -> List:
        side = self._policy_side()
        fair = self._compute_fair(side)
        if fair is None:
            return []
        rungs = [
            RungSpec(edge_bps=r.edge_bps, size=r.size, min_edge_bps=r.min_edge_bps, enabled=r.enabled)
            for r in self.config.rungs
        ]
        return build_ladder_targets(
            fair=fair,
            rungs=rungs,
            total_size_cap=self.config.total_size_cap,
            side=side,
            tick=self.config.maker_tick,
            buffer_ticks=self.config.buffer_ticks,
            inventory=self._unhedged_base_signed(),
            max_inventory=self.config.max_inventory,
        )

    def _should_reprice(self, targets: List) -> bool:
        open_makers = self._open_maker_orders()
        if not open_makers:
            return bool(targets)
        elapsed = self._strategy.current_timestamp - self._last_reprice_ts
        if elapsed < self.config.min_reprice_interval_s:
            return False
        # Reprice only if the best target moved beyond the tick threshold.
        target_prices = sorted(t.price for t in targets)
        current_prices = sorted(o.order.price for o in open_makers if o.order is not None)
        if not current_prices or not target_prices:
            return True
        delta = abs(target_prices[0] - current_prices[0])
        return delta >= self.config.min_reprice_delta_ticks * self.config.maker_tick

    def _place_targets(self, targets: List) -> None:
        for target in targets:
            self._place_maker(target.price, target.size, target.edge_bps)
        self._last_reprice_ts = self._strategy.current_timestamp

    def _place_maker(self, price: Decimal, amount: Decimal, edge_bps: Decimal) -> None:
        connector = self.connectors[self.maker_connector]
        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT_MAKER,
            side=self.entry_side,
            amount=connector.quantize_order_amount(self.maker_trading_pair, amount),
            position_action=PositionAction.OPEN,
            price=connector.quantize_order_price(self.maker_trading_pair, price),
            metadata={"order_role": "maker", "edge_bps": str(edge_bps)},
        )
        self.maker_orders[order_id] = TrackedOrder(order_id=order_id)

    # ------------------------------------------------------------------ hedge hook

    def _size_hedge(self, pending_base: Decimal) -> Optional[Dict]:
        kis = self.connectors[self.hedge_connector]
        best_ask = kis.get_price_by_type(self.hedge_trading_pair, PriceType.BestAsk)
        if not best_ask or best_ask <= ZERO:
            return None
        hedge = compute_hedge_order(
            fill_qty=pending_base,
            share_per_unit=self.config.share_per_unit,
            kis_best_ask=Decimal(str(best_ask)),
            max_slippage_bps=self.config.hedge_max_slippage_bps,
            tick=self.config.hedge_tick,
        )
        amount = kis.quantize_order_amount(self.hedge_trading_pair, hedge.size)
        if amount <= ZERO:
            return None
        price = (
            Decimal("NaN")
            if self.config.hedge_order_type == OrderType.MARKET
            else kis.quantize_order_price(self.hedge_trading_pair, hedge.price)
        )
        return {
            "amount": amount,
            "price": price,
            "order_type": self.config.hedge_order_type,
            "metadata": {"order_role": "hedge"},
        }

    # ------------------------------------------------------------------ balance hook

    def _maker_balance_candidate(self) -> Optional[OrderCandidate]:
        fair = self._compute_fair(self._policy_side())
        if fair is None:
            return None
        return OrderCandidate(
            trading_pair=self.maker_trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT_MAKER,
            order_side=self.entry_side,
            amount=self.config.total_size_cap,
            price=fair,
        )
