"""
Runtime override for hummingbot-api AccountsService.

Adds inverse-pair fallback valuation so fiat tokens like KRW can be valued
in USDT when only the inverse market (e.g. USDT-KRW) is available.
"""

from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from services.accounts_service_original import *  # noqa: F401,F403
from services.accounts_service_original import AccountsService as BaseAccountsService


class AccountsService(BaseAccountsService):
    @staticmethod
    def _to_positive_decimal(value) -> Decimal:
        try:
            decimal_value = Decimal(str(value))
            return decimal_value if decimal_value > 0 else Decimal("0")
        except Exception:
            return Decimal("0")

    def _build_market_candidates(self, token: str, connector_name: str) -> Tuple[str, Optional[str]]:
        direct_market = self.get_default_market(token, connector_name)
        if "-" not in direct_market:
            return direct_market, None

        base, quote = direct_market.split("-", 1)
        inverse_market = f"{quote}-{base}"
        if inverse_market == direct_market:
            inverse_market = None

        return direct_market, inverse_market

    async def _get_connector_tokens_info(self, connector, connector_name: str) -> List[Dict]:
        """
        Get token info from a connector instance using RateOracle cached prices.

        Tries the RateOracle (instant, in-memory) first for each token.
        Falls back to exchange last traded price for direct market and, if needed,
        inverse market with reciprocal conversion.
        """
        balances = [
            {"token": key, "units": value}
            for key, value in connector.get_all_balances().items()
            if value != Decimal("0") and key not in settings.banned_tokens
        ]

        tokens_info: List[Dict] = []
        missing_pairs = set()
        pending_resolutions: List[Tuple[int, str, Optional[str]]] = []

        for balance in balances:
            token = balance["token"]
            units = Decimal(str(balance["units"]))
            price: Optional[Decimal]

            if "USD" in token:
                price = Decimal("1")
            else:
                rate = None
                if self._market_data_service:
                    rate = self._market_data_service.get_rate(token, "USDT")
                price = self._to_positive_decimal(rate)

                if price == 0:
                    direct_market, inverse_market = self._build_market_candidates(token, connector_name)
                    missing_pairs.add(direct_market)
                    if inverse_market is not None:
                        missing_pairs.add(inverse_market)
                    pending_resolutions.append((len(tokens_info), direct_market, inverse_market))
                    price = None

            tokens_info.append(
                {
                    "token": token,
                    "units": float(units),
                    "price": float(price) if price is not None else 0.0,
                    "value": float(price * units) if price is not None else 0.0,
                    "available_units": float(connector.get_available_balance(token)),
                }
            )

        if missing_pairs:
            fallback_prices = await self._safe_get_last_traded_prices(connector, sorted(missing_pairs))

            for info_index, direct_market, inverse_market in pending_resolutions:
                direct_price = self._to_positive_decimal(fallback_prices.get(direct_market, 0))
                resolved_price = direct_price

                if resolved_price == 0 and inverse_market is not None:
                    inverse_price = self._to_positive_decimal(fallback_prices.get(inverse_market, 0))
                    if inverse_price > 0:
                        resolved_price = Decimal("1") / inverse_price

                if resolved_price > 0:
                    units = Decimal(str(tokens_info[info_index]["units"]))
                    tokens_info[info_index]["price"] = float(resolved_price)
                    tokens_info[info_index]["value"] = float(resolved_price * units)

        return tokens_info
