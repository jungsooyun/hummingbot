"""
Integration tests for Lighter connector against the LIVE API.

These tests call the actual Lighter mainnet API (public endpoints only, no auth needed).
They verify that our response parsing logic works with real API responses.

Run with: PYENV_VERSION=py312 python -m pytest test/hummingbot/connector/exchange/lighter/test_lighter_integration.py -v -m integration
Skip with: PYENV_VERSION=py312 python -m pytest test/hummingbot/connector/exchange/lighter/ -v -m "not integration"
"""

import asyncio
import unittest

import aiohttp
import pytest

from hummingbot.connector.exchange.lighter import lighter_constants as CONSTANTS

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

# Lighter mainnet base URL
BASE_URL = CONSTANTS.REST_URL


async def _fetch(path: str, params: dict = None) -> dict:
    """Helper to make a GET request to the Lighter API."""
    url = f"{BASE_URL}/{path}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            resp.raise_for_status()
            return await resp.json()


class TestLighterIntegrationPublicAPI(unittest.TestCase):
    """Integration tests against the live Lighter API (public endpoints, no auth)."""

    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    # ── /api/v1/orderBooks ──────────────────────────────────────────

    def test_order_books_response_structure(self):
        """Verify /api/v1/orderBooks returns expected top-level structure."""
        data = self._run(_fetch(CONSTANTS.GET_TRADING_RULES_PATH_URL))

        self.assertIn("code", data)
        self.assertEqual(data["code"], 200)
        self.assertIn("order_books", data)
        self.assertIsInstance(data["order_books"], list)
        self.assertGreater(len(data["order_books"]), 0)

    def test_order_books_market_fields(self):
        """Verify each market entry has the fields our connector parses."""
        data = self._run(_fetch(CONSTANTS.GET_TRADING_RULES_PATH_URL))
        market = data["order_books"][0]

        required_fields = [
            "symbol", "market_id", "market_type", "status",
            "supported_size_decimals", "supported_price_decimals",
            "min_base_amount", "min_quote_amount",
        ]
        for field in required_fields:
            self.assertIn(field, market, f"Missing field: {field}")

    def test_order_books_has_spot_markets(self):
        """Verify at least one spot market exists (our connector supports spot)."""
        data = self._run(_fetch(CONSTANTS.GET_TRADING_RULES_PATH_URL))
        spot_markets = [m for m in data["order_books"] if m.get("market_type") == "spot"]
        self.assertGreater(len(spot_markets), 0, "No spot markets found")

    def test_order_books_spot_symbol_format(self):
        """Verify spot symbols use 'BASE/QUOTE' format."""
        data = self._run(_fetch(CONSTANTS.GET_TRADING_RULES_PATH_URL))
        spot_markets = [m for m in data["order_books"] if m.get("market_type") == "spot"]
        for market in spot_markets:
            self.assertIn("/", market["symbol"], f"Spot symbol {market['symbol']} missing '/'")

    # ── /api/v1/orderBookOrders ─────────────────────────────────────

    def test_order_book_orders_response_structure(self):
        """Verify /api/v1/orderBookOrders returns expected structure."""
        data = self._run(_fetch(CONSTANTS.GET_ORDER_BOOK_PATH_URL, {"market_id": 0, "limit": 5}))

        self.assertIn("code", data)
        self.assertEqual(data["code"], 200)
        self.assertIn("asks", data)
        self.assertIn("bids", data)
        self.assertIsInstance(data["asks"], list)
        self.assertIsInstance(data["bids"], list)

    def test_order_book_orders_entry_fields(self):
        """Verify each orderbook entry has price and remaining_base_amount."""
        data = self._run(_fetch(CONSTANTS.GET_ORDER_BOOK_PATH_URL, {"market_id": 0, "limit": 3}))

        if data["asks"]:
            ask = data["asks"][0]
            self.assertIn("price", ask)
            self.assertIn("remaining_base_amount", ask)
            # Verify they're parseable as floats
            float(ask["price"])
            float(ask["remaining_base_amount"])

        if data["bids"]:
            bid = data["bids"][0]
            self.assertIn("price", bid)
            self.assertIn("remaining_base_amount", bid)

    def test_order_book_orders_sorted(self):
        """Verify asks are ascending and bids are descending by price."""
        data = self._run(_fetch(CONSTANTS.GET_ORDER_BOOK_PATH_URL, {"market_id": 0, "limit": 10}))

        ask_prices = [float(a["price"]) for a in data["asks"]]
        bid_prices = [float(b["price"]) for b in data["bids"]]

        self.assertEqual(ask_prices, sorted(ask_prices), "Asks not sorted ascending")
        self.assertEqual(bid_prices, sorted(bid_prices, reverse=True), "Bids not sorted descending")

    # ── /api/v1/orderBookDetails ────────────────────────────────────

    def test_order_book_details_response_structure(self):
        """Verify /api/v1/orderBookDetails returns both perp and spot details."""
        data = self._run(_fetch(CONSTANTS.GET_LAST_TRADING_PRICES_PATH_URL))

        self.assertIn("code", data)
        self.assertEqual(data["code"], 200)
        self.assertIn("order_book_details", data)
        self.assertIn("spot_order_book_details", data)
        self.assertIsInstance(data["order_book_details"], list)
        self.assertIsInstance(data["spot_order_book_details"], list)

    def test_order_book_details_has_last_trade_price(self):
        """Verify each detail entry has last_trade_price as a number."""
        data = self._run(_fetch(CONSTANTS.GET_LAST_TRADING_PRICES_PATH_URL))

        for detail in data["order_book_details"][:3]:
            self.assertIn("last_trade_price", detail)
            self.assertIsInstance(detail["last_trade_price"], (int, float))
            self.assertIn("symbol", detail)
            self.assertIn("market_id", detail)

    def test_order_book_details_spot_has_last_trade_price(self):
        """Verify spot details also have last_trade_price."""
        data = self._run(_fetch(CONSTANTS.GET_LAST_TRADING_PRICES_PATH_URL))

        if data["spot_order_book_details"]:
            detail = data["spot_order_book_details"][0]
            self.assertIn("last_trade_price", detail)
            self.assertIn("symbol", detail)

    # ── /api/v1/recentTrades ────────────────────────────────────────

    def test_recent_trades_response_structure(self):
        """Verify /api/v1/recentTrades returns expected structure."""
        data = self._run(_fetch(CONSTANTS.GET_RECENT_TRADES_PATH_URL, {"market_id": 0, "limit": 3}))

        self.assertIn("code", data)
        self.assertEqual(data["code"], 200)
        self.assertIn("trades", data)
        self.assertIsInstance(data["trades"], list)

    def test_recent_trades_entry_fields(self):
        """Verify trade entries have the fields we parse."""
        data = self._run(_fetch(CONSTANTS.GET_RECENT_TRADES_PATH_URL, {"market_id": 0, "limit": 2}))

        if data["trades"]:
            trade = data["trades"][0]
            required_fields = ["trade_id", "market_id", "size", "price", "timestamp"]
            for field in required_fields:
                self.assertIn(field, trade, f"Missing field: {field}")

    # ── Cross-validation ────────────────────────────────────────────

    def test_market_id_consistent_across_endpoints(self):
        """Verify market_id=0 returns data in both orderBooks and orderBookOrders."""
        markets = self._run(_fetch(CONSTANTS.GET_TRADING_RULES_PATH_URL))
        market_ids = {m["market_id"] for m in markets["order_books"]}
        self.assertIn(0, market_ids, "market_id=0 not found in orderBooks")

        orderbook = self._run(_fetch(CONSTANTS.GET_ORDER_BOOK_PATH_URL, {"market_id": 0, "limit": 1}))
        self.assertEqual(orderbook["code"], 200)

    def test_spread_positive(self):
        """Verify best ask > best bid (positive spread) for an active market."""
        data = self._run(_fetch(CONSTANTS.GET_ORDER_BOOK_PATH_URL, {"market_id": 0, "limit": 1}))

        if data["asks"] and data["bids"]:
            best_ask = float(data["asks"][0]["price"])
            best_bid = float(data["bids"][0]["price"])
            self.assertGreater(best_ask, best_bid, f"Negative spread: ask={best_ask}, bid={best_bid}")


if __name__ == "__main__":
    unittest.main()
