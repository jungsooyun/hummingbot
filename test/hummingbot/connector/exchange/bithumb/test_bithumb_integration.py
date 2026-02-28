"""
Integration tests for Bithumb connector against the LIVE API.

Run with:
PYENV_VERSION=py312 python -m pytest test/hummingbot/connector/exchange/bithumb/test_bithumb_integration.py -v -m integration
"""

import asyncio
import unittest

import aiohttp
import pytest
from bidict import bidict

from hummingbot.connector.exchange.bithumb import bithumb_constants as CONSTANTS
from hummingbot.connector.exchange.bithumb.bithumb_exchange import BithumbExchange

pytestmark = pytest.mark.integration

BASE_URL = CONSTANTS.REST_URL


async def _fetch(path: str, params: dict | None = None):
    url = f"{BASE_URL}/{path}"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()


class TestBithumbIntegrationPublicAPI(unittest.TestCase):
    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    def test_market_all_response_structure(self):
        data = self._run(_fetch(CONSTANTS.GET_TRADING_RULES_PATH_URL))
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 100)
        self.assertIn("market", data[0])

    def test_market_all_contains_krw_btc(self):
        data = self._run(_fetch(CONSTANTS.GET_TRADING_RULES_PATH_URL))
        markets = {item.get("market") for item in data}
        self.assertIn("KRW-BTC", markets)

    def test_ticker_endpoint_for_krw_btc(self):
        data = self._run(_fetch(CONSTANTS.GET_LAST_TRADING_PRICES_PATH_URL, {"markets": "KRW-BTC"}))
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)
        ticker = data[0]
        self.assertIn("trade_price", ticker)
        self.assertGreater(float(ticker["trade_price"]), 0)

    def test_orderbook_endpoint_for_krw_btc(self):
        data = self._run(_fetch(CONSTANTS.GET_ORDER_BOOK_PATH_URL, {"markets": "KRW-BTC"}))
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)
        ob = data[0]
        self.assertIn("orderbook_units", ob)
        self.assertGreater(len(ob["orderbook_units"]), 0)

    def test_connector_orderbook_snapshot_fetch_live(self):
        async def run_check():
            exchange = BithumbExchange(
                bithumb_access_key="dummy",
                bithumb_secret_key="dummy",
                trading_pairs=["BTC-KRW"],
                trading_required=False,
            )
            exchange._set_trading_pair_symbol_map(bidict({"KRW-BTC": "BTC-KRW"}))
            data_source = exchange._create_order_book_data_source()
            snapshot = await data_source._order_book_snapshot("BTC-KRW")
            self.assertGreater(len(snapshot.content["bids"]), 0)
            self.assertGreater(len(snapshot.content["asks"]), 0)

        self._run(run_check())

    def test_connector_last_price_fetch_live(self):
        async def run_check():
            exchange = BithumbExchange(
                bithumb_access_key="dummy",
                bithumb_secret_key="dummy",
                trading_pairs=["BTC-KRW"],
                trading_required=False,
            )
            exchange._set_trading_pair_symbol_map(bidict({"KRW-BTC": "BTC-KRW"}))
            price = await exchange._get_last_traded_price("BTC-KRW")
            self.assertGreater(price, 0)

        self._run(run_check())


if __name__ == "__main__":
    unittest.main()
