import unittest

from hummingbot.connector.exchange.bithumb import bithumb_utils


class TestBithumbUtils(unittest.TestCase):
    def test_is_exchange_information_valid(self):
        self.assertTrue(bithumb_utils.is_exchange_information_valid({"market_warning": "NONE"}))
        self.assertFalse(bithumb_utils.is_exchange_information_valid({"market_warning": "CAUTION"}))

    def test_split_market_symbol(self):
        base, quote = bithumb_utils.split_market_symbol("KRW-BTC")
        self.assertEqual(base, "BTC")
        self.assertEqual(quote, "KRW")

    def test_split_market_symbol_invalid(self):
        with self.assertRaises(ValueError):
            bithumb_utils.split_market_symbol("BTCKRW")
