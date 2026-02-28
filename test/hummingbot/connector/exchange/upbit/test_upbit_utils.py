import unittest

from hummingbot.connector.exchange.upbit import upbit_utils


class TestUpbitUtils(unittest.TestCase):
    def test_is_exchange_information_valid(self):
        self.assertTrue(upbit_utils.is_exchange_information_valid({"market_warning": "NONE"}))
        self.assertFalse(upbit_utils.is_exchange_information_valid({"market_warning": "CAUTION"}))

    def test_split_market_symbol(self):
        base, quote = upbit_utils.split_market_symbol("KRW-BTC")
        self.assertEqual(base, "BTC")
        self.assertEqual(quote, "KRW")

    def test_split_market_symbol_invalid(self):
        with self.assertRaises(ValueError):
            upbit_utils.split_market_symbol("BTCKRW")
