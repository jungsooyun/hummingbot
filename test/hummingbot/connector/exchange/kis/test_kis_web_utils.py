import unittest

from hummingbot.connector.exchange.kis import kis_web_utils as web_utils


class KisWebUtilsBackoffTest(unittest.TestCase):
    def test_first_failure_returns_base(self):
        self.assertEqual(5.0, web_utils.reconnect_backoff(1))
        # Defensive: 0/negative also clamp to base (never < base, never spam faster).
        self.assertEqual(5.0, web_utils.reconnect_backoff(0))

    def test_exponential_growth(self):
        self.assertEqual(10.0, web_utils.reconnect_backoff(2))
        self.assertEqual(20.0, web_utils.reconnect_backoff(3))
        self.assertEqual(40.0, web_utils.reconnect_backoff(4))

    def test_capped(self):
        # 5 * 2^4 = 80 -> capped at 60; stays capped for higher failure counts.
        self.assertEqual(60.0, web_utils.reconnect_backoff(5))
        self.assertEqual(60.0, web_utils.reconnect_backoff(50))

    def test_custom_base_and_cap(self):
        self.assertEqual(1.0, web_utils.reconnect_backoff(1, base=1.0, cap=10.0))
        self.assertEqual(10.0, web_utils.reconnect_backoff(9, base=1.0, cap=10.0))


if __name__ == "__main__":
    unittest.main()
