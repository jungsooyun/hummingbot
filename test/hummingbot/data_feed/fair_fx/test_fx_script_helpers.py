import unittest
from decimal import Decimal

from hummingbot.core.data_type.common import PriceType
from hummingbot.data_feed.fair_fx.fx_script_helpers import (
    decrypt_client_secret,
    discover_fx_market,
    make_usdt_getter,
)

D = Decimal


class FakeSecretsManager:
    """Mimics ETHKeyFileSecretManger: decrypt_secret_value raises on plaintext."""

    def decrypt_secret_value(self, attr, value):
        if not value.startswith("enc:"):
            raise ValueError("not an encrypted keystore")
        return value[len("enc:"):]


class FakeConnector:
    def __init__(self, bid, ask, raises=False):
        self._bid = bid
        self._ask = ask
        self._raises = raises

    def get_price_by_type(self, trading_pair, price_type):
        if self._raises:
            raise RuntimeError("no book")
        return self._bid if price_type == PriceType.BestBid else self._ask


class FakeCfg:
    def __init__(self, fx_connector=None, fx_trading_pair=None):
        self.fx_connector = fx_connector
        self.fx_trading_pair = fx_trading_pair


class FxScriptHelpersTest(unittest.TestCase):
    def test_getter_returns_decimals(self):
        conns = {"upbit": FakeConnector(D("1410"), D("1420"))}
        getter = make_usdt_getter(conns, "upbit", "USDT-KRW")
        self.assertEqual(getter(), (D("1410"), D("1420")))

    def test_getter_nan_to_none(self):
        conns = {"upbit": FakeConnector(Decimal("NaN"), D("1420"))}
        getter = make_usdt_getter(conns, "upbit", "USDT-KRW")
        self.assertEqual(getter(), (None, D("1420")))

    def test_getter_nonpositive_to_none(self):
        conns = {"upbit": FakeConnector(D("0"), D("-5"))}
        getter = make_usdt_getter(conns, "upbit", "USDT-KRW")
        self.assertEqual(getter(), (None, None))

    def test_getter_missing_connector(self):
        getter = make_usdt_getter({}, "upbit", "USDT-KRW")
        self.assertEqual(getter(), (None, None))

    def test_getter_exception_returns_none(self):
        conns = {"upbit": FakeConnector(D("1410"), D("1420"), raises=True)}
        getter = make_usdt_getter(conns, "upbit", "USDT-KRW")
        self.assertEqual(getter(), (None, None))

    def test_discover_from_config(self):
        cfgs = [FakeCfg(), FakeCfg("bithumb", "USDT-KRW")]
        self.assertEqual(discover_fx_market(cfgs), ("bithumb", "USDT-KRW"))

    def test_discover_default_when_absent(self):
        cfgs = [FakeCfg(), FakeCfg()]
        self.assertEqual(discover_fx_market(cfgs), ("upbit", "USDT-KRW"))

    # Lesson: client-config is_secure fields (toss_fx) are NOT decrypted at load
    # (unlike connector configs), so get_secret_value() returns the encrypted
    # keystore. decrypt_client_secret peels it at point of use.
    def test_decrypt_client_secret_decrypts_keystore(self):
        sm = FakeSecretsManager()
        self.assertEqual(decrypt_client_secret(sm, "toss_client_id", "enc:abc123"), "abc123")

    def test_decrypt_client_secret_plaintext_passthrough(self):
        # Already-plaintext (e.g., tests / non-secure path) -> returned unchanged.
        sm = FakeSecretsManager()
        self.assertEqual(decrypt_client_secret(sm, "toss_client_id", "abc123"), "abc123")

    def test_decrypt_client_secret_empty(self):
        self.assertEqual(decrypt_client_secret(FakeSecretsManager(), "toss_client_id", ""), "")

    def test_decrypt_client_secret_no_manager(self):
        self.assertEqual(decrypt_client_secret(None, "toss_client_id", "enc:abc"), "enc:abc")


if __name__ == "__main__":
    unittest.main()
