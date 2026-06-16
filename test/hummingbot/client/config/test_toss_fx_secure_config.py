import unittest

from hummingbot.client.config.client_config_map import ClientConfigMap, TossFxConfigMap
from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.client.config.security import Security


def _secrets_manager(password="some-password"):
    return ETHKeyFileSecretManger(password=password)


class TossFxSecureConfigTest(unittest.TestCase):
    def test_fields_are_secure(self):
        fields = TossFxConfigMap.model_fields
        for name in ("toss_client_id", "toss_client_secret"):
            extra = fields[name].json_schema_extra or {}
            self.assertTrue(extra.get("is_secure"), f"{name} must be is_secure")

    def test_default_empty_does_not_crash(self):
        cm = ClientConfigMap()
        self.assertEqual(cm.toss_fx.toss_client_id.get_secret_value(), "")
        self.assertEqual(cm.toss_fx.toss_client_secret.get_secret_value(), "")

    def test_secure_round_trip(self):
        Security.secrets_manager = _secrets_manager()
        enc_id = Security.secrets_manager.encrypt_secret_value("toss_client_id", "client-abc")
        enc_secret = Security.secrets_manager.encrypt_secret_value("toss_client_secret", "secret-xyz")

        cm = ClientConfigMap()
        cm.toss_fx = TossFxConfigMap(toss_client_id=enc_id, toss_client_secret=enc_secret)
        adapter = ClientConfigAdapter(cm)

        # still encrypted in place
        self.assertEqual(enc_id, adapter.toss_fx.toss_client_id.get_secret_value())

        adapter._decrypt_all_internal_secrets()

        self.assertEqual("client-abc", adapter.toss_fx.toss_client_id.get_secret_value())
        self.assertEqual("secret-xyz", adapter.toss_fx.toss_client_secret.get_secret_value())


if __name__ == "__main__":
    unittest.main()
