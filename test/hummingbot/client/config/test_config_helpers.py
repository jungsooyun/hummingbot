import asyncio
import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Awaitable, Optional
from unittest.mock import MagicMock, patch

from pydantic import Field, SecretStr

from hummingbot.client.config import config_helpers
from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger
from hummingbot.client.config.config_data_types import BaseClientModel, BaseConnectorConfigMap
from hummingbot.client.config.config_helpers import (
    ClientConfigAdapter,
    ReadOnlyClientConfigAdapter,
    get_connector_config_yml_path,
    get_strategy_config_map,
    list_connector_configs,
    load_connector_config_map_from_file,
    save_to_yml,
)
from hummingbot.client.config.security import Security
from hummingbot.client.config.strategy_config_data_types import BaseStrategyConfigMap
from hummingbot.strategy.avellaneda_market_making.avellaneda_market_making_config_map_pydantic import (
    AvellanedaMarketMakingConfigMap,
)


class ConfigHelpersTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.ev_loop = asyncio.get_event_loop()
        self._original_connectors_conf_dir_path = config_helpers.CONNECTORS_CONF_DIR_PATH

    def tearDown(self) -> None:
        config_helpers.CONNECTORS_CONF_DIR_PATH = self._original_connectors_conf_dir_path
        super().tearDown()

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    @staticmethod
    def get_async_sleep_fn(delay: float):
        async def async_sleep(*_, **__):
            await asyncio.sleep(delay)

        return async_sleep

    def test_get_strategy_config_map(self):
        cm = get_strategy_config_map(strategy="avellaneda_market_making")
        self.assertIsInstance(cm.hb_config, AvellanedaMarketMakingConfigMap)
        self.assertFalse(hasattr(cm, "market"))  # uninitialized instance

    def test_save_to_yml(self):
        class DummyStrategy(BaseStrategyConfigMap):
            class Config:
                title = "pure_market_making"

            strategy: str = "pure_market_making"

        cm = ClientConfigAdapter(DummyStrategy())
        expected_str = """\
#####################################
###   pure_market_making config   ###
#####################################

strategy: pure_market_making
"""
        with TemporaryDirectory() as d:
            d = Path(d)
            temp_file_name = d / "cm.yml"
            save_to_yml(temp_file_name, cm)
            with open(temp_file_name) as f:
                actual_str = f.read()
        self.assertEqual(expected_str, actual_str)

    @patch("hummingbot.client.config.config_helpers.AllConnectorSettings.get_connector_config_keys")
    def test_load_connector_config_map_from_file_with_secrets(self, get_connector_config_keys_mock: MagicMock):
        class DummyConnectorModel(BaseConnectorConfigMap):
            connector: str = "binance"
            secret_attr: Optional[SecretStr] = Field(default=None, json_schema_extra={"is_secure": True, "is_connect_key": True})

        password = "some-pass"
        Security.secrets_manager = ETHKeyFileSecretManger(password)
        cm = ClientConfigAdapter(DummyConnectorModel(secret_attr="some_secret"))
        get_connector_config_keys_mock.return_value = DummyConnectorModel()
        with TemporaryDirectory() as d:
            d = Path(d)
            config_helpers.CONNECTORS_CONF_DIR_PATH = d
            temp_file_name = get_connector_config_yml_path(cm.connector)
            save_to_yml(temp_file_name, cm)
            cm_loaded = load_connector_config_map_from_file(temp_file_name)

        self.assertEqual(cm, cm_loaded)

    def test_decrypt_config_map_secret_values(self):
        class DummySubModel(BaseClientModel):
            secret_attr: SecretStr

            class Config:
                title = "dummy_sub_model"

        class DummyModel(BaseClientModel):
            sub_model: DummySubModel

            class Config:
                title = "dummy_model"

        Security.secrets_manager = ETHKeyFileSecretManger(password="some-password")
        secret_value = "some_secret"
        encrypted_secret_value = Security.secrets_manager.encrypt_secret_value("secret_attr", secret_value)
        sub_model = DummySubModel(secret_attr=encrypted_secret_value)
        instance = ClientConfigAdapter(DummyModel(sub_model=sub_model))

        self.assertEqual(encrypted_secret_value, instance.sub_model.secret_attr.get_secret_value())

        instance._decrypt_all_internal_secrets()

        self.assertEqual(secret_value, instance.sub_model.secret_attr.get_secret_value())

    def test_list_connector_configs_ignores_non_yml_files(self):
        # JEP-196: non-*.yml files (.bak/.orig/.save/editor-temp/copies) in conf/connectors/
        # must NOT shadow connector config. A kis.yml.bak with a different body previously
        # raced kis.yml (os.scandir order is OS/inode-dependent) and silently won.
        with TemporaryDirectory() as d:
            d = Path(d)
            config_helpers.CONNECTORS_CONF_DIR_PATH = d
            (d / "kis.yml").write_text("connector: kis\nkis_ws_enabled: 'true'\n")
            # Sibling files that must be ignored regardless of scandir order.
            (d / "kis.yml.bak").write_text("connector: kis\nkis_ws_enabled: 'false'\n")
            (d / "kis.yml.orig").write_text("connector: kis\n")
            (d / "kis.yml.save").write_text("connector: kis\n")
            (d / "kis.yml.swp").write_text("connector: kis\n")
            (d / "kis copy.yaml").write_text("connector: kis\n")

            configs = list_connector_configs()

        names = sorted(p.name for p in configs)
        self.assertEqual(["kis.yml"], names)

    def test_list_connector_configs_warns_on_duplicate_connector_key(self):
        # JEP-196: two *.yml files resolving to the same `connector:` key must produce a
        # deterministic WARNING and return exactly ONE entry (the lex-first file).
        # Previously the function returned both files, making Security.decrypt_all()'s
        # last-write-wins semantics contradict the warning (which named kis.yml as winner
        # while kis_backup.yml actually loaded). Now only the winner is returned.
        with TemporaryDirectory() as d:
            d = Path(d)
            config_helpers.CONNECTORS_CONF_DIR_PATH = d
            (d / "kis.yml").write_text("connector: kis\nkis_ws_enabled: 'true'\n")
            (d / "kis_backup.yml").write_text("connector: kis\nkis_ws_enabled: 'false'\n")

            with self.assertLogs(level="WARNING") as log_ctx:
                configs = list_connector_configs()

        # (a) Warning fires.
        warning_text = "\n".join(log_ctx.output)
        self.assertTrue(any("WARNING" in line for line in log_ctx.output))

        # (b) Exactly ONE entry returned for the duplicated connector key.
        self.assertEqual(1, len(configs))

        # (c) The returned/kept entry is the lex-first file (kis.yml < kis_backup.yml).
        self.assertEqual("kis.yml", configs[0].name)

        # (d) Warning message names the actual winner (lex-first) and the ignored duplicate.
        self.assertIn("kis.yml", warning_text)
        self.assertIn("kis_backup.yml", warning_text)

    def test_list_connector_configs_no_warning_for_distinct_connectors(self):
        # JEP-196: legitimate single-file-per-connector setups must not warn.
        with TemporaryDirectory() as d:
            d = Path(d)
            config_helpers.CONNECTORS_CONF_DIR_PATH = d
            (d / "kis.yml").write_text("connector: kis\n")
            (d / "binance.yml").write_text("connector: binance\n")
            (d / "_template.yml").write_text("connector: ignored\n")
            (d / ".hidden.yml").write_text("connector: ignored\n")

            logger = logging.getLogger()
            with patch.object(logger, "warning") as warning_mock:
                configs = list_connector_configs()

            warning_mock.assert_not_called()

        names = sorted(p.name for p in configs)
        self.assertEqual(["binance.yml", "kis.yml"], names)


class ReadOnlyClientAdapterTest(unittest.TestCase):

    def test_read_only_adapter_can_be_created(self):
        adapter = ClientConfigAdapter(ClientConfigMap())
        read_only_adapter = ReadOnlyClientConfigAdapter(adapter.hb_config)

        self.assertEqual(adapter.hb_config, read_only_adapter.hb_config)

    def test_read_only_adapter_raises_exception_when_setting_value(self):
        read_only_adapter = ReadOnlyClientConfigAdapter(ClientConfigMap())
        initial_instance_id = read_only_adapter.instance_id

        with self.assertRaises(AttributeError) as context:
            read_only_adapter.instance_id = "newInstanceID"

        self.assertEqual("Cannot set an attribute on a read-only client adapter", str(context.exception))
        self.assertEqual(initial_instance_id, read_only_adapter.instance_id)
