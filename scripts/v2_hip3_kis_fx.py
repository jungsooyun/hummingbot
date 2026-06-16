import os
from typing import Dict

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.data_feed.fair_fx.fair_fx_source import FairFxSource
from hummingbot.data_feed.fair_fx.fx_script_helpers import discover_fx_market, make_usdt_getter
from hummingbot.data_feed.fair_fx.toss_fx_client import TossFxClient
from scripts.v2_with_controllers import V2WithControllers, V2WithControllersConfig


class V2Hip3KisFxConfig(V2WithControllersConfig):
    script_file_name: str = os.path.basename(__file__)
    fx_poll_interval_s: float = 30.0
    fx_max_bank_age_s: float = 120.0


class V2Hip3KisFx(V2WithControllers):
    """V2WithControllers + live FairFxSource lifecycle (JEP-148).

    Owns the process-wide ``FairFxSource``: it builds the ``TossFxClient`` from the
    encrypted client config and a USDT-KRW getter from ``self.connectors``, then
    configures + starts the source BEFORE controllers tick (in the synchronous
    ``start``), and stops it AFTER ``super().on_stop()`` (controllers stop first;
    the singleton must outlive them). Mirrors ``TradingCore`` driving
    ``RateOracle.start()/stop()``. Idempotent guards in ``FairFxSource`` make this safe.
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: V2Hip3KisFxConfig):
        super().__init__(connectors, config)
        self.config = config

    def _build_fx_source_deps(self):
        cfg = HummingbotApplication.main_application().client_config_map
        client_id = cfg.toss_fx.toss_client_id.get_secret_value()
        client_secret = cfg.toss_fx.toss_client_secret.get_secret_value()
        toss_client = TossFxClient(client_id, client_secret)
        controller_configs = [c.config for c in self.controllers.values()]
        fx_connector, fx_trading_pair = discover_fx_market(controller_configs)
        usdt_getter = make_usdt_getter(self.connectors, fx_connector, fx_trading_pair)
        return toss_client, usdt_getter

    def start(self, clock: Clock, timestamp: float) -> None:
        toss_client, usdt_getter = self._build_fx_source_deps()
        source = FairFxSource.get_instance()
        source.configure(
            toss_client,
            usdt_getter,
            poll_interval_s=self.config.fx_poll_interval_s,
            max_bank_age_s=self.config.fx_max_bank_age_s,
        )
        source.start()
        super().start(clock, timestamp)

    async def on_stop(self):
        await super().on_stop()
        FairFxSource.get_instance().stop()
