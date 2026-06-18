from hummingbot.client.settings import AllConnectorSettings


class TestKisFuturesDiscovery:
    def test_kis_futures_is_registered(self):
        """kis_futures must appear in AllConnectorSettings after scaffold install."""
        assert "kis_futures" in AllConnectorSettings.get_connector_settings()

    def test_kis_futures_in_derivative_set(self):
        """kis_futures must be categorised as a derivative connector."""
        assert "kis_futures" in AllConnectorSettings.get_derivative_names()
