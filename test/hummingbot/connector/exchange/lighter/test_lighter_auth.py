import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.lighter.lighter_auth import LighterAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class TestLighterAuth(unittest.TestCase):
    def setUp(self):
        self.api_key = "test_private_key_hex"
        self.account_index = 1
        self.auth = LighterAuth(
            api_key=self.api_key,
            account_index=self.account_index,
        )

    def test_rest_authenticate_get_request_no_change(self):
        """GET requests should pass through without modification."""
        request = RESTRequest(method=RESTMethod.GET, url="https://example.com/api/v1/orderBooks")
        loop = asyncio.get_event_loop()
        authenticated = loop.run_until_complete(self.auth.rest_authenticate(request))
        self.assertEqual(authenticated.url, request.url)

    def test_account_index_property(self):
        """Should expose account_index for use in queries."""
        self.assertEqual(self.auth.account_index, 1)

    def test_api_key_stored(self):
        """Should store api_key for signer client initialization."""
        self.assertEqual(self.auth.api_key, self.api_key)

    def test_auth_token_generation(self):
        """Should generate auth token via signer client."""
        mock_signer = MagicMock()
        mock_signer.create_auth_token_with_expiry = AsyncMock(return_value="test_token_123")
        self.auth._signer_client = mock_signer
        loop = asyncio.get_event_loop()
        token = loop.run_until_complete(self.auth.get_auth_token())
        self.assertEqual(token, "test_token_123")

    def test_ws_authenticate_passthrough(self):
        """WS auth should pass through (no pre-auth needed)."""
        request = MagicMock()
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.auth.ws_authenticate(request))
        self.assertEqual(result, request)

    def test_signer_client_initially_none(self):
        """Signer client should be None until explicitly set."""
        self.assertIsNone(self.auth.signer_client)

    def test_auth_token_returns_empty_without_signer(self):
        """Without signer client, get_auth_token should return empty string."""
        loop = asyncio.get_event_loop()
        token = loop.run_until_complete(self.auth.get_auth_token())
        self.assertEqual(token, "")

    def test_rest_authenticate_returns_same_request_object(self):
        """rest_authenticate should return the exact same request object."""
        request = RESTRequest(method=RESTMethod.POST, url="https://example.com/api/v1/order")
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.auth.rest_authenticate(request))
        self.assertIs(result, request)
