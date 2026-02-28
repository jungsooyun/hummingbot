import asyncio
import hashlib
import unittest

import jwt

from hummingbot.connector.exchange.upbit.upbit_auth import UpbitAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class TestUpbitAuth(unittest.TestCase):
    def setUp(self):
        self.access_key = "test-upbit-access"
        self.secret_key = "test-upbit-secret"
        self.auth = UpbitAuth(access_key=self.access_key, secret_key=self.secret_key)

    def test_rest_authenticate_adds_jwt_header_and_query_hash(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.upbit.com/v1/orders/open",
            params={"market": "KRW-BTC", "states[]": ["wait", "watch"]},
        )

        loop = asyncio.get_event_loop()
        authenticated = loop.run_until_complete(self.auth.rest_authenticate(request))

        auth_header = authenticated.headers["Authorization"]
        self.assertTrue(auth_header.startswith("Bearer "))

        token = auth_header.split(" ", 1)[1]
        payload = jwt.decode(token, self.secret_key, algorithms=["HS512"])

        self.assertEqual(payload["access_key"], self.access_key)
        self.assertIn("nonce", payload)
        self.assertEqual(payload["query_hash_alg"], "SHA512")

        expected_query = "market=KRW-BTC&states[]=wait&states[]=watch"
        expected_hash = hashlib.sha512(expected_query.encode("utf-8")).hexdigest()
        self.assertEqual(payload["query_hash"], expected_hash)

    def test_rest_authenticate_without_query_excludes_query_hash(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.upbit.com/v1/accounts",
        )

        loop = asyncio.get_event_loop()
        authenticated = loop.run_until_complete(self.auth.rest_authenticate(request))

        token = authenticated.headers["Authorization"].split(" ", 1)[1]
        payload = jwt.decode(token, self.secret_key, algorithms=["HS512"])

        self.assertEqual(payload["access_key"], self.access_key)
        self.assertNotIn("query_hash", payload)
        self.assertNotIn("query_hash_alg", payload)

    def test_ws_authenticate_passthrough(self):
        class DummyRequest:
            pass

        request = DummyRequest()
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.auth.ws_authenticate(request))
        self.assertIs(result, request)
