import asyncio
import hashlib
import unittest

import jwt

from hummingbot.connector.exchange.bithumb.bithumb_auth import BithumbAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class TestBithumbAuth(unittest.TestCase):
    def setUp(self):
        self.access_key = "test-bithumb-access"
        self.secret_key = "test-bithumb-secret"
        self.auth = BithumbAuth(access_key=self.access_key, secret_key=self.secret_key)

    def test_rest_authenticate_adds_jwt_header_and_timestamp(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://api.bithumb.com/v1/orders/open",
            params={"market": "KRW-BTC"},
        )

        loop = asyncio.get_event_loop()
        authenticated = loop.run_until_complete(self.auth.rest_authenticate(request))

        auth_header = authenticated.headers["Authorization"]
        self.assertTrue(auth_header.startswith("Bearer "))

        token = auth_header.split(" ", 1)[1]
        payload = jwt.decode(token, self.secret_key, algorithms=["HS256"])

        self.assertEqual(payload["access_key"], self.access_key)
        self.assertIn("nonce", payload)
        self.assertIn("timestamp", payload)
        self.assertIsInstance(payload["timestamp"], int)
        self.assertGreater(payload["timestamp"], 0)

        expected_query = "market=KRW-BTC"
        expected_hash = hashlib.sha512(expected_query.encode("utf-8")).hexdigest()
        self.assertEqual(payload["query_hash"], expected_hash)
        self.assertEqual(payload["query_hash_alg"], "SHA512")

    def test_rest_authenticate_hashes_post_body(self):
        request = RESTRequest(
            method=RESTMethod.POST,
            url="https://api.bithumb.com/v1/orders",
            data={
                "market": "KRW-BTC",
                "side": "bid",
                "volume": "0.1",
                "price": "1000",
                "ord_type": "limit",
            },
        )

        loop = asyncio.get_event_loop()
        authenticated = loop.run_until_complete(self.auth.rest_authenticate(request))

        token = authenticated.headers["Authorization"].split(" ", 1)[1]
        payload = jwt.decode(token, self.secret_key, algorithms=["HS256"])

        expected_query = "market=KRW-BTC&side=bid&volume=0.1&price=1000&ord_type=limit"
        expected_hash = hashlib.sha512(expected_query.encode("utf-8")).hexdigest()
        self.assertEqual(payload["query_hash"], expected_hash)

    def test_ws_authenticate_passthrough(self):
        class DummyRequest:
            pass

        request = DummyRequest()
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.auth.ws_authenticate(request))
        self.assertIs(result, request)
