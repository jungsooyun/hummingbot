import asyncio
import time
import unittest
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.kis.kis_auth import KisAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


def _make_mock_response(json_data: dict) -> MagicMock:
    """Create a mock aiohttp response with the given JSON data."""
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=json_data)
    return mock_resp


def _make_mock_session(post_side_effect):
    """Create a mock aiohttp.ClientSession that supports ``async with`` for
    both the session itself and the ``session.post(...)`` context manager.

    ``post_side_effect`` is a callable(url, json=None, **kwargs) -> mock_response.
    It is called synchronously; no need for it to be async.
    """
    captured = {"calls": []}

    class _PostContextManager:
        """Mimics the aiohttp response context manager returned by session.post()."""

        def __init__(self, url, json=None, **kwargs):
            self._url = url
            self._json = json
            self._kwargs = kwargs

        async def __aenter__(self):
            captured["calls"].append({
                "url": self._url,
                "json": self._json,
                **self._kwargs,
            })
            return post_side_effect(self._url, json=self._json, **self._kwargs)

        async def __aexit__(self, *args):
            pass

    class _SessionContextManager:
        """Mimics the aiohttp.ClientSession async context manager."""

        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def post(self, url, json=None, **kwargs):
            return _PostContextManager(url, json=json, **kwargs)

    return _SessionContextManager, captured


class TestKisAuth(unittest.TestCase):
    """Tests for KisAuth OAuth2 authentication module."""

    def setUp(self):
        self.app_key = "test_app_key_12345"
        self.app_secret = "test_app_secret_67890"
        self.auth = KisAuth(
            app_key=self.app_key,
            app_secret=self.app_secret,
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    def test_app_key_property(self):
        """Should expose app_key as a read-only property."""
        self.assertEqual(self.auth.app_key, self.app_key)

    def test_app_secret_property(self):
        """Should expose app_secret as a read-only property."""
        self.assertEqual(self.auth.app_secret, self.app_secret)

    def test_sandbox_default_false(self):
        """Sandbox should default to False."""
        self.assertFalse(self.auth.sandbox)

    def test_sandbox_when_set(self):
        """Should store sandbox flag when explicitly set."""
        auth = KisAuth(
            app_key=self.app_key,
            app_secret=self.app_secret,
            sandbox=True,
        )
        self.assertTrue(auth.sandbox)

    # ------------------------------------------------------------------
    # Token request format
    # ------------------------------------------------------------------

    def test_get_access_token_posts_correct_body(self):
        """_get_access_token should POST to /oauth2/tokenP with grant_type,
        appkey, and appsecret in the request body."""
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        token_response = {
            "access_token": "eyJ0eXA_test_token",
            "access_token_token_expired": future_expiry,
            "token_type": "Bearer",
            "expires_in": 86400,
        }

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(token_response)
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            token = loop.run_until_complete(self.auth._get_access_token())

        self.assertEqual(token, "eyJ0eXA_test_token")
        self.assertEqual(len(captured["calls"]), 1)
        call = captured["calls"][0]
        # Verify the URL contains /oauth2/tokenP
        self.assertIn("/oauth2/tokenP", call["url"])
        # Verify the POST body
        self.assertEqual(call["json"]["grant_type"], "client_credentials")
        self.assertEqual(call["json"]["appkey"], self.app_key)
        self.assertEqual(call["json"]["appsecret"], self.app_secret)

    def test_get_access_token_uses_production_url(self):
        """Production auth should use the production base URL."""
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        token_response = {
            "access_token": "prod_token",
            "access_token_token_expired": future_expiry,
            "token_type": "Bearer",
            "expires_in": 86400,
        }

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(token_response)
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.auth._get_access_token())

        self.assertIn("openapi.koreainvestment.com:9443", captured["calls"][0]["url"])

    def test_get_access_token_uses_sandbox_url(self):
        """Sandbox auth should use the sandbox base URL."""
        auth = KisAuth(
            app_key=self.app_key,
            app_secret=self.app_secret,
            sandbox=True,
        )
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        token_response = {
            "access_token": "sandbox_token",
            "access_token_token_expired": future_expiry,
            "token_type": "Bearer",
            "expires_in": 86400,
        }

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(token_response)
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            loop.run_until_complete(auth._get_access_token())

        self.assertIn("openapivts.koreainvestment.com:29443", captured["calls"][0]["url"])

    def test_get_access_token_raises_on_missing_token(self):
        """Should raise RuntimeError if response doesn't contain access_token."""
        error_response = {
            "error": "invalid_client",
            "error_description": "Bad credentials",
        }

        session_cls, _ = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(error_response)
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            with self.assertRaises(RuntimeError) as ctx:
                loop.run_until_complete(self.auth._get_access_token())
            self.assertIn("Failed to get KIS access token", str(ctx.exception))

    # ------------------------------------------------------------------
    # Token caching
    # ------------------------------------------------------------------

    def test_token_caching_second_call_reuses_cached(self):
        """Second call to _get_access_token should reuse the cached token
        and NOT make another HTTP call."""
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        token_response = {
            "access_token": "cached_token_value",
            "access_token_token_expired": future_expiry,
            "token_type": "Bearer",
            "expires_in": 86400,
        }

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(token_response)
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            token1 = loop.run_until_complete(self.auth._get_access_token())
            token2 = loop.run_until_complete(self.auth._get_access_token())

        self.assertEqual(token1, "cached_token_value")
        self.assertEqual(token2, "cached_token_value")
        self.assertEqual(len(captured["calls"]), 1, "Should only make one HTTP call")

    # ------------------------------------------------------------------
    # Token refresh on expiry
    # ------------------------------------------------------------------

    def test_token_refresh_when_expired(self):
        """An expired token should trigger a new HTTP call to get a fresh token."""
        past_expiry = (datetime.now() - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")

        call_count = {"n": 0}

        def post_handler(url, json=None, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _make_mock_response({
                    "access_token": "old_token",
                    "access_token_token_expired": past_expiry,
                    "token_type": "Bearer",
                    "expires_in": 0,
                })
            return _make_mock_response({
                "access_token": "new_token",
                "access_token_token_expired": future_expiry,
                "token_type": "Bearer",
                "expires_in": 86400,
            })

        session_cls, captured = _make_mock_session(post_handler)

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            token1 = loop.run_until_complete(self.auth._get_access_token())
            token2 = loop.run_until_complete(self.auth._get_access_token())

        self.assertEqual(token1, "old_token")
        self.assertEqual(token2, "new_token")
        self.assertEqual(
            len(captured["calls"]), 2,
            "Should make two HTTP calls (expired token triggers refresh)",
        )

    def test_token_refresh_60_seconds_before_expiry(self):
        """Token should be refreshed when within 60 seconds of expiry."""
        near_expiry = (datetime.now() + timedelta(seconds=30)).strftime("%Y-%m-%d %H:%M:%S")
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")

        call_count = {"n": 0}

        def post_handler(url, json=None, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _make_mock_response({
                    "access_token": "almost_expired_token",
                    "access_token_token_expired": near_expiry,
                    "token_type": "Bearer",
                    "expires_in": 30,
                })
            return _make_mock_response({
                "access_token": "fresh_token",
                "access_token_token_expired": future_expiry,
                "token_type": "Bearer",
                "expires_in": 86400,
            })

        session_cls, captured = _make_mock_session(post_handler)

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            token1 = loop.run_until_complete(self.auth._get_access_token())
            token2 = loop.run_until_complete(self.auth._get_access_token())

        self.assertEqual(token1, "almost_expired_token")
        self.assertEqual(token2, "fresh_token")
        self.assertEqual(
            len(captured["calls"]), 2,
            "Token near expiry should trigger refresh",
        )

    def test_token_fallback_23_hour_lifetime(self):
        """If access_token_token_expired is missing, should use
        23-hour fallback lifetime and cache correctly."""
        token_response = {
            "access_token": "fallback_lifetime_token",
            # Intentionally missing access_token_token_expired
            "token_type": "Bearer",
            "expires_in": 86400,
        }

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(token_response)
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            token1 = loop.run_until_complete(self.auth._get_access_token())
            token2 = loop.run_until_complete(self.auth._get_access_token())

        self.assertEqual(token1, "fallback_lifetime_token")
        self.assertEqual(token2, "fallback_lifetime_token")
        self.assertEqual(
            len(captured["calls"]), 1,
            "Fallback lifetime should still cache the token",
        )

    def test_token_fallback_on_unparseable_expiry(self):
        """Unparseable expiry string should fall back to 23-hour lifetime."""
        token_response = {
            "access_token": "bad_expiry_token",
            "access_token_token_expired": "not-a-valid-date",
            "token_type": "Bearer",
            "expires_in": 86400,
        }

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(token_response)
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            token = loop.run_until_complete(self.auth._get_access_token())
            token2 = loop.run_until_complete(self.auth._get_access_token())

        self.assertEqual(token, "bad_expiry_token")
        self.assertEqual(
            len(captured["calls"]), 1,
            "Bad expiry should still cache with fallback lifetime",
        )

    # ------------------------------------------------------------------
    # rest_authenticate
    # ------------------------------------------------------------------

    def test_rest_authenticate_adds_correct_headers(self):
        """rest_authenticate should add Authorization, appkey, appsecret,
        and custtype headers."""
        # Pre-populate the cached token to avoid HTTP call
        self.auth._access_token = "test_bearer_token"
        self.auth._token_expires_at = time.time() + 3600

        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price-2",
        )

        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.auth.rest_authenticate(request))

        self.assertIs(result, request, "Should return the same request object")
        self.assertEqual(result.headers["Authorization"], "Bearer test_bearer_token")
        self.assertEqual(result.headers["appkey"], self.app_key)
        self.assertEqual(result.headers["appsecret"], self.app_secret)
        self.assertEqual(result.headers["custtype"], "P")

    def test_rest_authenticate_preserves_existing_headers(self):
        """rest_authenticate should preserve any existing headers on the request."""
        self.auth._access_token = "test_bearer_token"
        self.auth._token_expires_at = time.time() + 3600

        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://openapi.koreainvestment.com:9443/uapi/test",
            headers={"tr_id": "FHPST01010000", "Content-Type": "application/json"},
        )

        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.auth.rest_authenticate(request))

        # Existing headers preserved
        self.assertEqual(result.headers["tr_id"], "FHPST01010000")
        self.assertEqual(result.headers["Content-Type"], "application/json")
        # Auth headers added
        self.assertEqual(result.headers["Authorization"], "Bearer test_bearer_token")
        self.assertEqual(result.headers["appkey"], self.app_key)
        self.assertEqual(result.headers["appsecret"], self.app_secret)
        self.assertEqual(result.headers["custtype"], "P")

    def test_rest_authenticate_does_not_add_tr_id(self):
        """rest_authenticate should NOT add tr_id -- that is set per-request
        by the exchange class."""
        self.auth._access_token = "test_bearer_token"
        self.auth._token_expires_at = time.time() + 3600

        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://openapi.koreainvestment.com:9443/uapi/test",
        )

        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.auth.rest_authenticate(request))

        self.assertNotIn("tr_id", result.headers)

    def test_rest_authenticate_fetches_token_if_not_cached(self):
        """If no cached token, rest_authenticate should fetch one before
        adding headers."""
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        token_response = {
            "access_token": "freshly_fetched_token",
            "access_token_token_expired": future_expiry,
            "token_type": "Bearer",
            "expires_in": 86400,
        }

        session_cls, _ = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(token_response)
        )

        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://openapi.koreainvestment.com:9443/uapi/test",
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(self.auth.rest_authenticate(request))

        self.assertEqual(result.headers["Authorization"], "Bearer freshly_fetched_token")

    # ------------------------------------------------------------------
    # ws_authenticate (pass-through)
    # ------------------------------------------------------------------

    def test_ws_authenticate_passthrough(self):
        """WS auth should pass through unchanged (KIS has no WS auth)."""
        mock_ws_request = MagicMock()
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.auth.ws_authenticate(mock_ws_request))
        self.assertIs(result, mock_ws_request)


if __name__ == "__main__":
    unittest.main()
