import asyncio
import json
import os
import tempfile
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
        # Isolate the on-disk token cache so tests never read/write the real
        # hummingbot data dir or leak cached tokens across tests.
        self._cache_dir = tempfile.mkdtemp(prefix="kis_auth_test_")
        self.auth = KisAuth(
            app_key=self.app_key,
            app_secret=self.app_secret,
            token_cache_path=self._cache_dir,
        )

    def tearDown(self):
        import shutil
        shutil.rmtree(getattr(self, "_cache_dir", None) or "", ignore_errors=True)

    def _new_auth(self, **kwargs):
        """KisAuth with an isolated, per-call temp cache dir (no real I/O)."""
        kwargs.setdefault("app_key", self.app_key)
        kwargs.setdefault("app_secret", self.app_secret)
        kwargs.setdefault("token_cache_path", tempfile.mkdtemp(prefix="kis_auth_test_"))
        return KisAuth(**kwargs)

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
            token_cache_path=self._cache_dir,
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
            token_cache_path=self._cache_dir,
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

    def test_get_access_token_single_flight_under_concurrency(self):
        """Concurrent callers must share a single token fetch (single-flight),
        not each issue their own. KIS rate-limits token issuance to 1 call/60s
        and expects the daily token to be reused, so a startup burst of
        authenticated requests must collapse to ONE /oauth2/tokenP call."""
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        token_response = {
            "access_token": "shared_token",
            "access_token_token_expired": future_expiry,
            "token_type": "Bearer",
            "expires_in": 86400,
        }

        def slow_response(url, json=None, **kw):
            resp = MagicMock()
            resp.status = 200

            async def _json():
                # Yield control so every concurrent caller reaches the lock before
                # the first fetch completes and caches the token.
                await asyncio.sleep(0.02)
                return token_response

            resp.json = _json
            return resp

        session_cls, captured = _make_mock_session(slow_response)

        async def _run():
            return await asyncio.gather(*[self.auth._get_access_token() for _ in range(8)])

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            tokens = loop.run_until_complete(_run())

        self.assertEqual(tokens, ["shared_token"] * 8)
        self.assertEqual(len(captured["calls"]), 1,
                         "single-flight: 8 concurrent callers must trigger only one token fetch")

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

    # ------------------------------------------------------------------
    # WebSocket approval key
    # ------------------------------------------------------------------

    def test_initial_approval_key_returned_immediately(self):
        """When initial_approval_key is provided, get_ws_approval_key()
        should return it without making any HTTP calls."""
        auth = KisAuth(
            app_key=self.app_key,
            app_secret=self.app_secret,
            initial_approval_key="pre-loaded-approval-key",
            token_cache_path=self._cache_dir,
        )

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response({"approval_key": "should-not-be-used"})
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            key = loop.run_until_complete(auth.get_ws_approval_key())

        self.assertEqual(key, "pre-loaded-approval-key")
        self.assertEqual(len(captured["calls"]), 0, "Should not make any HTTP calls")

    def test_get_ws_approval_key_fetches_from_api(self):
        """When no cached approval key exists, get_ws_approval_key() should
        POST to /oauth2/Approval with appkey and secretkey."""
        approval_response = {"approval_key": "fetched-approval-key"}

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(approval_response)
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            key = loop.run_until_complete(self.auth.get_ws_approval_key())

        self.assertEqual(key, "fetched-approval-key")
        self.assertEqual(len(captured["calls"]), 1)
        call = captured["calls"][0]
        # Verify the URL contains /oauth2/Approval
        self.assertIn("/oauth2/Approval", call["url"])
        # Verify the POST body
        self.assertEqual(call["json"]["grant_type"], "client_credentials")
        self.assertEqual(call["json"]["appkey"], self.app_key)
        self.assertEqual(call["json"]["secretkey"], self.app_secret)

    def test_get_ws_approval_key_caches_result(self):
        """Second call to get_ws_approval_key() should return the cached key
        without making a new HTTP call."""
        approval_response = {"approval_key": "cached-approval-key"}

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(approval_response)
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            key1 = loop.run_until_complete(self.auth.get_ws_approval_key())
            key2 = loop.run_until_complete(self.auth.get_ws_approval_key())

        self.assertEqual(key1, "cached-approval-key")
        self.assertEqual(key2, "cached-approval-key")
        self.assertEqual(len(captured["calls"]), 1, "Should only make one HTTP call")

    def test_get_ws_approval_key_refreshes_when_expired(self):
        """After the approval key expires, get_ws_approval_key() should make
        a new HTTP call to fetch a fresh key."""
        call_count = {"n": 0}

        def post_handler(url, json=None, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _make_mock_response({"approval_key": "old-approval-key"})
            return _make_mock_response({"approval_key": "new-approval-key"})

        session_cls, captured = _make_mock_session(post_handler)

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            key1 = loop.run_until_complete(self.auth.get_ws_approval_key())

            # Simulate expiry by setting the expiry time to the past
            self.auth._approval_key_expires_at = time.time() - 1

            key2 = loop.run_until_complete(self.auth.get_ws_approval_key())

        self.assertEqual(key1, "old-approval-key")
        self.assertEqual(key2, "new-approval-key")
        self.assertEqual(
            len(captured["calls"]), 2,
            "Expired approval key should trigger a new HTTP call",
        )

    def test_get_ws_approval_key_sandbox_url(self):
        """Sandbox mode should use the sandbox REST URL for the approval
        key endpoint."""
        auth = KisAuth(
            app_key=self.app_key,
            app_secret=self.app_secret,
            sandbox=True,
            token_cache_path=self._cache_dir,
        )
        approval_response = {"approval_key": "sandbox-approval-key"}

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(approval_response)
        )

        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            key = loop.run_until_complete(auth.get_ws_approval_key())

        self.assertEqual(key, "sandbox-approval-key")
        self.assertEqual(len(captured["calls"]), 1)
        self.assertIn("openapivts.koreainvestment.com:29443", captured["calls"][0]["url"])
        self.assertIn("/oauth2/Approval", captured["calls"][0]["url"])


class TestKisAuthDiskPersistence(unittest.TestCase):
    """Tests for the on-disk persistence of the KIS daily OAuth token and the
    WebSocket approval key.  KIS issues at most one token/approval key per
    app_key per day and expects reuse; persisting it to disk lets a process
    restart within the validity window reuse the cached token instead of
    re-issuing (which would burn the 1-call/60s rate limit and the day's token)."""

    def setUp(self):
        self.app_key = "test_app_key_disk"
        self.app_secret = "test_app_secret_disk"
        self._tmpdir = tempfile.mkdtemp(prefix="kis_token_cache_")

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _cache_file(self, auth: KisAuth) -> str:
        return auth.token_cache_path

    # ------------------------------------------------------------------
    # Token persistence
    # ------------------------------------------------------------------

    def test_valid_on_disk_token_loaded_on_construction_no_http(self):
        """A valid (non-expired) cached token on disk should be loaded at
        construction so _get_access_token() returns it with ZERO HTTP calls."""
        # Seed: write a cache file as a prior process would have.
        auth1 = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        cache_file = self._cache_file(auth1)
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, "w") as f:
            json.dump({
                "access_token": "disk_token",
                "token_expires_at": time.time() + 23 * 3600,
                "approval_key": None,
                "approval_key_expires_at": 0.0,
            }, f)

        # New process: construct a fresh KisAuth pointed at the same path.
        auth2 = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)

        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response({"access_token": "should-not-be-used"})
        )
        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            token = loop.run_until_complete(auth2._get_access_token())

        self.assertEqual(token, "disk_token")
        self.assertEqual(len(captured["calls"]), 0, "Valid on-disk token must avoid all HTTP calls")

    def test_fetch_writes_file_and_new_instance_reuses_with_zero_http(self):
        """After a real fetch the cache file is written; a NEW KisAuth at the
        same path reuses it with zero HTTP calls."""
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        token_response = {
            "access_token": "persisted_token",
            "access_token_token_expired": future_expiry,
            "token_type": "Bearer",
        }
        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(token_response)
        )

        auth1 = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            t1 = loop.run_until_complete(auth1._get_access_token())

        self.assertEqual(t1, "persisted_token")
        self.assertEqual(len(captured["calls"]), 1)
        self.assertTrue(os.path.exists(self._cache_file(auth1)))

        # Second process reuses it without HTTP.
        auth2 = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        session_cls2, captured2 = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response({"access_token": "fresh"})
        )
        with patch("aiohttp.ClientSession", session_cls2):
            t2 = loop.run_until_complete(auth2._get_access_token())

        self.assertEqual(t2, "persisted_token")
        self.assertEqual(len(captured2["calls"]), 0, "Reuse from disk must avoid HTTP")

    def test_expired_on_disk_token_triggers_fetch_and_rewrite(self):
        """An expired on-disk token must NOT be reused: a fetch happens and the
        file is rewritten with the fresh token."""
        auth_seed = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        cache_file = self._cache_file(auth_seed)
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, "w") as f:
            json.dump({
                "access_token": "expired_token",
                "token_expires_at": time.time() - 10,  # already expired
                "approval_key": None,
                "approval_key_expires_at": 0.0,
            }, f)

        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        token_response = {
            "access_token": "renewed_token",
            "access_token_token_expired": future_expiry,
        }
        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response(token_response)
        )

        auth = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            token = loop.run_until_complete(auth._get_access_token())

        self.assertEqual(token, "renewed_token")
        self.assertEqual(len(captured["calls"]), 1, "Expired disk token must trigger a fetch")
        with open(cache_file) as f:
            data = json.load(f)
        self.assertEqual(data["access_token"], "renewed_token")

    def test_corrupt_cache_file_does_not_crash_and_fetches_fresh(self):
        """A corrupt/invalid JSON cache file must be silently ignored; the
        connector fetches fresh and does not crash on construction."""
        auth_seed = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        cache_file = self._cache_file(auth_seed)
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, "w") as f:
            f.write("{not valid json at all ::::")

        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response({
                "access_token": "after_corrupt",
                "access_token_token_expired": future_expiry,
            })
        )

        # Construction must not raise despite the corrupt file.
        auth = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            token = loop.run_until_complete(auth._get_access_token())

        self.assertEqual(token, "after_corrupt")
        self.assertEqual(len(captured["calls"]), 1)

    def test_missing_cache_file_no_crash(self):
        """Missing cache file → construction succeeds and a fetch happens."""
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response({
                "access_token": "no_file_token",
                "access_token_token_expired": future_expiry,
            })
        )
        auth = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            token = loop.run_until_complete(auth._get_access_token())
        self.assertEqual(token, "no_file_token")
        self.assertEqual(len(captured["calls"]), 1)

    # ------------------------------------------------------------------
    # Approval key persistence
    # ------------------------------------------------------------------

    def test_valid_on_disk_approval_key_loaded_no_http(self):
        """A valid cached approval key on disk should be loaded at construction
        so get_ws_approval_key() returns it with ZERO HTTP calls."""
        auth_seed = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        cache_file = self._cache_file(auth_seed)
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, "w") as f:
            json.dump({
                "access_token": None,
                "token_expires_at": 0.0,
                "approval_key": "disk_approval",
                "approval_key_expires_at": time.time() + 86400,
            }, f)

        auth = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response({"approval_key": "should-not-be-used"})
        )
        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            key = loop.run_until_complete(auth.get_ws_approval_key())

        self.assertEqual(key, "disk_approval")
        self.assertEqual(len(captured["calls"]), 0)

    def test_approval_key_fetch_persists_and_new_instance_reuses(self):
        """After fetching an approval key it is persisted; a new KisAuth reuses
        it with zero HTTP calls."""
        session_cls, captured = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response({"approval_key": "persisted_approval"})
        )
        auth1 = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            k1 = loop.run_until_complete(auth1.get_ws_approval_key())

        self.assertEqual(k1, "persisted_approval")
        self.assertEqual(len(captured["calls"]), 1)

        auth2 = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        session_cls2, captured2 = _make_mock_session(
            lambda url, json=None, **kw: _make_mock_response({"approval_key": "fresh"})
        )
        with patch("aiohttp.ClientSession", session_cls2):
            k2 = loop.run_until_complete(auth2.get_ws_approval_key())

        self.assertEqual(k2, "persisted_approval")
        self.assertEqual(len(captured2["calls"]), 0)

    def test_token_and_approval_share_one_file_without_clobbering(self):
        """Persisting the approval key must not wipe a previously persisted
        token (and vice-versa) — both live in the same cache file."""
        future_expiry = (datetime.now() + timedelta(hours=23)).strftime("%Y-%m-%d %H:%M:%S")
        token_resp = {"access_token": "tok", "access_token_token_expired": future_expiry}
        approval_resp = {"approval_key": "appr"}

        def handler(url, json=None, **kw):
            if "Approval" in url:
                return _make_mock_response(approval_resp)
            return _make_mock_response(token_resp)

        session_cls, _ = _make_mock_session(handler)
        auth = KisAuth(self.app_key, self.app_secret, token_cache_path=self._tmpdir)
        with patch("aiohttp.ClientSession", session_cls):
            loop = asyncio.get_event_loop()
            loop.run_until_complete(auth._get_access_token())
            loop.run_until_complete(auth.get_ws_approval_key())

        with open(self._cache_file(auth)) as f:
            data = json.load(f)
        self.assertEqual(data["access_token"], "tok")
        self.assertEqual(data["approval_key"], "appr")

    def test_different_app_keys_use_different_files(self):
        """Distinct app_keys (and sandbox flags) must not collide on disk."""
        a1 = KisAuth("key_one", "secret", token_cache_path=self._tmpdir)
        a2 = KisAuth("key_two", "secret", token_cache_path=self._tmpdir)
        a3 = KisAuth("key_one", "secret", sandbox=True, token_cache_path=self._tmpdir)
        self.assertNotEqual(a1.token_cache_path, a2.token_cache_path)
        self.assertNotEqual(a1.token_cache_path, a3.token_cache_path)


if __name__ == "__main__":
    unittest.main()
