import unittest
from decimal import Decimal

from hummingbot.data_feed.fair_fx.toss_fx_client import TossFxClient, TossFxError


class FakeResp:
    def __init__(self, status, payload=None, text=""):
        self.status = status
        self._payload = payload
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._payload

    async def text(self):
        return self._text


class FakeSession:
    def __init__(self, token_payload=None, rate_payload=None, token_status=200, rate_status=200):
        self.token_payload = token_payload or {
            "access_token": "tok", "token_type": "Bearer", "expires_in": 86400,
        }
        self.rate_payload = rate_payload or {"result": {"midRate": "1375", "rate": "1380.5"}}
        self.token_status = token_status
        self.rate_status = rate_status
        self.post_calls = 0
        self.get_calls = 0
        self.last_post_headers = None
        self.last_get_headers = None
        self.closed = False

    def post(self, url, data=None, headers=None):
        self.post_calls += 1
        self.last_post_headers = headers
        return FakeResp(self.token_status, self.token_payload, text="token-err")

    def get(self, url, params=None, headers=None):
        self.get_calls += 1
        self.last_get_headers = headers
        return FakeResp(self.rate_status, self.rate_payload, text="rate-err")

    async def close(self):
        self.closed = True


class TossFxClientTest(unittest.IsolatedAsyncioTestCase):
    async def test_requests_send_browser_user_agent(self):
        # Toss sits behind Cloudflare; the default aiohttp UA gets intermittently
        # WAF-blocked (403 "request could not be satisfied"). Both the token POST
        # and the rate GET must carry a browser-like User-Agent + Accept: json.
        sess = FakeSession()
        c = TossFxClient("id", "sec", http_factory=lambda: sess)
        await c.fetch_exchange_rate()
        for hdrs in (sess.last_post_headers, sess.last_get_headers):
            self.assertIsNotNone(hdrs)
            self.assertIn("User-Agent", hdrs)
            self.assertIn("Mozilla", hdrs["User-Agent"])
            self.assertEqual("application/json", hdrs.get("Accept"))
        # GET still carries the bearer token alongside the default headers.
        self.assertTrue(sess.last_get_headers["Authorization"].startswith("Bearer "))

    async def test_requests_pin_non_zstd_accept_encoding(self):
        # JEP-285: Toss sits behind Cloudflare, which began serving Content-Encoding: zstd
        # that the bot's aiohttp cannot decode -> "Can not decode content-encoding: zstd"
        # (or a JSON "Unterminated string" on the raw zstd bytes) -> FX poll fails ->
        # data-readiness gate HOLDs quoting. Pin Accept-Encoding to encodings aiohttp can
        # decode so Cloudflare negotiates gzip/deflate, never zstd. Both the token POST and
        # the rate GET must carry it.
        sess = FakeSession()
        c = TossFxClient("id", "sec", http_factory=lambda: sess)
        await c.fetch_exchange_rate()
        for hdrs in (sess.last_post_headers, sess.last_get_headers):
            self.assertIsNotNone(hdrs)
            self.assertIn("Accept-Encoding", hdrs)
            self.assertNotIn("zstd", hdrs["Accept-Encoding"])
            self.assertIn("gzip", hdrs["Accept-Encoding"])

    async def test_token_cached_within_ttl(self):
        sess = FakeSession()
        c = TossFxClient("id", "sec", http_factory=lambda: sess)
        await c.fetch_exchange_rate()
        await c.fetch_exchange_rate()
        self.assertEqual(sess.post_calls, 1)   # one token issuance
        self.assertEqual(sess.get_calls, 2)    # two rate fetches

    async def test_token_refreshed_after_expiry(self):
        sess = FakeSession(token_payload={"access_token": "tok", "token_type": "Bearer", "expires_in": 100})
        c = TossFxClient("id", "sec", http_factory=lambda: sess, token_ttl_buffer_s=60.0)
        clock = [1000.0]
        c._now = lambda: clock[0]
        await c.fetch_exchange_rate()
        self.assertEqual(sess.post_calls, 1)
        clock[0] = 1000.0 + 41   # usable TTL = 100 - 60 = 40s; advance past it
        await c.fetch_exchange_rate()
        self.assertEqual(sess.post_calls, 2)

    async def test_concurrent_calls_single_token(self):
        import asyncio
        sess = FakeSession()
        c = TossFxClient("id", "sec", http_factory=lambda: sess)
        await asyncio.gather(*[c.fetch_exchange_rate() for _ in range(8)])
        self.assertEqual(sess.post_calls, 1)

    async def test_parse_rate_from_documented_field(self):
        sess = FakeSession(rate_payload={"result": {"midRate": "1375", "rate": "1380.5"}})
        c = TossFxClient("id", "sec", http_factory=lambda: sess)
        rate = await c.fetch_exchange_rate()
        self.assertEqual(rate, Decimal("1375"))   # midRate, not rate

    async def test_rate_http_error_raises(self):
        sess = FakeSession(rate_status=500)
        c = TossFxClient("id", "sec", http_factory=lambda: sess)
        with self.assertRaises(TossFxError):
            await c.fetch_exchange_rate()

    async def test_token_http_error_raises(self):
        sess = FakeSession(token_status=401)
        c = TossFxClient("id", "sec", http_factory=lambda: sess)
        with self.assertRaises(TossFxError):
            await c.fetch_exchange_rate()

    async def test_rate_401_invalidates_token_and_retries(self):
        # Toss can expire a token EARLIER than the issued `expires_in` claims. When the
        # rate GET 401s with a token our cache still considers valid, the client must
        # invalidate the cached token, re-issue, and retry ONCE -> success.
        class FlakySession(FakeSession):
            def get(self, url, params=None, headers=None):
                self.get_calls += 1
                self.last_get_headers = headers
                status = 401 if self.get_calls == 1 else 200
                return FakeResp(status, self.rate_payload, text="invalid-token")

        sess = FlakySession()
        c = TossFxClient("id", "sec", http_factory=lambda: sess)
        rate = await c.fetch_exchange_rate()
        self.assertEqual(rate, Decimal("1375"))
        self.assertEqual(sess.get_calls, 2)    # first 401, retried 200
        self.assertEqual(sess.post_calls, 2)   # token re-issued after invalidation

    async def test_rate_persistent_401_raises_after_single_retry(self):
        # A genuinely bad credential 401s on every attempt — retry ONCE then surface
        # the error (no infinite loop).
        sess = FakeSession(rate_status=401)
        c = TossFxClient("id", "sec", http_factory=lambda: sess)
        with self.assertRaises(TossFxError):
            await c.fetch_exchange_rate()
        self.assertEqual(sess.get_calls, 2)    # one retry, then give up
        self.assertEqual(sess.post_calls, 2)


if __name__ == "__main__":
    unittest.main()
