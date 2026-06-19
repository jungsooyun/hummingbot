"""Toss Open API FX client — OAuth2 client-credentials token cache + exchange-rate fetch.

Source of truth: https://openapi.tossinvest.com/openapi-docs/latest/openapi.json
- POST /oauth2/token  (application/x-www-form-urlencoded; grant_type=client_credentials,
  client_id, client_secret) -> {access_token, token_type:"Bearer", expires_in:<seconds>}
- GET  /api/v1/exchange-rate?baseCurrency=USD&quoteCurrency=KRW
  -> {result:{rate, midRate, ...}}.  We use `midRate` (매매기준율 / 은행간 mid rate)
  as the trusted bank anchor — NOT `rate` (매수 환율, the directional buy rate).

Token-cache structure mirrors kis_auth: a single long-lived token, reused with a
TTL buffer and an asyncio.Lock double-check guard (single-flight on concurrent calls).
No X-Tossinvest-Account header — market info is account-independent.
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Callable, Optional

import aiohttp


class TossFxError(Exception):
    """Raised on non-200 from Toss (token or rate). Callers treat as stale FX."""


class TossFxClient:
    BASE = "https://openapi.tossinvest.com"
    TOKEN_PATH = "/oauth2/token"
    RATE_PATH = "/api/v1/exchange-rate"

    # Toss sits behind Cloudflare; the default aiohttp User-Agent gets intermittently
    # WAF-blocked (HTTP 403 "The request could not be satisfied"). Send a browser-like
    # UA + Accept so the requests are not flagged as bot traffic.
    _DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        http_factory: Optional[Callable[[], aiohttp.ClientSession]] = None,
        token_ttl_buffer_s: float = 60.0,
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        self._http_factory = http_factory or (lambda: aiohttp.ClientSession())
        self._buffer = float(token_ttl_buffer_s)
        self._session: Optional[aiohttp.ClientSession] = None
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._token_lock = asyncio.Lock()

    def _now(self) -> float:
        return time.time()

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = self._http_factory()
        return self._session

    async def _get_token(self) -> str:
        if self._token is not None and self._now() < self._token_expiry:
            return self._token
        async with self._token_lock:
            if self._token is not None and self._now() < self._token_expiry:
                return self._token
            session = self._ensure_session()
            data = {
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            }
            async with session.post(self.BASE + self.TOKEN_PATH, data=data, headers=dict(self._DEFAULT_HEADERS)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise TossFxError(f"token request failed: {resp.status} {body}")
                payload = await resp.json()
            self._token = payload["access_token"]
            expires_in = float(payload.get("expires_in", 0.0))
            self._token_expiry = self._now() + max(0.0, expires_in - self._buffer)
            return self._token

    def _invalidate_token(self) -> None:
        """Drop the cached token so the next _get_token() re-issues. Used when the
        rate endpoint rejects a token our cache still believed valid."""
        self._token = None
        self._token_expiry = 0.0

    async def fetch_exchange_rate(self) -> Decimal:
        """Return the bank mid rate (KRW per USD) as Decimal. Non-200 -> TossFxError.

        Toss can revoke a token EARLIER than the issued `expires_in` claims (or rotate
        keys), so a rate GET can 401 with a token our cache still considers valid. On a
        401 we invalidate the cache, re-issue once, and retry — converting the ~25-min
        FX outage (token-expiry-without-refresh) into a transparent refresh. A second
        401 surfaces the error (bad credential), with no infinite loop.
        """
        params = {"baseCurrency": "USD", "quoteCurrency": "KRW"}
        for attempt in (0, 1):
            token = await self._get_token()
            session = self._ensure_session()
            headers = {**self._DEFAULT_HEADERS, "Authorization": f"Bearer {token}"}
            async with session.get(self.BASE + self.RATE_PATH, params=params, headers=headers) as resp:
                if resp.status == 401 and attempt == 0:
                    self._invalidate_token()
                    continue
                if resp.status != 200:
                    body = await resp.text()
                    raise TossFxError(f"exchange-rate request failed: {resp.status} {body}")
                payload = await resp.json()
                return self._parse_rate(payload)

    @staticmethod
    def _parse_rate(payload: dict) -> Decimal:
        result = payload["result"]
        return Decimal(str(result["midRate"]))

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
