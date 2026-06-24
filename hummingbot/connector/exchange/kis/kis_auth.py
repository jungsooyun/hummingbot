import asyncio
import hashlib
import json
import logging
import os
import tempfile
import time
from datetime import datetime
from typing import Dict, Optional

import aiohttp

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest

_logger = logging.getLogger(__name__)

# Safety margin (seconds) applied when deciding whether an on-disk cached
# entry is still usable -- mirrors the in-memory 60s refresh buffer.
_DISK_CACHE_MARGIN = 60

# These mirror kis_constants.py but are duplicated here to avoid importing
# the full constants module (which pulls in OrderState and heavy deps) at auth
# time.  The token endpoint must be usable before the exchange is fully wired.
_REST_URL = "https://openapi.koreainvestment.com:9443"
_REST_SANDBOX_URL = "https://openapivts.koreainvestment.com:29443"
_TOKEN_PATH = "oauth2/tokenP"
_WS_APPROVAL_PATH = "oauth2/Approval"


class KisAuth(AuthBase):
    """OAuth2 authentication for Korea Investment & Securities (KIS) API.

    KIS uses an OAuth2 client_credentials flow.  A POST to ``/oauth2/tokenP``
    with ``appkey`` and ``appsecret`` returns a Bearer access token that is
    valid for ~24 hours.  This class caches the token and refreshes it
    automatically 60 seconds before expiry.

    For WebSocket connections, KIS uses a separate ``approval_key`` obtained
    via POST ``/oauth2/Approval``.  This key is used in WS subscription
    messages, not as a header like the REST token.

    The ``rest_authenticate`` method injects the following headers into every
    authenticated request:

    * ``Authorization: Bearer {token}``
    * ``appkey``
    * ``appsecret``
    * ``custtype: P`` (personal)

    **Note:** ``tr_id`` is intentionally NOT set here -- it varies per API
    endpoint and is added by the exchange class on a per-request basis.
    """

    def __init__(self, app_key: str, app_secret: str, sandbox: bool = False,
                 initial_token: Optional[str] = None,
                 initial_approval_key: Optional[str] = None,
                 token_cache_path: Optional[str] = None):
        self._app_key = app_key
        self._app_secret = app_secret
        self._sandbox = sandbox

        # REST token cache
        self._access_token: Optional[str] = initial_token
        # If an initial token is provided, set a far-future expiry
        self._token_expires_at: float = (time.time() + 86400) if initial_token else 0.0

        # WS approval key cache
        self._approval_key: Optional[str] = initial_approval_key
        self._approval_key_expires_at: float = (time.time() + 86400) if initial_approval_key else 0.0

        # Disk persistence: resolve the cache file path. ``token_cache_path`` may
        # be a directory (the daily-token cache file is derived inside it, keyed
        # by app_key + sandbox) -- tests pass an isolated temp dir. ``None`` means
        # auto-derive from hummingbot's persistent data directory.
        self._token_cache_path: str = self._resolve_cache_path(token_cache_path)

        # Hydrate the in-memory cache from disk so a process restart within the
        # token's validity window reuses the daily token instead of re-issuing
        # (KIS rate-limits issuance to 1/60s and expects daily-token reuse).
        self._load_from_disk()

        # Single-flight locks: KIS issues at most one token (and one approval key)
        # per app_key per minute and expects the daily token to be reused. Without
        # these, the many authenticated requests fired concurrently at connector
        # startup each trigger their own token fetch, get serialised behind the
        # 1-call/60s rate limit, and stall the connector for minutes. The lock makes
        # concurrent callers await a single fetch and then share the cached token.
        self._token_lock = asyncio.Lock()
        self._approval_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Disk persistence helpers
    # ------------------------------------------------------------------

    @property
    def token_cache_path(self) -> str:
        """Absolute path of the on-disk token cache file for this account."""
        return self._token_cache_path

    def _cache_key(self) -> str:
        """A short, collision-resistant key derived from app_key + sandbox flag.

        The app secret is intentionally excluded from the filename; only a hash
        of the public app_key (plus environment flag) is used so different
        accounts / sandbox modes do not share a file.
        """
        raw = f"{self._app_key}|sandbox={self._sandbox}".encode("utf-8")
        digest = hashlib.sha256(raw).hexdigest()[:16]
        return digest

    def _default_data_dir(self) -> str:
        """Best-effort resolution of hummingbot's persistent data directory.

        Order: hummingbot.data_path() (the clean, mounted ``<prefix>/data`` dir
        in the docker deployment) -> ``<cwd>/data`` -> a temp dir.  Every step is
        guarded so connector construction never crashes on path resolution.
        """
        try:
            from hummingbot import data_path
            return data_path()
        except Exception:
            pass
        try:
            cwd_data = os.path.join(os.getcwd(), "data")
            os.makedirs(cwd_data, exist_ok=True)
            return cwd_data
        except Exception:
            return tempfile.gettempdir()

    def _resolve_cache_path(self, token_cache_path: Optional[str]) -> str:
        """Resolve the full cache *file* path.

        If ``token_cache_path`` is None, auto-derive the data dir.  If it points
        at an existing directory (or has no file extension), treat it as a
        directory and place the keyed cache file inside it.  Otherwise treat it
        as an explicit file path.
        """
        try:
            base = token_cache_path if token_cache_path is not None else self._default_data_dir()
            # Treat as a directory unless it looks like an explicit .json file.
            if os.path.isdir(base) or not base.endswith(".json"):
                return os.path.join(base, f"kis_token_cache_{self._cache_key()}.json")
            return base
        except Exception:
            # Last-resort fallback: a temp-dir file so persistence still works.
            return os.path.join(tempfile.gettempdir(), f"kis_token_cache_{self._cache_key()}.json")

    def _load_from_disk(self) -> None:
        """Hydrate in-memory token/approval fields from the on-disk cache.

        Missing file, corrupt/invalid JSON, unreadable file, or expired entries
        are silently ignored -- the connector falls back to fetching fresh and
        never crashes on construction.
        """
        try:
            with open(self._token_cache_path, "r") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return
            now = time.time()

            # Do not overwrite an explicitly-provided initial token/key.
            if self._access_token is None:
                token = data.get("access_token")
                expires_at = data.get("token_expires_at")
                if (isinstance(token, str) and token
                        and isinstance(expires_at, (int, float))
                        and now < expires_at - _DISK_CACHE_MARGIN):
                    self._access_token = token
                    self._token_expires_at = float(expires_at)

            if self._approval_key is None:
                approval = data.get("approval_key")
                appr_expires = data.get("approval_key_expires_at")
                if (isinstance(approval, str) and approval
                        and isinstance(appr_expires, (int, float))
                        and now < appr_expires - _DISK_CACHE_MARGIN):
                    self._approval_key = approval
                    self._approval_key_expires_at = float(appr_expires)
        except FileNotFoundError:
            return
        except Exception:
            # Corrupt / unreadable / unexpected: ignore and fetch fresh.
            _logger.debug("KIS token cache could not be loaded; fetching fresh.", exc_info=True)

    def _persist_to_disk(self) -> None:
        """Atomically write the current token + approval key to disk.

        Best-effort: a temp file in the same directory is written then
        ``os.replace``d into place so concurrent readers never see a partial
        file.  Any failure (read-only FS, permissions) is swallowed -- on-disk
        persistence is an optimization, not a correctness requirement.
        """
        payload = {
            "access_token": self._access_token,
            "token_expires_at": self._token_expires_at,
            "approval_key": self._approval_key,
            "approval_key_expires_at": self._approval_key_expires_at,
        }
        try:
            directory = os.path.dirname(self._token_cache_path) or "."
            os.makedirs(directory, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(prefix=".kis_token_", suffix=".tmp", dir=directory)
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(payload, f)
                os.replace(tmp_path, self._token_cache_path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception:
            _logger.debug("KIS token cache could not be persisted.", exc_info=True)

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def app_key(self) -> str:
        return self._app_key

    @property
    def app_secret(self) -> str:
        return self._app_secret

    @property
    def sandbox(self) -> bool:
        return self._sandbox

    # ------------------------------------------------------------------
    # AuthBase interface
    # ------------------------------------------------------------------

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """Add KIS authentication headers to the REST request.

        Fetches (or reuses a cached) OAuth2 access token, then sets
        ``Authorization``, ``appkey``, ``appsecret``, and ``custtype``
        headers.
        """
        token = await self._get_access_token()

        headers: Dict[str, str] = {}
        if request.headers is not None:
            headers.update(request.headers)

        headers["Authorization"] = f"Bearer {token}"
        headers["appkey"] = self._app_key
        headers["appsecret"] = self._app_secret
        headers["custtype"] = "P"

        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """KIS WebSocket auth is handled via approval_key in subscription messages.

        The WSRequest itself doesn't need header modification — the approval_key
        is embedded in each subscription JSON payload by the data sources.
        """
        return request

    # ------------------------------------------------------------------
    # OAuth2 token management
    # ------------------------------------------------------------------

    async def _get_access_token(self) -> str:
        """Return a valid access token, refreshing if necessary.

        The token is cached and reused until 60 seconds before its expiry
        time.  The expiry is parsed from the ``access_token_token_expired``
        field in the token response (format ``YYYY-MM-DD HH:MM:SS``).  If
        parsing fails, a fallback lifetime of 23 hours is used.
        """
        now = time.time()
        if self._access_token and now < self._token_expires_at - 60:
            return self._access_token

        async with self._token_lock:
            # Re-check inside the lock: a concurrent caller may have fetched while
            # we were waiting, in which case we reuse the now-cached daily token
            # instead of issuing another one.
            now = time.time()
            if self._access_token and now < self._token_expires_at - 60:
                return self._access_token

            base_url = _REST_SANDBOX_URL if self._sandbox else _REST_URL
            url = f"{base_url}/{_TOKEN_PATH}"

            body = {
                "grant_type": "client_credentials",
                "appkey": self._app_key,
                "appsecret": self._app_secret,
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=body) as response:
                    payload = await response.json()

            token = payload.get("access_token")
            if not token:
                raise RuntimeError(f"Failed to get KIS access token: {payload}")

            # Parse expiry from response, fallback to 23 hours from now
            expires_at_raw = payload.get("access_token_token_expired")
            expires_at = now + 23 * 3600  # fallback
            if isinstance(expires_at_raw, str):
                try:
                    expires_at = datetime.strptime(
                        expires_at_raw, "%Y-%m-%d %H:%M:%S"
                    ).timestamp()
                except ValueError:
                    pass  # keep fallback

            self._access_token = token
            self._token_expires_at = expires_at
            self._persist_to_disk()
            return token

    async def invalidate_token(self) -> None:
        """Force the next ``_get_access_token`` call to re-issue a fresh token.

        ``_get_access_token`` refreshes purely by the local TTL clock; it has no
        error-driven re-auth path. When the SERVER invalidates the token early
        (KIS msg_cd EGW00123 "기간이 만료된 token") the cached ``_access_token`` is
        still non-None and ``_token_expires_at`` is still in the future by the
        local clock, so the fast-path guard returns the dead token verbatim —
        a deadlock the connector cannot escape without a restart.

        Zeroing ``_token_expires_at`` (under ``_token_lock``, mirroring every
        other token-store mutation) makes the next ``_get_access_token`` fall
        through its guard and POST ``oauth2/tokenP`` for a new token. Single-flight
        is preserved: ``_get_access_token`` re-checks the guard inside the lock,
        so concurrent callers still share one fetch.

        Note: the disk cache is intentionally NOT cleared here — ``_get_access_token``
        overwrites it (``_persist_to_disk``) on the very next successful fetch, and the
        forced-reauth cooldown in the connector bounds re-issue to KIS's 1/min ceiling.
        """
        async with self._token_lock:
            self._access_token = None
            self._token_expires_at = 0.0

    # ------------------------------------------------------------------
    # WebSocket approval key
    # ------------------------------------------------------------------

    async def get_ws_approval_key(self) -> str:
        """Return a valid WebSocket approval key, fetching if necessary.

        KIS WebSocket uses a separate approval_key (different from the REST
        access token).  It is obtained via POST ``/oauth2/Approval`` with
        ``appkey`` and ``secretkey``.  The key is cached for ~24 hours.
        """
        now = time.time()
        if self._approval_key and now < self._approval_key_expires_at - 60:
            return self._approval_key

        async with self._approval_lock:
            # Re-check inside the lock (single-flight, see _get_access_token).
            now = time.time()
            if self._approval_key and now < self._approval_key_expires_at - 60:
                return self._approval_key

            base_url = _REST_SANDBOX_URL if self._sandbox else _REST_URL
            url = f"{base_url}/{_WS_APPROVAL_PATH}"

            body = {
                "grant_type": "client_credentials",
                "appkey": self._app_key,
                "secretkey": self._app_secret,
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=body) as response:
                    payload = await response.json()

            approval_key = payload.get("approval_key")
            if not approval_key:
                raise RuntimeError(f"Failed to get KIS WS approval key: {payload}")

            self._approval_key = approval_key
            # KIS approval key is valid for 1 day
            self._approval_key_expires_at = now + 86400
            self._persist_to_disk()
            return approval_key
