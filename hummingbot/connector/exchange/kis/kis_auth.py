import time
from datetime import datetime
from typing import Dict, Optional

import aiohttp

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest

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
                 initial_approval_key: Optional[str] = None):
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
        return token

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
        return approval_key
