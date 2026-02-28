from __future__ import annotations

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class LighterAuth(AuthBase):
    """
    Auth handler for Lighter DEX.

    Uses lighter-sdk SignerClient for transaction signing.
    REST GET requests need no auth. POST requests (orders) are signed by SignerClient.
    WS subscriptions include account_index directly (no pre-authentication).
    """

    def __init__(self, api_key: str, account_index: int):
        super().__init__()
        self._api_key = api_key
        self._account_index = account_index
        self._signer_client = None
        self._auth_token: str | None = None

    @property
    def api_key(self) -> str:
        return self._api_key

    @property
    def account_index(self) -> int:
        return self._account_index

    @property
    def signer_client(self):
        return self._signer_client

    async def get_auth_token(self) -> str:
        """Generate auth token via signer client if available."""
        if self._signer_client is not None:
            self._auth_token = await self._signer_client.create_auth_token_with_expiry()
        return self._auth_token or ""

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """Pass through - Lighter REST auth is handled by SignerClient at order level."""
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """Pass through - Lighter WS uses account_index in subscriptions, no pre-auth."""
        return request
