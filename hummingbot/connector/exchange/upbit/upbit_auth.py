import hashlib
import json
import time
import uuid
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlencode

import jwt

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class UpbitAuth(AuthBase):
    def __init__(self, access_key: str, secret_key: str):
        self._access_key = access_key
        self._secret_key = secret_key

    @property
    def access_key(self) -> str:
        return self._access_key

    @property
    def secret_key(self) -> str:
        return self._secret_key

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        query_string = self._build_query_string(request)

        payload: Dict[str, Any] = {
            "access_key": self._access_key,
            "nonce": str(uuid.uuid4()),
        }

        if query_string:
            payload["query_hash"] = hashlib.sha512(query_string.encode("utf-8")).hexdigest()
            payload["query_hash_alg"] = "SHA512"

        token = jwt.encode(payload, self._secret_key, algorithm="HS512")

        headers = dict(request.headers or {})
        headers["Authorization"] = f"Bearer {token}"
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    @classmethod
    def _build_query_string(cls, request: RESTRequest) -> str:
        parts = []

        if request.params:
            parts.append(cls._encode_params(request.params))

        if request.data:
            data: Any = request.data
            if isinstance(data, bytes):
                data = data.decode("utf-8")
                try:
                    parsed = json.loads(data)
                    if isinstance(parsed, dict):
                        parts.append(cls._encode_params(parsed))
                    else:
                        parts.append(data)
                except Exception:
                    parts.append(data)
            elif isinstance(data, str):
                try:
                    parsed = json.loads(data)
                    if isinstance(parsed, dict):
                        parts.append(cls._encode_params(parsed))
                    else:
                        parts.append(data)
                except Exception:
                    parts.append(data)
            elif isinstance(data, dict):
                parts.append(cls._encode_params(data))

        return "&".join(part for part in parts if part)

    @staticmethod
    def _encode_params(params: Dict[str, Any]) -> str:
        return unquote(urlencode(params, doseq=True))
