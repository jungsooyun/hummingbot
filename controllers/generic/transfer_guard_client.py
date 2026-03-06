from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import parse_qsl, quote, urlsplit

import aiohttp


class TransferGuardError(Exception):
    def __init__(self, message: str, *, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class AuthError(TransferGuardError):
    pass


class NotFoundError(TransferGuardError):
    pass


class ConflictError(TransferGuardError):
    pass


class RateLimitError(TransferGuardError):
    pass


class ServerError(TransferGuardError):
    pass


class NetworkError(TransferGuardError):
    pass


@dataclass
class SignalResult:
    request_id: str
    state: str
    deduplicated: bool
    reason: Optional[str]


@dataclass
class ApprovalResult:
    request_id: str
    state: str


@dataclass
class RequestStatus:
    request_id: str
    state: str
    asset: Optional[str] = None
    amount: Optional[str] = None
    from_exchange: Optional[str] = None
    to_exchange: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


def build_canonical_string(
    *,
    method: str,
    path: str,
    sorted_query: str,
    body_bytes: bytes,
    timestamp: str,
    nonce: str,
) -> str:
    body_hash = hashlib.sha256(body_bytes).hexdigest()
    return "\n".join([
        method.upper(),
        path,
        sorted_query,
        body_hash,
        timestamp,
        nonce,
    ])


def _encode_sorted_query(query_items: list[tuple[str, str]]) -> str:
    if not query_items:
        return ""
    sorted_items = sorted((str(k), str(v)) for k, v in query_items)
    return "&".join(f"{quote(k, safe='')}={quote(v, safe='')}" for k, v in sorted_items)


def _extract_path_and_query(
    path: str, query: Optional[Mapping[str, Any]] = None,
) -> tuple[str, str]:
    split = urlsplit(path)
    base_path = split.path or "/"
    query_items = parse_qsl(split.query, keep_blank_values=True)
    if query:
        for key, value in query.items():
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    query_items.append((str(key), str(item)))
            elif value is not None:
                query_items.append((str(key), str(value)))
    return base_path, _encode_sorted_query(query_items)


class TransferGuardClient:
    def __init__(
        self,
        *,
        base_url: str,
        keys: Dict[str, Tuple[str, str]],
        timeout_seconds: float = 10.0,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._keys = keys
        self._timeout_seconds = timeout_seconds
        self._session = session
        self._owns_session = session is None

    async def close(self):
        if self._session is not None and self._owns_session:
            await self._session.close()
            self._session = None

    async def send_signal(
        self,
        *,
        route_key: str,
        amount: Decimal,
        signal_type: str,
        event_id: str,
        callback_url: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> SignalResult:
        payload = {
            "event_id": event_id,
            "signal_type": signal_type,
            "amount": str(amount),
            "route_key": route_key,
            "metadata": metadata or {},
        }
        if callback_url:
            payload["callback_url"] = callback_url
        data = await self._request("signal", "POST", "/v1/signals", payload=payload)
        return SignalResult(
            request_id=str(data.get("request_id")),
            state=str(data.get("state")),
            deduplicated=bool(data.get("deduplicated", False)),
            reason=data.get("reason"),
        )

    async def approve_request(self, *, request_id: str, approver_id: str) -> ApprovalResult:
        payload = {"approved": True, "approver_id": approver_id}
        data = await self._request("approval", "POST", f"/v1/approvals/{request_id}", payload=payload)
        return ApprovalResult(request_id=str(data.get("request_id")), state=str(data.get("state")))

    async def get_request(self, *, request_id: str) -> RequestStatus:
        data = await self._request("read", "GET", f"/v1/requests/{request_id}")
        return RequestStatus(
            request_id=str(data.get("id") or request_id),
            state=str(data.get("state")),
            asset=data.get("asset"),
            amount=str(data.get("amount")) if data.get("amount") is not None else None,
            from_exchange=data.get("from_exchange"),
            to_exchange=data.get("to_exchange"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )

    async def _request(
        self,
        purpose: str,
        method: str,
        path: str,
        payload: Optional[dict] = None,
        query: Optional[Mapping[str, Any]] = None,
    ) -> dict:
        if self._session is None:
            self._session = aiohttp.ClientSession()

        key_id, secret = self._keys.get(purpose, ("", ""))
        if not key_id or not secret:
            raise AuthError(f"Missing key configuration for purpose={purpose}")

        request_path, sorted_query = _extract_path_and_query(path, query)
        body_bytes = (
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
            if payload is not None
            else b""
        )
        headers = self._build_auth_headers(
            method=method,
            path=request_path,
            sorted_query=sorted_query,
            body_bytes=body_bytes,
            key_id=key_id,
            secret=secret,
        )
        if payload is not None:
            headers["Content-Type"] = "application/json"

        url = f"{self._base_url}{request_path}"
        if sorted_query:
            url = f"{url}?{sorted_query}"

        try:
            async with self._session.request(
                method.upper(),
                url,
                data=body_bytes if payload is not None else None,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self._timeout_seconds),
            ) as response:
                status = response.status
                text = await response.text()
        except (aiohttp.ClientError, TimeoutError) as e:
            raise NetworkError(f"Network error while calling {method} {request_path}: {e}") from e

        data: dict = {}
        if text:
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                data = {"detail": text}

        if 200 <= status < 300:
            return data
        self._raise_for_status(status=status, data=data, method=method, path=request_path)
        raise TransferGuardError("Unexpected request failure", status=status)

    @staticmethod
    def _build_auth_headers(
        *,
        method: str,
        path: str,
        sorted_query: str,
        body_bytes: bytes,
        key_id: str,
        secret: str,
    ) -> Dict[str, str]:
        timestamp = str(int(time.time()))
        nonce = uuid.uuid4().hex
        canonical = build_canonical_string(
            method=method,
            path=path,
            sorted_query=sorted_query,
            body_bytes=body_bytes,
            timestamp=timestamp,
            nonce=nonce,
        )
        signature = hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
        return {
            "X-QTG-Key-Id": key_id,
            "X-QTG-Timestamp": timestamp,
            "X-QTG-Nonce": nonce,
            "X-QTG-Signature": signature,
        }

    @staticmethod
    def _raise_for_status(*, status: int, data: dict, method: str, path: str):
        detail = data.get("detail") if isinstance(data, dict) else None
        message = f"QTG {method.upper()} {path} failed: status={status}, detail={detail}"
        if status == 401:
            raise AuthError(message, status=status)
        if status == 404:
            raise NotFoundError(message, status=status)
        if status == 409:
            raise ConflictError(message, status=status)
        if status == 429:
            raise RateLimitError(message, status=status)
        if status >= 500:
            raise ServerError(message, status=status)
        raise TransferGuardError(message, status=status)
