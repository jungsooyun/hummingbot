import json
from decimal import Decimal

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

import aiohttp

from controllers.generic.transfer_guard_client import (
    ApprovalResult,
    AuthError,
    ConflictError,
    NetworkError,
    NotFoundError,
    RateLimitError,
    RequestStatus,
    ServerError,
    SignalResult,
    TransferGuardClient,
    _extract_path_and_query,
    build_canonical_string,
)


class _FakeResponse:
    def __init__(self, *, status, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        if isinstance(self._payload, str):
            return self._payload
        return json.dumps(self._payload)


class _FakeSession:
    def __init__(self):
        self._responses = []
        self.calls = []

    def add_response(self, *, status, payload):
        self._responses.append((status, payload))

    def request(self, method, url, data=None, headers=None, timeout=None):
        self.calls.append({"method": method, "url": url, "data": data, "headers": headers, "timeout": timeout})
        status, payload = self._responses.pop(0)
        return _FakeResponse(status=status, payload=payload)

    async def close(self):
        return None


class _FakeErrorSession(_FakeSession):
    def __init__(self, error):
        super().__init__()
        self._error = error

    def request(self, method, url, data=None, headers=None, timeout=None):
        raise self._error


class TestTransferGuardClient(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.session = _FakeSession()
        self.client = TransferGuardClient(
            base_url="http://localhost:8100",
            keys={
                "signal": ("signal-key", "signal-secret"),
                "approval": ("approval-key", "approval-secret"),
                "read": ("read-key", "read-secret"),
            },
            session=self.session,
        )

    async def asyncTearDown(self):
        await self.client.close()
        await super().asyncTearDown()

    def test_build_canonical_string(self):
        canonical = build_canonical_string(
            method="POST",
            path="/v1/signals",
            sorted_query="a=1&b=2",
            body_bytes=b'{"x":1}',
            timestamp="1700000000",
            nonce="abc",
        )
        self.assertTrue(canonical.startswith("POST\n/v1/signals\na=1&b=2\n"))
        self.assertIn("\n1700000000\nabc", canonical)

    async def test_send_signal_success(self):
        self.session.add_response(status=200, payload={
            "request_id": "req-1",
            "state": "PENDING_APPROVAL",
            "deduplicated": False,
            "reason": None,
        })
        result = await self.client.send_signal(
            route_key="upbit-bithumb-xrp",
            amount=Decimal("1.2"),
            signal_type="insufficient_balance",
            event_id="evt-1",
            metadata={"controller": "x"},
        )
        self.assertIsInstance(result, SignalResult)
        self.assertEqual("req-1", result.request_id)
        self.assertEqual("PENDING_APPROVAL", result.state)
        call = self.session.calls[0]
        self.assertEqual("POST", call["method"])
        headers = call["headers"]
        self.assertIn("X-QTG-Key-Id", headers)
        self.assertIn("X-QTG-Timestamp", headers)
        self.assertIn("X-QTG-Nonce", headers)
        self.assertIn("X-QTG-Signature", headers)

    async def test_get_request_success(self):
        self.session.add_response(status=200, payload={
            "id": "req-2",
            "state": "WITHDRAWAL_SUBMITTED",
            "asset": "XRP",
            "amount": "10",
            "from_exchange": "upbit",
            "to_exchange": "bithumb",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:05Z",
        })
        result = await self.client.get_request(request_id="req-2")
        self.assertIsInstance(result, RequestStatus)
        self.assertEqual("req-2", result.request_id)
        self.assertEqual("WITHDRAWAL_SUBMITTED", result.state)
        self.assertEqual("XRP", result.asset)

    async def test_approve_conflict_raises(self):
        self.session.add_response(status=409, payload={"detail": "request expired"})
        with self.assertRaises(ConflictError):
            await self.client.approve_request(request_id="req-3", approver_id="controller")

    async def test_get_request_404_raises_not_found(self):
        self.session.add_response(status=404, payload={"detail": "request not found"})
        with self.assertRaises(NotFoundError):
            await self.client.get_request(request_id="does-not-exist")

    # ── 9 new tests ──────────────────────────────────────────────

    def test_canonical_string_empty_body(self):
        import hashlib
        expected_hash = hashlib.sha256(b"").hexdigest()
        canonical = build_canonical_string(
            method="GET",
            path="/v1/requests/req-1",
            sorted_query="",
            body_bytes=b"",
            timestamp="1700000000",
            nonce="xyz",
        )
        lines = canonical.split("\n")
        # lines: method, path, sorted_query, body_hash, timestamp, nonce
        self.assertEqual(lines[3], expected_hash)

    def test_canonical_string_query_sorting(self):
        _path, sorted_query = _extract_path_and_query("/test?b=2&a=1")
        self.assertEqual(sorted_query, "a=1&b=2")

    async def test_send_signal_deduplicated(self):
        self.session.add_response(status=200, payload={
            "request_id": "req-dup",
            "state": "PENDING_APPROVAL",
            "deduplicated": True,
            "reason": None,
        })
        result = await self.client.send_signal(
            route_key="upbit-bithumb-xrp",
            amount=Decimal("5.0"),
            signal_type="insufficient_balance",
            event_id="evt-dup",
        )
        self.assertIsInstance(result, SignalResult)
        self.assertTrue(result.deduplicated)

    async def test_send_signal_failed_route_paused(self):
        self.session.add_response(status=200, payload={
            "request_id": "req-fail",
            "state": "FAILED",
            "deduplicated": False,
            "reason": "Route paused",
        })
        result = await self.client.send_signal(
            route_key="upbit-bithumb-xrp",
            amount=Decimal("1.0"),
            signal_type="insufficient_balance",
            event_id="evt-fail",
        )
        self.assertEqual(result.state, "FAILED")
        self.assertEqual(result.reason, "Route paused")

    async def test_approve_request_success(self):
        self.session.add_response(status=200, payload={
            "request_id": "req-4",
            "state": "READY_FOR_EXECUTION",
        })
        result = await self.client.approve_request(request_id="req-4", approver_id="controller")
        self.assertIsInstance(result, ApprovalResult)
        self.assertEqual(result.request_id, "req-4")
        self.assertEqual(result.state, "READY_FOR_EXECUTION")

    async def test_auth_error_401(self):
        self.session.add_response(status=401, payload={"detail": "invalid key"})
        with self.assertRaises(AuthError):
            await self.client.get_request(request_id="req-auth")

    async def test_rate_limit_429(self):
        self.session.add_response(status=429, payload={"detail": "rate limited"})
        with self.assertRaises(RateLimitError):
            await self.client.get_request(request_id="req-rate")

    async def test_server_error_500(self):
        self.session.add_response(status=500, payload={"detail": "internal error"})
        with self.assertRaises(ServerError):
            await self.client.get_request(request_id="req-server")

    async def test_network_error_timeout(self):
        error_session = _FakeErrorSession(aiohttp.ClientError("timeout"))
        client = TransferGuardClient(
            base_url="http://localhost:8100",
            keys={
                "signal": ("signal-key", "signal-secret"),
                "approval": ("approval-key", "approval-secret"),
                "read": ("read-key", "read-secret"),
            },
            session=error_session,
        )
        try:
            with self.assertRaises(NetworkError):
                await client.get_request(request_id="req-timeout")
        finally:
            await client.close()
