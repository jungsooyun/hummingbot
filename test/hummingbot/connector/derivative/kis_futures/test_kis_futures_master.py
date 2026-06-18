import asyncio
import io
import os
import tempfile
import zipfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aioresponses import aioresponses

from hummingbot.connector.derivative.kis_futures.kis_futures_master import (
    FuturesContract,
    RemapGuard,
    download_master,
    parse_master_bytes,
    resolve_front_month,
)
from hummingbot.connector.derivative.kis_futures import kis_futures_constants as CONSTANTS

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture_bytes() -> bytes:
    with open(FIXTURES_DIR / "fo_stk_code_sample.txt", "rb") as f:
        return f.read()


def _make_zip(inner_bytes: bytes, inner_name: str = CONSTANTS.MASTER_INNER) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(inner_name, inner_bytes)
    return buf.getvalue()


class TestParseMaster:
    def test_parse_and_front_month(self):
        """Fixture has 2 005930 futures + 1 spread + 1 000660; parse + resolve = A11607."""
        contracts = parse_master_bytes(_load_fixture_bytes())
        front = resolve_front_month(contracts, "005930")
        assert front is not None
        assert front.short_code == "A11607"
        assert front.standard_code == "KR4A11670002"
        assert front.expiry_yyyymm == "202607"
        assert front.multiplier == Decimal("10")
        assert front.underlying == "005930"

    def test_front_month_skips_spreads(self):
        """info_type 2 (SP) rows must be filtered out; 000660 resolves to A16607."""
        contracts = parse_master_bytes(_load_fixture_bytes())
        # All returned contracts must be info_type 1 or 3 (spreads excluded)
        assert all(c.short_code != "D1160701" for c in contracts)
        sk = resolve_front_month(contracts, "000660")
        assert sk is not None
        assert sk.short_code == "A16607"

    def test_front_month_picks_nearest_expiry(self):
        """Both 202607 and 202608 present for 005930 — front is 202607."""
        contracts = parse_master_bytes(_load_fixture_bytes())
        samsungs = [c for c in contracts if c.underlying == "005930"]
        assert len(samsungs) == 2
        front = resolve_front_month(contracts, "005930")
        assert front.expiry_yyyymm == "202607"
        assert front.short_code == "A11607"

    def test_integrity_rejects_bad_standard_code(self):
        """Standard codes that don't match KR4[A-Z0-9]{9} must raise ValueError."""
        bad = "1|A1|BADCODE|x| |0|1|005930|x\n".encode("cp949")
        with pytest.raises(ValueError, match="master integrity"):
            parse_master_bytes(bad)

    def test_integrity_rejects_empty(self):
        """File with zero parsable rows must raise ValueError."""
        empty = b"\n   \n\n"
        with pytest.raises(ValueError, match="zero parsable rows"):
            parse_master_bytes(empty)


class TestRemapGuard:
    def test_remap_guard(self):
        guard = RemapGuard()
        assert guard.can_remap(False, Decimal("0")) is True
        assert guard.can_remap(True, Decimal("0")) is False
        assert guard.can_remap(False, Decimal("3")) is False
        assert guard.can_remap(True, Decimal("3")) is False


class TestDownloadMaster:
    def test_download_master_normal(self):
        """Normal GET returns a zip -> inner bytes returned."""
        inner = _load_fixture_bytes()
        zip_bytes = _make_zip(inner)

        async def _run():
            with aioresponses() as m:
                m.get(CONSTANTS.MASTER_URL, body=zip_bytes, status=200)
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    result = await download_master(session)
            return result

        result = asyncio.run(_run())
        # Inner bytes should decode to valid master rows
        contracts = parse_master_bytes(result)
        assert len(contracts) > 0

    def test_download_master_cached_path(self):
        """When cert error occurs, fall back to cached_path zip."""
        inner = _load_fixture_bytes()
        zip_bytes = _make_zip(inner)

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp.write(zip_bytes)
            tmp_path = tmp.name

        try:
            async def _run():
                import aiohttp
                # Monkeypatch session.get to raise ClientConnectorCertificateError
                class FakeSession:
                    def get(self, url, **kwargs):
                        return _RaiseCertError()

                class _RaiseCertError:
                    async def __aenter__(self):
                        import ssl
                        raise aiohttp.ClientConnectorCertificateError(
                            MagicMock(), ssl.SSLError("cert verify failed")
                        )
                    async def __aexit__(self, *_):
                        pass

                return await download_master(FakeSession(), cached_path=tmp_path)

            result = asyncio.run(_run())
            contracts = parse_master_bytes(result)
            assert len(contracts) > 0
        finally:
            os.unlink(tmp_path)

    def test_download_master_tls_failclosed(self):
        """No cert-pin + no cache + cert error -> ValueError (fail-closed)."""
        async def _run():
            import aiohttp

            class FakeSession:
                def get(self, url, **kwargs):
                    return _RaiseCertError()

            class _RaiseCertError:
                async def __aenter__(self):
                    import ssl
                    raise aiohttp.ClientConnectorCertificateError(
                        MagicMock(), ssl.SSLError("cert verify failed")
                    )
                async def __aexit__(self, *_):
                    pass

            await download_master(FakeSession())

        with pytest.raises(ValueError, match="fail-closed"):
            asyncio.run(_run())
