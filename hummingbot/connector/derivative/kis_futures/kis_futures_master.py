"""
KIS domestic individual stock futures — master file parser.

The master file (fo_stk_code_mts.mst) is a pipe-delimited cp949 text file
distributed as a zip from CONSTANTS.MASTER_URL.  It covers all listed stock
futures and spread contracts; this module filters down to tradeable outright
futures (info_type 1 = KOSPI, 3 = KOSDAQ) and resolves the front-month
contract for a given underlying stock code.
"""
import io
import re
import zipfile
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

import aiohttp

from hummingbot.connector.derivative.kis_futures import kis_futures_constants as CONSTANTS

# -- compile once at module load -------------------------------------------- #
_STD_CODE_RE = re.compile(r"^KR4[A-Z0-9]{9}$")
_MULT_RE = re.compile(r"\(\s*(\d+)\s*\)")
_YYYYMM_RE = re.compile(r"\b(20\d{4})\b")


@dataclass(frozen=True)
class FuturesContract:
    short_code: str       # e.g. A11607
    standard_code: str    # e.g. KR4A11670002
    underlying: str       # underlying stock code, e.g. 005930
    expiry_yyyymm: str    # e.g. 202607
    multiplier: Decimal   # contract multiplier, e.g. Decimal("10")
    name: str             # Korean name string from master file


def parse_master_bytes(raw: bytes) -> List[FuturesContract]:
    """Parse raw cp949 bytes from the KIS master file into FuturesContract list.

    Filters to info_type 1 (KOSPI stock futures) and 3 (KOSDAQ stock futures);
    excludes spreads (2, 4) and options (5, 6).

    Raises ValueError if standard codes are malformed or zero rows parse.
    """
    text = raw.decode("cp949", errors="replace")
    out: List[FuturesContract] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        cols = line.split("|")
        if len(cols) < 9:
            continue
        info_type, short, std, kor_name, _atm, _acpr, _mmsc, unas, _unas_name = cols[:9]
        # 1 = KOSPI stock future, 3 = KOSDAQ stock future; skip SP/options
        if info_type not in ("1", "3"):
            continue
        std_clean = std.strip()
        if not _STD_CODE_RE.match(std_clean):
            raise ValueError(f"master integrity: bad standard code {std!r}")
        m_mult = _MULT_RE.search(kor_name)
        multiplier = Decimal(m_mult.group(1).strip()) if m_mult else Decimal("10")
        m_exp = _YYYYMM_RE.search(kor_name)
        expiry = m_exp.group(1) if m_exp else ""
        out.append(FuturesContract(
            short_code=short.strip(),
            standard_code=std_clean,
            underlying=unas.strip(),
            expiry_yyyymm=expiry,
            multiplier=multiplier,
            name=kor_name.strip(),
        ))
    if not out:
        raise ValueError("master integrity: zero parsable rows")
    return out


def resolve_front_month(contracts: List[FuturesContract], underlying: str) -> Optional[FuturesContract]:
    """Return the nearest-expiry (front-month) contract for *underlying*.

    Returns None if no contract with a parseable expiry is found.
    """
    cands = [c for c in contracts if c.underlying == underlying and c.expiry_yyyymm]
    if not cands:
        return None
    return sorted(cands, key=lambda c: c.expiry_yyyymm)[0]


class RemapGuard:
    """Fail-closed guard: block symbol remap while open orders or a non-zero position exist.

    Remap (rolling to the next front month) during an active position would
    cause order-tracking divergence.  Callers must drain positions and cancel
    all orders before calling can_remap.
    """

    def can_remap(self, has_open_orders: bool, position_amount: Decimal) -> bool:
        """Return True only when there are no open orders AND the position is flat."""
        return (not has_open_orders) and (position_amount == Decimal("0"))


# --------------------------------------------------------------------------- #
# Zip helpers
# --------------------------------------------------------------------------- #

def _unzip(zbytes: bytes) -> bytes:
    """Extract the master inner file from a zip payload."""
    with zipfile.ZipFile(io.BytesIO(zbytes)) as z:
        return z.read(CONSTANTS.MASTER_INNER)


# --------------------------------------------------------------------------- #
# Download
# --------------------------------------------------------------------------- #

async def download_master(
        session: aiohttp.ClientSession,
        *,
        cert_fingerprint_sha256: Optional[bytes] = None,
        cached_path: Optional[str] = None,
) -> bytes:
    """Download and unzip the KIS futures master file.  Fail-closed on TLS errors.

    The master file changes daily, so we pin the SERVER CERT fingerprint via
    ``aiohttp.Fingerprint`` when provided; we NEVER disable verification globally.

    Resolution order:
    1. Normal GET (system trust store).
    2. On ClientConnectorCertificateError:
       a. Fall back to ``cached_path`` zip if provided.
       b. Retry with explicit ``cert_fingerprint_sha256`` if provided.
       c. Raise ValueError (fail-closed) — do not trade on a stale/unverified master.
    """
    try:
        async with session.get(CONSTANTS.MASTER_URL) as resp:
            resp.raise_for_status()
            return _unzip(await resp.read())
    except aiohttp.ClientConnectorCertificateError:
        pass  # fall through to recovery paths below

    if cached_path:
        with open(cached_path, "rb") as f:
            return _unzip(f.read())

    if cert_fingerprint_sha256:
        async with session.get(
            CONSTANTS.MASTER_URL,
            ssl=aiohttp.Fingerprint(cert_fingerprint_sha256),
        ) as resp:
            resp.raise_for_status()
            return _unzip(await resp.read())

    raise ValueError(
        "master TLS verification failed and no cert-pin/cache configured (fail-closed)"
    )
