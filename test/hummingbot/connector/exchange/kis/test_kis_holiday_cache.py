"""JEP-231 KisHolidayCache unit tests."""
import json
import os
from datetime import date

import pytest

from hummingbot.connector.exchange.kis.kis_holiday_cache import KisHolidayCache


def test_unknown_when_no_file(tmp_path):
    c = KisHolidayCache(cache_path=str(tmp_path / "h.json"))
    assert c.is_trading_day(date(2026, 6, 26)) is None   # 미상 → None (fail-closed 상위 처리)


def test_persist_and_read_trading_day(tmp_path):
    p = str(tmp_path / "h.json")
    c = KisHolidayCache(cache_path=p)
    # output: KIS chk-holiday 형식 — opnd_yn(영업일여부) Y/N per date
    c.update([
        {"bass_dt": "20260626", "opnd_yn": "Y"},  # 거래일
        {"bass_dt": "20260627", "opnd_yn": "N"},  # 휴장(토)
    ])
    assert c.is_trading_day(date(2026, 6, 26)) is True
    assert c.is_trading_day(date(2026, 6, 27)) is False
    # 영속: 새 인스턴스가 디스크에서 hydrate
    c2 = KisHolidayCache(cache_path=p)
    assert c2.is_trading_day(date(2026, 6, 26)) is True


def test_date_outside_cached_range_is_unknown(tmp_path):
    c = KisHolidayCache(cache_path=str(tmp_path / "h.json"))
    c.update([{"bass_dt": "20260626", "opnd_yn": "Y"}])
    assert c.is_trading_day(date(2027, 1, 1)) is None  # 캐시 범위 밖 → 미상
