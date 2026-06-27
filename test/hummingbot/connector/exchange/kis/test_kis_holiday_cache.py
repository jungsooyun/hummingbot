"""JEP-231 KisHolidayCache unit tests."""
import asyncio
import json
import logging
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


# --- JEP-231 fix (MED): refresh_holiday_cache must only report success when TODAY is
# actually covered. An rt_cd=0 with empty/partial output must NOT mark the day refreshed
# (else the daily guard stops retrying and today stays fail-closed all day).

def _exchange_with_cache(tmp_path, payload):
    from hummingbot.connector.exchange.kis.kis_exchange import KisExchange
    ex = KisExchange.__new__(KisExchange)
    ex._holiday_cache = KisHolidayCache(cache_path=str(tmp_path / "h.json"))
    ex.logger = lambda: logging.getLogger("test")

    async def _api_get(**kwargs):
        return payload

    ex._api_get = _api_get
    return ex


def test_refresh_true_when_today_covered(tmp_path):
    ex = _exchange_with_cache(tmp_path, {"rt_cd": "0", "output": [{"bass_dt": "20260626", "opnd_yn": "Y"}]})
    ok = asyncio.run(ex.refresh_holiday_cache("20260626"))
    assert ok is True
    assert ex.is_trading_day(date(2026, 6, 26)) is True


def test_refresh_false_when_output_empty(tmp_path):
    ex = _exchange_with_cache(tmp_path, {"rt_cd": "0", "output": []})
    ok = asyncio.run(ex.refresh_holiday_cache("20260626"))
    assert ok is False  # today not covered → keep retry open (don't mark day done)
    assert ex.is_trading_day(date(2026, 6, 26)) is None


def test_refresh_false_when_today_not_in_output(tmp_path):
    ex = _exchange_with_cache(tmp_path, {"rt_cd": "0", "output": [{"bass_dt": "20260701", "opnd_yn": "Y"}]})
    ok = asyncio.run(ex.refresh_holiday_cache("20260626"))
    assert ok is False
    assert ex.is_trading_day(date(2026, 6, 26)) is None
    assert ex.is_trading_day(date(2026, 7, 1)) is True  # other rows still cached


def test_refresh_false_on_rt_cd_error(tmp_path):
    ex = _exchange_with_cache(tmp_path, {"rt_cd": "1", "msg1": "boom"})
    ok = asyncio.run(ex.refresh_holiday_cache("20260626"))
    assert ok is False
