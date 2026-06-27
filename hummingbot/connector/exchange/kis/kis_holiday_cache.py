"""JEP-231 KRX 휴장일 디스크 캐시. stdlib only — SessionCalendar가 sync로 읽음.

KIS CTCA0903R(국내휴장일조회) 응답을 {YYYY-MM-DD: bool(거래일)}로 정규화해 영속.
kis_auth.py의 영속 패턴(atomic replace)을 미러한다.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date
from typing import Dict, List, Optional


class KisHolidayCache:
    def __init__(self, cache_path: str):
        self._path = cache_path
        self._days: Dict[str, bool] = {}   # "YYYY-MM-DD" -> True(거래일)/False(휴장)
        self._load()

    @staticmethod
    def _key(d: date) -> str:
        return d.isoformat()

    def is_trading_day(self, d: date) -> Optional[bool]:
        """캐시에 있으면 True/False, 범위 밖/미로드면 None(미상 → 상위 fail-closed)."""
        return self._days.get(self._key(d))

    def update(self, rows: List[dict]) -> None:
        """CTCA0903R output 행(bass_dt=YYYYMMDD, opnd_yn=Y/N)을 병합 + 영속."""
        for r in rows:
            raw = str(r.get("bass_dt", "")).strip()
            if len(raw) != 8:
                continue
            iso = f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
            self._days[iso] = (str(r.get("opnd_yn", "")).strip().upper() == "Y")
        self._persist()

    def _load(self) -> None:
        try:
            with open(self._path, "r") as f:
                data = json.load(f)
            if isinstance(data, dict):
                self._days = {k: bool(v) for k, v in data.get("days", {}).items()}
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            self._days = {}

    def _persist(self) -> None:
        try:
            directory = os.path.dirname(self._path) or "."
            os.makedirs(directory, exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix=".kis_holiday_", suffix=".tmp", dir=directory)
            with os.fdopen(fd, "w") as f:
                json.dump({"days": self._days}, f)
            os.replace(tmp, self._path)
        except OSError:
            pass
