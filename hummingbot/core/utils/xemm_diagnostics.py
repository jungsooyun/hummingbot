import dataclasses
import json
import time
from decimal import Decimal
from enum import Enum
from logging import Logger
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import Mock

XEMM_DIAG_PREFIX = "XEMM_DIAG "
HB_DIAG_WS_RECV_TS_MS = "hb_diag_ws_recv_ts_ms"
HB_DIAG_WS_RECV_MONO_TS_MS = "hb_diag_ws_recv_mono_ts_ms"


def wall_clock_ms() -> int:
    return int(time.time() * 1000)


def monotonic_ms() -> int:
    return time.perf_counter_ns() // 1_000_000


def to_json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.name
    if dataclasses.is_dataclass(value):
        return {key: to_json_safe(val) for key, val in dataclasses.asdict(value).items()}
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mock):
        return repr(value)
    if isinstance(value, dict):
        return {str(key): to_json_safe(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_json_safe(item) for item in value]
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return to_json_safe(model_dump())
    return value


def build_xemm_diag_payload(diag_type: str, **fields: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "diag_type": diag_type,
        "wall_ts_ms": wall_clock_ms(),
        "mono_ts_ms": monotonic_ms(),
    }
    payload.update({key: to_json_safe(value) for key, value in fields.items()})
    return payload


def log_xemm_diag(logger: Logger, diag_type: str, **fields: Any) -> Dict[str, Any]:
    payload = build_xemm_diag_payload(diag_type=diag_type, **fields)
    logger.info("%s%s", XEMM_DIAG_PREFIX, json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True))
    return payload


def extract_xemm_diag_payload(log_line: str) -> Optional[Dict[str, Any]]:
    prefix_index = log_line.find(XEMM_DIAG_PREFIX)
    if prefix_index == -1:
        return None
    json_payload = log_line[prefix_index + len(XEMM_DIAG_PREFIX):].strip()
    if not json_payload:
        return None
    try:
        return json.loads(json_payload)
    except json.JSONDecodeError:
        return None
