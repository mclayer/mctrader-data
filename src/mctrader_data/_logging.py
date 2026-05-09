"""Log helper: redact raw_json from record dicts before emit."""
from __future__ import annotations
import os


def redact_raw_json(record: dict) -> dict:
    """Return copy of record with raw_json redacted (default behaviour).

    - Normal log: raw_json field 제거
    - Error log (is_error=True): raw_json 앞 200자 + truncation suffix
    - MCTRADER_DEBUG_RAW_JSON=1: full raw_json 허용
    """
    if os.environ.get("MCTRADER_DEBUG_RAW_JSON") == "1":
        return record
    result = dict(record)
    if "raw_json" in result:
        raw = result["raw_json"]
        if raw is None:
            del result["raw_json"]
        elif isinstance(raw, str):
            # 기본 동작: 제거
            del result["raw_json"]
    return result


def redact_raw_json_error(record: dict) -> dict:
    """Error log용: raw_json 앞 200자 + '[truncated, len=N]'."""
    if os.environ.get("MCTRADER_DEBUG_RAW_JSON") == "1":
        return record
    result = dict(record)
    if "raw_json" in result:
        raw = result["raw_json"]
        if raw is not None and isinstance(raw, str) and len(raw) > 200:
            result["raw_json"] = raw[:200] + f" [truncated, len={len(raw)}]"
    return result
