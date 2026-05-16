"""TierReader — facade orchestration (MCT-170 Phase 2).

ADR-029 D1=C (facade priority chain) + D4=B (local fallback) + D8=C (cutoff timestamp).

Priority chain:
1. reader_cache.get(key) → hit_cache
2. dr_mode.current_state() == OPEN/UNKNOWN_TIER → NAS skip → local fallback 분기
3. tier 판정 (tier=L1/L2/L3 prefix)
   - 판정 실패 → dr_mode.set_mode(UNKNOWN_TIER) → nas_unreachable
4. tier=L1 → l1_reader.read() / tier=L2/L3 → cold_reader.read()
5. NAS GET success → dr_mode.record_success + reader_cache.put
6. NAS GET fail → dr_mode.record_failure
   - cutoff 이전 + local 파일 존재 → local_fallback (is_legacy=True)
   - 그 외 → nas_unreachable

READER_LOCAL_FALLBACK_CUTOFF env: ISO8601 (default 2026-09-01T00:00:00Z).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

logger = logging.getLogger(__name__)

_DEFAULT_CUTOFF_ISO = "2026-09-01T00:00:00Z"

TierReadStatus = Literal[
    "hit_cache",
    "hit_nas",
    "not_found",
    "nas_unreachable",
    "local_fallback",
]


@dataclass(frozen=True)
class TierReadResult:
    """TierReader read 결과.

    status enum (6종):
    - "hit_cache"      : reader_cache hit
    - "hit_nas"        : NAS GET 정상 (tier=L1/L2/L3)
    - "not_found"      : NAS 404
    - "nas_unreachable": NAS 단절 또는 DR OPEN/UNKNOWN_TIER
    - "local_fallback" : NAS 실패 + cutoff 이전 + local 파일 존재
    """

    status: TierReadStatus
    data: bytes = b""
    nas_object_key: str = ""
    read_latency_ms: float = 0.0
    cache_hit: bool = False
    is_legacy: bool = False
    tier: str = ""


def _parse_cutoff(iso: str) -> datetime:
    """ISO8601 string → datetime (UTC)."""
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _extract_tier(partition_path: str) -> str | None:
    """partition_path 에서 tier= prefix 추출.

    예: "tier=L1/..." → "L1"
        "tier=L2/..." → "L2"
        "exchange=..." → None (tier= 없음)
    """
    for part in partition_path.split("/"):
        if part.startswith("tier="):
            return part[len("tier="):]
    return None


def _extract_date(partition_path: str) -> str | None:
    """partition_path 에서 date= 추출.

    예: "date=20260101" → "20260101"
    """
    for part in partition_path.split("/"):
        if part.startswith("date="):
            return part[len("date="):]
    return None


def _parse_partition_date(partition_path: str) -> datetime | None:
    """partition 날짜 → datetime (UTC). 파싱 실패 시 None."""
    date_str = _extract_date(partition_path)
    if not date_str:
        return None
    try:
        # YYYYMMDD 또는 YYYY-MM-DD
        date_str_clean = date_str.replace("-", "")
        if len(date_str_clean) == 8:
            return datetime(
                int(date_str_clean[:4]),
                int(date_str_clean[4:6]),
                int(date_str_clean[6:8]),
                tzinfo=timezone.utc,
            )
    except Exception:
        pass
    return None


class TierReader:
    """TierReader — facade orchestration.

    DI 기반 — cold_reader, l1_reader, reader_cache, dr_mode, endpoint_router.
    cutoff_timestamp: None 시 env READER_LOCAL_FALLBACK_CUTOFF 또는 default.
    """

    def __init__(
        self,
        cold_reader: Any,
        l1_reader: Any,
        reader_cache: Any,
        dr_mode: Any,
        endpoint_router: Any,
        *,
        local_path_base: Path | None = None,
        cutoff_timestamp: datetime | None = None,
    ) -> None:
        self._cold_reader = cold_reader
        self._l1_reader = l1_reader
        self._reader_cache = reader_cache
        self._dr_mode = dr_mode
        self._endpoint_router = endpoint_router
        self._local_path_base = local_path_base

        # cutoff 결정: 인자 → env → default
        if cutoff_timestamp is not None:
            self._cutoff = cutoff_timestamp
        else:
            env_iso = os.environ.get("READER_LOCAL_FALLBACK_CUTOFF", _DEFAULT_CUTOFF_ISO)
            self._cutoff = _parse_cutoff(env_iso)

    def read(self, partition_path: str) -> TierReadResult:
        """TierReader priority chain read.

        1. reader_cache hit → hit_cache
        2. DR OPEN/UNKNOWN_TIER → local fallback 분기
        3. tier 판정 → tier=L1 → l1_reader, tier=L2/L3 → cold_reader
        4. NAS GET success → dr_mode.record_success + cache populate
        5. NAS GET fail → dr_mode.record_failure + local fallback or nas_unreachable
        """
        start = time.monotonic()

        # Step 1: cache lookup
        cached = self._reader_cache.get(partition_path)
        if cached is not None:
            return TierReadResult(
                status="hit_cache",
                data=cached,
                nas_object_key=partition_path,
                read_latency_ms=(time.monotonic() - start) * 1000,
                cache_hit=True,
            )

        # Step 2: DR state check
        dr_state = self._dr_mode.current_state()
        if dr_state in ("OPEN", "UNKNOWN_TIER"):
            return self._local_fallback_or_unreachable(
                partition_path, start, dr_state=dr_state
            )

        # Step 3: tier 판정
        tier = _extract_tier(partition_path)
        if tier is None:
            logger.error(
                "tier_reader.read tier_unparseable — path=%s → UNKNOWN_TIER",
                partition_path,
            )
            self._dr_mode.set_mode("UNKNOWN_TIER", reason="tier_unparseable")
            return TierReadResult(
                status="nas_unreachable",
                nas_object_key=partition_path,
                read_latency_ms=(time.monotonic() - start) * 1000,
                tier="",
            )

        # Step 4: tier별 NAS read
        nas_result = self._read_l1(partition_path) if tier == "L1" else self._read_cold(partition_path)

        # Step 5: success/fail 처리
        nas_success = getattr(nas_result, "status", "") in ("hit_nas", "stale_refreshed", "legacy_node_default")
        nas_data = getattr(nas_result, "data", b"")
        nas_key = getattr(nas_result, "nas_object_key", partition_path)

        if nas_success:
            self._dr_mode.record_success()
            self._reader_cache.put(partition_path, nas_data)
            return TierReadResult(
                status="hit_nas",
                data=nas_data,
                nas_object_key=nas_key,
                read_latency_ms=(time.monotonic() - start) * 1000,
                tier=tier,
            )

        # NAS fail
        nas_status = getattr(nas_result, "status", "nas_unreachable")
        self._dr_mode.record_failure(status_code=503, latency_ms=(time.monotonic() - start) * 1000)

        if nas_status == "not_found":
            return TierReadResult(
                status="not_found",
                nas_object_key=nas_key,
                read_latency_ms=(time.monotonic() - start) * 1000,
                tier=tier,
            )

        # nas_unreachable → local fallback 시도
        return self._local_fallback_or_unreachable(partition_path, start, tier=tier)

    def _read_l1(self, partition_path: str) -> Any:
        """l1_reader.read() 호출 — path에서 symbol/date/hour 파싱."""
        # L1 path layout:
        # tier=L1/exchange=DEFAULT/symbol={SYM}/date={DATE}/hour={HH}/{SYM}_{DATE}_{HH}.parquet
        parts = partition_path.split("/")
        symbol = ""
        date = ""
        hour = 0
        for part in parts:
            if part.startswith("symbol="):
                symbol = part[len("symbol="):]
            elif part.startswith("date="):
                date = part[len("date="):].replace("-", "")
            elif part.startswith("hour="):
                try:
                    hour = int(part[len("hour="):])
                except ValueError:
                    hour = 0
        if symbol and date:
            return self._l1_reader.read(symbol=symbol, date=date, hour=hour)
        # fallback: partition_path 직접 시도
        return self._l1_reader.read(symbol="UNKNOWN", date="00000000", hour=0)

    def _read_cold(self, partition_path: str) -> Any:
        """cold_reader.read() 호출."""
        return self._cold_reader.read(partition_path)

    def _local_fallback_or_unreachable(
        self,
        partition_path: str,
        start: float,
        *,
        dr_state: str = "",
        tier: str = "",
    ) -> TierReadResult:
        """local fallback 시도 or nas_unreachable.

        cutoff 이전 partition + local 파일 존재 → local_fallback
        그 외 → nas_unreachable
        """
        # UNKNOWN_TIER → fallback 거부
        if dr_state == "UNKNOWN_TIER":
            return TierReadResult(
                status="nas_unreachable",
                nas_object_key=partition_path,
                read_latency_ms=(time.monotonic() - start) * 1000,
                tier=tier,
            )

        if self._local_path_base is None:
            return TierReadResult(
                status="nas_unreachable",
                nas_object_key=partition_path,
                read_latency_ms=(time.monotonic() - start) * 1000,
                tier=tier,
            )

        # cutoff 검사
        partition_dt = _parse_partition_date(partition_path)
        if partition_dt is None or partition_dt >= self._cutoff:
            return TierReadResult(
                status="nas_unreachable",
                nas_object_key=partition_path,
                read_latency_ms=(time.monotonic() - start) * 1000,
                tier=tier,
            )

        # local 파일 조회
        local_file = self._local_path_base / partition_path
        if local_file.exists():
            try:
                data = local_file.read_bytes()
                logger.info(
                    "tier_reader.read local_fallback — path=%s size=%d",
                    partition_path,
                    len(data),
                )
                return TierReadResult(
                    status="local_fallback",
                    data=data,
                    nas_object_key=partition_path,
                    read_latency_ms=(time.monotonic() - start) * 1000,
                    is_legacy=True,
                    tier=tier,
                )
            except Exception as exc:
                logger.warning(
                    "tier_reader.read local_fallback read error — path=%s exc=%s",
                    partition_path,
                    type(exc).__name__,
                )

        return TierReadResult(
            status="nas_unreachable",
            nas_object_key=partition_path,
            read_latency_ms=(time.monotonic() - start) * 1000,
            tier=tier,
        )
