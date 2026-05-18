"""_SizeGatedStreamCache — NAS GET sort-phase bytes 캐시 (MCT-203).

compactor-sort-key Story (#96) final review §6 #4: l2/l3 _compact_*_nas 가
content-derived sort 시 동일 NAS object 를 2회 full-download (2N+1 GET).
get_streaming = boto3 Body.read() 완전 읽기 후 BytesIO wrap (full download)
이므로 sort-phase bytes 가 이미 full — write/schema phase 재사용 가능.

INV-4 (MCT-163, ≤256MB peak RSS+tracemalloc delta) hard bound:
누적 cached bytes < threshold (default 128MB) 시만 캐시. 초과 key = cache
skip → get_streaming streaming fallback (현행 1-object/time 격리 동작).

behavioral invariant: cache = bytes-only. sort/run_id 로직 read-only —
caller 가 nas_keys 순서·내용 변경 0 (byte-identical output + INV-9 보존).
"""
from __future__ import annotations

import logging
from io import BytesIO
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader

_log = logging.getLogger(__name__)

_DEFAULT_THRESHOLD = 128 * 1024 * 1024  # 128MB — INV-4 256MB budget 의 1/2


class _SizeGatedStreamCache:
    """sort-phase get_streaming bytes 를 size-gate 내에서 캐시, 재access 시 재사용.

    1 _compact_*_nas 호출 = 1 인스턴스 (compaction 단위, 종료 시 GC).
    """

    def __init__(self, threshold_bytes: int = _DEFAULT_THRESHOLD) -> None:
        self._cache: dict[str, bytes] = {}
        self._total = 0
        self._threshold = threshold_bytes

    def get_or_fetch(self, nas_uploader: NASUploader, nas_key: str) -> IO[bytes]:
        """캐시 hit → fresh BytesIO(cached). miss → get_streaming full download
        후 누적 < threshold 면 적재. 항상 fresh BytesIO 반환 (seek 독립)."""
        cached = self._cache.get(nas_key)
        if cached is not None:
            return BytesIO(cached)
        from mctrader_data.nas_storage.get_streaming import get_streaming

        stream = get_streaming(nas_uploader=nas_uploader, nas_key=nas_key)
        data = stream.read()
        if self._total + len(data) <= self._threshold:
            self._cache[nas_key] = data
            self._total += len(data)
        else:
            _log.debug(
                "[_SizeGatedStreamCache] size-gate skip key=%s (총 %d + %d > %d) — streaming fallback",
                nas_key, self._total, len(data), self._threshold,
            )
        return BytesIO(data)
