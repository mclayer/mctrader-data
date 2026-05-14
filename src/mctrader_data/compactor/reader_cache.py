# src/mctrader_data/compactor/reader_cache.py
"""reader_cache.py — D7 reader cache placeholder (MCT-169 AC-6).

MCT-169: AC-6 placeholder 선행. 실 구현 = MCT-170 scope (D7 cache 95% hit + <100ms p99).
본 파일은 인터페이스 stub — MCT-170 진입 gate (R-1 mitigation: NAS read latency 완화).

Design decision (ADR-029 D7=A):
  D7: Reader cache = 95% hit + <100ms p99 (aggressive cache).
  NAS 우선 reader 의 hot path 체감 지연 억제 (R-1: local 50us → NAS LAN 2-5ms).

MCT-170 진입 gate:
  본 파일 존재 = MCT-170 이 인터페이스 계약을 따라 LRU cache 구현 의무.
  NullReaderCache = MCT-169 동안 사용되는 no-op (latency degradation 허용).
  MCT-170 LAND 시: NullReaderCache → LRUReaderCache 교체 (DI 통해 runner.py 주입).
"""
from __future__ import annotations

from typing import IO, Protocol, runtime_checkable


@runtime_checkable
class ReaderCache(Protocol):
    """D7 reader cache interface (MCT-170 구현 계약).

    get(): nas_key → IO[bytes] | None (cache hit: stream, miss: None)
    put(): nas_key + data → cache 저장 (LRU eviction 의무)
    invalidate(): 명시적 캐시 무효화 (promote_l1() 완료 후 호출 의무)
    """

    def get(self, nas_key: str) -> IO[bytes] | None:
        """Cache hit 시 byte stream 반환, miss 시 None."""
        ...

    def put(self, nas_key: str, data: bytes) -> None:
        """NAS GET 결과 캐시 저장 (MCT-170 LRU 구현)."""
        ...

    def invalidate(self, nas_key: str) -> None:
        """nas_key 캐시 무효화 (promote_l1 완료 후 호출)."""
        ...


class NullReaderCache:
    """No-op cache — MCT-169 placeholder (R-1 mitigation: MCT-170 전 임시 허용).

    MCT-170 이 실 LRU cache 로 대체. 본 class 사용 시:
    - 캐시 miss 항상 (NAS 직접 GET)
    - L2/L3 compaction 시 NAS latency 2-5ms/segment 그대로 노출 (R-1 risk ACCEPTED)
    - MCT-170 LAND 전 용인 (별 Story scope)
    """

    def get(self, nas_key: str) -> IO[bytes] | None:
        """항상 None (cache miss) — MCT-170 placeholder."""
        return None

    def put(self, nas_key: str, data: bytes) -> None:
        """no-op — MCT-170 placeholder."""

    def invalidate(self, nas_key: str) -> None:
        """no-op — MCT-170 placeholder."""


# Module-level default: NullReaderCache (MCT-169 동안)
# MCT-170 runner.py 주입 시 LRUReaderCache 로 교체
_DEFAULT_CACHE: ReaderCache = NullReaderCache()
