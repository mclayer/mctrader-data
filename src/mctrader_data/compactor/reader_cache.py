# src/mctrader_data/compactor/reader_cache.py
"""reader_cache.py — D7 LRU reader cache (MCT-170).

ADR-029 D7=A: Reader cache 95% hit + <100ms p99 NFR 충족.
NAS 우선 reader 의 hot path 체감 지연 억제 (R-1: local 50us → NAS LAN 2-5ms).

MCT-169 placeholder (NullReaderCache) → MCT-170 LRUReaderCache 교체.
thread-safe: 모든 mutate operation threading.Lock 보호.
"""
from __future__ import annotations

import io
import threading
from collections import OrderedDict
from typing import IO, Protocol, runtime_checkable


@runtime_checkable
class ReaderCache(Protocol):
    """D7 reader cache interface (ADR-029 D7=A 계약).

    get(): nas_key → IO[bytes] | None (cache hit: stream, miss: None)
    put(): nas_key + data → cache 저장 (LRU eviction 의무)
    invalidate(): 명시적 캐시 무효화 (promote_l1() 완료 후 호출 의무)
    """

    def get(self, nas_key: str) -> IO[bytes] | None:
        """Cache hit 시 byte stream 반환, miss 시 None."""
        ...

    def put(self, nas_key: str, data: bytes) -> None:
        """NAS GET 결과 캐시 저장 (LRU eviction)."""
        ...

    def invalidate(self, nas_key: str) -> None:
        """nas_key 캐시 무효화 (promote_l1 완료 후 호출)."""
        ...


class LRUReaderCache:
    """LRU + byte-size budget enforcement reader cache (MCT-170 D7=A).

    - max_bytes: 전체 캐시 byte 상한 (default 256 MB)
    - get(): hit 시 io.BytesIO wrap + move_to_end (LRU recent), miss None
    - put(): byte budget enforcement: oldest entry 부터 evict 후 저장
    - invalidate(): pop + _current_bytes 감소
    - current_bytes(): Prometheus metric input
    - thread-safe: _lock 으로 모든 mutate 보호
    """

    def __init__(self, max_bytes: int = 256 * 1024 * 1024) -> None:
        self.max_bytes = max_bytes
        self._cache: OrderedDict[str, bytes] = OrderedDict()
        self._lock = threading.Lock()
        self._current_bytes: int = 0

    def get(self, nas_key: str) -> IO[bytes] | None:
        """Cache hit 시 io.BytesIO 반환 + LRU move_to_end, miss None."""
        with self._lock:
            if nas_key not in self._cache:
                return None
            self._cache.move_to_end(nas_key)  # most-recently-used end
            return io.BytesIO(self._cache[nas_key])

    def put(self, nas_key: str, data: bytes) -> None:
        """byte budget enforcement 후 캐시 저장.

        기존 key 덮어쓰기: 이전 entry 제거 후 재삽입.
        budget: while _current_bytes + len(data) > max_bytes: popitem(last=False)
        빈 cache + 단일 entry > max_bytes: 무한루프 방지를 위해 while 종료 후 저장.
        """
        with self._lock:
            # 기존 key 덮어쓰기 — 먼저 제거
            if nas_key in self._cache:
                self._current_bytes -= len(self._cache[nas_key])
                del self._cache[nas_key]

            # byte budget enforcement: oldest (last=False) 부터 evict
            while self._current_bytes + len(data) > self.max_bytes and self._cache:
                _, evicted = self._cache.popitem(last=False)
                self._current_bytes -= len(evicted)

            # 저장 (단일 entry 가 max_bytes 초과해도 저장 — 무한루프 방지)
            self._cache[nas_key] = data
            self._current_bytes += len(data)

    def invalidate(self, nas_key: str) -> None:
        """nas_key 제거 + _current_bytes 감소 (promote_l1 완료 후 호출)."""
        with self._lock:
            if nas_key in self._cache:
                self._current_bytes -= len(self._cache[nas_key])
                del self._cache[nas_key]

    def current_bytes(self) -> int:
        """현재 캐시 사용 bytes (Prometheus metric input)."""
        with self._lock:
            return self._current_bytes


# Module-level default: LRUReaderCache (MCT-170 — NullReaderCache placeholder 제거)
# engine 측 runner.py 에서 DI 주입 시 이 기본값 대신 외부 인스턴스 사용 가능
_DEFAULT_CACHE: ReaderCache = LRUReaderCache()
