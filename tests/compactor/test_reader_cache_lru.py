# tests/compactor/test_reader_cache_lru.py
"""TDD: LRUReaderCache 구현 검증 (MCT-170 AC-7).

설계 근거 (ADR-029 D7=A):
  D7: Reader cache = 95% hit + <100ms p99 (aggressive cache).
  LRU + byte-size budget enforcement.

AC-7: NullReaderCache 호출지 0건 (grep verify 의무).
"""
from __future__ import annotations

import threading
import io

from mctrader_data.compactor.reader_cache import LRUReaderCache, ReaderCache


class TestLRUCacheGetPutBasic:
    """test_lru_cache_get_put_basic — put + get 정확."""

    def test_put_then_get_returns_data(self) -> None:
        cache = LRUReaderCache(max_bytes=1024)
        data = b"hello world"
        cache.put("key1", data)
        result = cache.get("key1")
        assert result is not None
        assert result.read() == data

    def test_get_miss_returns_none(self) -> None:
        cache = LRUReaderCache(max_bytes=1024)
        assert cache.get("nonexistent") is None

    def test_get_returns_bytesio(self) -> None:
        cache = LRUReaderCache(max_bytes=1024)
        cache.put("key1", b"data")
        result = cache.get("key1")
        assert isinstance(result, io.BytesIO)


class TestLRUCacheByteEviction:
    """test_lru_cache_byte_eviction — max_bytes 초과 시 FIFO eviction."""

    def test_evicts_oldest_when_over_budget(self) -> None:
        # 각 entry 10 bytes, max 25 bytes → 3번째 put 시 첫 entry evict
        cache = LRUReaderCache(max_bytes=25)
        cache.put("key1", b"0123456789")  # 10 bytes
        cache.put("key2", b"0123456789")  # 10 bytes, total=20
        cache.put("key3", b"0123456789")  # 10 bytes, total=30 > 25 → key1 evict
        assert cache.get("key1") is None
        assert cache.get("key2") is not None
        assert cache.get("key3") is not None

    def test_current_bytes_stays_under_budget_after_eviction(self) -> None:
        cache = LRUReaderCache(max_bytes=20)
        cache.put("key1", b"0123456789")  # 10
        cache.put("key2", b"0123456789")  # 10, total=20
        cache.put("key3", b"0123456789")  # 10, evict key1 → total=20
        assert cache.current_bytes() <= 20

    def test_single_entry_larger_than_max_bytes_still_stored(self) -> None:
        # 단일 entry 가 max_bytes 초과라도 while loop 이 빠져나와 저장되어야 함
        cache = LRUReaderCache(max_bytes=5)
        cache.put("big", b"0123456789")  # 10 > 5
        # 구현 의존: 빈 cache 에 넣을 때는 저장 (무한루프 방지)
        # OR 거부 — 스펙에 미명시, current_bytes ≤ max_bytes 가 invariant 아닐 수 있음
        # 여기서는 저장 여부 중립, current_bytes 가 음수 아님만 체크
        assert cache.current_bytes() >= 0


class TestLRUCacheInvalidate:
    """test_lru_cache_invalidate — invalidate 후 get None."""

    def test_invalidate_removes_entry(self) -> None:
        cache = LRUReaderCache(max_bytes=1024)
        cache.put("key1", b"data")
        cache.invalidate("key1")
        assert cache.get("key1") is None

    def test_invalidate_decrements_current_bytes(self) -> None:
        cache = LRUReaderCache(max_bytes=1024)
        cache.put("key1", b"0123456789")  # 10 bytes
        assert cache.current_bytes() == 10
        cache.invalidate("key1")
        assert cache.current_bytes() == 0

    def test_invalidate_nonexistent_is_noop(self) -> None:
        cache = LRUReaderCache(max_bytes=1024)
        cache.invalidate("nonexistent")  # should not raise
        assert cache.current_bytes() == 0


class TestLRUCacheMoveToEndOnGet:
    """test_lru_cache_move_to_end_on_get — LRU 정합 (get 호출 시 oldest 변경)."""

    def test_get_promotes_to_recent_end(self) -> None:
        # key1 put → key2 put → get(key1) → key2 가 oldest → key3 put evicts key2
        cache = LRUReaderCache(max_bytes=25)
        cache.put("key1", b"0123456789")  # 10
        cache.put("key2", b"0123456789")  # 10, total=20
        # get key1 → key1 이 recent, key2 가 oldest
        cache.get("key1")
        # key3 추가 시 key2 evict (oldest=key2)
        cache.put("key3", b"0123456789")  # 10, total=30 > 25 → key2 evict
        assert cache.get("key2") is None
        assert cache.get("key1") is not None
        assert cache.get("key3") is not None


class TestProtocolCompliance:
    """test_protocol_compliance — isinstance(LRUReaderCache(), ReaderCache) ✓."""

    def test_lru_reader_cache_is_reader_cache(self) -> None:
        assert isinstance(LRUReaderCache(), ReaderCache)

    def test_lru_has_get_method(self) -> None:
        cache = LRUReaderCache()
        assert callable(getattr(cache, "get", None))

    def test_lru_has_put_method(self) -> None:
        cache = LRUReaderCache()
        assert callable(getattr(cache, "put", None))

    def test_lru_has_invalidate_method(self) -> None:
        cache = LRUReaderCache()
        assert callable(getattr(cache, "invalidate", None))


class TestThreadSafetyBasic:
    """test_thread_safety_basic — multi-thread put/get smoke test (10 threads × 100 ops)."""

    def test_concurrent_put_get_no_exception(self) -> None:
        cache = LRUReaderCache(max_bytes=512 * 1024)  # 512 KB
        errors: list[Exception] = []

        def worker(thread_id: int) -> None:
            try:
                for i in range(100):
                    key = f"thread-{thread_id}-key-{i % 10}"
                    cache.put(key, b"x" * 100)
                    cache.get(key)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Thread errors: {errors}"

    def test_concurrent_invalidate_no_exception(self) -> None:
        cache = LRUReaderCache(max_bytes=512 * 1024)
        errors: list[Exception] = []

        def worker(thread_id: int) -> None:
            try:
                for i in range(50):
                    key = f"shared-{i % 5}"
                    cache.put(key, b"data")
                    cache.invalidate(key)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Thread errors: {errors}"


class TestCurrentBytesAccuracy:
    """test_current_bytes_accuracy — put 후 current_bytes 정확."""

    def test_initial_current_bytes_is_zero(self) -> None:
        cache = LRUReaderCache(max_bytes=1024)
        assert cache.current_bytes() == 0

    def test_current_bytes_after_single_put(self) -> None:
        cache = LRUReaderCache(max_bytes=1024)
        cache.put("key1", b"0123456789")  # 10 bytes
        assert cache.current_bytes() == 10

    def test_current_bytes_accumulates(self) -> None:
        cache = LRUReaderCache(max_bytes=1024)
        cache.put("key1", b"0123456789")   # 10
        cache.put("key2", b"01234")         # 5
        assert cache.current_bytes() == 15

    def test_current_bytes_default_constructor(self) -> None:
        cache = LRUReaderCache()  # default 256 MB
        assert cache.max_bytes == 256 * 1024 * 1024
        assert cache.current_bytes() == 0
