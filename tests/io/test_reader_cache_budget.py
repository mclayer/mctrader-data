"""byte budget enforcement test — ReaderCache max_bytes param (MCT-170 Phase 2).

Tests:
- max_bytes=None → unlimited (기존 behavior preserve)
- max_bytes 초과 시 LRU eviction by byte size
- current_bytes() 정확
- put 후 eviction 순서 (가장 오래된 entry 먼저)
- 동일 key 갱신 시 current_bytes 정확
- flush_all() 후 current_bytes == 0
"""

from __future__ import annotations

import pytest

from mctrader_data.io.reader_cache import ReaderCache


class TestBytesBudgetUnlimited:
    """max_bytes=None — unlimited (MCT-154 backward compat)."""

    def test_unlimited_accepts_any_size(self):
        """max_bytes=None 시 byte size 제한 없이 put 허용."""
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        large_value = b"x" * 10_000_000  # 10 MB
        cache.put("big_key", large_value)
        assert cache.get("big_key") == large_value

    def test_current_bytes_tracks_even_unlimited(self):
        """max_bytes=None 시에도 current_bytes() 정확히 추적."""
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        cache.put("a", b"hello")
        cache.put("b", b"world!")
        assert cache.current_bytes() == 5 + 6  # "hello" + "world!"

    def test_current_bytes_zero_initially(self):
        """초기 current_bytes == 0."""
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        assert cache.current_bytes() == 0


class TestBytesBudgetEnforcement:
    """max_bytes 설정 시 eviction 동작."""

    def test_put_within_budget_no_eviction(self):
        """budget 이내 put → eviction 없음."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0, max_bytes=100)
        cache.put("a", b"x" * 40)
        cache.put("b", b"x" * 40)
        assert cache.current_bytes() == 80
        assert cache.get("a") is not None
        assert cache.get("b") is not None

    def test_put_exceeds_budget_evicts_oldest(self):
        """budget 초과 시 가장 오래된 entry 먼저 evict."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0, max_bytes=50)
        cache.put("first", b"x" * 30)   # 30 bytes
        cache.put("second", b"x" * 30)  # 60 bytes total → evict "first"
        # "first" evicted, "second" remains
        assert cache.get("first") is None
        assert cache.get("second") is not None
        assert cache.current_bytes() == 30

    def test_put_single_large_value_evicts_all(self):
        """단일 대형 value 가 전체 기존 entry evict."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0, max_bytes=100)
        cache.put("a", b"x" * 30)
        cache.put("b", b"x" * 30)
        cache.put("c", b"x" * 30)
        # 90 bytes now; put 80-byte value → evict until space available
        cache.put("big", b"x" * 80)
        # a, b, c all evicted; "big" remains
        assert cache.get("a") is None
        assert cache.get("b") is None
        assert cache.get("c") is None
        assert cache.get("big") is not None
        assert cache.current_bytes() == 80

    def test_current_bytes_exact_after_multiple_puts(self):
        """여러 put 후 current_bytes 정확."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0, max_bytes=200)
        cache.put("a", b"x" * 50)
        cache.put("b", b"x" * 70)
        cache.put("c", b"x" * 30)
        assert cache.current_bytes() == 150

    def test_update_existing_key_adjusts_bytes(self):
        """동일 key 갱신 시 current_bytes 정확히 업데이트."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0, max_bytes=200)
        cache.put("key", b"x" * 50)
        assert cache.current_bytes() == 50
        cache.put("key", b"x" * 80)
        assert cache.current_bytes() == 80

    def test_flush_all_resets_current_bytes(self):
        """flush_all() 후 current_bytes == 0."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0, max_bytes=200)
        cache.put("a", b"x" * 50)
        cache.put("b", b"x" * 60)
        cache.flush_all()
        assert cache.current_bytes() == 0

    def test_budget_exactly_at_limit(self):
        """budget 정확히 한계치 — eviction 없음."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0, max_bytes=100)
        cache.put("a", b"x" * 50)
        cache.put("b", b"x" * 50)
        # exactly at limit — no eviction
        assert cache.current_bytes() == 100
        assert cache.get("a") is not None
        assert cache.get("b") is not None

    def test_budget_one_over_evicts_lru(self):
        """budget +1 초과 → LRU 1 entry evict."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0, max_bytes=100)
        cache.put("a", b"x" * 50)  # inserted first → LRU
        cache.put("b", b"x" * 50)
        # now 101 bytes: evict "a"
        cache.put("c", b"x" * 1)
        assert cache.get("a") is None
        assert cache.get("b") is not None
        assert cache.get("c") is not None

    def test_value_larger_than_budget_stored_alone(self):
        """단일 value 가 budget 보다 크더라도 저장 가능 (전체 evict 후 저장)."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0, max_bytes=50)
        cache.put("small", b"x" * 20)
        cache.put("huge", b"x" * 100)  # larger than max_bytes itself
        # "small" evicted; "huge" stored even though > budget
        assert cache.get("small") is None
        assert cache.get("huge") is not None
        assert cache.current_bytes() == 100


class TestBytesBudgetBackwardCompat:
    """MCT-154 backward compat — max_bytes 미전달 시 기존 behavior."""

    def test_default_no_max_bytes_unlimited(self):
        """max_bytes 미설정 = unlimited — 기존 capacity 기반 eviction만."""
        cache = ReaderCache(capacity=2, ttl_seconds=300.0)
        cache.put("a", b"x" * 1000)
        cache.put("b", b"x" * 1000)
        cache.put("c", b"x" * 1000)  # capacity eviction (LRU 'a' evicted)
        assert cache.get("a") is None
        assert cache.get("b") is not None
        assert cache.get("c") is not None

    def test_hit_miss_stats_unaffected(self):
        """byte budget 추가 후 hit_ratio stats 정상."""
        cache = ReaderCache(capacity=10, ttl_seconds=300.0, max_bytes=1000)
        cache.put("k", b"data")
        cache.get("k")   # hit
        cache.get("x")   # miss
        assert cache.hit_ratio() == pytest.approx(0.5)
