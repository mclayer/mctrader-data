"""MCT-154 Phase 2 — reader cache flush + verify barrier test suite.

Coverage (Story §8.1):
- P0-4 ~ P0-6: cache flush + verify probe + partial fail (S3, AC-2)
- P1-5, P1-6: TTL eviction + LRU capacity evict
- P2-1, P2-3: prefix freeze / CacheFlushResult enum SSOT
"""

from __future__ import annotations

import time

import pytest

from mctrader_data.io.reader_cache import CacheFlushResult, ReaderCache


# ============================================================================
# P0-4 — cache flush_all normal (AC-2)
# ============================================================================


def test_cache_flush_all_normal():
    """P0-4: 100 entry populate -> flush_all() -> status='flushed' + flushed_count=100."""
    cache = ReaderCache(capacity=200, ttl_seconds=60.0)
    for i in range(100):
        cache.put(f"key-{i}", f"value-{i}".encode())

    result = cache.flush_all()
    assert isinstance(result, CacheFlushResult)
    assert result.status == "flushed"
    assert result.flushed_count == 100
    assert result.remaining_count == 0
    assert result.flush_duration_ms >= 0.0
    # cache empty after flush
    assert cache.stats()["size"] == 0


# ============================================================================
# P0-5 — cache flush verify_probe_failed (S3 박제 fail-safe)
# ============================================================================


def test_cache_flush_verify_probe_failed(monkeypatch):
    """P0-5: verify probe FAIL simulation (mocked get returns probe value) -> status='verify_probe_failed'."""
    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    cache.put("key-1", b"value-1")

    original_get = cache.get
    call_count = {"n": 0}

    def _broken_get(key):
        call_count["n"] += 1
        # 첫 verify probe call 만 가짜로 b"verify_probe" return (verify FAIL simulation)
        if key.startswith("__verify_probe_"):
            return b"verify_probe"  # probe still resident — fail
        return original_get(key)

    monkeypatch.setattr(cache, "get", _broken_get)

    result = cache.flush_all()
    assert result.status == "verify_probe_failed"
    assert result.flushed_count == 1
    assert result.verify_probe_key.startswith("__verify_probe_")


# ============================================================================
# P0-6 — flush partial failure race detection
# ============================================================================


def test_cache_flush_partial_failed_race(monkeypatch):
    """P0-6: flush 후 cache 측 entry 잔존 (race) -> status='partial_flush_failed' + remaining > 0."""
    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    cache.put("key-1", b"value-1")
    cache.put("key-2", b"value-2")

    # monkeypatch: probe key 제거 후 cache 측 race entry inject (추가 entry 가 남음)
    original_put = cache.put

    def _race_put(key, value):
        original_put(key, value)
        # verify probe put 직후 race 로 추가 entry 잔존
        if key.startswith("__verify_probe_"):
            # bypass lock for race simulation — 직접 _cache 접근
            cache._cache["race-entry"] = type(
                "E", (), {"value": b"x", "inserted_at": time.monotonic(), "last_accessed_at": time.monotonic()}
            )()

    monkeypatch.setattr(cache, "put", _race_put)
    result = cache.flush_all()

    assert result.status in ("partial_flush_failed",)
    assert result.remaining_count > 0


# ============================================================================
# P1-5 — TTL eviction lazy on access
# ============================================================================


def test_cache_ttl_eviction_lazy_on_access():
    """P1-5: TTL=0.1s + entry inserted -> 0.2s 후 get() = miss (TTL evicted)."""
    cache = ReaderCache(capacity=64, ttl_seconds=0.1)
    cache.put("key-1", b"value-1")

    # 즉시 get -> hit
    assert cache.get("key-1") == b"value-1"

    # TTL 만료 후 get -> miss
    time.sleep(0.15)
    assert cache.get("key-1") is None
    stats = cache.stats()
    assert stats["miss_count"] >= 1


# ============================================================================
# P1-6 — LRU capacity evict
# ============================================================================


def test_cache_lru_capacity_evict():
    """P1-6: capacity=3 + 4번째 put -> LRU evict (가장 오래 미접근)."""
    cache = ReaderCache(capacity=3, ttl_seconds=60.0)
    cache.put("key-1", b"v1")
    cache.put("key-2", b"v2")
    cache.put("key-3", b"v3")
    # access key-1 -> key-2 가 가장 오래 미접근 됨
    assert cache.get("key-1") == b"v1"
    cache.put("key-4", b"v4")  # capacity 도달 -> LRU (key-2) evict

    assert cache.get("key-2") is None  # evicted
    assert cache.get("key-1") == b"v1"
    assert cache.get("key-3") == b"v3"
    assert cache.get("key-4") == b"v4"


# ============================================================================
# verify_flushed() — empty cache + verify probe pass
# ============================================================================


def test_verify_flushed_empty_cache_pass():
    """verify_flushed() = empty cache + probe round-trip pass -> True."""
    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    assert cache.verify_flushed() is True


def test_verify_flushed_with_residual_returns_false():
    """verify_flushed() = 잔존 entry 검출 -> False."""
    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    cache.put("key-1", b"v1")
    assert cache.verify_flushed() is False


# ============================================================================
# P2-3 — CacheFlushResult enum SSOT
# ============================================================================


def test_cache_flush_result_enum_ssot():
    """P2-3: CacheFlushResult.status 가 §6.8.1 3 enum 만."""
    from typing import get_args, get_type_hints

    expected = {"flushed", "partial_flush_failed", "verify_probe_failed"}
    hints = get_type_hints(CacheFlushResult)
    status_args = set(get_args(hints["status"]))
    assert status_args == expected


# ============================================================================
# Idempotency — flush_all twice on empty cache (§11.6)
# ============================================================================


def test_flush_all_idempotent_on_empty():
    """§11.6: 다중 호출 시 동일 결과 (cache empty 시도 정상 flush + verify pass)."""
    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    r1 = cache.flush_all()
    r2 = cache.flush_all()
    assert r1.status == "flushed"
    assert r2.status == "flushed"
    assert r1.remaining_count == 0
    assert r2.remaining_count == 0


# ============================================================================
# hit_ratio metric (AC-5 smoke test 측 input)
# ============================================================================


def test_cache_hit_ratio_metric_for_smoke():
    """AC-5 smoke test 측 hit_ratio 정상 계산."""
    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    cache.put("k", b"v")
    cache.get("k")  # hit
    cache.get("missing")  # miss
    cache.get("k")  # hit
    ratio = cache.hit_ratio()
    assert 0.0 < ratio < 1.0
    assert ratio == pytest.approx(2 / 3, rel=0.01)


@pytest.mark.skip(
    reason=(
        "MCT-183 relocate: Gauge emit = MCT-185 cold-read cutover owner, "
        "dead-in-data no-op (채택안 A). engine.metrics absent in data repo."
    )
)
def test_stats_emits_prometheus_gauges():
    """FIX-MCT-180 engine#55 P1 (iter2 contract 정정): stats() 호출 시
    nas_reader_cache_hit_ratio / nas_reader_p99_ms Gauge producer wiring 검증.

    SCOPE: 본 test = **cold reader 경로 simulation** (ReaderCache 직접
    인스턴스화 → stats() 호출 = backtest-runner / ColdReader.run_smoke_test()
    cutover 경로 모사). **paper-engine daemon 경로 아님** — paper daemon =
    PaperRunner WS tick, ReaderCache 미인스턴스화 (Phase 0 verify 실증,
    MCT-170 reader_cache = NAS cold read 전용). hit_ratio/p99 Gauge =
    cold reader 컴포넌트 한정 metric (AC-5 contract 정정, ADR-030 §D8)."""
    from mctrader_engine.metrics import (  # noqa: F401 — skipped, engine absent
        nas_reader_cache_hit_ratio,
        nas_reader_p99_ms,
    )

    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    cache.put("k", b"v")
    cache.get("k")  # hit
    cache.get("missing")  # miss
    cache.get("k")  # hit

    # stats() 호출 전 = stale (이전 test 값 가능) — 호출 후 live 값 확정
    stats = cache.stats()

    assert nas_reader_cache_hit_ratio._value.get() == pytest.approx(
        stats["hit_ratio"]
    ), "stats() 호출 시 hit_ratio Gauge producer wiring 미동작"
    assert nas_reader_p99_ms._value.get() == pytest.approx(
        stats["p99_ms"]
    ), "stats() 호출 시 p99_ms Gauge producer wiring 미동작"
    assert stats["hit_ratio"] == pytest.approx(2 / 3, rel=0.01)


def test_p99_ms_nearest_rank_ceil_no_low_bias():
    """FIX-MCT-180 engine#55 P2: p99_ms() nearest-rank ceil — 작은 N 저편향 0.

    100 sample [1..100]ms 에서 nearest-rank p99 = rank ceil(0.99*100)=99 →
    0-indexed 98 → 99.0ms (floor 기반 int(100*0.99)-1=98 와 동일하지만,
    50 sample 에서 floor 는 idx 48=49.0, ceil 은 idx 49=50.0 — ceil 이 정합)."""
    cache = ReaderCache(capacity=4096, ttl_seconds=3600.0, latency_window_size=4096)
    # 50 sample sliding window 직접 주입 (get() miss latency ~0 회피)
    cache._read_latencies_ms.clear()
    for v in range(1, 51):  # 1.0 .. 50.0
        cache._read_latencies_ms.append(float(v))
    p99 = cache.p99_ms()
    # nearest-rank: rank = ceil(0.99*50) = ceil(49.5) = 50 → 0-indexed 49 → 50.0
    assert p99 == pytest.approx(50.0), (
        f"nearest-rank ceil p99 기대 50.0 (저편향 0), 실제 {p99}"
    )
