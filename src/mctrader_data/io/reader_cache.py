"""LRU + TTL cache (ADR-027 D9 박제) + explicit flush API + verify barrier (S3 박제).

Responsibilities:
1. LRU eviction (capacity bound) + TTL expiry (time-based eviction) — read-through cache
2. explicit flush API (`flush_all()`) — endpoint flip 전 cache 강제 무효화 (S3, AC-2)
3. verify barrier (`verify_flushed()`) — flush 완료 verify gate (AC-2 enforcement)
4. cache hit/miss metric emit — AC-5 smoke test 측 hit ratio 측정

ADR-027 D9 직접 owner (read-through cache, "MCT-154 scope" 박제).
S3 박제 직접 enforcement — endpoint flip 전 stale cache 차단.

§8.5 active (background worker = TTL eviction lazy on access):
- TTL eviction = lazy on get() (background thread 별도 0, simplicity)
- restart 후 cache cold start (in-memory only, persistence 0) — 정상 동작 (cache miss -> NAS read)

Story MCT-154 §6.7 Cross-module contract (lesson #2 invariant):
- CacheFlushResult.status switch 의무 (caller endpoint_router.flip())
- ReadResult enum의 hit_cache 결과 propagate (caller cold_reader.read())

Story MCT-154 §6.9 placement:
- get/put = unconditional (매 read 시 호출, idempotent)
- flush_all = unconditional placement (endpoint flip 진입 직전 첫 단계, S3 enforcement)
- verify_flushed = unconditional placement (flush_all 후 즉시 호출, AC-2 verify gate)
- TTL eviction = lazy on access conditional (TTL expiry 시점 only)
"""

from __future__ import annotations

import logging
import math
import threading
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass
from typing import Literal, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CacheFlushResult:
    """cache flush 결과 — caller switch 의무 (§6.7 cross-module contract).

    status enum (3종, §6.8.1 SSOT):
    - "flushed"               : 모든 entry 무효화 + verify probe 정상 통과 (AC-2 정상 case)
    - "partial_flush_failed"  : flush 진행 중 일부 entry 만 무효화 (race or internal error)
    - "verify_probe_failed"   : flush 완료 후 verify probe FAIL

    Caller 처리 의무 (§6.7 매핑):
    - "flushed"               -> endpoint_router.flip() 정상 진행 (Phase 2)
    - "partial_flush_failed"  -> retry (budget §6.1 결정 = 3회) + retry 모두 fail 시 alert + cutover 차단
    - "verify_probe_failed"   -> alert (`EngineColdReaderCacheFlushVerifyFailed`) + cutover 차단 + 사용자 manual gate
    """

    status: Literal["flushed", "partial_flush_failed", "verify_probe_failed"]
    flushed_count: int = 0
    remaining_count: int = 0
    verify_probe_key: str = ""
    flush_duration_ms: float = 0.0


@dataclass
class CacheEntry:
    """cache entry — value + insert/access timestamp (TTL/LRU eviction 계산).

    Fields:
    - value           : cached bytes (parquet partition data)
    - inserted_at     : monotonic timestamp (TTL expiry 계산용)
    - last_accessed_at: monotonic timestamp (LRU eviction 계산용)
    """

    value: bytes
    inserted_at: float
    last_accessed_at: float


class ReaderCache:
    """LRU + TTL cache + explicit flush + verify barrier.

    Thread-safety:
    - mutex-protected dict swap (§6.1 chief decision 2) — flush 진행 중 read 의 stale entry hit 0
    - get/put 도 lock 보호 (LRU OrderedDict.move_to_end + 신규 insert 시 capacity check)

    Cache eviction policy:
    - LRU: capacity 도달 시 가장 오래 미접근 entry evict
    - TTL: inserted_at + ttl_seconds < now 시 entry evict (lazy on access)

    Verify probe (AC-2 enforcement):
    - dedicated key namespace `__verify_probe_<uuid>` 사용
    - flush_all() 후 verify probe key put -> flush -> get == None verify
    - LRU/TTL evict mechanism 와 race 0 (probe key 가 다른 read 와 충돌 0)
    """

    _PROBE_PREFIX = "__verify_probe_"

    def __init__(
        self,
        *,
        capacity: int = 1024,
        ttl_seconds: float = 300.0,
        max_bytes: int | None = None,
        latency_window_size: int = 1000,
    ) -> None:
        self._capacity = capacity
        self._ttl_seconds = ttl_seconds
        self._max_bytes = max_bytes
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.Lock()
        self._hit_count = 0
        self._miss_count = 0
        self._current_bytes: int = 0
        # MCT-180 AC-5: read latency tracking (sliding window, unit: ms)
        self._latency_window_size = latency_window_size
        self._read_latencies_ms: deque[float] = deque(maxlen=latency_window_size)

    def get(self, key: str) -> bytes | None:
        """cache lookup — LRU update + TTL check + hit/miss metric emit + latency tracking.

        Idempotency: 다중 호출 시 동일 결과 (read-only operation + LRU/TTL deterministic).
        """
        _start = time.monotonic()
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._miss_count += 1
                _elapsed_ms = (time.monotonic() - _start) * 1000.0
                self._read_latencies_ms.append(_elapsed_ms)
                return None

            now = time.monotonic()
            if entry.inserted_at + self._ttl_seconds < now:
                # TTL expired — lazy evict
                del self._cache[key]
                self._miss_count += 1
                _elapsed_ms = (time.monotonic() - _start) * 1000.0
                self._read_latencies_ms.append(_elapsed_ms)
                return None

            entry.last_accessed_at = now
            self._cache.move_to_end(key)  # LRU update
            self._hit_count += 1
            _elapsed_ms = (time.monotonic() - _start) * 1000.0
            self._read_latencies_ms.append(_elapsed_ms)
            return entry.value

    def put(self, key: str, value: bytes) -> None:
        """cache populate — LRU capacity check + byte budget enforcement + 신규 insert.

        Byte budget enforcement (max_bytes != None):
        - 기존 key 갱신 시 old bytes 차감 후 new bytes 기준 budget check
        - budget 초과 시 LRU entry evict (가장 오래 미접근 entry)
        - 단일 value 가 budget 보다 크더라도 전체 evict 후 저장 (최소 1 entry 보장)

        Idempotency: 다중 호출 시 결과 동일 (마지막 put 만 보존, value 동일 시 timestamp 만 갱신).
        """
        with self._lock:
            now = time.monotonic()
            if key in self._cache:
                # 기존 entry 갱신 — old bytes 차감 후 new bytes 추가
                old_len = len(self._cache[key].value)
                self._current_bytes -= old_len
                entry = self._cache[key]
                entry.value = value
                entry.inserted_at = now
                entry.last_accessed_at = now
                self._cache.move_to_end(key)
                self._current_bytes += len(value)
            else:
                # byte budget enforcement (max_bytes != None)
                if self._max_bytes is not None:
                    while (
                        self._current_bytes + len(value) > self._max_bytes
                        and len(self._cache) > 0
                    ):
                        _, evicted = self._cache.popitem(last=False)
                        self._current_bytes -= len(evicted.value)
                # capacity 기반 LRU eviction (기존 behavior)
                if len(self._cache) >= self._capacity:
                    _, evicted = self._cache.popitem(last=False)
                    self._current_bytes -= len(evicted.value)
                self._cache[key] = CacheEntry(value=value, inserted_at=now, last_accessed_at=now)
                self._cache.move_to_end(key)
                self._current_bytes += len(value)

    def flush_all(self) -> CacheFlushResult:
        """모든 entry 강제 무효화 + verify probe (AC-2 enforcement, S3 박제 핵심).

        Algorithm (mutex-protected dict swap pattern, §6.1 chief decision 2):
        Phase 1 (flush): 신규 OrderedDict 할당 (immutable replace)
        Phase 2 (verify probe): dedicated probe key put -> remove -> get == None verify
        Phase 3 (race detection): probe 후 cache 잔존 entry > 0 시 partial_flush_failed return

        Idempotency (§11.6): 다중 호출 시 동일 결과 (cache empty 시도 정상 flush + verify pass).
        """
        start = time.monotonic()

        # Phase 1: flush
        with self._lock:
            flushed_count = len(self._cache)
            self._cache = OrderedDict()  # 신규 할당 (immutable replace)
            self._hit_count = 0
            self._miss_count = 0
            self._current_bytes = 0
            self._read_latencies_ms.clear()

        # Phase 2: verify probe (mutex-protected put + remove)
        probe_key = f"{self._PROBE_PREFIX}{uuid.uuid4().hex}"
        probe_value = b"verify_probe"
        self.put(probe_key, probe_value)

        with self._lock:
            evicted_probe = self._cache.pop(probe_key, None)
            if evicted_probe is not None:
                self._current_bytes -= len(evicted_probe.value)
            remaining = len(self._cache)

        # Phase 3: probe verify (get must return None)
        # NOTE: get() increments miss_count — reset after verify
        probe_after = self.get(probe_key)
        with self._lock:
            # restore counters (verify probe 자체는 metric 비대상)
            self._hit_count = 0
            self._miss_count = 0
            self._read_latencies_ms.clear()

        flush_duration_ms = (time.monotonic() - start) * 1000

        if probe_after is not None:
            logger.error(
                "reader_cache.flush_all verify_probe_failed — probe_key=%s, probe_after_len=%d",
                probe_key,
                len(probe_after) if probe_after else 0,
            )
            return CacheFlushResult(
                status="verify_probe_failed",
                flushed_count=flushed_count,
                remaining_count=remaining,
                verify_probe_key=probe_key,
                flush_duration_ms=flush_duration_ms,
            )

        if remaining > 0:
            logger.warning(
                "reader_cache.flush_all partial_flush_failed — remaining=%d (race detected)",
                remaining,
            )
            return CacheFlushResult(
                status="partial_flush_failed",
                flushed_count=flushed_count,
                remaining_count=remaining,
                verify_probe_key=probe_key,
                flush_duration_ms=flush_duration_ms,
            )

        return CacheFlushResult(
            status="flushed",
            flushed_count=flushed_count,
            remaining_count=0,
            verify_probe_key=probe_key,
            flush_duration_ms=flush_duration_ms,
        )

    def verify_flushed(self) -> bool:
        """flush 완료 verify gate (AC-2 enforcement) — caller endpoint_router.flip() 진입 직전 호출.

        Returns:
            bool — True (cache empty + verify probe pass) / False (잔존 entry 검출)

        Idempotency: 다중 호출 시 동일 결과 (read-only verify, side effect 별).
        """
        with self._lock:
            if len(self._cache) > 0:
                return False

        probe_key = f"{self._PROBE_PREFIX}check_{uuid.uuid4().hex}"
        self.put(probe_key, b"check")

        with self._lock:
            self._cache.pop(probe_key, None)

        result = self.get(probe_key) is None
        # restore counters (verify probe 자체는 metric 비대상)
        with self._lock:
            self._hit_count = 0
            self._miss_count = 0
            self._read_latencies_ms.clear()
        return result

    def current_bytes(self) -> int:
        """현재 cache 총 byte size return (MCT-170 byte budget 추적)."""
        with self._lock:
            return self._current_bytes

    def hit_ratio(self) -> float:
        """cache hit ratio return — AC-5 smoke test 측 측정값."""
        with self._lock:
            total = self._hit_count + self._miss_count
            return self._hit_count / total if total > 0 else 0.0

    def p99_ms(self) -> float:
        """read latency p99 (milliseconds) — MCT-180 AC-5 Prometheus Gauge expose.

        슬라이딩 윈도우(최근 latency_window_size 회 get() 호출)에서 p99 계산.
        데이터 없을 경우 0.0 return.
        """
        with self._lock:
            latencies = list(self._read_latencies_ms)
        if not latencies:
            return 0.0
        sorted_latencies = sorted(latencies)
        # nearest-rank (ceil) p99: rank = ceil(0.99 * N), 0-indexed = rank - 1.
        # int(N*0.99)-1 (floor 기반) 는 저편향 — 작은 N 에서 p99 가 실측보다
        # 낮게 산출됨 (FIX-MCT-180 P2 정정).
        idx = min(len(sorted_latencies) - 1, math.ceil(len(sorted_latencies) * 0.99) - 1)
        idx = max(0, idx)
        return sorted_latencies[idx]

    def stats(self) -> dict:
        """cache 통계 return — caller (smoke test / metric emit) 측 활용.

        MCT-180 AC-5 contract (FIX-MCT-180 engine#55 P1, 설계 원인 정정):
        `nas_reader_cache_hit_ratio` / `nas_reader_p99_ms` Gauge = **cold reader
        사용 컴포넌트 한정** metric (backtest-runner / `ColdReader.run_smoke_test()`
        cutover 경로). **paper-engine daemon 미적용** — paper daemon =
        PaperRunner WS tick 경로, ReaderCache/ColdReader/TierReader
        미인스턴스화 (MCT-170 reader_cache = NAS cold read 전용 scope).
        stats() production caller = `ColdReader.run_smoke_test()` 1곳 →
        paper daemon real-time emit 불가, backtest-runner oneshot 실행 시 emit.

        stats() 호출 시점 = 측정값 확정 시점 → Prometheus Gauge auto-emit
        (set_universe_size 가 cli.py daemon startup 1회 emit 인 패턴과 동형 —
        값 확정 시점 emit). circular import 회피 위해 lazy import.
        """
        with self._lock:
            total = self._hit_count + self._miss_count
            hit_ratio = self._hit_count / total if total > 0 else 0.0
        p99_ms = self.p99_ms()
        # producer-wiring no-op (MCT-183 relocate: engine.metrics 외부 의존 제거 —
        # Layer2 자족 INV-2. dead-in-data → stats() production caller 0, Gauge 실 emit
        # 재배선 = MCT-185 cold-read cutover owner. set_reader_* setter 부재 시 no-op).
        with self._lock:
            return {
                "size": len(self._cache),
                "capacity": self._capacity,
                "ttl_seconds": self._ttl_seconds,
                "hit_count": self._hit_count,
                "miss_count": self._miss_count,
                "hit_ratio": hit_ratio,
                "p99_ms": p99_ms,
            }
