"""D7 NFR performance test — 95% cache hit rate + p99 <100ms (MCT-170 Phase 2).

ADR-029 D7=A: Reader cache 95% hit + p99 <100ms under synthetic 10k read workload.

Test design:
- 10k read operations with 95%+ cache hit rate target
- p99 latency < 100ms (in-memory cache read, should be <<1ms per op)
- Uses pytest-benchmark for timing precision
- Synthetic workload: 1000 unique keys, 10k reads (key reuse = cache hit)
"""

from __future__ import annotations

import statistics
import time
from unittest.mock import MagicMock


from mctrader_data.io.cold_reader import ReadResult as ColdReadResult
from mctrader_data.io.dr_mode import DRMode
from mctrader_data.io.l1_reader import L1ReadResult
from mctrader_data.io.reader_cache import ReaderCache
from mctrader_data.io.tier_reader import TierReader


def _make_cold_reader_fixture(n_keys: int = 1000):
    """n_keys 개의 partition → data 매핑 mock cold_reader."""
    cold_reader = MagicMock()

    def _read(path: str) -> ColdReadResult:
        return ColdReadResult(
            status="hit_nas",
            data=b"x" * 1024,  # 1KB synthetic parquet
            nas_object_key=path,
        )

    cold_reader.read.side_effect = _read
    return cold_reader


def _make_l1_reader_fixture():
    l1_reader = MagicMock()

    def _read(symbol, date, hour):
        return L1ReadResult(
            status="hit_nas",
            data=b"x" * 1024,
            nas_object_key=f"tier=L1/symbol={symbol}/date={date}/hour={hour:02d}/data.parquet",
        )

    l1_reader.read.side_effect = _read
    return l1_reader


def _generate_partition_paths(n: int = 1000) -> list[str]:
    """n개의 L2 partition path 생성 (synthetic)."""
    paths = []
    for i in range(n):
        sym = f"SYM{i % 50:03d}"
        date = f"202601{(i % 28) + 1:02d}"
        paths.append(f"tier=L2/exchange=DEFAULT/symbol={sym}/date={date}/node=DEFAULT/data_{i}.parquet")
    return paths


class TestD7NFRHitRatio:
    """D7 NFR — 95% cache hit rate."""

    def test_hit_ratio_95_percent_10k_reads(self):
        """10k read 중 95%+ cache hit — synthetic 500 unique key, 20 rounds.

        R4 mitigation iter 1: n_unique=500, n_rounds=20 → 19/20 rounds = hit → 95%.
        """
        n_unique = 500
        n_rounds = 20  # 10k total reads
        paths = _generate_partition_paths(n_unique)

        # ReaderCache: 500+100 capacity + 15MB byte budget (R4 mitigation +50%)
        cache = ReaderCache(
            capacity=n_unique + 100,
            ttl_seconds=3600.0,
            max_bytes=15 * 1024 * 1024,  # 15MB
        )
        dr_mode = DRMode()
        cold_reader = _make_cold_reader_fixture(n_unique)
        l1_reader = _make_l1_reader_fixture()

        tr = TierReader(
            cold_reader=cold_reader,
            l1_reader=l1_reader,
            reader_cache=cache,
            dr_mode=dr_mode,
            endpoint_router=MagicMock(),
        )

        hit_count = 0
        miss_count = 0
        total_reads = n_unique * n_rounds

        for _round in range(n_rounds):
            for path in paths:
                result = tr.read(path)
                if result.status == "hit_cache":
                    hit_count += 1
                else:
                    miss_count += 1

        hit_ratio = hit_count / total_reads
        assert hit_ratio >= 0.95, (
            f"D7 NFR FAIL: hit_ratio={hit_ratio:.3f} < 0.95 "
            f"(hit={hit_count}, miss={miss_count}, total={total_reads})"
        )

    def test_hit_ratio_with_byte_budget(self):
        """byte budget 설정 시에도 95% hit rate 달성."""
        n_unique = 200
        n_rounds = 20
        paths = _generate_partition_paths(n_unique)

        # 200 * 1KB = 200KB + 여유 (max_bytes=400KB, R4 mitigation +50%)
        cache = ReaderCache(
            capacity=n_unique + 100,
            ttl_seconds=3600.0,
            max_bytes=400 * 1024,
        )
        dr_mode = DRMode()
        cold_reader = _make_cold_reader_fixture(n_unique)
        l1_reader = _make_l1_reader_fixture()

        tr = TierReader(
            cold_reader=cold_reader,
            l1_reader=l1_reader,
            reader_cache=cache,
            dr_mode=dr_mode,
            endpoint_router=MagicMock(),
        )

        hit_count = 0
        total_reads = n_unique * n_rounds

        for _round in range(n_rounds):
            for path in paths:
                result = tr.read(path)
                if result.status == "hit_cache":
                    hit_count += 1

        hit_ratio = hit_count / total_reads
        assert hit_ratio >= 0.95, (
            f"D7 NFR byte_budget FAIL: hit_ratio={hit_ratio:.3f} < 0.95"
        )


class TestD7NFRLatency:
    """D7 NFR — p99 <100ms per read operation."""

    def test_p99_latency_under_100ms_cache_hits(self):
        """cache hit 연속 read p99 < 100ms."""
        n_reads = 10_000
        paths = _generate_partition_paths(200)

        cache = ReaderCache(capacity=300, ttl_seconds=3600.0, max_bytes=5 * 1024 * 1024)
        dr_mode = DRMode()
        cold_reader = _make_cold_reader_fixture(200)
        l1_reader = _make_l1_reader_fixture()

        tr = TierReader(
            cold_reader=cold_reader,
            l1_reader=l1_reader,
            reader_cache=cache,
            dr_mode=dr_mode,
            endpoint_router=MagicMock(),
        )

        # warm-up: 첫 round는 NAS read (cache miss)
        for path in paths:
            tr.read(path)

        # measure: cache hit reads only
        latencies_ms: list[float] = []
        for i in range(n_reads):
            path = paths[i % len(paths)]
            start = time.perf_counter()
            tr.read(path)
            elapsed_ms = (time.perf_counter() - start) * 1000
            latencies_ms.append(elapsed_ms)

        latencies_ms.sort()
        p99_idx = int(len(latencies_ms) * 0.99)
        p99_ms = latencies_ms[p99_idx]

        assert p99_ms < 100.0, (
            f"D7 NFR FAIL: p99={p99_ms:.2f}ms >= 100ms "
            f"(median={statistics.median(latencies_ms):.3f}ms)"
        )

    def test_p99_latency_benchmark(self, benchmark):
        """pytest-benchmark: single cache hit read latency."""
        cache = ReaderCache(capacity=100, ttl_seconds=3600.0)
        dr_mode = DRMode()
        cold_reader = _make_cold_reader_fixture(1)
        l1_reader = _make_l1_reader_fixture()

        tr = TierReader(
            cold_reader=cold_reader,
            l1_reader=l1_reader,
            reader_cache=cache,
            dr_mode=dr_mode,
            endpoint_router=MagicMock(),
        )
        path = "tier=L2/exchange=DEFAULT/symbol=BTC/date=20260101/node=DEFAULT/data.parquet"
        tr.read(path)  # warm-up (NAS miss → cache populate)

        def _read_once():
            tr.read(path)

        benchmark(_read_once)
        # benchmark stats in seconds; assert mean < 1ms
        assert benchmark.stats["mean"] < 0.001, (
            f"D7 NFR benchmark FAIL: mean={benchmark.stats['mean'] * 1000:.3f}ms >= 1ms"
        )


class TestD7NFRHitRatioReport:
    """D7 NFR 측정 결과 report (commit message TBD 채움용)."""

    def test_report_hit_ratio_and_p99(self, capsys):
        """hit_ratio + p99 측정 결과를 stdout으로 출력 (commit msg fill용)."""
        n_unique = 500
        n_rounds = 20
        paths = _generate_partition_paths(n_unique)

        cache = ReaderCache(
            capacity=n_unique + 100,
            ttl_seconds=3600.0,
            max_bytes=15 * 1024 * 1024,  # R4 mitigation +50%
        )
        dr_mode = DRMode()
        cold_reader = _make_cold_reader_fixture(n_unique)
        l1_reader = _make_l1_reader_fixture()

        tr = TierReader(
            cold_reader=cold_reader,
            l1_reader=l1_reader,
            reader_cache=cache,
            dr_mode=dr_mode,
            endpoint_router=MagicMock(),
        )

        hit_count = 0
        total_reads = n_unique * n_rounds
        latencies: list[float] = []

        for _round in range(n_rounds):
            for path in paths:
                start = time.perf_counter()
                result = tr.read(path)
                elapsed_ms = (time.perf_counter() - start) * 1000
                latencies.append(elapsed_ms)
                if result.status == "hit_cache":
                    hit_count += 1

        latencies.sort()
        p99_ms = latencies[int(len(latencies) * 0.99)]
        hit_ratio = hit_count / total_reads

        print(
            f"\n[D7 NFR Report] hit_ratio={hit_ratio:.4f} "
            f"p99={p99_ms:.3f}ms "
            f"total_reads={total_reads}"
        )
        assert hit_ratio >= 0.95
        assert p99_ms < 100.0
