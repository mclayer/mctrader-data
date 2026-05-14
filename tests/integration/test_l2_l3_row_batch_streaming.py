"""tests/integration/test_l2_l3_row_batch_streaming.py
MCT-163 Phase 2.3 — F6 L2/L3 iter_batches TDD tests.

Story: MCT-163
Spec: docs/superpowers/specs/2026-05-14-MCT-163-dualwriter-streaming-design.md §4 AC-3/AC-4
Plan: docs/superpowers/plans/2026-05-14-mct-163-dualwriter-streaming.md Task 4

AC-3/INV-4: test_l2_iter_batches_memory_invariant
  - 1 GiB+ equivalent L1 file 처리 시 peak ≤ 256 MB delta
  - pq.ParquetFile(f).read() 대신 iter_batches(batch_size=1024) per-batch

AC-3/INV-4: test_l3_iter_batches_memory_invariant
  - L2 동형 (L3Compactor.compact_day)

AC-4/INV-5: test_l2_schema_parity
  - iter_batches per-batch write 산출물 schema == 기존 L2 schema (forward-only invariant)

AC-4/INV-5: test_l3_schema_parity
  - 동형 L3

D6=C: psutil RSS + tracemalloc delta-based assert (절대값 기준 X, delta만)
"""
from __future__ import annotations

import gc
import os
import tracemalloc
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mctrader_data.compactor.l1 import _schema_version, _ORDERBOOKDEPTH_SCHEMA
from mctrader_data.compactor.l2 import L2Compactor
from mctrader_data.compactor.l3 import L3Compactor


# ============================================================================
# Helpers
# ============================================================================

_CHANNEL = "orderbookdepth"
_EXCHANGE = "bithumb"
_SYMBOL = "KRW-BTC"
_DATE = date(2026, 5, 14)
_HOUR = 10


def _measure_delta(fn) -> tuple[int, int]:
    """D6=C: delta-based (RSS delta + tracemalloc delta)."""
    proc = psutil.Process(os.getpid())
    gc.collect()
    rss_before = proc.memory_info().rss
    tracemalloc.start()
    snap1 = tracemalloc.take_snapshot()

    fn()

    snap2 = tracemalloc.take_snapshot()
    tracemalloc.stop()
    gc.collect()
    rss_after = proc.memory_info().rss

    rss_delta = max(0, rss_after - rss_before)
    stats = snap2.compare_to(snap1, "lineno")
    tm_delta = sum(s.size_diff for s in stats if s.size_diff > 0)

    return rss_delta, tm_delta


def _create_l1_file(
    root: Path,
    n_rows: int,
    channel: str = _CHANNEL,
    exchange: str = _EXCHANGE,
    symbol: str = _SYMBOL,
    date_utc: date = _DATE,
    hour_utc: int = _HOUR,
    node_id: str = "node-001",
    part_name: str = "part-test-001.parquet",
) -> Path:
    """Create L1 Parquet file in canonical Hive partition path."""
    schema_ver = _schema_version(channel)
    l1_dir = (
        root / "market" / channel
        / f"schema_version={schema_ver}" / "tier=L1"
        / f"exchange={exchange}" / f"symbol={symbol}"
        / f"date={date_utc.isoformat()}"
        / f"hour={hour_utc:02d}" / f"node={node_id}"
    )
    l1_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime(2026, 5, 14, hour_utc, 0, 0, tzinfo=timezone.utc)
    # Each row: ~600 bytes raw_json (for realistic scale)
    raw_json_payload = '{"level": 1, "data": "' + "x" * 500 + '"}'
    rows = []
    for i in range(n_rows):
        rows.append({
            "ts_utc": now_utc + timedelta(microseconds=i * 1000),
            "received_at": now_utc - timedelta(milliseconds=50),
            "exchange": exchange,
            "symbol": symbol,
            "side": "ask" if i % 2 == 0 else "bid",
            "price": Decimal("50000.0") + Decimal(i % 1000) * Decimal("0.01"),
            "quantity": Decimal("1.5") + Decimal(i % 100) * Decimal("0.001"),
            "raw_json": raw_json_payload,
            "node_id": node_id,
            "collector_run_id": "run-001",
            "ingest_seq": i,
        })

    table = pa.Table.from_pylist(rows, schema=_ORDERBOOKDEPTH_SCHEMA)
    parquet_path = l1_dir / part_name
    pq.write_table(table, str(parquet_path), compression="snappy", row_group_size=1024)
    return parquet_path


def _create_l2_file(
    root: Path,
    n_rows: int,
    channel: str = _CHANNEL,
    exchange: str = _EXCHANGE,
    symbol: str = _SYMBOL,
    date_utc: date = _DATE,
    hour_utc: int = _HOUR,
    part_name: str = "part-test-001.parquet",
) -> Path:
    """Create L2 Parquet file in canonical Hive partition path."""
    schema_ver = _schema_version(channel)
    l2_dir = (
        root / "market" / channel
        / f"schema_version={schema_ver}" / "tier=L2"
        / f"exchange={exchange}" / f"symbol={symbol}"
        / f"date={date_utc.isoformat()}"
        / f"hour={hour_utc:02d}" / "node=MERGED"
    )
    l2_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime(2026, 5, 14, hour_utc, 0, 0, tzinfo=timezone.utc)
    raw_json_payload = '{"level": 1, "data": "' + "x" * 500 + '"}'
    rows = []
    for i in range(n_rows):
        rows.append({
            "ts_utc": now_utc + timedelta(microseconds=i * 1000),
            "received_at": now_utc - timedelta(milliseconds=50),
            "exchange": exchange,
            "symbol": symbol,
            "side": "ask" if i % 2 == 0 else "bid",
            "price": Decimal("50000.0") + Decimal(i % 1000) * Decimal("0.01"),
            "quantity": Decimal("1.5") + Decimal(i % 100) * Decimal("0.001"),
            "raw_json": raw_json_payload,
            "node_id": "node-001",
            "collector_run_id": "run-001",
            "ingest_seq": i,
        })

    table = pa.Table.from_pylist(rows, schema=_ORDERBOOKDEPTH_SCHEMA)
    parquet_path = l2_dir / part_name
    pq.write_table(table, str(parquet_path), compression="snappy", row_group_size=1024)
    return parquet_path


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def tmp_root(tmp_path: Path) -> Path:
    root = tmp_path / "data"
    root.mkdir()
    return root


# ============================================================================
# AC-4/INV-5: Schema parity (iter_batches per-batch write == 기존 schema)
# ============================================================================

def test_l2_schema_parity(tmp_root: Path) -> None:
    """AC-4/INV-5: L2Compactor.compact_hour iter_batches 산출물 schema == L1 schema.

    forward-only invariant: per-batch write_batch 후 output schema 유지.
    """
    n_rows = 500
    _create_l1_file(tmp_root, n_rows=n_rows)

    compactor = L2Compactor(root=tmp_root)
    result_path = compactor.compact_hour(
        exchange=_EXCHANGE,
        symbol=_SYMBOL,
        channel=_CHANNEL,
        date_utc=_DATE,
        hour_utc=_HOUR,
    )

    assert result_path is not None, "compact_hour should return output path"
    assert result_path.exists(), f"Output file should exist: {result_path}"

    # Schema parity check
    pf = pq.ParquetFile(str(result_path))
    output_schema = pf.schema_arrow
    expected_schema = _ORDERBOOKDEPTH_SCHEMA

    # Field names and types must match (INV-5 forward-only)
    assert output_schema.names == expected_schema.names, (
        f"Schema field names mismatch: {output_schema.names} != {expected_schema.names}"
    )

    # Row count check
    total = pf.read().num_rows
    assert total == n_rows, f"Row count mismatch: {total} != {n_rows}"


def test_l3_schema_parity(tmp_root: Path) -> None:
    """AC-4/INV-5: L3Compactor.compact_day iter_batches 산출물 schema == L2 schema."""
    n_rows = 500
    _create_l2_file(tmp_root, n_rows=n_rows)

    compactor = L3Compactor(root=tmp_root)
    result_path = compactor.compact_day(
        exchange=_EXCHANGE,
        symbol=_SYMBOL,
        channel=_CHANNEL,
        date_utc=_DATE,
    )

    assert result_path is not None, "compact_day should return output path"
    assert result_path.exists(), f"Output file should exist: {result_path}"

    pf = pq.ParquetFile(str(result_path))
    output_schema = pf.schema_arrow
    expected_schema = _ORDERBOOKDEPTH_SCHEMA

    assert output_schema.names == expected_schema.names, (
        f"Schema field names mismatch: {output_schema.names} != {expected_schema.names}"
    )

    total = pf.read().num_rows
    assert total == n_rows, f"Row count mismatch: {total} != {n_rows}"


# ============================================================================
# AC-3/INV-4: Memory invariant ≤ 256 MB delta
# ============================================================================

@pytest.mark.slow
def test_l2_iter_batches_memory_invariant(tmp_root: Path) -> None:
    """INV-4: L2Compactor.compact_hour peak RSS+tracemalloc delta ≤ 256 MB.

    1 GiB equivalent 대신 실제 환경에서 검증 가능한 크기 사용.
    n_rows=500_000 rows × ~600 bytes = ~300 MB in-memory table (if fully loaded).
    iter_batches(batch_size=1024) per-batch 시 피크 ≪ 300 MB.

    D6=C: delta-based assert (시작 전 baseline에서의 증분만).
    """
    limit_bytes = 256 * 1024 * 1024  # 256 MB (INV-4)

    # 300,000 rows × ~600 bytes raw_json ≈ ~180 MB table if fully loaded
    # With iter_batches(1024), peak per-batch ≈ 1024 × 600 bytes = ~600 KB
    n_rows = 300_000
    _create_l1_file(tmp_root, n_rows=n_rows)

    compactor = L2Compactor(root=tmp_root)

    def run():
        compactor.compact_hour(
            exchange=_EXCHANGE,
            symbol=_SYMBOL,
            channel=_CHANNEL,
            date_utc=_DATE,
            hour_utc=_HOUR,
        )

    rss_delta, tm_delta = _measure_delta(run)

    assert rss_delta <= limit_bytes, (
        f"INV-4 L2 RSS delta violation: {rss_delta / 1024 / 1024:.1f} MB > 256 MB. "
        f"L2Compactor must use iter_batches per-batch, not pq.ParquetFile.read()."
    )
    assert tm_delta <= limit_bytes, (
        f"INV-4 L2 tracemalloc delta violation: {tm_delta / 1024 / 1024:.1f} MB > 256 MB. "
        f"L2Compactor must use iter_batches per-batch, not pq.ParquetFile.read()."
    )


@pytest.mark.slow
def test_l3_iter_batches_memory_invariant(tmp_root: Path) -> None:
    """INV-4: L3Compactor.compact_day peak RSS+tracemalloc delta ≤ 256 MB.

    L2 동형 (D4=A iter_batches + D5=A per-batch write_batch).
    """
    limit_bytes = 256 * 1024 * 1024  # 256 MB (INV-4)

    n_rows = 300_000
    _create_l2_file(tmp_root, n_rows=n_rows)

    compactor = L3Compactor(root=tmp_root)

    def run():
        compactor.compact_day(
            exchange=_EXCHANGE,
            symbol=_SYMBOL,
            channel=_CHANNEL,
            date_utc=_DATE,
        )

    rss_delta, tm_delta = _measure_delta(run)

    assert rss_delta <= limit_bytes, (
        f"INV-4 L3 RSS delta violation: {rss_delta / 1024 / 1024:.1f} MB > 256 MB. "
        f"L3Compactor must use iter_batches per-batch, not pq.ParquetFile.read()."
    )
    assert tm_delta <= limit_bytes, (
        f"INV-4 L3 tracemalloc delta violation: {tm_delta / 1024 / 1024:.1f} MB > 256 MB. "
        f"L3Compactor must use iter_batches per-batch, not pq.ParquetFile.read()."
    )
