# tests/integration/test_upbit_l1_partition.py
"""MCT-166 Phase 2 -- integration test: upbit L1 orderbooksnapshot WAL -> parquet.

Story: MCT-166 Phase 2 (QADeveloperAgent lane -- integration test)
AC-2: fix LAND -> orderbooksnapshot WAL -> L1 parquet 생성 검증 (WAL 주입 방식, 30분 수집 대체)
R2: bithumb L1 회귀 방지 (기존 orderbookdepth/transaction L1 영향 없음)

Test-1: test_upbit_orderbooksnapshot_wal_compacts_to_l1
  Given: upbit orderbooksnapshot WAL segment (sealed)
  When: L1Compactor.compact_segment()
  Then: tier=L1/exchange=upbit/... parquet 생성

Test-2: test_upbit_l1_parquet_schema
  AC-2 schema 검증: orderbooksnapshot schema version + column names

Test-3: test_bithumb_orderbookdepth_regression
  R2: bithumb orderbookdepth L1 compaction 정상 (regression)

Test-4: test_bithumb_transaction_regression
  R2: bithumb transaction L1 compaction 정상 (regression)

Test-5: test_upbit_orderbooksnapshot_row_count
  AC-2: flattened row count = sum of (bids + asks) per WAL record
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from mctrader_data.compactor.l1 import L1Compactor


@pytest.fixture
def short_root():
    """Shorter root dir for tests with deep Hive paths + ts-prefix filenames.

    Windows MAX_PATH=260 limit: ts-prefix adds ~17 chars per parquet filename.
    tempfile.TemporaryDirectory() gives ~46-char base vs pytest tmp_path ~97 chars.
    """
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sealed(
    root: Path,
    exchange: str,
    channel: str,
    symbol: str = "KRW-BTC",
    date: str = "2026-05-14",
    node_id: str = "N1",
    ts: str = "20260514T100000Z",
    records: list[dict] | None = None,
) -> Path:
    """Create a sealed WAL segment with given records.

    Filename follows WAL convention: segment-<YYYYMMDDTHHMMSSZ>-<node_id>.ndjson.sealed
    (parse_ts_from_segment + parse_node_id_from_segment 호환 — Task 3 ts-prefix 의무).

    node_id default = 'N1' (short) to stay under Windows MAX_PATH=260 limit:
    the ts-prefix adds 17 chars to the parquet filename vs legacy pattern.
    """
    wal_dir = root / "wal" / exchange / channel / symbol / date
    wal_dir.mkdir(parents=True, exist_ok=True)
    filename = f"segment-{ts}-{node_id}"
    seg_path = wal_dir / f"{filename}.ndjson"
    sealed_path = wal_dir / f"{filename}.ndjson.sealed"

    lines = [json.dumps(r, ensure_ascii=False) for r in (records or [])]
    seg_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    seg_path.rename(sealed_path)
    return sealed_path


def _make_orderbooksnapshot_records(n: int = 2) -> list[dict]:
    """Generate n orderbooksnapshot WAL records."""
    records = []
    for i in range(n):
        ts = datetime(2026, 5, 14, 10, i, 0, tzinfo=timezone.utc).isoformat()
        records.append({
            "ts_utc": ts,
            "received_at": ts,
            "exchange": "upbit",
            "symbol": "KRW-BTC",
            "bids": [
                {"price": f"{50000000 - i * 100}", "quantity": "0.01"},
                {"price": f"{49999000 - i * 100}", "quantity": "0.02"},
            ],
            "asks": [
                {"price": f"{50001000 + i * 100}", "quantity": "0.01"},
                {"price": f"{50002000 + i * 100}", "quantity": "0.02"},
            ],
            "raw_json": json.dumps({"type": "orderbook", "code": "KRW-BTC"}),
            "channel": "orderbooksnapshot",
        })
    return records


def _make_orderbookdepth_records(n: int = 2) -> list[dict]:
    """Generate n orderbookdepth WAL records (bithumb)."""
    records = []
    for i in range(n):
        ts = datetime(2026, 5, 14, 10, i, 0, tzinfo=timezone.utc).isoformat()
        records.append({
            "ts_utc": ts,
            "received_at": ts,
            "exchange": "bithumb",
            "symbol": "KRW-ETH",
            "changes": [
                {"side": "bid", "price": f"{3000000 + i}", "quantity": "0.5"},
                {"side": "ask", "price": f"{3001000 + i}", "quantity": "0.3"},
            ],
            "raw_json": json.dumps({"type": "orderbookdepth"}),
            "channel": "orderbookdepth",
        })
    return records


def _make_transaction_records(n: int = 2) -> list[dict]:
    """Generate n transaction WAL records (bithumb)."""
    records = []
    for i in range(n):
        ts = datetime(2026, 5, 14, 10, i, 0, tzinfo=timezone.utc).isoformat()
        records.append({
            "ts_utc": ts,
            "received_at": ts,
            "exchange": "bithumb",
            "symbol": "KRW-BTC",
            "price": f"{50000000 + i}",
            "quantity": "0.001",
            "side": "bid",
            "raw_json": json.dumps({"type": "transaction"}),
            "channel": "transaction",
        })
    return records


# ---------------------------------------------------------------------------
# Test-1: upbit orderbooksnapshot WAL -> L1 parquet
# ---------------------------------------------------------------------------

def test_upbit_orderbooksnapshot_wal_compacts_to_l1(short_root: Path) -> None:
    records = _make_orderbooksnapshot_records(n=3)
    sealed = _make_sealed(
        root=short_root,
        exchange="upbit",
        channel="orderbooksnapshot",
        records=records,
    )
    compactor = L1Compactor(root=short_root)
    parquet_path = compactor.compact_segment(sealed)

    assert parquet_path.exists(), f"parquet not created: {parquet_path}"
    # verify path components (Hive partition)
    assert "exchange=upbit" in str(parquet_path), f"exchange partition missing: {parquet_path}"
    assert "tier=L1" in str(parquet_path), f"tier partition missing: {parquet_path}"
    assert "orderbooksnapshot" in str(parquet_path), f"channel missing: {parquet_path}"


# ---------------------------------------------------------------------------
# Test-2: schema validation
# ---------------------------------------------------------------------------

def test_upbit_l1_parquet_schema(short_root: Path) -> None:
    records = _make_orderbooksnapshot_records(n=1)
    sealed = _make_sealed(
        root=short_root,
        exchange="upbit",
        channel="orderbooksnapshot",
        records=records,
    )
    compactor = L1Compactor(root=short_root)
    parquet_path = compactor.compact_segment(sealed)

    table = pq.ParquetFile(parquet_path).read()
    schema = table.schema

    # orderbooksnapshot schema columns (from orderbook_snapshot_storage.py)
    required_cols = {"ts_utc", "received_at", "exchange", "symbol", "side",
                     "level", "price", "quantity", "baseline_seq", "payload_hash"}
    actual_cols = set(schema.names)
    missing = required_cols - actual_cols
    assert not missing, f"missing columns: {missing}. actual: {actual_cols}"


# ---------------------------------------------------------------------------
# Test-3: bithumb orderbookdepth regression (R2)
# ---------------------------------------------------------------------------

def test_bithumb_orderbookdepth_regression(short_root: Path) -> None:
    records = _make_orderbookdepth_records(n=2)
    sealed = _make_sealed(
        root=short_root,
        exchange="bithumb",
        channel="orderbookdepth",
        records=records,
    )
    compactor = L1Compactor(root=short_root)
    parquet_path = compactor.compact_segment(sealed)

    assert parquet_path.exists(), f"bithumb orderbookdepth parquet not created: {parquet_path}"
    assert "exchange=bithumb" in str(parquet_path)
    assert "orderbookdepth" in str(parquet_path)


# ---------------------------------------------------------------------------
# Test-4: bithumb transaction regression (R2)
# ---------------------------------------------------------------------------

def test_bithumb_transaction_regression(tmp_path: Path) -> None:
    records = _make_transaction_records(n=2)
    sealed = _make_sealed(
        root=tmp_path,
        exchange="bithumb",
        channel="transaction",
        records=records,
    )
    compactor = L1Compactor(root=tmp_path)
    parquet_path = compactor.compact_segment(sealed)

    assert parquet_path.exists(), f"bithumb transaction parquet not created: {parquet_path}"
    assert "exchange=bithumb" in str(parquet_path)
    assert "transaction" in str(parquet_path)


# ---------------------------------------------------------------------------
# Test-5: row count = sum of (bids + asks) per WAL record (AC-2 flat shape)
# ---------------------------------------------------------------------------

def test_upbit_orderbooksnapshot_row_count(short_root: Path) -> None:
    # Each record has 2 bids + 2 asks = 4 rows. n=3 records -> 12 rows
    records = _make_orderbooksnapshot_records(n=3)
    sealed = _make_sealed(
        root=short_root,
        exchange="upbit",
        channel="orderbooksnapshot",
        records=records,
    )
    compactor = L1Compactor(root=short_root)
    parquet_path = compactor.compact_segment(sealed)

    table = pq.ParquetFile(parquet_path).read()
    expected_rows = 3 * (2 + 2)  # 3 records * (2 bids + 2 asks)
    assert table.num_rows == expected_rows, (
        f"row count mismatch: expected {expected_rows}, got {table.num_rows}"
    )
