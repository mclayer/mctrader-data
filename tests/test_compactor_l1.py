# tests/test_compactor_l1.py
"""Tests for L1Compactor: INV-3 idempotency, INV-4 sort, INV-5 schema, INV-6 lineage."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed
from mctrader_data.compactor.l1 import L1Compactor


def _write_sealed_segment(tmp_path: Path, records: list[dict], node_id: str = "NODE_A") -> Path:
    """Write records to WAL and close (seals the segment)."""
    ing = WalIngester(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        channel="transaction", node_id=node_id,
        segment_seconds=86400,  # never auto-seal during test
    )
    for r in records:
        ing.append(r)
    ing.close()
    sealed = scan_sealed(tmp_path)
    assert len(sealed) == 1
    return sealed[0]


def _make_tick_record(seq: int, ts_offset_sec: int = 0) -> dict:
    ts = datetime(2026, 5, 9, 0, 0, ts_offset_sec, tzinfo=timezone.utc)
    return {
        "ts_utc": ts.isoformat(),
        "received_at": ts.isoformat(),
        "exchange": "bithumb",
        "symbol": "KRW-BTC",
        "price": Decimal("100000"),
        "quantity": Decimal("0.01"),
        "side": "buy",
        "raw_json": None,
        "channel": "transaction",
    }


def test_l1_compact_produces_parquet(tmp_path: Path) -> None:
    records = [_make_tick_record(i, i) for i in range(5)]
    sealed = _write_sealed_segment(tmp_path, records)
    compactor = L1Compactor(root=tmp_path)
    parquet_path = compactor.compact_segment(sealed)
    assert parquet_path.exists()
    assert parquet_path.suffix == ".parquet"
    # Use ParquetFile to read a single file — pq.read_table() triggers Hive
    # auto-discovery and merges path components (exchange=, symbol=, ...) as
    # partition columns, conflicting with the columns already in the schema.
    tbl = pq.ParquetFile(parquet_path).read()
    assert tbl.num_rows == 5


def test_l1_parquet_path_contains_tier_and_node(tmp_path: Path) -> None:
    records = [_make_tick_record(0)]
    sealed = _write_sealed_segment(tmp_path, records, node_id="NODE_A")
    compactor = L1Compactor(root=tmp_path)
    parquet_path = compactor.compact_segment(sealed)
    parts = parquet_path.parts
    assert "tier=L1" in parts
    assert "node=NODE_A" in parts
    assert "exchange=bithumb" in parts
    assert "symbol=KRW-BTC" in parts
    # date is dynamic — just check the pattern
    date_parts = [p for p in parts if p.startswith("date=")]
    assert len(date_parts) == 1


def test_l1_idempotent_double_compaction(tmp_path: Path) -> None:
    """INV-3: compact same sealed segment twice → identical Parquet sha256."""
    records = [_make_tick_record(i, i) for i in range(10)]
    sealed = _write_sealed_segment(tmp_path, records)
    compactor = L1Compactor(root=tmp_path)
    p1 = compactor.compact_segment(sealed)
    p2 = compactor.compact_segment(sealed)
    sha1 = hashlib.sha256(p1.read_bytes()).hexdigest()
    sha2 = hashlib.sha256(p2.read_bytes()).hexdigest()
    assert sha1 == sha2


def test_l1_out_of_order_sorted(tmp_path: Path) -> None:
    """INV-4: out-of-order records are sorted by ts_utc in output Parquet."""
    records = [_make_tick_record(i, 9 - i) for i in range(10)]  # ts in reverse order
    sealed = _write_sealed_segment(tmp_path, records)
    compactor = L1Compactor(root=tmp_path)
    parquet_path = compactor.compact_segment(sealed)
    # Use ParquetFile for single-file reads to avoid Hive partition auto-discovery.
    tbl = pq.ParquetFile(parquet_path).read()
    ts_col = tbl.column("ts_utc").to_pylist()
    assert ts_col == sorted(ts_col)


def test_l1_lineage_file_created(tmp_path: Path) -> None:
    """INV-6: lineage JSON created alongside Parquet."""
    records = [_make_tick_record(0)]
    sealed = _write_sealed_segment(tmp_path, records)
    compactor = L1Compactor(root=tmp_path)
    parquet_path = compactor.compact_segment(sealed)
    lineage_files = list(parquet_path.parent.glob("lineage-*.json"))
    assert len(lineage_files) == 1
    lineage = json.loads(lineage_files[0].read_text())
    assert "compacted_from" in lineage
    assert lineage["node_id"] == "NODE_A"


def test_l1_sealed_marked_compacted(tmp_path: Path) -> None:
    """After compact, sealed segment gets .compacted marker."""
    records = [_make_tick_record(0)]
    sealed = _write_sealed_segment(tmp_path, records)
    compactor = L1Compactor(root=tmp_path)
    compactor.compact_segment(sealed)
    assert Path(str(sealed) + ".compacted").exists()
    assert len(scan_sealed(tmp_path)) == 0
