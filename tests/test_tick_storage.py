"""Tests for tick_storage.py (MCT-58)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from mctrader_data.tick_storage import TICK_SCHEMA_VERSION, TickRecord, TickWriter


def _ts(offset_min: int = 0) -> datetime:
    from datetime import timedelta

    return datetime(2026, 5, 4, 0, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=offset_min)


def _record(offset_min: int = 0) -> TickRecord:
    return TickRecord(
        ts_utc=_ts(offset_min), received_at=_ts(offset_min),
        exchange="bithumb", symbol="KRW-BTC",
        price=Decimal("100000000"), quantity=Decimal("0.01"),
        side="buy", raw_json='{"x":"y"}',
    )


def test_writer_open_and_close_creates_file(tmp_path: Path) -> None:
    w = TickWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="abc123")
    w.append(_record(0))
    w.close()
    parts = list((tmp_path / "market" / "ticks").rglob("*.parquet"))
    assert len(parts) == 1
    assert "abc123" in parts[0].name


def test_writer_partition_path_layout(tmp_path: Path) -> None:
    w = TickWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1")
    w.append(_record(0))
    w.close()
    expected_partition = (
        tmp_path / "market" / "ticks"
        / f"schema_version={TICK_SCHEMA_VERSION}"
        / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-04"
    )
    assert expected_partition.exists()
    assert (expected_partition / "part-s1.parquet").exists()


def test_writer_round_trip_preserves_decimal(tmp_path: Path) -> None:
    w = TickWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1")
    for i in range(3):
        w.append(_record(i))
    w.close()
    parts = list((tmp_path / "market" / "ticks").rglob("*.parquet"))
    # Read single file directly (skip Hive partition auto-discovery)
    table = pq.ParquetFile(parts[0]).read()
    assert table.num_rows == 3
    assert "ts_utc" in table.column_names
    assert "price" in table.column_names


def test_writer_batch_size_triggers_flush(tmp_path: Path) -> None:
    w = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1", batch_size=2,
    )
    w.append(_record(0))
    w.append(_record(1))
    # second append should trigger flush
    parts = list((tmp_path / "market" / "ticks").rglob("*.parquet"))
    assert len(parts) == 1
    w.close()


def test_writer_close_idempotent(tmp_path: Path) -> None:
    w = TickWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1")
    w.append(_record(0))
    w.close()
    w.close()  # second call should be a no-op


def test_writer_append_after_close_raises(tmp_path: Path) -> None:
    w = TickWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1")
    w.close()
    with pytest.raises(RuntimeError, match="closed"):
        w.append(_record(0))


# MCT-91 — HA writer (node= partition + new file naming + parquet metadata + logical key)
def test_tick_writer_node_id_partition_and_filename(tmp_path: Path) -> None:
    """node_id + collector_run_id 명시 시 ADR-009 §D2.1 + §D10.7 layout."""
    w = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_A", collector_run_id="NODE_A-20260505T223456Z",
    )
    w.append(_record(0))
    w.close()
    expected_partition = (
        tmp_path / "market" / "ticks"
        / f"schema_version={TICK_SCHEMA_VERSION}"
        / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-04"
        / "node=NODE_A"
    )
    assert expected_partition.exists()
    parquets = list(expected_partition.glob("*.parquet"))
    assert len(parquets) == 1
    assert parquets[0].name == "NODE_A-20260505T223456Z-0.parquet"


def test_tick_writer_parquet_metadata_node_id(tmp_path: Path) -> None:
    """node_id 명시 시 parquet metadata 에 node_id field."""
    w = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_B", collector_run_id="NODE_B-20260505T120000Z",
    )
    w.append(_record(0))
    w.close()
    parquet = next((tmp_path / "market" / "ticks").rglob("*.parquet"))
    pf = pq.ParquetFile(parquet)
    meta = pf.schema_arrow.metadata or {}
    assert meta.get(b"node_id") == b"NODE_B"


def test_tick_writer_legacy_no_node_id(tmp_path: Path) -> None:
    """node_id 미명시 시 기존 layout (backward compat)."""
    w = TickWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="legacy-1")
    w.append(_record(0))
    w.close()
    parquet = next((tmp_path / "market" / "ticks").rglob("*.parquet"))
    assert parquet.name == "part-legacy-1.parquet"
    assert "node=" not in parquet.as_posix()


def test_tick_logical_key_columns_preserved(tmp_path: Path) -> None:
    """ADR-009 §D10.7 logical key 6 column (exchange/symbol/ts_utc/price/quantity/side) 보존."""
    w = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_A", collector_run_id="NODE_A-20260505T120000Z",
    )
    w.append(_record(0))
    w.close()
    parquet = next((tmp_path / "market" / "ticks").rglob("*.parquet"))
    table = pq.ParquetFile(parquet).read()
    logical_key_cols = ["exchange", "symbol", "ts_utc", "price", "quantity", "side"]
    for col in logical_key_cols:
        assert col in table.schema.names, f"missing logical key column: {col}"
        assert table[col].null_count == 0, f"logical key column {col} has nulls"
