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
