"""Tests for orderbook_storage.py (MCT-58)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from mctrader_data.orderbook_storage import (
    ORDERBOOK_SCHEMA_VERSION,
    OrderbookEventRecord,
    OrderbookWriter,
)


def _ts(offset_sec: int = 0) -> datetime:
    from datetime import timedelta

    return datetime(2026, 5, 4, 0, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset_sec)


def _snapshot_record(level: int, side: str = "bid") -> OrderbookEventRecord:
    return OrderbookEventRecord(
        ts_utc=_ts(0), received_at=_ts(0),
        exchange="bithumb", symbol="KRW-BTC",
        event_type="snapshot", side=side, level=level,
        price=Decimal("100000000") + Decimal(level), quantity=Decimal("0.05"),
    )


def _delta_record(side: str = "bid") -> OrderbookEventRecord:
    return OrderbookEventRecord(
        ts_utc=_ts(1), received_at=_ts(1),
        exchange="bithumb", symbol="KRW-BTC",
        event_type="delta", side=side, level=-1,
        price=Decimal("99999999"), quantity=Decimal("0.10"),
    )


def test_writer_writes_snapshot_records(tmp_path: Path) -> None:
    w = OrderbookWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1")
    w.append_many([_snapshot_record(0, "bid"), _snapshot_record(0, "ask")])
    w.close()
    parts = list((tmp_path / "market" / "orderbook").rglob("*.parquet"))
    assert len(parts) == 1


def test_writer_partition_path_layout(tmp_path: Path) -> None:
    w = OrderbookWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1")
    w.append(_snapshot_record(0))
    w.close()
    expected_partition = (
        tmp_path / "market" / "orderbook"
        / f"schema_version={ORDERBOOK_SCHEMA_VERSION}"
        / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-04"
    )
    assert expected_partition.exists()


def test_writer_mixed_snapshot_delta_round_trip(tmp_path: Path) -> None:
    w = OrderbookWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1")
    w.append(_snapshot_record(0, "bid"))
    w.append(_snapshot_record(0, "ask"))
    w.append(_delta_record("bid"))
    w.close()
    parts = list((tmp_path / "market" / "orderbook").rglob("*.parquet"))
    table = pq.ParquetFile(parts[0]).read()
    assert table.num_rows == 3
    types = table.column("event_type").to_pylist()
    assert types == ["snapshot", "snapshot", "delta"]


def test_writer_batch_size_triggers_flush(tmp_path: Path) -> None:
    w = OrderbookWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1", batch_size=2,
    )
    w.append(_snapshot_record(0))
    w.append(_snapshot_record(1))
    parts = list((tmp_path / "market" / "orderbook").rglob("*.parquet"))
    assert len(parts) == 1
    w.close()


def test_writer_close_idempotent(tmp_path: Path) -> None:
    w = OrderbookWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1")
    w.append(_snapshot_record(0))
    w.close()
    w.close()


def test_writer_append_after_close_raises(tmp_path: Path) -> None:
    w = OrderbookWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="s1")
    w.close()
    with pytest.raises(RuntimeError, match="closed"):
        w.append(_snapshot_record(0))
