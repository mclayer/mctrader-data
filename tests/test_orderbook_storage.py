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


# MCT-91 — HA writer (node= partition + new file naming + parquet metadata + logical key)
def test_orderbook_writer_node_id_partition_and_filename(tmp_path: Path) -> None:
    """node_id + collector_run_id 명시 시 ADR-009 §D2.1 + §D11.8 layout."""
    w = OrderbookWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_A", collector_run_id="NODE_A-20260505T223456Z",
    )
    w.append(_snapshot_record(0))
    w.close()
    expected_partition = (
        tmp_path / "market" / "orderbook"
        / f"schema_version={ORDERBOOK_SCHEMA_VERSION}"
        / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-04"
        / "node=NODE_A"
    )
    assert expected_partition.exists()
    parquets = list(expected_partition.glob("*.parquet"))
    assert len(parquets) == 1
    assert parquets[0].name == "NODE_A-20260505T223456Z-0.parquet"


def test_orderbook_writer_parquet_metadata_node_id(tmp_path: Path) -> None:
    w = OrderbookWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_B", collector_run_id="NODE_B-20260505T120000Z",
    )
    w.append(_snapshot_record(0))
    w.close()
    parquet = next((tmp_path / "market" / "orderbook").rglob("*.parquet"))
    pf = pq.ParquetFile(parquet)
    meta = pf.schema_arrow.metadata or {}
    assert meta.get(b"node_id") == b"NODE_B"


def test_orderbook_writer_legacy_no_node_id(tmp_path: Path) -> None:
    w = OrderbookWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="legacy-ob"
    )
    w.append(_snapshot_record(0))
    w.close()
    parquet = next((tmp_path / "market" / "orderbook").rglob("*.parquet"))
    assert parquet.name == "part-legacy-ob.parquet"
    assert "node=" not in parquet.as_posix()


def test_orderbook_logical_key_columns_preserved(tmp_path: Path) -> None:
    """ADR-009 §D11.8 logical key 8 column (exchange/symbol/ts_utc/event_type/side/level/price/quantity) 보존."""
    w = OrderbookWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_A", collector_run_id="NODE_A-20260505T120000Z",
    )
    w.append(_snapshot_record(0))
    w.append(_delta_record())
    w.close()
    parquet = next((tmp_path / "market" / "orderbook").rglob("*.parquet"))
    table = pq.ParquetFile(parquet).read()
    logical_key_cols = [
        "exchange", "symbol", "ts_utc", "event_type", "side", "level", "price", "quantity"
    ]
    for col in logical_key_cols:
        assert col in table.schema.names, f"missing logical key column: {col}"
        assert table[col].null_count == 0, f"logical key column {col} has nulls"
