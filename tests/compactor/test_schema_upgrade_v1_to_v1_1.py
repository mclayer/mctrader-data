# tests/compactor/test_schema_upgrade_v1_to_v1_1.py
"""MCT-141 — tick.v1 (8 col) → tick.v1.1 (11 col) Parquet schema upgrade reader.

Coverage:
- legacy v1.0 Parquet read via v1.1 reader → 3 new cols defaulted
  (ingest_seq=NULL, payload_hash=NULL, validation_status="OK")
- native v1.1 Parquet read → 3 new cols preserved
- TICK_V1_1_SCHEMA structural assertions (11 cols, types)
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mctrader_data.compactor.schema_upgrade import (
    TICK_V1_1_SCHEMA,
    TICK_V1_1_SCHEMA_VERSION,
    read_tick_parquet_as_v1_1,
    upgrade_v1_table_to_v1_1,
)
from mctrader_data.tick_storage import TICK_SCHEMA_VERSION, _TICK_SCHEMA


def _v1_table() -> pa.Table:
    """Construct a 2-row tick.v1 (8 col) Arrow table."""
    return pa.Table.from_pydict(
        {
            "ts_utc": [
                datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 5, 12, 0, 0, 1, tzinfo=timezone.utc),
            ],
            "received_at": [
                datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 5, 12, 0, 0, 1, tzinfo=timezone.utc),
            ],
            "exchange": ["bithumb", "bithumb"],
            "symbol": ["KRW-BTC", "KRW-BTC"],
            "price": [Decimal("100000000.123456789012345678"), Decimal("100000001.000000000000000000")],
            "quantity": [Decimal("0.001000000000000000"), Decimal("0.002000000000000000")],
            "side": ["buy", "sell"],
            "raw_json": ['{"a":1}', None],
        },
        schema=_TICK_SCHEMA,
    )


def test_tick_v1_1_schema_has_11_columns():
    assert TICK_V1_1_SCHEMA_VERSION == "tick.v1.1"
    names = TICK_V1_1_SCHEMA.names
    assert len(names) == 11
    # Baseline 8 from v1 preserved
    for col in ("ts_utc", "received_at", "exchange", "symbol", "price", "quantity", "side", "raw_json"):
        assert col in names
    # 3 new v1.1 cols
    assert "ingest_seq" in names
    assert "payload_hash" in names
    assert "validation_status" in names


def test_upgrade_v1_table_to_v1_1_defaults_new_columns():
    src = _v1_table()
    upgraded = upgrade_v1_table_to_v1_1(src)
    assert upgraded.num_columns == 11
    assert upgraded.num_rows == 2
    assert upgraded.schema.field("ingest_seq").type == pa.uint64()
    assert upgraded.schema.field("payload_hash").type == pa.string()
    assert upgraded.schema.field("validation_status").type == pa.string()

    ingest_seq = upgraded.column("ingest_seq").to_pylist()
    payload_hash = upgraded.column("payload_hash").to_pylist()
    validation_status = upgraded.column("validation_status").to_pylist()
    assert ingest_seq == [None, None]
    assert payload_hash == [None, None]
    assert validation_status == ["OK", "OK"]


def test_read_tick_parquet_as_v1_1_reads_legacy_v1_file(tmp_path: Path):
    src = _v1_table()
    target = tmp_path / "legacy.parquet"
    pq.write_table(src, str(target))

    upgraded = read_tick_parquet_as_v1_1(target)
    assert upgraded.schema.equals(TICK_V1_1_SCHEMA, check_metadata=False)
    assert upgraded.num_rows == 2
    # legacy row defaults
    assert upgraded.column("validation_status").to_pylist() == ["OK", "OK"]
    assert upgraded.column("ingest_seq").to_pylist() == [None, None]
    assert upgraded.column("payload_hash").to_pylist() == [None, None]


def test_read_tick_parquet_as_v1_1_preserves_native_v1_1(tmp_path: Path):
    """v1.1-native file → reader passes through without defaulting."""
    native = pa.Table.from_pydict(
        {
            "ts_utc": [datetime(2026, 5, 12, 0, 0, 2, tzinfo=timezone.utc)],
            "received_at": [datetime(2026, 5, 12, 0, 0, 2, tzinfo=timezone.utc)],
            "exchange": ["bithumb"],
            "symbol": ["KRW-BTC"],
            "price": [Decimal("100000002.000000000000000000")],
            "quantity": [Decimal("0.003000000000000000")],
            "side": ["buy"],
            "raw_json": [None],
            "ingest_seq": [42],
            "payload_hash": ["abc123"],
            "validation_status": ["GAP"],
        },
        schema=TICK_V1_1_SCHEMA,
    )
    target = tmp_path / "native.parquet"
    pq.write_table(native, str(target))

    upgraded = read_tick_parquet_as_v1_1(target)
    assert upgraded.column("ingest_seq").to_pylist() == [42]
    assert upgraded.column("payload_hash").to_pylist() == ["abc123"]
    assert upgraded.column("validation_status").to_pylist() == ["GAP"]


def test_legacy_schema_version_unchanged():
    """tick_storage.TICK_SCHEMA_VERSION stays at v1.0 — v1.1 is owned by schema_upgrade module."""
    assert TICK_SCHEMA_VERSION == "tick.v1"
    assert TICK_V1_1_SCHEMA_VERSION != TICK_SCHEMA_VERSION
