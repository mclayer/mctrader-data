# src/mctrader_data/compactor/schema_upgrade.py
"""tick.v1 (8 col) → tick.v1.1 (11 col) Parquet schema upgrade (MCT-141).

Owner: Epic MCT-112 Story-7 — resolves Story-5/6 forwarded open question.

Schema delta (additive, SemVer MINOR per ADR-008):
- baseline 8 cols preserved: ts_utc, received_at, exchange, symbol, price,
  quantity, side, raw_json (per ``mctrader_data.tick_storage._TICK_SCHEMA``).
- 3 new cols appended for ingestion provenance + integrity:
    * ``ingest_seq``         (uint64, nullable)  — monotonic per-stream counter
    * ``payload_hash``       (string, nullable)  — SHA-256(canonical body)[:16]
    * ``validation_status``  (string, default="OK")
      enum: "OK" / "GAP" / "MALFORMED" / "RECONNECT_BOUNDARY"

Logical schema mirrors ``mctrader_market.schemas.tick.TickRowV1_1``. This module
is the physical PyArrow schema + reader; the logical row class stays in
mctrader-market (boundary contract, ADR-009 §D10.8).

Backward compatibility:
- legacy tick.v1 (8 col) Parquet → ``read_tick_parquet_as_v1_1`` defaults the
  3 new cols (ingest_seq=NULL, payload_hash=NULL, validation_status="OK").
- native tick.v1.1 (11 col) Parquet → passes through unchanged.
- v1.0 reader keeps reading v1.1 files because the new cols are appended
  (PyArrow strict-schema reads on existing columns).
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.tick_storage import _TICK_SCHEMA  # 8-col baseline

TICK_V1_1_SCHEMA_VERSION = "tick.v1.1"
"""SemVer tag for the upgraded tick row schema (ADR-009 §D10.8)."""

# 11-col physical schema. Order mirrors the logical class (TickRowV1_1) for
# PyArrow Hive-partition friendliness; the 8 baseline fields use the same types
# as ``_TICK_SCHEMA`` so legacy files cast trivially.
TICK_V1_1_SCHEMA: pa.Schema = pa.schema([
    pa.field("ts_utc", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("received_at", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("price", pa.decimal128(38, 18), nullable=False),
    pa.field("quantity", pa.decimal128(38, 18), nullable=False),
    pa.field("side", pa.string(), nullable=False),
    pa.field("raw_json", pa.string(), nullable=True),
    # v1.1 extension (additive)
    pa.field("ingest_seq", pa.uint64(), nullable=True),
    pa.field("payload_hash", pa.string(), nullable=True),
    pa.field("validation_status", pa.string(), nullable=False),
])

_V1_BASELINE_COLS: tuple[str, ...] = tuple(_TICK_SCHEMA.names)  # ts_utc..raw_json (8)
_V1_1_EXTENSION_COLS: tuple[str, ...] = ("ingest_seq", "payload_hash", "validation_status")


def upgrade_v1_table_to_v1_1(table: pa.Table) -> pa.Table:
    """Project a v1.0 ``table`` into the v1.1 11-col schema.

    For each missing extension column, append a NULL/default-filled array of
    matching length. Idempotent on already-upgraded tables (returns as-is when
    the input schema already equals ``TICK_V1_1_SCHEMA``).
    """
    if table.schema.equals(TICK_V1_1_SCHEMA, check_metadata=False):
        return table

    n = table.num_rows
    columns = list(table.columns)
    names = list(table.column_names)

    # ingest_seq → all NULL uint64
    if "ingest_seq" not in names:
        columns.append(pa.array([None] * n, type=pa.uint64()))
        names.append("ingest_seq")
    # payload_hash → all NULL string
    if "payload_hash" not in names:
        columns.append(pa.array([None] * n, type=pa.string()))
        names.append("payload_hash")
    # validation_status → default "OK" string
    if "validation_status" not in names:
        columns.append(pa.array(["OK"] * n, type=pa.string()))
        names.append("validation_status")

    upgraded = pa.Table.from_arrays(columns, names=names)
    # Cast to canonical schema (re-orders cols + asserts types).
    return upgraded.cast(TICK_V1_1_SCHEMA)


def read_tick_parquet_as_v1_1(path: Path | str) -> pa.Table:
    """Read a Parquet file (v1.0 or v1.1) into the v1.1 11-col schema.

    Uses ``pq.ParquetFile`` (not ``pq.read_table(dir)``) per L1Compactor convention
    to avoid PyArrow Hive auto-discovery conflicts.
    """
    pf = pq.ParquetFile(str(path))
    table = pf.read()
    return upgrade_v1_table_to_v1_1(table)


__all__ = [
    "TICK_V1_1_SCHEMA",
    "TICK_V1_1_SCHEMA_VERSION",
    "read_tick_parquet_as_v1_1",
    "upgrade_v1_table_to_v1_1",
]
