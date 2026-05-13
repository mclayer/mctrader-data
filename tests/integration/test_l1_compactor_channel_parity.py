# tests/integration/test_l1_compactor_channel_parity.py
"""
Integration tests for L1 channel parity + orderbookdepth schema.

Story: MCT-162 Phase 2 (QADeveloperAgent lane — integration test)
Contract: Story §8 Test Contract (5 integration test suite)

Test-1: test_orderbookdepth_converter_passes
  FR-1 + FR-2 verify: sample WAL NDJSON 3-line → L1 parquet, schema parity

Test-2: test_orderbookdepth_no_notimplementederror
  FR-1 verify: _schema_version("orderbookdepth") → return "orderbook_depth.v1"

Test-3: test_unsupported_channel_fail_fast
  INV-3 silent skip 차단: unsupported channel → NotImplementedError raise, NOT silent catch

Test-4: test_unsupported_channel_prometheus_emit
  FR-3 + NFR-4: unsupported channel → Counter +1, fail-fast invariant + cardinality bounded low

Test-5: test_orderbookdepth_parquet_schema_adr_009_d11_9
  INV-5 + FR-5: generated parquet schema = ADR-009 §D11.9.2 정합
  - 11 column (or exact count per ADR-009 §D11.9.2)
  - column names in exact order + dtype invariants (raw_json = large_string mandatory)

ADR-009 §D11.9 schema contract (orderbook_depth.v1 per-level flat row, 11 column):
  | Column | Type | Nullable |
  |---|---|---|
  | ts_utc | timestamp[us, UTC] | no |
  | received_at | timestamp[us, UTC] | no |
  | exchange | string | no |
  | symbol | string | no |
  | side | string | no |
  | price | decimal128(38, 18) | no |
  | quantity | decimal128(38, 18) | no |
  | raw_json | **large_string (LargeUtf8)** | yes |
  | node_id | string | no |
  | collector_run_id | string | no |
  | ingest_seq | int64 | no |

ADR-027 D4 amendment: fail-fast invariant + Prometheus emit on unsupported channel
  - _schema_version(channel) → return version or raise NotImplementedError
  - Unsupported → Prometheus counter ``compactor_unsupported_channel_total{channel}`` emit

WAL payload sample (bithumb orderbookdepth):
  {
    "ts_utc": "2026-05-10T17:55:02.849786+00:00",
    "received_at": "2026-05-10T17:55:00.171083+00:00",
    "exchange": "bithumb",
    "symbol": "KRW-NIL",
    "changes": [
      {"side": "ask", "price": "90.79", "quantity": "28701.748"},
      {"side": "bid", "price": "89.2", "quantity": "5483.6"}
    ],
    "raw_json": "...",
    "channel": "orderbookdepth"
  }

Flat transform rule: 1 WAL frame (N levels) → N parquet rows (per-level flatten).
  Total row count = Σ len(frame.changes) across all frames in segment.
"""

from __future__ import annotations

import json
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import prometheus_client

from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed
from mctrader_data.compactor.l1 import L1Compactor, _schema_version


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def tmp_wal_root(tmp_path: Path) -> Path:
    """Temporary WAL root directory for tests."""
    return tmp_path


@pytest.fixture
def sample_orderbookdepth_ndjson() -> str:
    """Sample 3-line NDJSON payload (bithumb orderbookdepth)."""
    line1 = json.dumps({
        "ts_utc": "2026-05-10T17:55:02.849786+00:00",
        "received_at": "2026-05-10T17:55:00.171083+00:00",
        "exchange": "bithumb",
        "symbol": "KRW-NIL",
        "changes": [
            {"side": "ask", "price": "90.79", "quantity": "28701.748"},
            {"side": "bid", "price": "89.2", "quantity": "5483.6"}
        ],
        "raw_json": "{\"type\":\"orderbookdepth\",\"changes\":[...]}",
        "channel": "orderbookdepth"
    })
    line2 = json.dumps({
        "ts_utc": "2026-05-10T17:55:03.141891+00:00",
        "received_at": "2026-05-10T17:55:01.500000+00:00",
        "exchange": "bithumb",
        "symbol": "KRW-NIL",
        "changes": [
            {"side": "ask", "price": "90.79", "quantity": "0"},  # qty=0 = remove level
        ],
        "raw_json": "{\"type\":\"orderbookdepth\",\"changes\":[...]}",
        "channel": "orderbookdepth"
    })
    line3 = json.dumps({
        "ts_utc": "2026-05-10T17:55:04.200000+00:00",
        "received_at": "2026-05-10T17:55:02.000000+00:00",
        "exchange": "bithumb",
        "symbol": "KRW-NIL",
        "changes": [
            {"side": "ask", "price": "90.85", "quantity": "100.5"},
            {"side": "bid", "price": "89.15", "quantity": "200.0"},
            {"side": "bid", "price": "89.1", "quantity": "300.0"}
        ],
        "raw_json": "{\"type\":\"orderbookdepth\",\"changes\":[...]}",
        "channel": "orderbookdepth"
    })
    return "\n".join([line1, line2, line3])


def _write_orderbookdepth_segment(
    tmp_wal_root: Path,
    ndjson_content: str,
    exchange: str = "bithumb",
    symbol: str = "KRW-NIL",
    node_id: str = "NODE_A"
) -> Path:
    """Write orderbookdepth NDJSON directly to WAL, seal, and return sealed path.

    This helper bypasses WalIngester to write pre-formed NDJSON (since we don't have
    OrderbookDepthRecord dataclass yet during TDD RED phase).
    """
    wal_dir = (
        tmp_wal_root
        / "wal"
        / exchange
        / "orderbookdepth"
        / symbol
        / "2026-05-10"
    )
    wal_dir.mkdir(parents=True, exist_ok=True)

    # Write NDJSON segment (not sealed yet)
    segment_path = wal_dir / f"node={node_id}_seq=0.ndjson"
    segment_path.write_text(ndjson_content, encoding="utf-8")

    # Seal it (rename to .sealed)
    sealed_path = Path(str(segment_path) + ".sealed")
    segment_path.rename(sealed_path)

    return sealed_path


# ============================================================================
# Test 1: test_orderbookdepth_converter_passes
# ============================================================================

def test_orderbookdepth_converter_passes(
    tmp_wal_root: Path,
    sample_orderbookdepth_ndjson: str
) -> None:
    """
    Test-1: L1Compactor.compact_segment converts orderbookdepth WAL NDJSON → L1 parquet.

    Verifies:
      - L1 parquet file created
      - row count = Σ len(frame.changes) per-frame flatten
      - column count = 11 (per ADR-009 §D11.9.2)
    """
    sealed = _write_orderbookdepth_segment(tmp_wal_root, sample_orderbookdepth_ndjson)
    compactor = L1Compactor(root=tmp_wal_root)

    # Compact the segment
    parquet_path = compactor.compact_segment(sealed)

    # Verify parquet exists
    assert parquet_path.exists(), f"Parquet file not created: {parquet_path}"
    assert parquet_path.suffix == ".parquet"

    # Read parquet and verify structure
    tbl = pq.ParquetFile(parquet_path).read()

    # Expected row count: 2 + 1 + 3 = 6 rows total (per-level flatten)
    expected_rows = 2 + 1 + 3
    assert tbl.num_rows == expected_rows, (
        f"Expected {expected_rows} rows (per-level flatten), "
        f"got {tbl.num_rows}"
    )

    # Verify column count (11 per ADR-009 §D11.9.2)
    assert tbl.num_columns == 11, (
        f"Expected 11 columns (ADR-009 §D11.9.2), got {tbl.num_columns}. "
        f"Schema: {tbl.schema}"
    )


# ============================================================================
# Test 2: test_orderbookdepth_no_notimplementederror
# ============================================================================

def test_orderbookdepth_no_notimplementederror() -> None:
    """
    Test-2: _schema_version("orderbookdepth") returns "orderbook_depth.v1".

    Verifies:
      - FR-1: channel "orderbookdepth" is supported in allowlist
      - NotImplementedError is NOT raised
    """
    result = _schema_version("orderbookdepth")
    assert result == "orderbook_depth.v1", (
        f"Expected 'orderbook_depth.v1', got {result!r}"
    )


# ============================================================================
# Test 3: test_unsupported_channel_fail_fast
# ============================================================================

def test_unsupported_channel_fail_fast() -> None:
    """
    Test-3: Unsupported channel raises NotImplementedError (silent skip 차단).

    Verifies:
      - INV-3 silent skip 차단 invariant: fail-fast behavior
      - Error message contains channel name + "Supported:" list
      - ADR-027 D4 amendment compliance (fail-fast vs silent skip)
    """
    unsupported_channel = "mock_unsupported_channel_xyz"

    with pytest.raises(NotImplementedError) as exc_info:
        _schema_version(unsupported_channel)

    error_msg = str(exc_info.value)
    assert "not supported" in error_msg.lower(), (
        f"Error message missing 'not supported': {error_msg}"
    )
    assert "Supported:" in error_msg, (
        f"Error message missing 'Supported:' list: {error_msg}"
    )
    # Verify that sorted keys are in the message (minimal check)
    assert "orderbookdepth" in error_msg or "orderbooksnapshot" in error_msg, (
        f"Error message missing channel names: {error_msg}"
    )


# ============================================================================
# Test 4: test_unsupported_channel_prometheus_emit
# ============================================================================

def test_unsupported_channel_prometheus_emit() -> None:
    """
    Test-4: Unsupported channel increments Prometheus counter.

    Verifies:
      - FR-3: compactor_unsupported_channel_total{channel} Counter +1 on error
      - NFR-4: cardinality bounded low (channel name only)
      - Silent skip 차단 + observability obligation
    """
    from mctrader_data.nas_metrics.prometheus_exporters import (
        compactor_unsupported_channel_total
    )

    unsupported_channel = "mock_unsupported_channel_xyz"

    # Clear registry to start fresh (test isolation)
    # Note: In production, Prometheus client uses a global registry,
    # so we need to be careful with concurrent tests. For now, we'll
    # read the before/after values.

    # Get before value
    before = compactor_unsupported_channel_total.labels(
        channel=unsupported_channel
    )._value.get()

    # Call _schema_version (will raise + increment counter)
    with pytest.raises(NotImplementedError):
        _schema_version(unsupported_channel)

    # Get after value
    after = compactor_unsupported_channel_total.labels(
        channel=unsupported_channel
    )._value.get()

    # Verify counter incremented by 1
    assert after == before + 1, (
        f"Expected counter to increment by 1, "
        f"before={before}, after={after}"
    )


# ============================================================================
# Test 5: test_orderbookdepth_parquet_schema_adr_009_d11_9
# ============================================================================

def test_orderbookdepth_parquet_schema_adr_009_d11_9(
    tmp_wal_root: Path,
    sample_orderbookdepth_ndjson: str
) -> None:
    """
    Test-5: Generated L1 parquet schema matches ADR-009 §D11.9.2 specification.

    Verifies:
      - Column count = 11
      - Column names in exact order (per ADR-009 §D11.9.2)
      - Column dtypes match specification:
        * ts_utc / received_at = timestamp[us, UTC]
        * exchange / symbol / side / node_id / collector_run_id = string
        * price / quantity = decimal128(38, 18)
        * raw_json = large_string (LargeUtf8) [CRITICAL]
        * ingest_seq = int64

      INV-5 + FR-5 verify: ADR-009 §D11.9.6 large_string 의무 (overflow 차단)
    """
    sealed = _write_orderbookdepth_segment(tmp_wal_root, sample_orderbookdepth_ndjson)
    compactor = L1Compactor(root=tmp_wal_root)

    parquet_path = compactor.compact_segment(sealed)
    tbl = pq.ParquetFile(parquet_path).read()
    schema = tbl.schema

    # ---- Column count verification ----
    assert schema.num_fields == 11, (
        f"ADR-009 §D11.9.2 specifies 11 columns, got {schema.num_fields}. "
        f"Schema: {schema}"
    )

    # ---- Column names + order verification ----
    # Per ADR-009 §D11.9.2 (exact order required)
    expected_columns = [
        "ts_utc",
        "received_at",
        "exchange",
        "symbol",
        "side",
        "price",
        "quantity",
        "raw_json",
        "node_id",
        "collector_run_id",
        "ingest_seq",
    ]
    actual_columns = [schema.field(i).name for i in range(schema.num_fields)]

    assert actual_columns == expected_columns, (
        f"Column name/order mismatch. Expected: {expected_columns}, "
        f"got: {actual_columns}"
    )

    # ---- Column dtype verification ----
    def get_field_type(name: str) -> pa.DataType:
        return schema.field(name).type

    # Timestamp columns
    ts_utc_type = get_field_type("ts_utc")
    assert pa.types.is_timestamp(ts_utc_type), (
        f"ts_utc should be timestamp, got {ts_utc_type}"
    )
    assert ts_utc_type.tz == "UTC", (
        f"ts_utc should have UTC timezone, got {ts_utc_type.tz}"
    )

    received_at_type = get_field_type("received_at")
    assert pa.types.is_timestamp(received_at_type), (
        f"received_at should be timestamp, got {received_at_type}"
    )
    assert received_at_type.tz == "UTC", (
        f"received_at should have UTC timezone, got {received_at_type.tz}"
    )

    # String columns
    for col in ["exchange", "symbol", "side", "node_id", "collector_run_id"]:
        col_type = get_field_type(col)
        assert pa.types.is_string(col_type), (
            f"{col} should be string, got {col_type}"
        )

    # Decimal columns (38, 18)
    for col in ["price", "quantity"]:
        col_type = get_field_type(col)
        assert pa.types.is_decimal(col_type), (
            f"{col} should be decimal, got {col_type}"
        )
        assert col_type.precision == 38 and col_type.scale == 18, (
            f"{col} should be decimal128(38, 18), got {col_type}"
        )

    # raw_json = LARGE_STRING (LargeUtf8) — CRITICAL per ADR-009 §D11.9.6
    raw_json_type = get_field_type("raw_json")
    assert pa.types.is_large_string(raw_json_type), (
        f"raw_json MUST be large_string (LargeUtf8), got {raw_json_type}. "
        f"(i32 offset 4GB cap → i64 offset 8EB cap — overflow 차단 의무)"
    )

    # ingest_seq = int64
    ingest_seq_type = get_field_type("ingest_seq")
    assert pa.types.is_int64(ingest_seq_type), (
        f"ingest_seq should be int64, got {ingest_seq_type}"
    )


# ============================================================================
# Regression test: existing channels unchanged
# ============================================================================

def test_schema_version_transaction_unchanged() -> None:
    """AC-5: Regression — transaction schema version unchanged."""
    result = _schema_version("transaction")
    assert result == "tick.v1", (
        f"transaction schema changed! Expected 'tick.v1', got {result!r}"
    )


def test_schema_version_orderbooksnapshot_unchanged() -> None:
    """AC-5: Regression — orderbooksnapshot schema version unchanged."""
    result = _schema_version("orderbooksnapshot")
    assert result == "orderbook_snapshot.v1", (
        f"orderbooksnapshot schema changed! Expected 'orderbook_snapshot.v1', got {result!r}"
    )
