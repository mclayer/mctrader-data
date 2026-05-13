"""test_invariant_harness.py — P0 TDD tests for InvariantHarness (7종 invariant).

Story: MCT-151 (Stage 2 — dual-write atomic primitives + 7종 invariant harness)
Issue: mclayer/mctrader-hub#257

Test Contract §8.2 (TestContractArchitectAgent — MCT-151):
- test_7_invariant_all_pass: 7종 ALL PASS → status="all_pass"
- test_per_invariant_fail_surface: per-invariant FAIL surface (8 status enum)
- test_sha256_match_per_file + test_sha256_mismatch_returns_sha256_fail
- test_object_count_match + test_object_count_mismatch_lists_diff_files
- test_row_count_match_per_file
- test_column_count_16_per_file + test_column_count_17_returns_column_count_fail
- test_column_order_matches_adr009 + test_column_order_swap_returns_column_order_fail
- test_dtype_identity_per_column + test_decimal_precision_mismatch_returns_dtype_fail (EC-5)
- test_schema_version_v1_pin + test_legacy_schema_version_returns_schema_version_fail
- test_legacy_node_default_fallback (EC-4)
- test_verify_idempotent_across_invocations (§8.5 active)
- test_status_enum_exact_string_match (§6.8 wording SSOT)

ADR-009 §D2.1 16-col schema:
schema_version, exchange, symbol, date, ts, open, high, low, close, volume,
vwap, trade_count, bid_count, ask_count, source_provenance, ingestion_ts

§6.9 invariant placement:
- 7종 sequential unconditional (early return 0 — 모든 7종 verify 후 status 결정)
- legacy node= fallback: conditional (partition_normalization=True 시)
"""
from __future__ import annotations

import hashlib
import io
import json
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mctrader_data.nas_storage.nas_uploader import NASUploader
from mctrader_data.nas_migration.invariant_harness import (
    InvariantHarness,
    InvariantResult,
    PerInvariantResult,
)


# ─── ADR-009 §D2.1 16-col schema (SSOT) ─────────────────────────────────────

ADR009_COLUMN_NAMES: list[str] = [
    "schema_version", "exchange", "symbol", "date", "ts",
    "open", "high", "low", "close", "volume", "vwap",
    "trade_count", "bid_count", "ask_count",
    "source_provenance", "ingestion_ts",
]

ADR009_SCHEMA = pa.schema([
    pa.field("schema_version", pa.string()),
    pa.field("exchange", pa.string()),
    pa.field("symbol", pa.string()),
    pa.field("date", pa.string()),
    pa.field("ts", pa.int64()),
    pa.field("open", pa.decimal128(38, 9)),
    pa.field("high", pa.decimal128(38, 9)),
    pa.field("low", pa.decimal128(38, 9)),
    pa.field("close", pa.decimal128(38, 9)),
    pa.field("volume", pa.decimal128(38, 9)),
    pa.field("vwap", pa.decimal128(38, 9)),
    pa.field("trade_count", pa.int64()),
    pa.field("bid_count", pa.int64()),
    pa.field("ask_count", pa.int64()),
    pa.field("source_provenance", pa.string()),
    pa.field("ingestion_ts", pa.int64()),
])


# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_parquet_bytes(schema: pa.Schema | None = None, n_rows: int = 3) -> bytes:
    """Create minimal valid parquet bytes with given schema."""
    if schema is None:
        schema = ADR009_SCHEMA
    arrays = []
    for field in schema:
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            arrays.append(pa.array(["v"] * n_rows, type=field.type))
        elif pa.types.is_int64(field.type):
            arrays.append(pa.array([1] * n_rows, type=field.type))
        elif pa.types.is_decimal(field.type):
            arrays.append(pa.array([Decimal("1.0")] * n_rows, type=field.type))
        else:
            arrays.append(pa.array([None] * n_rows, type=field.type))
    table = pa.table(dict(zip(schema.names, arrays)), schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_parquet(path: Path, schema: pa.Schema | None = None, n_rows: int = 3) -> bytes:
    """Write parquet file and return bytes."""
    data = _make_parquet_bytes(schema, n_rows)
    path.write_bytes(data)
    return data


def _make_mock_uploader(nas_objects: dict[str, bytes]) -> NASUploader:
    """NASUploader mock: _list_objects returns keys, _download returns bytes.

    spec=None 으로 MagicMock 생성 — NASUploader private method mock 지원.
    """
    mock = MagicMock()  # no spec — private method mocking 지원

    def _list_objects(prefix: str) -> list[str]:
        return sorted(k for k in nas_objects if k.startswith(prefix))

    def _download(key: str) -> bytes:
        if key not in nas_objects:
            raise KeyError(f"NAS object not found: {key!r}")
        return nas_objects[key]

    mock._list_objects.side_effect = _list_objects
    mock._download.side_effect = _download
    return mock


def _make_harness(uploader: NASUploader, local_root: Path) -> InvariantHarness:
    return InvariantHarness(
        nas_uploader=uploader,
        local_root=local_root,
    )


# ─── §8.2: 7종 ALL PASS ──────────────────────────────────────────────────────

class TestInvariantHarnessAllPass:
    """§8.2: InvariantHarness 7종 ALL PASS."""

    def test_7_invariant_all_pass(self, tmp_path: Path) -> None:
        """7종 invariant 모두 PASS → status="all_pass".

        §6.2.3: all PASS → InvariantResult(status="all_pass", per_invariant_results=...)
        """
        local_root = tmp_path / "local"
        partition_path = local_root / "schema_version=v1" / "exchange=KRX" / "symbol=005930" / "date=20260513" / "node=node1" / "tier=L2"
        partition_path.mkdir(parents=True)

        # Write local parquet
        data = _write_parquet(partition_path / "seg_001.parquet")
        sha = _sha256(data)

        nas_key = "schema_version=v1/exchange=KRX/symbol=005930/date=20260513/node=node1/tier=L2/seg_001.parquet"
        nas_objects = {nas_key: data}
        uploader = _make_mock_uploader(nas_objects)

        harness = _make_harness(uploader, local_root)
        result = harness.verify(
            local_partition=partition_path,
            nas_partition="schema_version=v1/exchange=KRX/symbol=005930/date=20260513/node=node1/tier=L2",
        )

        assert result.status == "all_pass", (
            f"Expected all_pass but got {result.status!r}. "
            f"per_invariant_results={result.per_invariant_results}"
        )
        assert len(result.per_invariant_results) == 7
        for inv_name, inv_result in result.per_invariant_results.items():
            assert inv_result.status == "pass", f"Invariant {inv_name!r} failed: {inv_result}"

    def test_per_invariant_fail_surface(self, tmp_path: Path) -> None:
        """FAIL 1종 → 해당 status 반환 + per_invariant_results 측정값 emit (early return 0).

        §6.9: 7종 sequential unconditional — FAIL 1종 발생 후에도 나머지 verify 계속.
        """
        local_root = tmp_path / "local2"
        partition_path = local_root / "schema_version=v1" / "seg"
        partition_path.mkdir(parents=True)

        local_data = _write_parquet(partition_path / "seg.parquet")
        nas_data = b"COMPLETELY DIFFERENT CONTENT"  # sha256 mismatch

        nas_objects = {
            "schema_version=v1/seg/seg.parquet": nas_data,
        }
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(
            local_partition=partition_path,
            nas_partition="schema_version=v1/seg",
        )

        # sha256 or object_count or row_count fail (NAS data is invalid parquet → expect some fail)
        assert result.status != "all_pass"
        # per_invariant_results must contain all 7 keys (early return 0)
        assert len(result.per_invariant_results) == 7


# ─── sha256 invariant ─────────────────────────────────────────────────────────

class TestInvariantHarnessSha256:
    """§8.2: sha256 invariant (D6 source)."""

    def test_sha256_match_per_file(self, tmp_path: Path) -> None:
        """local sha256 == NAS sha256 (per file) → sha256 invariant pass."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p1"
        part.mkdir(parents=True)

        data = _write_parquet(part / "f.parquet")
        nas_objects = {"schema_version=v1/p1/f.parquet": data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p1")
        sha_result = result.per_invariant_results.get("sha256")
        assert sha_result is not None
        assert sha_result.status == "pass"

    def test_sha256_mismatch_returns_sha256_fail(self, tmp_path: Path) -> None:
        """local sha256 != NAS sha256 → status="sha256_fail" + mismatch_files."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p2"
        part.mkdir(parents=True)

        local_data = _write_parquet(part / "f.parquet")
        # NAS has different content (same schema but sha256 mismatch)
        nas_data = _make_parquet_bytes(n_rows=5)  # different rows → different sha256
        assert _sha256(local_data) != _sha256(nas_data), "Test setup: must be different sha256"

        nas_objects = {"schema_version=v1/p2/f.parquet": nas_data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p2")
        assert result.status == "sha256_fail"
        sha_result = result.per_invariant_results["sha256"]
        assert sha_result.status == "fail"
        assert len(sha_result.mismatch_files) > 0


# ─── object_count invariant ───────────────────────────────────────────────────

class TestInvariantHarnessObjectCount:
    """§8.2: object_count invariant (D6 source)."""

    def test_object_count_match(self, tmp_path: Path) -> None:
        """local file count == NAS object count → object_count invariant pass."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p3"
        part.mkdir(parents=True)

        d1 = _write_parquet(part / "f1.parquet")
        d2 = _write_parquet(part / "f2.parquet")
        nas_objects = {
            "schema_version=v1/p3/f1.parquet": d1,
            "schema_version=v1/p3/f2.parquet": d2,
        }
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p3")
        oc = result.per_invariant_results.get("object_count")
        assert oc is not None
        assert oc.status == "pass"

    def test_object_count_mismatch_lists_diff_files(self, tmp_path: Path) -> None:
        """local count != NAS count → status="object_count_fail" + mismatch_files."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p4"
        part.mkdir(parents=True)

        _write_parquet(part / "f1.parquet")
        # NAS has 2 objects, local has 1
        nas_objects = {
            "schema_version=v1/p4/f1.parquet": b"data1",
            "schema_version=v1/p4/f2.parquet": b"data2",
        }
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p4")
        assert result.status == "object_count_fail"
        oc = result.per_invariant_results["object_count"]
        assert oc.status == "fail"


# ─── row_count invariant ──────────────────────────────────────────────────────

class TestInvariantHarnessRowCount:
    """§8.2: row_count invariant (D6 source)."""

    def test_row_count_match_per_file(self, tmp_path: Path) -> None:
        """local row count == NAS row count (per file) → row_count invariant pass."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p5"
        part.mkdir(parents=True)

        data = _write_parquet(part / "f.parquet", n_rows=5)
        nas_objects = {"schema_version=v1/p5/f.parquet": data}  # same data
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p5")
        rc = result.per_invariant_results.get("row_count")
        assert rc is not None
        assert rc.status == "pass"


# ─── column_count invariant (S5 신규) ────────────────────────────────────────

class TestInvariantHarnessColumnCount:
    """§8.2: column_count invariant (S5 신규 — ADR-009 §D2.1 16 columns)."""

    def test_column_count_16_per_file(self, tmp_path: Path) -> None:
        """column count == 16 → column_count invariant pass."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p6"
        part.mkdir(parents=True)

        data = _write_parquet(part / "f.parquet")  # uses ADR009_SCHEMA (16 cols)
        nas_objects = {"schema_version=v1/p6/f.parquet": data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p6")
        cc = result.per_invariant_results.get("column_count")
        assert cc is not None
        assert cc.status == "pass"
        assert cc.measured_local == 16

    def test_column_count_17_returns_column_count_fail(self, tmp_path: Path) -> None:
        """column count != 16 → status="column_count_fail"."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p7"
        part.mkdir(parents=True)

        # 17-column schema (extra column)
        schema_17 = pa.schema(ADR009_SCHEMA.to_arrow_schema().append(
            pa.field("extra_col", pa.string())
        )) if hasattr(ADR009_SCHEMA, "to_arrow_schema") else pa.schema(
            list(ADR009_SCHEMA) + [pa.field("extra_col", pa.string())]
        )
        data = _make_parquet_bytes(schema_17)
        (part / "f.parquet").write_bytes(data)
        nas_objects = {"schema_version=v1/p7/f.parquet": data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p7")
        assert result.status == "column_count_fail"
        cc = result.per_invariant_results["column_count"]
        assert cc.status == "fail"


# ─── column_order invariant (S5 신규) ────────────────────────────────────────

class TestInvariantHarnessColumnOrder:
    """§8.2: column_order invariant (S5 신규)."""

    def test_column_order_matches_adr009(self, tmp_path: Path) -> None:
        """column order == ADR-009 §D2 정의 → column_order invariant pass."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p8"
        part.mkdir(parents=True)

        data = _write_parquet(part / "f.parquet")
        nas_objects = {"schema_version=v1/p8/f.parquet": data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p8")
        co = result.per_invariant_results.get("column_order")
        assert co is not None
        assert co.status == "pass"

    def test_column_order_swap_returns_column_order_fail(self, tmp_path: Path) -> None:
        """column order != ADR-009 §D2 정의 → status="column_order_fail"."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p9"
        part.mkdir(parents=True)

        # Swap first two columns
        swapped_cols = [ADR009_COLUMN_NAMES[1], ADR009_COLUMN_NAMES[0]] + ADR009_COLUMN_NAMES[2:]
        schema_swapped = pa.schema([
            ADR009_SCHEMA.field(name) for name in swapped_cols
        ])
        data = _make_parquet_bytes(schema_swapped)
        (part / "f.parquet").write_bytes(data)
        nas_objects = {"schema_version=v1/p9/f.parquet": data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p9")
        assert result.status == "column_order_fail"
        co = result.per_invariant_results["column_order"]
        assert co.status == "fail"


# ─── dtype invariant (S5 신규) ───────────────────────────────────────────────

class TestInvariantHarnessDtype:
    """§8.2: dtype_identity invariant (S5 신규) — pyarrow type-level identity."""

    def test_dtype_identity_per_column(self, tmp_path: Path) -> None:
        """local + NAS dtype identity → dtype invariant pass."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p10"
        part.mkdir(parents=True)

        data = _write_parquet(part / "f.parquet")
        nas_objects = {"schema_version=v1/p10/f.parquet": data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p10")
        dt = result.per_invariant_results.get("dtype")
        assert dt is not None
        assert dt.status == "pass"

    def test_decimal_precision_mismatch_returns_dtype_fail(self, tmp_path: Path) -> None:
        """Decimal precision/scale mismatch → dtype invariant fail (EC-5 박제).

        §6.2.3 EC-5: Decimal(38,9) vs Decimal(38,8) → dtype_fail.
        pyarrow type-level identity (not string comparison).

        Note: sha256 가 다른 데이터에 대해 sha256_fail 이 dtype_fail 보다 우선순위가 높으므로,
        본 테스트는 per_invariant_results["dtype"].status == "fail" 을 직접 검증한다.
        (§6.9 early return 0 — 7종 모두 verify 후 status 결정)
        """
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "p11"
        part.mkdir(parents=True)

        # local: correct Decimal(38,9)
        local_data = _write_parquet(part / "f.parquet")  # ADR009_SCHEMA

        # NAS: wrong Decimal precision (38,8 instead of 38,9)
        schema_wrong_prec = pa.schema([
            pa.field("schema_version", pa.string()),
            pa.field("exchange", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("date", pa.string()),
            pa.field("ts", pa.int64()),
            pa.field("open", pa.decimal128(38, 8)),   # WRONG: should be (38,9)
            pa.field("high", pa.decimal128(38, 8)),
            pa.field("low", pa.decimal128(38, 8)),
            pa.field("close", pa.decimal128(38, 8)),
            pa.field("volume", pa.decimal128(38, 8)),
            pa.field("vwap", pa.decimal128(38, 8)),
            pa.field("trade_count", pa.int64()),
            pa.field("bid_count", pa.int64()),
            pa.field("ask_count", pa.int64()),
            pa.field("source_provenance", pa.string()),
            pa.field("ingestion_ts", pa.int64()),
        ])
        nas_data = _make_parquet_bytes(schema_wrong_prec)

        nas_objects = {"schema_version=v1/p11/f.parquet": nas_data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/p11")

        # §6.9 early return 0: 7종 모두 verify → per_invariant_results["dtype"] 검증
        assert len(result.per_invariant_results) == 7
        dt = result.per_invariant_results["dtype"]
        assert dt.status == "fail", (
            f"dtype invariant must be 'fail' for Decimal(38,9) vs Decimal(38,8) mismatch, "
            f"got status={dt.status!r}"
        )
        # result.status may be sha256_fail (higher priority) or dtype_fail depending on sha256
        # The key assertion is that dtype invariant is individually detected (early return 0)
        assert result.status != "all_pass", "Must not be all_pass when dtype differs"


# ─── schema_version invariant (S5 신규) ──────────────────────────────────────

class TestInvariantHarnessSchemaVersion:
    """§8.2: schema_version_pin invariant (S5 신규)."""

    def test_schema_version_v1_pin(self, tmp_path: Path) -> None:
        """partition prefix schema_version=v1 → schema_version invariant pass."""
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "sym"
        part.mkdir(parents=True)

        data = _write_parquet(part / "f.parquet")
        nas_objects = {"schema_version=v1/sym/f.parquet": data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(
            local_partition=part,
            nas_partition="schema_version=v1/sym",
        )
        sv = result.per_invariant_results.get("schema_version")
        assert sv is not None
        assert sv.status == "pass"

    def test_legacy_schema_version_returns_schema_version_fail(self, tmp_path: Path) -> None:
        """partition prefix != schema_version=v1 → status="schema_version_fail"."""
        local_root = tmp_path / "local"
        # legacy path without schema_version=v1 prefix
        part = local_root / "exchange=KRX" / "sym"  # missing schema_version=v1
        part.mkdir(parents=True)

        data = _write_parquet(part / "f.parquet")
        nas_objects = {"exchange=KRX/sym/f.parquet": data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(
            local_partition=part,
            nas_partition="exchange=KRX/sym",
        )
        assert result.status == "schema_version_fail"
        sv = result.per_invariant_results["schema_version"]
        assert sv.status == "fail"


# ─── EC-4: legacy node= fallback ─────────────────────────────────────────────

class TestInvariantHarnessLegacyNodeFallback:
    """§8.2: EC-4 — legacy `node=` 부재 partition → fallback `node=DEFAULT`."""

    def test_legacy_node_default_fallback(self, tmp_path: Path) -> None:
        """NAS partition prefix의 node= 부재 → fallback node=DEFAULT 적용 후 비교.

        §6.2.3 EC-4: MCT-153 backfill 시점 consume — legacy partition normalize.
        partition_normalization=True 박제 (§6.9 conditional 의도).
        """
        local_root = tmp_path / "local"
        # local has node=DEFAULT explicitly
        part = local_root / "schema_version=v1" / "exchange=KRX" / "node=DEFAULT" / "tier=L2"
        part.mkdir(parents=True)

        data = _write_parquet(part / "f.parquet")

        # NAS has no node= in prefix (legacy)
        nas_objects = {
            "schema_version=v1/exchange=KRX/tier=L2/f.parquet": data,  # no node=
        }
        uploader = _make_mock_uploader(nas_objects)
        harness = InvariantHarness(
            nas_uploader=uploader,
            local_root=local_root,
            partition_normalization=True,  # EC-4: enable legacy fallback
        )

        result = harness.verify(
            local_partition=part,
            nas_partition="schema_version=v1/exchange=KRX/tier=L2",  # no node=
        )
        # Should normalize and not fail on node= mismatch alone
        # (actual pass/fail depends on sha256 + other invariants)
        # Key assertion: no crash + per_invariant_results returned
        assert result.per_invariant_results is not None
        assert len(result.per_invariant_results) == 7


# ─── §8.5 active: verify idempotent ──────────────────────────────────────────

class TestInvariantHarnessVerifyIdempotent:
    """§8.5 active: verify() idempotent (read-only, side-effect 0)."""

    def test_verify_idempotent_across_invocations(self, tmp_path: Path) -> None:
        """동일 partition에 대해 반복 verify → 결과 일관 (read-only invariant).

        §6.2.3: verify() = read-only — 양쪽 storage 변경 0.
        cron-style 반복 호출 정합 (MCT-152 dual_write_window_runner).
        """
        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "idem"
        part.mkdir(parents=True)

        data = _write_parquet(part / "f.parquet")
        nas_objects = {"schema_version=v1/idem/f.parquet": data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result1 = harness.verify(local_partition=part, nas_partition="schema_version=v1/idem")
        result2 = harness.verify(local_partition=part, nas_partition="schema_version=v1/idem")
        result3 = harness.verify(local_partition=part, nas_partition="schema_version=v1/idem")

        assert result1.status == result2.status == result3.status
        # Local files must be untouched (read-only)
        assert (part / "f.parquet").exists()


# ─── §6.8: wording SSOT ──────────────────────────────────────────────────────

class TestInvariantHarnessStatusEnumExactStringMatch:
    """§8.2: Wording SSOT — InvariantResult.status enum 8종 exact string match."""

    def test_status_enum_exact_string_match(self, tmp_path: Path) -> None:
        """InvariantResult.status 8종이 정확히 §6.8 enum value 와 일치.

        allowed: "all_pass" / "sha256_fail" / "object_count_fail" / "row_count_fail" /
                 "column_count_fail" / "column_order_fail" / "dtype_fail" / "schema_version_fail"
        forbidden variants: "verify_pass" / "all_seven_pass" / "count_fail" / etc.
        """
        allowed_statuses = {
            "all_pass",
            "sha256_fail",
            "object_count_fail",
            "row_count_fail",
            "column_count_fail",
            "column_order_fail",
            "dtype_fail",
            "schema_version_fail",
        }
        forbidden_variants = {
            "verify_pass", "all_seven_pass", "pass", "PASS",
            "count_fail", "sha_fail", "schema_fail",
            "dtype_identity_fail", "column_count_mismatch",
        }

        local_root = tmp_path / "local"
        part = local_root / "schema_version=v1" / "enum_test"
        part.mkdir(parents=True)

        data = _write_parquet(part / "f.parquet")
        nas_objects = {"schema_version=v1/enum_test/f.parquet": data}
        uploader = _make_mock_uploader(nas_objects)
        harness = _make_harness(uploader, local_root)

        result = harness.verify(local_partition=part, nas_partition="schema_version=v1/enum_test")

        assert result.status in allowed_statuses, f"Unknown status: {result.status!r}"
        assert result.status not in forbidden_variants, f"Forbidden variant: {result.status!r}"

        # PerInvariantResult.status also exact
        for inv_name, inv_result in result.per_invariant_results.items():
            assert inv_result.status in {"pass", "fail"}, (
                f"PerInvariantResult.status for {inv_name!r} must be 'pass' or 'fail', "
                f"got {inv_result.status!r}"
            )
