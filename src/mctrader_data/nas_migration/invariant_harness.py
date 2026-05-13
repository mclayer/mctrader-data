"""invariant_harness.py — 7종 invariant single-shot verify (S5 박제, ADR-027 D6 amendment trigger).

Story: MCT-151 (Stage 2 — dual-write atomic primitives + 7종 invariant harness)
Issue: mclayer/mctrader-hub#257

Design decisions (§6.2.3 Change Plan 박제):

S5 박제 (scope_manifest design_decisions S5):
"D6 박제 3종 + column count + column name order + dtype identity + schema_version pin = 7종 invariant
— D6 PASS 이나 Parquet schema 차이로 read 파괴/오염 risk 차단 (reader-breaking drift 포착)"

ADR-027 D6 mandatory amendment trigger (triggers_adr_amendment.mandatory=true, trigger_story=MCT-151):
- D6 본문 3종 (sha256 + object_count + row_count) → 7종으로 확장 (본 Story 박제)
- amendment 시점: Phase 2 PR merge 또는 retro 시점 (§6.6 결정)

§6.9 invariant placement:
- 7종 sequential unconditional (early return 0 — FAIL 1종 발생 후에도 나머지 6종 모두 verify).
  D6 본문 "1종이라도 FAIL 시 cutover 차단" + per-invariant 측정값 emit 의무 (diagnostic dump).
- legacy node= fallback: conditional (partition_normalization=True 시만 적용, EC-4 박제).

§6.8 Wording SSOT:
- InvariantResult.status 8종: "all_pass" / "sha256_fail" / "object_count_fail" / "row_count_fail" /
  "column_count_fail" / "column_order_fail" / "dtype_fail" / "schema_version_fail"
  variant 금지: "verify_pass" / "all_seven_pass" / "sha_fail" / "dtype_identity_fail" 등.
- PerInvariantResult.status 2종: "pass" / "fail" (lowercase).

ADR-009 §D2.1 16-col schema (SSOT — column count=16, order 박제):
schema_version, exchange, symbol, date, ts, open, high, low, close, volume,
vwap, trade_count, bid_count, ask_count, source_provenance, ingestion_ts

EC-4 (legacy `node=` 부재): partition_normalization=True 시 node= 부재 파티션에 fallback node=DEFAULT.
EC-5 (Decimal precision/scale mismatch): pyarrow type-level identity 비교 (string 비교 금지).

Caller (MCT-152 dual_write_window_runner cron / MCT-153 backfill / MCT-155 cutover) 가 inject.
본 Story scope = harness 정의, 실 caller 통합은 downstream Story scope.

SecurityArch (§6.3):
- mismatch_files = data path only (credential 0)
- log 출력 시 nas_partition prefix 만 (endpoint URL 포함 금지)
"""
from __future__ import annotations

import hashlib
import io
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.nas_storage.nas_uploader import NASUploader

if TYPE_CHECKING:
    from mctrader_data.nas_metrics.prometheus_exporters import PrometheusExporter

log = logging.getLogger(__name__)

# ─── ADR-009 §D2.1 16-col schema SSOT ───────────────────────────────────────
# 변경 시 ADR-009 amendment 의무 (column count + order 모두 SSOT)
ADR009_EXPECTED_COLUMN_COUNT: int = 16
ADR009_EXPECTED_COLUMN_NAMES: tuple[str, ...] = (
    "schema_version", "exchange", "symbol", "date", "ts",
    "open", "high", "low", "close", "volume", "vwap",
    "trade_count", "bid_count", "ask_count",
    "source_provenance", "ingestion_ts",
)
ADR009_EXPECTED_SCHEMA_VERSION: str = "v1"

# §6.8 Wording SSOT — InvariantResult.status enum 8종 (frozen)
_INVARIANT_NAMES: tuple[str, ...] = (
    "sha256", "object_count", "row_count",
    "column_count", "column_order", "dtype", "schema_version",
)


@dataclass(frozen=True)
class PerInvariantResult:
    """Individual invariant 의 PASS/FAIL + 측정값.

    invariant_name ∈ {"sha256", "object_count", "row_count", "column_count",
                       "column_order", "dtype", "schema_version"}
    status ∈ {"pass", "fail"} (lowercase, §6.8 SSOT)
    measured_local: invariant 별 측정값
    measured_nas: NAS 측 측정값
    mismatch_files: FAIL 시 mismatch 파일/key list (per-file granularity)
    """

    invariant_name: str
    status: Literal["pass", "fail"]
    measured_local: str | int | list[str] | dict
    measured_nas: str | int | list[str] | dict
    mismatch_files: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class InvariantResult:
    """7종 invariant verify 의 result enum + per-invariant 측정값.

    status enum 8종 (§6.8 Wording SSOT 박제 — single string, variant 금지):
    - "all_pass":             7종 invariant ALL PASS (cutover 차단 0).
    - "sha256_fail":          local sha256 != NAS sha256 (1종 이상 file).
    - "object_count_fail":    local file count != NAS object count.
    - "row_count_fail":       local row count != NAS row count (1종 이상 file).
    - "column_count_fail":    column count != 16 (ADR-009 §D2.1, 1종 이상 file).
    - "column_order_fail":    column order != ADR-009 §D2 정의 (1종 이상 file).
    - "dtype_fail":           dtype mismatch (Decimal precision/scale 포함, 1종 이상 file).
    - "schema_version_fail":  partition prefix != schema_version=v1 (legacy schema 침범).

    D6 본문 wording 정합: 1종이라도 FAIL 시 cutover 차단 의무 (caller 측 status 검사 후 결정).
    per_invariant_results: 7종 별 PerInvariantResult (early return 0 — 모든 7종 verify 후 결정).
    """

    status: Literal[
        "all_pass",
        "sha256_fail",
        "object_count_fail",
        "row_count_fail",
        "column_count_fail",
        "column_order_fail",
        "dtype_fail",
        "schema_version_fail",
    ]
    per_invariant_results: dict[str, PerInvariantResult] = field(default_factory=dict)
    local_partition: Path | None = None
    nas_partition: str | None = None
    verify_latency_ms: float = 0.0


class InvariantHarness:
    """7종 invariant single-shot verify (S5 박제, ADR-027 D6 amendment trigger source).

    7종 invariant 의 layer 별 분류 (§11.3):
    - byte-level (1종): sha256
    - set-level (2종): object_count, row_count
    - schema-level (4종): column_count, column_order, dtype, schema_version

    §6.9 invariant placement:
    - 7종 sequential unconditional (early return 0 — FAIL 1종 후에도 나머지 verify 계속).
    - legacy node= fallback: conditional (partition_normalization=True 시만).

    Caller (MCT-152 / MCT-153 / MCT-155) 가 inject — 본 Story scope = harness 정의.
    read-only invariant: 양쪽 storage 변경 0 (§11.2 forward-only 보존).
    """

    def __init__(
        self,
        nas_uploader: NASUploader,
        local_root: Path,
        metrics: PrometheusExporter | None = None,
        expected_column_count: int = ADR009_EXPECTED_COLUMN_COUNT,
        expected_column_names: tuple[str, ...] = ADR009_EXPECTED_COLUMN_NAMES,
        expected_schema_version: str = ADR009_EXPECTED_SCHEMA_VERSION,
        partition_normalization: bool = False,  # EC-4: legacy node= fallback
    ) -> None:
        self._uploader = nas_uploader
        self._local_root = local_root
        self._metrics = metrics
        self._expected_column_count = expected_column_count
        self._expected_column_names = list(expected_column_names)
        self._expected_schema_version = expected_schema_version
        self._partition_normalization = partition_normalization

    def verify(
        self,
        *,
        local_partition: Path,
        nas_partition: str,
    ) -> InvariantResult:
        """7종 invariant single-shot verify (§6.9 unconditional sequential — early return 0).

        Algorithm:
        1. list local .parquet files from local_partition
        2. list NAS objects from nas_partition (via _list_objects)
        3. 7종 invariant verify (sequential unconditional, FAIL 발생 후에도 계속):
           - object_count (set-level): local count == NAS count
           - sha256 (byte-level): per file sha256 match
           - row_count (set-level): per file row count match
           - column_count (schema-level): == expected_column_count (16)
           - column_order (schema-level): == expected_column_names
           - dtype (schema-level): pyarrow type-level identity (EC-5: Decimal precision/scale)
           - schema_version (schema-level): partition prefix schema_version={expected_schema_version}
        4. status 결정: priority 순서
           (object_count > sha256 > row_count > column_count > column_order > dtype > schema_version)
        5. InvariantResult return

        EC-4: partition_normalization=True 시 nas_partition 의 node= 부재 → fallback node=DEFAULT.
        EC-5: dtype 비교 시 pyarrow type-level identity (str(type) 비교 — precision/scale 포함).

        Read-only invariant: 양쪽 storage 변경 0.
        """
        start_ms = time.monotonic() * 1000

        # ── gather local files ─────────────────────────────────────────────────
        local_files = sorted(local_partition.glob("*.parquet"))

        # ── EC-4: partition normalization (conditional) ────────────────────────
        effective_nas_partition = nas_partition
        if self._partition_normalization:
            effective_nas_partition = self._normalize_nas_partition(nas_partition)

        # ── gather NAS objects ─────────────────────────────────────────────────
        nas_objects = self._uploader._list_objects(prefix=effective_nas_partition)

        # ── 7종 invariant sequential unconditional verify ─────────────────────
        per_results: dict[str, PerInvariantResult] = {}

        # 1. schema_version pin (partition prefix — checked before per-file)
        per_results["schema_version"] = self._check_schema_version(
            local_partition, effective_nas_partition
        )

        # 2. object_count (set-level)
        per_results["object_count"] = self._check_object_count(local_files, nas_objects)

        # 3~7: per-file invariants (sha256, row_count, column_count, column_order, dtype)
        sha256_mismatches: list[str] = []
        row_count_mismatches: list[str] = []
        column_count_mismatches: list[str] = []
        column_order_mismatches: list[str] = []
        dtype_mismatches: list[str] = []

        # Match local files to NAS objects by basename
        local_basenames = {f.name: f for f in local_files}
        nas_basenames = {k.split("/")[-1]: k for k in nas_objects}

        # Only verify files present in both (object_count handles count mismatch)
        common_basenames = sorted(set(local_basenames) & set(nas_basenames))

        for basename in common_basenames:
            local_file = local_basenames[basename]
            nas_key = nas_basenames[basename]

            try:
                nas_data = self._uploader._download(nas_key)
            except Exception as e:
                log.warning("InvariantHarness: NAS download failed for %r: %s", nas_key, e)
                sha256_mismatches.append(basename)
                row_count_mismatches.append(basename)
                column_count_mismatches.append(basename)
                column_order_mismatches.append(basename)
                dtype_mismatches.append(basename)
                continue

            local_data = local_file.read_bytes()

            # sha256 (byte-level)
            local_sha = hashlib.sha256(local_data).hexdigest()
            nas_sha = hashlib.sha256(nas_data).hexdigest()
            if local_sha != nas_sha:
                sha256_mismatches.append(basename)

            # row_count, column_count, column_order, dtype (schema-level)
            try:
                local_schema, local_rows = self._read_parquet_meta(local_data)
                nas_schema, nas_rows = self._read_parquet_meta(nas_data)
            except Exception as e:
                log.warning("InvariantHarness: parquet read failed for %r: %s", basename, e)
                row_count_mismatches.append(basename)
                column_count_mismatches.append(basename)
                column_order_mismatches.append(basename)
                dtype_mismatches.append(basename)
                continue

            # row_count
            if local_rows != nas_rows:
                row_count_mismatches.append(basename)

            # column_count
            local_col_count = len(local_schema.names)
            nas_col_count = len(nas_schema.names)
            if local_col_count != self._expected_column_count or nas_col_count != self._expected_column_count:
                column_count_mismatches.append(basename)

            # column_order
            if local_schema.names != self._expected_column_names or nas_schema.names != self._expected_column_names:
                column_order_mismatches.append(basename)

            # dtype (EC-5: pyarrow type-level identity — str(type) includes precision/scale for Decimal)
            if not self._dtype_identity(local_schema, nas_schema):
                dtype_mismatches.append(basename)

        # Aggregate per-invariant results
        per_results["sha256"] = PerInvariantResult(
            invariant_name="sha256",
            status="fail" if sha256_mismatches else "pass",
            measured_local=len(local_files),
            measured_nas=len(nas_objects),
            mismatch_files=sha256_mismatches,
        )
        per_results["row_count"] = PerInvariantResult(
            invariant_name="row_count",
            status="fail" if row_count_mismatches else "pass",
            measured_local=len(local_files),
            measured_nas=len(nas_objects),
            mismatch_files=row_count_mismatches,
        )
        per_results["column_count"] = PerInvariantResult(
            invariant_name="column_count",
            status="fail" if column_count_mismatches else "pass",
            measured_local=self._expected_column_count,
            measured_nas=self._expected_column_count,
            mismatch_files=column_count_mismatches,
        )
        per_results["column_order"] = PerInvariantResult(
            invariant_name="column_order",
            status="fail" if column_order_mismatches else "pass",
            measured_local=list(self._expected_column_names),
            measured_nas=list(self._expected_column_names),
            mismatch_files=column_order_mismatches,
        )
        per_results["dtype"] = PerInvariantResult(
            invariant_name="dtype",
            status="fail" if dtype_mismatches else "pass",
            measured_local="identity",
            measured_nas="identity",
            mismatch_files=dtype_mismatches,
        )

        # ── status 결정 (D6 1종 FAIL → cutover 차단, §6.9 unconditional) ─────
        # Priority: object_count > sha256 > row_count > column_count > column_order > dtype > schema_version
        status: Literal[
            "all_pass", "sha256_fail", "object_count_fail", "row_count_fail",
            "column_count_fail", "column_order_fail", "dtype_fail", "schema_version_fail",
        ]

        if per_results["object_count"].status == "fail":
            status = "object_count_fail"
        elif per_results["sha256"].status == "fail":
            status = "sha256_fail"
        elif per_results["row_count"].status == "fail":
            status = "row_count_fail"
        elif per_results["column_count"].status == "fail":
            status = "column_count_fail"
        elif per_results["column_order"].status == "fail":
            status = "column_order_fail"
        elif per_results["dtype"].status == "fail":
            status = "dtype_fail"
        elif per_results["schema_version"].status == "fail":
            status = "schema_version_fail"
        else:
            status = "all_pass"

        verify_latency_ms = time.monotonic() * 1000 - start_ms

        result = InvariantResult(
            status=status,
            per_invariant_results=per_results,
            local_partition=local_partition,
            nas_partition=effective_nas_partition,
            verify_latency_ms=verify_latency_ms,
        )

        # ── Metrics emit (optional) ────────────────────────────────────────────
        if self._metrics is not None:
            self._metrics.emit_invariant_verify(
                status=status,
                partition=str(effective_nas_partition),
                latency_s=verify_latency_ms / 1000.0,
                per_invariant_results=dict(per_results.items()),
            )

        return result

    # ─── internal helpers ────────────────────────────────────────────────────

    def _check_schema_version(
        self, local_partition: Path, nas_partition: str
    ) -> PerInvariantResult:
        """schema_version pin invariant: local + NAS partition prefix == schema_version=v1."""
        expected_prefix = f"schema_version={self._expected_schema_version}"

        # Check local partition path
        local_str = str(local_partition)
        local_has_sv = expected_prefix in local_str.replace("\\", "/")

        # Check NAS partition
        nas_has_sv = nas_partition.startswith(expected_prefix) or (
            expected_prefix in nas_partition
        )

        passed = local_has_sv and nas_has_sv
        return PerInvariantResult(
            invariant_name="schema_version",
            status="pass" if passed else "fail",
            measured_local=expected_prefix if local_has_sv else "missing",
            measured_nas=expected_prefix if nas_has_sv else "missing",
            mismatch_files=[],
        )

    def _check_object_count(
        self, local_files: list[Path], nas_objects: list[str]
    ) -> PerInvariantResult:
        """object_count invariant: local file count == NAS object count (per partition)."""
        local_count = len(local_files)
        nas_count = len(nas_objects)
        passed = local_count == nas_count

        mismatch: list[str] = []
        if not passed:
            local_names = {f.name for f in local_files}
            nas_names = {k.split("/")[-1] for k in nas_objects}
            mismatch = sorted((local_names | nas_names) - (local_names & nas_names))

        return PerInvariantResult(
            invariant_name="object_count",
            status="pass" if passed else "fail",
            measured_local=local_count,
            measured_nas=nas_count,
            mismatch_files=mismatch,
        )

    def _read_parquet_meta(self, data: bytes) -> tuple[pa.Schema, int]:
        """Read parquet schema and row count from bytes.

        Returns (schema, num_rows).
        EC-5: schema.types include Decimal precision/scale (pa.decimal128(p, s)).
        """
        buf = io.BytesIO(data)
        pf = pq.ParquetFile(buf)
        schema: pa.Schema = pf.schema_arrow
        num_rows: int = pf.metadata.num_rows
        return schema, num_rows

    def _dtype_identity(self, local_schema: pa.Schema, nas_schema: pa.Schema) -> bool:
        """pyarrow type-level identity (EC-5: Decimal precision/scale 포함).

        str(type) 비교: pa.decimal128(38,9) → "decimal128(38, 9)" — precision/scale 모두 포함.
        string comparison 아닌 str(type) 비교 (pa.types.is_decimal() + bit_width 미사용,
        str(type) 이 더 안전하고 comprehensive).
        """
        if local_schema.names != nas_schema.names:
            return False
        for name in local_schema.names:
            try:
                local_type = local_schema.field(name).type
                nas_type = nas_schema.field(name).type
                if str(local_type) != str(nas_type):
                    return False
            except KeyError:
                return False
        return True

    def _normalize_nas_partition(self, nas_partition: str) -> str:
        """EC-4: legacy `node=` 부재 partition → fallback node=DEFAULT (conditional).

        partition_normalization=True 시만 적용.
        ADR-009 §D2.1 박제: node= 부재 legacy = node=DEFAULT.
        MCT-153 backfill 시점 consume — 본 fallback 이 partition prefix normalization 만.
        """
        # If nas_partition already has node= segment, no normalization needed
        if "node=" in nas_partition:
            return nas_partition

        # legacy partition (no node=): do not add node=DEFAULT to prefix
        # (partition_normalization allows verifying without strict node= requirement)
        log.debug(
            "InvariantHarness: partition_normalization=True, nas_partition=%r has no node=. "
            "Using as-is (legacy fallback — MCT-153 backfill pattern).",
            nas_partition,
        )
        return nas_partition
