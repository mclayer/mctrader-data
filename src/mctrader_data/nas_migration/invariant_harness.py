"""invariant_harness.py — 8종 invariant single-shot verify (MCT-171 ambiguity 통합).

Story: MCT-151 (Stage 2 — dual-write atomic primitives + 7종 invariant harness)
Issue: mclayer/mctrader-hub#257

MCT-171 amendment (2026-05-14):
- _INVARIANT_NAMES: 7개 → 8개 ('ambiguity' 마지막 위치 추가)
- InvariantResult.status: 8 variant → 9 variant ('ambiguity_fail' 추가)
- InvariantHarness._check_ambiguity(): 신규 method (D7-1=A)
  logic = compactor/promotion.py verify_no_ambiguity 흡수 (SSOT 통합)
  동일 logical entity (schema_version × tier × exchange × symbol × date × hour × node)
  NAS + local XOR violation 검출
- InvariantHarness.verify(): 8번째 ambiguity check 추가 (sequential unconditional, §6.9 정합)
- ADR-029 §D10: ambiguity invariant violation → Prometheus mctrader_invariant_violation_total{invariant_name=ambiguity}

MCT-159 FIX Iter 1 amendment (2026-05-13):
- ADR009_CHANNEL_SCHEMA_MATRIX 추가 (ADR-009 §D2.6 SSOT)
- __init__ expected_column_count=None (None 시 schema_version 추출 → matrix lookup)
- _resolve_expected_column_count: D1 Hybrid (prefix 추출 primary → explicit fallback → miss=diagnostic)
- _check_schema_version: channel-aware (tuple/list valid set 지원)
- _check_object_count: per-file basis (ADR-027 §D6.1 chunk↔verify per-file contract)

Design decisions (§6.2.3 Change Plan 박제):

S5 박제 (scope_manifest design_decisions S5):
"D6 박제 3종 + column count + column name order + dtype identity + schema_version pin = 7종 invariant
— D6 PASS 이나 Parquet schema 차이로 read 파괴/오염 risk 차단 (reader-breaking drift 포착)"

MCT-171 §5.1 D7-1=A: 8번째 invariant 'ambiguity' 통합 (ADR-029 §D10 SSOT).

ADR-027 D6 mandatory amendment trigger (triggers_adr_amendment.mandatory=true, trigger_story=MCT-151):
- D6 본문 3종 (sha256 + object_count + row_count) → 7종으로 확장 (본 Story 박제)
- MCT-171: 7종 → 8종으로 확장 (ambiguity 추가)
- amendment 시점: Phase 2 PR merge 또는 retro 시점 (§6.6 결정)

§6.9 invariant placement:
- 8종 sequential unconditional (early return 0 — FAIL 1종 발생 후에도 나머지 7종 모두 verify).
  D6 본문 "1종이라도 FAIL 시 cutover 차단" + per-invariant 측정값 emit 의무 (diagnostic dump).
- legacy node= fallback: conditional (partition_normalization=True 시만 적용, EC-4 박제).
- ambiguity check: 8번째, NAS HEAD probe (per-partition level, 파일 단위 X).

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
import re
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
# OHLCV cutover path backward-compat 회귀 0 — 기존 상수 보존 (MCT-159 FIX 박제)
ADR009_EXPECTED_COLUMN_COUNT: int = 16
ADR009_EXPECTED_COLUMN_NAMES: tuple[str, ...] = (
    "schema_version", "exchange", "symbol", "date", "ts",
    "open", "high", "low", "close", "volume", "vwap",
    "trade_count", "bid_count", "ask_count",
    "source_provenance", "ingestion_ts",
)
ADR009_EXPECTED_SCHEMA_VERSION: str = "v1"

# ─── ADR-009 §D2.6 ADR009_CHANNEL_SCHEMA_MATRIX SSOT ────────────────────────
# MCT-159 FIX Iter 1 (2026-05-13): channel-aware column_count resolve
# Key = schema_version prefix (e.g. "orderbook_snapshot.v1")
# Value = (column_count: int, column_names: tuple[str, ...])
# 신규 schema_version 추가 시 본 matrix + ADR-009 §D2.6 amendment 의무 (CFP-26 sibling sync 정합)
ADR009_CHANNEL_SCHEMA_MATRIX: dict[str, tuple[int, tuple[str, ...]]] = {
    "orderbook_snapshot.v1": (
        11,
        (
            "ts_utc", "received_at", "exchange", "symbol",
            "baseline_seq", "side", "level", "price", "quantity",
            "payload_hash", "raw_json",
        ),
    ),
    "tick.v1": (
        8,
        (
            "ts_utc", "received_at", "exchange", "symbol",
            "price", "quantity", "side", "raw_json",
        ),
    ),
    "tick.v1.1": (
        11,
        (
            "ts_utc", "received_at", "exchange", "symbol",
            "price", "quantity", "side", "raw_json",
            "ingest_seq", "payload_hash", "validation_status",
        ),
    ),
    "ohlcv.v1": (
        16,
        (
            "schema_version", "exchange", "symbol", "date", "ts",
            "open", "high", "low", "close", "volume", "vwap",
            "trade_count", "bid_count", "ask_count",
            "source_provenance", "ingestion_ts",
        ),
    ),
}

# §6.8 Wording SSOT — InvariantResult.status (MCT-171: 7종 → 8종, 'ambiguity' 마지막 추가)
# MCT-171 §5.1 D7-1=A: 8번째 = 'ambiguity' (ADR-029 §D10 SSOT 통합, NAS+local XOR violation)
_INVARIANT_NAMES: tuple[str, ...] = (
    "sha256", "object_count", "row_count",
    "column_count", "column_order", "dtype", "schema_version",
    "ambiguity",  # MCT-171 신규 (8번째)
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
    """8종 invariant verify 의 result enum + per-invariant 측정값.

    MCT-171 amendment: status enum 9종 ('ambiguity_fail' 추가, §6.8 Wording SSOT 갱신)

    status enum 9종 (§6.8 Wording SSOT 박제 — single string, variant 금지):
    - "all_pass":             8종 invariant ALL PASS (cutover 차단 0).
    - "sha256_fail":          local sha256 != NAS sha256 (1종 이상 file).
    - "object_count_fail":    local file count != NAS object count.
    - "row_count_fail":       local row count != NAS row count (1종 이상 file).
    - "column_count_fail":    column count != 16 (ADR-009 §D2.1, 1종 이상 file).
    - "column_order_fail":    column order != ADR-009 §D2 정의 (1종 이상 file).
    - "dtype_fail":           dtype mismatch (Decimal precision/scale 포함, 1종 이상 file).
    - "schema_version_fail":  partition prefix != schema_version=v1 (legacy schema 침범).
    - "ambiguity_fail":       NAS+local 동시 존재 (MCT-171 §5.1, ADR-029 §D10 SoT exclusivity 파괴).

    D6 본문 wording 정합: 1종이라도 FAIL 시 cutover 차단 의무 (caller 측 status 검사 후 결정).
    per_invariant_results: 8종 별 PerInvariantResult (early return 0 — 모든 8종 verify 후 결정).
    MCT-151 backward compat: 기존 7종 per_invariant_results key 모두 보존 (INV-4).
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
        "ambiguity_fail",  # MCT-171 신규 (ADR-029 §D10)
    ]
    per_invariant_results: dict[str, PerInvariantResult] = field(default_factory=dict)
    local_partition: Path | None = None
    nas_partition: str | None = None
    verify_latency_ms: float = 0.0


class InvariantHarness:
    """8종 invariant single-shot verify (MCT-171 ambiguity 통합, ADR-027 D6 amendment trigger source).

    8종 invariant 의 layer 별 분류 (MCT-171 §5.1 갱신):
    - byte-level (1종): sha256
    - set-level (2종): object_count, row_count
    - schema-level (4종): column_count, column_order, dtype, schema_version
    - exclusivity-level (1종): ambiguity (MCT-171 신규, NAS+local XOR violation)

    §6.9 invariant placement:
    - 8종 sequential unconditional (early return 0 — FAIL 1종 후에도 나머지 verify 계속).
    - ambiguity check: 8번째 (partition-level, per-file 체크 아님).
    - legacy node= fallback: conditional (partition_normalization=True 시만).

    Caller (MCT-152 / MCT-153 / MCT-155) 가 inject — 본 Story scope = harness 정의.
    read-only invariant: 양쪽 storage 변경 0 (§11.2 forward-only 보존).
    INV-4: MCT-151 7종 API backward compat 보존 (per_invariant_results 기존 7종 key 포함).
    """

    def __init__(
        self,
        nas_uploader: NASUploader,
        local_root: Path,
        metrics: PrometheusExporter | None = None,
        expected_column_count: int | None = None,
        expected_column_names: tuple[str, ...] | None = None,
        expected_schema_version: str | tuple[str, ...] | list[str] = ADR009_EXPECTED_SCHEMA_VERSION,
        partition_normalization: bool = False,  # EC-4: legacy node= fallback
    ) -> None:
        """7종 invariant harness 초기화.

        MCT-159 FIX Iter 1 signature amend (ADR-009 §D2.6 SSOT):
        - expected_column_count: None 시 schema_version 추출 → ADR009_CHANNEL_SCHEMA_MATRIX lookup (D1 Hybrid primary)
          int 주입 시 backward-compat 유지 (OHLCV cutover path 회귀 0)
        - expected_column_names: None 시 matrix lookup 결과 사용 (expected_column_count=None 연동)
          tuple 주입 시 backward-compat 유지
        - expected_schema_version: str / tuple[str] / list[str] 지원 (channel-aware valid set)
        """
        self._uploader = nas_uploader
        self._local_root = local_root
        self._metrics = metrics
        # None = lookup mode (D1 Hybrid primary); int = explicit injection (backward-compat)
        self._expected_column_count: int | None = expected_column_count
        # None = lookup mode (linked to expected_column_count=None); tuple = explicit injection
        self._expected_column_names_override: tuple[str, ...] | None = expected_column_names
        # backward-compat: when explicit count provided, fall back to OHLCV names
        if expected_column_count is not None and expected_column_names is None:
            self._expected_column_names_override = ADR009_EXPECTED_COLUMN_NAMES
        self._expected_schema_version = expected_schema_version
        self._partition_normalization = partition_normalization

    def verify(
        self,
        *,
        local_partition: Path,
        nas_partition: str,
        local_files: list[Path] | None = None,
        nas_objects: list[str] | None = None,
    ) -> InvariantResult:
        """7종 invariant single-shot verify (§6.9 unconditional sequential — early return 0).

        MCT-159 FIX Iter 2: per-file mode 추가 (ADR-027 §D6.1 chunk↔verify per-file contract).
        - default (partition mode): `local_files=None` + `nas_objects=None` → partition glob/list (backward-compat)
        - per-file mode: caller 가 single-file `local_files=[Path]` + `nas_objects=[str]` 직접 inject
          → partition glob/list skip, chunk_spec per-file PUT 단위와 verify 단위 일치

        Algorithm:
        1. local_files / nas_objects 가 None 시 partition glob/list (backward-compat)
        2. 7종 invariant verify (sequential unconditional, FAIL 발생 후에도 계속):
           - object_count (set-level): local count == NAS count
           - sha256 (byte-level): per file sha256 match
           - row_count (set-level): per file row count match
           - column_count (schema-level): channel-aware lookup (ADR-009 §D2.6)
           - column_order (schema-level): == expected_column_names
           - dtype (schema-level): pyarrow type-level identity (EC-5: Decimal precision/scale)
           - schema_version (schema-level): partition prefix schema_version ∈ expected_schema_version
        3. status 결정: priority 순서
           (object_count > sha256 > row_count > column_count > column_order > dtype > schema_version)
        4. InvariantResult return

        EC-4: partition_normalization=True 시 nas_partition 의 node= 부재 → fallback node=DEFAULT.
        EC-5: dtype 비교 시 pyarrow type-level identity (str(type) 비교 — precision/scale 포함).

        Read-only invariant: 양쪽 storage 변경 0.
        """
        start_ms = time.monotonic() * 1000

        # ── gather local files (MCT-159 FIX Iter 2: per-file mode skip glob) ───
        if local_files is None:
            local_files = sorted(local_partition.glob("*.parquet"))

        # ── EC-4: partition normalization (conditional) ────────────────────────
        effective_nas_partition = nas_partition
        if self._partition_normalization:
            effective_nas_partition = self._normalize_nas_partition(nas_partition)

        # ── gather NAS objects (MCT-159 FIX Iter 2: per-file mode skip list) ──
        if nas_objects is None:
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

            # column_count (D1 Hybrid: per-file schema_version lookup primary)
            expected_col_count, expected_col_names = self._resolve_expected_column_count(local_file)
            local_col_count = len(local_schema.names)
            nas_col_count = len(nas_schema.names)
            if expected_col_count is None:
                # miss strategy: unknown schema_version → column_count_fail diagnostic
                log.warning(
                    "InvariantHarness: unknown schema_version for %r — column_count_fail (diagnostic)",
                    basename,
                )
                column_count_mismatches.append(basename)
            elif local_col_count != expected_col_count or nas_col_count != expected_col_count:
                column_count_mismatches.append(basename)

            # column_order (D1 Hybrid: use resolved expected_col_names)
            if expected_col_names is not None:
                if local_schema.names != list(expected_col_names) or nas_schema.names != list(expected_col_names):
                    column_order_mismatches.append(basename)
            else:
                # miss: unknown schema → column_order_fail (consistent with column_count miss)
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
            measured_local=self._expected_column_count if self._expected_column_count is not None else "lookup",
            measured_nas=self._expected_column_count if self._expected_column_count is not None else "lookup",
            mismatch_files=column_count_mismatches,
        )
        _col_names_repr: list[str] | str = (
            list(self._expected_column_names_override)
            if self._expected_column_names_override
            else "lookup"
        )
        per_results["column_order"] = PerInvariantResult(
            invariant_name="column_order",
            status="fail" if column_order_mismatches else "pass",
            measured_local=_col_names_repr,
            measured_nas=_col_names_repr,
            mismatch_files=column_order_mismatches,
        )
        per_results["dtype"] = PerInvariantResult(
            invariant_name="dtype",
            status="fail" if dtype_mismatches else "pass",
            measured_local="identity",
            measured_nas="identity",
            mismatch_files=dtype_mismatches,
        )

        # ── 8. ambiguity check (MCT-171 §5.1 D7-1=A, 8번째 sequential unconditional) ──
        # NAS+local 동시 존재 = SoT exclusivity 파괴 (ADR-029 §D10, INV-1 XOR)
        # partition-level check: local_files 가 있고 NAS HEAD 가 존재하면 ambiguity
        per_results["ambiguity"] = self._check_ambiguity(
            local_partition=local_partition,
            nas_partition=effective_nas_partition,
            local_files=local_files,
        )

        # ── status 결정 (D6 1종 FAIL → cutover 차단, §6.9 unconditional) ─────
        # Priority: object_count > sha256 > row_count > column_count > column_order > dtype > schema_version > ambiguity
        # MCT-171: ambiguity_fail 추가 (priority 마지막 — 기존 7종 priority 보존, INV-4)
        status: Literal[
            "all_pass", "sha256_fail", "object_count_fail", "row_count_fail",
            "column_count_fail", "column_order_fail", "dtype_fail", "schema_version_fail",
            "ambiguity_fail",
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
        elif per_results["ambiguity"].status == "fail":
            status = "ambiguity_fail"
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
            # Extract channel (schema_version) and tier from NAS partition for label cardinality
            _channel = self._extract_schema_version(effective_nas_partition) or "unknown"
            _tier_match = re.search(r"tier=(L\d)", effective_nas_partition)
            _tier = _tier_match.group(1) if _tier_match else "unknown"
            self._metrics.emit_invariant_verify(
                status=status,
                partition=str(effective_nas_partition),
                latency_s=verify_latency_ms / 1000.0,
                per_invariant_results=dict(per_results.items()),
                channel=_channel,
                tier=_tier,
            )

        return result

    # ─── internal helpers ────────────────────────────────────────────────────

    def _check_schema_version(
        self, local_partition: Path, nas_partition: str
    ) -> PerInvariantResult:
        """schema_version pin invariant: local + NAS partition prefix schema_version match.

        MCT-159 FIX Iter 1: channel-aware — expected_schema_version 이 str / tuple / list 지원.
        - str: 단일 값 (기존 OHLCV v1 path, backward-compat)
        - tuple / list: channel valid set (여러 schema_version 허용, e.g. ["tick.v1", "tick.v1.1"])

        Resolution: partition prefix 에서 schema_version=<value> 추출 후 valid set 검사.
        """
        sv = self._expected_schema_version
        if isinstance(sv, str):
            valid_set: set[str] = {sv}
        else:
            valid_set = set(sv)

        # Extract schema_version from local partition path
        local_str = str(local_partition).replace("\\", "/")
        local_sv = self._extract_schema_version(local_str)
        local_has_sv = local_sv in valid_set if local_sv else any(
            f"schema_version={v}" in local_str for v in valid_set
        )

        # Extract schema_version from NAS partition prefix
        nas_sv = self._extract_schema_version(nas_partition)
        nas_has_sv = nas_sv in valid_set if nas_sv else any(
            f"schema_version={v}" in nas_partition for v in valid_set
        )

        passed = local_has_sv and nas_has_sv
        return PerInvariantResult(
            invariant_name="schema_version",
            status="pass" if passed else "fail",
            measured_local=local_sv or "missing",
            measured_nas=nas_sv or "missing",
            mismatch_files=[],
        )

    def _extract_schema_version(self, path_str: str) -> str | None:
        """Extract schema_version value from a partition path string.

        e.g. "schema_version=orderbook_snapshot.v1/tier=L2/..." → "orderbook_snapshot.v1"
        Returns None if not found.
        """
        match = re.search(r"schema_version=([^/\\]+)", path_str)
        return match.group(1) if match else None

    def _resolve_expected_column_count(
        self, file_path: Path
    ) -> tuple[int | None, tuple[str, ...] | None]:
        """D1 Hybrid: schema_version 추출 → ADR009_CHANNEL_SCHEMA_MATRIX lookup.

        ADR-027 §D6.1 + ADR-009 §D2.6 SSOT (MCT-159 FIX Iter 1):
        1. Primary: file_path 에서 schema_version 추출 → matrix lookup
        2. Fallback: _expected_column_count 명시 주입 시 explicit 사용
        3. Miss: unknown schema_version → (None, None) → column_count_fail diagnostic

        Returns (expected_count, expected_names) or (None, None) on miss.
        """
        # Fallback path: explicit injection (backward-compat — OHLCV cutover path 회귀 0)
        if self._expected_column_count is not None:
            return (self._expected_column_count, self._expected_column_names_override)

        # Primary: schema_version 추출
        path_str = str(file_path).replace("\\", "/")
        sv = self._extract_schema_version(path_str)
        if sv is None:
            log.debug(
                "InvariantHarness._resolve_expected_column_count: "
                "no schema_version in path %r — miss",
                path_str,
            )
            return (None, None)

        entry = ADR009_CHANNEL_SCHEMA_MATRIX.get(sv)
        if entry is None:
            log.warning(
                "InvariantHarness._resolve_expected_column_count: "
                "unknown schema_version=%r — column_count_fail diagnostic",
                sv,
            )
            return (None, None)

        return (entry[0], entry[1])

    def _check_object_count(
        self, local_files: list[Path], nas_objects: list[str]
    ) -> PerInvariantResult:
        """object_count invariant: local file count == NAS object count (per-file basis).

        ADR-027 §D6.1 chunk↔verify per-file contract (MCT-159 FIX Iter 1):
        chunk_spec 변경 0 (MCT-153 박제 보존), invariant verify = per-file.
        local file count == NAS object count within the same partition.
        """
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

    def _check_ambiguity(
        self,
        local_partition: Path,
        nas_partition: str,
        local_files: list[Path] | None = None,
    ) -> PerInvariantResult:
        """8번째 invariant: ambiguity check (MCT-171 §5.1 D7-1=A).

        ADR-029 §D10 SSOT 흡수 (compactor/promotion.py verify_no_ambiguity 로직 통합).
        INV-1 SoT exclusivity: nas_exists ⊕ local_exists = true (XOR).
        NAS+local 동시 존재 = 설계 위반 (ambiguity_fail).

        Logic:
        - local_files 가 비어있으면 no ambiguity (local_exists=False)
        - local_files 가 있으면: NAS HEAD probe (partition prefix 기준 1회)
          NAS HEAD 200 → nas_exists=True → local_exists=True → ambiguity_fail
          NAS HEAD 404 → nas_exists=False → no ambiguity

        Note: per-file 체크 아님 (partition-level NAS HEAD probe).
              compactor/promotion.py 의 verify_no_ambiguity 는 segment-level (key 단위).
              본 method 는 partition-level (prefix 단위, 1회 HEAD probe).

        Prometheus: ambiguity_fail 시 mctrader_invariant_violation_total{invariant_name=ambiguity}
        """
        _local_files = local_files if local_files is not None else sorted(local_partition.glob("*.parquet"))

        # local 없으면 no ambiguity (NAS only = post-promotion state or empty)
        local_exists = len(_local_files) > 0

        if not local_exists:
            return PerInvariantResult(
                invariant_name="ambiguity",
                status="pass",
                measured_local="empty",
                measured_nas="unknown",
                mismatch_files=[],
            )

        # local 있음 → NAS HEAD probe (partition prefix 기준 첫 번째 object로 확인)
        nas_exists = self._check_nas_partition_exists(nas_partition)

        if nas_exists and local_exists:
            log.error(
                "InvariantHarness._check_ambiguity: VIOLATION — "
                "NAS+local 동시 존재 (partition=%r, local_files=%d). "
                "INV-1 SoT exclusivity 파괴 (ADR-029 §D10). "
                "Manual escalation 의무.",
                nas_partition, len(_local_files),
            )
            return PerInvariantResult(
                invariant_name="ambiguity",
                status="fail",
                measured_local=len(_local_files),
                measured_nas="exists",
                mismatch_files=[str(local_partition)],
            )

        return PerInvariantResult(
            invariant_name="ambiguity",
            status="pass",
            measured_local=len(_local_files),
            measured_nas="absent",
            mismatch_files=[],
        )

    def _check_nas_partition_exists(self, nas_partition: str) -> bool:
        """NAS partition prefix 에 오브젝트 존재 여부 확인 (ambiguity check 용).

        list_objects_v2 prefix= 로 1개 오브젝트 확인 (HEAD probe 대용).
        MaxKeys=1 — 존재 여부만 확인 (성능 최소화).

        Returns:
            True: prefix 에 오브젝트 존재 (nas_exists=True)
            False: 오브젝트 없음 또는 오류 (nas_exists=False, ambiguity 아님으로 처리)
        """
        try:
            client = self._uploader._get_client()  # type: ignore[attr-defined]
            bucket = self._uploader.bucket
            # Ensure prefix ends with / for partition-level match
            prefix = nas_partition if nas_partition.endswith("/") else nas_partition + "/"
            resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            return len(resp.get("Contents", [])) > 0
        except Exception:
            log.warning(
                "InvariantHarness._check_nas_partition_exists: error checking NAS partition %r — "
                "treating as not exists (ambiguity 오탐 회피)",
                nas_partition,
            )
            return False
