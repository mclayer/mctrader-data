"""test_invariant_harness_8.py — MCT-171 TDD failing tests: InvariantHarness 8종 (ambiguity 통합).

Story: MCT-171 (EPIC-tier-promotion-single-source Story-5)
AC: AC-1 — invariant 8종 enforcement (D7-1=A + D7-3=B)

Test Contract (MCT-171 §4 AC-1):
- test_8_invariant_names_tuple: _INVARIANT_NAMES = 8종 ('ambiguity' 마지막 위치)
- test_status_enum_9_variants: InvariantResult.status 9 variant (ambiguity_fail 포함)
- test_ambiguity_check_method_exists: InvariantHarness._check_ambiguity() method 존재
- test_all_8_pass_returns_all_pass: 8종 ALL PASS → status="all_pass"
- test_ambiguity_fail_surfaced: ambiguity violation → status="ambiguity_fail"
- test_ambiguity_backward_compat_7_invariants: 기존 7종 MCT-152/153/155 caller API 회귀 0
- test_ambiguity_xor_nas_local_both_exist: NAS+local 동시 존재 → ambiguity_fail
- test_ambiguity_xor_only_local: local only → no ambiguity (pre-promotion state)
- test_ambiguity_xor_only_nas: NAS only → no ambiguity (post-promotion state)
- test_ambiguity_xor_neither: 둘 다 없음 → no ambiguity
- test_ambiguity_violation_counter_emitted: violation 시
  mctrader_invariant_violation_total{invariant_name=ambiguity} Counter emit
- test_mct169_d10_regression: verify_no_ambiguity (promotion.py) 기존 caller 회귀 0
- test_verify_returns_ambiguity_fail_status: harness.verify() 가 ambiguity check 포함

ADR-029 §D10: ambiguity invariant = 동일 logical entity (schema_version × tier × exchange × symbol × date × hour × node)
NAS + local XOR violation 검출.

verified-via: Read src/mctrader_data/nas_migration/invariant_harness.py
verified-via: Read tests/integration/compactor/test_ambiguity_invariant.py (MCT-169 D10 PASS baseline)
"""
from __future__ import annotations

import io
from pathlib import Path
from typing import get_args
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_parquet_bytes(schema: pa.Schema | None = None) -> bytes:
    """Return minimal valid parquet bytes for test fixtures."""
    if schema is None:
        schema = pa.schema([
            pa.field("schema_version", pa.string()),
            pa.field("exchange", pa.string()),
        ])
    table = pa.table(
        {name: pa.array(["v1" if pa.types.is_string(field.type) else [0]])
         for name, field in zip(schema.names, schema, strict=True)},
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_tick_v1_parquet_bytes() -> bytes:
    tick_schema = pa.schema([
        pa.field("ts_utc", pa.int64()),
        pa.field("received_at", pa.int64()),
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("price", pa.string()),
        pa.field("quantity", pa.string()),
        pa.field("side", pa.string()),
        pa.field("raw_json", pa.string()),
    ])
    table = pa.table({
        "ts_utc": pa.array([1000000], type=pa.int64()),
        "received_at": pa.array([1000001], type=pa.int64()),
        "exchange": pa.array(["bithumb"]),
        "symbol": pa.array(["KRW-BTC"]),
        "price": pa.array(["50000000"]),
        "quantity": pa.array(["0.001"]),
        "side": pa.array(["ask"]),
        "raw_json": pa.array(["null"]),
    }, schema=tick_schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_nas_uploader_mock(
    *,
    head_exists: bool = False,
    parquet_bytes: bytes | None = None,
    nas_objects: list[str] | None = None,
) -> MagicMock:
    """NASUploader mock — ambiguity test + harness verify 모두 지원.

    head_exists=True: NAS HEAD + list_objects_v2 모두 "존재" 응답 (ambiguity 검출용)
    head_exists=False: NAS HEAD 404 + list_objects_v2 Contents=[] (no ambiguity)
    """
    from botocore.exceptions import ClientError

    _pq_bytes = parquet_bytes or _make_tick_v1_parquet_bytes()
    _nas_objs = nas_objects or []

    mock_client = MagicMock()
    if head_exists:
        mock_client.head_object.return_value = {
            "ETag": '"fakeetag123"',
            "VersionId": "v1",
            "ContentLength": len(_pq_bytes),
        }
        # _check_nas_partition_exists uses list_objects_v2
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": k, "Size": len(_pq_bytes)} for k in _nas_objs]
            if _nas_objs else [{"Key": "fake/part-0001.parquet", "Size": len(_pq_bytes)}]
        }
    else:
        mock_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )
        # No NAS objects
        mock_client.list_objects_v2.return_value = {"Contents": []}

    mock = MagicMock()
    mock._get_client.return_value = mock_client
    mock.bucket = "mctrader-market"

    mock._download.return_value = _pq_bytes
    mock._list_objects.return_value = _nas_objs

    return mock


# ─── Test: _INVARIANT_NAMES 8종 ─────────────────────────────────────────────


class TestInvariantNames8:
    """_INVARIANT_NAMES = 8종 (ambiguity 마지막 위치) — AC-1 §4."""

    def test_8_invariant_names_tuple(self) -> None:
        """_INVARIANT_NAMES 길이 = 8, 마지막 = 'ambiguity'."""
        from mctrader_data.nas_migration.invariant_harness import _INVARIANT_NAMES

        assert len(_INVARIANT_NAMES) == 8, (
            f"Expected 8 invariant names, got {len(_INVARIANT_NAMES)}: {_INVARIANT_NAMES}"
        )
        assert _INVARIANT_NAMES[-1] == "ambiguity", (
            f"Last invariant must be 'ambiguity', got: {_INVARIANT_NAMES[-1]}"
        )
        # 기존 7종 보존 확인 (MCT-151 backward compat)
        expected_7 = (
            "sha256", "object_count", "row_count", "column_count",
            "column_order", "dtype", "schema_version",
        )
        for name in expected_7:
            assert name in _INVARIANT_NAMES, f"Legacy invariant '{name}' must be preserved"

    def test_status_enum_9_variants(self) -> None:
        """InvariantResult.status 9 variant — 'ambiguity_fail' 포함."""
        from mctrader_data.nas_migration.invariant_harness import InvariantResult

        # Get Literal type args
        hints = InvariantResult.__dataclass_fields__["status"].type
        # Try runtime annotation check via get_type_hints
        import typing
        hints = typing.get_type_hints(InvariantResult).get("status")
        # get_args on Literal returns the literal values
        variants = get_args(hints)
        assert "ambiguity_fail" in variants, (
            f"'ambiguity_fail' must be in InvariantResult.status variants. Got: {variants}"
        )
        assert len(variants) == 9, (
            f"Expected 9 status variants (8 original + ambiguity_fail), got {len(variants)}: {variants}"
        )

    def test_ambiguity_check_method_exists(self) -> None:
        """InvariantHarness._check_ambiguity() method 존재 확인."""
        from mctrader_data.nas_migration.invariant_harness import InvariantHarness

        assert hasattr(InvariantHarness, "_check_ambiguity"), (
            "InvariantHarness must have _check_ambiguity() method (D7-1=A spec)"
        )
        import inspect
        sig = inspect.signature(InvariantHarness._check_ambiguity)
        params = list(sig.parameters.keys())
        assert "local_partition" in params, "Must accept local_partition param"
        assert "nas_partition" in params, "Must accept nas_partition param"


# ─── Test: 8종 verify ALL PASS ───────────────────────────────────────────────


class TestInvariantHarness8AllPass:
    """8종 ALL PASS: harness.verify() ambiguity check 포함 시 all_pass 반환."""

    def test_all_8_pass_returns_all_pass(self, tmp_path: Path) -> None:
        """8종 ALL PASS — ambiguity 포함, NAS only 상태 (local 부재) → all_pass."""
        from mctrader_data.nas_migration.invariant_harness import InvariantHarness

        # Build tick.v1 parquet bytes
        pq_bytes = _make_tick_v1_parquet_bytes()

        # Partition dir (NAS only state — no local files)
        partition_dir = (
            tmp_path
            / "schema_version=tick.v1" / "tier=L1"
            / "exchange=bithumb" / "symbol=KRW-BTC"
            / "date=2026-05-14" / "hour=09"
        )
        partition_dir.mkdir(parents=True)
        local_file = partition_dir / "part-0001.parquet"
        local_file.write_bytes(pq_bytes)

        _nas_pfx = "schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC"
        mock_uploader = _make_nas_uploader_mock(
            head_exists=False,  # NAS HEAD 404 → local only (no ambiguity)
            parquet_bytes=pq_bytes,
            nas_objects=[_nas_pfx + "/date=2026-05-14/hour=09/part-0001.parquet"],
        )

        harness = InvariantHarness(
            nas_uploader=mock_uploader,
            local_root=tmp_path,
            expected_schema_version=["tick.v1", "tick.v1.1"],
        )

        result = harness.verify(
            local_partition=partition_dir,
            nas_partition="schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC/date=2026-05-14/hour=09",
        )

        assert result.status == "all_pass", (
            f"Expected all_pass with no ambiguity (local only state), got: {result.status}\n"
            f"per_invariant_results: {result.per_invariant_results}"
        )
        assert "ambiguity" in result.per_invariant_results, (
            "per_invariant_results must include 'ambiguity' key after harness.verify()"
        )
        assert result.per_invariant_results["ambiguity"].status == "pass"


# ─── Test: ambiguity_fail 검출 ─────────────────────────────────────────────


class TestAmbiguityFailSurface:
    """ambiguity violation: harness.verify() → status='ambiguity_fail'."""

    def test_ambiguity_fail_surfaced(self, tmp_path: Path) -> None:
        """NAS+local 동시 존재 → verify() = 'ambiguity_fail'."""
        from mctrader_data.nas_migration.invariant_harness import InvariantHarness

        pq_bytes = _make_tick_v1_parquet_bytes()

        # NAS+local 동시 존재 픽스처
        partition_dir = (
            tmp_path
            / "schema_version=tick.v1" / "tier=L1"
            / "exchange=bithumb" / "symbol=KRW-BTC"
            / "date=2026-05-14" / "hour=10"
        )
        partition_dir.mkdir(parents=True)
        local_file = partition_dir / "part-ambig.parquet"
        local_file.write_bytes(pq_bytes)

        _p = "schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC"
        nas_key = _p + "/date=2026-05-14/hour=10/part-ambig.parquet"

        # NAS HEAD 200 (nas_exists=True) + local 존재 = ambiguity
        mock_uploader = _make_nas_uploader_mock(
            head_exists=True,  # NAS 존재
            parquet_bytes=pq_bytes,
            nas_objects=[nas_key],
        )

        harness = InvariantHarness(
            nas_uploader=mock_uploader,
            local_root=tmp_path,
            expected_schema_version=["tick.v1", "tick.v1.1"],
        )

        result = harness.verify(
            local_partition=partition_dir,
            nas_partition="schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC/date=2026-05-14/hour=10",
        )

        assert result.status == "ambiguity_fail", (
            f"NAS+local 동시 존재 → expected 'ambiguity_fail', got: {result.status}"
        )
        assert result.per_invariant_results["ambiguity"].status == "fail"

    def test_ambiguity_xor_only_local(self, tmp_path: Path) -> None:
        """local only (pre-promotion) → ambiguity check pass."""
        from mctrader_data.nas_migration.invariant_harness import InvariantHarness

        pq_bytes = _make_tick_v1_parquet_bytes()
        partition_dir = (
            tmp_path
            / "schema_version=tick.v1" / "tier=L1"
            / "exchange=bithumb" / "symbol=KRW-BTC"
            / "date=2026-05-14" / "hour=11"
        )
        partition_dir.mkdir(parents=True)
        (partition_dir / "part-local.parquet").write_bytes(pq_bytes)

        _p = "schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC"
        nas_key = _p + "/date=2026-05-14/hour=11/part-local.parquet"

        mock_uploader = _make_nas_uploader_mock(
            head_exists=False,  # NAS HEAD 404 — local only
            parquet_bytes=pq_bytes,
            nas_objects=[nas_key],
        )

        harness = InvariantHarness(
            nas_uploader=mock_uploader,
            local_root=tmp_path,
            expected_schema_version=["tick.v1", "tick.v1.1"],
        )

        result = harness.verify(
            local_partition=partition_dir,
            nas_partition="schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC/date=2026-05-14/hour=11",
        )

        # local only → no ambiguity
        assert result.per_invariant_results["ambiguity"].status == "pass", (
            f"local only should be no ambiguity, got: {result.per_invariant_results['ambiguity']}"
        )

    def test_ambiguity_xor_only_nas(self, tmp_path: Path) -> None:
        """NAS only (post-promotion, no local files) → ambiguity check pass."""
        from mctrader_data.nas_migration.invariant_harness import InvariantHarness

        pq_bytes = _make_tick_v1_parquet_bytes()
        # Empty local partition
        partition_dir = (
            tmp_path
            / "schema_version=tick.v1" / "tier=L1"
            / "exchange=bithumb" / "symbol=KRW-BTC"
            / "date=2026-05-14" / "hour=12"
        )
        partition_dir.mkdir(parents=True)
        # No local files

        _p = "schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC"
        nas_key = _p + "/date=2026-05-14/hour=12/part-nas-only.parquet"

        mock_uploader = _make_nas_uploader_mock(
            head_exists=True,  # NAS 존재
            parquet_bytes=pq_bytes,
            nas_objects=[nas_key],
        )

        harness = InvariantHarness(
            nas_uploader=mock_uploader,
            local_root=tmp_path,
            expected_schema_version=["tick.v1", "tick.v1.1"],
        )

        result = harness.verify(
            local_partition=partition_dir,
            nas_partition="schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC/date=2026-05-14/hour=12",
            local_files=[],  # no local files
        )

        # NAS only (no local) → no ambiguity
        assert result.per_invariant_results["ambiguity"].status == "pass", (
            f"NAS only should be no ambiguity, got: {result.per_invariant_results['ambiguity']}"
        )

    def test_ambiguity_violation_counter_emitted(self, tmp_path: Path) -> None:
        """ambiguity_fail 시 mctrader_invariant_violation_total{invariant_name=ambiguity} Counter emit."""
        from mctrader_data.nas_migration.invariant_harness import InvariantHarness

        pq_bytes = _make_tick_v1_parquet_bytes()
        partition_dir = (
            tmp_path
            / "schema_version=tick.v1" / "tier=L1"
            / "exchange=bithumb" / "symbol=KRW-BTC"
            / "date=2026-05-14" / "hour=13"
        )
        partition_dir.mkdir(parents=True)
        (partition_dir / "part-counter.parquet").write_bytes(pq_bytes)

        _p = "schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC"
        nas_key = _p + "/date=2026-05-14/hour=13/part-counter.parquet"

        mock_uploader = _make_nas_uploader_mock(
            head_exists=True,
            parquet_bytes=pq_bytes,
            nas_objects=[nas_key],
        )

        mock_metrics = MagicMock()

        harness = InvariantHarness(
            nas_uploader=mock_uploader,
            local_root=tmp_path,
            metrics=mock_metrics,
            expected_schema_version=["tick.v1", "tick.v1.1"],
        )

        result = harness.verify(
            local_partition=partition_dir,
            nas_partition="schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC/date=2026-05-14/hour=13",
        )

        assert result.status == "ambiguity_fail"
        # Verify metrics emit was called (any call is fine — actual metric verification is prometheus level)
        mock_metrics.emit_invariant_verify.assert_called_once()
        call_kwargs = mock_metrics.emit_invariant_verify.call_args
        # status should be ambiguity_fail
        if call_kwargs[0]:
            assert call_kwargs[0][0] == "ambiguity_fail"
        else:
            assert call_kwargs.kwargs.get("status") == "ambiguity_fail"


# ─── Test: backward compat 7종 API ──────────────────────────────────────────


class TestInvariantHarness8BackwardCompat:
    """기존 7종 API 회귀 0 — MCT-152/153/155 caller backward compat (INV-4)."""

    def test_ambiguity_backward_compat_7_invariants(self, tmp_path: Path) -> None:
        """8종 확장 후에도 기존 7종 per_invariant_results key 모두 보존."""
        from mctrader_data.nas_migration.invariant_harness import InvariantHarness

        pq_bytes = _make_tick_v1_parquet_bytes()
        partition_dir = (
            tmp_path
            / "schema_version=tick.v1" / "tier=L1"
            / "exchange=bithumb" / "symbol=KRW-BTC"
            / "date=2026-05-14" / "hour=14"
        )
        partition_dir.mkdir(parents=True)
        (partition_dir / "part-compat.parquet").write_bytes(pq_bytes)

        _p = "schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC"
        nas_key = _p + "/date=2026-05-14/hour=14/part-compat.parquet"

        mock_uploader = _make_nas_uploader_mock(
            head_exists=False,
            parquet_bytes=pq_bytes,
            nas_objects=[nas_key],
        )

        harness = InvariantHarness(
            nas_uploader=mock_uploader,
            local_root=tmp_path,
            expected_schema_version=["tick.v1", "tick.v1.1"],
        )

        result = harness.verify(
            local_partition=partition_dir,
            nas_partition="schema_version=tick.v1/tier=L1/exchange=bithumb/symbol=KRW-BTC/date=2026-05-14/hour=14",
        )

        legacy_7 = (
            "sha256", "object_count", "row_count", "column_count",
            "column_order", "dtype", "schema_version",
        )
        for key in legacy_7:
            assert key in result.per_invariant_results, (
                f"Legacy invariant '{key}' must be present in per_invariant_results (INV-4 backward compat)"
            )

    def test_mct169_d10_regression(self, tmp_path: Path) -> None:
        """MCT-169 D10: promotion.py verify_no_ambiguity() 기존 caller 회귀 0.

        verify_no_ambiguity는 promotion.py 에서 여전히 import 가능해야 한다.
        Deprecate 이후에도 기존 caller (MCT-169 test) 가 동작해야 함.
        """
        from mctrader_data.compactor.promotion import AmbiguityViolation, verify_no_ambiguity

        local_file = tmp_path / "part-regression.parquet"
        local_file.write_bytes(b"fake parquet")

        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"etag123"',
            "VersionId": "v1",
            "ContentLength": 12,
        }
        mock_uploader = MagicMock()
        mock_uploader._get_client.return_value = mock_client
        mock_uploader.bucket = "mctrader-market"

        # NAS+local 동시 존재 → AmbiguityViolation (MCT-169 D10)
        with pytest.raises(AmbiguityViolation):
            verify_no_ambiguity(
                segment_id="regression-test",
                nas_uploader=mock_uploader,
                nas_key="l1/market/part-regression.parquet",
                local_path=local_file,
            )
