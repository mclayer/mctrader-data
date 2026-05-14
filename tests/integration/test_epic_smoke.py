# tests/integration/test_epic_smoke.py
"""MCT-172 TDD failing tests: 8 invariant cross-Story integration smoke.

Story: MCT-172 (EPIC-tier-promotion-single-source Story-6)
AC: AC-1 — 8 invariant cross-Story integration smoke (D8-1=A)
AC: AC-2 — ambiguity invariant synthetic baseline (D8-2=C)
AC: AC-4 — promotion.py verify_no_ambiguity 제거 (D8-5=A)

Test Contract (MCT-172 §4 AC-1/2/4):
- test_invariant_harness_8_ssot_verify_no_ambiguity_absent: promotion.py 에서
  verify_no_ambiguity 가 제거됐는지 확인 (D8-5=A, INV-1 SSOT 통합 완료 게이트)
- test_invariant_harness_verify_8_all_pass: InvariantHarness.verify() → 8 invariant
  ALL PASS (empty local + empty NAS partition = no files = all_pass 기대)
- test_invariant_harness_8_per_invariant_keys: per_invariant_results 에 8 invariant
  key 모두 포함 (sha256/object_count/row_count/column_count/column_order/dtype/
  schema_version/ambiguity)
- test_ambiguity_invariant_all_pass_no_local: local files 없으면 ambiguity = pass
- test_ambiguity_invariant_violation_via_harness: NAS+local 동시 존재 → harness.verify()
  = ambiguity_fail (AC-1, D8-1=A, AC-2)
- test_promotion_public_api_no_verify_no_ambiguity: promotion 모듈 공개 API 에
  verify_no_ambiguity 심볼 부재 확인 (D8-5=A, grep 0건 code-level gate)
- test_mct_152_153_155_169_171_caller_regression: 기존 caller 스택 (MCT-152/153/155/
  169/171) InvariantHarness.verify() API 회귀 0 (per_invariant_results 7종 key 보존)

D8-1=A: InvariantHarness 8종 SSOT (MCT-171 LAND) 의 verify() 호출 → 8 PerInvariantResult.
D8-5=A: promotion.py verify_no_ambiguity 즉시 제거 — 본 test 가 TDD red gate.

TDD red phase 설계:
  test_invariant_harness_8_ssot_verify_no_ambiguity_absent / test_promotion_public_api_no_verify_no_ambiguity
  는 현재 FAIL (verify_no_ambiguity 아직 promotion.py 에 잔존).
  DeveloperAgent (Task 2) 가 cleanup 후 PASS 로 전환.

verified-via: Read src/mctrader_data/compactor/promotion.py
verified-via: Read src/mctrader_data/nas_migration/invariant_harness.py
verified-via: Read tests/integration/test_invariant_harness_8.py
"""
from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq


# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_minimal_parquet_bytes() -> bytes:
    """Return minimal valid Parquet bytes for harness fixture."""
    schema = pa.schema([
        pa.field("schema_version", pa.string()),
        pa.field("exchange", pa.string()),
    ])
    table = pa.table({"schema_version": pa.array(["v1"]), "exchange": pa.array(["upbit"])}, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_invariant_harness_mock_all_pass(tmp_path: Path):
    """Return InvariantHarness with mocked NASUploader that returns empty NAS (no ambiguity).

    NAS partition: empty → object_count=0, ambiguity=pass (local also empty).
    Uses schema_version=ohlcv.v1 in partition paths to satisfy schema_version invariant.
    """
    from mctrader_data.nas_migration.invariant_harness import InvariantHarness

    mock_client = MagicMock()
    # _list_objects returns empty list (no NAS objects)
    mock_uploader = MagicMock()
    mock_uploader._list_objects.return_value = []
    # _check_nas_partition_exists: no objects → False
    mock_client.list_objects_v2.return_value = {"Contents": []}
    mock_uploader._get_client.return_value = mock_client
    mock_uploader.bucket = "mctrader-market"

    harness = InvariantHarness(
        nas_uploader=mock_uploader,
        local_root=tmp_path,
        expected_column_count=None,
        expected_schema_version=("ohlcv.v1", "v1"),  # allow both forms
    )
    return harness, mock_uploader


# NAS/local partition key includes schema_version= segment to pass schema_version invariant
_NAS_PARTITION_ALL_PASS = "schema_version=ohlcv.v1/tier=L1/"
_NAS_PARTITION_AMBIGUITY = "schema_version=ohlcv.v1/tier=L1"
_LOCAL_SUBDIR_ALL_PASS = "schema_version=ohlcv.v1/tier=L1"


def _make_invariant_harness_mock_ambiguity(tmp_path: Path):
    """Return InvariantHarness with mocked NASUploader that simulates NAS+local 동시 존재."""
    from mctrader_data.nas_migration.invariant_harness import InvariantHarness

    mock_client = MagicMock()
    # _check_nas_partition_exists: NAS has objects → True (ambiguity)
    mock_client.list_objects_v2.return_value = {
        "Contents": [{"Key": "schema_version=ohlcv.v1/tier=L1/part-001.parquet"}]
    }
    mock_uploader = MagicMock()
    mock_uploader._list_objects.return_value = ["schema_version=ohlcv.v1/tier=L1/part-001.parquet"]
    mock_uploader._download.return_value = _make_minimal_parquet_bytes()
    mock_uploader._get_client.return_value = mock_client
    mock_uploader.bucket = "mctrader-market"

    harness = InvariantHarness(
        nas_uploader=mock_uploader,
        local_root=tmp_path,
        expected_column_count=None,
        expected_schema_version=("ohlcv.v1", "v1"),
    )
    return harness, mock_uploader


# ─── AC-4: D8-5=A TDD red gate — verify_no_ambiguity 제거 확인 ────────────────


class TestVerifyNoAmbiguityRemoval:
    """D8-5=A: promotion.py verify_no_ambiguity 제거 gate (TDD red phase until Task 2 PASS)."""

    def test_invariant_harness_8_ssot_verify_no_ambiguity_absent(self) -> None:
        """verify_no_ambiguity 가 promotion 모듈에서 제거됐는지 확인 (INV-1 SSOT 통합 완료).

        TDD red gate: 현재 FAIL (verify_no_ambiguity 아직 promotion.py 에 존재).
        DeveloperAgent cleanup 후 PASS.

        D8-5=A: InvariantHarness._check_ambiguity() = SSOT. promotion.py 측 동명 함수 제거.
        """
        import mctrader_data.compactor.promotion as promotion_mod

        # verify_no_ambiguity 는 promotion 모듈 공개 심볼에 없어야 한다
        public_symbols = [s for s in dir(promotion_mod) if not s.startswith("_")]
        assert "verify_no_ambiguity" not in public_symbols, (
            "D8-5=A violation: verify_no_ambiguity 가 promotion.py 에 아직 존재. "
            "DeveloperAgent Task 2 (cleanup) 완료 후 이 test 는 PASS 여야 한다. "
            f"현재 public symbols: {[s for s in public_symbols if 'ambig' in s.lower() or 'verify' in s.lower()]}"
        )

    def test_promotion_public_api_no_verify_no_ambiguity(self) -> None:
        """promotion 모듈 에서 verify_no_ambiguity 심볼이 없음을 확인 (D8-5=A, grep 0건 gate).

        TDD red gate: 현재 FAIL (verify_no_ambiguity 아직 존재).
        DeveloperAgent cleanup 후 PASS.
        """
        import mctrader_data.compactor.promotion as promotion_mod

        # hasattr 로 직접 확인 — cleanup 전에는 True (test FAIL), 후에는 False (test PASS)
        has_symbol = hasattr(promotion_mod, "verify_no_ambiguity")
        assert not has_symbol, (
            "D8-5=A gate: promotion.py 에 verify_no_ambiguity 심볼 여전히 존재. "
            "DeveloperAgent Task 2 cleanup 완료 후 이 test 는 PASS 여야 한다."
        )


# ─── AC-1: 8 invariant cross-Story integration smoke ────────────────────────


class TestInvariantHarness8Smoke:
    """D8-1=A: InvariantHarness 8종 verify() 호출 → ALL PASS (cross-Story integration smoke)."""

    def test_invariant_harness_verify_8_all_pass(self, tmp_path: Path) -> None:
        """InvariantHarness.verify() empty partition → all_pass (8 invariant 포함).

        local 파일 없음 + NAS empty → object_count=0 both, ambiguity=pass (no local).
        D8-1=A: 8종 ALL PASS = cross-Story E2E smoke gate.
        partition path 에 schema_version= 포함해야 schema_version invariant PASS.
        """
        from mctrader_data.nas_migration.invariant_harness import InvariantResult

        # schema_version= segment 포함 (schema_version invariant pass 조건)
        partition_dir = tmp_path / _LOCAL_SUBDIR_ALL_PASS
        partition_dir.mkdir(parents=True)

        harness, _ = _make_invariant_harness_mock_all_pass(tmp_path)

        result = harness.verify(
            local_partition=partition_dir,
            nas_partition=_NAS_PARTITION_ALL_PASS,
        )

        assert isinstance(result, InvariantResult)
        # empty-empty는 object_count: both 0 → pass, ambiguity: no local → pass
        assert result.status == "all_pass", (
            f"8 invariant ALL PASS 기대. status={result.status!r}, "
            f"per_invariant_results fail keys: "
            f"{[k for k, v in result.per_invariant_results.items() if v.status == 'fail']}"
        )

    def test_invariant_harness_8_per_invariant_keys(self, tmp_path: Path) -> None:
        """per_invariant_results 에 8 invariant key 모두 포함 (MCT-171 LAND verify).

        Keys: sha256 / object_count / row_count / column_count / column_order /
              dtype / schema_version / ambiguity (8종).
        """
        partition_dir = tmp_path / _LOCAL_SUBDIR_ALL_PASS
        partition_dir.mkdir(parents=True)

        harness, _ = _make_invariant_harness_mock_all_pass(tmp_path)
        result = harness.verify(
            local_partition=partition_dir,
            nas_partition=_NAS_PARTITION_ALL_PASS,
        )

        expected_keys = {
            "sha256", "object_count", "row_count",
            "column_count", "column_order", "dtype", "schema_version",
            "ambiguity",
        }
        actual_keys = set(result.per_invariant_results.keys())
        missing = expected_keys - actual_keys
        assert not missing, (
            f"per_invariant_results 에 누락된 invariant key: {missing}. "
            "MCT-171 LAND 8종 SSOT 정합 필요."
        )

    def test_ambiguity_invariant_all_pass_no_local(self, tmp_path: Path) -> None:
        """local files 없으면 ambiguity invariant = pass (post-promotion SoT state).

        INV-1 XOR: local_exists=False → ambiguity 없음 (NAS only = valid).
        """
        partition_dir = tmp_path / _LOCAL_SUBDIR_ALL_PASS
        partition_dir.mkdir(parents=True)
        # local 파일 없음 (empty dir)

        harness, _ = _make_invariant_harness_mock_all_pass(tmp_path)
        result = harness.verify(
            local_partition=partition_dir,
            nas_partition=_NAS_PARTITION_ALL_PASS,
        )

        ambiguity_result = result.per_invariant_results.get("ambiguity")
        assert ambiguity_result is not None, "ambiguity key가 per_invariant_results에 없음"
        assert ambiguity_result.status == "pass", (
            f"local 없음 → ambiguity pass 기대. status={ambiguity_result.status!r}"
        )

    def test_ambiguity_invariant_violation_via_harness(self, tmp_path: Path) -> None:
        """NAS+local 동시 존재 → harness.verify() = ambiguity_fail (AC-1, AC-2, D8-1=A).

        InvariantHarness._check_ambiguity() SSOT 동작 확인 (MCT-171 통합).
        ambiguity_fail 은 8번째 priority — 앞 7종 PASS 시 ambiguity_fail 반영.
        """
        from mctrader_data.nas_migration.invariant_harness import InvariantResult

        partition_dir = tmp_path / _LOCAL_SUBDIR_ALL_PASS
        partition_dir.mkdir(parents=True)

        # local file 생성 (local_exists=True)
        local_file = partition_dir / "part-001.parquet"
        local_file.write_bytes(_make_minimal_parquet_bytes())

        harness, _ = _make_invariant_harness_mock_ambiguity(tmp_path)
        result = harness.verify(
            local_partition=partition_dir,
            nas_partition=_NAS_PARTITION_AMBIGUITY,
        )

        assert isinstance(result, InvariantResult)
        ambiguity_result = result.per_invariant_results.get("ambiguity")
        assert ambiguity_result is not None, "ambiguity key 없음"
        assert ambiguity_result.status == "fail", (
            f"NAS+local 동시 존재 → ambiguity fail 기대. status={ambiguity_result.status!r}"
        )


# ─── Backward compat regression: MCT-152/153/155/169/171 caller API 회귀 0 ───


class TestCallerAPIRegression:
    """MCT-152/153/155/169/171 caller 측 InvariantHarness.verify() API 회귀 0."""

    def test_mct_152_153_155_169_171_caller_regression(self, tmp_path: Path) -> None:
        """기존 caller (MCT-152/153/155/169/171) API — per_invariant_results 7종 key 보존.

        INV-4 backward compat: 기존 7종 key (sha256/object_count/row_count/column_count/
        column_order/dtype/schema_version) 모두 per_invariant_results 에 존재.
        """
        partition_dir = tmp_path / _LOCAL_SUBDIR_ALL_PASS
        partition_dir.mkdir(parents=True)

        harness, _ = _make_invariant_harness_mock_all_pass(tmp_path)
        result = harness.verify(
            local_partition=partition_dir,
            nas_partition=_NAS_PARTITION_ALL_PASS,
        )

        # MCT-151 7종 backward compat keys
        legacy_keys = {
            "sha256", "object_count", "row_count",
            "column_count", "column_order", "dtype", "schema_version",
        }
        actual_keys = set(result.per_invariant_results.keys())
        missing = legacy_keys - actual_keys
        assert not missing, (
            f"MCT-151 7종 backward compat key 누락: {missing}. INV-4 위반."
        )

    def test_invariant_harness_promote_l1_api_still_works(self, tmp_path: Path) -> None:
        """promote_l1() API (MCT-169 LAND) — cleanup 후에도 동작 (caller 회귀 0).

        promote_l1 은 cleanup 대상이 아님 — verify_no_ambiguity 만 제거 대상.
        """
        from mctrader_data.compactor.promotion import promote_l1, PromotionResult

        local_file = tmp_path / "part-regression.parquet"
        local_file.write_bytes(b"fake parquet data")

        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"etag_regression"',
            "VersionId": "v-reg-1",
            "ContentLength": 16,
        }
        mock_uploader = MagicMock()
        mock_uploader._get_client.return_value = mock_client
        mock_uploader.bucket = "mctrader-market"

        result = promote_l1(
            local_path=local_file,
            nas_uploader=mock_uploader,
            nas_key="l1/market/part-regression.parquet",
            segment_id="regression-epic-smoke",
        )
        assert isinstance(result, PromotionResult)
        assert result.status == "promoted"
        assert not local_file.exists()
