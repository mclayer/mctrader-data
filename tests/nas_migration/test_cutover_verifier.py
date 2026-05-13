"""test_cutover_verifier.py — P0 TDD tests for CutoverVerifier (RPO=0 verify).

Story: MCT-155 (Stage 2 — Local GC + Secret rotation + RPO=0 verify + Stage 2 종료 gate)
Issue: mclayer/mctrader-hub#274

Test Contract §8.1 (TestContractArchitectAgent — MCT-155):
- T-1 RpoVerifyResult.status = rpo_zero_verified — diff 0 + 7종 invariant ALL PASS
- T-2 RpoVerifyResult.status = drift_detected — diff > 0 (segment missing)
- T-3 RpoVerifyResult.status = drift_detected — 7종 invariant 1종 이상 FAIL
- T-4 RpoVerifyResult.status = verify_inconclusive — NAS unreachable transient
- T-5 InvariantHarness inject mock — 7종 invariant ALL PASS pathway
- T-6 cutover rollback signal emit path (FAIL 시 — operator manual gate verify)
- T-status_enum_exact_string_match (§6.8 wording SSOT)

§6.8 Wording SSOT:
- status enum 3종: "rpo_zero_verified" / "drift_detected" / "verify_inconclusive"
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mctrader_data.nas_migration.cutover_verifier import (
    CutoverVerifier,
    RpoVerifyResult,
)
from mctrader_data.nas_migration.invariant_harness import InvariantResult


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_nas_uploader() -> MagicMock:
    """NASUploader mock: _list_objects returns keys."""
    mock = MagicMock()
    mock._list_objects = MagicMock(return_value=[])
    return mock


@pytest.fixture
def mock_invariant_harness() -> MagicMock:
    """InvariantHarness mock: verify returns ALL PASS."""
    mock = MagicMock()
    mock.verify.return_value = InvariantResult(
        status="all_pass",
        per_invariant_results={},
        verify_latency_ms=10.0,
    )
    return mock


@pytest.fixture
def local_l2_root(tmp_path: Path) -> Path:
    """Create local L2 root directory."""
    root = tmp_path / "L2"
    root.mkdir()
    return root


@pytest.fixture
def cutover_verifier(
    mock_nas_uploader: MagicMock,
    mock_invariant_harness: MagicMock,
    local_l2_root: Path,
) -> CutoverVerifier:
    """Build CutoverVerifier with mocks."""
    return CutoverVerifier(
        nas_uploader=mock_nas_uploader,
        invariant_harness=mock_invariant_harness,
        local_l2_root=local_l2_root,
    )


def _create_local_parquet(
    local_l2_root: Path,
    partition: str,
    filename: str,
    content: bytes = b"test",
    mtime_offset_seconds: float = -10.0,
) -> Path:
    """Helper: create a local parquet file in given partition.

    mtime_offset_seconds = how many seconds before *now* to set mtime (default -10s,
    so file appears to exist at cutover-1s timestamp).
    """
    import os
    import time

    partition_dir = local_l2_root / partition
    partition_dir.mkdir(parents=True, exist_ok=True)
    parquet_file = partition_dir / filename
    parquet_file.write_bytes(content)
    # Set mtime so file exists at cutover-1s
    target_mtime = time.time() + mtime_offset_seconds
    os.utime(parquet_file, (target_mtime, target_mtime))
    return parquet_file


# ─── T-1: rpo_zero_verified — diff 0 + ALL PASS ──────────────────────────────


def test_rpo_zero_verified_diff_0_and_all_pass(
    cutover_verifier: CutoverVerifier,
    mock_nas_uploader: MagicMock,
    local_l2_root: Path,
) -> None:
    """T-1: diff 0 + 7종 invariant ALL PASS → status='rpo_zero_verified'."""
    # Create local segment with mtime in past
    _create_local_parquet(
        local_l2_root, "exchange=upbit/symbol=BTC_KRW/date=2025-11-01", "part-0.parquet"
    )
    # NAS has same basename
    mock_nas_uploader._list_objects.return_value = [
        "schema_version=v1/tier=L2/exchange=upbit/symbol=BTC_KRW/date=2025-11-01/part-0.parquet"
    ]

    cutover_ts = datetime.now(timezone.utc).isoformat()
    result = cutover_verifier.verify_rpo_zero(cutover_ts)

    assert result.status == "rpo_zero_verified"
    assert len(result.diff_segments) == 0


# ─── T-2: drift_detected — diff > 0 (segment missing) ────────────────────────


def test_drift_detected_segment_missing_in_nas(
    cutover_verifier: CutoverVerifier,
    mock_nas_uploader: MagicMock,
    local_l2_root: Path,
) -> None:
    """T-2: cutover-1s segment missing in NAS @ +1s → status='drift_detected'."""
    _create_local_parquet(
        local_l2_root,
        "exchange=upbit/symbol=BTC_KRW/date=2025-11-01",
        "missing-on-nas.parquet",
    )
    mock_nas_uploader._list_objects.return_value = []  # NAS empty

    cutover_ts = datetime.now(timezone.utc).isoformat()
    result = cutover_verifier.verify_rpo_zero(cutover_ts)

    assert result.status == "drift_detected"
    assert "missing-on-nas.parquet" in result.diff_segments


# ─── T-3: drift_detected — 7종 invariant FAIL ────────────────────────────────


def test_drift_detected_invariant_fail(
    cutover_verifier: CutoverVerifier,
    mock_nas_uploader: MagicMock,
    mock_invariant_harness: MagicMock,
    local_l2_root: Path,
) -> None:
    """T-3: 7종 invariant 1종 이상 FAIL → status='drift_detected'."""
    _create_local_parquet(
        local_l2_root,
        "exchange=upbit/symbol=BTC_KRW/date=2025-11-01",
        "part-0.parquet",
    )
    mock_nas_uploader._list_objects.return_value = [
        "schema_version=v1/tier=L2/exchange=upbit/symbol=BTC_KRW/date=2025-11-01/part-0.parquet"
    ]
    # invariant returns sha256_fail
    mock_invariant_harness.verify.return_value = InvariantResult(
        status="sha256_fail",
        per_invariant_results={},
        verify_latency_ms=10.0,
    )

    cutover_ts = datetime.now(timezone.utc).isoformat()
    result = cutover_verifier.verify_rpo_zero(cutover_ts)

    assert result.status == "drift_detected"
    assert result.invariant_result is not None
    assert result.invariant_result.status == "sha256_fail"


# ─── T-4: verify_inconclusive — NAS unreachable ─────────────────────────────


def test_verify_inconclusive_nas_unreachable(
    cutover_verifier: CutoverVerifier,
    mock_nas_uploader: MagicMock,
) -> None:
    """T-4: NAS unreachable transient → status='verify_inconclusive'."""
    mock_nas_uploader._list_objects.side_effect = ConnectionError("NAS unreachable")

    cutover_ts = datetime.now(timezone.utc).isoformat()
    result = cutover_verifier.verify_rpo_zero(cutover_ts)

    assert result.status == "verify_inconclusive"
    assert "NAS unreachable" in result.verify_error


# ─── T-4b: verify_inconclusive — timestamp parse fail ────────────────────────


def test_verify_inconclusive_timestamp_parse_fail(
    cutover_verifier: CutoverVerifier,
) -> None:
    """T-4b: timestamp parse 실패 → status='verify_inconclusive'."""
    result = cutover_verifier.verify_rpo_zero("not-a-valid-iso-timestamp")
    assert result.status == "verify_inconclusive"
    assert "parse failed" in result.verify_error


# ─── T-5: InvariantHarness inject mock — ALL PASS pathway ────────────────────


def test_invariant_harness_inject_all_pass(
    cutover_verifier: CutoverVerifier,
    mock_invariant_harness: MagicMock,
    mock_nas_uploader: MagicMock,
    local_l2_root: Path,
) -> None:
    """T-5: InvariantHarness inject → ALL PASS pathway verify (call check)."""
    _create_local_parquet(
        local_l2_root,
        "exchange=upbit/symbol=BTC_KRW/date=2025-11-01",
        "part-0.parquet",
    )
    mock_nas_uploader._list_objects.return_value = [
        "schema_version=v1/tier=L2/exchange=upbit/symbol=BTC_KRW/date=2025-11-01/part-0.parquet"
    ]

    cutover_ts = datetime.now(timezone.utc).isoformat()
    result = cutover_verifier.verify_rpo_zero(cutover_ts)

    # InvariantHarness.verify called for at least one partition
    assert mock_invariant_harness.verify.call_count >= 1
    assert result.status == "rpo_zero_verified"


# ─── T-6: cutover rollback signal emit path (FAIL 시) ───────────────────────


def test_drift_detected_emits_signal_via_status(
    cutover_verifier: CutoverVerifier,
    mock_nas_uploader: MagicMock,
    local_l2_root: Path,
) -> None:
    """T-6: drift_detected status 가 caller 측 cutover rollback signal emit path."""
    _create_local_parquet(
        local_l2_root,
        "exchange=upbit/symbol=BTC_KRW/date=2025-11-01",
        "missing.parquet",
    )
    mock_nas_uploader._list_objects.return_value = []

    cutover_ts = datetime.now(timezone.utc).isoformat()
    result = cutover_verifier.verify_rpo_zero(cutover_ts)

    # status='drift_detected' 가 cutover rollback signal emit trigger (caller 책임)
    assert result.status == "drift_detected"
    # diff_segments 박제 evidence (operator manual gate input)
    assert len(result.diff_segments) > 0


# ─── status enum exact string match (§6.8 wording SSOT) ──────────────────────


@pytest.mark.parametrize(
    "expected_status",
    ["rpo_zero_verified", "drift_detected", "verify_inconclusive"],
)
def test_status_enum_exact_string_match(expected_status: str) -> None:
    """§6.8 wording SSOT: status enum 정확한 string 만 허용."""
    result = RpoVerifyResult(status=expected_status)  # type: ignore[arg-type]
    assert result.status == expected_status


# ─── Idempotency (§11.6) ─────────────────────────────────────────────────────


def test_verify_idempotent_across_invocations(
    cutover_verifier: CutoverVerifier,
    mock_nas_uploader: MagicMock,
    local_l2_root: Path,
) -> None:
    """§11.6 idempotency: 다중 호출 시 동일 결과 (cutover_timestamp 동일)."""
    _create_local_parquet(
        local_l2_root,
        "exchange=upbit/symbol=BTC_KRW/date=2025-11-01",
        "part-0.parquet",
    )
    mock_nas_uploader._list_objects.return_value = [
        "schema_version=v1/tier=L2/exchange=upbit/symbol=BTC_KRW/date=2025-11-01/part-0.parquet"
    ]

    cutover_ts = datetime.now(timezone.utc).isoformat()
    result1 = cutover_verifier.verify_rpo_zero(cutover_ts)
    result2 = cutover_verifier.verify_rpo_zero(cutover_ts)

    assert result1.status == result2.status
    assert result1.diff_segments == result2.diff_segments
