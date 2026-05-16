# tests/integration/compactor/test_ambiguity_invariant.py
"""MCT-169 D10=A: ambiguity invariant enforcement tests (MCT-172 caller migrate).

MCT-172 (D8-5=A): verify_no_ambiguity 제거 → InvariantHarness._check_ambiguity() 경유.
INV-1 SoT exclusivity: ∀ segment, nas_exists ⊕ local_exists = true (XOR).
NAS+local 동시 존재 fixture 시 ambiguity 위반 확인 (AC-3, AC-8).

Test Contract (MCT-169 §6, MCT-172 D8-5=A migrate):
- test_ambiguity_violation_raised: NAS+local 동시 존재 fixture → ambiguity_fail (AC-3, AC-8)
- test_nas_only_no_violation: NAS 존재 + local 부재 → no violation (INV-1 XOR 정합)
- test_local_only_no_violation: local 존재 + NAS 부재 → no violation (INV-1 XOR 정합)
- test_neither_exists_no_violation: 둘 다 부재 → no violation (empty state)
- test_post_promotion_no_ambiguity: promote_l1() 완료 후 → no ambiguity (INV-1)
- test_invariant_xor_property: XOR invariant — promotion 전후 상태 변환 확인

D10=A: NAS+local 동시 존재 = violation → InvariantHarness._check_ambiguity() fail
INV-1: post-promotion 시점 = NAS only (local 부재) → XOR = true

MCT-172 D8-5=A: verify_no_ambiguity 는 promotion.py 에서 제거됨 (SSOT = InvariantHarness).
  caller migrate: harness._check_ambiguity() 또는 harness.verify() per_invariant_results["ambiguity"].

verified-via: Read src/mctrader_data/compactor/promotion.py (verify_no_ambiguity 제거 확인)
verified-via: Read src/mctrader_data/nas_migration/invariant_harness.py (_check_ambiguity method)
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock


# ─── helpers ────────────────────────────────────────────────────────────────


def _make_nas_uploader_mock(
    *,
    head_exists: bool = True,
    head_etag: str = "abc123",
    head_version_id: str | None = "v1",
) -> MagicMock:
    """Return a mock NASUploader for ambiguity tests.

    InvariantHarness._check_nas_partition_exists 는 list_objects_v2 를 사용하므로
    mock_client.list_objects_v2 설정.
    """
    mock_client = MagicMock()
    if head_exists:
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "test-segment-001/part.parquet"}]
        }
        mock_client.head_object.return_value = {
            "ETag": f'"{head_etag}"',
            "VersionId": head_version_id,
            "ContentLength": 1024,
            "Metadata": {"sha256": "fakehash"},
        }
    else:
        mock_client.list_objects_v2.return_value = {"Contents": []}
        from botocore.exceptions import ClientError
        mock_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )

    mock = MagicMock()
    mock._get_client.return_value = mock_client
    mock._list_objects.return_value = (
        ["test-segment-001/part.parquet"] if head_exists else []
    )
    mock.bucket = "mctrader-market"
    return mock


def _make_harness(tmp_path: Path, nas_uploader: MagicMock):
    """Return InvariantHarness for ambiguity check."""
    from mctrader_data.nas_migration.invariant_harness import InvariantHarness
    return InvariantHarness(
        nas_uploader=nas_uploader,
        local_root=tmp_path,
        expected_schema_version=("ohlcv.v1", "v1"),
    )


# ─── tests ───────────────────────────────────────────────────────────────────


class TestAmbiguityInvariant:
    """D10=A: NAS+local 동시 존재 = violation enforcement (INV-1 SoT exclusivity).

    MCT-172 D8-5=A: verify_no_ambiguity 제거 → InvariantHarness._check_ambiguity() SSOT.
    """

    def test_ambiguity_violation_raised(self, tmp_path: Path) -> None:
        """AC-3, AC-8: NAS+local 동시 존재 fixture → ambiguity_fail.

        D10=A invariant: nas_exists ∧ local_exists = violation.
        InvariantHarness._check_ambiguity() 가 fail 반환 = violation 검출 확인 (AC-8).

        MCT-172 D8-5=A: verify_no_ambiguity → InvariantHarness._check_ambiguity() 경유.
        """
        local_file = tmp_path / "part-abc.parquet"
        local_file.write_bytes(b"fake parquet")

        # NAS HEAD 성공 (nas_exists=True) + local 존재 (local_exists=True) = violation
        mock_uploader = _make_nas_uploader_mock(head_exists=True)
        harness = _make_harness(tmp_path, mock_uploader)

        result = harness._check_ambiguity(
            local_partition=tmp_path,
            nas_partition="schema_version=ohlcv.v1/tier=L1",
            local_files=[local_file],
        )

        assert result.status == "fail", (
            f"NAS+local 동시 존재 → ambiguity fail 기대. status={result.status!r}"
        )
        assert result.invariant_name == "ambiguity"
        assert len(result.mismatch_files) > 0

    def test_nas_only_no_violation(self, tmp_path: Path) -> None:
        """INV-1 XOR: NAS 존재 + local 부재 → no violation (XOR = true)."""
        # local 파일 없음 (local_exists=False)
        mock_uploader = _make_nas_uploader_mock(head_exists=True)
        harness = _make_harness(tmp_path, mock_uploader)

        result = harness._check_ambiguity(
            local_partition=tmp_path,
            nas_partition="schema_version=ohlcv.v1/tier=L1",
            local_files=[],  # empty = no local files
        )

        assert result.status == "pass", (
            f"NAS only (local empty) → ambiguity pass 기대. status={result.status!r}"
        )

    def test_local_only_no_violation(self, tmp_path: Path) -> None:
        """INV-1 XOR: local 존재 + NAS 부재 → no violation (XOR = true, pre-promotion state)."""
        local_file = tmp_path / "part-local-only.parquet"
        local_file.write_bytes(b"fake parquet")

        # NAS 없음 (nas_exists=False)
        mock_uploader = _make_nas_uploader_mock(head_exists=False)
        harness = _make_harness(tmp_path, mock_uploader)

        result = harness._check_ambiguity(
            local_partition=tmp_path,
            nas_partition="schema_version=ohlcv.v1/tier=L1",
            local_files=[local_file],
        )

        assert result.status == "pass", (
            f"local only (NAS absent) → ambiguity pass 기대. status={result.status!r}"
        )

    def test_neither_exists_no_violation(self, tmp_path: Path) -> None:
        """INV-1: 둘 다 부재 → no violation (empty/cleaned state)."""
        mock_uploader = _make_nas_uploader_mock(head_exists=False)
        harness = _make_harness(tmp_path, mock_uploader)

        result = harness._check_ambiguity(
            local_partition=tmp_path,
            nas_partition="schema_version=ohlcv.v1/tier=L1",
            local_files=[],  # neither local nor NAS
        )

        assert result.status == "pass", (
            f"둘 다 부재 → ambiguity pass 기대. status={result.status!r}"
        )

    def test_post_promotion_no_ambiguity(self, tmp_path: Path) -> None:
        """INV-1: promote_l1() 완료 후 → no ambiguity.

        Promotion = local delete → NAS only 상태. XOR = true.
        post-promotion: local 없음 → harness._check_ambiguity() pass.
        """
        from mctrader_data.compactor.promotion import promote_l1

        import hashlib as _hashlib
        local_content = b"fake parquet data"
        local_file = tmp_path / "part-promote.parquet"
        local_file.write_bytes(local_content)
        local_sha256 = _hashlib.sha256(local_content).hexdigest()

        # Mock NASUploader: HEAD verify PASS + list_objects_v2 (NAS exists post-promotion)
        # MCT-189: head_object() 4-tuple dict 직접 mock (ETag already stripped)
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "schema_version=ohlcv.v1/tier=L1/part-promote.parquet"}]
        }
        mock_uploader = MagicMock()
        mock_uploader._get_client.return_value = mock_client
        mock_uploader.head_object.return_value = {
            "ETag": "etag_promote",
            "VersionId": "version-1",
            "sha256": local_sha256,
            "ContentLength": len(local_content),
        }
        mock_uploader._list_objects.return_value = [
            "schema_version=ohlcv.v1/tier=L1/part-promote.parquet"
        ]
        mock_uploader.bucket = "mctrader-market"

        nas_key = "schema_version=ohlcv.v1/tier=L1/part-promote.parquet"

        # promote_l1() 실행 — NAS HEAD verify PASS → local delete
        result = promote_l1(
            local_path=local_file,
            nas_uploader=mock_uploader,
            nas_key=nas_key,
            segment_id="test-segment-005",
        )

        assert result.status == "promoted"
        assert not local_file.exists()  # local 삭제 확인 (AC-2)

        # post-promotion: local 없음 → harness._check_ambiguity() pass
        harness = _make_harness(tmp_path, mock_uploader)
        ambiguity_result = harness._check_ambiguity(
            local_partition=tmp_path,
            nas_partition="schema_version=ohlcv.v1/tier=L1",
            local_files=[],  # post-promotion: local empty
        )

        assert ambiguity_result.status == "pass", (
            f"post-promotion: local 없음 → ambiguity pass 기대. status={ambiguity_result.status!r}"
        )

    def test_invariant_xor_property(self, tmp_path: Path) -> None:
        """D10=A XOR invariant: promotion 전후 상태 변환 확인.

        pre-promotion: local_exists=True, nas_exists=True → violation (ambiguity)
        post-promotion: local_exists=False, nas_exists=True → OK (INV-1)
        """
        from mctrader_data.compactor.promotion import promote_l1

        import hashlib as _hashlib
        xor_content = b"parquet content"
        local_file = tmp_path / "part-xor-test.parquet"
        local_file.write_bytes(xor_content)
        xor_sha256 = _hashlib.sha256(xor_content).hexdigest()

        mock_client = MagicMock()
        # NAS has objects (nas_exists=True)
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "schema_version=ohlcv.v1/tier=L1/part-xor-test.parquet"}]
        }
        # MCT-189: head_object() 4-tuple dict 직접 mock (ETag already stripped)
        mock_uploader = MagicMock()
        mock_uploader._get_client.return_value = mock_client
        mock_uploader.head_object.return_value = {
            "ETag": "etag_xor",
            "VersionId": "version-xor",
            "sha256": xor_sha256,
            "ContentLength": len(xor_content),
        }
        mock_uploader._list_objects.return_value = [
            "schema_version=ohlcv.v1/tier=L1/part-xor-test.parquet"
        ]
        mock_uploader.bucket = "mctrader-market"

        harness = _make_harness(tmp_path, mock_uploader)

        # pre-promotion ambiguity check: NAS 존재 + local 존재 = violation
        pre_result = harness._check_ambiguity(
            local_partition=tmp_path,
            nas_partition="schema_version=ohlcv.v1/tier=L1",
            local_files=[local_file],
        )
        assert pre_result.status == "fail", (
            f"pre-promotion: NAS+local → ambiguity fail 기대. status={pre_result.status!r}"
        )

        # promote_l1: HEAD verify PASS → local delete (grace 0)
        nas_key = "schema_version=ohlcv.v1/tier=L1/part-xor-test.parquet"
        result = promote_l1(
            local_path=local_file,
            nas_uploader=mock_uploader,
            nas_key=nas_key,
            segment_id="xor-001",
        )
        assert result.status == "promoted"
        assert not local_file.exists()

        # post-promotion: NAS 존재 + local 부재 → no violation (INV-1 XOR)
        post_result = harness._check_ambiguity(
            local_partition=tmp_path,
            nas_partition="schema_version=ohlcv.v1/tier=L1",
            local_files=[],  # post-promotion: local empty
        )
        assert post_result.status == "pass", (
            f"post-promotion: local empty → ambiguity pass 기대. status={post_result.status!r}"
        )
