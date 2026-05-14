# tests/integration/compactor/test_ambiguity_invariant.py
"""MCT-169 D10=A: ambiguity invariant enforcement tests.

INV-1 SoT exclusivity: ∀ segment, nas_exists ⊕ local_exists = true (XOR).
NAS+local 동시 존재 fixture 시 AmbiguityViolation raise 확인 (AC-3, AC-8).

Test Contract (MCT-169 §6):
- test_ambiguity_violation_raised: NAS+local 동시 존재 fixture → AmbiguityViolation raise (AC-3, AC-8)
- test_nas_only_no_violation: NAS 존재 + local 부재 → no violation (INV-1 XOR 정합)
- test_local_only_no_violation: local 존재 + NAS 부재 → no violation (INV-1 XOR 정합)
- test_neither_exists_no_violation: 둘 다 부재 → no violation (empty state)
- test_post_promotion_no_ambiguity: promote_l1() 완료 후 verify_no_ambiguity → no violation (INV-1)
- test_invariant_xor_property: XOR invariant — promotion 전후 상태 변환 확인

D10=A: NAS+local 동시 존재 = violation → pytest FAIL 유도 (AC-8)
INV-1: post-promotion 시점 = NAS only (local 부재) → XOR = true
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from mctrader_data.nas_storage.nas_uploader import PutResult


# ─── helpers ────────────────────────────────────────────────────────────────


def _make_nas_uploader_mock(
    *,
    head_exists: bool = True,
    head_etag: str = "abc123",
    head_version_id: str | None = "v1",
) -> MagicMock:
    """Return a mock NASUploader for ambiguity tests.

    promotion.py 는 nas_uploader._get_client().head_object() 를 호출하므로
    _get_client() 반환 mock 의 head_object 를 설정.
    """
    from botocore.exceptions import ClientError

    mock_client = MagicMock()
    if head_exists:
        mock_client.head_object.return_value = {
            "ETag": f'"{head_etag}"',
            "VersionId": head_version_id,
            "ContentLength": 1024,
            "Metadata": {"sha256": "fakehash"},
        }
    else:
        mock_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )

    mock = MagicMock()
    mock._get_client.return_value = mock_client
    mock.bucket = "mctrader-market"
    return mock


# ─── tests ───────────────────────────────────────────────────────────────────


class TestAmbiguityInvariant:
    """D10=A: NAS+local 동시 존재 = violation enforcement (INV-1 SoT exclusivity)."""

    def test_ambiguity_violation_raised(self, tmp_path: Path) -> None:
        """AC-3, AC-8: NAS+local 동시 존재 fixture → AmbiguityViolation raise.

        D10=A invariant: nas_exists ∧ local_exists = violation.
        본 test 가 PASS = invariant enforcement 확인 (AC-8: pytest fail 유도 아닌,
        violation 검출 함수 자체의 FAIL raise 확인).
        """
        from mctrader_data.compactor.promotion import AmbiguityViolation, verify_no_ambiguity

        local_file = tmp_path / "part-abc.parquet"
        local_file.write_bytes(b"fake parquet")

        # NAS HEAD 성공 (nas_exists=True) + local 존재 (local_exists=True) = violation
        mock_uploader = _make_nas_uploader_mock(head_exists=True)

        with pytest.raises(AmbiguityViolation) as exc_info:
            verify_no_ambiguity(
                segment_id="test-segment-001",
                nas_uploader=mock_uploader,
                nas_key="l1/market/transaction/part-abc.parquet",
                local_path=local_file,
            )

        assert "ambiguity" in str(exc_info.value).lower() or "violation" in str(exc_info.value).lower()

    def test_nas_only_no_violation(self, tmp_path: Path) -> None:
        """INV-1 XOR: NAS 존재 + local 부재 → no violation (XOR = true)."""
        from mctrader_data.compactor.promotion import verify_no_ambiguity

        local_file = tmp_path / "part-nas-only.parquet"
        # local 파일 생성 안 함 (local_exists=False)
        assert not local_file.exists()

        mock_uploader = _make_nas_uploader_mock(head_exists=True)

        # Should NOT raise — NAS only = valid SoT state
        verify_no_ambiguity(
            segment_id="test-segment-002",
            nas_uploader=mock_uploader,
            nas_key="l1/market/transaction/part-nas-only.parquet",
            local_path=local_file,
        )

    def test_local_only_no_violation(self, tmp_path: Path) -> None:
        """INV-1 XOR: local 존재 + NAS 부재 → no violation (XOR = true, pre-promotion state)."""
        from mctrader_data.compactor.promotion import verify_no_ambiguity

        local_file = tmp_path / "part-local-only.parquet"
        local_file.write_bytes(b"fake parquet")

        # NAS HEAD 404 (nas_exists=False)
        mock_uploader = _make_nas_uploader_mock(head_exists=False)

        # Should NOT raise — local only = valid pre-promotion state
        verify_no_ambiguity(
            segment_id="test-segment-003",
            nas_uploader=mock_uploader,
            nas_key="l1/market/transaction/part-local-only.parquet",
            local_path=local_file,
        )

    def test_neither_exists_no_violation(self, tmp_path: Path) -> None:
        """INV-1: 둘 다 부재 → no violation (empty/cleaned state)."""
        from mctrader_data.compactor.promotion import verify_no_ambiguity

        local_file = tmp_path / "part-neither.parquet"
        assert not local_file.exists()

        mock_uploader = _make_nas_uploader_mock(head_exists=False)

        # Should NOT raise — neither exists = valid cleaned state
        verify_no_ambiguity(
            segment_id="test-segment-004",
            nas_uploader=mock_uploader,
            nas_key="l1/market/transaction/part-neither.parquet",
            local_path=local_file,
        )

    def test_post_promotion_no_ambiguity(self, tmp_path: Path) -> None:
        """INV-1: promote_l1() 완료 후 verify_no_ambiguity → no violation.

        Promotion = local delete → NAS only 상태. XOR = true.
        """
        from mctrader_data.compactor.promotion import promote_l1, verify_no_ambiguity, PromotionResult

        local_file = tmp_path / "part-promote.parquet"
        local_file.write_bytes(b"fake parquet data")

        # Mock NASUploader: HEAD verify PASS + VersionId available
        # promotion.py 는 _get_client().head_object() 호출 → mock_client 에 설정
        mock_client = MagicMock()
        head_response = {
            "ETag": '"etag_promote"',
            "VersionId": "version-1",
            "ContentLength": len(b"fake parquet data"),
            "Metadata": {"sha256": "fakehash"},
        }
        mock_client.head_object.return_value = head_response
        mock_uploader = MagicMock()
        mock_uploader._get_client.return_value = mock_client
        mock_uploader.bucket = "mctrader-market"

        nas_key = "l1/market/transaction/part-promote.parquet"

        # promote_l1() 실행 — NAS HEAD verify PASS → local delete
        result = promote_l1(
            local_path=local_file,
            nas_uploader=mock_uploader,
            nas_key=nas_key,
            segment_id="test-segment-005",
        )

        assert result.status == "promoted"
        assert not local_file.exists()  # local 삭제 확인 (AC-2)

        # post-promotion: NAS 존재 + local 부재 → no violation (INV-1)
        verify_no_ambiguity(
            segment_id="test-segment-005",
            nas_uploader=mock_uploader,
            nas_key=nas_key,
            local_path=local_file,
        )

    def test_invariant_xor_property(self, tmp_path: Path) -> None:
        """D10=A XOR invariant: promotion 전후 상태 변환 확인.

        pre-promotion: local_exists=True, nas_exists=True → violation (ambiguity)
        post-promotion: local_exists=False, nas_exists=True → OK (INV-1)
        """
        from mctrader_data.compactor.promotion import (
            AmbiguityViolation,
            promote_l1,
            verify_no_ambiguity,
        )

        local_file = tmp_path / "part-xor-test.parquet"
        local_file.write_bytes(b"parquet content")

        nas_key = "l1/market/transaction/part-xor-test.parquet"

        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"etag_xor"',
            "VersionId": "version-xor",
            "ContentLength": len(b"parquet content"),
            "Metadata": {"sha256": "xorhash"},
        }
        mock_uploader = MagicMock()
        mock_uploader._get_client.return_value = mock_client
        mock_uploader.bucket = "mctrader-market"

        # pre-promotion ambiguity check: NAS 존재 + local 존재 = violation
        with pytest.raises(AmbiguityViolation):
            verify_no_ambiguity(
                segment_id="xor-001",
                nas_uploader=mock_uploader,
                nas_key=nas_key,
                local_path=local_file,
            )

        # promote_l1: HEAD verify PASS → local delete (grace 0)
        result = promote_l1(
            local_path=local_file,
            nas_uploader=mock_uploader,
            nas_key=nas_key,
            segment_id="xor-001",
        )
        assert result.status == "promoted"
        assert not local_file.exists()

        # post-promotion: NAS 존재 + local 부재 → no violation (INV-1 XOR)
        verify_no_ambiguity(
            segment_id="xor-001",
            nas_uploader=mock_uploader,
            nas_key=nas_key,
            local_path=local_file,
        )
