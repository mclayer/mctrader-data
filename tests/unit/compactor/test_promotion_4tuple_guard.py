"""test_promotion_4tuple_guard.py — Unit tests for promote_l1() 4중 verify + pre-delete guard.

MCT-189 D-4 C + D-8 B:
- sha256 mismatch → PromotionVerifyError + local 보존 (INV-4)
- ContentLength mismatch → PromotionVerifyError + local 보존 (INV-4)
- pre-delete guard ETag 변경 (race) → PromotionVerifyError + local 보존 (INV-4)
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mctrader_data.compactor.promotion import PromotionVerifyError, promote_l1
from mctrader_data.nas_storage.nas_uploader import NASUploader


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_mock_uploader(head_side_effect) -> NASUploader:
    """head_object() 에 side_effect를 주입한 NASUploader mock."""
    mock = MagicMock(spec=NASUploader)
    mock.head_object.side_effect = head_side_effect
    return mock


class TestPromotion4TupleGuard:
    """promote_l1() 4중 verify + pre-delete guard 검증."""

    def test_sha256_mismatch_raises_and_local_preserved(self, tmp_path: Path) -> None:
        """NAS sha256 ≠ local sha256 → PromotionVerifyError + local 파일 보존."""
        content = b"hello world parquet content"
        local = tmp_path / "seg.parquet"
        local.write_bytes(content)

        wrong_nas_sha256 = "0" * 64  # 의도적으로 다른 sha256

        # head_object 2회 호출 (1차 verify + pre-delete guard 대비)
        # sha256 mismatch는 1차에서 발생하므로 2번째 호출은 무관
        uploader = _make_mock_uploader([
            # 1차 HEAD: sha256 mismatch
            {
                "ETag": "etag-abc",
                "VersionId": None,
                "sha256": wrong_nas_sha256,
                "ContentLength": len(content),
            },
        ])

        with pytest.raises(PromotionVerifyError, match="sha256 mismatch"):
            promote_l1(
                local_path=local,
                nas_uploader=uploader,
                nas_key="l1/seg.parquet",
                segment_id="seg-001",
            )

        assert local.exists(), "INV-4: sha256 mismatch 시 local 파일 보존 의무"

    def test_content_length_mismatch_raises_and_local_preserved(self, tmp_path: Path) -> None:
        """NAS ContentLength ≠ local size → PromotionVerifyError + local 파일 보존."""
        content = b"parquet data bytes here"
        local = tmp_path / "seg.parquet"
        local.write_bytes(content)

        local_sha256 = _sha256(content)
        wrong_length = len(content) + 9999  # 의도적으로 다른 크기

        uploader = _make_mock_uploader([
            {
                "ETag": "etag-xyz",
                "VersionId": None,
                "sha256": local_sha256,
                "ContentLength": wrong_length,  # mismatch
            },
        ])

        with pytest.raises(PromotionVerifyError, match="ContentLength mismatch"):
            promote_l1(
                local_path=local,
                nas_uploader=uploader,
                nas_key="l1/seg.parquet",
                segment_id="seg-002",
            )

        assert local.exists(), "INV-4: ContentLength mismatch 시 local 파일 보존 의무"

    def test_pre_delete_guard_etag_change_raises_and_local_preserved(
        self, tmp_path: Path
    ) -> None:
        """pre-delete guard HEAD에서 ETag 변경 → PromotionVerifyError + local 파일 보존."""
        content = b"segment content for race detection"
        local = tmp_path / "seg.parquet"
        local.write_bytes(content)

        local_sha256 = _sha256(content)

        # side_effect: 1차 HEAD(verify pass) → 2차 HEAD(pre-delete guard, ETag 변경)
        uploader = _make_mock_uploader([
            # 1차 HEAD: sha256/ContentLength PASS
            {
                "ETag": "etag-initial",
                "VersionId": None,
                "sha256": local_sha256,
                "ContentLength": len(content),
            },
            # 2차 HEAD: pre-delete guard — ETag 변경 (concurrent overwrite 시뮬레이션)
            {
                "ETag": "etag-CHANGED",
                "VersionId": None,
                "sha256": local_sha256,
                "ContentLength": len(content),
            },
        ])

        with pytest.raises(PromotionVerifyError, match="pre-delete guard mismatch"):
            promote_l1(
                local_path=local,
                nas_uploader=uploader,
                nas_key="l1/seg.parquet",
                segment_id="seg-003",
            )

        assert local.exists(), "INV-4: pre-delete guard mismatch 시 local 파일 보존 의무"

    def test_happy_path_local_deleted(self, tmp_path: Path) -> None:
        """4중 verify + pre-delete guard 모두 통과 → local 파일 삭제."""
        content = b"valid parquet file content"
        local = tmp_path / "seg.parquet"
        local.write_bytes(content)

        local_sha256 = _sha256(content)
        good_response = {
            "ETag": "etag-good",
            "VersionId": "v1",
            "sha256": local_sha256,
            "ContentLength": len(content),
        }

        uploader = _make_mock_uploader([good_response, good_response])

        result = promote_l1(
            local_path=local,
            nas_uploader=uploader,
            nas_key="l1/seg.parquet",
            segment_id="seg-004",
        )

        assert result.status == "promoted"
        assert not local.exists(), "4중 verify + guard 통과 시 local 삭제 의무"

    def test_sha256_none_in_nas_skips_sha256_check(self, tmp_path: Path) -> None:
        """NAS sha256 없는 경우 (legacy object) sha256 verify skip → ContentLength로만 검증."""
        content = b"legacy object content"
        local = tmp_path / "seg.parquet"
        local.write_bytes(content)

        good_response = {
            "ETag": "etag-legacy",
            "VersionId": None,
            "sha256": None,  # legacy object: sha256 없음
            "ContentLength": len(content),
        }

        uploader = _make_mock_uploader([good_response, good_response])

        result = promote_l1(
            local_path=local,
            nas_uploader=uploader,
            nas_key="l1/seg.parquet",
            segment_id="seg-005",
        )

        # sha256 없어도 ContentLength 일치 + guard PASS → promoted
        assert result.status == "promoted"
        assert not local.exists()
