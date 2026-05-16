"""test_dual_writer_self_delete.py — Unit tests for DualWriter self-delete (MCT-189 D-2 A).

write() committed 브랜치: source(data as Path) promote_l1() 4중 verify 후 삭제.
put_l1() committed 브랜치: path promote_l1() 4중 verify 후 삭제.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

from mctrader_data.nas_storage.dual_writer import DualWriter
from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_uploader_for_streaming(content: bytes) -> NASUploader:
    """put_streaming() → uploaded + head_object() 4-tuple PASS mock."""
    mock = MagicMock(spec=NASUploader)
    sha256_val = _sha256(content)

    mock.put_streaming.return_value = PutResult(
        status="uploaded",
        object_etag="etag-ok",
        latency_ms=1.0,
    )
    # head_object() 4-tuple dict 반환 (promote_l1 verify path)
    mock.head_object.return_value = {
        "ETag": "etag-ok",
        "VersionId": None,
        "sha256": sha256_val,
        "ContentLength": len(content),
    }
    return mock


class TestDualWriterSelfDelete:
    """write() / put_l1() committed 후 source self-delete 검증."""

    def test_write_commit_source_deleted(self, tmp_path: Path) -> None:
        """write(data=Path) committed → source 파일이 삭제돼야 한다."""
        content = b"parquet source content for self-delete test"
        source = tmp_path / "source.parquet"
        source.write_bytes(content)

        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "tier" / "dest.parquet"
        local_dest.parent.mkdir(parents=True)

        uploader = _make_uploader_for_streaming(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        result = writer.write(
            local_path=local_dest,
            nas_key="tier/dest.parquet",
            data=source,
            sha256=_sha256(content),
        )

        assert result.status == "committed"
        assert not source.exists(), "MCT-189 D-2 A: committed 후 source 파일 삭제 의무"

    def test_write_commit_dest_still_exists(self, tmp_path: Path) -> None:
        """write(data=Path) committed → dest(local_path) 파일은 여전히 존재해야 한다."""
        content = b"test content for dest survival"
        source = tmp_path / "src.parquet"
        source.write_bytes(content)

        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "dest.parquet"

        uploader = _make_uploader_for_streaming(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        writer.write(
            local_path=local_dest,
            nas_key="dest.parquet",
            data=source,
            sha256=_sha256(content),
        )

        assert local_dest.exists(), "committed 후 dest(local_path) 파일은 보존돼야 한다"

    def test_write_bytes_path_source_not_deleted(self, tmp_path: Path) -> None:
        """write(data=bytes) 경우 source 개념 없음 — 기존 동작 불변."""
        content = b"bytes path data"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_path = local_root / "file.parquet"

        mock_uploader = MagicMock(spec=NASUploader)
        mock_uploader.put.return_value = PutResult(
            status="uploaded", object_etag="etag", latency_ms=1.0
        )

        writer = DualWriter(nas_uploader=mock_uploader, local_root=local_root)
        result = writer.write(
            local_path=local_path,
            nas_key="file.parquet",
            data=content,
            sha256=_sha256(content),
        )

        assert result.status == "committed"
        assert local_path.exists()  # bytes path → local_path 존재

    def test_write_promote_verify_fail_local_only_and_enqueue(self, tmp_path: Path) -> None:
        """P0-1: promote_l1 verify 실패 시 → status=local_only + retry_queue.enqueue() + source 보존.

        MCT-189 spec D-2 A + plan Task 14 Step 1:
        - verify-fail = NAS PUT 성공했으나 HEAD 불일치 → source 영구 orphan 방지
        - retry_queue.enqueue(key, data, sha256) 호출 의무
        - "committed" 반환 금지 — 거짓 신호 차단
        """
        content = b"verify fail scenario"
        source = tmp_path / "src_verify_fail.parquet"
        source.write_bytes(content)

        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "dest_vf.parquet"

        mock_retry_queue = MagicMock()
        mock_retry_queue.enqueue.return_value = MagicMock(status="ok")

        mock_uploader = MagicMock(spec=NASUploader)
        mock_uploader.put_streaming.return_value = PutResult(
            status="uploaded", object_etag="etag-ok", latency_ms=1.0
        )
        # sha256 mismatch → promote_l1 PromotionVerifyError
        mock_uploader.head_object.return_value = {
            "ETag": "etag-ok",
            "VersionId": None,
            "sha256": "0" * 64,  # wrong sha256
            "ContentLength": len(content),
        }
        # P0-1: retry_queue 접근 (NASUploader._retry_queue)
        mock_uploader._retry_queue = mock_retry_queue  # noqa: SLF001

        writer = DualWriter(nas_uploader=mock_uploader, local_root=local_root)
        result = writer.write(
            local_path=local_dest,
            nas_key="dest_vf.parquet",
            data=source,
            sha256=_sha256(content),
        )

        # P0-1: verify-fail → local_only (not "committed"), source 보존, retry_queue enqueue
        assert result.status == "local_only", "verify-fail 시 local_only 반환 의무 (committed 반환 금지)"
        assert source.exists(), "promote verify 실패 시 source 보존 의무 (INV-4)"
        mock_retry_queue.enqueue.assert_called_once_with(
            key="dest_vf.parquet", data=source, sha256=_sha256(content)
        )

    def test_write_local_only_source_not_deleted(self, tmp_path: Path) -> None:
        """write() local_only 시 source 삭제 미실행 (D-2 A는 committed만 해당)."""
        content = b"local only scenario"
        source = tmp_path / "src_local_only.parquet"
        source.write_bytes(content)

        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "dest_lo.parquet"

        mock_uploader = MagicMock(spec=NASUploader)
        mock_uploader.put_streaming.return_value = PutResult(
            status="queued", object_etag="", latency_ms=1.0
        )

        writer = DualWriter(nas_uploader=mock_uploader, local_root=local_root)
        result = writer.write(
            local_path=local_dest,
            nas_key="dest_lo.parquet",
            data=source,
            sha256=_sha256(content),
        )

        assert result.status == "local_only"
        assert source.exists(), "local_only 시 source 보존 (삭제 미실행)"
