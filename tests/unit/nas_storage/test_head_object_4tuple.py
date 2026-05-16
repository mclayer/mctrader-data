"""test_head_object_4tuple.py — Unit tests for NASUploader.head_object() 4-tuple verify primitive.

MCT-189 D-4 C: head_object() returns normalized 4-field dict.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from mctrader_data.nas_storage.nas_uploader import NASUploader


def _make_uploader() -> NASUploader:
    return NASUploader(
        endpoint="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-bucket",
    )


class TestHeadObject4Tuple:
    """NASUploader.head_object() 4-tuple field 검증."""

    def test_etag_stripped_of_quotes(self) -> None:
        """ETag 값이 surrounding double-quote 없이 반환돼야 한다."""
        uploader = _make_uploader()
        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"abc123def456"',
            "VersionId": "v1",
            "Metadata": {"sha256": "deadbeef"},
            "ContentLength": 1024,
        }
        with patch.object(uploader, "_get_client", return_value=mock_client):
            result = uploader.head_object("some/key")

        assert result["ETag"] == "abc123def456"

    def test_version_id_returned(self) -> None:
        """VersionId가 그대로 반환돼야 한다."""
        uploader = _make_uploader()
        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"etag"',
            "VersionId": "ver-001",
            "Metadata": {},
            "ContentLength": 512,
        }
        with patch.object(uploader, "_get_client", return_value=mock_client):
            result = uploader.head_object("some/key")

        assert result["VersionId"] == "ver-001"

    def test_version_id_none_when_absent(self) -> None:
        """VersionId 부재 시 None이 반환돼야 한다."""
        uploader = _make_uploader()
        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"etag"',
            "Metadata": {},
            "ContentLength": 512,
        }
        with patch.object(uploader, "_get_client", return_value=mock_client):
            result = uploader.head_object("some/key")

        assert result["VersionId"] is None

    def test_sha256_from_metadata(self) -> None:
        """sha256가 Metadata 딕셔너리에서 추출돼야 한다."""
        uploader = _make_uploader()
        mock_client = MagicMock()
        sha256_val = "a" * 64
        mock_client.head_object.return_value = {
            "ETag": '"etag"',
            "Metadata": {"sha256": sha256_val},
            "ContentLength": 256,
        }
        with patch.object(uploader, "_get_client", return_value=mock_client):
            result = uploader.head_object("some/key")

        assert result["sha256"] == sha256_val

    def test_sha256_none_when_metadata_absent(self) -> None:
        """Metadata sha256 부재 시 None이 반환돼야 한다."""
        uploader = _make_uploader()
        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"etag"',
            "Metadata": {},
            "ContentLength": 256,
        }
        with patch.object(uploader, "_get_client", return_value=mock_client):
            result = uploader.head_object("some/key")

        assert result["sha256"] is None

    def test_sha256_none_when_metadata_key_missing(self) -> None:
        """Metadata 자체가 없을 때도 sha256은 None이어야 한다."""
        uploader = _make_uploader()
        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"etag"',
            "ContentLength": 256,
        }
        with patch.object(uploader, "_get_client", return_value=mock_client):
            result = uploader.head_object("some/key")

        assert result["sha256"] is None

    def test_content_length_as_int(self) -> None:
        """ContentLength가 int로 반환돼야 한다."""
        uploader = _make_uploader()
        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"etag"',
            "Metadata": {},
            "ContentLength": 2048,
        }
        with patch.object(uploader, "_get_client", return_value=mock_client):
            result = uploader.head_object("some/key")

        assert result["ContentLength"] == 2048
        assert isinstance(result["ContentLength"], int)

    def test_all_four_fields_present(self) -> None:
        """반환 dict에 4개 field가 모두 존재해야 한다."""
        uploader = _make_uploader()
        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"abc"',
            "VersionId": "v99",
            "Metadata": {"sha256": "beef"},
            "ContentLength": 100,
        }
        with patch.object(uploader, "_get_client", return_value=mock_client):
            result = uploader.head_object("key")

        assert set(result.keys()) == {"ETag", "VersionId", "sha256", "ContentLength"}

    def test_bucket_and_key_forwarded(self) -> None:
        """head_object 호출 시 올바른 Bucket과 Key가 전달돼야 한다."""
        uploader = _make_uploader()
        mock_client = MagicMock()
        mock_client.head_object.return_value = {
            "ETag": '"abc"',
            "Metadata": {},
            "ContentLength": 0,
        }
        with patch.object(uploader, "_get_client", return_value=mock_client):
            uploader.head_object("tier/prefix/file.parquet")

        mock_client.head_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="tier/prefix/file.parquet",
        )
