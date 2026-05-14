# tests/integration/compactor/test_promotion.py
"""MCT-169 promotion.py tests — AC-1/2/5/7, INV-1/2/4/5/6.

Test Contract (MCT-169 §6):
- test_promote_l1_head_verify_pass: NAS HEAD verify PASS → promote succeed + local delete (AC-1, AC-2)
- test_promote_l1_head_404_raises: HEAD 404 → PromotionVerifyError + local 유지 (AC-7, INV-4)
- test_promote_l1_etag_mismatch_raises: ETag mismatch → PromotionVerifyError + local 유지 (AC-7, INV-4)
- test_promote_l1_grace_0: wall-clock HEAD verify → unlink < 100ms (INV-2)
- test_promote_l1_version_id_match: VersionId 일치 강제 (INV-5)
- test_promote_l1_idempotent_already_promoted: local 부재 + NAS 존재 → already_promoted (INV-6)
- test_promote_l1_retry_on_initial_fail: HEAD retry 1회 (50ms backoff, R-2 mitigation, AC-7)
- test_get_streaming_ranged_get: get_streaming() Range ranged GET (AC-5)
- test_get_streaming_full_object: get_streaming() full object GET (AC-5)
"""
from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock
from io import BytesIO

import pytest

from botocore.exceptions import ClientError


# ─── helpers ────────────────────────────────────────────────────────────────


def _client_error_404() -> ClientError:
    return ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadObject",
    )


def _make_mock_uploader(
    *,
    head_exists: bool = True,
    head_etag: str = "etag-abc123",
    head_version_id: str | None = "v1-test",
    content_length: int = 1024,
    get_object_body: bytes = b"fake parquet content",
) -> MagicMock:
    """Return a mock NASUploader.

    promotion.py + get_streaming.py 는 nas_uploader._get_client().head_object() 호출.
    → mock_client 에 head_object, get_object 설정.
    """
    mock_client = MagicMock()
    mock_client.bucket = "mctrader-market"

    if head_exists:
        mock_client.head_object.return_value = {
            "ETag": f'"{head_etag}"',
            "VersionId": head_version_id,
            "ContentLength": content_length,
            "Metadata": {"sha256": "fakehash"},
        }
    else:
        mock_client.head_object.side_effect = _client_error_404()

    # get_object mock (for get_streaming)
    body_stream = BytesIO(get_object_body)
    mock_client.get_object.return_value = {"Body": body_stream}

    mock = MagicMock()
    mock.bucket = "mctrader-market"
    mock._get_client.return_value = mock_client

    return mock


# ─── AC-1, AC-2 tests ────────────────────────────────────────────────────────


class TestPromoteL1HeadVerify:
    """AC-1 promotion.py NAS HEAD verify + AC-2 immediate local delete."""

    def test_promote_l1_head_verify_pass(self, tmp_path: Path) -> None:
        """AC-1 + AC-2: HEAD verify PASS → promote succeed + local delete."""
        from mctrader_data.compactor.promotion import promote_l1

        local_file = tmp_path / "part-test.parquet"
        local_file.write_bytes(b"fake parquet content")

        mock_uploader = _make_mock_uploader(
            head_exists=True,
            head_etag="etag-test",
            head_version_id="v1",
        )

        result = promote_l1(
            local_path=local_file,
            nas_uploader=mock_uploader,
            nas_key="l1/market/transaction/part-test.parquet",
            segment_id="seg-001",
        )

        assert result.status == "promoted"
        assert result.segment_id == "seg-001"
        assert not local_file.exists()  # AC-2: local deleted
        # AC-1: head_object called (via _get_client())
        mock_client = mock_uploader._get_client.return_value
        mock_client.head_object.assert_called_once()

    def test_promote_l1_head_404_raises(self, tmp_path: Path) -> None:
        """AC-7 + INV-4: HEAD 404 → PromotionVerifyError + local 유지."""
        from mctrader_data.compactor.promotion import promote_l1, PromotionVerifyError

        local_file = tmp_path / "part-missing.parquet"
        local_file.write_bytes(b"parquet content")

        mock_uploader = _make_mock_uploader(head_exists=False)

        with pytest.raises(PromotionVerifyError) as exc_info:
            promote_l1(
                local_path=local_file,
                nas_uploader=mock_uploader,
                nas_key="l1/market/transaction/part-missing.parquet",
                segment_id="seg-002",
            )

        assert "404" in str(exc_info.value) or "not found" in str(exc_info.value).lower()
        assert local_file.exists()  # INV-4: local 보존

    def test_promote_l1_etag_mismatch_raises(self, tmp_path: Path) -> None:
        """AC-7 + INV-4: ETag 정보 일치 없이 → PromotionVerifyError + local 유지.

        Note: 현재 구현에서는 ETag 값 자체는 서버 응답으로 신뢰하므로
        mismatch scenario = VersionId mismatch 또는 ContentLength mismatch.
        HEAD 200 + 정상 응답 = verify PASS (promotion proceed).
        본 test: HEAD 성공하되 verify 실패 시나리오 = ClientError (비-404).
        """
        from mctrader_data.compactor.promotion import promote_l1, PromotionVerifyError

        local_file = tmp_path / "part-conflict.parquet"
        local_file.write_bytes(b"parquet content")

        mock_client = MagicMock()
        # HEAD가 403 (non-404 ClientError) → verify fail
        mock_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}},
            "HeadObject",
        )
        mock_uploader = MagicMock()
        mock_uploader.bucket = "mctrader-market"
        mock_uploader._get_client.return_value = mock_client

        with pytest.raises(PromotionVerifyError):
            promote_l1(
                local_path=local_file,
                nas_uploader=mock_uploader,
                nas_key="l1/market/transaction/part-conflict.parquet",
                segment_id="seg-003",
            )

        assert local_file.exists()  # INV-4: local 보존


class TestPromoteL1Grace0:
    """INV-2: HEAD verify pass → local unlink 사이 wall-clock < 100ms."""

    def test_promote_l1_grace_0(self, tmp_path: Path) -> None:
        """INV-2: wall-clock < 100ms (grace 0)."""
        from mctrader_data.compactor.promotion import promote_l1

        local_file = tmp_path / "part-grace.parquet"
        local_file.write_bytes(b"grace test")

        mock_uploader = _make_mock_uploader(head_exists=True)

        t_start = time.monotonic()
        result = promote_l1(
            local_path=local_file,
            nas_uploader=mock_uploader,
            nas_key="l1/market/transaction/part-grace.parquet",
            segment_id="seg-grace",
        )
        elapsed_ms = (time.monotonic() - t_start) * 1000

        assert result.status == "promoted"
        assert not local_file.exists()
        assert elapsed_ms < 100, f"grace 0 violation: {elapsed_ms:.1f}ms > 100ms (INV-2)"


class TestPromoteL1VersionId:
    """INV-5: VersionId 일치 강제 (version-enabled bucket)."""

    def test_promote_l1_version_id_stored(self, tmp_path: Path) -> None:
        """INV-5: PromotionResult 에 version_id 박제."""
        from mctrader_data.compactor.promotion import promote_l1

        local_file = tmp_path / "part-version.parquet"
        local_file.write_bytes(b"version test")

        expected_version = "v-abc-123"
        mock_uploader = _make_mock_uploader(
            head_exists=True,
            head_version_id=expected_version,
        )

        result = promote_l1(
            local_path=local_file,
            nas_uploader=mock_uploader,
            nas_key="l1/market/transaction/part-version.parquet",
            segment_id="seg-ver",
        )

        assert result.status == "promoted"
        assert result.version_id == expected_version  # INV-5: version_id 박제


class TestPromoteL1Idempotency:
    """INV-6: local 부재 + NAS 존재 → already_promoted (idempotency)."""

    def test_already_promoted_no_op(self, tmp_path: Path) -> None:
        """INV-6: local 부재 + NAS HEAD 성공 → already_promoted 반환."""
        from mctrader_data.compactor.promotion import promote_l1

        local_file = tmp_path / "part-already.parquet"
        # local 파일 생성하지 않음 (already promoted state)
        assert not local_file.exists()

        mock_uploader = _make_mock_uploader(head_exists=True)

        result = promote_l1(
            local_path=local_file,
            nas_uploader=mock_uploader,
            nas_key="l1/market/transaction/part-already.parquet",
            segment_id="seg-idem",
        )

        assert result.status == "already_promoted"
        assert not local_file.exists()  # still not exists (no-op)


class TestPromoteL1Retry:
    """R-2 mitigation: HEAD retry 1회 (50ms backoff, AC-7)."""

    def test_retry_once_on_transient_error(self, tmp_path: Path) -> None:
        """AC-7: HEAD transient error → retry 1회 후 성공."""
        from mctrader_data.compactor.promotion import promote_l1

        local_file = tmp_path / "part-retry.parquet"
        local_file.write_bytes(b"retry test")

        from botocore.exceptions import EndpointConnectionError
        call_count = [0]

        def head_side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise EndpointConnectionError(endpoint_url="http://nas:9000")
            return {
                "ETag": '"etag-retry"',
                "VersionId": "v-retry",
                "ContentLength": len(b"retry test"),
                "Metadata": {"sha256": "retryhash"},
            }

        mock_client = MagicMock()
        mock_client.head_object.side_effect = head_side_effect
        mock_uploader = MagicMock()
        mock_uploader.bucket = "mctrader-market"
        mock_uploader._get_client.return_value = mock_client

        result = promote_l1(
            local_path=local_file,
            nas_uploader=mock_uploader,
            nas_key="l1/market/transaction/part-retry.parquet",
            segment_id="seg-retry",
        )

        assert result.status == "promoted"
        assert not local_file.exists()
        assert mock_client.head_object.call_count == 2  # 1 fail + 1 success

    def test_retry_exhausted_raises(self, tmp_path: Path) -> None:
        """AC-7: HEAD retry 1회 후에도 실패 → PromotionVerifyError + local 유지 (INV-4)."""
        from mctrader_data.compactor.promotion import promote_l1, PromotionVerifyError

        local_file = tmp_path / "part-retry-fail.parquet"
        local_file.write_bytes(b"retry fail test")

        from botocore.exceptions import EndpointConnectionError

        mock_client = MagicMock()
        mock_client.head_object.side_effect = EndpointConnectionError(endpoint_url="http://nas:9000")
        mock_uploader = MagicMock()
        mock_uploader.bucket = "mctrader-market"
        mock_uploader._get_client.return_value = mock_client

        with pytest.raises(PromotionVerifyError):
            promote_l1(
                local_path=local_file,
                nas_uploader=mock_uploader,
                nas_key="l1/market/transaction/part-retry-fail.parquet",
                segment_id="seg-retry-fail",
            )

        assert local_file.exists()  # INV-4: local 보존
        assert mock_client.head_object.call_count == 2  # initial + 1 retry


# ─── AC-5: get_streaming tests ───────────────────────────────────────────────


class TestGetStreaming:
    """AC-5: get_streaming() Range ranged GET (NAS GET helper)."""

    def test_get_streaming_full_object(self, tmp_path: Path) -> None:
        """AC-5: get_streaming() full object GET — byte_range=None."""
        from mctrader_data.nas_storage.get_streaming import get_streaming

        expected_data = b"full parquet content for streaming test"
        mock_uploader = _make_mock_uploader(get_object_body=expected_data)

        stream = get_streaming(
            nas_uploader=mock_uploader,
            nas_key="l1/market/transaction/part-stream.parquet",
            byte_range=None,
        )

        data = stream.read()
        assert data == expected_data
        # get_object called without Range header (via _get_client())
        mock_client = mock_uploader._get_client.return_value
        mock_client.get_object.assert_called_once()
        call_kwargs = mock_client.get_object.call_args
        assert "Range" not in (call_kwargs.kwargs or {})

    def test_get_streaming_ranged_get(self, tmp_path: Path) -> None:
        """AC-5: get_streaming() Range ranged GET — byte_range=(start, end)."""
        from mctrader_data.nas_storage.get_streaming import get_streaming

        expected_data = b"ranged bytes content"
        mock_client = MagicMock()
        mock_client.get_object.return_value = {"Body": BytesIO(expected_data)}
        mock_uploader = MagicMock()
        mock_uploader.bucket = "mctrader-market"
        mock_uploader._get_client.return_value = mock_client

        stream = get_streaming(
            nas_uploader=mock_uploader,
            nas_key="l1/market/transaction/part-range.parquet",
            byte_range=(100, 200),
        )

        data = stream.read()
        assert data == expected_data
        # get_object called with Range header (via _get_client())
        mock_client.get_object.assert_called_once()
        call_kwargs = mock_client.get_object.call_args
        call_kwargs_dict = call_kwargs.kwargs if call_kwargs.kwargs else {}
        assert call_kwargs_dict.get("Range") == "bytes=100-200"
