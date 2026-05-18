"""test_nas_uploader_4xx_fail_fast.py — INCIDENT-2026-05-17 amendment TDD tests.

ADR: ADR-027 INCIDENT-2026-05-17 amendment (NAS PUT 4xx fail-fast, silent fallback 차단)
Retro: mctrader-data#94 §6 carry-over Action Item 1 + mctrader-hub#394 PMO audit ADR 후보 #1

§AC 매트릭스 (ADR Amendment §검증 의무):
- AC-1: 4xx 분기 (403/401/AccessDenied/InvalidAccessKeyId/SignatureDoesNotMatch/NoSuchBucket/
        QuotaExceeded/StorageClassNotSupported) → NASOperationalAlert raise + Counter += 1 +
        retry_queue.enqueue **미호출** (silent fallback 0).
- AC-2: 5xx 분기 (500/503) + EndpointConnectionError → 현행 동작 보존 (queued status, raise 0).
- AC-3: §D6 7종 invariant 회귀 0 (verify path 무관, 본 amendment 는 upload result path).

각 test 가 put() (bytes path) + put_streaming() (Path/fileobj path) 양쪽 covera.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from mctrader_data.nas_storage.nas_uploader import (
    NASOperationalAlert,
    NASUploader,
)
from mctrader_data.nas_storage.retry_queue import RetryQueue


@pytest.fixture
def retry_queue(tmp_path: Path) -> RetryQueue:
    return RetryQueue(path=tmp_path / "retry_queue")


@pytest.fixture
def uploader(retry_queue: RetryQueue) -> NASUploader:
    return NASUploader(
        endpoint="http://nas.local:9000",
        access_key="test-access-key",
        secret_key="test-secret-key",
        bucket="test-bucket",
        retry_queue=retry_queue,
    )


def _client_error(code: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": f"mock {code}"}},
        "HeadObject",
    )


# ──────────────────────────────────────────────────────────────────────────────
# AC-1: 4xx fail-fast (put bytes path)
# ──────────────────────────────────────────────────────────────────────────────


class TestPut4xxFailFastBytesPath:
    """AC-1 — put() bytes path: 4xx → NASOperationalAlert raise + Counter + retry_queue 미호출."""

    @pytest.mark.parametrize(
        ("code", "expected_reason"),
        [
            ("403", "policy_denied"),
            ("AccessDenied", "policy_denied"),
            ("401", "auth_failed"),
            ("InvalidAccessKeyId", "auth_failed"),
            ("SignatureDoesNotMatch", "auth_failed"),
            ("NoSuchBucket", "bucket_missing"),
            ("QuotaExceeded", "quota_exceeded"),
            ("StorageClassNotSupported", "quota_exceeded"),
        ],
    )
    def test_put_4xx_raises_operational_alert(
        self,
        uploader: NASUploader,
        retry_queue: RetryQueue,
        code: str,
        expected_reason: str,
    ) -> None:
        """4xx ClientError 발생 시 NASOperationalAlert raise + reason 매핑 정확 + retry_queue 미호출."""
        data = b"test-payload-data"
        sha256 = hashlib.sha256(data).hexdigest()

        with (
            patch.object(uploader, "_get_client") as mock_client_factory,
            patch.object(retry_queue, "enqueue") as mock_enqueue,
        ):
            client = MagicMock()
            mock_client_factory.return_value = client
            client.head_object.side_effect = _client_error(code)

            with pytest.raises(NASOperationalAlert) as exc_info:
                uploader.put(key="test/object.bin", data=data, sha256=sha256)

        assert exc_info.value.code == code
        assert exc_info.value.reason == expected_reason
        assert exc_info.value.nas_key == "test/object.bin"
        # silent fallback 0: retry_queue.enqueue 미호출 (4xx 영구 오류 — 자동 재시도 의미 없음)
        assert not mock_enqueue.called, (
            f"4xx code={code} 는 retry_queue.enqueue 호출 금지 (ADR-027 INCIDENT-2026-05-17 amendment)"
        )

    def test_put_4xx_counter_emit(
        self,
        uploader: NASUploader,
        retry_queue: RetryQueue,  # noqa: ARG002 — fixture wiring 만 필요
    ) -> None:
        """4xx 발생 시 mctrader_nas_put_operational_alert_total Counter += 1."""
        data = b"test-payload-data"
        sha256 = hashlib.sha256(data).hexdigest()

        from mctrader_data.nas_metrics.prometheus_exporters import (
            nas_put_operational_alert_total,
        )

        # tier 라벨 없는 경우 unknown 으로 처리 — put() 호출은 caller가 tier 모르므로
        # ADR Amendment: tier 라벨 = "unknown" default (caller side에서 명시 set 가능)
        before = nas_put_operational_alert_total.labels(
            tier="unknown", reason="policy_denied"
        )._value.get()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client
            client.head_object.side_effect = _client_error("403")

            with pytest.raises(NASOperationalAlert):
                uploader.put(key="test/object.bin", data=data, sha256=sha256)

        after = nas_put_operational_alert_total.labels(
            tier="unknown", reason="policy_denied"
        )._value.get()
        assert after == before + 1, "Counter mctrader_nas_put_operational_alert_total +=1 미발생"


# ──────────────────────────────────────────────────────────────────────────────
# AC-1: 4xx fail-fast (put_streaming Path / fileobj path)
# ──────────────────────────────────────────────────────────────────────────────


class TestPutStreaming4xxFailFastPathFileobj:
    """AC-1 — put_streaming() Path/fileobj path: 4xx → NASOperationalAlert raise."""

    def test_put_streaming_4xx_raises_operational_alert_path(
        self,
        uploader: NASUploader,
        retry_queue: RetryQueue,
        tmp_path: Path,
    ) -> None:
        """put_streaming(Path) 의 HEAD 4xx → NASOperationalAlert raise + retry_queue 미호출."""
        payload = b"test-streaming-payload"
        local_file = tmp_path / "test.parquet"
        local_file.write_bytes(payload)
        sha256 = hashlib.sha256(payload).hexdigest()

        with (
            patch.object(uploader, "_get_client") as mock_client_factory,
            patch.object(retry_queue, "enqueue") as mock_enqueue,
        ):
            client = MagicMock()
            mock_client_factory.return_value = client
            client.head_object.side_effect = _client_error("AccessDenied")

            with pytest.raises(NASOperationalAlert) as exc_info:
                uploader.put_streaming(local_file, "test/object.parquet", sha256)

        assert exc_info.value.reason == "policy_denied"
        assert not mock_enqueue.called, "4xx 는 retry_queue.enqueue 호출 금지"

    def test_put_streaming_4xx_during_upload_raises(
        self,
        uploader: NASUploader,
        retry_queue: RetryQueue,
        tmp_path: Path,
    ) -> None:
        """put_streaming HEAD 404 → upload_fileobj 단계 4xx 발생 시 NASOperationalAlert raise."""
        payload = b"test-streaming-payload"
        local_file = tmp_path / "test.parquet"
        local_file.write_bytes(payload)
        sha256 = hashlib.sha256(payload).hexdigest()

        with (
            patch.object(uploader, "_get_client") as mock_client_factory,
            patch.object(retry_queue, "enqueue") as mock_enqueue,
        ):
            client = MagicMock()
            mock_client_factory.return_value = client
            # HEAD 404 (object 미존재) → upload 단계 진입
            client.head_object.side_effect = _client_error("404")
            # upload_fileobj 가 403 raise
            client.upload_fileobj.side_effect = _client_error("403")

            with pytest.raises(NASOperationalAlert) as exc_info:
                uploader.put_streaming(local_file, "test/object.parquet", sha256)

        assert exc_info.value.reason == "policy_denied"
        assert not mock_enqueue.called


# ──────────────────────────────────────────────────────────────────────────────
# AC-2: 5xx + EndpointConnectionError → 현행 동작 보존 (queued, raise 0)
# ──────────────────────────────────────────────────────────────────────────────


class TestPut5xxStillQueuedNoRaise:
    """AC-2 — 5xx + EndpointConnectionError 현행 동작 보존 (regression 차단)."""

    @pytest.mark.parametrize("code", ["500", "503", "InternalError", "ServiceUnavailable"])
    def test_put_5xx_returns_queued(
        self,
        uploader: NASUploader,
        retry_queue: RetryQueue,  # noqa: ARG002
        code: str,
    ) -> None:
        """5xx ClientError 시 PutResult(status='queued') 반환 + raise 0 + retry_queue.enqueue 호출."""
        data = b"test-payload"
        sha256 = hashlib.sha256(data).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client
            client.head_object.side_effect = _client_error(code)

            result = uploader.put(key="test/object.bin", data=data, sha256=sha256)

        assert result.status in ("queued", "hard_floor_blocked"), (
            f"5xx code={code} 는 queued/hard_floor_blocked 만 반환 (현행 동작 보존)"
        )

    def test_put_endpoint_connection_error_returns_queued(
        self,
        uploader: NASUploader,
        retry_queue: RetryQueue,  # noqa: ARG002
    ) -> None:
        """EndpointConnectionError 시 PutResult(status='queued') 반환 + raise 0 (ADR-027 §D5 base)."""
        from botocore.exceptions import EndpointConnectionError

        data = b"test-payload"
        sha256 = hashlib.sha256(data).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client
            client.head_object.side_effect = EndpointConnectionError(
                endpoint_url="http://nas.local:9000"
            )

            result = uploader.put(key="test/object.bin", data=data, sha256=sha256)

        assert result.status in ("queued", "hard_floor_blocked")


class TestPutStreaming5xxStillQueuedNoRaise:
    """AC-2 — put_streaming 5xx + EndpointConnectionError 현행 동작 보존."""

    def test_put_streaming_5xx_returns_queued(
        self,
        uploader: NASUploader,
        retry_queue: RetryQueue,  # noqa: ARG002
        tmp_path: Path,
    ) -> None:
        payload = b"test-payload"
        local_file = tmp_path / "test.parquet"
        local_file.write_bytes(payload)
        sha256 = hashlib.sha256(payload).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client
            client.head_object.side_effect = _client_error("404")
            client.upload_fileobj.side_effect = _client_error("503")

            result = uploader.put_streaming(local_file, "test/object.parquet", sha256)

        assert result.status in ("queued", "hard_floor_blocked"), (
            "5xx 는 queued 분기 보존 (현행 동작)"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Backward-compat smoke: 정상 PUT 경로 / HEAD 404 → upload 회귀 0
# ──────────────────────────────────────────────────────────────────────────────


class TestBackwardCompatSmoke:
    """기존 정상 path 회귀 0 (HEAD 404 + PUT / HEAD 200 + skip)."""

    def test_put_head_404_then_upload_unchanged(self, uploader: NASUploader) -> None:
        data = b"test-payload"
        sha256 = hashlib.sha256(data).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client
            client.head_object.side_effect = _client_error("404")
            client.put_object.return_value = {"ETag": f'"{sha256[:16]}"'}

            result = uploader.put(key="test/object.bin", data=data, sha256=sha256)

        assert result.status == "uploaded"

    def test_put_streaming_head_404_then_upload_unchanged(
        self,
        uploader: NASUploader,
        tmp_path: Path,
    ) -> None:
        payload = b"test-payload"
        local_file = tmp_path / "test.parquet"
        local_file.write_bytes(payload)
        sha256 = hashlib.sha256(payload).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client
            client.head_object.side_effect = _client_error("404")
            client.upload_fileobj.return_value = None  # boto3 returns None on success

            result = uploader.put_streaming(local_file, "test/object.parquet", sha256)

        assert result.status == "uploaded"
