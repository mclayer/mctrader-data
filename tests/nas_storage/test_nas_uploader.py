"""test_nas_uploader.py — P0 TDD tests for NASUploader.

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253

Test Contract §8 (TestContractArchitectAgent):
- test_head_then_put_idempotency: HEAD 200+ETag match → SKIP / HEAD 404 → PUT (mock)
- test_log_redacts_credentials: NAS endpoint URL / boto3 access_key / secret_key 가 log output에 평문 노출 0 (FIX#1 F7)
- test_put_failure_propagates_to_retry_queue: NAS unreachable → retry_queue.enqueue (mock)
- test_endpoint_unreachable_returns_queued_no_raise: NAS unreachable 시 raise 0 (ADR-027 D5 invariant)
- test_idempotent_skip_on_match: same key + same sha256 → skip (PutResult status="skipped_idempotent")
- test_conflict_raise_on_mismatch: sha256 mismatch → ConditionalWriteConflict raise
"""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mctrader_data.nas_storage.nas_uploader import (
    ConditionalWriteConflict,
    NASUploader,
    PutResult,
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


class TestHeadThenPutIdempotency:
    """§8.2 Invariant: HEAD-then-PUT idempotency."""

    def test_head_404_then_put(self, uploader: NASUploader) -> None:
        """HEAD 404 → PUT 수행 (신규 object). PutResult status='uploaded'."""
        data = b"test-payload-data"
        sha256 = hashlib.sha256(data).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client

            from botocore.exceptions import ClientError
            client.head_object.side_effect = ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
            client.put_object.return_value = {"ETag": f'"{sha256[:16]}"'}

            result = uploader.put(key="test/object.bin", data=data, sha256=sha256)

        assert result.status == "uploaded"
        assert client.put_object.called

    def test_head_200_etag_match_skip(self, uploader: NASUploader) -> None:
        """HEAD 200 + ETag match → SKIP. PutResult status='skipped_idempotent'. PUT 미호출."""
        data = b"test-payload-data"
        sha256 = hashlib.sha256(data).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client

            client.head_object.return_value = {
                "ETag": f'"{sha256}"',
                "Metadata": {"sha256": sha256},
                "ContentLength": len(data),
            }

            result = uploader.put(key="test/object.bin", data=data, sha256=sha256)

        assert result.status == "skipped_idempotent"
        assert not client.put_object.called

    def test_head_200_sha256_mismatch_raises(self, uploader: NASUploader) -> None:
        """HEAD 200 + sha256 mismatch → ConditionalWriteConflict raise. §8.2 invariant."""
        data = b"new-data"
        sha256 = hashlib.sha256(data).hexdigest()
        existing_sha256 = hashlib.sha256(b"old-data").hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client

            client.head_object.return_value = {
                "ETag": f'"{existing_sha256}"',
                "Metadata": {"sha256": existing_sha256},
                "ContentLength": 8,
            }

            with pytest.raises(ConditionalWriteConflict):
                uploader.put(key="test/object.bin", data=data, sha256=sha256)

        assert not client.put_object.called


class TestLogRedactsCredentials:
    """FIX#1 F7: credential masking invariant. §8.2 박제."""

    def test_log_redacts_credentials(
        self, caplog: pytest.LogCaptureFixture, retry_queue: RetryQueue
    ) -> None:
        """NAS endpoint URL / access_key / secret_key 가 log output에 평문 노출 0."""
        access_key = "SUPER_SECRET_ACCESS_KEY_1234"
        secret_key = "SUPER_SECRET_SECRET_KEY_5678"
        endpoint = "http://nas-private.local:9000"

        uploader = NASUploader(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket="test-bucket",
            retry_queue=retry_queue,
        )

        data = b"payload"
        sha256 = hashlib.sha256(data).hexdigest()

        with caplog.at_level(logging.DEBUG, logger="mctrader_data.nas_storage.nas_uploader"):
            with patch.object(uploader, "_get_client") as mock_client_factory:
                client = MagicMock()
                mock_client_factory.return_value = client

                from botocore.exceptions import ClientError
                client.head_object.side_effect = ClientError(
                    {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
                )
                client.put_object.return_value = {"ETag": f'"{sha256[:16]}"'}

                uploader.put(key="test/cred-test.bin", data=data, sha256=sha256)

        full_log = "\n".join(caplog.messages)
        assert access_key not in full_log, "access_key must not appear in logs"
        assert secret_key not in full_log, "secret_key must not appear in logs"
        assert f"{access_key}@" not in full_log, "credential-embedded URL must not appear in logs"
        assert f"{secret_key}@" not in full_log, "credential-embedded URL must not appear in logs"


class TestPutFailurePropagatesRetryQueue:
    """AC-2: NAS unreachable → retry_queue.enqueue. ADR-027 D5 invariant (raise 0)."""

    def test_endpoint_unreachable_enqueues_and_returns_queued(
        self, uploader: NASUploader, retry_queue: RetryQueue
    ) -> None:
        """EndpointConnectionError → retry_queue.enqueue() → status='queued', raise 0."""
        data = b"payload-to-queue"
        sha256 = hashlib.sha256(data).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client

            from botocore.exceptions import EndpointConnectionError
            client.head_object.side_effect = EndpointConnectionError(
                endpoint_url="http://nas.local:9000"
            )

            result = uploader.put(key="test/queued-object.bin", data=data, sha256=sha256)

        assert result.status == "queued"
        assert retry_queue.depth() == 1

    def test_endpoint_unreachable_does_not_raise(
        self, uploader: NASUploader
    ) -> None:
        """§8.2 invariant: hot path 무영향 — put() 가 NAS unreachable 시 절대 raise 0."""
        data = b"payload"
        sha256 = hashlib.sha256(data).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client

            from botocore.exceptions import EndpointConnectionError
            client.head_object.side_effect = EndpointConnectionError(
                endpoint_url="http://nas.local:9000"
            )

            try:
                uploader.put(key="test/no-raise.bin", data=data, sha256=sha256)
            except Exception as exc:
                pytest.fail(f"put() raised unexpectedly: {exc!r}")
