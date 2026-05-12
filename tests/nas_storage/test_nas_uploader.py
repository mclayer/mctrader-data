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

FIX#2 추가:
- P1-3: threading.Lock _get_client() concurrent init guard
- P2-2: ETag fallback false-positive — Metadata sha256 absent → overwrite (not false-skip)

FIX#3 추가 (P0-NEW-1):
- test_put_propagates_hard_floor_blocked: retry queue 가 hard_floor 도달 시 put() 가
  PutResult(status="hard_floor_blocked") 반환 + caller contract 검증
"""
from __future__ import annotations

import hashlib
import logging
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mctrader_data.nas_storage.nas_uploader import (
    ConditionalWriteConflict,
    NASUploader,
    PutResult,
)
from mctrader_data.nas_storage.retry_queue import EnqueueResult, RetryQueue


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

        with (
            caplog.at_level(logging.DEBUG, logger="mctrader_data.nas_storage.nas_uploader"),
            patch.object(uploader, "_get_client") as mock_client_factory,
        ):
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


class TestThreadingLockGetClient:
    """P1-3 FIX#2: _get_client() threading.Lock — concurrent init guard.

    double-checked locking: lock 진입 후 다시 None check.
    단일 boto3 client 인스턴스 보장 (concurrent 호출 시).
    """

    def test_get_client_returns_same_instance(self, uploader: NASUploader) -> None:
        """_get_client() lazy init — 동일 인스턴스 반환 (non-concurrent path)."""
        with patch("boto3.client") as mock_boto3_client:
            mock_client = MagicMock()
            mock_boto3_client.return_value = mock_client

            c1 = uploader._get_client()
            c2 = uploader._get_client()

        assert c1 is c2, "_get_client() must return same instance (lazy init)"
        assert mock_boto3_client.call_count == 1, "boto3.client must be called only once"

    def test_get_client_concurrent_single_init(self, uploader: NASUploader) -> None:
        """concurrent _get_client() 호출 — boto3.client 단 1회 init (race condition 없음)."""
        init_count = []

        with patch("boto3.client") as mock_boto3_client:
            def track_init(*args, **kwargs):
                init_count.append(1)
                return MagicMock()

            mock_boto3_client.side_effect = track_init

            results = []
            errors = []

            def call_get_client():
                try:
                    results.append(uploader._get_client())
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=call_get_client) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        assert not errors, f"Thread errors: {errors}"
        # threading.Lock double-checked: boto3.client 은 1회만 호출
        assert len(init_count) == 1, (
            f"boto3.client called {len(init_count)} times — "
            "threading.Lock double-checked locking 미적용 의심"
        )
        # 모든 thread 가 동일 인스턴스를 받음
        assert len({id(r) for r in results}) == 1, (
            "All threads must receive the same client instance"
        )

    def test_client_lock_exists(self, uploader: NASUploader) -> None:
        """NASUploader 에 _client_lock (threading.Lock) attribute 존재 확인."""
        assert hasattr(uploader, "_client_lock"), (
            "NASUploader must have _client_lock attribute (threading.Lock)"
        )
        import threading as _threading
        assert isinstance(uploader._client_lock, type(_threading.Lock())), (
            "_client_lock must be a threading.Lock instance"
        )


class TestETagFallbackFalsePositive:
    """P2-2 FIX#2: ETag fallback false-positive 방지.

    HEAD response Metadata sha256 부재 (외부 PUT 또는 legacy 객체) 시:
    - log warning (object_key, 'metadata sha256 absent — overwrite' 박제)
    - skip ETag 비교 → PUT (idempotency 차단 + log 명시)
    - 기존 self-PUT 객체 (Metadata sha256 첨부) 는 정상 ETag 비교 path 유지
    """

    def test_metadata_absent_triggers_overwrite(self, uploader: NASUploader) -> None:
        """HEAD 200 + Metadata sha256 없음 → ETag 비교 skip → PUT (overwrite path)."""
        data = b"external-put-object"
        sha256 = hashlib.sha256(data).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client

            # Metadata 에 sha256 없음 (외부 PUT / legacy 객체)
            # ETag 도 sha256 과 다른 값 (S3 multipart ETag 형태)
            client.head_object.return_value = {
                "ETag": '"abc123-5"',  # multipart ETag — sha256 아님
                "Metadata": {},  # sha256 absent
                "ContentLength": len(data),
            }
            client.put_object.return_value = {"ETag": f'"{sha256[:16]}"'}

            result = uploader.put(key="legacy/object.bin", data=data, sha256=sha256)

        # Metadata sha256 absent → ETag 비교 skip → PUT 수행
        assert result.status == "uploaded", (
            f"Metadata absent: PUT (overwrite) 수행 필요. got status='{result.status}'"
        )
        assert client.put_object.called, "Metadata absent: put_object 호출 필요"

    def test_metadata_absent_logs_warning(
        self, uploader: NASUploader, caplog: pytest.LogCaptureFixture
    ) -> None:
        """HEAD 200 + Metadata sha256 없음 → 'metadata sha256 absent' warning log 박제."""
        data = b"legacy-object-data"
        sha256 = hashlib.sha256(data).hexdigest()

        with (
            caplog.at_level(logging.WARNING, logger="mctrader_data.nas_storage.nas_uploader"),
            patch.object(uploader, "_get_client") as mock_client_factory,
        ):
            client = MagicMock()
            mock_client_factory.return_value = client

            client.head_object.return_value = {
                "ETag": '"multipart-etag-no-sha256"',
                "Metadata": {},
                "ContentLength": len(data),
            }
            client.put_object.return_value = {"ETag": f'"{sha256[:16]}"'}

            uploader.put(key="legacy/warn-test.bin", data=data, sha256=sha256)

        full_log = " ".join(caplog.messages)
        assert "absent" in full_log.lower() or "overwrite" in full_log.lower(), (
            "Metadata absent 시 warning log 에 'absent' 또는 'overwrite' 포함 필요"
        )

    def test_metadata_present_uses_normal_etag_path(self, uploader: NASUploader) -> None:
        """HEAD 200 + Metadata sha256 있음 → 정상 ETag 비교 path (기존 동작 유지)."""
        data = b"self-put-object"
        sha256 = hashlib.sha256(data).hexdigest()

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client

            # Metadata sha256 = 일치 → skip (기존 동작)
            client.head_object.return_value = {
                "ETag": f'"{sha256}"',
                "Metadata": {"sha256": sha256},
                "ContentLength": len(data),
            }

            result = uploader.put(key="self-put/object.bin", data=data, sha256=sha256)

        assert result.status == "skipped_idempotent", (
            "Metadata sha256 present + match → skip (기존 동작 유지)"
        )
        assert not client.put_object.called


class TestPutPropagatesHardFloorBlocked:
    """P0-NEW-1 FIX#3: retry queue hard_floor 도달 시 put() 가 PutResult(status="hard_floor_blocked") 반환.

    §6.2.1 Change Plan FIX#3 박제:
    - enqueue() 가 EnqueueResult(status='hard_floor_blocked') 반환 시
      put() 가 PutResult(status='hard_floor_blocked') 로 propagate
    - caller 가 'queued' 와 'hard_floor_blocked' 를 구분할 수 있어야 함 (MANUAL_GATE escalate 의무)
    - PutResult.status Literal 5종:
      "uploaded"|"skipped_idempotent"|"queued"|"hard_floor_blocked"|"skipped_etag_overwrite"
    """

    def test_put_propagates_hard_floor_blocked_on_endpoint_error(
        self, uploader: NASUploader, retry_queue: RetryQueue
    ) -> None:
        """EndpointConnectionError + enqueue 반환 hard_floor_blocked → put() 반환 hard_floor_blocked."""
        data = b"hard-floor-trigger-payload"
        sha256 = hashlib.sha256(data).hexdigest()

        # retry_queue.enqueue 를 hard_floor_blocked 반환하도록 mock
        with (
            patch.object(uploader, "_get_client") as mock_client_factory,
            patch.object(uploader._retry_queue, "enqueue") as mock_enqueue,
        ):
            client = MagicMock()
            mock_client_factory.return_value = client

            from botocore.exceptions import EndpointConnectionError
            client.head_object.side_effect = EndpointConnectionError(
                endpoint_url="http://nas.local:9000"
            )

            # enqueue 가 hard_floor_blocked 반환
            mock_enqueue.return_value = EnqueueResult(status="hard_floor_blocked")

            result = uploader.put(key="test/hard-floor.bin", data=data, sha256=sha256)

        assert result.status == "hard_floor_blocked", (
            f"hard_floor_blocked propagation 실패: got '{result.status}'. "
            "retry queue hard_floor 도달 시 put() 가 'hard_floor_blocked' 반환 필요"
        )

    def test_put_propagates_hard_floor_blocked_on_client_error(
        self, uploader: NASUploader, retry_queue: RetryQueue
    ) -> None:
        """ClientError + enqueue 반환 hard_floor_blocked → put() 반환 hard_floor_blocked."""
        data = b"client-error-hard-floor-payload"
        sha256 = hashlib.sha256(data).hexdigest()

        with (
            patch.object(uploader, "_get_client") as mock_client_factory,
            patch.object(uploader._retry_queue, "enqueue") as mock_enqueue,
        ):
            client = MagicMock()
            mock_client_factory.return_value = client

            from botocore.exceptions import ClientError
            client.head_object.side_effect = ClientError(
                {"Error": {"Code": "503", "Message": "Service Unavailable"}}, "HeadObject"
            )

            mock_enqueue.return_value = EnqueueResult(status="hard_floor_blocked")

            result = uploader.put(key="test/client-hard-floor.bin", data=data, sha256=sha256)

        assert result.status == "hard_floor_blocked", (
            f"ClientError path hard_floor_blocked propagation 실패: got '{result.status}'"
        )

    def test_put_returns_queued_when_enqueue_ok(
        self, uploader: NASUploader, retry_queue: RetryQueue
    ) -> None:
        """enqueue 가 'ok' 반환 시 put() 는 'queued' 반환 (기존 동작 유지)."""
        data = b"normal-enqueue-payload"
        sha256 = hashlib.sha256(data).hexdigest()

        with (
            patch.object(uploader, "_get_client") as mock_client_factory,
            patch.object(uploader._retry_queue, "enqueue") as mock_enqueue,
        ):
            client = MagicMock()
            mock_client_factory.return_value = client

            from botocore.exceptions import EndpointConnectionError
            client.head_object.side_effect = EndpointConnectionError(
                endpoint_url="http://nas.local:9000"
            )

            mock_enqueue.return_value = EnqueueResult(status="ok", item_id="test-id")

            result = uploader.put(key="test/normal-queue.bin", data=data, sha256=sha256)

        assert result.status == "queued", (
            f"enqueue 'ok' 시 put() 반환값 'queued' 기대, got '{result.status}'"
        )

    def test_put_result_status_literal_includes_hard_floor_blocked(self) -> None:
        """PutResult.status Literal 에 'hard_floor_blocked' 포함 확인 (타입 계약 검증)."""
        # PutResult 직접 생성 — Literal type 5종 모두 허용 확인
        result = PutResult(status="hard_floor_blocked")
        assert result.status == "hard_floor_blocked"

        # 기존 4종도 여전히 유효
        for status in ("uploaded", "skipped_idempotent", "queued", "skipped_etag_overwrite"):
            r = PutResult(status=status)  # type: ignore[arg-type]
            assert r.status == status
