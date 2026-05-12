"""nas_uploader.py — Production-grade NAS MinIO uploader with HEAD-then-PUT idempotency.

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253
ADR: ADR-027 D5 (NAS unreachable failure mode) / D2 (HTTP + 4중 mitigation)

Design decisions (§6.2.1 Change Plan 박제):
- HEAD-then-PUT idempotency (S4):
  - HEAD 200 + sha256 match → SKIP (idempotent, status='skipped_idempotent')
  - HEAD 404 → PUT (신규 object, status='uploaded')
  - HEAD 200 + sha256 mismatch → ConditionalWriteConflict raise (silent overwrite 0)
  - NAS unreachable (EndpointConnectionError) → retry_queue.enqueue() → status='queued' (raise 0)
- boto3 client (S3 API): endpoint = NAS_MINIO_ENDPOINT, creds = NAS_MINIO_ACCESS_KEY/SECRET_KEY
- Env namespace: NAS_MINIO_* (기존 MINIO_ENDPOINT hot path 침범 0, EC-1 박제)
- credential masking obligation: log 에 access_key / secret_key 평문 노출 0 (FIX#1 F7)
- ADR-027 D5 invariant: put() 가 NAS unreachable 시 절대 raise 0 → hot path 무영향

SecurityArch (§6.3):
- log 출력 시 endpoint URL masking: host:port 만 포함 (auth 정보 embedded URL 금지)
- fail reason label = generic enum (raw boto3 exception message embed 금지)
- retry queue persisted data: credential 0 (data only)
"""
from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

log = logging.getLogger(__name__)


class ConditionalWriteConflict(Exception):
    """HEAD 200 + sha256 mismatch — silent overwrite 금지, caller 결정 의무.

    §6.2.1 박제: forward-only invariant (ADR-009 §D12.2) 보장.
    """


@dataclass(frozen=True)
class PutResult:
    """put() 반환값."""

    status: Literal["uploaded", "skipped_idempotent", "queued"]
    object_etag: str = ""
    latency_ms: float = 0.0


class NASUploader:
    """Production-grade NAS MinIO uploader.

    Responsibilities:
    - HEAD-then-PUT idempotency (S4): same key + same sha256 → skip; mismatch → conflict error.
    - retry_queue integration: NAS unreachable → enqueue + return queued status (no raise to caller).
    - Prometheus 4종 metric emit per put() call.

    Env: NAS_MINIO_ENDPOINT / NAS_MINIO_ACCESS_KEY / NAS_MINIO_SECRET_KEY (별 namespace,
    기존 MINIO_ENDPOINT 침범 0, EC-1 박제).

    ADR-027 D5 invariant:
    - NAS unreachable → retry_queue.enqueue() → status='queued' (raise 0)
    - hot path L3 compaction 차단 0
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str = "mctrader-market",  # ADR-027 D1 정합
        retry_queue=None,  # RetryQueue | None (순환 import 방지)
        metrics=None,  # PrometheusExporter | None
    ) -> None:
        self._endpoint = endpoint
        # credentials — NEVER logged (FIX#1 F7)
        self._access_key = access_key
        self._secret_key = secret_key
        self.bucket = bucket
        self._retry_queue = retry_queue
        self._metrics = metrics
        self.__client = None  # lazy boto3 client

    def _get_client(self):
        """Lazy boto3 client creation. credentials 는 절대 log 출력 금지."""
        if self.__client is None:
            self.__client = boto3.client(
                "s3",
                endpoint_url=self._endpoint,
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
                config=Config(
                    retries={"max_attempts": 1, "mode": "standard"},
                    connect_timeout=10,
                    read_timeout=120,
                ),
            )
        return self.__client

    def put(
        self, key: str, data: bytes | Path, *, sha256: str | None = None
    ) -> PutResult:
        """HEAD-then-PUT idempotency.

        Returns PutResult(
            status=Literal['uploaded', 'skipped_idempotent', 'queued'],
            object_etag,
            latency_ms
        ).

        AC-1: HEAD 검사 → existing object 의 sha256 match → skip + idempotent.
        AC-1: mismatch 시 ConditionalWriteConflict raise (caller 결정).
        AC-2: EndpointConnectionError → retry_queue.enqueue() → status='queued' (raise 0).
        ADR-027 D5: put() 가 NAS unreachable 시 raise 0 → hot path 무영향.
        """
        t_start = time.perf_counter()

        # sha256 계산 (미제공 시)
        if sha256 is None:
            raw = data if isinstance(data, bytes) else Path(data).read_bytes()
            sha256 = hashlib.sha256(raw).hexdigest()

        # data bytes 확보
        raw_data = data.read_bytes() if isinstance(data, Path) else data

        # NAS unreachable 처리 wrapper
        try:
            result = self._put_with_idempotency(key=key, data=raw_data, sha256=sha256)
        except EndpointConnectionError:
            # ADR-027 D5 invariant: raise 0, retry_queue.enqueue()
            # log masking: endpoint host:port 만 (auth 정보 0)
            safe_endpoint = _mask_endpoint(self._endpoint)
            log.warning("[nas_uploader] endpoint unreachable endpoint=%s key=%s", safe_endpoint, key)
            if self._retry_queue is not None:
                self._retry_queue.enqueue(key=key, data=raw_data, sha256=sha256)
            if self._metrics:
                latency_s = time.perf_counter() - t_start
                self._metrics.emit_fail(
                    bucket=self.bucket, reason="endpoint_unreachable", latency_s=latency_s
                )
            return PutResult(
                status="queued",
                latency_ms=(time.perf_counter() - t_start) * 1000,
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            safe_endpoint = _mask_endpoint(self._endpoint)
            if code in ("401", "403", "InvalidAccessKeyId", "AccessDenied"):
                reason = "auth_failed"
            elif code in ("QuotaExceeded", "StorageClassNotSupported"):
                reason = "quota_exceeded"
            else:
                reason = "unknown"
            log.warning(
                "[nas_uploader] client error endpoint=%s key=%s code=%s",
                safe_endpoint, key, code,
            )
            if self._retry_queue is not None:
                self._retry_queue.enqueue(key=key, data=raw_data, sha256=sha256)
            if self._metrics:
                latency_s = time.perf_counter() - t_start
                self._metrics.emit_fail(bucket=self.bucket, reason=reason, latency_s=latency_s)
            return PutResult(
                status="queued",
                latency_ms=(time.perf_counter() - t_start) * 1000,
            )

        latency_ms = (time.perf_counter() - t_start) * 1000

        # Prometheus metric emit
        if self._metrics:
            latency_s = latency_ms / 1000
            if result.status in ("uploaded", "skipped_idempotent"):
                self._metrics.emit_success(bucket=self.bucket, latency_s=latency_s)

        return PutResult(
            status=result.status,
            object_etag=result.object_etag,
            latency_ms=latency_ms,
        )

    def _put_with_idempotency(
        self, key: str, data: bytes, sha256: str
    ) -> PutResult:
        """HEAD 검사 후 PUT or SKIP.

        ConditionalWriteConflict raise 시 caller(put()) 에서 catch 후 결정.
        EndpointConnectionError → caller(put()) 에서 retry_queue.enqueue().
        """
        client = self._get_client()
        t_head = time.perf_counter()

        try:
            head_response = client.head_object(Bucket=self.bucket, Key=key)
            head_latency_s = time.perf_counter() - t_head

            # HEAD 성공 (200) → sha256 비교
            if self._metrics:
                self._metrics.emit_head(bucket=self.bucket, latency_s=head_latency_s)

            existing_sha256 = (
                head_response.get("Metadata", {}).get("sha256")
                or head_response.get("ETag", "").strip('"')
            )

            if existing_sha256 == sha256:
                log.info(
                    "[nas_uploader] idempotent skip key=%s sha256=%s…",
                    key, sha256[:8],
                )
                return PutResult(
                    status="skipped_idempotent",
                    object_etag=existing_sha256,
                )
            else:
                # sha256 mismatch → ConditionalWriteConflict raise (ADR-009 forward-only)
                safe_endpoint = _mask_endpoint(self._endpoint)
                log.error(
                    "[nas_uploader] sha256 mismatch endpoint=%s key=%s "
                    "existing=%.8s… new=%.8s…",
                    safe_endpoint, key, existing_sha256, sha256,
                )
                raise ConditionalWriteConflict(
                    f"key={key} existing_sha256={existing_sha256[:8]}… new_sha256={sha256[:8]}…"
                )

        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "404":
                # HEAD 404 → object 없음 → PUT
                if self._metrics:
                    head_latency_s = time.perf_counter() - t_head
                    self._metrics.emit_head(bucket=self.bucket, latency_s=head_latency_s)
            else:
                raise

        # PUT (HEAD 404 path)
        response = client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            Metadata={"sha256": sha256},
        )
        etag = response.get("ETag", "").strip('"')
        log.info("[nas_uploader] uploaded key=%s etag=%s bytes=%d", key, etag, len(data))

        return PutResult(status="uploaded", object_etag=etag)


def _mask_endpoint(endpoint: str) -> str:
    """endpoint URL 에서 인증 정보 제거 — host:port 만 반환.

    FIX#1 F7: log 출력 시 credential masking.
    e.g., 'http://ACCESS:SECRET@nas.local:9000' → 'http://nas.local:9000'
    """
    try:
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(endpoint)
        # netloc 에서 userinfo 제거
        netloc = parsed.hostname or ""
        if parsed.port:
            netloc = f"{netloc}:{parsed.port}"
        masked = urlunparse((parsed.scheme, netloc, parsed.path, "", "", ""))
        return masked
    except Exception:
        return "[masked]"
