"""nas_uploader.py — Production-grade NAS MinIO uploader with HEAD-then-PUT idempotency.

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253
ADR: ADR-027 D5 (NAS unreachable failure mode) / D2 (HTTP + 4중 mitigation)

Design decisions (§6.2.1 Change Plan 박제, FIX#3 갱신):
- HEAD-then-PUT idempotency (S4):
  - HEAD 200 + Metadata sha256 present + match → SKIP (idempotent, status='skipped_idempotent')
  - HEAD 200 + Metadata sha256 absent (외부 PUT / legacy 객체) → log warning + PUT overwrite (P2-2 FIX#2)
  - HEAD 200 + sha256 mismatch → ConditionalWriteConflict raise (silent overwrite 0)
  - HEAD 404 → PUT (신규 object, status='uploaded')
  - NAS unreachable (EndpointConnectionError) → suppress_enqueue 분기:
    - suppress_enqueue=False (기본값): retry_queue.enqueue() → EnqueueResult 분기:
      - enqueued ('ok') → status='queued' (raise 0)
      - hard_floor_blocked → status='hard_floor_blocked' (raise 0, caller MANUAL_GATE 의무)
    - suppress_enqueue=True (drain 호출 시): EndpointConnectionError raise (drain 측 catch 후 retain)
- boto3 client (S3 API): endpoint = NAS_MINIO_ENDPOINT, creds = NAS_MINIO_ACCESS_KEY/SECRET_KEY
- Env namespace: NAS_MINIO_* (기존 MINIO_ENDPOINT hot path 침범 0, EC-1 박제)
- credential masking obligation: log 에 access_key / secret_key 평문 노출 0 (FIX#1 F7)
- ADR-027 D5 invariant: put() 가 NAS unreachable 시 (suppress_enqueue=False) 절대 raise 0 → hot path 무영향
- threading.Lock _get_client(): double-checked locking — concurrent init 시 단 1회 boto3.client() 호출 (P1-3 FIX#2)
- PutResult.status 5종 (FIX#3 P0-NEW-1):
  "uploaded" | "skipped_idempotent" | "queued" | "hard_floor_blocked" | "skipped_etag_overwrite"

MCT-163 F3 (D1=B, D3=A):
- put_streaming(local_path_or_fileobj, nas_key, sha256): boto3 upload_fileobj + TransferConfig
  - multipart idiomatic (D1=B): chunk-wise upload, 메모리 전체 로드 0 (INV-4)
  - backward compat: 기존 put(key, data, sha256) signature 보존 (INV-2)
  - caller sha256 SSOT: caller가 단일 hash 계산 후 주입 (D2=A, INV-3)
  - DualWriter.write() 내부에서 put → put_streaming 교체 (read_bytes 0)

SecurityArch (§6.3):
- log 출력 시 endpoint URL masking: host:port 만 포함 (auth 정보 embedded URL 금지)
- fail reason label = generic enum (raw boto3 exception message embed 금지)
- retry queue persisted data: credential 0 (data only)
"""
from __future__ import annotations

import hashlib
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Literal

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

log = logging.getLogger(__name__)


class ConditionalWriteConflict(Exception):
    """HEAD 200 + sha256 mismatch — silent overwrite 금지, caller 결정 의무.

    §6.2.1 박제: forward-only invariant (ADR-009 §D12.2) 보장.
    """


class NASOperationalAlert(Exception):
    """NAS PUT 4xx (auth/policy/quota 영구 오류) — silent fallback 차단, 운영자 개입 의무.

    ADR-027 INCIDENT-2026-05-17 amendment (§D5 amend) 박제:
    - 4xx ClientError 발견 시 retry_queue 흡수 금지 + 본 exception raise + Counter +=1.
    - 운영자 개입 의무 (key rotation / IAM policy / bucket 재생성). retry_queue 흡수 시 자동
      해소 0 / 무한 backlog 누적만 발생. silent skip pathology 와 동형 (ADR-027 Amendment 1/2 sibling).

    Attributes:
        code: boto3 ClientError 의 `Error.Code` 원본 (e.g., '403', 'AccessDenied').
        reason: bounded low cardinality 분류 (auth_failed / policy_denied / quota_exceeded / bucket_missing).
        tier: NAS object tier label (L1/L2/L3/unknown — caller side 명시 set, default unknown).
        nas_key: NAS object key (debugging trail).
    """

    def __init__(self, code: str, reason: str, tier: str, nas_key: str, msg: str = "") -> None:
        self.code = code
        self.reason = reason
        self.tier = tier
        self.nas_key = nas_key
        detail = msg or f"code={code} reason={reason} tier={tier} key={nas_key}"
        super().__init__(f"NAS PUT operational alert: {detail}")


# INCIDENT-2026-05-17 amendment: 4xx code → reason matrix (bounded low cardinality)
# 본 매트릭스 = ADR-027 INCIDENT-2026-05-17 amendment §Decision 1 의 박제 SSOT.
_FAIL_FAST_CODE_TO_REASON: dict[str, str] = {
    # auth 영역 (보통 자동 해소 0, key rotation 의무)
    "401": "auth_failed",
    "InvalidAccessKeyId": "auth_failed",
    "SignatureDoesNotMatch": "auth_failed",
    # policy 영역 (IAM/Bucket policy denial)
    "403": "policy_denied",
    "AccessDenied": "policy_denied",
    # bucket 부재 (운영자 bucket 재생성 의무)
    "NoSuchBucket": "bucket_missing",
    # quota / storage class (capacity/plan 변경 의무)
    "QuotaExceeded": "quota_exceeded",
    "StorageClassNotSupported": "quota_exceeded",
}


def _classify_4xx(code: str) -> str | None:
    """4xx fail-fast 분류 lookup. None 반환 = 4xx 비대상 (5xx/일반 → 기존 queued 분기)."""
    return _FAIL_FAST_CODE_TO_REASON.get(code)


@dataclass(frozen=True)
class PutResult:
    """put() 반환값.

    status 5종 (FIX#3 P0-NEW-1):
    - "uploaded": PUT 성공 (신규 object 업로드)
    - "skipped_idempotent": HEAD 200 + sha256 match → skip (동일 내용)
    - "queued": NAS unreachable → retry_queue.enqueue() 성공 (ADR-027 D5 invariant)
    - "hard_floor_blocked": retry queue hard floor 도달 → MANUAL_GATE escalate 의무
      (caller 가 'queued' 와 구분, SOP runner 에 escalation signal 전달)
    - "skipped_etag_overwrite": Metadata sha256 absent (legacy 객체) 경우 overwrite 후 반환
    """

    status: Literal["uploaded", "skipped_idempotent", "queued", "hard_floor_blocked", "skipped_etag_overwrite"]
    object_etag: str = ""
    latency_ms: float = 0.0


class NASUploader:
    """Production-grade NAS MinIO uploader.

    Responsibilities:
    - HEAD-then-PUT idempotency (S4): same key + same sha256 → skip; mismatch → conflict error.
    - retry_queue integration: NAS unreachable → enqueue + return queued status (no raise to caller).
    - Prometheus 4종 metric emit per put() call.
    - threading.Lock _get_client(): concurrent init guard (P1-3 FIX#2).

    Env: NAS_MINIO_ENDPOINT / NAS_MINIO_ACCESS_KEY / NAS_MINIO_SECRET_KEY (별 namespace,
    기존 MINIO_ENDPOINT 침범 0, EC-1 박제).

    ADR-027 D5 invariant:
    - NAS unreachable + suppress_enqueue=False → retry_queue.enqueue() → status='queued' (raise 0)
    - NAS unreachable + suppress_enqueue=True → EndpointConnectionError raise (drain 측 retain)
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
        # P1-3 FIX#2: threading.Lock for _get_client() double-checked locking
        self._client_lock = threading.Lock()

    def _get_client(self):
        """Lazy boto3 client creation with threading.Lock (P1-3 FIX#2).

        double-checked locking pattern:
        1. lock 진입 전 None 체크 (fast path, lock-free)
        2. lock 진입 후 다시 None 체크 (race condition 방어)
        credentials 는 절대 log 출력 금지.
        """
        if self.__client is None:
            with self._client_lock:
                # double-checked: lock 진입 후 다시 None 확인
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
        self,
        key: str,
        data: bytes | Path,
        *,
        sha256: str | None = None,
        suppress_enqueue: bool = False,
    ) -> PutResult:
        """HEAD-then-PUT idempotency.

        FIX#2 suppress_enqueue parameter:
        - suppress_enqueue=False (기본값): NAS unreachable → retry_queue.enqueue() → status='queued' (raise 0)
          ADR-027 D5 invariant 유지 (hot path 무영향)
        - suppress_enqueue=True (drain 호출 시): NAS unreachable → EndpointConnectionError raise
          drain 측에서 catch + retain (재귀 enqueue 방지)

        Returns PutResult(
            status=Literal['uploaded', 'skipped_idempotent', 'queued'],
            object_etag,
            latency_ms
        ).

        AC-1: HEAD 검사 → existing object 의 sha256 match → skip + idempotent.
        AC-1: mismatch 시 ConditionalWriteConflict raise (caller 결정).
        AC-2: EndpointConnectionError + suppress_enqueue=False → retry_queue.enqueue() → status='queued'.
        ADR-027 D5: put() 가 NAS unreachable 시 (suppress_enqueue=False) raise 0 → hot path 무영향.
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
            if suppress_enqueue:
                # drain 호출 경로: raise (drain 측 catch + retain)
                raise

            # ADR-027 D5 invariant: raise 0, retry_queue.enqueue()
            # log masking: endpoint host:port 만 (auth 정보 0)
            safe_endpoint = _mask_endpoint(self._endpoint)
            log.warning("[nas_uploader] endpoint unreachable endpoint=%s key=%s", safe_endpoint, key)
            if self._metrics:
                latency_s = time.perf_counter() - t_start
                self._metrics.emit_fail(
                    bucket=self.bucket, reason="endpoint_unreachable", latency_s=latency_s
                )
            if self._retry_queue is not None:
                # FIX#3 P0-NEW-1: EnqueueResult.status propagation
                enq_result = self._retry_queue.enqueue(key=key, data=raw_data, sha256=sha256)
                if enq_result.status == "hard_floor_blocked":
                    log.critical(
                        "[nas_uploader] hard_floor_blocked — retry queue at hard floor "
                        "endpoint=%s key=%s MANUAL_GATE escalation required",
                        safe_endpoint, key,
                    )
                    return PutResult(
                        status="hard_floor_blocked",
                        latency_ms=(time.perf_counter() - t_start) * 1000,
                    )
                elif enq_result.status == "ok":
                    return PutResult(
                        status="queued",
                        latency_ms=(time.perf_counter() - t_start) * 1000,
                    )
                else:
                    # unexpected enqueue status — log + fallback to queued
                    log.error(
                        "[nas_uploader] unexpected enqueue status=%s key=%s — treating as queued",
                        enq_result.status, key,
                    )
                    return PutResult(
                        status="queued",
                        latency_ms=(time.perf_counter() - t_start) * 1000,
                    )
            return PutResult(
                status="queued",
                latency_ms=(time.perf_counter() - t_start) * 1000,
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            safe_endpoint = _mask_endpoint(self._endpoint)

            # INCIDENT-2026-05-17 amendment (ADR-027 §D5 amend): 4xx fail-fast
            # auth/policy/quota 영구 오류 → retry_queue 흡수 금지 + NASOperationalAlert raise.
            fail_fast_reason = _classify_4xx(code)
            if fail_fast_reason is not None:
                from mctrader_data.nas_metrics.prometheus_exporters import (  # noqa: PLC0415
                    nas_put_operational_alert_total,
                )
                nas_put_operational_alert_total.labels(
                    tier="unknown", reason=fail_fast_reason
                ).inc()
                if self._metrics:
                    latency_s = time.perf_counter() - t_start
                    self._metrics.emit_fail(
                        bucket=self.bucket, reason=fail_fast_reason, latency_s=latency_s
                    )
                log.critical(
                    "[nas_uploader] 4xx fail-fast endpoint=%s key=%s code=%s reason=%s "
                    "— retry_queue 흡수 금지, operator 개입 의무 (ADR-027 INCIDENT-2026-05-17 amendment)",
                    safe_endpoint, key, code, fail_fast_reason,
                )
                raise NASOperationalAlert(
                    code=code, reason=fail_fast_reason, tier="unknown", nas_key=key
                ) from exc

            # 본 분기 도달 = _classify_4xx None (4xx 매트릭스 외 code).
            # QuotaExceeded/StorageClassNotSupported 는 _FAIL_FAST_CODE_TO_REASON 에 포함되어
            # 위에서 이미 raise — 잔여 code 는 5xx/일반 → reason="unknown" 단일.
            reason = "unknown"
            log.warning(
                "[nas_uploader] client error endpoint=%s key=%s code=%s",
                safe_endpoint, key, code,
            )
            if self._metrics:
                latency_s = time.perf_counter() - t_start
                self._metrics.emit_fail(bucket=self.bucket, reason=reason, latency_s=latency_s)
            if not suppress_enqueue and self._retry_queue is not None:
                # FIX#3 P0-NEW-1: EnqueueResult.status propagation
                enq_result = self._retry_queue.enqueue(key=key, data=raw_data, sha256=sha256)
                if enq_result.status == "hard_floor_blocked":
                    log.critical(
                        "[nas_uploader] hard_floor_blocked — retry queue at hard floor "
                        "endpoint=%s key=%s code=%s MANUAL_GATE escalation required",
                        safe_endpoint, key, code,
                    )
                    return PutResult(
                        status="hard_floor_blocked",
                        latency_ms=(time.perf_counter() - t_start) * 1000,
                    )
                elif enq_result.status == "ok":
                    return PutResult(
                        status="queued",
                        latency_ms=(time.perf_counter() - t_start) * 1000,
                    )
                else:
                    log.error(
                        "[nas_uploader] unexpected enqueue status=%s key=%s code=%s — treating as queued",
                        enq_result.status, key, code,
                    )
                    return PutResult(
                        status="queued",
                        latency_ms=(time.perf_counter() - t_start) * 1000,
                    )
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

        FIX#2 P2-2 ETag fallback false-positive 방지:
        - HEAD 200 + Metadata sha256 present → 정상 ETag 비교 path
        - HEAD 200 + Metadata sha256 absent (외부 PUT / legacy 객체) → log warning + PUT overwrite
          (false-positive skip 차단: ETag 비교 skip)

        ConditionalWriteConflict raise 시 caller(put()) 에서 catch 후 결정.
        EndpointConnectionError → caller(put()) 에서 suppress_enqueue 분기.
        """
        client = self._get_client()
        t_head = time.perf_counter()

        try:
            head_response = client.head_object(Bucket=self.bucket, Key=key)
            head_latency_s = time.perf_counter() - t_head

            # HEAD 성공 (200) → Metadata sha256 확인
            if self._metrics:
                self._metrics.emit_head(bucket=self.bucket, latency_s=head_latency_s)

            metadata = head_response.get("Metadata", {})
            existing_sha256_from_metadata = metadata.get("sha256")

            if existing_sha256_from_metadata is None:
                # P2-2 FIX#2: Metadata sha256 absent → ETag 비교 skip → PUT overwrite
                # 외부 PUT 또는 legacy 객체: false-positive skip 차단
                log.warning(
                    "[nas_uploader] metadata sha256 absent — overwrite key=%s "
                    "(external/legacy object, ETag comparison skipped)",
                    key,
                )
                # fall through to PUT (no raise)
            elif existing_sha256_from_metadata == sha256:
                # Metadata sha256 present + match → SKIP (idempotent)
                log.info(
                    "[nas_uploader] idempotent skip key=%s sha256=%s…",
                    key, sha256[:8],
                )
                return PutResult(
                    status="skipped_idempotent",
                    object_etag=existing_sha256_from_metadata,
                )
            else:
                # sha256 mismatch → ConditionalWriteConflict raise (ADR-009 forward-only)
                safe_endpoint = _mask_endpoint(self._endpoint)
                log.error(
                    "[nas_uploader] sha256 mismatch endpoint=%s key=%s "
                    "existing=%.8s… new=%.8s…",
                    safe_endpoint, key, existing_sha256_from_metadata, sha256,
                )
                raise ConditionalWriteConflict(
                    f"key={key} existing_sha256={existing_sha256_from_metadata[:8]}… new_sha256={sha256[:8]}…"
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

        # PUT (HEAD 404 path or Metadata absent overwrite path)
        response = client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            Metadata={"sha256": sha256},
        )
        etag = response.get("ETag", "").strip('"')
        log.info("[nas_uploader] uploaded key=%s etag=%s bytes=%d", key, etag, len(data))

        return PutResult(status="uploaded", object_etag=etag)

    def enqueue_retry(self, key: str, data: bytes | Path, sha256: str) -> None:
        """retry_queue 에 직접 enqueue (MCT-189 D-2 A: promote verify-fail orphan 방지).

        DualWriter 가 promote_l1() verify 실패 시 source 를 retry_queue 에 재등록하기 위한
        public gateway. _retry_queue None 시 log.error (orphan 위험 가시화).

        production cli.py:827-836 에서 NASUploader 생성 시 retry_queue 주입 보장 — 정상 경로는
        rq is not None. None = 테스트/단독 사용 환경 (silent-skip → error log 로 가시화).

        Args:
            key: NAS object key (tier prefix 포함)
            data: bytes 또는 Path (RetryQueue.enqueue 시그니처 정합)
            sha256: sha256 hexdigest
        """
        rq = self._retry_queue
        if rq is None:
            log.error(
                "[nas_uploader] enqueue_retry: retry_queue not configured — orphan risk! "
                "key=%r (production path: cli.py NASUploader 생성 시 retry_queue 주입 의무)",
                key,
            )
            return
        rq.enqueue(key=key, data=data, sha256=sha256)

    def head_object(self, key: str) -> dict:
        """4-tuple verify primitive (MCT-189 D-4 C).

        Returns:
            dict with keys:
            - "ETag": str — S3 ETag stripped of surrounding quotes
            - "VersionId": str | None — bucket versioning (None if not versioned)
            - "sha256": str | None — Metadata sha256 (None if absent / legacy object)
            - "ContentLength": int — object size in bytes

        Raises:
            botocore.exceptions.ClientError: HEAD 404 or non-404 S3 error
            botocore.exceptions.EndpointConnectionError: NAS unreachable
        """
        client = self._get_client()
        response = client.head_object(Bucket=self.bucket, Key=key)
        metadata = response.get("Metadata", {}) or {}
        return {
            "ETag": response.get("ETag", "").strip('"'),
            "VersionId": response.get("VersionId"),
            "sha256": metadata.get("sha256"),
            "ContentLength": int(response.get("ContentLength", 0)),
        }

    def put_streaming(
        self,
        local_path_or_fileobj: Path | IO[bytes],
        nas_key: str,
        sha256: str,
    ) -> PutResult:
        """Streaming upload via boto3 upload_fileobj + TransferConfig (MCT-163 F3, D1=B, D3=A).

        Backward compat: 기존 put(key, data=bytes) 를 대체하지 않음 (INV-2).
        DualWriter.write(data=Path) 가 내부적으로 본 method 호출 (F3 streaming path).

        D1=B: boto3 upload_fileobj + TransferConfig(multipart_chunksize=8MB, multipart_threshold=8MB)
          - 메모리 전체 로드 0 (INV-4: DualWriter ≤ 50 MB peak delta)
          - multipart idiomatic (NAS MinIO 호환)
        D2=A: caller-side sha256 SSOT — sha256 반드시 caller가 계산 후 주입 (INV-3)
          - multipart ETag ≠ sha256: S3 multipart ETag = parts hash, sha256 = content hash (별도)
        D3=A: 별 method (put() signature 보존, backward compat 격리)

        HEAD-then-PUT idempotency: sha256 metadata Metadata={'sha256': sha256} 전달 (INV-3 정합).
        NAS unreachable / ClientError: put() 와 동일 suppress_enqueue=False 패턴 (ADR-027 D5).

        Returns:
            PutResult(status='uploaded' | 'skipped_idempotent' | 'queued' | 'hard_floor_blocked')

        Raises:
            ConditionalWriteConflict: HEAD 200 + sha256 mismatch (forward-only invariant)
        """
        t_start = time.perf_counter()
        client = self._get_client()

        # HEAD idempotency check (sha256 match → skip, mismatch → conflict)
        try:
            head_response = client.head_object(Bucket=self.bucket, Key=nas_key)
            metadata = head_response.get("Metadata", {})
            existing_sha256 = metadata.get("sha256")
            if existing_sha256 is not None:
                if existing_sha256 == sha256:
                    log.info(
                        "[nas_uploader] put_streaming idempotent skip key=%s sha256=%s…",
                        nas_key, sha256[:8],
                    )
                    return PutResult(
                        status="skipped_idempotent",
                        object_etag=existing_sha256,
                        latency_ms=(time.perf_counter() - t_start) * 1000,
                    )
                else:
                    safe_endpoint = _mask_endpoint(self._endpoint)
                    log.error(
                        "[nas_uploader] put_streaming sha256 mismatch endpoint=%s key=%s "
                        "existing=%.8s… new=%.8s…",
                        safe_endpoint, nas_key, existing_sha256, sha256,
                    )
                    raise ConditionalWriteConflict(
                        f"key={nas_key} existing={existing_sha256[:8]}… new={sha256[:8]}…"
                    )
            else:
                # Metadata sha256 absent → overwrite (legacy object)
                safe_endpoint = _mask_endpoint(self._endpoint)
                log.warning(
                    "[nas_uploader] put_streaming metadata sha256 absent — overwrite key=%s",
                    nas_key,
                )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "404":
                pass  # HEAD 404 → object 없음 → upload 진행
            else:
                # INCIDENT-2026-05-17 amendment (ADR-027 §D5 amend): 4xx fail-fast (HEAD path)
                fail_fast_reason = _classify_4xx(code)
                if fail_fast_reason is not None:
                    from mctrader_data.nas_metrics.prometheus_exporters import (  # noqa: PLC0415
                        nas_put_operational_alert_total,
                    )
                    nas_put_operational_alert_total.labels(
                        tier="unknown", reason=fail_fast_reason
                    ).inc()
                    safe_endpoint = _mask_endpoint(self._endpoint)
                    log.critical(
                        "[nas_uploader] put_streaming HEAD 4xx fail-fast endpoint=%s key=%s "
                        "code=%s reason=%s — retry_queue 흡수 금지 (ADR-027 INCIDENT-2026-05-17)",
                        safe_endpoint, nas_key, code, fail_fast_reason,
                    )
                    raise NASOperationalAlert(
                        code=code, reason=fail_fast_reason, tier="unknown", nas_key=nas_key
                    ) from exc
                raise

        # Streaming upload via upload_fileobj + TransferConfig (D1=B)
        # 8 MB chunk — NAS MinIO multipart default threshold
        transfer_cfg = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,   # 8 MB
            multipart_chunksize=8 * 1024 * 1024,   # 8 MB per part
            max_concurrency=1,                       # sequential (memory 최소화)
            use_threads=False,
        )

        try:
            if isinstance(local_path_or_fileobj, Path):
                # Open as binary stream — read_bytes() 호출 0 (INV-4)
                with local_path_or_fileobj.open("rb") as fobj:
                    client.upload_fileobj(
                        fobj,
                        self.bucket,
                        nas_key,
                        ExtraArgs={"Metadata": {"sha256": sha256}},
                        Config=transfer_cfg,
                    )
            else:
                # fileobj path — upstream already opened
                client.upload_fileobj(
                    local_path_or_fileobj,
                    self.bucket,
                    nas_key,
                    ExtraArgs={"Metadata": {"sha256": sha256}},
                    Config=transfer_cfg,
                )
            log.info("[nas_uploader] put_streaming uploaded key=%s sha256=%s…", nas_key, sha256[:8])
            return PutResult(
                status="uploaded",
                object_etag="",  # upload_fileobj does not return ETag directly
                latency_ms=(time.perf_counter() - t_start) * 1000,
            )

        except EndpointConnectionError:
            safe_endpoint = _mask_endpoint(self._endpoint)
            log.warning(
                "[nas_uploader] put_streaming endpoint unreachable endpoint=%s key=%s",
                safe_endpoint, nas_key,
            )
            # ADR-027 D5: raise 0, retry_queue.enqueue()
            if self._retry_queue is not None:
                # For streaming, we need bytes for retry queue — read path once
                if isinstance(local_path_or_fileobj, Path):
                    data_bytes = local_path_or_fileobj.read_bytes()
                else:
                    data_bytes = local_path_or_fileobj.read()
                enq_result = self._retry_queue.enqueue(key=nas_key, data=data_bytes, sha256=sha256)
                if enq_result.status == "hard_floor_blocked":
                    return PutResult(
                        status="hard_floor_blocked",
                        latency_ms=(time.perf_counter() - t_start) * 1000,
                    )
            return PutResult(
                status="queued",
                latency_ms=(time.perf_counter() - t_start) * 1000,
            )

        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            safe_endpoint = _mask_endpoint(self._endpoint)

            # INCIDENT-2026-05-17 amendment (ADR-027 §D5 amend): 4xx fail-fast (upload path)
            fail_fast_reason = _classify_4xx(code)
            if fail_fast_reason is not None:
                from mctrader_data.nas_metrics.prometheus_exporters import (  # noqa: PLC0415
                    nas_put_operational_alert_total,
                )
                nas_put_operational_alert_total.labels(
                    tier="unknown", reason=fail_fast_reason
                ).inc()
                log.critical(
                    "[nas_uploader] put_streaming upload 4xx fail-fast endpoint=%s key=%s "
                    "code=%s reason=%s — retry_queue 흡수 금지 (ADR-027 INCIDENT-2026-05-17)",
                    safe_endpoint, nas_key, code, fail_fast_reason,
                )
                raise NASOperationalAlert(
                    code=code, reason=fail_fast_reason, tier="unknown", nas_key=nas_key
                ) from exc

            log.warning(
                "[nas_uploader] put_streaming client error endpoint=%s key=%s code=%s",
                safe_endpoint, nas_key, code,
            )
            if self._retry_queue is not None:
                if isinstance(local_path_or_fileobj, Path):
                    data_bytes = local_path_or_fileobj.read_bytes()
                else:
                    data_bytes = local_path_or_fileobj.read()
                enq_result = self._retry_queue.enqueue(key=nas_key, data=data_bytes, sha256=sha256)
                if enq_result.status == "hard_floor_blocked":
                    return PutResult(
                        status="hard_floor_blocked",
                        latency_ms=(time.perf_counter() - t_start) * 1000,
                    )
            return PutResult(
                status="queued",
                latency_ms=(time.perf_counter() - t_start) * 1000,
            )

    def _list_objects(self, prefix: str) -> list[str]:
        """List object keys in bucket with given prefix.

        MCT-151: InvariantHarness 가 object_count + sha256 + schema verify 시 사용.
        Returns list of full object keys matching prefix.

        ADR-027 D6: per-partition object listing (prefix = Hive partition prefix).
        SecurityArch: log 에 endpoint URL masking (host:port 만).
        """
        client = self._get_client()
        paginator = client.get_paginator("list_objects_v2")
        keys: list[str] = []
        try:
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
        except Exception as e:
            log.warning(
                "[nas_uploader] _list_objects failed: bucket=%s prefix=%s err=%s",
                self.bucket, prefix, type(e).__name__,
            )
            raise
        return sorted(keys)

    def _download(self, key: str) -> bytes:
        """Download object bytes from NAS bucket.

        MCT-151: InvariantHarness 가 sha256 + row_count + schema verify 시 사용.
        per-segment streaming (GET body) — NFR-2 latency budget 정합.

        SecurityArch: endpoint URL masking in log (host:port 만).
        """
        client = self._get_client()
        try:
            response = client.get_object(Bucket=self.bucket, Key=key)
            body: bytes = response["Body"].read()
            log.debug("[nas_uploader] _download key=%s bytes=%d", key, len(body))
            return body
        except ClientError as e:
            log.warning(
                "[nas_uploader] _download failed: bucket=%s key=%s err=%s",
                self.bucket, key, e.response.get("Error", {}).get("Code", "unknown"),
            )
            raise


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
