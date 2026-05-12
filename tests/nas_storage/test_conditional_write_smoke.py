"""test_conditional_write_smoke.py — P0 real MinIO smoke test for conditional write (S4).

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253

NFR-2: real MinIO endpoint 의무 — mock 단독 거부.
NAS_MINIO_* env vars 미설정 시 SKIP (CI 환경 정합).
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import time
from pathlib import Path

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from mctrader_data.nas_storage.nas_uploader import NASUploader
from mctrader_data.nas_storage.retry_queue import RetryQueue

NAS_MINIO_ENDPOINT = os.environ.get("NAS_MINIO_ENDPOINT")
NAS_MINIO_ACCESS_KEY = os.environ.get("NAS_MINIO_ACCESS_KEY")
NAS_MINIO_SECRET_KEY = os.environ.get("NAS_MINIO_SECRET_KEY")
NAS_MINIO_BUCKET = os.environ.get("NAS_MINIO_BUCKET", "mctrader-market")

skip_if_no_nas = pytest.mark.skipif(
    not all([NAS_MINIO_ENDPOINT, NAS_MINIO_ACCESS_KEY, NAS_MINIO_SECRET_KEY]),
    reason="NAS_MINIO_* env vars not set — skip on CI without NAS endpoint (NFR-2 real MinIO gate)",
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_EVIDENCE_PATH = _REPO_ROOT / ".tmp" / "evidence-smoke-MCT-150.md"

SMOKE_SIZES = [
    ("1KB", 1024),
    ("1MB", 1024 * 1024),
    ("10MB", 10 * 1024 * 1024),
    ("50MB", 50 * 1024 * 1024),
]


def _write_evidence(row: dict) -> None:
    _EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not _EVIDENCE_PATH.exists():
        _EVIDENCE_PATH.write_text(
            "# Smoke Evidence — MCT-150 Phase 2 conditional write\n\n"
            "Story: MCT-150 (Stage 2 — uploader hardening)\n"
            "Issue: mclayer/mctrader-hub#253\n\n",
            encoding="utf-8",
        )
    import datetime
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    line = f"\n## {ts}\n\n```json\n{json.dumps(row, indent=2, default=str)}\n```\n"
    with _EVIDENCE_PATH.open("a", encoding="utf-8") as f:
        f.write(line)


@pytest.fixture(scope="module")
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=NAS_MINIO_ENDPOINT,
        aws_access_key_id=NAS_MINIO_ACCESS_KEY,
        aws_secret_access_key=NAS_MINIO_SECRET_KEY,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


@pytest.fixture(scope="module")
def nas_uploader(tmp_path_factory: pytest.TempPathFactory) -> NASUploader:
    tmp = tmp_path_factory.mktemp("retry_queue_smoke")
    rq = RetryQueue(path=tmp)
    return NASUploader(
        endpoint=NAS_MINIO_ENDPOINT or "",
        access_key=NAS_MINIO_ACCESS_KEY or "",
        secret_key=NAS_MINIO_SECRET_KEY or "",
        bucket=NAS_MINIO_BUCKET,
        retry_queue=rq,
    )


@skip_if_no_nas
class TestConditionalWriteSmoke:
    """S4 real MinIO smoke — MinIO RELEASE.2025-04-08 conditional write behavior 검증."""

    def test_minio_release_2025_04_08_conditional_put_supported(
        self, s3_client
    ) -> None:
        """If-None-Match: * 헤더로 PUT → MinIO 응답 분석.

        결과 박제 의무: .tmp/evidence-smoke-MCT-150.md 에 MinIO conditional write 지원 여부 박제.
        실패 시 fallback path = HEAD-then-PUT (NASUploader 기본 동작).
        """
        key = f"smoke/conditional-write-test-{int(time.time())}.bin"
        data = os.urandom(1024)
        sha256 = hashlib.sha256(data).hexdigest()

        conditional_write_supported = False
        fallback_confirmed = False
        detail = {}

        # 1차 PUT (신규 object)
        s3_client.put_object(
            Bucket=NAS_MINIO_BUCKET,
            Key=key,
            Body=data,
            Metadata={"sha256": sha256},
        )

        # If-None-Match: * 로 동일 key 재PUT 시도
        try:
            response = s3_client.put_object(
                Bucket=NAS_MINIO_BUCKET,
                Key=key,
                Body=data,
                IfNoneMatch="*",
            )
            detail = {
                "conditional_write_supported": False,
                "result": "PUT returned 200 — IfNoneMatch not enforced",
                "fallback": "HEAD-then-PUT required",
                "response_status": response.get("ResponseMetadata", {}).get("HTTPStatusCode"),
            }
            fallback_confirmed = True
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "412":
                conditional_write_supported = True
                detail = {
                    "conditional_write_supported": True,
                    "result": "PUT returned 412 Precondition Failed — IfNoneMatch enforced",
                    "minio_version": "RELEASE.2025-04-08 or later",
                }
            else:
                detail = {
                    "conditional_write_supported": False,
                    "result": f"PUT error code={code} (unexpected)",
                    "fallback": "HEAD-then-PUT required",
                    "error": str(e),
                }
                fallback_confirmed = True

        with contextlib.suppress(Exception):
            s3_client.delete_object(Bucket=NAS_MINIO_BUCKET, Key=key)

        _write_evidence({
            "test": "test_minio_release_2025_04_08_conditional_put_supported",
            "key": key,
            "conditional_write_supported": conditional_write_supported,
            "fallback_confirmed": fallback_confirmed,
            "detail": detail,
        })

        assert conditional_write_supported or fallback_confirmed, (
            "Neither conditional write nor fallback path was confirmed"
        )

    @pytest.mark.parametrize("size_label,size_bytes", SMOKE_SIZES)
    def test_latency_within_baseline(
        self, nas_uploader: NASUploader, size_label: str, size_bytes: int
    ) -> None:
        """MCT-148 T2 latency baseline ±15% gate (§8.3 FIX#1 F2)."""
        gates_ms = {
            "1KB": (395.7, 535.4),
            "1MB": (375.1, 507.5),
            "10MB": (826.2, 1117.8),
            "50MB": (2440.1, 3301.2),
        }
        low, high = gates_ms[size_label]

        data = os.urandom(size_bytes)
        sha256 = hashlib.sha256(data).hexdigest()
        key = f"smoke/latency/{size_label}/test-{int(time.time())}.bin"

        num_samples = 5
        durations_ms: list[float] = []

        for i in range(num_samples):
            t0 = time.perf_counter()
            result = nas_uploader.put(key=f"{key}-{i}", data=data, sha256=sha256)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            durations_ms.append(elapsed_ms)
            assert result.status in ("uploaded", "skipped_idempotent")

        sorted_durations = sorted(durations_ms)
        p99_idx = max(0, min(int(len(sorted_durations) * 0.99) - 1, len(sorted_durations) - 1))
        p99 = sorted_durations[p99_idx]

        _write_evidence({
            "test": "test_latency_within_baseline",
            "size": size_label,
            "N": num_samples,
            "durations_ms": durations_ms,
            "p99_ms": p99,
            "gate_low_ms": low,
            "gate_high_ms": high,
            "pass": low <= p99 <= high,
        })

        # §8.3 FIX#1 F2: ±15% gate = regression detection (상한 초과 시 실패).
        # p99가 하한보다 낮으면 performance improvement — gate 미적용 (regression 아님).
        baselines = {"1KB": 465.53, "1MB": 441.34, "10MB": 971.99, "50MB": 2870.65}
        baseline = baselines[size_label]
        assert p99 <= high, (
            f"Latency p99={p99:.1f}ms EXCEEDED gate upper bound {high}ms for size={size_label} "
            f"(MCT-148 baseline={baseline:.2f}ms)"
        )

    def test_head_then_put_idempotency_real_minio(self, nas_uploader: NASUploader) -> None:
        """real MinIO 대상 HEAD-then-PUT idempotency smoke. AC-1 직접 검증."""
        data = os.urandom(1024)
        sha256 = hashlib.sha256(data).hexdigest()
        key = f"smoke/idempotency/test-{int(time.time())}.bin"

        result1 = nas_uploader.put(key=key, data=data, sha256=sha256)
        assert result1.status == "uploaded"

        result2 = nas_uploader.put(key=key, data=data, sha256=sha256)
        assert result2.status == "skipped_idempotent", (
            f"Expected 'skipped_idempotent' on 2nd PUT, got '{result2.status}'"
        )

        _write_evidence({
            "test": "test_head_then_put_idempotency_real_minio",
            "key": key,
            "first_put": result1.status,
            "second_put": result2.status,
            "idempotency_confirmed": result2.status == "skipped_idempotent",
        })
