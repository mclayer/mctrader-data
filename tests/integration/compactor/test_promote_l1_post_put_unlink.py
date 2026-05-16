"""test_promote_l1_post_put_unlink.py — Integration tests for promote_l1() with testcontainers MinIO.

MCT-189 D-4 C + D-8 B + D-2 A: promote_l1() 4중 verify + pre-delete guard + local unlink.

Scenarios:
- test_normal_path: PUT → 4중 verify → pre-delete guard → unlink (local 부재)
- test_head_404: PromotionVerifyError + local 보존
- test_concurrent_double_unlink: 2 thread, 1 success + ENOENT graceful
- test_pre_delete_guard_partition: monkeypatch 2nd HEAD ETag 변경 → PromotionVerifyError + local 보존
- test_sha256_mismatch: sha256 mismatch → PromotionVerifyError + local 보존
- test_content_length_mismatch: ContentLength mismatch → PromotionVerifyError + local 보존
- test_ambiguity_invariant_post_wiring: promote 후 InvariantHarness._check_ambiguity() violation 0
"""
from __future__ import annotations

import hashlib
import threading
from pathlib import Path
from unittest.mock import patch

import contextlib

import boto3
import pytest

from testcontainers.minio import MinioContainer


# ─── fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def minio_container():
    """Module-scope MinIO testcontainer (spin-up once per test module)."""
    with MinioContainer() as minio:
        yield minio


@pytest.fixture(scope="module")
def minio_client(minio_container):
    """boto3 S3 client connected to testcontainer MinIO."""
    cfg = minio_container.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name="us-east-1",
    )
    # Create test bucket (ignore if already exists)
    with contextlib.suppress(Exception):
        client.create_bucket(Bucket="test-promote")
    return client


@pytest.fixture(scope="module")
def nas_uploader(minio_container, minio_client):
    """NASUploader pointed at testcontainer MinIO."""
    from mctrader_data.nas_storage.nas_uploader import NASUploader
    cfg = minio_container.get_config()
    uploader = NASUploader(
        endpoint=f"http://{cfg['endpoint']}",
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        bucket="test-promote",
    )
    return uploader


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _put_object(minio_client, key: str, data: bytes) -> None:
    """Helper: PUT object with sha256 metadata to MinIO."""
    sha256_val = _sha256(data)
    minio_client.put_object(
        Bucket="test-promote",
        Key=key,
        Body=data,
        Metadata={"sha256": sha256_val},
    )


# ─── tests ───────────────────────────────────────────────────────────────────


class TestPromoteL1PostPutUnlink:
    """promote_l1() integration tests with real MinIO (testcontainers)."""

    def test_normal_path(self, tmp_path: Path, minio_client, nas_uploader) -> None:
        """PUT → 4중 verify (sha256 + ContentLength) → pre-delete guard → local 부재."""
        from mctrader_data.compactor.promotion import promote_l1

        content = b"normal path parquet content for MCT-189"
        nas_key = "l1/test_normal_path.parquet"
        local = tmp_path / "normal.parquet"
        local.write_bytes(content)

        _put_object(minio_client, nas_key, content)

        result = promote_l1(
            local_path=local,
            nas_uploader=nas_uploader,
            nas_key=nas_key,
            segment_id="normal-001",
        )

        assert result.status == "promoted"
        assert not local.exists(), "test_normal_path: local 삭제 의무 (grace 0, D3=C)"

    def test_head_404(self, tmp_path: Path, nas_uploader) -> None:
        """HEAD 404 → PromotionVerifyError + local 보존 (INV-4)."""
        from mctrader_data.compactor.promotion import promote_l1, PromotionVerifyError

        content = b"content that is NOT on NAS"
        local = tmp_path / "not_on_nas.parquet"
        local.write_bytes(content)
        # NAS에 업로드 안 함 → HEAD 404

        with pytest.raises(PromotionVerifyError, match="404"):
            promote_l1(
                local_path=local,
                nas_uploader=nas_uploader,
                nas_key="l1/does_not_exist.parquet",
                segment_id="head404-001",
            )

        assert local.exists(), "test_head_404: local 보존 의무 (INV-4)"

    def test_concurrent_double_unlink(self, tmp_path: Path, minio_client, nas_uploader) -> None:
        """2 thread 동시 promote_l1() → 1 success + ENOENT graceful (missing_ok=False)."""
        from mctrader_data.compactor.promotion import promote_l1, PromotionVerifyError

        content = b"concurrent unlink test content"
        nas_key = "l1/concurrent_unlink.parquet"
        local = tmp_path / "concurrent.parquet"
        local.write_bytes(content)

        _put_object(minio_client, nas_key, content)

        results = []
        errors = []

        def _promote():
            try:
                r = promote_l1(
                    local_path=local,
                    nas_uploader=nas_uploader,
                    nas_key=nas_key,
                    segment_id="concurrent-001",
                )
                results.append(r.status)
            except (PromotionVerifyError, FileNotFoundError, OSError) as e:
                errors.append(str(e))

        t1 = threading.Thread(target=_promote)
        t2 = threading.Thread(target=_promote)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # 합계: 최소 1 success. 2번째는 ENOENT 또는 PromotionVerifyError
        assert "promoted" in results, f"최소 1 success 기대. results={results} errors={errors}"
        assert not local.exists(), "최종적으로 local 부재 의무"

    def test_pre_delete_guard_partition(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """pre-delete guard 2nd HEAD ETag 변경 → PromotionVerifyError + local 보존."""
        from mctrader_data.compactor.promotion import promote_l1, PromotionVerifyError

        content = b"pre-delete guard test content"
        nas_key = "l1/guard_test.parquet"
        local = tmp_path / "guard.parquet"
        local.write_bytes(content)

        _put_object(minio_client, nas_key, content)

        call_count = [0]
        original_head = nas_uploader.head_object

        def patched_head(key: str) -> dict:
            call_count[0] += 1
            result = original_head(key)
            if call_count[0] == 2:
                # pre-delete guard 호출 시 ETag를 변경 (race 시뮬레이션)
                result = dict(result)
                result["ETag"] = "TAMPERED-ETag"
            return result

        with patch.object(nas_uploader, "head_object", side_effect=patched_head), pytest.raises(
            PromotionVerifyError, match="pre-delete guard mismatch"
        ):
            promote_l1(
                local_path=local,
                nas_uploader=nas_uploader,
                nas_key=nas_key,
                segment_id="guard-001",
            )

        assert local.exists(), "test_pre_delete_guard_partition: local 보존 의무 (INV-4)"

    def test_sha256_mismatch(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """NAS sha256 ≠ local sha256 → PromotionVerifyError + local 보존."""
        from mctrader_data.compactor.promotion import promote_l1, PromotionVerifyError

        local_content = b"local content for sha256 mismatch"
        nas_content = b"DIFFERENT content on NAS"  # sha256 다름
        nas_key = "l1/sha256_mismatch_test.parquet"
        local = tmp_path / "sha256_mismatch.parquet"
        local.write_bytes(local_content)

        # NAS에는 다른 내용으로 PUT (sha256 다름)
        _put_object(minio_client, nas_key, nas_content)

        with pytest.raises(PromotionVerifyError, match="sha256 mismatch"):
            promote_l1(
                local_path=local,
                nas_uploader=nas_uploader,
                nas_key=nas_key,
                segment_id="sha256-mismatch-001",
            )

        assert local.exists(), "test_sha256_mismatch: local 보존 의무 (INV-4)"

    def test_content_length_mismatch(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """NAS ContentLength ≠ local size → PromotionVerifyError + local 보존.

        sha256이 일치하더라도 ContentLength 불일치 시 verify fail.
        """
        from mctrader_data.compactor.promotion import promote_l1, PromotionVerifyError

        local_content = b"content for cl mismatch"
        nas_key = "l1/cl_mismatch_test.parquet"
        local = tmp_path / "cl_mismatch.parquet"
        local.write_bytes(local_content)

        # NAS에 PUT 후 head_object 반환을 monkeypatch하여 ContentLength 변경
        _put_object(minio_client, nas_key, local_content)

        original_head = nas_uploader.head_object

        def patched_head_cl(key: str) -> dict:
            result = original_head(key)
            result = dict(result)
            result["ContentLength"] = len(local_content) + 9999  # 의도적 mismatch
            return result

        with patch.object(nas_uploader, "head_object", side_effect=patched_head_cl), pytest.raises(
            PromotionVerifyError, match="ContentLength mismatch"
        ):
            promote_l1(
                local_path=local,
                nas_uploader=nas_uploader,
                nas_key=nas_key,
                segment_id="cl-mismatch-001",
            )

        assert local.exists(), "test_content_length_mismatch: local 보존 의무 (INV-4)"

    def test_ambiguity_invariant_post_wiring(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """promote 후 InvariantHarness._check_ambiguity() violation 0.

        post-promotion: local 삭제 → NAS only → XOR invariant (INV-1) PASS.
        """
        from mctrader_data.compactor.promotion import promote_l1
        from mctrader_data.nas_migration.invariant_harness import InvariantHarness

        content = b"ambiguity check post wiring content"
        nas_key = "l1/ambiguity_post_wiring.parquet"
        local = tmp_path / "ambiguity.parquet"
        local.write_bytes(content)

        _put_object(minio_client, nas_key, content)

        result = promote_l1(
            local_path=local,
            nas_uploader=nas_uploader,
            nas_key=nas_key,
            segment_id="ambiguity-wiring-001",
        )

        assert result.status == "promoted"
        assert not local.exists()

        # post-promotion: local 없음 → _check_ambiguity pass
        harness = InvariantHarness(nas_uploader=nas_uploader, local_root=tmp_path)
        ambiguity_result = harness._check_ambiguity(
            local_partition=tmp_path,
            nas_partition="l1",
            local_files=[],  # post-promotion: local empty
        )

        assert ambiguity_result.status == "pass", (
            f"post-promotion: ambiguity violation 0 기대. status={ambiguity_result.status!r}"
        )
