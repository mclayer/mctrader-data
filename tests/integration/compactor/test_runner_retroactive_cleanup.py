"""test_runner_retroactive_cleanup.py — Integration tests for scan_and_cleanup_legacy().

MCT-189 Phase 2 PR2: retroactive legacy parquet cleanup (runner.scan_and_cleanup_legacy).

Scenarios (4):
- test_legacy_with_nas_match_unlinks:      local + NAS(동일 sha256) → cleaned==1, local 부재 (INV-1 XOR)
- test_legacy_with_nas_missing_preserved:  local + NAS 부재 → preserved==1, local 존재 (INV-4 안전망)
- test_legacy_with_nas_sha256_mismatch_preserved: local + NAS sha256 mismatch → preserved==1, local 존재
- test_legacy_returns_correct_counts:      정상 + fail 혼합 → counts 정합
"""
from __future__ import annotations

import contextlib
import hashlib
from pathlib import Path

import boto3
import pytest

from testcontainers.minio import MinioContainer


# ─── module-scope fixtures (spin-up MinIO once per module) ───────────────────


@pytest.fixture(scope="module")
def minio_container():
    """Module-scope MinIO testcontainer."""
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
    with contextlib.suppress(Exception):
        client.create_bucket(Bucket="test-legacy-cleanup")
    return client


@pytest.fixture(scope="module")
def nas_uploader(minio_container, minio_client):
    """NASUploader pointed at testcontainer MinIO, bucket=test-legacy-cleanup."""
    from mctrader_data.nas_storage.nas_uploader import NASUploader

    cfg = minio_container.get_config()
    uploader = NASUploader(
        endpoint=f"http://{cfg['endpoint']}",
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        bucket="test-legacy-cleanup",
    )
    return uploader


# ─── helpers ─────────────────────────────────────────────────────────────────


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _put_object(minio_client, key: str, data: bytes) -> None:
    """PUT object with sha256 metadata to MinIO."""
    sha256_val = _sha256(data)
    minio_client.put_object(
        Bucket="test-legacy-cleanup",
        Key=key,
        Body=data,
        Metadata={"sha256": sha256_val},
    )


def _put_object_no_sha256(minio_client, key: str, data: bytes) -> None:
    """PUT object WITHOUT sha256 metadata (sha256 mismatch simulation: NAS sha256==None, ContentLength differs)."""
    # NAS side: no sha256 metadata → promotion.py checks ContentLength only.
    # Upload different-length data to trigger ContentLength mismatch → PromotionVerifyError.
    minio_client.put_object(
        Bucket="test-legacy-cleanup",
        Key=key,
        Body=data,
    )


def _make_legacy_parquet(root: Path, rel_path: str, content: bytes) -> Path:
    """Create a legacy local parquet under root/market/**/*.parquet layout."""
    full = root / "market" / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(content)
    return full


# ─── tests ───────────────────────────────────────────────────────────────────


class TestScanAndCleanupLegacy:
    """scan_and_cleanup_legacy() integration tests with real MinIO (testcontainers)."""

    def test_legacy_with_nas_match_unlinks(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """local parquet + NAS PUT(동일 sha256) → scan_and_cleanup_legacy → cleaned==1, local 부재 (INV-1 XOR)."""
        from mctrader_data.compactor.runner import scan_and_cleanup_legacy

        content = b"legacy parquet content matching NAS"
        rel = "exchange=upbit/symbol=BTC/tier=L1/date=2024-01-01/part-001.parquet"
        local = _make_legacy_parquet(tmp_path, rel, content)

        # NAS key = market/<rel>
        nas_key = f"market/{rel}"
        _put_object(minio_client, nas_key, content)

        result = scan_and_cleanup_legacy(tmp_path, nas_uploader)

        assert result["cleaned"] == 1, f"cleaned 기대 1, got {result}"
        assert result["preserved"] == 0
        assert result["errors"] == 0
        assert not local.exists(), "INV-1: NAS match 후 local 삭제 의무 (grace 0, D3=C)"

    def test_legacy_with_nas_missing_preserved(
        self, tmp_path: Path, nas_uploader
    ) -> None:
        """local parquet + NAS 부재 → preserved==1, local 존재 (INV-4 안전망)."""
        from mctrader_data.compactor.runner import scan_and_cleanup_legacy

        content = b"local only parquet not on NAS"
        rel = "exchange=upbit/symbol=ETH/tier=L1/date=2024-01-02/part-001.parquet"
        local = _make_legacy_parquet(tmp_path, rel, content)
        # NAS에 업로드 안 함 → HEAD 404 → PromotionVerifyError → preserved

        result = scan_and_cleanup_legacy(tmp_path, nas_uploader)

        assert result["preserved"] >= 1, f"preserved >= 1 기대, got {result}"
        assert result["errors"] == 0
        assert local.exists(), "INV-4: HEAD verify fail 시 local 보존 의무"

    def test_legacy_with_nas_sha256_mismatch_preserved(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """local + NAS sha256 mismatch → preserved==1, local 존재 (PromotionVerifyError + INV-4)."""
        from mctrader_data.compactor.runner import scan_and_cleanup_legacy

        local_content = b"local content for sha256 mismatch test"
        nas_content = b"COMPLETELY DIFFERENT NAS CONTENT with different length abc123"
        rel = "exchange=upbit/symbol=XRP/tier=L1/date=2024-01-03/part-001.parquet"
        local = _make_legacy_parquet(tmp_path, rel, local_content)

        # NAS에 다른 내용 PUT (sha256 mismatch → PromotionVerifyError)
        nas_key = f"market/{rel}"
        _put_object(minio_client, nas_key, nas_content)

        result = scan_and_cleanup_legacy(tmp_path, nas_uploader)

        assert result["preserved"] >= 1, f"preserved >= 1 기대, got {result}"
        assert result["errors"] == 0
        assert local.exists(), "INV-4: sha256 mismatch 시 local 보존 의무"

    def test_legacy_returns_correct_counts(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """정상(NAS match) + fail(NAS 부재) 혼합 → cleaned/preserved counts 정합."""
        from mctrader_data.compactor.runner import scan_and_cleanup_legacy

        # 파일 1: NAS match (→ cleaned)
        content_ok = b"ok parquet content for count test abc"
        rel_ok = "exchange=bithumb/symbol=BTC/tier=L1/date=2024-02-01/part-001.parquet"
        local_ok = _make_legacy_parquet(tmp_path, rel_ok, content_ok)
        nas_key_ok = f"market/{rel_ok}"
        _put_object(minio_client, nas_key_ok, content_ok)

        # 파일 2: NAS 부재 (→ preserved)
        content_fail = b"fail parquet not on NAS for count test xyz"
        rel_fail = "exchange=bithumb/symbol=ETH/tier=L1/date=2024-02-02/part-001.parquet"
        local_fail = _make_legacy_parquet(tmp_path, rel_fail, content_fail)
        # NAS에 업로드 안 함

        result = scan_and_cleanup_legacy(tmp_path, nas_uploader)

        # cleaned >= 1 (파일 1), preserved >= 1 (파일 2)
        assert result["cleaned"] >= 1, f"cleaned >= 1 기대, got {result}"
        assert result["preserved"] >= 1, f"preserved >= 1 기대, got {result}"
        assert result["errors"] == 0

        # 파일 1: local 삭제됨 (cleaned)
        assert not local_ok.exists(), "NAS match 파일은 local 삭제 의무"
        # 파일 2: local 보존됨 (preserved)
        assert local_fail.exists(), "NAS 부재 파일은 local 보존 의무 (INV-4)"
