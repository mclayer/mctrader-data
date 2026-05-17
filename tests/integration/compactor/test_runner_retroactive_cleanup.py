"""test_runner_retroactive_cleanup.py — Integration tests for scan_and_cleanup_legacy().

MCT-189 Phase 2 PR2: retroactive legacy parquet cleanup (runner.scan_and_cleanup_legacy).

Scenarios (6):
- test_legacy_with_nas_match_unlinks:           local + NAS(동일 sha256) → cleaned==1, local 부재 (INV-1 XOR)
- test_legacy_with_nas_missing_preserved:       local + NAS 부재 → preserved==1, local 존재 (INV-4 안전망)
- test_legacy_with_nas_sha256_mismatch_preserved: local + NAS sha256 mismatch → preserved==1, local 존재
- test_legacy_returns_correct_counts:           정상 + fail 혼합 → cleaned==1 + preserved==1 exact counts
- test_legacy_batch_limit_caps_sweep:           batch_limit=3, 5 files — 1차 sweep cleaned==3, 2차 sweep remaining==2
- test_legacy_l2_uses_flat_key_unlinks:        tier=L2 flat key → NAS match → cleaned==1 (L2 회귀 가드)
"""
from __future__ import annotations

import contextlib
import hashlib
import sys
from pathlib import Path

import boto3
import pytest

def _docker_unavailable_reason() -> str | None:
    """Docker daemon / 플랫폼 미가용 사유 return (가용 시 None).

    FIX-MCT-180 data#67 P1: pytest.importorskip("testcontainers") 는 패키지
    설치만 검사 — Docker daemon / 플랫폼(Linux socket mount) 미검사로 CI
    windows-latest 에서 `-m "not slow"` 가 integration 마커를 deselect 하지
    않아 Docker socket mount 불가 FAIL. testcontainers Docker boundary 는
    Linux runner 전용.
    """
    if sys.platform == "win32":
        return "testcontainers Docker boundary requires Linux runner (win32 skip)"
    try:
        import docker  # type: ignore[import-untyped]

        docker.from_env().ping()
    except Exception as exc:  # noqa: BLE001 — Docker 미가용 사유 무관 일괄 skip
        return f"Docker daemon unavailable: {exc!r}"
    return None


# ─── module-scope fixtures (spin-up MinIO once per module) ───────────────────


@pytest.fixture(scope="module")
def minio_container():
    """Module-scope MinIO testcontainer."""
    _docker_skip = _docker_unavailable_reason()
    if _docker_skip is not None:
        pytest.skip(_docker_skip)
    from testcontainers.minio import MinioContainer  # type: ignore[import-untyped]

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
        """local parquet + NAS PUT(동일 sha256) → cleaned==1, local 부재 (INV-1 XOR)."""
        from mctrader_data.compactor.runner import scan_and_cleanup_legacy

        content = b"legacy parquet content matching NAS"
        rel = "exchange=upbit/symbol=BTC/tier=L1/date=2024-01-01/part-001.parquet"
        local = _make_legacy_parquet(tmp_path, rel, content)

        nas_key = f"l1/market/{rel}"
        _put_object(minio_client, nas_key, content)

        result = scan_and_cleanup_legacy(tmp_path, nas_uploader)

        assert result["cleaned"] == 1
        assert result["preserved"] == 0
        assert result["errors"] == 0
        assert "batch_limit" in result
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

        assert result["cleaned"] == 0
        assert result["preserved"] == 1
        assert result["errors"] == 0
        assert "batch_limit" in result
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

        nas_key = f"l1/market/{rel}"
        _put_object(minio_client, nas_key, nas_content)

        result = scan_and_cleanup_legacy(tmp_path, nas_uploader)

        assert result["cleaned"] == 0
        assert result["preserved"] == 1
        assert result["errors"] == 0
        assert "batch_limit" in result
        assert local.exists(), "INV-4: sha256 mismatch 시 local 보존 의무"

    def test_legacy_returns_correct_counts(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """정상(NAS match) + fail(NAS 부재) 혼합 → cleaned==1 + preserved==1 exact counts."""
        from mctrader_data.compactor.runner import scan_and_cleanup_legacy, _LEGACY_BATCH_DEFAULT

        # 파일 1: NAS match (→ cleaned)
        content_ok = b"ok parquet content for count test abc"
        rel_ok = "exchange=bithumb/symbol=BTC/tier=L1/date=2024-02-01/part-001.parquet"
        local_ok = _make_legacy_parquet(tmp_path, rel_ok, content_ok)
        nas_key_ok = f"l1/market/{rel_ok}"
        _put_object(minio_client, nas_key_ok, content_ok)

        # 파일 2: NAS 부재 (→ preserved)
        content_fail = b"fail parquet not on NAS for count test xyz"
        rel_fail = "exchange=bithumb/symbol=ETH/tier=L1/date=2024-02-02/part-001.parquet"
        local_fail = _make_legacy_parquet(tmp_path, rel_fail, content_fail)
        # NAS에 업로드 안 함

        result = scan_and_cleanup_legacy(tmp_path, nas_uploader)

        assert result["cleaned"] == 1
        assert result["preserved"] == 1
        assert result["errors"] == 0
        assert result["batch_limit"] == _LEGACY_BATCH_DEFAULT

        assert not local_ok.exists(), "NAS match 파일은 local 삭제 의무"
        assert local_fail.exists(), "NAS 부재 파일은 local 보존 의무 (INV-4)"

    def test_legacy_batch_limit_caps_sweep(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """batch_limit=3, NAS-matching files 5개 — 1차 sweep=3, 2차 sweep=2 (자체 페이싱 검증).

        cursor 별도 불요 — unlink된 file은 glob 결과에서 자연 사라져
        다음 batch 가 나머지 picks up.
        """
        from mctrader_data.compactor.runner import scan_and_cleanup_legacy

        # NAS-matching parquet 5개 준비 (모두 cleaned 대상)
        files: list[Path] = []
        for i in range(5):
            content = f"batch cap test content file {i} padding".encode()
            rel = f"exchange=upbit/symbol=BATCH{i}/tier=L1/date=2024-03-01/part-001.parquet"
            local = _make_legacy_parquet(tmp_path, rel, content)
            nas_key = f"l1/market/{rel}"
            _put_object(minio_client, nas_key, content)
            files.append(local)

        # 1차 sweep: batch_limit=3 → 최대 3개 처리
        r1 = scan_and_cleanup_legacy(tmp_path, nas_uploader, batch_limit=3)

        assert r1["batch_limit"] == 3
        assert r1["cleaned"] == 3, f"1차 sweep: batch_limit=3 → cleaned==3 기대. got {r1}"
        assert r1["preserved"] == 0
        assert r1["errors"] == 0

        # 1차 sweep 후 unlinked file 3개 부재 확인
        deleted_count = sum(1 for f in files if not f.exists())
        assert deleted_count == 3, f"1차 sweep 후 3개 삭제 기대. deleted={deleted_count}"

        # 2차 sweep: 나머지 2개 처리 (batch_limit=3이지만 남은 파일 2개)
        r2 = scan_and_cleanup_legacy(tmp_path, nas_uploader, batch_limit=3)

        assert r2["cleaned"] == 2, f"2차 sweep: 나머지 2개 cleaned 기대. got {r2}"
        assert r2["preserved"] == 0
        assert r2["errors"] == 0

        # 모든 파일 삭제 확인
        remaining = [f for f in files if f.exists()]
        assert remaining == [], f"모든 파일 삭제 의무. remaining={remaining}"

    def test_legacy_l2_uses_flat_key_unlinks(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """tier=L2 는 평면 key (l1/ prefix 미부착) → NAS match 시 cleaned==1 (회귀 가드).

        _dispatch_dual_write 가 L2/L3 를 평면 relative_to(root) 로 PUT 하므로
        cleanup 도 평면 키로 조회해야 한다 (tier-aware 분기 L2 경로 박제).
        """
        from mctrader_data.compactor.runner import scan_and_cleanup_legacy

        content = b"legacy L2 parquet flat-key scheme"
        rel = (
            "orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L2/"
            "exchange=bithumb/symbol=KRW-SOL/date=2026-05-17/hour=22/node=MERGED/part-l2flat.parquet"
        )
        local = _make_legacy_parquet(tmp_path, rel, content)

        nas_key = f"market/{rel}"  # 평면 — l1/ prefix 없음
        _put_object(minio_client, nas_key, content)

        result = scan_and_cleanup_legacy(tmp_path, nas_uploader)

        assert result["cleaned"] == 1
        assert result["preserved"] == 0
        assert result["errors"] == 0
        assert not local.exists(), "L2 평면 키 NAS match → local 삭제 (INV-1 XOR)"
