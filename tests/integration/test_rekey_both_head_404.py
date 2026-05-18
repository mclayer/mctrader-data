"""Integration tests for U3-MIGRATE P0-1: both_head_404 guard.

Change Plan §11.6:986-1003 decision matrix:
  source_404 + target_404 (both_head_404) → status=failed + reason="both_head_404"
    + P0 alert (mctrader_l1_rekey_failed_total{reason="both_head_404"} +1)
    + sentinel write 금지

Test:
- test_both_head_404_yields_failed_not_done: source deleted before copy + dst absent
  → final status = failed (NOT done), Counter{reason="both_head_404"} >= 1
- test_source_404_target_200_yields_skipped: source deleted but dst present + sha256 match
  → final status = done (skipped_already_migrated path)
"""
from __future__ import annotations

import hashlib
import os

import pytest

try:
    from moto import mock_s3
except ImportError:
    from moto import mock_aws as mock_s3

import boto3


@pytest.fixture
def aws_credentials():
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
    os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def mock_s3_client(aws_credentials):
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        yield client


@pytest.fixture
def s3_bucket_versioned(mock_s3_client):
    mock_s3_client.create_bucket(Bucket="mctrader-market")
    mock_s3_client.put_bucket_versioning(
        Bucket="mctrader-market",
        VersioningConfiguration={"Status": "Enabled"},
    )
    yield mock_s3_client


def _make_uploader(client):
    from mctrader_data.nas_storage.nas_uploader import NASUploader
    uploader = NASUploader(endpoint="http://localhost:9000", access_key="t", secret_key="t")
    uploader._NASUploader__client = client
    return uploader


class TestBothHead404Guard:
    """P0-1 guard tests: source_not_found branch decision matrix (§11.6:986-1003).

    Both tests simulate a partition that was discovered in a prior sweep (manifest has it
    as pending) but when the copy attempt executes, the source is already gone.
    We inject the partition directly into the manifest to decouple from _discover_l1_objects
    (which filters to only keys that still exist in the bucket).
    """

    def _build_orch(self, uploader, tmp_path, exchange, channel, audit_dir):
        from mctrader_data.nas_migration.rekey import RekeyOrchestrator
        return RekeyOrchestrator(
            nas_uploader=uploader,
            root=tmp_path,
            exchange=exchange,
            channel=channel,
            batch_size=10,
            dry_run=False,
            i_understand_irreversible=True,
            audit_dir=audit_dir,
        )

    def test_both_head_404_yields_failed_not_done(self, s3_bucket_versioned, tmp_path):
        """P0-1: source deleted (404) + dst absent (404) → status=failed, sentinel write 금지.

        Change Plan §11.6:1000-1003 verbatim:
          source_404 + target_404 (both_head_404)
            → status=failed + reason="both_head_404" + P0 alert

        Simulation: inject partition directly into manifest as pending, source absent from S3
        (simulates: partition discovered in prior sweep, source concurrently deleted before copy).
        """
        client = s3_bucket_versioned
        uploader = _make_uploader(client)
        exchange = "bithumb"
        channel = "orderbooksnapshot"

        old_key = (
            f"l1/{exchange}/{channel}/schema_version=orderbook_snapshot.v1/"
            f"tier=L1/exchange={exchange}/symbol=KRW-BTC/date=2026-05-13/part-0.parquet"
        )
        new_key = old_key[len("l1/"):]

        # Source is NOT put into S3 — simulates source already deleted
        # (source_not_found when copy_object tries HEAD or server-side copy)

        from mctrader_data.nas_migration.rekey import RekeyManifest

        audit_dir = tmp_path / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)

        # Build orchestrator to get partition_id helper
        orch = self._build_orch(uploader, tmp_path, exchange, channel, audit_dir)
        pid = orch._build_partition_id(old_key)

        # Pre-inject partition into manifest as pending (simulates prior discovery sweep)
        manifest_path = audit_dir / f"rekey-l1-manifest-{exchange}-{channel}.yaml"
        manifest = RekeyManifest(manifest_path, exchange=exchange, channel=channel, run_mode="live")
        manifest.upsert_pending(pid, old_key, new_key)
        manifest.write_atomic()

        # Capture failed counter labels
        captured_reasons: list[str] = []
        original_labels = orch._m_failed.labels

        def tracking_labels(**kwargs):
            captured_reasons.append(kwargs.get("reason", ""))
            return original_labels(**kwargs)

        orch._m_failed.labels = tracking_labels  # type: ignore[method-assign]

        result = orch.run()

        # Must record at least 1 failure (both_head_404)
        assert result.failed >= 1, (
            f"P0-1 FAIL: expected failed >= 1 for both_head_404, got failed={result.failed}"
        )

        # Counter must be emitted with reason="both_head_404"
        assert "both_head_404" in captured_reasons, (
            f"P0-1 FAIL: expected reason='both_head_404' in counter labels, got {captured_reasons}"
        )

        # Final manifest status = failed (NOT done)
        manifest_final = RekeyManifest(manifest_path, exchange=exchange, channel=channel)
        final_status = manifest_final.get_status(pid)
        assert final_status == "failed", (
            f"P0-1 FAIL: expected status=failed for both_head_404, got {final_status!r}"
        )

        # Sentinel must NOT exist (sentinel write 금지 per §11.6:1002)
        assert not orch._sentinel_exists(pid), (
            "P0-1 FAIL: sentinel must NOT exist for both_head_404 partition"
        )

    def test_source_404_target_200_sha256_match_yields_skipped(self, s3_bucket_versioned, tmp_path):
        """source_404 + target_200 + sha256 match → status=done (skipped_already_migrated).

        Change Plan §11.6:986-989:
          source_404 + target_200 (sha256 match) → skipped_already_migrated, sentinel write OK

        Simulation: inject partition into manifest as pending, source absent, dst present in S3.
        """
        client = s3_bucket_versioned
        uploader = _make_uploader(client)
        exchange = "bithumb"
        channel = "orderbooksnapshot"

        body = b"test-parquet-migrated"
        sha256 = hashlib.sha256(body).hexdigest()
        old_key = (
            f"l1/{exchange}/{channel}/schema_version=orderbook_snapshot.v1/"
            f"tier=L1/exchange={exchange}/symbol=KRW-ETH/date=2026-05-13/part-0.parquet"
        )
        new_key = old_key[len("l1/"):]

        # Seed ONLY the destination (source is absent — migration already done in prior run)
        client.put_object(
            Bucket="mctrader-market", Key=new_key, Body=body, Metadata={"sha256": sha256}
        )

        from mctrader_data.nas_migration.rekey import RekeyManifest

        audit_dir = tmp_path / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)

        orch = self._build_orch(uploader, tmp_path, exchange, channel, audit_dir)
        pid = orch._build_partition_id(old_key)

        # Pre-inject partition into manifest as pending
        manifest_path = audit_dir / f"rekey-l1-manifest-{exchange}-{channel}.yaml"
        manifest = RekeyManifest(manifest_path, exchange=exchange, channel=channel, run_mode="live")
        manifest.upsert_pending(pid, old_key, new_key)
        manifest.write_atomic()

        result = orch.run()

        # No failures
        assert result.failed == 0, (
            f"source_404+target_200 should not fail, got failed={result.failed}"
        )

        # Manifest shows done (skipped_already_migrated path)
        manifest_final = RekeyManifest(manifest_path, exchange=exchange, channel=channel)
        final_status = manifest_final.get_status(pid)
        assert final_status == "done", (
            f"source_404+target_200 should yield done, got {final_status!r}"
        )
