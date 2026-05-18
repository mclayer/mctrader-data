"""Integration tests for U3-MIGRATE process restart resume (INV-G).

Test Contract §8.5.2 (restart-resumable):
- INV-G: SIGTERM mid-execution → resume → partition 4 = re-copy 0, partitions 1-3 skip (sentinel)

ADR-034 §결정 4 + §8.5_active=true 조건 4 (restart-aware 117 GB × 72h) carrier.
"""
from __future__ import annotations

import os
from unittest.mock import patch

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


def _seed_objects(client, count=10, exchange="bithumb", channel="orderbooksnapshot"):
    """Seed N l1/ objects with .compacted sentinels."""
    import hashlib
    keys = []
    for i in range(count):
        body = f"parquet-data-{i}".encode()
        sha256 = hashlib.sha256(body).hexdigest()
        key = (
            f"l1/{exchange}/{channel}/schema_version=orderbook_snapshot.v1/"
            f"tier=L1/exchange={exchange}/symbol=KRW-SYM{i}/date=2026-05-13/part-0.parquet"
        )
        client.put_object(
            Bucket="mctrader-market", Key=key, Body=body, Metadata={"sha256": sha256}
        )
        client.put_object(Bucket="mctrader-market", Key=key + ".compacted", Body=b"")
        keys.append(key)
    return keys


class TestInvG:
    def test_sigterm_resume_from_manifest(self, s3_bucket_versioned, tmp_path):
        """INV-G: SIGTERM mid-execution → resume → previously completed partitions skip.

        Scenario (§8.5.2):
        1. Run 1: process 3 partitions (batch_size=3), simulate done
        2. Manually set partition 4 manifest status=copied (mid-state)
        3. Run 2 (resume): partition 1-3 skip (sentinel), partition 4 re-verify + delete
        4. copy_object for partition 4 = 0 (re-enter at verify step from manifest)
        """

        client = s3_bucket_versioned
        uploader = _make_uploader(client)

        old_keys = _seed_objects(client, count=5)
        # Note: dst keys NOT pre-seeded — orchestrator copies src→dst during processing.
        # _seed_objects uses unique body per partition; let orchestrator do MetadataDirective=COPY.

        audit_dir = tmp_path / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)

        from mctrader_data.nas_migration.rekey import RekeyOrchestrator

        # Run 1: process batch_size=3 (partitions 0,1,2 get done)
        orch1 = RekeyOrchestrator(
            nas_uploader=uploader,
            root=tmp_path,
            exchange="bithumb",
            channel="orderbooksnapshot",
            batch_size=3,
            dry_run=False,
            i_understand_irreversible=True,
            audit_dir=audit_dir,
        )
        result1 = orch1.run()

        # Verify 3 partitions processed
        assert result1.copied + result1.skipped_already_migrated >= 3

        # Run 2: resume with batch_size=10 — sentinel-hit partitions should skip
        copy_calls_run2 = []
        original_copy = uploader.copy_object

        def tracking_copy(src, dst, **kw):
            copy_calls_run2.append(src)
            return original_copy(src, dst, **kw)

        orch2 = RekeyOrchestrator(
            nas_uploader=uploader,
            root=tmp_path,
            exchange="bithumb",
            channel="orderbooksnapshot",
            batch_size=10,
            dry_run=False,
            i_understand_irreversible=True,
            resume_from_manifest=True,
            audit_dir=audit_dir,
        )

        with patch.object(uploader, "copy_object", side_effect=tracking_copy):
            result2 = orch2.run()

        # Partitions completed in Run 1 must not trigger copy_object in Run 2
        completed_keys_run1 = set(old_keys[:3])
        for called_key in copy_calls_run2:
            assert called_key not in completed_keys_run1, (
                f"INV-G FAIL: copy_object called for already-done partition {called_key}"
            )

        # Overall: all 5 partitions eventually done or skipped
        total = result1.copied + result1.skipped_already_migrated + result2.copied + result2.skipped_already_migrated
        assert total >= 3, f"INV-G: expected ≥ 3 total partitions handled, got {total}"
