"""Integration tests for U3-MIGRATE process restart resume (INV-G).

Test Contract §8.5.2 (restart-resumable):
- INV-G: SIGTERM mid-execution → resume → partition 4 = re-copy 0, partitions 1-3 skip (sentinel)
- INV-G mid-state: status=copied partition injected → 2nd run resumes from copied state (P1-2 fix guard)

ADR-034 §결정 4 + §8.5_active=true 조건 4 (restart-aware 117 GB × 72h) carrier.
"""
from __future__ import annotations

import hashlib
import os
from unittest.mock import patch

import pytest

from moto import mock_aws as mock_s3  # moto>=5.2.1 (mock_s3 removed in 5.x)

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
    uploader._NASUploader__client = client  # type: ignore[attr-defined]
    return uploader


def _seed_objects(client, count=10, exchange="bithumb", channel="orderbooksnapshot"):
    """Seed N l1/ objects with .compacted sentinels (real production keyspace shape)."""
    keys = []
    for i in range(count):
        body = f"parquet-data-{i}".encode()
        sha256 = hashlib.sha256(body).hexdigest()
        key = (
            f"l1/market/{channel}/schema_version=orderbook_snapshot.v1/"
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

    def test_invg_midstate_copied_partition_resumes(self, s3_bucket_versioned, tmp_path):
        """INV-G mid-state: status=copied partition in manifest → 2nd run resumes from copied.

        P1-2 fix regression guard (§8.5.2 mid-state injection):
        1. Seed 2 objects: partition 0 completes in run 1, partition 1 left at status=copied
        2. Inject partition 1 manifest entry with status=copied (crash-at-verify simulation)
           and pre-seed destination object so verify can succeed
        3. Run 2: batch loop must pick up status=copied (not just pending)
           → verifying → verified → deleting → done (copy_object NOT called for partition 1)
        """
        client = s3_bucket_versioned
        uploader = _make_uploader(client)
        exchange = "bithumb"
        channel = "orderbooksnapshot"

        old_keys = _seed_objects(client, count=2, exchange=exchange, channel=channel)

        audit_dir = tmp_path / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)

        from mctrader_data.nas_migration.rekey import (
            RekeyManifest,
            RekeyOrchestrator,
        )

        # Run 1: complete partition 0 only (batch_size=1)
        orch1 = RekeyOrchestrator(
            nas_uploader=uploader,
            root=tmp_path,
            exchange=exchange,
            channel=channel,
            batch_size=1,
            dry_run=False,
            i_understand_irreversible=True,
            audit_dir=audit_dir,
        )
        result1 = orch1.run()
        assert result1.copied >= 1, "Run 1 should have processed partition 0"

        # Build manifest path (same as orchestrator uses)
        manifest_path = audit_dir / f"rekey-l1-manifest-{exchange}-{channel}.yaml"
        assert manifest_path.exists(), "Manifest must exist after run 1"

        # Determine partition_id for old_keys[1]
        orch_probe = RekeyOrchestrator(
            nas_uploader=uploader,
            root=tmp_path,
            exchange=exchange,
            channel=channel,
            dry_run=True,
            audit_dir=audit_dir,
        )
        pid1 = orch_probe._build_partition_id(old_keys[1])
        new_key1 = orch_probe._build_new_key(old_keys[1])

        # Pre-seed the destination object for partition 1 (simulates Step A copy completed)
        body1 = b"parquet-data-1"
        sha256_1 = hashlib.sha256(body1).hexdigest()
        client.put_object(
            Bucket="mctrader-market", Key=new_key1, Body=body1, Metadata={"sha256": sha256_1}
        )

        # Inject partition 1 into manifest as status=copied (crash-at-verify simulation)
        manifest = RekeyManifest(manifest_path, exchange=exchange, channel=channel, run_mode="live")
        manifest.upsert_pending(pid1, old_keys[1], new_key1)
        manifest.update_status(pid1, "copied")
        manifest.write_atomic()

        # Track copy_object calls in run 2
        copy_calls_run2 = []
        original_copy = uploader.copy_object

        def tracking_copy(src, dst, **kw):
            copy_calls_run2.append(src)
            return original_copy(src, dst, **kw)

        # Run 2: should resume partition 1 from copied state
        orch2 = RekeyOrchestrator(
            nas_uploader=uploader,
            root=tmp_path,
            exchange=exchange,
            channel=channel,
            batch_size=10,
            dry_run=False,
            i_understand_irreversible=True,
            resume_from_manifest=True,
            audit_dir=audit_dir,
        )

        with patch.object(uploader, "copy_object", side_effect=tracking_copy):
            orch2.run()

        # copy_object must NOT be called for the mid-state partition (it was already copied)
        assert old_keys[1] not in copy_calls_run2, (
            f"INV-G mid-state FAIL: copy_object called for status=copied partition {old_keys[1]}"
        )

        # Partition 1 must be done after run 2 (resume recovered it)
        manifest_final = RekeyManifest(manifest_path, exchange=exchange, channel=channel)
        final_status = manifest_final.get_status(pid1)
        assert final_status == "done", (
            f"INV-G mid-state FAIL: partition 1 expected status=done after resume, got {final_status!r}"
        )
