"""Integration tests for U3-MIGRATE concurrent pidfile flock block (INV-I).

Test Contract §8 INV-I:
- INV-I: concurrent script pidfile flock block (second instance exit code != 0)
- OpRiskArch O-R3: sentinel write race 완화

ADR-034 §결정 4 + §3.5 pidfile flock (O-R3) 정합.
"""
from __future__ import annotations

import os
import sys

import pytest

# fcntl is POSIX-only; tests that use flock are skipped on Windows
if sys.platform != "win32":
    import fcntl
else:
    fcntl = None  # type: ignore[assignment]

from moto import mock_aws as mock_s3  # moto>=5.2.1 (mock_s3 removed in 5.x)

import boto3


@pytest.fixture
def aws_credentials():
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
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


class TestInvI:
    @pytest.mark.skipif(sys.platform == "win32", reason="fcntl (flock) not available on Windows")
    def test_pidfile_flock_second_instance_blocks(self, s3_bucket_versioned, tmp_path):
        """INV-I: second instance cannot acquire pidfile flock → SystemExit(2).

        Simulates concurrent execution via manual flock on pidfile.
        """
        from mctrader_data.nas_migration.rekey import RekeyOrchestrator

        client = s3_bucket_versioned
        uploader = _make_uploader(client)
        audit_dir = tmp_path / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        pidfile = audit_dir / "rekey-l1-migration.pid"

        # First: acquire the flock manually (simulate running instance)
        first_fobj = open(pidfile, "w")  # noqa: SIM115 — must stay open for lock lifetime
        fcntl.flock(first_fobj.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        first_fobj.write(str(99999))
        first_fobj.flush()

        try:
            # Second instance: should fail with SystemExit(2)
            orch = RekeyOrchestrator(
                nas_uploader=uploader,
                root=tmp_path,
                exchange="bithumb",
                channel="orderbooksnapshot",
                dry_run=True,
                audit_dir=audit_dir,
                pidfile_path=pidfile,
            )
            with pytest.raises(SystemExit) as exc_info:
                orch.run()
            assert exc_info.value.code == 2, (
                f"INV-I FAIL: expected exit code 2 (pidfile locked), got {exc_info.value.code}"
            )
        finally:
            # Release first lock
            fcntl.flock(first_fobj.fileno(), fcntl.LOCK_UN)
            first_fobj.close()

    @pytest.mark.skipif(sys.platform == "win32", reason="fcntl (flock) not available on Windows")
    def test_pidfile_released_after_run(self, s3_bucket_versioned, tmp_path):
        """INV-I: pidfile flock released after successful run (no orphan lock)."""
        from mctrader_data.nas_migration.rekey import RekeyOrchestrator

        client = s3_bucket_versioned
        uploader = _make_uploader(client)
        audit_dir = tmp_path / "audit"

        orch = RekeyOrchestrator(
            nas_uploader=uploader,
            root=tmp_path,
            exchange="bithumb",
            channel="orderbooksnapshot",
            dry_run=True,
            audit_dir=audit_dir,
        )
        orch.run()

        # After run: pidfile lock must be released (can be acquired again)
        pidfile = audit_dir / "rekey-l1-migration.pid"
        if pidfile.exists():
            test_fobj = open(pidfile, "w")  # noqa: SIM115 — must stay open for lock lifetime
            try:
                # Should not raise BlockingIOError
                fcntl.flock(test_fobj.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(test_fobj.fileno(), fcntl.LOCK_UN)
            finally:
                test_fobj.close()

    def test_execute_without_irreversible_flag_exits(self, s3_bucket_versioned, tmp_path):
        """PL 결정 #9: --execute without --i-understand-this-is-irreversible → SystemExit(3)."""
        from mctrader_data.nas_migration.rekey import RekeyOrchestrator

        client = s3_bucket_versioned
        uploader = _make_uploader(client)
        audit_dir = tmp_path / "audit"

        orch = RekeyOrchestrator(
            nas_uploader=uploader,
            root=tmp_path,
            exchange="bithumb",
            channel="orderbooksnapshot",
            dry_run=False,  # execute mode
            i_understand_irreversible=False,  # gate NOT set
            audit_dir=audit_dir,
        )
        with pytest.raises(SystemExit) as exc_info:
            orch.run()
        assert exc_info.value.code == 3, (
            f"INV-I/PL#9 FAIL: expected exit 3, got {exc_info.value.code}"
        )
