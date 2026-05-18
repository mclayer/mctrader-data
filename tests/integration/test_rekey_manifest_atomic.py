"""Integration tests for U3-MIGRATE Manifest atomic write (INV-H).

Test Contract §8 INV-H:
- Manifest YAML atomic write (tempfile + os.replace, disk-full mock, partial YAML 0)
- SecurityArch M-3: Manifest YAML mid-execution corruption 완화

ADR-034 §결정 4 + §3.5 Manifest layout 정합.
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml


class TestInvH:
    def test_atomic_write_no_partial_yaml(self, tmp_path):
        """INV-H: Manifest atomic write — partial YAML 0 (tempfile + os.replace)."""
        from mctrader_data.nas_migration.rekey import RekeyManifest

        manifest_path = tmp_path / "audit" / "rekey-l1-manifest-bithumb-orderbooksnapshot.yaml"
        manifest = RekeyManifest(manifest_path, "bithumb", "orderbooksnapshot")
        manifest.upsert_pending(
            "part-0",
            old_key="l1/bithumb/orderbooksnapshot/part-0.parquet",
            new_key="bithumb/orderbooksnapshot/part-0.parquet",
        )
        manifest.write_atomic()

        assert manifest_path.exists(), "INV-H FAIL: Manifest not created"

        # Verify valid YAML (no partial write artifacts)
        content = manifest_path.read_text(encoding="utf-8")
        data = yaml.safe_load(content)
        assert isinstance(data, dict), "INV-H FAIL: YAML parse error (partial write?)"
        assert "partitions" in data, "INV-H FAIL: 'partitions' key missing"

    def test_atomic_write_no_tmp_file_on_success(self, tmp_path):
        """INV-H: .tmp file removed after successful atomic write."""
        from mctrader_data.nas_migration.rekey import RekeyManifest

        manifest_path = tmp_path / "audit" / "rekey-l1-manifest-bithumb-transaction.yaml"
        manifest = RekeyManifest(manifest_path, "bithumb", "transaction")
        manifest.upsert_pending("p0", "l1/bithumb/transaction/p0.parquet", "bithumb/transaction/p0.parquet")
        manifest.write_atomic()

        tmp_path_expected = manifest_path.with_suffix(".yaml.tmp")
        assert not tmp_path_expected.exists(), "INV-H FAIL: .tmp file not removed after atomic write"

    def test_atomic_write_disk_full_preserves_old(self, tmp_path):
        """INV-H: disk-full mock → old YAML preserved (os.replace not reached).

        write_atomic uses Path.write_text on the .tmp file, then os.replace.
        We mock Path.write_text to raise OSError (disk full simulation).
        """
        from mctrader_data.nas_migration.rekey import RekeyManifest

        manifest_path = tmp_path / "audit" / "rekey-l1-manifest-bithumb-orderbooksnapshot.yaml"

        # Write initial valid YAML
        manifest = RekeyManifest(manifest_path, "bithumb", "orderbooksnapshot")
        manifest.upsert_pending("p0", "l1/old.parquet", "new.parquet")
        manifest.write_atomic()
        original_content = manifest_path.read_text(encoding="utf-8")

        # Simulate disk-full: patch Path.write_text to fail on .tmp file only
        manifest2 = RekeyManifest(manifest_path, "bithumb", "orderbooksnapshot")
        manifest2.upsert_pending("p1", "l1/old2.parquet", "new2.parquet")

        original_write_text = Path.write_text

        def fail_on_tmp(self_path, *args, **kwargs):
            if str(self_path).endswith(".tmp"):
                raise OSError("No space left on device")
            return original_write_text(self_path, *args, **kwargs)

        with patch.object(Path, "write_text", fail_on_tmp), pytest.raises(OSError):
            manifest2.write_atomic()

        # Original YAML must be preserved
        preserved_content = manifest_path.read_text(encoding="utf-8")
        assert preserved_content == original_content, (
            "INV-H FAIL: original YAML corrupted by disk-full failure"
        )

    def test_atomic_write_fsync_called(self, tmp_path):
        """INV-H: os.fsync called during atomic write (durability guarantee)."""
        from mctrader_data.nas_migration.rekey import RekeyManifest

        manifest_path = tmp_path / "audit" / "rekey-l1-manifest-upbit-orderbooksnapshot.yaml"
        manifest = RekeyManifest(manifest_path, "upbit", "orderbooksnapshot")
        manifest.upsert_pending("p0", "l1/upbit/p0.parquet", "upbit/p0.parquet")

        fsync_calls = []
        original_fsync = os.fsync

        def tracking_fsync(fd):
            fsync_calls.append(fd)
            return original_fsync(fd)

        with patch("os.fsync", side_effect=tracking_fsync):
            manifest.write_atomic()

        assert len(fsync_calls) >= 1, "INV-H FAIL: os.fsync not called during atomic write"

    def test_status_counts_14_keys_in_yaml(self, tmp_path):
        """INV-H + INV-D: status_counts YAML contains all 14 keys."""
        from mctrader_data.nas_migration.rekey import RekeyManifest

        manifest_path = tmp_path / "audit" / "rekey-l1-manifest-bithumb-transaction.yaml"
        manifest = RekeyManifest(manifest_path, "bithumb", "transaction")
        manifest.write_atomic()

        data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        sc = data["status_counts"]

        required_keys = [
            "pending", "copying", "copied", "verifying", "verified",
            "deleting", "deleted", "done", "failed", "legacy_no_sha256", "rolled_back",
            "skipped_already_migrated", "skipped_already_copied", "skipped_not_compacted",
        ]
        assert len(required_keys) == 14, "schema sanity: must be exactly 14 keys"
        for k in required_keys:
            assert k in sc, f"INV-H/D FAIL: status_counts missing key '{k}'"
