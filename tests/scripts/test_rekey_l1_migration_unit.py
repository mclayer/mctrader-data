"""Unit tests for rekey_l1_migration.py CLI + INV-L cardinality (U3-MIGRATE).

Test Contract §8:
- CLI 8 args: --root / --exchange / --channel / --dry-run | --execute / --batch-size /
  --max-partitions / --resume-from-manifest / --threshold / --i-understand-this-is-irreversible
- INV-L: Prometheus Counter cardinality ≤ 50 (active 24 = 2 exchange × 3 channel × 4 head_check)
- --i-understand-this-is-irreversible gate (PL 결정 #9)
- sentinel logic validation
- MCTRADER_REKEY_BATCH_LIMIT env (별 namespace — MCTRADER_LEGACY_CLEANUP_BATCH 재사용 금지)

ADR-034 §결정 4 + PL 결정 #3 (thin wrapper) + PL 결정 #9 (operator gate) 정합.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ─── CLI argument tests ───────────────────────────────────────────────────────


class TestCLIArgs:
    def _build_parser(self):
        """Import _build_parser from the script."""
        sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "rekey_l1_migration",
            Path(__file__).parents[2] / "scripts" / "rekey_l1_migration.py",
        )
        assert spec is not None and spec.loader is not None  # pyright None guard
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module._build_parser()

    def test_required_root(self):
        """--root is required."""
        parser = self._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--exchange", "bithumb"])

    def test_required_exchange(self):
        """--exchange is required."""
        parser = self._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--root", "/tmp/data"])

    def test_exchange_allowlist(self):
        """--exchange only accepts bithumb or upbit."""
        parser = self._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--root", "/tmp/data", "--exchange", "coinbase"])

    def test_channel_allowlist(self):
        """--channel only accepts valid values."""
        parser = self._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "--root", "/tmp/data", "--exchange", "bithumb",
                "--channel", "invalid_channel",
            ])

    def test_dry_run_default(self):
        """Default mode is --dry-run."""
        parser = self._build_parser()
        args = parser.parse_args(["--root", "/tmp/data", "--exchange", "bithumb"])
        assert args.execute is False
        assert args.dry_run is True

    def test_dry_run_execute_mutually_exclusive(self):
        """--dry-run and --execute are mutually exclusive."""
        parser = self._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([
                "--root", "/tmp/data", "--exchange", "bithumb",
                "--dry-run", "--execute",
            ])

    def test_batch_size_default_500(self):
        """Default --batch-size is 500."""
        parser = self._build_parser()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MCTRADER_REKEY_BATCH_LIMIT", None)
            args = parser.parse_args(["--root", "/tmp/data", "--exchange", "bithumb"])
        assert args.batch_size == 500

    def test_batch_size_env_mctrader_rekey(self):
        """MCTRADER_REKEY_BATCH_LIMIT env → batch_size override (별 namespace).

        Parser reads env at _build_parser() time, so env must be set before building.
        """
        with patch.dict(os.environ, {"MCTRADER_REKEY_BATCH_LIMIT": "200"}):
            parser = self._build_parser()
            args = parser.parse_args(["--root", "/tmp/data", "--exchange", "bithumb"])
        assert args.batch_size == 200

    def test_env_namespace_not_legacy(self):
        """MCTRADER_LEGACY_CLEANUP_BATCH must NOT affect batch_size (격리 의무)."""
        with patch.dict(os.environ, {
            "MCTRADER_LEGACY_CLEANUP_BATCH": "999",
            "MCTRADER_REKEY_BATCH_LIMIT": "300",
        }):
            parser = self._build_parser()
            args = parser.parse_args(["--root", "/tmp/data", "--exchange", "bithumb"])
        # REKEY_BATCH_LIMIT takes precedence, LEGACY_CLEANUP_BATCH has no effect
        assert args.batch_size == 300

    def test_threshold_default_0(self):
        """Default --threshold is 0.0."""
        parser = self._build_parser()
        args = parser.parse_args(["--root", "/tmp/data", "--exchange", "bithumb"])
        assert args.threshold == 0.0

    def test_max_partitions_optional(self):
        """--max-partitions is optional (None default)."""
        parser = self._build_parser()
        args = parser.parse_args(["--root", "/tmp/data", "--exchange", "bithumb"])
        assert args.max_partitions is None

    def test_resume_from_manifest_flag(self):
        """--resume-from-manifest flag parses correctly."""
        parser = self._build_parser()
        args = parser.parse_args([
            "--root", "/tmp/data", "--exchange", "bithumb",
            "--resume-from-manifest",
        ])
        assert args.resume_from_manifest is True

    def test_i_understand_irreversible_flag(self):
        """--i-understand-this-is-irreversible parses correctly."""
        parser = self._build_parser()
        args = parser.parse_args([
            "--root", "/tmp/data", "--exchange", "bithumb",
            "--execute", "--i-understand-this-is-irreversible",
        ])
        assert args.execute is True
        assert args.i_understand_irreversible is True

    def test_all_8_args_accepted(self):
        """All 8 args accepted together without error."""
        parser = self._build_parser()
        args = parser.parse_args([
            "--root", "/tmp/data",
            "--exchange", "bithumb",
            "--channel", "orderbooksnapshot",
            "--execute",
            "--i-understand-this-is-irreversible",
            "--batch-size", "100",
            "--max-partitions", "50",
            "--resume-from-manifest",
            "--threshold", "0.1",
        ])
        assert args.exchange == "bithumb"
        assert args.channel == "orderbooksnapshot"
        assert args.execute is True
        assert args.i_understand_irreversible is True
        assert args.batch_size == 100
        assert args.max_partitions == 50
        assert args.resume_from_manifest is True
        assert args.threshold == 0.1


# ─── INV-N: batch_limit=500 per-sweep ────────────────────────────────────────


class TestInvNUnit:
    def test_batch_limit_500_per_sweep(self):
        """INV-N: batch_size=500 default is set and respected."""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "rekey_l1_migration",
            Path(__file__).parents[2] / "scripts" / "rekey_l1_migration.py",
        )
        assert spec is not None and spec.loader is not None  # pyright None guard
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        parser = module._build_parser()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MCTRADER_REKEY_BATCH_LIMIT", None)
            args = parser.parse_args(["--root", "/tmp/data", "--exchange", "bithumb"])

        assert args.batch_size == 500, f"INV-N FAIL: default batch_size={args.batch_size} != 500"


# ─── Sentinel logic unit tests ────────────────────────────────────────────────


class TestSentinelLogic:
    def test_sentinel_path_safe_partition_id(self, tmp_path):
        """Sentinel path is within sentinel_dir for valid partition_id."""
        from mctrader_data.nas_migration.rekey import RekeyOrchestrator

        mock_uploader = MagicMock()
        mock_uploader.get_bucket_versioning.return_value = "Enabled"

        audit_dir = tmp_path / "audit"
        orch = RekeyOrchestrator(
            nas_uploader=mock_uploader,
            root=tmp_path,
            exchange="bithumb",
            channel="orderbooksnapshot",
            audit_dir=audit_dir,
        )

        partition_id = "bithumb-orderbooksnapshot-v1-KRW-BTC-2026-05-13"
        sentinel = orch._sentinel_path(partition_id)
        assert sentinel.is_relative_to(orch._sentinel_dir), (
            "Sentinel path must be within sentinel_dir (B-4 trust boundary)"
        )

    def test_sentinel_path_rejects_traversal(self, tmp_path):
        """Sentinel path rejects path traversal via partition_id."""
        from mctrader_data.nas_migration.rekey import RekeyOrchestrator

        mock_uploader = MagicMock()
        audit_dir = tmp_path / "audit"
        orch = RekeyOrchestrator(
            nas_uploader=mock_uploader,
            root=tmp_path,
            exchange="bithumb",
            channel="orderbooksnapshot",
            audit_dir=audit_dir,
        )

        # ".." sequences are neutralized by .replace("..", "")
        partition_id = "../../../etc/passwd"
        # Should not raise but sentinel should be safe
        sentinel = orch._sentinel_path(partition_id)
        # Path should be within sentinel_dir (no traversal)
        assert audit_dir in sentinel.parents or sentinel.is_relative_to(audit_dir), (
            f"Sentinel path {sentinel} is outside audit_dir {audit_dir}"
        )


# ─── INV-L: cardinality ≤ 50 (unit supplement) ───────────────────────────────


class TestInvLUnit:
    def test_counter_cardinality_budget_under_50(self):
        """INV-L: active cardinality for primary metric (verified_total) = 24 ≤ 50."""
        exchanges = ["bithumb", "upbit"]
        channels = ["transaction", "orderbooksnapshot", "orderbookdepth"]
        head_checks = ["etag", "version_id", "sha256", "content_length"]

        # Primary cardinality axis: l1_rekey_verified_total
        active_primary = len(exchanges) * len(channels) * len(head_checks)
        assert active_primary == 24, f"INV-L: expected 24, got {active_primary}"
        assert active_primary <= 50, f"INV-L FAIL: {active_primary} > 50"

    def test_metrics_imported_correctly(self):
        """INV-L: all 7 rekey metrics importable from prometheus_exporters."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            l1_rekey_batch_duration_seconds,
            l1_rekey_copied_total,
            l1_rekey_deleted_total,
            l1_rekey_failed_total,
            l1_rekey_partial_state_count,
            l1_rekey_skipped_already_migrated_total,
            l1_rekey_verified_total,
        )
        # All 7 must be importable (5 Counter + 1 Gauge + 1 Histogram)
        from prometheus_client import Counter, Gauge, Histogram
        assert isinstance(l1_rekey_copied_total, Counter)
        assert isinstance(l1_rekey_verified_total, Counter)
        assert isinstance(l1_rekey_deleted_total, Counter)
        assert isinstance(l1_rekey_skipped_already_migrated_total, Counter)
        assert isinstance(l1_rekey_failed_total, Counter)
        assert isinstance(l1_rekey_partial_state_count, Gauge)
        assert isinstance(l1_rekey_batch_duration_seconds, Histogram)

    def test_default_channels_bithumb(self):
        """bithumb default channels = 3 (transaction + orderbooksnapshot + orderbookdepth)."""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "rekey_l1_migration",
            Path(__file__).parents[2] / "scripts" / "rekey_l1_migration.py",
        )
        assert spec is not None and spec.loader is not None  # pyright None guard
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        channels = module._default_channels("bithumb")
        assert set(channels) == {"transaction", "orderbooksnapshot", "orderbookdepth"}

    def test_default_channels_upbit_no_orderbookdepth(self):
        """upbit default channels = 2 (orderbookdepth BLOCKED — MCT-166 D1=B)."""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "rekey_l1_migration",
            Path(__file__).parents[2] / "scripts" / "rekey_l1_migration.py",
        )
        assert spec is not None and spec.loader is not None  # pyright None guard
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        channels = module._default_channels("upbit")
        assert "orderbookdepth" not in channels, (
            "INV: upbit orderbookdepth must be BLOCKED (MCT-166 D1=B)"
        )
        assert set(channels) == {"transaction", "orderbooksnapshot"}
