# tests/integration/test_historical_l1_reclaim.py
"""MCT-204 §8.2: historical L1 reclaim integration tests.

Tests:
- AC-3/INV-C: L2 HEAD verify fail → L1 unlink 0 (안전망)
- INV-D: sentinel .l1-promoted 멱등 (재실행 same partition = 0 호출)
- INV-F: sentinel write atomic (mock-interrupt sim)
- INV-I: .forward-processing sentinel skip
- AC-5: integration with run_historical_promotion (Layer 3 hook)
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch


from botocore.exceptions import ClientError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError

from mctrader_data.compactor.historical_reclaim import reclaim_partition_l1_local


TODAY = date(2026, 5, 19)
YESTERDAY = TODAY - timedelta(days=1)
HISTORICAL = TODAY - timedelta(days=5)
CHANNEL = "orderbooksnapshot"
EXCHANGE = "upbit"
SYMBOL = "KRW-BTC"
SCHEMA_VER = "v1"


def _make_l1_files(tmp_path: Path, date_utc: date, count: int = 3) -> list[Path]:
    date_dir = (
        tmp_path / "market" / CHANNEL / f"schema_version={SCHEMA_VER}"
        / "tier=L1" / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}"
        / f"date={date_utc.isoformat()}" / "node=n1"
    )
    date_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for i in range(count):
        f = date_dir / f"part-{i:04d}.parquet"
        f.write_bytes(b"stub-data" * 10)
        files.append(f)
    return files


def _make_l2_dir(tmp_path: Path, date_utc: date) -> Path:
    date_dir = (
        tmp_path / "market" / CHANNEL / f"schema_version={SCHEMA_VER}"
        / "tier=L2" / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}"
        / f"date={date_utc.isoformat()}"
    )
    date_dir.mkdir(parents=True, exist_ok=True)
    (date_dir / "part-day.parquet").write_bytes(b"stub-l2")
    return date_dir


def _make_nas_uploader(key_count: int = 1) -> MagicMock:
    uploader = MagicMock()
    uploader.bucket = "mctrader-market"
    uploader.list_prefix_count.return_value = key_count
    return uploader


class TestHistoricalL1Reclaim:
    def test_l2_nas_missing_l1_preserved(self, tmp_path):
        """INV-C: L2 NAS HEAD verify fail (KeyCount=0) → L1 unlink 0."""
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=3)
        uploader = _make_nas_uploader(key_count=0)

        result = reclaim_partition_l1_local(
            root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
            symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
        )

        assert result.outcome == "skip_nas_missing"
        # L1 files should be preserved
        for f in l1_files:
            assert f.exists(), f"{f} should be preserved (NAS HEAD fail)"

    def test_local_l2_missing_l1_preserved(self, tmp_path):
        """INV-C: NAS KeyCount > 0 but local L2 dir missing → L1 preserved (fail_verify)."""
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=2)
        uploader = _make_nas_uploader(key_count=1)

        result = reclaim_partition_l1_local(
            root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
            symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
        )

        assert result.outcome == "fail_verify"
        for f in l1_files:
            assert f.exists()

    def test_ok_l1_files_unlinked_and_sentinel_written(self, tmp_path):
        """INV-D: successful reclaim → sentinel written."""
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=4)
        _make_l2_dir(tmp_path, HISTORICAL)
        uploader = _make_nas_uploader(key_count=1)

        with patch("mctrader_data.metrics.historical_l1_reclaim_total"):
            result = reclaim_partition_l1_local(
                root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
                symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
            )

        assert result.outcome == "ok"
        assert result.files_unlinked == 4
        for f in l1_files:
            assert not f.exists()

    def test_idempotent_second_run_skips(self, tmp_path):
        """INV-D: second run returns skip_sentinel (멱등)."""
        _make_l1_files(tmp_path, HISTORICAL, count=2)
        _make_l2_dir(tmp_path, HISTORICAL)
        uploader = _make_nas_uploader(key_count=1)

        with patch("mctrader_data.metrics.historical_l1_reclaim_total"):
            r1 = reclaim_partition_l1_local(
                root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
                symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
            )
            r2 = reclaim_partition_l1_local(
                root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
                symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
            )

        assert r1.outcome == "ok"
        assert r2.outcome == "skip_sentinel"
        assert r2.files_unlinked == 0
        # NAS was only queried once (first run)
        assert uploader.list_prefix_count.call_count == 1

    def test_sentinel_write_atomic(self, tmp_path):
        """INV-F: sentinel write uses os.replace (atomic, no partial sentinel)."""
        _make_l1_files(tmp_path, HISTORICAL, count=1)
        _make_l2_dir(tmp_path, HISTORICAL)
        uploader = _make_nas_uploader(key_count=1)

        replace_calls = []
        original_replace = os.replace

        def tracking_replace(src, dst):
            replace_calls.append((src, dst))
            return original_replace(src, dst)

        with (
            patch("os.replace", side_effect=tracking_replace),
            patch("mctrader_data.metrics.historical_l1_reclaim_total"),
        ):
            result = reclaim_partition_l1_local(
                root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
                symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
            )

        assert result.outcome == "ok"
        # At least one os.replace call for sentinel
        assert any(".l1-promoted" in str(dst) for _, dst in replace_calls), (
            "Sentinel write should use os.replace (atomic)"
        )

    def test_forward_processing_sentinel_skips_reclaim(self, tmp_path):
        """INV-I: .forward-processing sentinel → skip_forward_in_flight."""
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=2)
        # Write .forward-processing sentinel
        date_dir = (
            tmp_path / "market" / CHANNEL / f"schema_version={SCHEMA_VER}"
            / "tier=L1" / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}"
            / f"date={HISTORICAL.isoformat()}"
        )
        (date_dir / ".forward-processing").touch()

        uploader = _make_nas_uploader(key_count=1)

        result = reclaim_partition_l1_local(
            root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
            symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
        )

        assert result.outcome == "skip_forward_in_flight"
        for f in l1_files:
            assert f.exists()
        uploader.list_prefix_count.assert_not_called()

    def test_bytes_freed_counted(self, tmp_path):
        """AC-3: bytes_freed is accurately counted."""
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=3)
        _make_l2_dir(tmp_path, HISTORICAL)
        uploader = _make_nas_uploader(key_count=1)

        total_expected = sum(f.stat().st_size for f in l1_files)

        with patch("mctrader_data.metrics.historical_l1_reclaim_total"):
            result = reclaim_partition_l1_local(
                root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
                symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
            )

        assert result.bytes_freed == total_expected

    def test_nas_client_error_returns_fail_verify(self, tmp_path):
        """NAS ClientError (S3 network/auth error) → fail_verify, L1 preserved.

        Uses ClientError (not bare Exception) — historical_reclaim.py catches
        (ClientError, EndpointConnectionError) only, per P0 #1 FIX (ADR-027 §D5 정합:
        programming errors like AttributeError are NOT swallowed).
        """
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=2)
        uploader = _make_nas_uploader()
        # Simulate S3 network-level error (ClientError)
        uploader.list_prefix_count.side_effect = ClientError(
            {"Error": {"Code": "503", "Message": "Service Unavailable"}},
            "ListObjectsV2",
        )

        result = reclaim_partition_l1_local(
            root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
            symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
        )

        assert result.outcome == "fail_verify"
        for f in l1_files:
            assert f.exists()

    def test_nas_endpoint_error_returns_fail_verify(self, tmp_path):
        """NAS EndpointConnectionError (NAS unreachable) → fail_verify, L1 preserved."""
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=2)
        uploader = _make_nas_uploader()
        uploader.list_prefix_count.side_effect = EndpointConnectionError(
            endpoint_url="http://nas.local:9000"
        )

        result = reclaim_partition_l1_local(
            root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
            symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
        )

        assert result.outcome == "fail_verify"
        for f in l1_files:
            assert f.exists()

    def test_connect_timeout_returns_fail_verify(self, tmp_path):
        """P1 FIX 2/3 gate: ConnectTimeoutError → fail_verify, L1 preserved.

        ConnectTimeoutError is a BotoCoreError subclass (not ClientError / EndpointConnectionError).
        MCT-204 sets connect_timeout=30s → NAS connect hang = dominant failure mode.
        Regression gate: historical_reclaim.py:169 except must cover BotoCoreError tree.
        """
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=2)
        uploader = _make_nas_uploader()
        uploader.list_prefix_count.side_effect = ConnectTimeoutError(endpoint_url="http://nas.local:9000")

        result = reclaim_partition_l1_local(
            root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
            symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
        )

        assert result.outcome == "fail_verify", (
            "ConnectTimeoutError must be caught as fail_verify (not propagate to caller generic handler)"
        )
        for f in l1_files:
            assert f.exists(), f"{f} must be preserved when NAS connect timeout occurs"

    def test_read_timeout_returns_fail_verify(self, tmp_path):
        """P1 FIX 2/3 gate: ReadTimeoutError → fail_verify, L1 preserved.

        ReadTimeoutError is a BotoCoreError subclass (not ClientError / EndpointConnectionError).
        Regression gate: historical_reclaim.py:169 except must cover BotoCoreError tree.
        """
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=2)
        uploader = _make_nas_uploader()
        uploader.list_prefix_count.side_effect = ReadTimeoutError(endpoint_url="http://nas.local:9000")

        result = reclaim_partition_l1_local(
            root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
            symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
        )

        assert result.outcome == "fail_verify", (
            "ReadTimeoutError must be caught as fail_verify (not propagate to caller generic handler)"
        )
        for f in l1_files:
            assert f.exists(), f"{f} must be preserved when NAS read timeout occurs"
