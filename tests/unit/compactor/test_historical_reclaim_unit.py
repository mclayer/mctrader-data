# tests/unit/compactor/test_historical_reclaim_unit.py
"""MCT-204 §8.1: reclaim_partition_l1_local unit tests.

Tests all 6 outcome branches:
- ok: L2 NAS HEAD verify pass → L1 files unlinked + sentinel written
- skip_sentinel: sentinel .l1-promoted already exists
- skip_today_window: date_utc >= now_snapshot-1
- skip_forward_in_flight: .forward-processing sentinel exists
- skip_nas_missing: L2 NAS list_objects_v2 KeyCount == 0
- fail_verify: local L2 date_dir missing
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mctrader_data.compactor.historical_reclaim import reclaim_partition_l1_local, _write_sentinel_atomic
from mctrader_data.nas_storage.nas_uploader import NASUploader

TODAY = date(2026, 5, 19)
YESTERDAY = TODAY - timedelta(days=1)
HISTORICAL = TODAY - timedelta(days=5)

CHANNEL = "orderbooksnapshot"
EXCHANGE = "upbit"
SYMBOL = "KRW-BTC"
SCHEMA_VER = "v1"


def _make_nas_uploader_mock(key_count: int = 1) -> MagicMock:
    """Create NASUploader mock with list_prefix_count returning key_count.

    Note: uses plain MagicMock() for backward-compat with existing tests.
    P1 #1 spec mock tests are in TestNASUploaderApiContractSpec below.
    """
    uploader = MagicMock()
    uploader.bucket = "mctrader-market"
    uploader.list_prefix_count.return_value = key_count
    return uploader


def _make_l1_files(tmp_path: Path, date_utc: date, count: int = 3) -> list[Path]:
    """Create stub L1 parquet files in the standard path."""
    date_dir = (
        tmp_path / "market" / CHANNEL / f"schema_version={SCHEMA_VER}"
        / "tier=L1" / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}"
        / f"date={date_utc.isoformat()}" / "node=n1"
    )
    date_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for i in range(count):
        f = date_dir / f"part-{i:04d}.parquet"
        f.write_bytes(b"stub" * 100)
        files.append(f)
    return files


def _make_l2_dir(tmp_path: Path, date_utc: date) -> Path:
    """Create stub L2 date_dir (local L2 presence)."""
    date_dir = (
        tmp_path / "market" / CHANNEL / f"schema_version={SCHEMA_VER}"
        / "tier=L2" / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}"
        / f"date={date_utc.isoformat()}"
    )
    date_dir.mkdir(parents=True, exist_ok=True)
    (date_dir / "part-day.parquet").write_bytes(b"stub-l2")
    return date_dir


class TestReclaimPartitionL1Local:
    def test_ok_l1_files_unlinked_sentinel_written(self, tmp_path):
        """ok: L2 NAS verify pass → L1 files unlinked + sentinel written."""
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=3)
        _make_l2_dir(tmp_path, HISTORICAL)
        uploader = _make_nas_uploader_mock(key_count=1)

        with patch("mctrader_data.metrics.historical_l1_reclaim_total"):
            result = reclaim_partition_l1_local(
                root=tmp_path,
                nas_uploader=uploader,
                exchange=EXCHANGE,
                symbol=SYMBOL,
                channel=CHANNEL,
                date_utc=HISTORICAL,
                now_snapshot=TODAY,
            )

        assert result.outcome == "ok"
        assert result.files_unlinked == 3
        assert result.bytes_freed > 0
        # L1 files should be gone
        for f in l1_files:
            assert not f.exists(), f"{f} should have been unlinked"
        # Sentinel should exist
        sentinel_candidates = list(tmp_path.glob(
            f"market/{CHANNEL}/schema_version=*/tier=L1"
            f"/exchange={EXCHANGE}/symbol={SYMBOL}/date={HISTORICAL.isoformat()}/.l1-promoted"
        ))
        assert len(sentinel_candidates) == 1

    def test_skip_sentinel_already_exists(self, tmp_path):
        """skip_sentinel: sentinel already exists → idempotent skip, no unlink."""
        l1_files = _make_l1_files(tmp_path, HISTORICAL, count=2)
        # Write sentinel
        date_dir = (
            tmp_path / "market" / CHANNEL / f"schema_version={SCHEMA_VER}"
            / "tier=L1" / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}"
            / f"date={HISTORICAL.isoformat()}"
        )
        sentinel = date_dir / ".l1-promoted"
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_bytes(b"")

        uploader = _make_nas_uploader_mock()

        result = reclaim_partition_l1_local(
            root=tmp_path,
            nas_uploader=uploader,
            exchange=EXCHANGE,
            symbol=SYMBOL,
            channel=CHANNEL,
            date_utc=HISTORICAL,
            now_snapshot=TODAY,
        )

        assert result.outcome == "skip_sentinel"
        assert result.files_unlinked == 0
        # L1 files should still exist (no unlink)
        for f in l1_files:
            assert f.exists()
        # NAS not called
        uploader.list_prefix_count.assert_not_called()

    def test_skip_today_window_today(self, tmp_path):
        """skip_today_window: date_utc == today → skip (forward window)."""
        uploader = _make_nas_uploader_mock()

        result = reclaim_partition_l1_local(
            root=tmp_path,
            nas_uploader=uploader,
            exchange=EXCHANGE,
            symbol=SYMBOL,
            channel=CHANNEL,
            date_utc=TODAY,
            now_snapshot=TODAY,
        )

        assert result.outcome == "skip_today_window"
        uploader.list_prefix_count.assert_not_called()

    def test_skip_today_window_yesterday(self, tmp_path):
        """skip_today_window: date_utc == yesterday (now_snapshot-1) → skip."""
        uploader = _make_nas_uploader_mock()

        result = reclaim_partition_l1_local(
            root=tmp_path,
            nas_uploader=uploader,
            exchange=EXCHANGE,
            symbol=SYMBOL,
            channel=CHANNEL,
            date_utc=YESTERDAY,
            now_snapshot=TODAY,
        )

        assert result.outcome == "skip_today_window"

    def test_skip_forward_in_flight_sentinel_present(self, tmp_path):
        """skip_forward_in_flight: .forward-processing sentinel exists → skip."""
        # Create .forward-processing sentinel
        date_dir = (
            tmp_path / "market" / CHANNEL / f"schema_version={SCHEMA_VER}"
            / "tier=L1" / f"exchange={EXCHANGE}" / f"symbol={SYMBOL}"
            / f"date={HISTORICAL.isoformat()}"
        )
        date_dir.mkdir(parents=True, exist_ok=True)
        (date_dir / ".forward-processing").touch()

        uploader = _make_nas_uploader_mock()

        result = reclaim_partition_l1_local(
            root=tmp_path,
            nas_uploader=uploader,
            exchange=EXCHANGE,
            symbol=SYMBOL,
            channel=CHANNEL,
            date_utc=HISTORICAL,
            now_snapshot=TODAY,
        )

        assert result.outcome == "skip_forward_in_flight"
        uploader.list_prefix_count.assert_not_called()

    def test_skip_nas_missing_key_count_zero(self, tmp_path):
        """skip_nas_missing: NAS list_objects_v2 KeyCount == 0 → L2 not committed."""
        _make_l1_files(tmp_path, HISTORICAL, count=2)
        uploader = _make_nas_uploader_mock(key_count=0)

        result = reclaim_partition_l1_local(
            root=tmp_path,
            nas_uploader=uploader,
            exchange=EXCHANGE,
            symbol=SYMBOL,
            channel=CHANNEL,
            date_utc=HISTORICAL,
            now_snapshot=TODAY,
        )

        assert result.outcome == "skip_nas_missing"
        assert result.files_unlinked == 0

    def test_fail_verify_local_l2_missing(self, tmp_path):
        """fail_verify: NAS KeyCount > 0 but local L2 dir missing → L1 preserved."""
        _make_l1_files(tmp_path, HISTORICAL, count=2)
        uploader = _make_nas_uploader_mock(key_count=1)
        # Do NOT create local L2 dir

        result = reclaim_partition_l1_local(
            root=tmp_path,
            nas_uploader=uploader,
            exchange=EXCHANGE,
            symbol=SYMBOL,
            channel=CHANNEL,
            date_utc=HISTORICAL,
            now_snapshot=TODAY,
        )

        assert result.outcome == "fail_verify"
        assert result.files_unlinked == 0

    def test_sentinel_write_atomic(self, tmp_path):
        """INV-F: _write_sentinel_atomic uses tempfile + os.replace."""
        sentinel = tmp_path / ".l1-promoted"
        _write_sentinel_atomic(sentinel)
        assert sentinel.exists()
        assert sentinel.stat().st_size == 0  # zero-byte marker

    def test_second_run_idempotent(self, tmp_path):
        """INV-D: second run on same partition returns skip_sentinel."""
        _make_l1_files(tmp_path, HISTORICAL, count=2)
        _make_l2_dir(tmp_path, HISTORICAL)
        uploader = _make_nas_uploader_mock(key_count=1)

        with patch("mctrader_data.metrics.historical_l1_reclaim_total"):
            result1 = reclaim_partition_l1_local(
                root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
                symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
            )
            # Second run
            result2 = reclaim_partition_l1_local(
                root=tmp_path, nas_uploader=uploader, exchange=EXCHANGE,
                symbol=SYMBOL, channel=CHANNEL, date_utc=HISTORICAL, now_snapshot=TODAY,
            )

        assert result1.outcome == "ok"
        assert result2.outcome == "skip_sentinel"
        assert result2.files_unlinked == 0


class TestNASUploaderApiContractSpec:
    """P1 #1 spec mock — real NASUploader attribute contract 박제.

    MagicMock() 은 auto-attribute 라 존재하지 않는 attr 접근을 허용 → P0 가 test 통과.
    MagicMock(spec=NASUploader) 는 NASUploader 의 실제 API surface 만 허용 →
    존재하지 않는 attr 접근 시 AttributeError 발생 → P0 재발 차단.

    P0 #1 fix 검증: `nas_uploader._s3` 접근 → AttributeError (spec mock 가 차단).
    P0 #1 fix 적용 후: `nas_uploader.list_prefix_count(...)` → spec mock 가 허용.
    """

    def test_spec_mock_rejects_s3_attribute(self):
        """spec=NASUploader mock 은 _s3 attr 을 허용하지 않는다 (P0 재발 차단 게이트)."""
        uploader = MagicMock(spec=NASUploader)
        # _s3 는 NASUploader 에 없는 attr → AttributeError 발생해야 함.
        with pytest.raises(AttributeError):
            _ = uploader._s3

    def test_spec_mock_allows_list_prefix_count(self):
        """spec=NASUploader mock 은 list_prefix_count 를 허용한다 (P0 #1 fix API 검증)."""
        uploader = MagicMock(spec=NASUploader)
        uploader.bucket = "mctrader-market"
        uploader.list_prefix_count.return_value = 1
        # 호출 가능해야 함 (AttributeError 0)
        result = uploader.list_prefix_count("some/prefix/", max_keys=1)
        assert result == 1

    def test_spec_mock_ok_path_calls_list_prefix_count(self, tmp_path):
        """spec=NASUploader mock 으로 ok 분기 통과 — P0 fix 이후 reclaim NAS verify 정상."""
        _make_l1_files(tmp_path, HISTORICAL, count=2)
        _make_l2_dir(tmp_path, HISTORICAL)

        uploader = MagicMock(spec=NASUploader)
        uploader.bucket = "mctrader-market"
        uploader.list_prefix_count.return_value = 1  # L2 NAS 객체 존재 시뮬레이션

        with patch("mctrader_data.metrics.historical_l1_reclaim_total"):
            result = reclaim_partition_l1_local(
                root=tmp_path,
                nas_uploader=uploader,
                exchange=EXCHANGE,
                symbol=SYMBOL,
                channel=CHANNEL,
                date_utc=HISTORICAL,
                now_snapshot=TODAY,
            )

        assert result.outcome == "ok"
        assert result.files_unlinked == 2
        # list_prefix_count 가 1회 호출됨 확인
        uploader.list_prefix_count.assert_called_once()

    def test_spec_mock_skip_nas_missing(self, tmp_path):
        """spec=NASUploader mock + list_prefix_count=0 → skip_nas_missing."""
        _make_l1_files(tmp_path, HISTORICAL, count=2)

        uploader = MagicMock(spec=NASUploader)
        uploader.bucket = "mctrader-market"
        uploader.list_prefix_count.return_value = 0  # L2 NAS 객체 없음

        result = reclaim_partition_l1_local(
            root=tmp_path,
            nas_uploader=uploader,
            exchange=EXCHANGE,
            symbol=SYMBOL,
            channel=CHANNEL,
            date_utc=HISTORICAL,
            now_snapshot=TODAY,
        )

        assert result.outcome == "skip_nas_missing"
        assert result.files_unlinked == 0
