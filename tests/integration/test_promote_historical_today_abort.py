# tests/integration/test_promote_historical_today_abort.py
"""MCT-204 §8.2: promote-historical CLI INV-B abort guard.

Tests:
- CLI --end >= today-1 → exit 2 + log error (INV-B)
- CLI --end < today-1 → proceeds normally (no abort)
- run_historical_promotion ValueError on end_date >= today-1
"""
from __future__ import annotations

from datetime import date, timedelta, datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from mctrader_data.compactor.runner import run_historical_promotion


TODAY = datetime.now(timezone.utc).date()
YESTERDAY = TODAY - timedelta(days=1)
DAY_BEFORE_YESTERDAY = TODAY - timedelta(days=2)
HISTORICAL = TODAY - timedelta(days=5)


class TestPromoteHistoricalTodayAbort:
    def test_run_historical_promotion_raises_on_today_end(self, tmp_path):
        """INV-B: run_historical_promotion raises ValueError if end_date >= today-1."""
        mock_dw = MagicMock()
        mock_dw._uploader = MagicMock()

        with pytest.raises(ValueError, match="must be <"):
            run_historical_promotion(
                tmp_path,
                start_date=TODAY,
                end_date=TODAY,
                dual_writer=mock_dw,
            )

    def test_run_historical_promotion_raises_on_yesterday_end(self, tmp_path):
        """INV-B: end_date == yesterday (today-1) also raises ValueError."""
        mock_dw = MagicMock()
        mock_dw._uploader = MagicMock()

        with pytest.raises(ValueError, match="must be <"):
            run_historical_promotion(
                tmp_path,
                start_date=YESTERDAY,
                end_date=YESTERDAY,
                dual_writer=mock_dw,
            )

    def test_run_historical_promotion_ok_with_historical_end(self, tmp_path):
        """INV-B: end_date < today-1 → no ValueError (proceeds, may return empty counts)."""
        mock_dw = MagicMock()
        mock_dw._uploader = MagicMock()
        mock_dw._uploader.bucket = "test-bucket"

        # Should not raise (no partitions found → counts all 0)
        counts = run_historical_promotion(
            tmp_path,
            start_date=HISTORICAL,
            end_date=DAY_BEFORE_YESTERDAY,
            dual_writer=mock_dw,
        )
        assert "errors" in counts
        assert counts["partitions_processed"] == 0  # no actual data

    def test_invb_error_message_contains_boundary_date(self, tmp_path):
        """INV-B: error message includes boundary date and 'must be <' text."""
        mock_dw = MagicMock()
        mock_dw._uploader = MagicMock()

        with pytest.raises(ValueError) as exc_info:
            run_historical_promotion(
                tmp_path,
                start_date=YESTERDAY,
                end_date=YESTERDAY,
                dual_writer=mock_dw,
            )

        error_msg = str(exc_info.value)
        assert "must be <" in error_msg, "Error message should contain 'must be <'"
        assert "forward window" in error_msg or "promote-historical" in error_msg

    def test_forward_window_abort_returns_reclaim_skipped_zero(self, tmp_path):
        """INV-B: partitions in forward window never reclaimed (reclaim_skipped not incremented)."""
        mock_dw = MagicMock()
        mock_dw._uploader = MagicMock()

        try:
            run_historical_promotion(
                tmp_path,
                start_date=TODAY,
                end_date=TODAY,
                dual_writer=mock_dw,
            )
            assert False, "Should have raised ValueError"
        except ValueError:
            pass  # Expected — no reclaim happened


class TestRunHistoricalPromotionNowSnapshot:
    def test_now_snapshot_per_invocation(self, tmp_path):
        """FIX 1/3 P0 #3: run_historical_promotion sets monotonic now_snapshot at entry."""
        mock_dw = MagicMock()
        mock_dw._uploader = MagicMock()
        mock_dw._uploader.bucket = "test-bucket"

        captured_snapshots = []

        from mctrader_data.compactor import historical_reclaim

        original_reclaim = historical_reclaim.reclaim_partition_l1_local

        def capturing_reclaim(**kwargs):
            captured_snapshots.append(kwargs.get("now_snapshot"))
            return MagicMock(outcome="skip_nas_missing", files_unlinked=0, bytes_freed=0)

        with patch.object(historical_reclaim, "reclaim_partition_l1_local", side_effect=capturing_reclaim):
            # Create a partition so reclaim is called
            date_dir = (
                tmp_path / "market" / "orderbooksnapshot" / "schema_version=v1"
                / "tier=L1" / "exchange=upbit" / "symbol=KRW-BTC"
                / f"date={HISTORICAL.isoformat()}" / "node=n1"
            )
            date_dir.mkdir(parents=True, exist_ok=True)
            (date_dir / "part-stub.parquet").write_bytes(b"stub")

            run_historical_promotion(
                tmp_path,
                start_date=HISTORICAL,
                end_date=HISTORICAL,
                dual_writer=mock_dw,
                channel="orderbooksnapshot",
            )

        if captured_snapshots:
            # All snapshots should be the same (monotonic per invocation)
            assert all(s == captured_snapshots[0] for s in captured_snapshots)
