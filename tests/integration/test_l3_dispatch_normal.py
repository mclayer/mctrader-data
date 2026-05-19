# tests/integration/test_l3_dispatch_normal.py
"""MCT-204 §8.2: L3 dispatch proceeds independent of L2 task completion (AC-4).

Tests:
- L3 runs based on cadence (L3_INTERVAL_SECONDS) independently of L2 stall
- _run_l3 uses _discover_partitions_in_range with tier=L2
- l3_pending_partitions Gauge emitted per (exchange, channel)
"""
from __future__ import annotations

import asyncio
import time
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from mctrader_data.compactor.runner import CompactorRunner, L3_INTERVAL_SECONDS


TODAY = date(2026, 5, 19)
YESTERDAY = TODAY - timedelta(days=1)


def _make_l2_parquet(
    root: Path,
    *,
    channel: str,
    exchange: str,
    symbol: str,
    date_utc: date,
    schema_ver: str = "v1",
) -> Path:
    date_dir = (
        root / "market" / channel / f"schema_version={schema_ver}"
        / "tier=L2" / f"exchange={exchange}" / f"symbol={symbol}"
        / f"date={date_utc.isoformat()}"
    )
    date_dir.mkdir(parents=True, exist_ok=True)
    f = date_dir / "part-day.parquet"
    f.write_bytes(b"stub-l2")
    return f


class TestL3DispatchNormal:
    def test_l3_discovers_l2_partitions(self, tmp_path):
        """AC-4: _run_l3 uses tier=L2 discovery (not L1)."""
        # Create L2 partition (not L1)
        _make_l2_parquet(
            tmp_path, channel="orderbooksnapshot",
            exchange="upbit", symbol="KRW-BTC", date_utc=TODAY,
        )
        # Create L1 partition with different symbol — should NOT appear in L3
        l1_dir = (
            tmp_path / "market" / "orderbooksnapshot" / "schema_version=v1"
            / "tier=L1" / "exchange=upbit" / "symbol=KRW-ETH"
            / f"date={TODAY.isoformat()}"
        )
        l1_dir.mkdir(parents=True, exist_ok=True)
        (l1_dir / "part-stub.parquet").write_bytes(b"stub-l1")

        runner = CompactorRunner(root=tmp_path)
        processed_partitions = []

        def mock_compact_day(*, exchange, symbol, channel, date_utc):
            processed_partitions.append((exchange, symbol, channel, date_utc))
            return None

        with patch.object(runner._l3, "compact_day", side_effect=mock_compact_day):
            runner._run_l3(now_snapshot=TODAY)

        # Should have processed only the L2 partition
        symbols = [sym for _, sym, _, _ in processed_partitions]
        assert "KRW-BTC" in symbols
        assert "KRW-ETH" not in symbols, "L1-only partition should NOT appear in _run_l3"

    def test_l3_pending_gauge_emitted(self, tmp_path):
        """AC-4: mctrader_l3_pending_partitions Gauge emitted per (exchange, channel)."""
        _make_l2_parquet(
            tmp_path, channel="orderbooksnapshot",
            exchange="upbit", symbol="KRW-BTC", date_utc=TODAY,
        )
        _make_l2_parquet(
            tmp_path, channel="orderbooksnapshot",
            exchange="upbit", symbol="KRW-ETH", date_utc=TODAY,
        )

        runner = CompactorRunner(root=tmp_path)

        set_calls = []
        mock_gauge = MagicMock()
        mock_gauge.labels.return_value.set.side_effect = lambda v: set_calls.append(v)

        with (
            patch.object(runner._l3, "compact_day", return_value=None),
            patch("mctrader_data.compactor.runner.compactor_l3_pending_partitions", mock_gauge),
        ):
            runner._run_l3(now_snapshot=TODAY)

        mock_gauge.labels.assert_called()
        assert set_calls, "l3_pending_partitions Gauge should have been set"

    def test_l3_independent_of_l2_stall(self, tmp_path):
        """AC-4: L3 step cadence-based, independent of L2 stall."""
        runner = CompactorRunner(root=tmp_path)

        l3_called = []

        def fast_l3(now_snapshot=None):
            l3_called.append(True)

        with (
            patch.object(runner, "_run_l3", side_effect=fast_l3),
            patch.object(runner, "_run_l2", return_value=None),
            patch("mctrader_data.compactor.runner.scan_sealed", return_value=[]),
            patch("mctrader_data.compactor.runner.run_gc"),
            patch("mctrader_data.compactor.runner.compactor_tier_pending_segments"),
            patch("mctrader_data.compactor.runner.compactor_cleanup_cycle_delay_seconds"),
            patch("mctrader_data.compactor.runner.compactor_step_stall_seconds"),
            patch("mctrader_data.compactor.runner.compactor_l3_pending_partitions"),
        ):
            runner._last_l2 = 0.0
            runner._last_l3 = 0.0

            asyncio.run(runner._tick())

        assert l3_called, "L3 should have been called"

    def test_l3_forward_window_scope(self, tmp_path):
        """INV-A (L3 동형): _run_l3 only processes [yesterday, today] L2 partitions."""
        # Create historical L2 (outside forward window)
        historical_d = TODAY - timedelta(days=5)
        _make_l2_parquet(
            tmp_path, channel="orderbooksnapshot",
            exchange="upbit", symbol="KRW-BTC", date_utc=historical_d,
        )
        # Create forward L2
        _make_l2_parquet(
            tmp_path, channel="orderbooksnapshot",
            exchange="upbit", symbol="KRW-BTC", date_utc=TODAY,
        )

        runner = CompactorRunner(root=tmp_path)
        processed_dates = set()

        def mock_compact_day(*, exchange, symbol, channel, date_utc):
            processed_dates.add(date_utc)
            return None

        with patch.object(runner._l3, "compact_day", side_effect=mock_compact_day):
            runner._run_l3(now_snapshot=TODAY)

        assert historical_d not in processed_dates, "Historical date should not appear in _run_l3"
        assert processed_dates.issubset({TODAY, YESTERDAY})
