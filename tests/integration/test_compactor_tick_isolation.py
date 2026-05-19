# tests/integration/test_compactor_tick_isolation.py
"""MCT-204 §8.2: asyncio step isolation — L2 stall sim → L3/cleanup 진입 박제.

Tests:
- AC-1: L2 stall/timeout → L3 and cleanup steps still execute (starvation 차단)
- INV-E: per-step stall timeout after which next step proceeds
- INV-H: forward ∩ historical = best-effort ∅ (combined with forward/historical tests)
"""
from __future__ import annotations

import asyncio
import time
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from mctrader_data.compactor.runner import CompactorRunner, LEGACY_CLEANUP_EVERY_N_CYCLES


def _make_runner(tmp_path: Path, step_timeout: float = 0.1) -> CompactorRunner:
    """Runner with short timeout for stall simulation."""
    import os
    with patch.dict("os.environ", {"MCTRADER_COMPACTOR_STEP_TIMEOUT_SECONDS": str(step_timeout)}):
        return CompactorRunner(root=tmp_path)


class TestCompactorTickIsolation:
    def test_l2_stall_does_not_block_l3_step(self, tmp_path):
        """AC-1/INV-E: L2 stall (timeout) → L3 step still executes."""
        runner = _make_runner(tmp_path, step_timeout=0.05)

        l3_called = []

        def stall_l2(now_snapshot=None):
            time.sleep(1.0)  # Will timeout

        def fast_l3(now_snapshot=None):
            l3_called.append(True)

        with (
            patch.object(runner, "_run_l2", side_effect=stall_l2),
            patch.object(runner, "_run_l3", side_effect=fast_l3),
            patch.object(runner._l1, "compact_segment", return_value=tmp_path),
            patch("mctrader_data.compactor.runner.scan_sealed", return_value=[]),
            patch("mctrader_data.compactor.runner.run_gc"),
            patch("mctrader_data.compactor.runner.compactor_tier_pending_segments"),
            patch("mctrader_data.compactor.runner.compactor_cleanup_cycle_delay_seconds"),
            patch("mctrader_data.compactor.runner.compactor_step_stall_seconds") as mock_stall,
            patch("mctrader_data.compactor.runner.compactor_l3_pending_partitions"),
        ):
            mock_stall.labels.return_value = MagicMock()
            runner._last_l2 = 0.0  # Force L2 to run
            runner._last_l3 = 0.0  # Force L3 to run

            async def _run_one_tick():
                await runner._tick()

            asyncio.run(_run_one_tick())

        assert l3_called, "L3 should have been called despite L2 timeout"

    def test_l2_stall_does_not_block_cleanup(self, tmp_path):
        """AC-1/INV-E: L2 stall → cleanup step still executes at cycle_count gate."""
        runner = _make_runner(tmp_path, step_timeout=0.05)
        # Force cleanup to trigger on first tick
        runner._cycle_count = LEGACY_CLEANUP_EVERY_N_CYCLES - 1
        mock_uploader = MagicMock()
        runner._dual_writer = MagicMock()
        runner._dual_writer._uploader = mock_uploader

        cleanup_called = []

        def stall_l2(now_snapshot=None):
            time.sleep(1.0)

        def track_cleanup(root, uploader, batch_limit=None):
            cleanup_called.append(True)
            return {"cleaned": 0, "preserved": 0, "errors": 0, "batch_limit": 500}

        with (
            patch.object(runner, "_run_l2", side_effect=stall_l2),
            patch.object(runner, "_run_l3", return_value=None),
            patch("mctrader_data.compactor.runner.scan_sealed", return_value=[]),
            patch("mctrader_data.compactor.runner.run_gc"),
            patch("mctrader_data.compactor.runner.scan_and_cleanup_legacy", side_effect=track_cleanup),
            patch("mctrader_data.compactor.runner.compactor_tier_pending_segments"),
            patch("mctrader_data.compactor.runner.compactor_cleanup_cycle_delay_seconds"),
            patch("mctrader_data.compactor.runner.compactor_step_stall_seconds") as mock_stall,
            patch("mctrader_data.compactor.runner.compactor_l3_pending_partitions"),
        ):
            mock_stall.labels.return_value = MagicMock()
            runner._last_l2 = 0.0

            asyncio.run(runner._tick())

        assert cleanup_called, "Cleanup should have been called despite L2 timeout"

    def test_step_timeout_emits_stall_metric(self, tmp_path):
        """INV-E: timeout → compactor_step_stall_seconds Gauge emitted."""
        runner = _make_runner(tmp_path, step_timeout=0.05)

        def slow_fn():
            time.sleep(1.0)

        stall_set_called = []
        mock_gauge = MagicMock()
        mock_gauge.labels.return_value.set.side_effect = lambda v: stall_set_called.append(v)

        with patch("mctrader_data.compactor.runner.compactor_step_stall_seconds", mock_gauge):
            asyncio.run(runner._run_step_with_timeout("l2", slow_fn))

        assert stall_set_called, "stall metric should have been emitted"
        assert stall_set_called[0] >= 0.04

    def test_cycle_count_increments_regardless_of_timeout(self, tmp_path):
        """cycle_count increments on _tick entry regardless of step timeout (drift 차단)."""
        runner = _make_runner(tmp_path, step_timeout=0.05)
        initial_count = runner._cycle_count

        def stall_l2(now_snapshot=None):
            time.sleep(1.0)

        with (
            patch.object(runner, "_run_l2", side_effect=stall_l2),
            patch.object(runner, "_run_l3", return_value=None),
            patch("mctrader_data.compactor.runner.scan_sealed", return_value=[]),
            patch("mctrader_data.compactor.runner.run_gc"),
            patch("mctrader_data.compactor.runner.compactor_tier_pending_segments"),
            patch("mctrader_data.compactor.runner.compactor_cleanup_cycle_delay_seconds"),
            patch("mctrader_data.compactor.runner.compactor_step_stall_seconds") as mock_stall,
            patch("mctrader_data.compactor.runner.compactor_l3_pending_partitions"),
        ):
            mock_stall.labels.return_value = MagicMock()
            runner._last_l2 = 0.0

            asyncio.run(runner._tick())

        assert runner._cycle_count == initial_count + 1

    def test_cleanup_delay_metric_emitted(self, tmp_path):
        """AC-1: cleanup_cycle_delay_seconds Gauge emitted when last_cleanup_complete > 0."""
        runner = _make_runner(tmp_path, step_timeout=5.0)
        runner._last_cleanup_complete = time.time() - 30.0  # 30s ago

        delay_set = []
        mock_gauge = MagicMock()
        mock_gauge.set.side_effect = lambda v: delay_set.append(v)

        with (
            patch.object(runner, "_run_l2", return_value=None),
            patch.object(runner, "_run_l3", return_value=None),
            patch("mctrader_data.compactor.runner.scan_sealed", return_value=[]),
            patch("mctrader_data.compactor.runner.run_gc"),
            patch("mctrader_data.compactor.runner.compactor_tier_pending_segments"),
            patch("mctrader_data.compactor.runner.compactor_cleanup_cycle_delay_seconds", mock_gauge),
            patch("mctrader_data.compactor.runner.compactor_step_stall_seconds"),
            patch("mctrader_data.compactor.runner.compactor_l3_pending_partitions"),
        ):
            asyncio.run(runner._tick())

        assert delay_set, "cleanup delay metric should have been emitted"
        assert delay_set[0] >= 25.0  # at least 25s delay
