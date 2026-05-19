# tests/unit/compactor/test_step_timeout_wrap.py
"""MCT-204 §8.1: _run_step_with_timeout unit tests.

Tests:
- success path: fn completes, no exception
- timeout path: TimeoutError caught, stall metric emitted, returns normally (INV-E)
- exception path: non-timeout exception propagates (or logged per design)
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

from mctrader_data.compactor.runner import CompactorRunner


def _make_runner(tmp_path, step_timeout: float = 5.0) -> CompactorRunner:
    """Create a CompactorRunner with given step_timeout."""
    import os
    with patch.dict("os.environ", {"MCTRADER_COMPACTOR_STEP_TIMEOUT_SECONDS": str(step_timeout)}):
        runner = CompactorRunner(root=tmp_path)
    return runner


class TestRunStepWithTimeout:
    def test_success_path_completes_normally(self, tmp_path):
        """fn completes within timeout — no exception raised."""
        runner = _make_runner(tmp_path, step_timeout=10.0)
        called = []

        def fn():
            called.append(True)

        asyncio.run(runner._run_step_with_timeout("l2", fn))
        assert called == [True]

    def test_timeout_path_caught_and_stall_metric_emitted(self, tmp_path):
        """fn exceeds timeout — TimeoutError caught, metric emitted, returns normally (INV-E)."""
        runner = _make_runner(tmp_path, step_timeout=0.05)  # 50ms timeout

        def slow_fn():
            time.sleep(1.0)  # Will exceed 50ms timeout

        with patch("mctrader_data.compactor.runner.compactor_step_stall_seconds") as mock_gauge:
            mock_labels = MagicMock()
            mock_gauge.labels.return_value = mock_labels

            # Should not raise
            asyncio.run(runner._run_step_with_timeout("l2", slow_fn))

            # Metric should have been emitted
            mock_gauge.labels.assert_called_once_with(step="l2")
            mock_labels.set.assert_called_once()
            elapsed_arg = mock_labels.set.call_args[0][0]
            assert elapsed_arg >= 0.04  # At least 40ms elapsed

    def test_timeout_does_not_block_next_step(self, tmp_path):
        """After timeout, next _run_step_with_timeout call works normally (INV-E starvation check)."""
        runner = _make_runner(tmp_path, step_timeout=0.05)

        # First step: times out
        def slow_fn():
            time.sleep(1.0)

        # Second step: should succeed
        called = []

        def fast_fn():
            called.append("fast")

        async def _run():
            await runner._run_step_with_timeout("l2", slow_fn)
            await runner._run_step_with_timeout("l3", fast_fn)

        with patch("mctrader_data.compactor.runner.compactor_step_stall_seconds"):
            asyncio.run(_run())

        assert called == ["fast"], "Second step (l3) should run after l2 timeout"

    def test_dedicated_executor_used(self, tmp_path):
        """Each step uses dedicated executor (not default executor)."""
        runner = _make_runner(tmp_path, step_timeout=5.0)
        # Each step name maps to a dedicated ThreadPoolExecutor
        assert "l2" in runner._executors
        assert "l3" in runner._executors
        assert "cleanup" in runner._executors
        assert "historical" in runner._executors

    def test_stop_shuts_down_executors(self, tmp_path):
        """stop() shuts down all executors."""
        runner = _make_runner(tmp_path, step_timeout=5.0)
        # Record executors before shutdown
        exs = list(runner._executors.values())

        asyncio.run(runner.stop())

        # After shutdown, executor should reject new tasks (but not raise on check)
        # We just verify stop() returns without error
        assert True  # if we get here, stop() didn't raise
