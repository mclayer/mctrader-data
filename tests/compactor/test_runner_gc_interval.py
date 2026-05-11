# tests/compactor/test_runner_gc_interval.py
"""Verify CompactorRunner._tick calls stdlib gc.collect() on a configurable interval.

MCT-133 A1 Task 6c: Task 4 added MCTRADER_COMPACTOR_GC_INTERVAL_SECONDS=300 to
compose.yml as an inert knob. This task wires it in: _tick() must invoke
gc.collect() once per interval to release pyarrow buffers held by the Python
heap, mitigating compactor memory growth under sustained L1→L2→L3 work.
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from mctrader_data.compactor.runner import CompactorRunner


@pytest.mark.asyncio
async def test_gc_collect_called_at_interval(tmp_path: Path, monkeypatch) -> None:
    """gc.collect() should fire once interval has elapsed since the last call."""
    monkeypatch.setenv("MCTRADER_COMPACTOR_GC_INTERVAL_SECONDS", "0.1")

    runner = CompactorRunner(root=tmp_path)

    with patch("mctrader_data.compactor.runner.gc.collect") as mock_collect:
        # First tick: _last_gc=0.0 so interval has effectively passed → 1 call.
        await runner._tick()
        first_count = mock_collect.call_count
        # Sleep less than interval — next tick must NOT call again.
        await asyncio.sleep(0.02)
        await runner._tick()
        assert mock_collect.call_count == first_count, (
            "gc.collect should not fire before interval elapses"
        )
        # Sleep past interval — next tick SHOULD call again.
        await asyncio.sleep(0.15)
        await runner._tick()
        assert mock_collect.call_count > first_count, (
            "gc.collect should fire after interval elapses"
        )


@pytest.mark.asyncio
async def test_filesystem_gc_still_runs(tmp_path: Path) -> None:
    """The existing filesystem gc (run_gc / .gc module) must still be invoked.

    Naming collision risk: import `gc as stdlib gc` and the existing `from .gc
    import run_gc` (or `from . import gc as ...`) — this test guards against
    accidentally breaking the filesystem-gc call site.
    """
    runner = CompactorRunner(root=tmp_path)
    # patch the bound symbol used inside _tick
    with patch("mctrader_data.compactor.runner.run_gc") as mock_run_gc:
        await runner._tick()
    assert mock_run_gc.called, "filesystem run_gc(root) must still execute every tick"
