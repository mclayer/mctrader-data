"""Verify tier_pending Gauge does not emit epoch-derived nonsense on fresh start."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from mctrader_data.compactor.runner import CompactorRunner
from mctrader_data.metrics import compactor_tier_pending_segments


@pytest.mark.asyncio
async def test_fresh_runner_emits_zero_pending_for_l2_l3(tmp_path: Path) -> None:
    """Fresh CompactorRunner must report pending=0 for L2/L3 until first cycle."""
    # Reset Gauge state
    compactor_tier_pending_segments.labels(tier="L2").set(-1)
    compactor_tier_pending_segments.labels(tier="L3").set(-1)

    runner = CompactorRunner(root=tmp_path)
    await runner._tick()

    l2_val = compactor_tier_pending_segments.labels(tier="L2")._value.get()
    l3_val = compactor_tier_pending_segments.labels(tier="L3")._value.get()

    # Should be 0 (since _last_l2 / _last_l3 = 0.0 = "never run")
    # Not millions (which would indicate epoch math leak)
    assert l2_val == 0, f"L2 pending should be 0 on fresh start, got {l2_val}"
    assert l3_val == 0, f"L3 pending should be 0 on fresh start, got {l3_val}"
