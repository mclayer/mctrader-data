"""Deterministic fixture generators — same seed → same ticks (Story MCT-145)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctrader_data.reconciliation import (
    generate_krw_rounding_edge,
    generate_threshold_boundary,
    generate_time_bar_boundary,
)


class TestThresholdBoundary:
    def test_same_seed_yields_identical_ticks(self):
        a = generate_threshold_boundary(seed=42, threshold=Decimal("100"), bar_count=3)
        b = generate_threshold_boundary(seed=42, threshold=Decimal("100"), bar_count=3)
        assert a.ticks == b.ticks
        assert a.expected_bar_count == b.expected_bar_count == 3
        assert a.category == "threshold_boundary"

    def test_different_seed_yields_different_ticks(self):
        a = generate_threshold_boundary(seed=42, threshold=Decimal("100"), bar_count=3)
        b = generate_threshold_boundary(seed=43, threshold=Decimal("100"), bar_count=3)
        assert a.ticks != b.ticks

    def test_each_bar_lands_exactly_on_threshold(self):
        """Cumulative quantity at the closing tick of each bar == threshold.

        With the canonical ``current_tick`` tie-breaking rule, this is the
        canary case: a buggy ``>`` comparison would carry over to the next bar.
        """
        fixture = generate_threshold_boundary(seed=42, threshold=Decimal("100"), bar_count=2)
        # 5 ticks per bar (4 partials + 1 closer) × 2 bars = 10 ticks
        assert len(fixture.ticks) == 10
        # Sum quantities of ticks 1-5 → should equal 100 exactly
        bar1_sum = sum((q for _, _, q in fixture.ticks[:5]), Decimal(0))
        bar2_sum = sum((q for _, _, q in fixture.ticks[5:]), Decimal(0))
        assert bar1_sum == Decimal("100")
        assert bar2_sum == Decimal("100")


class TestTimeBarBoundary:
    def test_same_seed_yields_identical_ticks(self):
        a = generate_time_bar_boundary(seed=7, timeframe_seconds=60, bar_count=2)
        b = generate_time_bar_boundary(seed=7, timeframe_seconds=60, bar_count=2)
        assert a.ticks == b.ticks
        assert a.expected_bar_count == 2
        assert a.category == "time_bar_boundary"

    def test_includes_right_edge_boundary_tick(self):
        """The trailing tick MUST land exactly on ``window_end`` of the last bar
        — that tick opens a NEW bar (half-open ``[start, end)`` rule)."""
        fixture = generate_time_bar_boundary(
            seed=7,
            timeframe_seconds=60,
            bar_count=3,
            base_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        # 2 ticks per bar (mid + right-edge-inside) + 1 boundary tick = 7 ticks
        assert len(fixture.ticks) == 7
        # The last tick is exactly base + 3*60s = 00:03:00
        expected_end = datetime(2026, 1, 1, 0, 3, 0, tzinfo=timezone.utc)
        assert fixture.ticks[-1][0] == expected_end


class TestKrwRoundingEdge:
    def test_same_seed_yields_identical_ticks(self):
        a = generate_krw_rounding_edge(seed=13, threshold=Decimal("10000"), bar_count=2)
        b = generate_krw_rounding_edge(seed=13, threshold=Decimal("10000"), bar_count=2)
        assert a.ticks == b.ticks
        assert a.expected_bar_count == 2
        assert a.category == "krw_rounding_edge"

    def test_per_bar_notional_meets_threshold(self):
        """Sum of (price * quantity) per bar must hit threshold exactly when
        accumulated via Decimal arithmetic — float math would diverge."""
        fixture = generate_krw_rounding_edge(seed=13, threshold=Decimal("10000"), bar_count=2)
        # 5 ticks per bar
        assert len(fixture.ticks) == 10
        bar1_notional = sum((p * q for _, p, q in fixture.ticks[:5]), Decimal(0))
        bar2_notional = sum((p * q for _, p, q in fixture.ticks[5:]), Decimal(0))
        assert bar1_notional == Decimal("10000")
        assert bar2_notional == Decimal("10000")


class TestValidation:
    def test_threshold_must_be_positive(self):
        with pytest.raises(ValueError, match="threshold"):
            generate_threshold_boundary(seed=1, threshold=Decimal(0), bar_count=1)

    def test_bar_count_must_be_positive(self):
        with pytest.raises(ValueError, match="bar_count"):
            generate_time_bar_boundary(seed=1, timeframe_seconds=60, bar_count=0)

    def test_timeframe_seconds_must_be_positive(self):
        with pytest.raises(ValueError, match="timeframe_seconds"):
            generate_time_bar_boundary(seed=1, timeframe_seconds=0, bar_count=1)
