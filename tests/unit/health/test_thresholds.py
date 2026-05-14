"""Unit tests for health.thresholds — MCT-165 Task 4."""

import pytest

from mctrader_data.health.thresholds import (
    static_volume_threshold,
    static_lag_threshold,
    rolling_threshold,
)


def test_static_volume_threshold_within_20pct_pass():
    result = static_volume_threshold(actual=4.2, expected=4.35, tol=0.20)
    assert result.verdict == "PASS"


def test_static_volume_threshold_outside_20pct_fail():
    result = static_volume_threshold(actual=2.0, expected=4.35, tol=0.20)
    assert result.verdict == "FAIL"


def test_static_volume_threshold_exact_boundary_pass():
    # 4.35 * 0.80 = 3.48 — exactly at boundary → PASS
    result = static_volume_threshold(actual=3.48, expected=4.35, tol=0.20)
    assert result.verdict == "PASS"


def test_static_lag_threshold_under_60s_pass():
    result = static_lag_threshold(actual_seconds=30)
    assert result.verdict == "PASS"


def test_static_lag_threshold_over_60s_fail():
    result = static_lag_threshold(actual_seconds=90)
    assert result.verdict == "FAIL"


def test_rolling_threshold_not_implemented():
    with pytest.raises(NotImplementedError, match="rolling baseline reserved.*ADR"):
        rolling_threshold(actual=4.2, window_days=7)
