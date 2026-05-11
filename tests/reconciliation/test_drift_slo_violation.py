"""Drift SLO violation — fail-closed gate behaviour (Story MCT-145 §8).

Verifies the ConsistencyDriftError contract:
- emitted ONLY when drift ≥ DRIFT_SLO_THRESHOLD
- carries the measured drift, threshold, and report payload
- composes through the harness.compare → report.assert_within_slo path
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctrader_data.reconciliation import (
    DRIFT_SLO_THRESHOLD,
    ConsistencyDriftError,
    HotColdConsistencyHarness,
)
from mctrader_market.protocols.information_bar import InformationBarModel
from mctrader_market.types import Symbol


def _bar(i: int, *, label: str = "vol_100") -> InformationBarModel:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=i)
    return InformationBarModel(
        bar_label=label,
        genesis_ts=ts,
        ts_close=ts + timedelta(seconds=1),
        threshold=Decimal("100"),
        exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
        open=Decimal(100),
        high=Decimal(101),
        low=Decimal(99),
        close=Decimal(100),
        volume=Decimal(100),
        value=Decimal(10000),
    )


class TestSloBoundary:
    """Drift exactly at the SLO threshold must trip (gate is ``< threshold``)."""

    def test_drift_at_exact_threshold_trips(self):
        # 1 / 10_000 = 0.0001 == SLO threshold → fail-closed (gate uses ``<``)
        hot = [_bar(i) for i in range(10_000)]
        cold = [_bar(i) for i in range(10_000) if i != 5]
        report = HotColdConsistencyHarness().compare(hot_bars=hot, cold_bars=cold)
        assert report.drift == DRIFT_SLO_THRESHOLD
        assert not report.is_within_slo
        with pytest.raises(ConsistencyDriftError):
            report.assert_within_slo()

    def test_drift_just_below_threshold_passes(self):
        # 1 / 10_001 ≈ 0.0000999... < 0.0001 → within SLO
        hot = [_bar(i) for i in range(10_001)]
        cold = [_bar(i) for i in range(10_001) if i != 5]
        report = HotColdConsistencyHarness().compare(hot_bars=hot, cold_bars=cold)
        assert report.drift < DRIFT_SLO_THRESHOLD
        report.assert_within_slo()  # MUST NOT raise

    def test_drift_above_threshold_trips(self):
        hot = [_bar(i) for i in range(1000)]
        cold = [_bar(i) for i in range(1000) if i not in {1, 2, 3, 4, 5}]
        report = HotColdConsistencyHarness().compare(hot_bars=hot, cold_bars=cold)
        assert report.drift > DRIFT_SLO_THRESHOLD
        with pytest.raises(ConsistencyDriftError) as exc_info:
            report.assert_within_slo()
        err = exc_info.value
        assert err.drift == report.drift
        assert err.threshold == DRIFT_SLO_THRESHOLD
        assert err.report is report


class TestErrorPayload:
    def test_error_message_includes_counts(self):
        hot = [_bar(i) for i in range(100)]
        cold = [_bar(i) for i in range(50)]
        report = HotColdConsistencyHarness().compare(hot_bars=hot, cold_bars=cold)
        with pytest.raises(ConsistencyDriftError) as exc_info:
            report.assert_within_slo()
        msg = str(exc_info.value)
        assert "fail-closed" in msg
        assert "mismatch_count" in msg
        assert "hot=100" in msg
        assert "cold=50" in msg

    def test_custom_threshold_is_respected(self):
        # tighten the SLO to 1e-9 → even a single drop on a tiny dataset trips
        hot = [_bar(i) for i in range(10)]
        cold = [_bar(i) for i in range(10) if i != 0]
        report = HotColdConsistencyHarness(threshold=Decimal("1e-9")).compare(
            hot_bars=hot, cold_bars=cold
        )
        with pytest.raises(ConsistencyDriftError) as exc_info:
            report.assert_within_slo()
        assert exc_info.value.threshold == Decimal("1e-9")
