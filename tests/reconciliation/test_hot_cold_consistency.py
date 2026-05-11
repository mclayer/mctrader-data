"""Hot/Cold consistency harness — drift SLO < 0.01% gate (Story MCT-145).

Both Hot and Cold paths delegate to the Story-3 aggregator core (MCT-137),
so given identical ticks they MUST emit byte-identical
:class:`InformationBarModel` streams. This test simulates a Hot driver and
a Cold driver feeding the same canonical aggregator, then verifies the
harness reports zero drift.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctrader_data.aggregation.core import (
    TickBarAggregator,
    TimeBarAggregator,
    VolumeBarAggregator,
)
from mctrader_data.reconciliation import (
    DRIFT_SLO_THRESHOLD,
    ConsistencyDriftError,
    HotColdConsistencyHarness,
)
from mctrader_data.reconciliation.edge_case_fixtures import (
    generate_threshold_boundary,
)
from mctrader_market.schemas.tick import TickRowV1_1
from mctrader_market.types import Symbol


def _tick(
    ts: datetime,
    price: Decimal,
    qty: Decimal,
    *,
    exchange: str = "bithumb",
    symbol: str = "KRW-BTC",
    trade_id: str | None = None,
) -> TickRowV1_1:
    return TickRowV1_1(
        ts_utc=ts,
        exchange=exchange,
        symbol=Symbol.from_string(symbol),
        trade_id=trade_id or f"{exchange}:{symbol}:{ts.isoformat()}",
        price=price,
        quantity=qty,
        side="BUY",
        is_taker=True,
        ingest_seq=None,
        payload_hash=None,
        validation_status="OK",
    )


def _drive(aggregator, ticks):
    """Run an aggregator over a tick iterable, return the list of closed bars."""
    bars = []
    for t in ticks:
        bar = aggregator.process_tick(t)
        if bar is not None:
            bars.append(bar)
    return bars


def _fixture_to_ticks(fixture, *, base_exchange: str = "bithumb", base_symbol: str = "KRW-BTC"):
    return [
        _tick(ts, price, qty, exchange=base_exchange, symbol=base_symbol, trade_id=f"t{i}")
        for i, (ts, price, qty) in enumerate(fixture.ticks)
    ]


class TestZeroDrift:
    """Hot and Cold over identical ticks → mismatch_count == 0 → drift = 0."""

    def test_volume_bar_zero_drift(self):
        fixture = generate_threshold_boundary(
            seed=42, threshold=Decimal("100"), bar_count=3
        )
        ticks = _fixture_to_ticks(fixture)
        hot = _drive(VolumeBarAggregator(threshold=Decimal("100")), ticks)
        cold = _drive(VolumeBarAggregator(threshold=Decimal("100")), ticks)
        assert len(hot) == len(cold) == 3

        harness = HotColdConsistencyHarness()
        report = harness.compare(hot_bars=hot, cold_bars=cold)
        assert report.drift == Decimal(0)
        assert report.matched_count == 3
        assert report.mismatch_count == 0
        assert report.is_within_slo

    def test_time_bar_zero_drift(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ticks = [
            _tick(base + timedelta(seconds=i * 30), Decimal(100 + i), Decimal(1))
            for i in range(10)
        ]
        # 30s × 10 = 300s; with timeframe=60s, expect ~5 bars (boundary-flush)
        hot = _drive(TimeBarAggregator(timeframe=timedelta(seconds=60)), ticks)
        cold = _drive(TimeBarAggregator(timeframe=timedelta(seconds=60)), ticks)

        harness = HotColdConsistencyHarness()
        report = harness.compare(hot_bars=hot, cold_bars=cold)
        assert report.drift == Decimal(0)
        assert report.mismatch_count == 0

    def test_tick_bar_zero_drift(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ticks = [
            _tick(base + timedelta(seconds=i), Decimal(100), Decimal(1))
            for i in range(20)
        ]
        hot = _drive(TickBarAggregator(threshold=5), ticks)
        cold = _drive(TickBarAggregator(threshold=5), ticks)

        report = HotColdConsistencyHarness().compare(hot_bars=hot, cold_bars=cold)
        assert report.drift == Decimal(0)
        assert len(hot) == len(cold) == 4

    def test_empty_streams_consistent(self):
        report = HotColdConsistencyHarness().compare(hot_bars=[], cold_bars=[])
        assert report.drift == Decimal(0)
        assert report.is_within_slo
        # assert no exception
        report.assert_within_slo()


class TestSloGate:
    """Drift ≥ 0.01% trips ConsistencyDriftError."""

    def test_full_mismatch_raises(self):
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        hot_ticks = [
            _tick(base + timedelta(seconds=i), Decimal(100), Decimal(1))
            for i in range(20)
        ]
        # Cold drives different ticks → all keys differ
        cold_ticks = [
            _tick(base + timedelta(seconds=100 + i), Decimal(100), Decimal(1))
            for i in range(20)
        ]
        hot = _drive(TickBarAggregator(threshold=5), hot_ticks)
        cold = _drive(TickBarAggregator(threshold=5), cold_ticks)

        report = HotColdConsistencyHarness().compare(hot_bars=hot, cold_bars=cold)
        assert report.drift > DRIFT_SLO_THRESHOLD
        assert not report.is_within_slo

        with pytest.raises(ConsistencyDriftError) as exc_info:
            report.assert_within_slo()
        assert exc_info.value.drift == report.drift
        assert exc_info.value.threshold == DRIFT_SLO_THRESHOLD

    def test_single_bar_drop_below_slo_when_dataset_large(self):
        """One missing bar over a 20 000-bar dataset = 0.005% drift < SLO."""
        # We don't drive 20k actual ticks; we construct synthetic bars directly.
        from mctrader_market.protocols.information_bar import InformationBarModel

        def _make_bar(i: int) -> InformationBarModel:
            ts = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=i)
            return InformationBarModel(
                bar_label="vol_100",
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

        hot = [_make_bar(i) for i in range(20_000)]
        cold = [_make_bar(i) for i in range(20_000) if i != 12345]
        report = HotColdConsistencyHarness().compare(hot_bars=hot, cold_bars=cold)
        # 1 / 20_000 = 0.00005 < 0.0001 → within SLO
        assert report.drift < DRIFT_SLO_THRESHOLD
        assert report.is_within_slo
        report.assert_within_slo()


class TestReportSummary:
    def test_summary_dict_has_required_keys(self):
        report = HotColdConsistencyHarness().compare(hot_bars=[], cold_bars=[])
        summary = report.summary()
        required = {
            "hot_count", "cold_count", "matched_count",
            "drift", "threshold", "is_within_slo",
        }
        assert required <= set(summary.keys())

    def test_threshold_validation(self):
        with pytest.raises(ValueError):
            HotColdConsistencyHarness(threshold=Decimal(0))
