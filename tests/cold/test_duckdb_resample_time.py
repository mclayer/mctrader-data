"""Cold path DuckDB resample_time — closed Timeframe OHLCV bars.

Covers
------
- 1m bucket OHLCV correctness (open / high / low / close / volume / value).
- ``source = "transaction_derived"`` provenance on every emitted Candle.
- Determinism: identical fixture → identical bars across runs.
- Cross-engine: arbitrary-seconds buckets routed via ``resample_information_bar``
  (``time_<seconds>``) for cases outside the closed Timeframe enum.
- Boundary semantics: half-open ``[start, end)`` — bar at the right boundary is
  not closed within the window.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from mctrader_data.cold import DuckDBResampler

from .conftest import TickSpec


class TestResampleTimeOneMinute:
    def test_emits_one_bar_per_closed_minute_with_transaction_derived_source(
        self, tmp_path, write_ticks, utc
    ):
        base = utc(2026, 1, 1, 0, 0, 0)
        ticks = [
            TickSpec(base, "100", "1"),
            TickSpec(base + timedelta(seconds=30), "110", "2"),
            TickSpec(base + timedelta(minutes=1), "105", "1"),
            TickSpec(base + timedelta(minutes=1, seconds=30), "115", "3"),
            # Sentinel in third minute → forces the second minute's bar to close.
            TickSpec(base + timedelta(minutes=2), "120", "1"),
        ]
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        candles = list(
            resampler.resample_time(
                symbol="KRW-BTC",
                timeframe="1m",
                start=base,
                end=base + timedelta(minutes=3),
            )
        )
        assert len(candles) == 2
        c1, c2 = candles
        assert c1.ts_utc == base
        assert c1.open == Decimal("100")
        assert c1.high == Decimal("110")
        assert c1.low == Decimal("100")
        assert c1.close == Decimal("110")
        assert c1.volume == Decimal("3")
        assert c1.source == "transaction_derived"

        assert c2.ts_utc == base + timedelta(minutes=1)
        assert c2.open == Decimal("105")
        assert c2.high == Decimal("115")
        assert c2.low == Decimal("105")
        assert c2.close == Decimal("115")
        assert c2.volume == Decimal("4")
        assert c2.source == "transaction_derived"

    def test_arbitrary_seconds_rejected_for_resample_time(self, tmp_path):
        resampler = DuckDBResampler(root=tmp_path)
        with pytest.raises(ValueError, match="closed Timeframe"):
            list(
                resampler.resample_time(
                    symbol="KRW-BTC",
                    timeframe="47s",
                    start=__import__("datetime").datetime(
                        2026, 1, 1, tzinfo=__import__("datetime").timezone.utc
                    ),
                    end=__import__("datetime").datetime(
                        2026, 1, 2, tzinfo=__import__("datetime").timezone.utc
                    ),
                )
            )


class TestArbitrarySecondsViaInformationBar:
    """Imagine ``time_13`` / ``time_47`` / ``time_600`` buckets.

    Closed Timeframe enum does not cover these — the Cold API exposes them via
    ``resample_information_bar(bar_label="time_<seconds>")`` (ADR-009 §D15).
    """

    @pytest.mark.parametrize("seconds", [13, 47, 60, 300])
    def test_information_bar_time_arbitrary_seconds(
        self, tmp_path, write_ticks, utc, seconds
    ):
        base = utc(2026, 1, 1, 0, 0, 0)
        # Generate enough span to close at least 2 windows for every parametrised
        # seconds value (largest = 300 → need >= 600 seconds of ticks).
        span_seconds = max(700, seconds * 2 + 50)
        ticks = []
        for i in range(span_seconds):
            ticks.append(
                TickSpec(base + timedelta(seconds=i), str(100 + (i % 11)), "1")
            )
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        bars = list(
            resampler.resample_information_bar(
                symbol="KRW-BTC",
                bar_label=f"time_{seconds}",
                start=base,
                end=base + timedelta(seconds=span_seconds + 10),
            )
        )
        assert bars  # at least one closed bar
        for b in bars:
            assert b.bar_label == f"time_{seconds}"
            assert b.ts_close - b.genesis_ts == timedelta(seconds=seconds)
            assert b.high >= b.open
            assert b.low <= b.open

    def test_determinism_across_runs(self, tmp_path, write_ticks, utc):
        base = utc(2026, 1, 1, 0, 0, 0)
        ticks = [
            TickSpec(base + timedelta(seconds=i), str(100 + (i % 7)), "1")
            for i in range(50)
        ]
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        run1 = list(
            resampler.resample_information_bar(
                symbol="KRW-BTC",
                bar_label="time_13",
                start=base,
                end=base + timedelta(seconds=100),
            )
        )
        run2 = list(
            resampler.resample_information_bar(
                symbol="KRW-BTC",
                bar_label="time_13",
                start=base,
                end=base + timedelta(seconds=100),
            )
        )
        # Bit-for-bit equality — determinism contract from Story-3 aggregators.
        assert [b.model_dump() for b in run1] == [b.model_dump() for b in run2]
