"""Cold path DuckDB resample_information_bar — volume / tick / dollar bars.

Routes raw ticks through the Story-3 (MCT-137) aggregation core. Tests assert:
- Volume threshold closure (sum of base-volume).
- Tick count threshold closure.
- Dollar (notional = price × qty) threshold closure.
- ``bar_label`` discriminator + threshold round-trip.
- Determinism across repeated runs.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from mctrader_data.cold import DuckDBResampler

from .conftest import TickSpec


class TestVolumeBar:
    def test_volume_bar_closes_when_cumulative_volume_reaches_threshold(
        self, tmp_path, write_ticks, utc
    ):
        base = utc(2026, 1, 1, 0, 0, 0)
        # threshold=10 → bar closes on the tick whose merge sums to >= 10.
        ticks = [
            TickSpec(base + timedelta(seconds=i), "100", "3")
            for i in range(8)
        ]
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        bars = list(
            resampler.resample_information_bar(
                symbol="KRW-BTC",
                bar_label="vol_10",
                start=base,
                end=base + timedelta(seconds=10),
            )
        )
        # 3 + 3 + 3 + 3 = 12 → first bar closes on tick #4 (cumulative volume 12 >= 10).
        # Then tick #5..#8 → 3 + 3 + 3 + 3 = 12 → second bar closes on tick #8.
        assert len(bars) == 2
        assert bars[0].bar_label == "vol_10"
        assert bars[0].volume == Decimal("12")
        assert bars[1].volume == Decimal("12")


class TestTickBar:
    def test_tick_bar_closes_at_fixed_count(self, tmp_path, write_ticks, utc):
        base = utc(2026, 1, 1, 0, 0, 0)
        ticks = [TickSpec(base + timedelta(seconds=i), str(100 + i), "1") for i in range(10)]
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        bars = list(
            resampler.resample_information_bar(
                symbol="KRW-BTC",
                bar_label="tick_5",
                start=base,
                end=base + timedelta(seconds=20),
            )
        )
        assert len(bars) == 2
        for b in bars:
            assert b.bar_label == "tick_5"


class TestDollarBar:
    def test_dollar_bar_closes_when_notional_reaches_threshold(
        self, tmp_path, write_ticks, utc
    ):
        base = utc(2026, 1, 1, 0, 0, 0)
        # price=100, qty=1 → 100 KRW notional per tick. threshold=500 → closes on tick #5.
        ticks = [TickSpec(base + timedelta(seconds=i), "100", "1") for i in range(10)]
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        bars = list(
            resampler.resample_information_bar(
                symbol="KRW-BTC",
                bar_label="dollar_500",
                start=base,
                end=base + timedelta(seconds=20),
            )
        )
        assert len(bars) == 2
        for b in bars:
            assert b.bar_label == "dollar_500"
            assert b.value == Decimal("500")


class TestBarLabelDiscriminator:
    @pytest.mark.parametrize("label", [
        "garbage",
        "minute_60",
        "vol_",
        "vol_-1",
        "vol_0",
    ])
    def test_invalid_bar_label_rejected(self, tmp_path, label, utc):
        resampler = DuckDBResampler(root=tmp_path)
        base = utc(2026, 1, 1, 0, 0, 0)
        with pytest.raises(ValueError):
            list(
                resampler.resample_information_bar(
                    symbol="KRW-BTC",
                    bar_label=label,
                    start=base,
                    end=base + timedelta(minutes=1),
                )
            )


class TestDeterminism:
    def test_volume_bar_byte_identical_across_runs(self, tmp_path, write_ticks, utc):
        base = utc(2026, 1, 1, 0, 0, 0)
        ticks = [TickSpec(base + timedelta(seconds=i), str(100 + (i % 5)), "2") for i in range(40)]
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        run1 = [
            b.model_dump() for b in resampler.resample_information_bar(
                "KRW-BTC", "vol_15", base, base + timedelta(seconds=60)
            )
        ]
        run2 = [
            b.model_dump() for b in resampler.resample_information_bar(
                "KRW-BTC", "vol_15", base, base + timedelta(seconds=60)
            )
        ]
        assert run1 == run2
