"""``source = "transaction_derived"`` invariant (ADR-009 §D8) on Cold path output.

Story-2 (MCT-136) introduced the provenance discriminator. Story-5 (MCT-139,
this Story) must stamp **every** Cold path candle with the
``transaction_derived`` value so downstream consumers (engine paper/live
reconciliation, web UI) can distinguish Cold-derived OHLCV from legacy candle
emitter output.
"""

from __future__ import annotations

from datetime import timedelta

from mctrader_data.cold import DuckDBResampler

from .conftest import TickSpec


class TestDerivedViewSourceField:
    def test_every_candle_carries_transaction_derived_source(
        self, tmp_path, write_ticks, utc
    ):
        base = utc(2026, 1, 1, 0, 0, 0)
        ticks = []
        for i in range(120):
            ticks.append(TickSpec(base + timedelta(seconds=i), str(100 + (i % 7)), "1"))
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
        assert candles, "fixture should yield at least one closed minute bar"
        for c in candles:
            assert c.source == "transaction_derived"
            assert c.contract_metadata_version == "ohlcv.v1"

    def test_information_bar_contract_metadata_version(self, tmp_path, write_ticks, utc):
        base = utc(2026, 1, 1, 0, 0, 0)
        ticks = [TickSpec(base + timedelta(seconds=i), "100", "1") for i in range(30)]
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        bars = list(
            resampler.resample_information_bar(
                symbol="KRW-BTC",
                bar_label="tick_5",
                start=base,
                end=base + timedelta(seconds=60),
            )
        )
        assert bars
        for b in bars:
            assert b.contract_metadata_version == "info_bar.v1"
