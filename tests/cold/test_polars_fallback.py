"""Cold path Polars fallback — DuckDB equivalence test.

Asserts that :class:`PolarsResampler` produces byte-identical bars to
:class:`DuckDBResampler` for the same fixture. This is the cross-engine SSOT
guarantee — both implementations route ticks through the Story-3 aggregation
core; only the scan / sort engine differs.

If Polars is unavailable in the test environment, the test is skipped (Polars
is an optional dependency, see :mod:`mctrader_data.cold.polars_fallback`).
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from .conftest import TickSpec

polars = pytest.importorskip("polars")  # noqa: F841  — skip module if missing


from mctrader_data.cold import DuckDBResampler, PolarsResampler  # noqa: E402


class TestEquivalence:
    def test_polars_matches_duckdb_information_bar(self, tmp_path, write_ticks, utc):
        base = utc(2026, 1, 1, 0, 0, 0)
        ticks = [
            TickSpec(base + timedelta(seconds=i), str(100 + (i % 9)), str(1 + (i % 4)))
            for i in range(60)
        ]
        write_ticks(ticks)

        duckdb_res = DuckDBResampler(root=tmp_path)
        polars_res = PolarsResampler(root=tmp_path)

        duckdb_bars = [
            b.model_dump() for b in duckdb_res.resample_information_bar(
                "KRW-BTC", "vol_20", base, base + timedelta(seconds=90)
            )
        ]
        polars_bars = [
            b.model_dump() for b in polars_res.resample_information_bar(
                "KRW-BTC", "vol_20", base, base + timedelta(seconds=90)
            )
        ]
        assert duckdb_bars == polars_bars

    def test_polars_matches_duckdb_time_candle(self, tmp_path, write_ticks, utc):
        base = utc(2026, 1, 1, 0, 0, 0)
        ticks = [
            TickSpec(base + timedelta(seconds=i), str(100 + (i % 5)), "1")
            for i in range(180)
        ]
        write_ticks(ticks)

        duckdb_res = DuckDBResampler(root=tmp_path)
        polars_res = PolarsResampler(root=tmp_path)

        duckdb_candles = [
            c.model_dump() for c in duckdb_res.resample_time(
                "KRW-BTC", "1m", base, base + timedelta(minutes=4)
            )
        ]
        polars_candles = [
            c.model_dump() for c in polars_res.resample_time(
                "KRW-BTC", "1m", base, base + timedelta(minutes=4)
            )
        ]
        assert duckdb_candles == polars_candles
