"""Cold path partition pruning — Hive predicate pushdown.

These tests assert that:
- Predicates on Hive partition columns (``symbol`` / ``date`` / ``exchange``)
  restrict the result set correctly when multiple symbols / dates / exchanges
  coexist under the same root.
- Date-range pruning skips out-of-window partitions (no rows leaked).
- Exchange filter narrows results to the requested exchange.
"""

from __future__ import annotations

from datetime import timedelta

from mctrader_data.cold import DuckDBResampler

from .conftest import TickSpec


class TestSymbolPruning:
    def test_only_requested_symbol_returned(self, tmp_path, write_ticks, utc):
        base = utc(2026, 1, 1, 0, 0, 0)
        ticks = []
        for i in range(20):
            ticks.append(TickSpec(base + timedelta(seconds=i), "100", "1", symbol="KRW-BTC"))
            ticks.append(TickSpec(base + timedelta(seconds=i), "200", "1", symbol="KRW-ETH"))
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        bars = list(
            resampler.resample_information_bar(
                symbol="KRW-BTC",
                bar_label="tick_5",
                start=base,
                end=base + timedelta(seconds=30),
            )
        )
        # Every emitted bar belongs to the requested symbol.
        from mctrader_market.types import Symbol
        assert all(b.symbol == Symbol.from_string("KRW-BTC") for b in bars)


class TestDatePruning:
    def test_date_range_excludes_out_of_window_partition(self, tmp_path, write_ticks, utc):
        # Day 1 has 5 ticks; Day 3 has 5 ticks; query window covers Day 1 only.
        day1 = utc(2026, 1, 1, 12, 0, 0)
        day3 = utc(2026, 1, 3, 12, 0, 0)
        ticks = []
        for i in range(5):
            ticks.append(TickSpec(day1 + timedelta(seconds=i), "100", "1"))
            ticks.append(TickSpec(day3 + timedelta(seconds=i), "200", "1"))
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        bars = list(
            resampler.resample_information_bar(
                symbol="KRW-BTC",
                bar_label="tick_3",
                start=utc(2026, 1, 1, 0, 0, 0),
                end=utc(2026, 1, 2, 0, 0, 0),
            )
        )
        # All bars must come from Day 1 only — Day 3 partition pruned out.
        for b in bars:
            assert utc(2026, 1, 1, 0, 0, 0) <= b.genesis_ts < utc(2026, 1, 2, 0, 0, 0)


class TestExchangePruning:
    def test_exchange_filter_narrows_results(self, tmp_path, write_ticks, utc):
        base = utc(2026, 1, 1, 0, 0, 0)
        ticks = []
        for i in range(10):
            ticks.append(TickSpec(base + timedelta(seconds=i), "100", "1", exchange="bithumb"))
            ticks.append(TickSpec(base + timedelta(seconds=i), "200", "1", exchange="upbit"))
        write_ticks(ticks)

        resampler = DuckDBResampler(root=tmp_path)
        only_bithumb = list(
            resampler.resample_information_bar(
                symbol="KRW-BTC",
                bar_label="tick_5",
                start=base,
                end=base + timedelta(seconds=30),
                exchange="bithumb",
            )
        )
        assert all(b.exchange == "bithumb" for b in only_bithumb)
