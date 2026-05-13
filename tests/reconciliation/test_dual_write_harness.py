"""Dual-write harness — legacy candle vs transaction-derived diff (Story MCT-145).

Uses in-memory provider stubs implementing the Protocol contracts. Real-world
runs wire :class:`mctrader_data.cold.duckdb_resample.DuckDBResampler` to the
``DerivedCandleReader`` slot and a legacy-Parquet reader to ``LegacyCandleReader``;
both are external to this harness module.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mctrader_data.reconciliation import (
    DualWriteHarness,
)
from mctrader_data.reconciliation.dual_write_harness import SymbolContract
from mctrader_market.candle import CandleModel
from mctrader_market.types import Symbol, Timeframe


def _candle(
    ts: datetime,
    *,
    source: str,
    open_: Decimal = Decimal("100"),
    high: Decimal = Decimal("101"),
    low: Decimal = Decimal("99"),
    close: Decimal = Decimal("100"),
    volume: Decimal = Decimal("10"),
    symbol: str = "KRW-BTC",
    exchange: str = "bithumb",
    timeframe: Timeframe = Timeframe.M1,
) -> CandleModel:
    return CandleModel(
        ts_utc=ts,
        exchange=exchange,
        symbol=Symbol.from_string(symbol),
        timeframe=timeframe,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        value=open_ * volume,
        source=source,  # type: ignore[arg-type]
    )


class _StubLegacy:
    def __init__(self, candles_by_symbol: dict[str, list[CandleModel]]):
        self._by_symbol = candles_by_symbol

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[CandleModel]:
        return [
            c for c in self._by_symbol.get(symbol, [])
            if start <= c.ts_utc < end
        ]


class _StubDerived:
    def __init__(self, candles_by_symbol: dict[str, list[CandleModel]]):
        self._by_symbol = candles_by_symbol

    def resample_time(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        *,
        exchange: str | None = None,
    ) -> Iterator[CandleModel]:
        for c in self._by_symbol.get(symbol, []):
            if start <= c.ts_utc < end:
                yield c


class TestSampling:
    def test_empty_universe_yields_empty_report(self):
        harness = DualWriteHarness(
            legacy_provider=_StubLegacy({}),
            derived_provider=_StubDerived({}),
            random_seed=20260512,
        )
        report = harness.run(universe=[], day=date(2026, 5, 12))
        assert report.sample_size == 0
        assert report.diffs == ()

    def test_same_seed_yields_reproducible_sample(self):
        universe = [
            SymbolContract(symbol=f"KRW-S{i:03d}", timeframe="1m", exchange="bithumb")
            for i in range(100)
        ]
        h1 = DualWriteHarness(
            legacy_provider=_StubLegacy({}),
            derived_provider=_StubDerived({}),
            random_seed=20260512,
        )
        h2 = DualWriteHarness(
            legacy_provider=_StubLegacy({}),
            derived_provider=_StubDerived({}),
            random_seed=20260512,
        )
        r1 = h1.run(universe=universe, day=date(2026, 5, 12))
        r2 = h2.run(universe=universe, day=date(2026, 5, 12))
        # Both runs select the same symbols → same diff entries (all empty here)
        symbols_1 = tuple(d.symbol for d in r1.diffs)
        symbols_2 = tuple(d.symbol for d in r2.diffs)
        assert symbols_1 == symbols_2

    def test_min_sample_clamp_applied(self):
        """Tiny universe (5 symbols) with 2% fraction → min_sample=3 clamps up."""
        universe = [
            SymbolContract(symbol=f"KRW-S{i:03d}", timeframe="1m", exchange="bithumb")
            for i in range(5)
        ]
        harness = DualWriteHarness(
            legacy_provider=_StubLegacy({}),
            derived_provider=_StubDerived({}),
            sample_fraction=Decimal("0.02"),
            min_sample=3,
            max_sample=10,
            random_seed=1,
        )
        report = harness.run(universe=universe, day=date(2026, 5, 12))
        # 0.02 × 5 = 0.1 → 0, clamped up to min_sample=3
        assert report.sample_size == 3

    def test_max_sample_clamp_applied(self):
        universe = [
            SymbolContract(symbol=f"KRW-S{i:03d}", timeframe="1m", exchange="bithumb")
            for i in range(1000)
        ]
        harness = DualWriteHarness(
            legacy_provider=_StubLegacy({}),
            derived_provider=_StubDerived({}),
            sample_fraction=Decimal("0.5"),  # 500 ≥ max_sample=100
            min_sample=10,
            max_sample=100,
            random_seed=1,
        )
        report = harness.run(universe=universe, day=date(2026, 5, 12))
        assert report.sample_size == 100


class TestDiff:
    def test_identical_legacy_and_derived_yield_full_match(self):
        base = datetime(2026, 5, 12, tzinfo=timezone.utc)
        legacy_candles = [
            _candle(base + timedelta(minutes=i), source="legacy_candle")
            for i in range(10)
        ]
        derived_candles = [
            _candle(base + timedelta(minutes=i), source="transaction_derived")
            for i in range(10)
        ]
        # legacy and derived share identical OHLCV by construction (same ctor args
        # except for source), so the diff should report all matched.
        harness = DualWriteHarness(
            legacy_provider=_StubLegacy({"KRW-BTC": legacy_candles}),
            derived_provider=_StubDerived({"KRW-BTC": derived_candles}),
            sample_fraction=Decimal("1.0"),
            min_sample=1,
            max_sample=10,
            random_seed=1,
        )
        report = harness.run(
            universe=[SymbolContract(symbol="KRW-BTC", timeframe="1m")],
            day=date(2026, 5, 12),
        )
        assert len(report.diffs) == 1
        d = report.diffs[0]
        assert d.legacy_count == 10
        assert d.derived_count == 10
        assert d.matched_count == 10
        assert d.mismatch_count == 0
        assert d.count_mismatch_pct == Decimal(0)

    def test_missing_derived_candle_shows_as_legacy_only(self):
        base = datetime(2026, 5, 12, tzinfo=timezone.utc)
        legacy_candles = [
            _candle(base + timedelta(minutes=i), source="legacy_candle")
            for i in range(10)
        ]
        derived_candles = [
            _candle(base + timedelta(minutes=i), source="transaction_derived")
            for i in range(10) if i != 5
        ]
        harness = DualWriteHarness(
            legacy_provider=_StubLegacy({"KRW-BTC": legacy_candles}),
            derived_provider=_StubDerived({"KRW-BTC": derived_candles}),
            sample_fraction=Decimal("1.0"),
            min_sample=1,
            max_sample=10,
            random_seed=1,
        )
        report = harness.run(
            universe=[SymbolContract(symbol="KRW-BTC", timeframe="1m")],
            day=date(2026, 5, 12),
        )
        d = report.diffs[0]
        assert d.legacy_count == 10
        assert d.derived_count == 9
        assert d.matched_count == 9
        assert len(d.legacy_only_ts) == 1
        assert len(d.derived_only_ts) == 0
        assert d.count_mismatch_pct == Decimal(1) / Decimal(10)

    def test_ohlcv_cell_diff_recorded(self):
        base = datetime(2026, 5, 12, tzinfo=timezone.utc)
        legacy_candles = [_candle(base, source="legacy_candle", close=Decimal("100.5"))]
        derived_candles = [_candle(base, source="transaction_derived", close=Decimal("100.6"))]
        harness = DualWriteHarness(
            legacy_provider=_StubLegacy({"KRW-BTC": legacy_candles}),
            derived_provider=_StubDerived({"KRW-BTC": derived_candles}),
            sample_fraction=Decimal("1.0"),
            min_sample=1,
            max_sample=10,
            random_seed=1,
        )
        report = harness.run(
            universe=[SymbolContract(symbol="KRW-BTC", timeframe="1m")],
            day=date(2026, 5, 12),
        )
        d = report.diffs[0]
        assert d.matched_count == 0
        assert len(d.ohlcv_mismatches) == 1
        ts_iso, summary = d.ohlcv_mismatches[0]
        assert "close" in summary
        assert "100.5" in summary and "100.6" in summary


class TestReportSummary:
    def test_aggregate_count_mismatch_pct(self):
        base = datetime(2026, 5, 12, tzinfo=timezone.utc)
        # Symbol A: 10 legacy / 9 derived. Symbol B: 10 / 10. → 1 / 20 = 0.05
        a_legacy = [
            _candle(base + timedelta(minutes=i), source="legacy_candle", symbol="KRW-A")
            for i in range(10)
        ]
        a_derived = [
            _candle(base + timedelta(minutes=i), source="transaction_derived", symbol="KRW-A")
            for i in range(10) if i != 5
        ]
        b_legacy = [
            _candle(base + timedelta(minutes=i), source="legacy_candle", symbol="KRW-B")
            for i in range(10)
        ]
        b_derived = [
            _candle(base + timedelta(minutes=i), source="transaction_derived", symbol="KRW-B")
            for i in range(10)
        ]
        harness = DualWriteHarness(
            legacy_provider=_StubLegacy({"KRW-A": a_legacy, "KRW-B": b_legacy}),
            derived_provider=_StubDerived({"KRW-A": a_derived, "KRW-B": b_derived}),
            sample_fraction=Decimal("1.0"),
            min_sample=1,
            max_sample=10,
            random_seed=1,
        )
        report = harness.run(
            universe=[
                SymbolContract(symbol="KRW-A", timeframe="1m"),
                SymbolContract(symbol="KRW-B", timeframe="1m"),
            ],
            day=date(2026, 5, 12),
        )
        assert report.total_legacy_bars == 20
        assert report.total_derived_bars == 19
        assert report.total_mismatches == 1
        assert report.aggregate_count_mismatch_pct == Decimal(1) / Decimal(20)

    def test_summary_dict_contract(self):
        harness = DualWriteHarness(
            legacy_provider=_StubLegacy({}),
            derived_provider=_StubDerived({}),
            random_seed=1,
        )
        report = harness.run(universe=[], day=date(2026, 5, 12))
        s = report.summary()
        assert {
            "day", "sample_size", "universe_size", "sample_fraction_used",
            "total_legacy_bars", "total_derived_bars", "total_mismatches",
            "aggregate_count_mismatch_pct", "random_seed",
        } <= set(s.keys())


class TestValidation:
    def test_invalid_sample_fraction(self):
        with pytest.raises(ValueError, match="sample_fraction"):
            DualWriteHarness(
                legacy_provider=_StubLegacy({}),
                derived_provider=_StubDerived({}),
                sample_fraction=Decimal(0),
                random_seed=1,
            )

    def test_min_sample_must_be_positive(self):
        with pytest.raises(ValueError, match="min_sample"):
            DualWriteHarness(
                legacy_provider=_StubLegacy({}),
                derived_provider=_StubDerived({}),
                min_sample=0,
                random_seed=1,
            )

    def test_max_sample_below_min_rejected(self):
        with pytest.raises(ValueError, match="max_sample"):
            DualWriteHarness(
                legacy_provider=_StubLegacy({}),
                derived_provider=_StubDerived({}),
                min_sample=100,
                max_sample=50,
                random_seed=1,
            )
