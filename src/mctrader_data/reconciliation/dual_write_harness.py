"""Dual-write reconciliation harness — legacy candle vs transaction-derived.

During the 2-4 week dual-write transition (Epic MCT-112 Story-11), the system
simultaneously emits:

- Legacy candle Parquet (exchange OHLCV REST/WS) — ``source="legacy_candle"``
- Transaction-derived candles (Cold path :class:`DuckDBResampler`) —
  ``source="transaction_derived"``

This harness draws a daily random sample of ``(symbol, contract, window)``
tuples (1-5% of the universe, OR a fixed 100 windows/day) and produces a
diff report: per-window bar count mismatch + OHLCV cell-level diff. The
report feeds the Story-12 retirement exit criterion — legacy candle can
only be retired when the harness reports stable bar-count parity over the
full 2-4 week window.

Sampling strategy
-----------------
- ``sample_fraction`` (default 0.02 = 2%) — proportional random sample with
  deterministic seed.
- ``min_sample`` / ``max_sample`` — clamp bounds (default 100 / 500).
- ``random_seed`` — required; ensures the harness is reproducible across
  retro audits.

Public API
----------
::

    harness = DualWriteHarness(
        legacy_provider=legacy_candle_reader,
        derived_provider=duckdb_resampler,
        sample_fraction=Decimal("0.02"),
        random_seed=20260512,
    )
    report = harness.run(universe=symbol_contract_pairs, day=date(2026, 5, 12))
    print(report.summary())
"""

from __future__ import annotations

import random
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from mctrader_market.candle import CandleModel


@runtime_checkable
class LegacyCandleReader(Protocol):
    """Provider for pre-cutoff ``source="legacy_candle"`` candles.

    Mirrors :class:`mctrader_engine.consumers.candle_view.LegacyCandleProvider`
    (Story MCT-143) — the same iterator-of-CandleModel contract.
    """

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> Iterable[CandleModel]:
        ...


@runtime_checkable
class DerivedCandleReader(Protocol):
    """Provider for ``source="transaction_derived"`` candles (e.g.
    :class:`mctrader_data.cold.duckdb_resample.DuckDBResampler`)."""

    def resample_time(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        *,
        exchange: str | None = None,
    ) -> Iterable[CandleModel]:
        ...


@dataclass(frozen=True)
class SymbolContract:
    """One sampled ``(symbol, timeframe, exchange)`` row for daily reconciliation."""

    symbol: str
    timeframe: str
    exchange: str | None = None


@dataclass(frozen=True)
class _OHLCVKey:
    """Hashable key used to align legacy vs derived candles for one window."""

    ts_utc: str  # ISO-8601


@dataclass(frozen=True)
class SymbolWindowDiff:
    """Diff outcome for one ``(symbol, day)`` reconciliation."""

    symbol: str
    timeframe: str
    day: date
    legacy_count: int
    derived_count: int
    matched_count: int
    legacy_only_ts: tuple[str, ...] = field(default_factory=tuple)
    derived_only_ts: tuple[str, ...] = field(default_factory=tuple)
    ohlcv_mismatches: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    """Each entry: ``(ts_iso, diff_summary)`` where diff_summary is a short
    'open/high/low/close/volume' delta string. Detailed cell-level diff kept
    to a string to bound report size during multi-week runs."""

    @property
    def mismatch_count(self) -> int:
        return len(self.legacy_only_ts) + len(self.derived_only_ts) + len(self.ohlcv_mismatches)

    @property
    def count_mismatch_pct(self) -> Decimal:
        """``abs(legacy - derived) / max(legacy, derived)``, ``Decimal(0)`` when both 0."""
        denom = max(self.legacy_count, self.derived_count)
        if denom == 0:
            return Decimal(0)
        delta = abs(self.legacy_count - self.derived_count)
        return Decimal(delta) / Decimal(denom)


@dataclass(frozen=True)
class DualWriteReport:
    """Daily reconciliation outcome across the sampled universe."""

    day: date
    sample_size: int
    universe_size: int
    sample_fraction_used: Decimal
    diffs: tuple[SymbolWindowDiff, ...]
    random_seed: int
    edge_case_attached: tuple[str, ...] = field(default_factory=tuple)

    @property
    def total_legacy_bars(self) -> int:
        return sum(d.legacy_count for d in self.diffs)

    @property
    def total_derived_bars(self) -> int:
        return sum(d.derived_count for d in self.diffs)

    @property
    def total_mismatches(self) -> int:
        return sum(d.mismatch_count for d in self.diffs)

    @property
    def aggregate_count_mismatch_pct(self) -> Decimal:
        denom = max(self.total_legacy_bars, self.total_derived_bars)
        if denom == 0:
            return Decimal(0)
        return Decimal(self.total_mismatches) / Decimal(denom)

    def summary(self) -> dict[str, Any]:
        """Compact dict suitable for log archival."""
        return {
            "day": self.day.isoformat(),
            "sample_size": self.sample_size,
            "universe_size": self.universe_size,
            "sample_fraction_used": str(self.sample_fraction_used),
            "total_legacy_bars": self.total_legacy_bars,
            "total_derived_bars": self.total_derived_bars,
            "total_mismatches": self.total_mismatches,
            "aggregate_count_mismatch_pct": str(self.aggregate_count_mismatch_pct),
            "random_seed": self.random_seed,
            "edge_case_attached": list(self.edge_case_attached),
        }


def _ohlcv_diff_summary(legacy: CandleModel, derived: CandleModel) -> str:
    """One-line diff summary across O/H/L/C/V (and optional value)."""
    parts: list[str] = []
    if legacy.open != derived.open:
        parts.append(f"open: {legacy.open} vs {derived.open}")
    if legacy.high != derived.high:
        parts.append(f"high: {legacy.high} vs {derived.high}")
    if legacy.low != derived.low:
        parts.append(f"low: {legacy.low} vs {derived.low}")
    if legacy.close != derived.close:
        parts.append(f"close: {legacy.close} vs {derived.close}")
    if legacy.volume != derived.volume:
        parts.append(f"volume: {legacy.volume} vs {derived.volume}")
    if legacy.value != derived.value and legacy.value is not None and derived.value is not None:
        parts.append(f"value: {legacy.value} vs {derived.value}")
    return "; ".join(parts) if parts else "<identical>"


class DualWriteHarness:
    """Daily reconciliation of legacy vs transaction-derived candles.

    Parameters
    ----------
    legacy_provider:
        Source of ``source="legacy_candle"`` candles (pre-cutoff path).
    derived_provider:
        Source of ``source="transaction_derived"`` candles
        (:class:`DuckDBResampler` or equivalent).
    sample_fraction:
        Fraction of the universe to sample per run. Default ``Decimal("0.02")``.
    min_sample / max_sample:
        Clamp bounds for the sample size. Default 100 / 500.
    random_seed:
        Required deterministic seed for reproducibility audit.
    """

    def __init__(
        self,
        *,
        legacy_provider: LegacyCandleReader,
        derived_provider: DerivedCandleReader,
        sample_fraction: Decimal = Decimal("0.02"),
        min_sample: int = 100,
        max_sample: int = 500,
        random_seed: int,
    ) -> None:
        if sample_fraction <= 0 or sample_fraction > 1:
            raise ValueError(f"sample_fraction must be in (0, 1], got {sample_fraction}")
        if min_sample <= 0:
            raise ValueError(f"min_sample must be > 0, got {min_sample}")
        if max_sample < min_sample:
            raise ValueError(f"max_sample ({max_sample}) must be >= min_sample ({min_sample})")
        self._legacy = legacy_provider
        self._derived = derived_provider
        self._sample_fraction = sample_fraction
        self._min_sample = min_sample
        self._max_sample = max_sample
        self._random_seed = random_seed

    def run(
        self,
        *,
        universe: Iterable[SymbolContract],
        day: date,
        edge_case_attached: Iterable[str] | None = None,
    ) -> DualWriteReport:
        """Sample universe → fetch legacy + derived for each → emit report.

        Args:
            universe: full pool of ``(symbol, timeframe, exchange)`` to sample
                from. Iterated fully in memory; for very large universes the
                caller should chunk by exchange.
            day: UTC day to reconcile. Window is ``[day 00:00, day+1 00:00)``.
            edge_case_attached: optional list of edge-case categories that
                were independently exercised in this run (informational only,
                recorded on the report for retro audit).

        Returns:
            :class:`DualWriteReport`.
        """
        universe_list = list(universe)
        if not universe_list:
            return DualWriteReport(
                day=day,
                sample_size=0,
                universe_size=0,
                sample_fraction_used=self._sample_fraction,
                diffs=(),
                random_seed=self._random_seed,
                edge_case_attached=tuple(edge_case_attached or ()),
            )

        rng = random.Random(self._random_seed)
        # Compute target sample size from fraction, clamped to [min, max].
        target_decimal = Decimal(len(universe_list)) * self._sample_fraction
        target = int(target_decimal)
        if target < self._min_sample:
            target = self._min_sample
        if target > self._max_sample:
            target = self._max_sample
        if target > len(universe_list):
            target = len(universe_list)
        sample = rng.sample(universe_list, target)

        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        diffs: list[SymbolWindowDiff] = list(
            self._iter_diffs(sample=sample, day=day, start=start, end=end)
        )
        return DualWriteReport(
            day=day,
            sample_size=len(sample),
            universe_size=len(universe_list),
            sample_fraction_used=self._sample_fraction,
            diffs=tuple(diffs),
            random_seed=self._random_seed,
            edge_case_attached=tuple(edge_case_attached or ()),
        )

    # ------------------------------------------------------------- internals

    def _iter_diffs(
        self,
        *,
        sample: list[SymbolContract],
        day: date,
        start: datetime,
        end: datetime,
    ) -> Iterator[SymbolWindowDiff]:
        for entry in sample:
            legacy_iter = self._legacy.get_candles(entry.symbol, entry.timeframe, start, end)
            derived_iter = self._derived.resample_time(
                entry.symbol, entry.timeframe, start, end, exchange=entry.exchange
            )
            yield _diff_window(entry=entry, day=day, legacy=legacy_iter, derived=derived_iter)


def _diff_window(
    *,
    entry: SymbolContract,
    day: date,
    legacy: Iterable[CandleModel],
    derived: Iterable[CandleModel],
) -> SymbolWindowDiff:
    """Build :class:`SymbolWindowDiff` for one ``(symbol, day)`` cell."""
    legacy_by_ts: dict[_OHLCVKey, CandleModel] = {}
    derived_by_ts: dict[_OHLCVKey, CandleModel] = {}

    for c in legacy:
        legacy_by_ts[_OHLCVKey(ts_utc=c.ts_utc.isoformat())] = c
    for c in derived:
        derived_by_ts[_OHLCVKey(ts_utc=c.ts_utc.isoformat())] = c

    matched = 0
    ohlcv_mismatches: list[tuple[str, str]] = []
    for k, lc in legacy_by_ts.items():
        if k in derived_by_ts:
            dc = derived_by_ts[k]
            if _candle_ohlcv_equal(lc, dc):
                matched += 1
            else:
                ohlcv_mismatches.append((k.ts_utc, _ohlcv_diff_summary(lc, dc)))

    legacy_only = tuple(k.ts_utc for k in legacy_by_ts if k not in derived_by_ts)
    derived_only = tuple(k.ts_utc for k in derived_by_ts if k not in legacy_by_ts)

    return SymbolWindowDiff(
        symbol=entry.symbol,
        timeframe=entry.timeframe,
        day=day,
        legacy_count=len(legacy_by_ts),
        derived_count=len(derived_by_ts),
        matched_count=matched,
        legacy_only_ts=legacy_only,
        derived_only_ts=derived_only,
        ohlcv_mismatches=tuple(ohlcv_mismatches),
    )


def _candle_ohlcv_equal(a: CandleModel, b: CandleModel) -> bool:
    """Compare OHLCV (and value if both non-None) — exact Decimal equality."""
    if a.open != b.open:
        return False
    if a.high != b.high:
        return False
    if a.low != b.low:
        return False
    if a.close != b.close:
        return False
    if a.volume != b.volume:
        return False
    return not (a.value is not None and b.value is not None and a.value != b.value)


__all__ = [
    "DerivedCandleReader",
    "DualWriteHarness",
    "DualWriteReport",
    "LegacyCandleReader",
    "SymbolContract",
    "SymbolWindowDiff",
]
