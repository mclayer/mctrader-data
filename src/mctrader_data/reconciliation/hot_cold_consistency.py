"""Hot/Cold consistency harness — drift SLO < 0.01% gate (ADR-025 §D5).

The Hot path (engine streaming aggregator, MCT-142) and the Cold path
(``DuckDBResampler``, MCT-139) both delegate to the same Story-3 aggregator
core (MCT-137). Given identical tick input, both paths MUST emit
byte-identical :class:`InformationBarModel` sequences.

This harness verifies that invariant. The SLO threshold mirrors ADR-025 §D5
Risk-2 mitigation: bar-count mismatch fraction < 0.01% is allowed for
operational tolerance (e.g. partial windows at the edge of a backfill range);
anything ≥ 0.01% trips the fail-closed gate.

Public API
----------
::

    harness = HotColdConsistencyHarness(threshold=DRIFT_SLO_THRESHOLD)
    report = harness.compare(hot_bars=hot_iter, cold_bars=cold_iter)
    report.assert_within_slo()   # raises ConsistencyDriftError if drift ≥ SLO
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from mctrader_market.protocols.information_bar import InformationBarModel


DRIFT_SLO_THRESHOLD: Decimal = Decimal("0.0001")
"""Hot/Cold consistency SLO — 0.01% mismatch fraction.

ADR-025 §D5 Risk-2 mitigation. Bar-count mismatch ratio is computed as
``abs(hot_count - cold_count) / max(hot_count, cold_count)``; any value
greater than or equal to this threshold trips the fail-closed gate.
"""


class ConsistencyDriftError(RuntimeError):
    """Raised by :meth:`HotColdReport.assert_within_slo` when drift ≥ SLO.

    Attributes:
        drift: measured drift fraction (Decimal).
        threshold: SLO threshold against which drift was compared.
        report: the :class:`HotColdReport` that triggered the error.
    """

    def __init__(self, *, drift: Decimal, threshold: Decimal, report: HotColdReport) -> None:
        self.drift = drift
        self.threshold = threshold
        self.report = report
        super().__init__(
            f"Hot/Cold drift {drift!s} ≥ SLO {threshold!s} — fail-closed. "
            f"mismatch_count={report.mismatch_count} hot={report.hot_count} cold={report.cold_count}"
        )


@dataclass(frozen=True)
class _BarKey:
    """Comparison key over the bar identity tuple (excludes OHLCV payload)."""

    bar_label: str
    genesis_ts: str  # ISO-8601 string for stable hashing
    ts_close: str
    exchange: str
    symbol: str


@dataclass(frozen=True)
class _OHLCV:
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    value: Decimal | None


def _key_of(bar: InformationBarModel) -> _BarKey:
    return _BarKey(
        bar_label=bar.bar_label,
        genesis_ts=bar.genesis_ts.isoformat(),
        ts_close=bar.ts_close.isoformat(),
        exchange=bar.exchange,
        symbol=str(bar.symbol),
    )


def _ohlcv_of(bar: InformationBarModel) -> _OHLCV:
    return _OHLCV(
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        value=bar.value,
    )


@dataclass(frozen=True)
class HotColdReport:
    """Outcome of one Hot vs Cold comparison run.

    Attributes:
        hot_count: total bars emitted by the Hot path.
        cold_count: total bars emitted by the Cold path.
        matched_count: bars present in both with identical OHLCV.
        ohlcv_mismatches: list of ``(bar_key, hot_ohlcv, cold_ohlcv)`` tuples
            where the key matched but OHLCV did not.
        hot_only_keys: bar keys present in Hot but missing in Cold.
        cold_only_keys: bar keys present in Cold but missing in Hot.
        threshold: SLO threshold used for the gate.
    """

    hot_count: int
    cold_count: int
    matched_count: int
    ohlcv_mismatches: tuple[tuple[_BarKey, _OHLCV, _OHLCV], ...] = field(default_factory=tuple)
    hot_only_keys: tuple[_BarKey, ...] = field(default_factory=tuple)
    cold_only_keys: tuple[_BarKey, ...] = field(default_factory=tuple)
    threshold: Decimal = DRIFT_SLO_THRESHOLD

    @property
    def mismatch_count(self) -> int:
        """Total bar mismatches (key-level + OHLCV-level)."""
        return len(self.hot_only_keys) + len(self.cold_only_keys) + len(self.ohlcv_mismatches)

    @property
    def drift(self) -> Decimal:
        """Drift fraction: ``mismatch_count / max(hot_count, cold_count)``.

        Returns ``Decimal(0)`` when both counts are 0 (empty input is
        trivially consistent).
        """
        denom = max(self.hot_count, self.cold_count)
        if denom == 0:
            return Decimal(0)
        return Decimal(self.mismatch_count) / Decimal(denom)

    @property
    def is_within_slo(self) -> bool:
        return self.drift < self.threshold

    def assert_within_slo(self) -> None:
        """Raise :class:`ConsistencyDriftError` if drift ≥ SLO threshold."""
        d = self.drift
        if d >= self.threshold:
            raise ConsistencyDriftError(drift=d, threshold=self.threshold, report=self)

    def summary(self) -> dict[str, Any]:
        """Compact dict summary for logging / report archival."""
        return {
            "hot_count": self.hot_count,
            "cold_count": self.cold_count,
            "matched_count": self.matched_count,
            "mismatch_count": self.mismatch_count,
            "hot_only_count": len(self.hot_only_keys),
            "cold_only_count": len(self.cold_only_keys),
            "ohlcv_mismatch_count": len(self.ohlcv_mismatches),
            "drift": str(self.drift),
            "threshold": str(self.threshold),
            "is_within_slo": self.is_within_slo,
        }


class HotColdConsistencyHarness:
    """Compare Hot vs Cold path bar streams under the drift SLO.

    Parameters
    ----------
    threshold:
        Drift SLO fraction. Defaults to :data:`DRIFT_SLO_THRESHOLD` (0.01%).

    Usage
    -----
    >>> harness = HotColdConsistencyHarness()
    >>> report = harness.compare(hot_bars=hot_iter, cold_bars=cold_iter)
    >>> report.summary()
    {'hot_count': 1000, 'cold_count': 1000, 'matched_count': 1000, ...}
    >>> report.assert_within_slo()  # raises if drift >= 0.01%
    """

    def __init__(self, *, threshold: Decimal = DRIFT_SLO_THRESHOLD) -> None:
        if threshold <= 0:
            raise ValueError(f"threshold ({threshold}) must be > 0")
        self._threshold = threshold

    def compare(
        self,
        *,
        hot_bars: Iterable[InformationBarModel],
        cold_bars: Iterable[InformationBarModel],
    ) -> HotColdReport:
        """Stream both iterables fully, build per-key indexes, return report.

        Memory: O(hot_count + cold_count) — acceptable for the dual-write
        verification window (≤ few thousand bars per daily sample). For
        long-horizon backfills callers should chunk by symbol-window.
        """
        hot_map: dict[_BarKey, _OHLCV] = {}
        cold_map: dict[_BarKey, _OHLCV] = {}

        hot_count = 0
        for bar in hot_bars:
            hot_map[_key_of(bar)] = _ohlcv_of(bar)
            hot_count += 1
        cold_count = 0
        for bar in cold_bars:
            cold_map[_key_of(bar)] = _ohlcv_of(bar)
            cold_count += 1

        matched: list[_BarKey] = []
        ohlcv_mismatches: list[tuple[_BarKey, _OHLCV, _OHLCV]] = []
        for key, hot_ohlcv in hot_map.items():
            if key in cold_map:
                if hot_ohlcv == cold_map[key]:
                    matched.append(key)
                else:
                    ohlcv_mismatches.append((key, hot_ohlcv, cold_map[key]))
        hot_only = tuple(k for k in hot_map if k not in cold_map)
        cold_only = tuple(k for k in cold_map if k not in hot_map)

        return HotColdReport(
            hot_count=hot_count,
            cold_count=cold_count,
            matched_count=len(matched),
            ohlcv_mismatches=tuple(ohlcv_mismatches),
            hot_only_keys=hot_only,
            cold_only_keys=cold_only,
            threshold=self._threshold,
        )


__all__ = [
    "DRIFT_SLO_THRESHOLD",
    "ConsistencyDriftError",
    "HotColdConsistencyHarness",
    "HotColdReport",
]
