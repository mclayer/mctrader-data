"""Deterministic edge-case fixture generators (Story MCT-145).

Three categories of edge cases that historically cause Hot/Cold drift in
threshold-based aggregation. Each generator is **fully deterministic** —
identical ``seed`` produces identical tick sequences across Python versions
and platforms (``random.Random`` is the canonical SSOT).

Categories
----------
1. :func:`generate_threshold_boundary` — cumulative metric lands **exactly** on
   the threshold value (``state.volume == threshold``, not >). Per ADR-025
   §determinism the canonical ``tie_breaking="current_tick"`` rule closes the
   bar on this tick — but a buggy ``>`` comparison would carry over.
2. :func:`generate_time_bar_boundary` — tick whose ``ts_utc`` lands at exactly
   ``window_end`` (the half-open ``[start, end)`` boundary). The new tick must
   open a fresh bar, never be merged into the previous.
3. :func:`generate_krw_rounding_edge` — notional accumulator boundary where
   Decimal precision matters: ``price × quantity`` with non-trivial fractional
   parts that, if floored or rounded mid-pipeline, would skew the dollar bar
   closure tick.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Literal


@dataclass(frozen=True)
class EdgeCaseFixture:
    """Deterministic tick fixture for a specific edge case.

    Attributes:
        category: ``"threshold_boundary"`` | ``"time_bar_boundary"`` |
            ``"krw_rounding_edge"``.
        ticks: list of ``(ts_utc, price, quantity)`` tuples; consumers convert
            to :class:`~mctrader_market.schemas.tick.TickRowV1_1` via the
            harness adapter or the Cold path test conftest.
        expected_bar_count: number of bars the aggregator MUST close given
            the canonical tie-breaking rule.
        seed: the seed used — required for reproducibility audit.
        symbol: canonical ``"{quote}-{base}"`` (default ``"KRW-BTC"``).
        exchange: default ``"bithumb"``.
    """

    category: Literal["threshold_boundary", "time_bar_boundary", "krw_rounding_edge"]
    ticks: tuple[tuple[datetime, Decimal, Decimal], ...]
    expected_bar_count: int
    seed: int
    symbol: str = "KRW-BTC"
    exchange: str = "bithumb"


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def generate_threshold_boundary(
    *,
    seed: int,
    threshold: Decimal,
    bar_count: int = 3,
    base_ts: datetime | None = None,
) -> EdgeCaseFixture:
    """Generate a sequence of volume ticks where cumulative quantity lands
    **exactly** on ``threshold`` for each bar.

    The generator emits ``N-1`` non-boundary ticks per bar with random small
    quantities, then a closing tick whose quantity exactly fills the remaining
    gap to ``threshold``. The canonical ``current_tick`` tie-breaking rule must
    close the bar on the gap-filler tick.

    Args:
        seed: deterministic seed for the non-boundary tick quantities.
        threshold: target volume threshold (Decimal, > 0).
        bar_count: number of bars to generate (each lands on the boundary).
        base_ts: starting UTC timestamp; defaults to ``2026-01-01T00:00:00Z``.

    Returns:
        :class:`EdgeCaseFixture` with ``category="threshold_boundary"``.
    """
    if threshold <= 0:
        raise ValueError(f"threshold ({threshold}) must be > 0")
    if bar_count <= 0:
        raise ValueError(f"bar_count ({bar_count}) must be > 0")

    rng = random.Random(seed)
    base = base_ts or _utc(2026, 1, 1)
    # Use Decimal step quantities — never float — to preserve drift-free math.
    ticks: list[tuple[datetime, Decimal, Decimal]] = []
    ts = base
    # Generate "partials" totalling threshold - 1 per bar, then a closing tick
    # whose quantity is exactly the gap (i.e. 1 unit) so cumulative == threshold.
    for _ in range(bar_count):
        partials_count = 4
        # Each partial is in [1/4, 3/4] of (threshold - 1) / partials_count;
        # the closing tick fills the residual exactly.
        remaining = threshold - Decimal(1)
        for partial_idx in range(partials_count - 1):
            # deterministic Decimal in [0.1, 0.9] of the per-partial budget
            choice = Decimal(rng.randint(10, 90)) / Decimal(100)
            per_partial = (remaining / Decimal(partials_count - partial_idx))
            qty = (per_partial * choice).quantize(Decimal("0.01"))
            if qty <= 0:
                qty = Decimal("0.01")
            ticks.append((ts, Decimal("100"), qty))
            ts = ts + timedelta(seconds=1)
            remaining = remaining - qty
        # last partial: whatever is left up to threshold-1
        if remaining > 0:
            ticks.append((ts, Decimal("100"), remaining))
            ts = ts + timedelta(seconds=1)
        # gap-filler closing tick: exactly +1 → cumulative == threshold.
        ticks.append((ts, Decimal("100"), Decimal(1)))
        ts = ts + timedelta(seconds=1)

    return EdgeCaseFixture(
        category="threshold_boundary",
        ticks=tuple(ticks),
        expected_bar_count=bar_count,
        seed=seed,
    )


def generate_time_bar_boundary(
    *,
    seed: int,
    timeframe_seconds: int,
    bar_count: int = 3,
    base_ts: datetime | None = None,
) -> EdgeCaseFixture:
    """Generate a sequence where each bar contains 2 ticks: one at
    ``window_start + 1us`` and one at ``window_end - 1us`` — and a fresh tick
    at exactly ``window_end`` (the next bar's start) which must open a new bar.

    Args:
        seed: deterministic seed for price/qty randomisation.
        timeframe_seconds: bar window in seconds (whole int, > 0).
        bar_count: number of bars to generate.
        base_ts: starting UTC timestamp; defaults to a timeframe-aligned epoch.

    Returns:
        :class:`EdgeCaseFixture` with ``category="time_bar_boundary"``.
    """
    if timeframe_seconds <= 0:
        raise ValueError(f"timeframe_seconds ({timeframe_seconds}) must be > 0")
    if bar_count <= 0:
        raise ValueError(f"bar_count ({bar_count}) must be > 0")

    rng = random.Random(seed)
    # Pick a base aligned to the timeframe so window math is exact.
    base = _utc(2026, 1, 1) if base_ts is None else base_ts

    tf = timedelta(seconds=timeframe_seconds)
    ticks: list[tuple[datetime, Decimal, Decimal]] = []
    for i in range(bar_count):
        window_start = base + i * tf
        window_end = window_start + tf
        # mid-window tick
        mid_price = Decimal(100 + rng.randint(0, 10))
        mid_qty = Decimal(rng.randint(1, 5))
        ticks.append((window_start + timedelta(microseconds=1), mid_price, mid_qty))
        # right-edge-inclusive tick (still inside [window_start, window_end))
        edge_price = Decimal(100 + rng.randint(0, 10))
        edge_qty = Decimal(rng.randint(1, 5))
        ticks.append((window_end - timedelta(microseconds=1), edge_price, edge_qty))
    # Finally, one tick exactly at the last window_end — this must open a NEW
    # bar (boundary is open on the right). We add it but it will not close,
    # so it is not counted in expected_bar_count.
    ticks.append((base + bar_count * tf, Decimal(100), Decimal(1)))

    return EdgeCaseFixture(
        category="time_bar_boundary",
        ticks=tuple(ticks),
        expected_bar_count=bar_count,
        seed=seed,
    )


def generate_krw_rounding_edge(
    *,
    seed: int,
    threshold: Decimal,
    bar_count: int = 3,
    base_ts: datetime | None = None,
) -> EdgeCaseFixture:
    """Generate dollar-bar ticks whose ``price × quantity`` notional includes
    non-trivial fractional KRW that, if rounded mid-pipeline, would skew bar
    closure.

    Ticks use prices like ``101.7`` and quantities like ``0.013`` — exact
    Decimal multiplication is required to land precisely on ``threshold``.

    Args:
        seed: deterministic seed for price/qty selection.
        threshold: notional threshold for dollar bar (Decimal, > 0).
        bar_count: number of bars to generate.
        base_ts: starting UTC timestamp; defaults to ``2026-01-01T00:00:00Z``.

    Returns:
        :class:`EdgeCaseFixture` with ``category="krw_rounding_edge"``.
    """
    if threshold <= 0:
        raise ValueError(f"threshold ({threshold}) must be > 0")
    if bar_count <= 0:
        raise ValueError(f"bar_count ({bar_count}) must be > 0")

    rng = random.Random(seed)
    base = base_ts or _utc(2026, 1, 1)
    ticks: list[tuple[datetime, Decimal, Decimal]] = []
    ts = base
    # Each bar accumulates fractional notional across 4 ticks, then a closing
    # tick whose price × quantity fills the residual to threshold exactly.
    for _ in range(bar_count):
        accumulated = Decimal(0)
        partials = 4
        for _ in range(partials):
            # Generate a "weird" price like 101.7 and qty like 0.013 — non-round.
            price = Decimal(100) + Decimal(rng.randint(1, 9)) / Decimal(10)
            qty = Decimal(rng.randint(11, 19)) / Decimal(1000)
            notional = price * qty
            # ensure we don't overshoot too soon
            if accumulated + notional > threshold * Decimal("0.8"):
                notional = (threshold * Decimal("0.2")).quantize(Decimal("0.000001"))
                qty = (notional / price).quantize(Decimal("0.000001"))
            ticks.append((ts, price, qty))
            ts = ts + timedelta(milliseconds=100)
            accumulated = accumulated + (price * qty)
        # closing tick: residual = threshold - accumulated
        residual = threshold - accumulated
        if residual <= 0:
            # extremely unlikely with above safeguards, but defend with a tiny tick
            residual = Decimal("0.000001")
        close_price = Decimal(100)
        close_qty = (residual / close_price).quantize(Decimal("0.00000001"))
        # adjust so price * qty == residual exactly: re-derive price.
        close_price = (residual / close_qty)
        # quantize close_price to KRW-typical 1-won granularity if possible —
        # but DollarBarAggregator only needs price * qty >= threshold.
        ticks.append((ts, close_price, close_qty))
        ts = ts + timedelta(milliseconds=100)

    return EdgeCaseFixture(
        category="krw_rounding_edge",
        ticks=tuple(ticks),
        expected_bar_count=bar_count,
        seed=seed,
    )


__all__ = [
    "EdgeCaseFixture",
    "generate_krw_rounding_edge",
    "generate_threshold_boundary",
    "generate_time_bar_boundary",
]
