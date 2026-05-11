"""4 bar aggregation algorithms (ADR-025 §core — Hot/Cold SSOT).

Algorithms
----------
- :class:`TimeBarAggregator`     — fixed-interval ``[start, end)`` time bar.
- :class:`VolumeBarAggregator`   — cumulative base-volume threshold.
- :class:`TickBarAggregator`     — fixed trade-count threshold.
- :class:`DollarBarAggregator`   — cumulative notional (price × quantity) threshold.

Determinism guarantees (ADR-025 §determinism)
---------------------------------------------
- No random, no threading, no wall-clock reads.
- Per-symbol state machines isolated by ``symbol`` key.
- Tie-breaking SSOT: cumulative metric == threshold → close bar **on the
  triggering tick** (``tie_breaking="current_tick"``). This is the canonical
  rule referenced in :mod:`mctrader_data.aggregation.contract_metadata`.

Boundary contract
-----------------
Input  : :class:`mctrader_market.schemas.tick.TickRowV1_1` (Decimal price/qty).
Output : :class:`mctrader_market.protocols.information_bar.InformationBarModel`
         (Pydantic v2 frozen, Decimal38_18 columns, UTC datetimes).

Hot path callers should treat the returned bar as immutable; the emitter does
not retain references after :meth:`process_tick` returns.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mctrader_market.protocols.information_bar import InformationBarModel
from mctrader_market.schemas.tick import TickRowV1_1
from mctrader_market.types import Symbol


# ---------------------------------------------------------------------------
# Per-symbol running state
# ---------------------------------------------------------------------------
@dataclass
class _BarState:
    """Mutable per-symbol running OHLC + threshold accumulator.

    Internal only. Aggregators reset this whenever a bar closes.
    """

    genesis_ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal(0)
    # notional sum stored as Decimal here for legibility; aggregators that need
    # drift-free integer math use scaled ints separately (see _DollarState).
    value: Decimal = Decimal(0)
    last_ts: datetime = field(default_factory=lambda: datetime(1970, 1, 1, tzinfo=timezone.utc))
    tick_count: int = 0

    def update_ohlc(self, price: Decimal) -> None:
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price


# ---------------------------------------------------------------------------
# Base aggregator
# ---------------------------------------------------------------------------
class _BaseAggregator(ABC):
    """Common per-symbol state management + bar emission.

    Subclasses implement :meth:`_should_close` and :meth:`_bar_label`.
    """

    def __init__(self) -> None:
        self._states: dict[Symbol, _BarState] = {}

    @abstractmethod
    def _should_close(self, state: _BarState, tick: TickRowV1_1) -> bool:
        """Return True iff this tick (already merged into ``state``) closes a bar."""

    @abstractmethod
    def _bar_label(self) -> str:
        """Return ``bar_label`` string for emitted bars (e.g. ``"vol_1000"``)."""

    @abstractmethod
    def _threshold_decimal(self) -> Decimal:
        """Return threshold as Decimal for ``InformationBarModel.threshold``."""

    def _new_state(self, tick: TickRowV1_1) -> _BarState:
        return _BarState(
            genesis_ts=tick.ts_utc,
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=tick.quantity,
            value=tick.price * tick.quantity,
            last_ts=tick.ts_utc,
            tick_count=1,
        )

    def _merge(self, state: _BarState, tick: TickRowV1_1) -> None:
        state.update_ohlc(tick.price)
        state.volume = state.volume + tick.quantity
        state.value = state.value + tick.price * tick.quantity
        state.last_ts = tick.ts_utc
        state.tick_count += 1

    def _emit(self, state: _BarState, tick: TickRowV1_1, exchange: str, symbol: Symbol) -> InformationBarModel:
        # ADR-009 §D15 invariant: ts_close > genesis_ts (strict).
        # For single-tick threshold bars (overshoot / threshold=1) the triggering tick's
        # ts_utc == genesis_ts. Advance ts_close by 1 microsecond — the smallest
        # representable Python datetime delta — to satisfy the strict inequality without
        # introducing wall-clock-derived nondeterminism.
        ts_close = tick.ts_utc
        if ts_close <= state.genesis_ts:
            ts_close = state.genesis_ts + timedelta(microseconds=1)
        return InformationBarModel(
            bar_label=self._bar_label(),
            genesis_ts=state.genesis_ts,
            ts_close=ts_close,
            threshold=self._threshold_decimal(),
            exchange=exchange,
            symbol=symbol,
            open=state.open,
            high=state.high,
            low=state.low,
            close=state.close,
            volume=state.volume,
            value=state.value,
        )

    def process_tick(self, tick: TickRowV1_1) -> InformationBarModel | None:
        """Process one tick; emit a closed bar iff threshold reached.

        Returns ``None`` while the bar is still in progress.
        """
        # Subclasses with boundary semantics (time bar) may override this hook
        # to close based on the *upcoming* tick before merging it. The default
        # implementation merges first, then checks closure.
        return self._default_process(tick)

    def _default_process(self, tick: TickRowV1_1) -> InformationBarModel | None:
        symbol = tick.symbol
        state = self._states.get(symbol)
        if state is None:
            self._states[symbol] = self._new_state(tick)
            # Threshold-based bars may also close on first tick (overshoot).
            new_state = self._states[symbol]
            if self._should_close(new_state, tick):
                bar = self._emit(new_state, tick, tick.exchange, symbol)
                del self._states[symbol]
                return bar
            return None

        self._merge(state, tick)
        if self._should_close(state, tick):
            bar = self._emit(state, tick, tick.exchange, symbol)
            del self._states[symbol]
            return bar
        return None


# ---------------------------------------------------------------------------
# Time bar — [start, end) inclusion. Closure is *boundary-triggered*: a tick
# whose ts_utc falls in the next window flushes the previous bar before being
# merged. This requires overriding process_tick rather than relying on
# post-merge check.
# ---------------------------------------------------------------------------
class TimeBarAggregator(_BaseAggregator):
    """Fixed wall-clock interval bar.

    ``timeframe=timedelta(minutes=1)`` → ``bar_label="time_60"``.
    Window semantics: ``[genesis_ts, genesis_ts + timeframe)`` — a tick at
    exactly the right edge starts a new bar.
    """

    def __init__(self, timeframe: timedelta) -> None:
        super().__init__()
        if timeframe.total_seconds() <= 0:
            raise ValueError(f"timeframe must be > 0, got {timeframe!r}")
        self._timeframe = timeframe
        # cache integer-second representation for label + threshold
        seconds_decimal = Decimal(int(timeframe.total_seconds()))
        if seconds_decimal != Decimal(str(timeframe.total_seconds())):
            raise ValueError(f"timeframe must be a whole number of seconds, got {timeframe!r}")
        self._seconds = int(seconds_decimal)

    def _bar_label(self) -> str:
        return f"time_{self._seconds}"

    def _threshold_decimal(self) -> Decimal:
        return Decimal(self._seconds)

    def _window_start(self, ts: datetime) -> datetime:
        """Align ``ts`` down to the most recent timeframe boundary from epoch."""
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = ts - epoch
        # integer floor division on microseconds for determinism
        total_us = delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
        tf = self._timeframe
        tf_us = tf.days * 86_400_000_000 + tf.seconds * 1_000_000 + tf.microseconds
        floored_us = (total_us // tf_us) * tf_us
        return epoch + timedelta(microseconds=floored_us)

    def _should_close(self, state: _BarState, tick: TickRowV1_1) -> bool:
        # Time bars do not use the post-merge hook; closure decided in process_tick.
        return False

    def process_tick(self, tick: TickRowV1_1) -> InformationBarModel | None:
        symbol = tick.symbol
        window_start = self._window_start(tick.ts_utc)
        window_end = window_start + self._timeframe
        state = self._states.get(symbol)

        if state is None:
            # First tick → seed a state aligned to its window.
            new_state = self._new_state(tick)
            new_state.genesis_ts = window_start
            self._states[symbol] = new_state
            return None

        if tick.ts_utc < state.genesis_ts + self._timeframe:
            # Same window — merge and continue.
            self._merge(state, tick)
            return None

        # Tick belongs to a later window → flush previous bar at its window end,
        # then start a fresh state from this tick.
        ts_close = state.genesis_ts + self._timeframe
        bar = InformationBarModel(
            bar_label=self._bar_label(),
            genesis_ts=state.genesis_ts,
            ts_close=ts_close,
            threshold=self._threshold_decimal(),
            exchange=tick.exchange,
            symbol=symbol,
            open=state.open,
            high=state.high,
            low=state.low,
            close=state.close,
            volume=state.volume,
            value=state.value,
        )
        # Seed the next bar from this tick.
        next_state = self._new_state(tick)
        next_state.genesis_ts = window_start
        self._states[symbol] = next_state
        # Unused parameter
        _ = window_end
        return bar


# ---------------------------------------------------------------------------
# Volume bar — cumulative base-volume threshold (current_tick tie-breaking).
# ---------------------------------------------------------------------------
class VolumeBarAggregator(_BaseAggregator):
    """Cumulative quantity threshold bar.

    Closes (and emits) on the tick whose merge causes ``state.volume >=
    threshold``. The triggering tick is included in the closed bar.
    """

    def __init__(self, threshold: Decimal) -> None:
        super().__init__()
        if threshold <= 0:
            raise ValueError(f"threshold ({threshold}) must be > 0")
        self._threshold = threshold

    def _bar_label(self) -> str:
        return f"vol_{self._threshold}"

    def _threshold_decimal(self) -> Decimal:
        return self._threshold

    def _should_close(self, state: _BarState, tick: TickRowV1_1) -> bool:
        # Unused
        _ = tick
        return state.volume >= self._threshold


# ---------------------------------------------------------------------------
# Tick bar — fixed trade count threshold.
# ---------------------------------------------------------------------------
class TickBarAggregator(_BaseAggregator):
    """Tick-count threshold bar."""

    def __init__(self, threshold: int) -> None:
        super().__init__()
        if threshold <= 0:
            raise ValueError(f"threshold ({threshold}) must be > 0")
        self._threshold = threshold

    def _bar_label(self) -> str:
        return f"tick_{self._threshold}"

    def _threshold_decimal(self) -> Decimal:
        return Decimal(self._threshold)

    def _should_close(self, state: _BarState, tick: TickRowV1_1) -> bool:
        _ = tick
        return state.tick_count >= self._threshold


# ---------------------------------------------------------------------------
# Dollar bar — cumulative notional (price × quantity) threshold.
#
# Drift-free integer arithmetic: the accumulator runs on Decimal because
# ``InformationBarModel.value`` is Decimal38_18 at the boundary; for KRW use
# the underlying Decimal multiplication is exact (no division). Scaled-int
# helpers in :mod:`scaled_int` are exposed for callers that need to compare
# notional sums across Hot/Cold reconciliation.
# ---------------------------------------------------------------------------
class DollarBarAggregator(_BaseAggregator):
    """Cumulative notional (KRW) threshold bar."""

    def __init__(self, threshold: Decimal) -> None:
        super().__init__()
        if threshold <= 0:
            raise ValueError(f"threshold ({threshold}) must be > 0")
        self._threshold = threshold

    def _bar_label(self) -> str:
        return f"dollar_{self._threshold}"

    def _threshold_decimal(self) -> Decimal:
        return self._threshold

    def _should_close(self, state: _BarState, tick: TickRowV1_1) -> bool:
        _ = tick
        return state.value >= self._threshold


__all__ = [
    "DollarBarAggregator",
    "TickBarAggregator",
    "TimeBarAggregator",
    "VolumeBarAggregator",
]
