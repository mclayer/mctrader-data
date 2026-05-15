"""Cold path DuckDB resample over transaction Parquet (Epic MCT-112 Story-5).

Reads tick Parquet files persisted by :class:`mctrader_data.compactor.l1.L1Compactor`
at the canonical Hive layout::

    <root>/market/transaction/schema_version=tick.v1/tier=L1/
      exchange=<exchange>/symbol=<symbol>/date=<YYYY-MM-DD>/
      node=<node_id>/part-<run_id>.parquet

DuckDB's ``hive_partitioning=true`` flag on :func:`read_parquet` pushes
``exchange`` / ``symbol`` / ``date`` predicates down to the partition pruner,
limiting Parquet scans to the matching date range.

Determinism contract
--------------------
- :meth:`DuckDBResampler.resample_time` — Time bar with arbitrary integer
  seconds. SQL ``time_bucket`` provides the bucketing, but to guarantee bit-for-bit
  Hot/Cold equality we route each tick through
  :class:`mctrader_data.aggregation.core.TimeBarAggregator` post-fetch — DuckDB
  is the partition pruner + sort, the algorithm of record stays in Story-3.
- :meth:`DuckDBResampler.resample_information_bar` — Volume / Tick / Dollar
  threshold bars. Story-3 aggregators are the SSOT; DuckDB streams sorted ticks
  in and the aggregator emits :class:`InformationBarModel` rows.

This guarantees Hot path (asyncio engine consuming live ticks) and Cold path
(DuckDB over historical Parquet) produce **byte-identical bars** for the same
input universe — the explicit goal of Hot/Cold consistency SLO (Story-11
reconciliation harness).

Source field
------------
All emitted :class:`CandleModel` rows carry ``source="transaction_derived"``
per ADR-009 §D8 — distinguishing Cold-derived OHLCV bars from legacy candle
emitters (exchange OHLCV REST/WS).
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from mctrader_market.candle import CandleModel
from mctrader_market.protocols.information_bar import InformationBarModel
from mctrader_market.schemas.tick import TickRowV1_1
from mctrader_market.types import Symbol, Timeframe

from mctrader_market.aggregation import (  # noqa: E402 — market SSOT (MCT-182 fix1, ADR-031 §D1 INV-4)
    DollarBarAggregator,
    TickBarAggregator,
    TimeBarAggregator,
    VolumeBarAggregator,
)
from mctrader_data.tick_storage import TICK_SCHEMA_VERSION

BAR_LABEL_PREFIXES: tuple[str, ...] = ("time_", "vol_", "tick_", "dollar_")
"""Allowed ``bar_label`` discriminators (mirrors ADR-009 §D15)."""

_SECONDS_BY_TIMEFRAME: dict[Timeframe, int] = {
    Timeframe.M1: 60,
    Timeframe.M5: 300,
    Timeframe.M15: 900,
    Timeframe.H1: 3600,
    Timeframe.H4: 14_400,
    Timeframe.D1: 86_400,
}
"""Closed Timeframe → integer seconds map (matches enum.delta but pre-computed)."""

# Pattern used to derive a deterministic synthetic trade_id when reading legacy
# tick.v1 Parquet (which lacks the v1.1 trade_id column). See module docstring
# for full rationale + the open question forwarded to ArchitectPL.
_LEGACY_TRADE_ID_TEMPLATE = "{exchange}:{symbol}:{ts}:{seq}"


@dataclass(frozen=True)
class _BarLabelSpec:
    """Parsed ``bar_label`` discriminator: kind + numeric threshold."""

    kind: Literal["time", "vol", "tick", "dollar"]
    threshold: Decimal

    @property
    def label(self) -> str:
        return f"{self.kind}_{self.threshold}"


_BAR_LABEL_RE = re.compile(r"^(time|vol|tick|dollar)_(.+)$")


def parse_bar_label(bar_label: str) -> _BarLabelSpec:
    """Parse ``bar_label`` (e.g. ``"vol_1000"``) into kind + threshold.

    Raises:
        ValueError: if the label does not match an allowed prefix or the suffix
            cannot be coerced to :class:`Decimal`.
    """
    m = _BAR_LABEL_RE.match(bar_label)
    if m is None:
        raise ValueError(
            f"bar_label must match one of {BAR_LABEL_PREFIXES} prefixes, got {bar_label!r}"
        )
    kind = m.group(1)
    suffix = m.group(2)
    try:
        threshold = Decimal(suffix)
    except Exception as err:
        raise ValueError(
            f"bar_label suffix {suffix!r} must be a Decimal-coercible numeric, got {bar_label!r}"
        ) from err
    if threshold <= 0:
        raise ValueError(f"bar_label threshold ({threshold}) must be > 0, got {bar_label!r}")
    return _BarLabelSpec(kind=kind, threshold=threshold)  # type: ignore[arg-type]


class DuckDBResampler:
    """Cold path resampler — DuckDB read_parquet + Story-3 aggregators.

    Parameters
    ----------
    root:
        Filesystem root containing ``market/transaction/schema_version=tick.v1/``
        (the L1 Compactor output root).

    Usage
    -----
    >>> resampler = DuckDBResampler(root=Path("/data"))
    >>> for candle in resampler.resample_time("KRW-BTC", "1m", start, end):
    ...     emit(candle)
    >>> for bar in resampler.resample_information_bar("KRW-BTC", "vol_1000", start, end):
    ...     emit(bar)
    """

    def __init__(self, root: Path) -> None:
        self._root = Path(root)

    # ------------------------------------------------------------- public API

    def resample_time(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        *,
        exchange: str | None = None,
    ) -> Iterator[CandleModel]:
        """Resample transaction Parquet → time-bucketed OHLCV candles.

        Args:
            symbol: canonical ``"{quote}-{base}"`` symbol string.
            timeframe: either a closed :class:`Timeframe` value (``"1m"`` /
                ``"5m"`` / ``"15m"`` / ``"1h"`` / ``"4h"`` / ``"1d"``) or an
                arbitrary integer-seconds string (e.g. ``"47s"`` / ``"13s"`` /
                ``"600s"``).
            start, end: half-open UTC window ``[start, end)``.
            exchange: optional exchange predicate; when omitted all exchanges
                are scanned.

        Yields:
            :class:`CandleModel` with ``source="transaction_derived"`` for
            closed Timeframe inputs. For arbitrary seconds inputs (outside the
            closed enum), use :meth:`resample_information_bar` with
            ``bar_label="time_<seconds>"`` instead.

        Raises:
            ValueError: if ``timeframe`` is not a closed Timeframe value. Use
                ``resample_information_bar(bar_label="time_<seconds>", ...)``
                for arbitrary-seconds buckets.
        """
        _validate_window(start, end)
        seconds = _coerce_closed_timeframe_seconds(timeframe)
        tf_enum = _seconds_to_closed_timeframe(seconds)
        aggregator = TimeBarAggregator(timeframe=timedelta(seconds=seconds))

        symbol_obj = Symbol.from_string(symbol)
        for tick in self._iter_ticks(symbol, start, end, exchange=exchange):
            bar = aggregator.process_tick(tick)
            if bar is not None:
                yield self._to_candle(bar, symbol_obj, tf_enum)

        # The TimeBarAggregator emits on boundary-cross; the *last* in-flight
        # window only closes when a tick from the *next* window arrives. For
        # half-open ``[start, end)`` queries that means the final window is
        # held in state and never emitted unless we inject a synthetic tick
        # at ``end``. Cold path semantics: only fully-closed bars are returned
        # (matches the engine's "is_complete" contract for live consumers).

    def resample_information_bar(
        self,
        symbol: str,
        bar_label: str,
        start: datetime,
        end: datetime,
        *,
        exchange: str | None = None,
    ) -> Iterator[InformationBarModel]:
        """Resample transaction Parquet → information bars via Story-3 core.

        Supports any ADR-009 §D15 discriminator: ``time_<seconds>``,
        ``vol_<threshold>``, ``tick_<count>``, ``dollar_<value>``.

        Args:
            symbol: canonical ``"{quote}-{base}"`` symbol string.
            bar_label: ADR-009 §D15 discriminator (see
                :data:`BAR_LABEL_PREFIXES`).
            start, end: half-open UTC window ``[start, end)``.
            exchange: optional exchange predicate.

        Yields:
            :class:`InformationBarModel` instances emitted by the canonical
            Story-3 aggregator for the requested bar kind. Determinism is
            inherited from the aggregator (no random / no wall-clock / no
            threading).
        """
        _validate_window(start, end)
        spec = parse_bar_label(bar_label)
        aggregator = _build_aggregator(spec)

        for tick in self._iter_ticks(symbol, start, end, exchange=exchange):
            bar = aggregator.process_tick(tick)
            if bar is not None:
                yield bar

    # -------------------------------------------------------------- internals

    def _iter_ticks(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        exchange: str | None,
    ) -> Iterator[TickRowV1_1]:
        """Stream sorted ticks from Parquet using DuckDB partition pruning.

        Predicate pushdown via ``hive_partitioning=true``:
            * ``symbol = ?``           (Hive partition column → directory skip)
            * ``exchange = ?``         (Hive partition column, optional)
        Row-level filter (with Parquet footer statistics skipping at file level):
            * ``ts_utc >= ? AND ts_utc < ?``

        The Hive ``date`` partition is intentionally not used here — see
        :func:`_build_select_sql` for the full rationale.
        """
        if not self._has_parquet_root():
            return
        with self._open_connection() as conn:
            glob_pattern = self._parquet_glob()
            sql = _build_select_sql(
                glob_pattern=glob_pattern,
                exchange_filter=exchange is not None,
            )
            params: list[Any] = [
                symbol,
                start,
                end,
            ]
            if exchange is not None:
                params.append(exchange)
            cursor = conn.execute(sql, params)
            seq = 0
            while True:
                rows = cursor.fetchmany(2048)
                if not rows:
                    break
                for row in rows:
                    yield self._row_to_tick(row, seq)
                    seq += 1

    @contextlib.contextmanager
    def _open_connection(self) -> Iterator[Any]:
        import duckdb

        conn = duckdb.connect(database=":memory:")
        # ADR-009 boundary requires UTC datetimes — pin DuckDB session TZ so
        # ``TIMESTAMP WITH TIME ZONE`` columns are returned with tzinfo=UTC
        # regardless of the host machine's locale. Without this DuckDB returns
        # the host's local TZ (e.g. Asia/Seoul on KR machines), which is
        # rejected by :class:`mctrader_market.types.UTCDateTime`.
        try:
            conn.execute("SET TimeZone='UTC'")
            yield conn
        finally:
            conn.close()

    def _has_parquet_root(self) -> bool:
        root_dir = self._tick_root()
        return root_dir.exists()

    def _tick_root(self) -> Path:
        return (
            self._root
            / "market"
            / "transaction"
            / f"schema_version={TICK_SCHEMA_VERSION}"
            / "tier=L1"
        )

    def _parquet_glob(self) -> str:
        # DuckDB accepts forward-slash globs on all platforms. Cover both with
        # and without the ``node=`` Hive level (HA collectors add it; non-HA
        # writers may omit it depending on configuration).
        return (self._tick_root().as_posix() + "/**/*.parquet")

    def _row_to_tick(self, row: tuple[Any, ...], seq: int) -> TickRowV1_1:
        ts_utc, exchange, symbol_str, price, quantity, side_raw = row
        # Normalise to UTC. DuckDB session is pinned to UTC in
        # :meth:`_open_connection`; this branch is defence-in-depth against
        # future DuckDB driver changes / external connection injection.
        if isinstance(ts_utc, datetime):
            ts_utc = (
                ts_utc.replace(tzinfo=timezone.utc)
                if ts_utc.tzinfo is None
                else ts_utc.astimezone(timezone.utc)
            )
        # tick.v1 storage uses lowercase "buy"/"sell"; aggregation core accepts
        # uppercase "BUY"/"SELL" per tick.v1.1. Normalise here.
        side_norm: Literal["BUY", "SELL"] = "BUY" if str(side_raw).upper() == "BUY" else "SELL"
        trade_id = _LEGACY_TRADE_ID_TEMPLATE.format(
            exchange=exchange,
            symbol=symbol_str,
            ts=ts_utc.isoformat(),
            seq=seq,
        )
        return TickRowV1_1(  # type: ignore[arg-type]
            ts_utc=ts_utc,
            exchange=str(exchange),
            symbol=Symbol.from_string(str(symbol_str)),
            trade_id=trade_id,
            price=Decimal(price) if not isinstance(price, Decimal) else price,
            quantity=Decimal(quantity) if not isinstance(quantity, Decimal) else quantity,
            side=side_norm,
            is_taker=True,
            ingest_seq=None,
            payload_hash=None,
            validation_status="OK",
        )

    def _to_candle(
        self,
        bar: InformationBarModel,
        symbol: Symbol,
        timeframe: Timeframe,
    ) -> CandleModel:
        """Convert an information bar (``time_<seconds>``) → :class:`CandleModel`.

        Sets ``source="transaction_derived"`` per ADR-009 §D8 — the load-bearing
        provenance field for downstream reconciliation.
        """
        return CandleModel(  # type: ignore[arg-type]
            ts_utc=bar.genesis_ts,
            exchange=bar.exchange,
            symbol=symbol,
            timeframe=timeframe,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            value=bar.value,
            source="transaction_derived",
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_window(start: datetime, end: datetime) -> None:
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start / end must be timezone-aware UTC datetimes")
    if start.utcoffset() != timezone.utc.utcoffset(start):
        raise ValueError(f"start must be UTC, got offset {start.utcoffset()}")
    if end.utcoffset() != timezone.utc.utcoffset(end):
        raise ValueError(f"end must be UTC, got offset {end.utcoffset()}")
    if end <= start:
        raise ValueError(f"end ({end.isoformat()}) must be > start ({start.isoformat()})")


def _coerce_closed_timeframe_seconds(timeframe: str) -> int:
    """Map a closed Timeframe string to its integer seconds.

    Rejects arbitrary-seconds strings (e.g. ``"47s"``) — the caller must use
    :meth:`DuckDBResampler.resample_information_bar` with
    ``bar_label="time_47"`` for those cases.
    """
    try:
        tf_enum = Timeframe(timeframe)
    except ValueError as err:
        raise ValueError(
            f"timeframe {timeframe!r} is not a closed Timeframe value "
            f"({[t.value for t in Timeframe]}). For arbitrary-seconds buckets, "
            f"use resample_information_bar(bar_label='time_<seconds>', ...)."
        ) from err
    return _SECONDS_BY_TIMEFRAME[tf_enum]


def _seconds_to_closed_timeframe(seconds: int) -> Timeframe:
    for tf, sec in _SECONDS_BY_TIMEFRAME.items():
        if sec == seconds:
            return tf
    raise ValueError(f"no closed Timeframe matches {seconds} seconds")


def _build_aggregator(spec: _BarLabelSpec):
    """Story-3 aggregator factory keyed by ``bar_label`` kind."""
    if spec.kind == "time":
        seconds = int(spec.threshold)
        if Decimal(seconds) != spec.threshold:
            raise ValueError(
                f"time bar threshold must be a whole number of seconds, got {spec.threshold}"
            )
        return TimeBarAggregator(timeframe=timedelta(seconds=seconds))
    if spec.kind == "vol":
        return VolumeBarAggregator(threshold=spec.threshold)
    if spec.kind == "tick":
        count = int(spec.threshold)
        if Decimal(count) != spec.threshold:
            raise ValueError(
                f"tick bar threshold must be a whole integer, got {spec.threshold}"
            )
        return TickBarAggregator(threshold=count)
    if spec.kind == "dollar":
        return DollarBarAggregator(threshold=spec.threshold)
    raise ValueError(f"unknown bar kind {spec.kind!r}")


def _build_select_sql(glob_pattern: str, *, exchange_filter: bool) -> str:
    """Build the SELECT statement targeting the L1 transaction Parquet glob.

    Predicates
    ----------
    - ``symbol = ?`` — Hive partition column, prunes whole symbol directories.
    - ``ts_utc >= ? AND ts_utc < ?`` — row-level filter on the column value.
      DuckDB statistics-based skipping prunes files whose [min, max] ts_utc
      bounds do not intersect the window.
    - ``exchange = ?`` — Hive partition column, optional.

    Note on the Hive ``date`` partition
    -----------------------------------
    L1 compactor writes ``date=`` from the **WAL ingest wall-clock**, not from
    the tick's ``ts_utc.date()``. For replayed / historical fixtures these can
    diverge, so we do **not** push a ``date`` predicate — that would prune
    correct data away. The ``ts_utc`` row-level filter is the canonical
    correctness boundary; ``date`` partitioning still helps in production where
    ingest time and data time agree because the file footer statistics on
    ``ts_utc`` already enable file-level skipping.
    """
    base = (
        "SELECT ts_utc, exchange, symbol, price, quantity, side "
        f"FROM read_parquet('{glob_pattern}', hive_partitioning=true) "
        "WHERE symbol = ? "
        "AND ts_utc >= ? AND ts_utc < ? "
    )
    if exchange_filter:
        base += "AND exchange = ? "
    base += "ORDER BY ts_utc"
    return base


__all__ = [
    "BAR_LABEL_PREFIXES",
    "DuckDBResampler",
    "parse_bar_label",
]
