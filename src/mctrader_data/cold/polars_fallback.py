"""Polars lazy DAG fallback resampler (Epic MCT-112 Story-5).

Same public surface as :class:`mctrader_data.cold.duckdb_resample.DuckDBResampler`
but backed by ``polars.scan_parquet`` lazy DAG. Used by callers who prefer the
Polars dataframe pipeline (e.g. research notebooks chaining additional ``with_columns``
operations) or who hit DuckDB resource limits on extreme universes.

Determinism contract
--------------------
Identical to the DuckDB path — both implementations route every tick through
:mod:`mctrader_data.aggregation.core` aggregators. Polars is only the scan +
sort + predicate pushdown engine; the algorithm of record is the Story-3 core.
This guarantees DuckDB ↔ Polars equivalence (cross-engine SSOT).

Polars dependency
-----------------
Polars is **not** declared in :file:`pyproject.toml` core dependencies — the
fallback is optional and import-guarded. Tests that require Polars install it
in the test environment. The :class:`PolarsResampler` constructor raises
:class:`ImportError` with a clear message if Polars is not available.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from mctrader_market.candle import CandleModel
from mctrader_market.protocols.information_bar import InformationBarModel
from mctrader_market.schemas.tick import TickRowV1_1
from mctrader_market.types import Symbol

from mctrader_market.aggregation import TimeBarAggregator  # market SSOT (MCT-182 fix1, ADR-031 §D1 INV-4)
from mctrader_data.cold.duckdb_resample import (
    _LEGACY_TRADE_ID_TEMPLATE,
    _SECONDS_BY_TIMEFRAME,
    _build_aggregator,
    _coerce_closed_timeframe_seconds,
    _seconds_to_closed_timeframe,
    _validate_window,
    parse_bar_label,
)
from mctrader_data.tick_storage import TICK_SCHEMA_VERSION


class PolarsResampler:
    """Polars lazy-frame resampler — mirror of :class:`DuckDBResampler`.

    Parameters
    ----------
    root:
        Filesystem root containing ``market/transaction/schema_version=tick.v1/``.

    Notes
    -----
    Reuses the same Story-3 aggregators as the DuckDB path so cross-engine
    consistency is guaranteed by construction. The only divergence is the
    scan / sort / pruning layer.
    """

    def __init__(self, root: Path) -> None:
        try:
            import polars  # noqa: F401  # type: ignore[import-untyped]
        except ImportError as err:
            raise ImportError(
                "PolarsResampler requires polars. Install with `pip install polars` "
                "or use DuckDBResampler instead."
            ) from err
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
        """Resample transaction Parquet → time-bucketed OHLCV candles (Polars)."""
        _validate_window(start, end)
        seconds = _coerce_closed_timeframe_seconds(timeframe)
        tf_enum = _seconds_to_closed_timeframe(seconds)
        aggregator = TimeBarAggregator(timeframe=timedelta(seconds=seconds))

        symbol_obj = Symbol.from_string(symbol)
        for tick in self._iter_ticks(symbol, start, end, exchange=exchange):
            bar = aggregator.process_tick(tick)
            if bar is not None:
                yield CandleModel(  # type: ignore[arg-type]
                    ts_utc=bar.genesis_ts,
                    exchange=bar.exchange,
                    symbol=symbol_obj,
                    timeframe=tf_enum,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    value=bar.value,
                    source="transaction_derived",
                )

    def resample_information_bar(
        self,
        symbol: str,
        bar_label: str,
        start: datetime,
        end: datetime,
        *,
        exchange: str | None = None,
    ) -> Iterator[InformationBarModel]:
        """Resample transaction Parquet → information bars (Polars + Story-3 core)."""
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
        import polars as pl  # type: ignore[import-untyped]

        root_dir = self._tick_root()
        if not root_dir.exists():
            return

        # Polars Hive partition autodiscovery — same predicates as DuckDB path.
        scan = pl.scan_parquet(
            (root_dir.as_posix() + "/**/*.parquet"),
            hive_partitioning=True,
        )
        # See DuckDBResampler docstring on the ``date`` partition: it is the WAL
        # ingest wall-clock, NOT the tick's ts_utc.date(). For replayed /
        # historical fixtures they diverge, so we drop the ``date`` predicate
        # and rely solely on the row-level ts_utc filter (Polars + Parquet
        # statistics deliver file-level skipping automatically).
        scan = scan.filter(
            (pl.col("symbol") == symbol)
            & (pl.col("ts_utc") >= start)
            & (pl.col("ts_utc") < end)
        )
        if exchange is not None:
            scan = scan.filter(pl.col("exchange") == exchange)
        scan = scan.select(["ts_utc", "exchange", "symbol", "price", "quantity", "side"])
        scan = scan.sort("ts_utc")

        # ``collect`` materialises into a DataFrame; ``iter_rows`` streams Python
        # tuples (no copy of internal columns). Acceptable for Cold path queries
        # which already constrain to a small symbol/date window.
        df = scan.collect()
        for seq, row in enumerate(df.iter_rows()):
            yield _row_to_tick(row, seq)

    def _tick_root(self) -> Path:
        return (
            self._root
            / "market"
            / "transaction"
            / f"schema_version={TICK_SCHEMA_VERSION}"
            / "tier=L1"
        )


def _row_to_tick(row: tuple[Any, ...], seq: int) -> TickRowV1_1:
    ts_utc, exchange, symbol_str, price, quantity, side_raw = row
    if isinstance(ts_utc, datetime):
        ts_utc = (
            ts_utc.replace(tzinfo=timezone.utc)
            if ts_utc.tzinfo is None
            else ts_utc.astimezone(timezone.utc)
        )
    side_norm: Literal["BUY", "SELL"] = "BUY" if str(side_raw).upper() == "BUY" else "SELL"
    trade_id = _LEGACY_TRADE_ID_TEMPLATE.format(
        exchange=exchange,
        symbol=symbol_str,
        ts=ts_utc.isoformat() if isinstance(ts_utc, datetime) else str(ts_utc),
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


# Expose seconds map for downstream tests / callers.
SECONDS_BY_TIMEFRAME = _SECONDS_BY_TIMEFRAME


__all__ = ["PolarsResampler", "SECONDS_BY_TIMEFRAME"]
