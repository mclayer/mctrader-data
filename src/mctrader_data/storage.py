"""Parquet/DuckDB write + read for ADR-009 v1 OHLCV schema."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_market.candle import CandleLike, CandleModel
from mctrader_market.types import Symbol, Timeframe

from mctrader_data.path import derive_partition_path, to_duckdb_glob
from mctrader_data.schema import SCHEMA_VERSION


def _candles_to_arrow(candles: Sequence[CandleLike]) -> pa.Table:
    """Build a PyArrow Table with ADR-009 v1 column order + types."""
    rows = {
        "ts_utc": [c.ts_utc for c in candles],
        "exchange": [c.exchange for c in candles],
        "symbol": [str(c.symbol) for c in candles],
        "timeframe": [c.timeframe.value for c in candles],
        "open": [str(c.open) for c in candles],
        "high": [str(c.high) for c in candles],
        "low": [str(c.low) for c in candles],
        "close": [str(c.close) for c in candles],
        "volume": [str(c.volume) for c in candles],
        "value": [str(c.value) if c.value is not None else None for c in candles],
        "schema_version": [SCHEMA_VERSION] * len(candles),
    }
    schema = pa.schema(
        [
            ("ts_utc", pa.timestamp("us", tz="UTC")),
            ("exchange", pa.string()),
            ("symbol", pa.string()),
            ("timeframe", pa.string()),
            ("open", pa.decimal128(38, 18)),
            ("high", pa.decimal128(38, 18)),
            ("low", pa.decimal128(38, 18)),
            ("close", pa.decimal128(38, 18)),
            ("volume", pa.decimal128(38, 18)),
            ("value", pa.decimal128(38, 18)),
            ("schema_version", pa.string()),
        ]
    )
    decimal_cols = ("open", "high", "low", "close", "volume", "value")
    arrays: list[pa.Array] = []
    for field in schema:
        col_data = rows[field.name]
        if field.name in decimal_cols:
            arrays.append(
                pa.array(
                    [Decimal(v) if v is not None else None for v in col_data],
                    type=field.type,
                )
            )
        elif field.name == "ts_utc":
            arrays.append(pa.array(col_data, type=field.type))
        else:
            arrays.append(pa.array(col_data, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def write_candles(
    candles: Sequence[CandleLike],
    *,
    root: Path,
    snapshot_id: str,
) -> Path:
    """Write a batch of candles as a single Parquet file under the canonical Hive partition.

    All candles MUST share ``(exchange, symbol, timeframe)``.
    The partition date is derived from the first candle's ``ts_utc.day``.
    Returns the partition directory path.
    """
    if not candles:
        raise ValueError("write_candles: empty candles batch")
    head = candles[0]
    partition = derive_partition_path(
        root=root,
        exchange=head.exchange,
        symbol=head.symbol,
        timeframe=head.timeframe,
        ts_utc=head.ts_utc,
    )
    partition.mkdir(parents=True, exist_ok=True)
    table = _candles_to_arrow(candles)
    target = partition / f"part-{snapshot_id}.parquet"
    pq.write_table(table, target, compression="snappy")
    return partition


def scan_candles(
    *,
    exchange: str,
    symbol: Symbol,
    timeframe: Timeframe,
    start: datetime,
    end: datetime,
    root: Path,
) -> Iterable[CandleModel]:
    """Read candles for ``[start, end)`` half-open interval.

    Returns an iterable of ``CandleModel`` (Pydantic v2 boundary).
    Sorted ASC by ``ts_utc``.
    """
    base_glob = to_duckdb_glob(
        root
        / "market"
        / "ohlcv"
        / f"schema_version={SCHEMA_VERSION}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe.value}"
        / "**"
        / "*.parquet"
    )
    con = duckdb.connect(":memory:", read_only=False)
    try:
        rel = con.sql(
            f"""
            SELECT ts_utc, exchange, symbol, timeframe,
                   open::VARCHAR AS open, high::VARCHAR AS high,
                   low::VARCHAR AS low, close::VARCHAR AS close,
                   volume::VARCHAR AS volume,
                   value::VARCHAR AS value
            FROM read_parquet('{base_glob}', hive_partitioning=true)
            WHERE ts_utc >= ? AND ts_utc < ?
            ORDER BY ts_utc ASC
            """,
            params=[start, end],
        )
        rows = rel.fetchall()
    finally:
        con.close()

    for row in rows:
        ts_utc, ex, sym_str, tf_str, o, h, lo, cl, vol, val = row
        yield CandleModel(
            ts_utc=cast(datetime, ts_utc),
            exchange=ex,
            symbol=Symbol.from_string(sym_str),
            timeframe=Timeframe(tf_str),
            open=Decimal(o),
            high=Decimal(h),
            low=Decimal(lo),
            close=Decimal(cl),
            volume=Decimal(vol),
            value=Decimal(val) if val is not None else None,
        )
