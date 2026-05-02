"""ADR-009 OHLCV v1 16-column canonical schema."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from mctrader_market.types import Decimal38_18, Symbol, Timeframe, UTCDateTime

SCHEMA_VERSION = "ohlcv.v1"

OHLCV_COLUMNS: tuple[str, ...] = (
    "ts_utc",
    "exchange",
    "symbol",
    "timeframe",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",
    "trade_count",
    "is_complete",
    "schema_version",
    "source_ingested_at",
    "data_snapshot_id",
    "data_hash",
)
"""ADR-009 v1 16-column canonical order (also Hive partition base path)."""


class OhlcvRow(BaseModel):
    """ADR-009 v1 single OHLCV row (Pydantic v2 boundary).

    Adapter responsibility: ts_utc, exchange, symbol, timeframe, open/high/low/close, volume, value
    Storage responsibility: trade_count, is_complete, schema_version, source_ingested_at,
                            data_snapshot_id, data_hash
    """

    model_config = ConfigDict(strict=True, frozen=True, arbitrary_types_allowed=True)

    ts_utc: UTCDateTime
    exchange: str
    symbol: Symbol
    timeframe: Timeframe
    open: Decimal38_18
    high: Decimal38_18
    low: Decimal38_18
    close: Decimal38_18
    volume: Decimal38_18
    value: Decimal38_18 | None = None
    trade_count: int | None = None
    is_complete: bool = True
    schema_version: str = SCHEMA_VERSION
    source_ingested_at: UTCDateTime | None = None
    data_snapshot_id: str | None = None
    data_hash: str | None = None
