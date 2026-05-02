"""Storage path resolution + Hive partition derivation (Windows-safe)."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from mctrader_market.types import Symbol, Timeframe

from mctrader_data.schema import SCHEMA_VERSION


def resolve_data_root(*, root_override: Path | None = None) -> Path:
    """Resolve storage root.

    Priority: ``root_override`` (CLI ``--root``) > ``MCTRADER_DATA_ROOT`` env > repo-local
    ``data/parquet/``.
    """
    if root_override is not None:
        return root_override.resolve(strict=False)
    env_value = os.environ.get("MCTRADER_DATA_ROOT")
    if env_value:
        return Path(env_value).resolve(strict=False)
    return (Path.cwd() / "data" / "parquet").resolve(strict=False)


def derive_partition_path(
    *,
    root: Path,
    exchange: str,
    symbol: Symbol,
    timeframe: Timeframe,
    ts_utc: datetime,
) -> Path:
    """Build the ADR-009 D2 Hive partition directory.

    Layout:
        ``{root}/market/ohlcv/schema_version=ohlcv.v1/exchange={ex}/symbol={sym}/
        timeframe={tf}/year={Y}/month={M}/date={D}/``

    The ``date`` segment uses the UTC date (KST 1d boundary handled at aggregation).
    """
    return (
        root
        / "market"
        / "ohlcv"
        / f"schema_version={SCHEMA_VERSION}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe.value}"
        / f"year={ts_utc.year:04d}"
        / f"month={ts_utc.month:02d}"
        / f"date={ts_utc.day:02d}"
    )


def to_duckdb_glob(path: Path) -> str:
    """Convert a path to a forward-slash DuckDB-friendly string (Windows-safe)."""
    return path.as_posix()
