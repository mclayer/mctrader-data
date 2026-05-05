"""Storage path resolution + Hive partition derivation (Windows-safe)."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Literal

from mctrader_market.types import Symbol, Timeframe

from mctrader_data.schema import SCHEMA_VERSION

Mode = Literal["historical", "paper"]


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
    mode: Mode | None = None,
    node_id: str | None = None,
) -> Path:
    """Build the ADR-009 D2 Hive partition directory.

    Default (``mode=None``, ``node_id=None``) keeps the 0.1.0 layout for backward
    compatibility:
    ``{root}/market/ohlcv/schema_version=ohlcv.v1/exchange/symbol/timeframe/year/month/date``

    With ``mode`` set (``"historical"`` or ``"paper"``), inserts ``mode={mode}`` between
    ``schema_version`` and ``exchange`` per MCT-20 design (no-mode partitions remain
    historical legacy on read).

    With ``node_id`` set (per MCT-91 / ADR-009 §D2.1), appends ``node={node_id}`` as the
    leaf-most Hive level for active-active HA partition split (write contention 0). When
    ``node_id`` is ``None``, no ``node=`` level is added — pre-HA legacy behavior. Mixed
    legacy partition layout 영구 지원: read-side X3 가 legacy partition 을 ``node=DEFAULT``
    로 mapping.
    """
    base = root / "market" / "ohlcv" / f"schema_version={SCHEMA_VERSION}"
    if mode is not None:
        base = base / f"mode={mode}"
    leaf = (
        base
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe.value}"
        / f"year={ts_utc.year:04d}"
        / f"month={ts_utc.month:02d}"
        / f"date={ts_utc.day:02d}"
    )
    if node_id is not None:
        leaf = leaf / f"node={node_id}"
    return leaf


def to_duckdb_glob(path: Path) -> str:
    """Convert a path to a forward-slash DuckDB-friendly string (Windows-safe)."""
    return path.as_posix()
