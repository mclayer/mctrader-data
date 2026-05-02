"""mctrader-data — OHLCV storage (Parquet/DuckDB) + ADR-009 v1 schema."""

from mctrader_data.path import resolve_data_root, derive_partition_path
from mctrader_data.policy import (
    PartialFailurePolicy,
    QuarantineReason,
    PolicyDecision,
)
from mctrader_data.schema import OhlcvRow, SCHEMA_VERSION, OHLCV_COLUMNS
from mctrader_data.storage import scan_candles, write_candles

__version__ = "0.1.0"

__all__ = [
    "OHLCV_COLUMNS",
    "OhlcvRow",
    "PartialFailurePolicy",
    "PolicyDecision",
    "QuarantineReason",
    "SCHEMA_VERSION",
    "__version__",
    "derive_partition_path",
    "resolve_data_root",
    "scan_candles",
    "write_candles",
]
