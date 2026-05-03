"""mctrader-data — OHLCV storage (Parquet/DuckDB) + ADR-009 v1 schema + Paper + RiskPolicy snapshot."""

from mctrader_data.path import (
    Mode,
    derive_partition_path,
    resolve_data_root,
    to_duckdb_glob,
)
from mctrader_data.paper_lineage import PaperLineage, canonical_jsonl_hash
from mctrader_data.paper_storage import write_paper_candles
from mctrader_data.policy import (
    PartialFailurePolicy,
    PolicyDecision,
    QuarantineReason,
)
from mctrader_data.risk_snapshot import read_risk_policy_snapshot, write_risk_policy_snapshot
from mctrader_data.schema import OHLCV_COLUMNS, SCHEMA_VERSION, OhlcvRow
from mctrader_data.storage import ScanMode, scan_candles, write_candles

__version__ = "0.3.0"

__all__ = [
    "Mode",
    "OHLCV_COLUMNS",
    "OhlcvRow",
    "PaperLineage",
    "PartialFailurePolicy",
    "PolicyDecision",
    "QuarantineReason",
    "SCHEMA_VERSION",
    "ScanMode",
    "__version__",
    "canonical_jsonl_hash",
    "derive_partition_path",
    "read_risk_policy_snapshot",
    "resolve_data_root",
    "scan_candles",
    "to_duckdb_glob",
    "write_candles",
    "write_paper_candles",
    "write_risk_policy_snapshot",
]
