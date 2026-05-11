"""Cold path — DuckDB SQL over Parquet + Polars lazy fallback (Epic MCT-112 Story-5).

Public API
----------
``DuckDBResampler``  — DuckDB connection wrapper with Hive partition pruning over
``market/transaction/schema_version=tick.v1/tier=L1/`` Parquet files.

``PolarsResampler``  — Polars lazy DAG fallback. Both resamplers import the same
:mod:`mctrader_data.aggregation` core algorithms so Hot path (engine) and Cold
path (DuckDB / Polars over Parquet) produce byte-identical bars.

Source provenance
-----------------
All emitted :class:`mctrader_market.candle.CandleModel` instances carry
``source = "transaction_derived"`` per ADR-009 §D8. Information bars carry the
``info_bar.v1`` contract metadata version per ADR-009 §D15.

Story scope
-----------
- Cold path resample API for backtest / research / reconciliation harness.
- Hot/Cold consistency SSOT — both paths reuse Story-3 (MCT-137) algorithms.
- Story-11 reconciliation harness consumes the Cold path output as ground truth
  reference for Hot path drift detection.

Notes on tick.v1 schema (open question forwarded to ArchitectPL)
----------------------------------------------------------------
Storage today persists ``tick.v1`` (8 columns: ts_utc, received_at, exchange,
symbol, price, quantity, side, raw_json). The aggregation core consumes
``tick.v1.1`` (11 columns including trade_id / is_taker / ingest_seq / etc.).
The Cold path adapter synthesises a deterministic ``trade_id`` from
``(ts_utc, exchange, symbol, side, price, sequence_in_partition)`` and defaults
``is_taker=True`` / ``validation_status="OK"`` so the aggregation core can run
unchanged. When the storage layer is upgraded to tick.v1.1 the synthesis path
will be replaced with native column reads.
"""

from mctrader_data.cold.duckdb_resample import (
    BAR_LABEL_PREFIXES,
    DuckDBResampler,
    parse_bar_label,
)
from mctrader_data.cold.polars_fallback import PolarsResampler

__all__ = [
    "BAR_LABEL_PREFIXES",
    "DuckDBResampler",
    "PolarsResampler",
    "parse_bar_label",
]
