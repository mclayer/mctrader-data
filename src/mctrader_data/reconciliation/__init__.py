"""Dual-write reconciliation harness (Story MCT-145 / Epic MCT-112 Story-11).

Three harness components support the 2-4 week dual-write verification window
before legacy candle retirement (Story-12):

- :class:`DualWriteHarness` — daily random sampling of legacy candle vs
  transaction-derived bars; OHLCV diff + bar-count mismatch report.
- :class:`HotColdConsistencyHarness` — Hot path (engine streaming) vs Cold path
  (DuckDB resample) byte-identical verification with drift SLO < 0.01%
  (ADR-025 §D5).
- :mod:`edge_case_fixtures` — deterministic edge-case generators: threshold
  boundary, time bar ``[start, end)`` inclusion, KRW rounding edge.

Drift SLO contract (ADR-025 §D5)
--------------------------------
SLO gate: mismatch_pct < 0.01% → PASS, ≥ 0.01% → FAIL + emit
:class:`ConsistencyDriftError`. The harness is fail-closed by design — a
failing reconciliation must not silently degrade Hot/Cold consistency.

Public API
----------
::

    from mctrader_data.reconciliation import (
        DualWriteHarness,
        HotColdConsistencyHarness,
        ConsistencyDriftError,
        DualWriteReport,
        HotColdReport,
    )
"""

from __future__ import annotations

from mctrader_data.reconciliation.dual_write_harness import (
    DualWriteHarness,
    DualWriteReport,
    SymbolWindowDiff,
)
from mctrader_data.reconciliation.edge_case_fixtures import (
    EdgeCaseFixture,
    generate_krw_rounding_edge,
    generate_threshold_boundary,
    generate_time_bar_boundary,
)
from mctrader_data.reconciliation.hot_cold_consistency import (
    DRIFT_SLO_THRESHOLD,
    ConsistencyDriftError,
    HotColdConsistencyHarness,
    HotColdReport,
)

__all__ = [
    "DRIFT_SLO_THRESHOLD",
    "ConsistencyDriftError",
    "DualWriteHarness",
    "DualWriteReport",
    "EdgeCaseFixture",
    "HotColdConsistencyHarness",
    "HotColdReport",
    "SymbolWindowDiff",
    "generate_krw_rounding_edge",
    "generate_threshold_boundary",
    "generate_time_bar_boundary",
]
