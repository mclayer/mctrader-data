"""Aggregation shim (MCT-182, ADR-031 §D1) — DEPRECATED, use mctrader_market.aggregation.

Layer 0 contract code relocated to mctrader_market (DAG 최하위 FOUNDATION).
This module re-exports from mctrader_market.aggregation for backward compatibility.
DeprecationWarning is emitted on import to signal migration path.

Migrate callers:
    # Before
    from mctrader_data.aggregation import ContractMetadata
    # After
    from mctrader_market.aggregation import ContractMetadata
"""

from __future__ import annotations

import warnings

warnings.warn(
    "mctrader_data.aggregation is deprecated (MCT-182, ADR-031 §D1). "
    "Use mctrader_market.aggregation instead.",
    DeprecationWarning,
    stacklevel=2,
)

from mctrader_market.aggregation import (  # noqa: E402
    ContractMetadata,
    DollarBarAggregator,
    TickBarAggregator,
    TimeBarAggregator,
    VolumeBarAggregator,
    compute_contract_id,
    from_scaled,
    to_scaled,
)

__all__ = [
    "ContractMetadata",
    "DollarBarAggregator",
    "TickBarAggregator",
    "TimeBarAggregator",
    "VolumeBarAggregator",
    "compute_contract_id",
    "from_scaled",
    "to_scaled",
]
