"""Aggregation Core Lib — Hot/Cold shared pure-Python core (ADR-025).

Story scope (MCT-137 / Epic MCT-112 Story-3):
- 4 bar 알고리즘 SSOT — time / volume / tick / dollar.
- per-symbol state machine + immutable contract metadata + SHA256 contract_id.
- KRW scaled-int boundary helper — Decimal drift 방지.

Public API (Hot/Cold consumer import target):
    from mctrader_data.aggregation import (
        TimeBarAggregator,
        VolumeBarAggregator,
        TickBarAggregator,
        DollarBarAggregator,
        ContractMetadata,
        compute_contract_id,
        to_scaled,
        from_scaled,
    )
"""

from __future__ import annotations

from mctrader_data.aggregation.contract_metadata import (
    ContractMetadata,
    compute_contract_id,
)
from mctrader_data.aggregation.core import (
    DollarBarAggregator,
    TickBarAggregator,
    TimeBarAggregator,
    VolumeBarAggregator,
)
from mctrader_data.aggregation.scaled_int import from_scaled, to_scaled

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
