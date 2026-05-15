"""Paper-mode lineage shim (MCT-182, ADR-031 §D1) — DEPRECATED, use mctrader_market.paper_lineage.

Layer 0 contract code relocated to mctrader_market (DAG 최하위 FOUNDATION).
This module re-exports from mctrader_market.paper_lineage for backward compatibility.
DeprecationWarning is emitted on import to signal migration path.

Migrate callers:
    # Before
    from mctrader_data.paper_lineage import PaperLineage, canonical_jsonl_hash
    # After
    from mctrader_market.paper_lineage import PaperLineage, canonical_jsonl_hash
"""

from __future__ import annotations

import warnings

warnings.warn(
    "mctrader_data.paper_lineage is deprecated (MCT-182, ADR-031 §D1). "
    "Use mctrader_market.paper_lineage instead.",
    DeprecationWarning,
    stacklevel=2,
)

from mctrader_market.paper_lineage import (  # noqa: E402
    PaperLineage,
    canonical_jsonl_hash,
)

__all__ = ["PaperLineage", "canonical_jsonl_hash"]
