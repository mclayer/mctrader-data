"""Paper-mode lineage extension (MCT-20 A3) — WebSocket batch hash + adapter naming.

ADR-009 lineage field semantics extended for WebSocket-aggregated bars:

- ``endpoint`` = ``wss://...`` URL
- ``request_params_hash`` = subscribe message canonical hash
- ``fetched_at_utc`` = aggregation_finalized_at (BarAggregator close time)
- ``response_hash`` = order-preserving normalized JSONL batch sha256
- ``adapter_name`` = ``mctrader-market-bithumb-ws``
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from mctrader_market.types import UTCDateTime


class PaperLineage(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, arbitrary_types_allowed=True)

    snapshot_id: str
    run_id: str
    exchange: str
    endpoint: str
    request_params_hash: str
    fetched_at_utc: UTCDateTime
    response_hash: str
    adapter_name: Literal["mctrader-market-bithumb-ws"]
    adapter_version: str


def canonical_jsonl_hash(messages: Iterable[dict[str, Any]]) -> str:
    """Order-preserving normalized JSONL sha256 (MCT-20 A3 option b)."""
    hasher = hashlib.sha256()
    for msg in messages:
        line = json.dumps(msg, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        hasher.update(line.encode("utf-8"))
        hasher.update(b"\n")
    return hasher.hexdigest()
