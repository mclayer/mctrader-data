"""Provenance column assignment (ADR-009 §D16 + ADR-026 §D3).

Epic MCT-112 Story-12 (MCT-146) — Legacy candle retirement + provenance row-level
박제 helper.

본 모듈은 row-level provenance 값 ("legacy_candle" / "transaction_derived") 의
**assignment policy** 만 노출. Parquet write 시점의 column 추가는 ohlcv schema
amendment (별도 Story-3 / Story-5 implementation seal 책임) — 본 모듈은 helper API.

Cross-references:

- ADR-009 §D16: provenance column allowed values + dual-namespace operation
- ADR-026 §D1 (legacy immutable SSOT) + §D2 (cutoff timestamp) + §D3 (provenance semantics)
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from mctrader_data.cutoff import is_pre_cutoff

__all__ = (
    "Provenance",
    "PROVENANCE_LEGACY_CANDLE",
    "PROVENANCE_TRANSACTION_DERIVED",
    "assign_provenance",
)

Provenance = Literal["legacy_candle", "transaction_derived"]

PROVENANCE_LEGACY_CANDLE: Provenance = "legacy_candle"
"""ADR-026 §D3: cutoff 이전 historic row (immutable SSOT, Bithumb candle API source)."""

PROVENANCE_TRANSACTION_DERIVED: Provenance = "transaction_derived"
"""ADR-026 §D3: cutoff 이후 row (Aggregation Core Lib derive, tick.v1.1 source)."""


def assign_provenance(ts: datetime, *, cutoff: datetime | None = None) -> Provenance:
    """Row-level provenance 결정 — ADR-026 §D3.

    - ``ts < cutoff_timestamp`` → ``"legacy_candle"`` (immutable historic SSOT).
    - ``ts >= cutoff_timestamp`` → ``"transaction_derived"`` (Aggregation Core derive).

    Args:
        ts: row 의 ``ts_utc`` (tz-aware UTC).
        cutoff: override (default = :data:`mctrader_data.cutoff.CUTOFF_TIMESTAMP`).

    Raises:
        ValueError: ``ts`` 가 naive datetime 일 때.
    """
    if is_pre_cutoff(ts, cutoff=cutoff):
        return PROVENANCE_LEGACY_CANDLE
    return PROVENANCE_TRANSACTION_DERIVED
