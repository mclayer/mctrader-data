"""Immutable contract metadata + SHA256 contract_id (ADR-025 §contract-metadata).

The ``ContractMetadata`` dataclass is frozen — runtime mutation is rejected.
``contract_id`` is a 16-hex prefix of SHA256 over a canonical serialization of
all fields, so any change in algorithm/threshold/precision/cutoff/version is
visibly distinct across emitted bars and downstream reconciliation.

Backward compatibility (ADR-008): the ``version`` field is part of the hash
input, so an ``info_bar.v2`` rollout never collides with v1 contract_ids.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal


def _ensure_utc(value: datetime, name: str) -> datetime:
    """Reject naive datetimes and non-UTC tz (force boundary discipline)."""
    if value.tzinfo is None:
        raise ValueError(f"{name} must be a timezone-aware UTC datetime, got naive: {value!r}")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must be in UTC timezone, got offset {value.utcoffset()}")
    return value


@dataclass(frozen=True)
class ContractMetadata:
    """Immutable metadata for an information-bar contract.

    Fields are canonicalised in :func:`compute_contract_id` ordering — any
    change to schema must bump ``version`` (ADR-008 SemVer MAJOR/MINOR).

    Attributes:
        bar_label: information-bar discriminator (e.g. ``"vol_1000"``).
        genesis_ts: emission-origin UTC timestamp.
        threshold: bar-close threshold (>0). Time bars use seconds-as-Decimal.
        precision: scaled-int fractional digit count (0 = KRW integer).
        rounding_rule: explicit Python ``decimal`` rounding mode name.
        source_cutoff: producer-side data freshness cutoff (UTC).
        tie_breaking: SSOT rule when cumulative metric == threshold exactly.
            Allowed values: ``"current_tick"`` | ``"next_tick"``.
        version: SemVer schema tag (default ``"info_bar.v1"``).
    """

    bar_label: str
    genesis_ts: datetime
    threshold: Decimal
    precision: int
    rounding_rule: str
    source_cutoff: datetime
    tie_breaking: str
    version: str = "info_bar.v1"
    # cached contract_id (computed lazily in __post_init__ via object.__setattr__)
    _contract_id: str = field(default="", init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        # Validate immutable invariants once at construction time.
        if not self.bar_label:
            raise ValueError("bar_label must be non-empty")
        if self.threshold <= 0:
            raise ValueError(f"threshold ({self.threshold}) must be > 0")
        if self.precision < 0:
            raise ValueError(f"precision must be >= 0, got {self.precision}")
        if self.tie_breaking not in ("current_tick", "next_tick"):
            raise ValueError(f"tie_breaking must be 'current_tick' or 'next_tick', got {self.tie_breaking!r}")
        _ensure_utc(self.genesis_ts, "genesis_ts")
        _ensure_utc(self.source_cutoff, "source_cutoff")
        # frozen dataclass: bypass __setattr__ ban with object.__setattr__ for cache.
        object.__setattr__(self, "_contract_id", _compute_contract_id(self))

    @property
    def contract_id(self) -> str:
        """SHA256-16-hex over canonical field serialization (cached)."""
        return self._contract_id


def _canonical_repr(metadata: ContractMetadata) -> str:
    """Canonical (stable) serialization for hashing.

    Order is fixed; floats forbidden; datetimes ISO-8601 with explicit UTC.
    """
    parts: tuple[str, ...] = (
        f"bar_label={metadata.bar_label}",
        f"genesis_ts={metadata.genesis_ts.isoformat()}",
        f"threshold={metadata.threshold!s}",
        f"precision={metadata.precision}",
        f"rounding_rule={metadata.rounding_rule}",
        f"source_cutoff={metadata.source_cutoff.isoformat()}",
        f"tie_breaking={metadata.tie_breaking}",
        f"version={metadata.version}",
    )
    return "|".join(parts)


def _compute_contract_id(metadata: ContractMetadata) -> str:
    """Internal helper to compute contract_id from a fully-constructed instance."""
    payload = _canonical_repr(metadata).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def compute_contract_id(metadata: ContractMetadata) -> str:
    """Public wrapper — returns the deterministic 16-hex contract_id."""
    return metadata.contract_id


__all__ = ["ContractMetadata", "compute_contract_id"]
