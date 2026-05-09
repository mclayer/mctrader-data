"""Halt / Skip / Quarantine partial failure policy (ADR-009 D5)."""

from __future__ import annotations

import hashlib
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

    from mctrader_market.candle import CandleLike
    from mctrader_market.types import Timeframe

# Sentinel returned by check_duplicate when hashes match (no-op, caller skips write).
DUPLICATE_SAME_HASH = "DUPLICATE_SAME_HASH"

# ADR-009 canonical schema fields required on every candle.
_REQUIRED_SCHEMA_FIELDS = ("ts_utc", "exchange", "symbol", "timeframe", "open", "high", "low", "close", "volume")


class PartialFailurePolicy(StrEnum):
    HALT = "halt"
    QUARANTINE = "quarantine"
    SKIP = "skip"


class QuarantineReason(StrEnum):
    GAP = "GAP"
    DUPLICATE_DIFFERENT_HASH = "DUPLICATE_DIFFERENT_HASH"
    VALUE_OUT_OF_RANGE = "VALUE_OUT_OF_RANGE"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    VALUE_ABSENCE = "VALUE_ABSENCE"
    # MCT-92 — active-active multi-node read-side dedup mismatch
    ACTIVE_ACTIVE_MISMATCH = "ACTIVE_ACTIVE_MISMATCH"


class PolicyDecision(StrEnum):
    HALT = "HALT"
    SKIP = "SKIP"
    QUARANTINE = "QUARANTINE"


_DEFAULT_TRIGGER_TABLE: dict[QuarantineReason, PolicyDecision] = {
    QuarantineReason.GAP: PolicyDecision.HALT,
    QuarantineReason.DUPLICATE_DIFFERENT_HASH: PolicyDecision.QUARANTINE,
    QuarantineReason.VALUE_OUT_OF_RANGE: PolicyDecision.QUARANTINE,
    QuarantineReason.SCHEMA_MISMATCH: PolicyDecision.HALT,
    QuarantineReason.VALUE_ABSENCE: PolicyDecision.QUARANTINE,
    # MCT-92 — multi-node active-active mismatch quarantine (best-effort dedup)
    QuarantineReason.ACTIVE_ACTIVE_MISMATCH: PolicyDecision.QUARANTINE,
}


def resolve_decision(
    reason: QuarantineReason,
    cli_policy: PartialFailurePolicy,
) -> PolicyDecision:
    """Resolve final decision from trigger reason and CLI override.

    - ``cli_policy=halt`` (default, conservative) — every reason → HALT
    - ``cli_policy=quarantine`` — gap/value out of range/value absence → QUARANTINE,
      schema mismatch / duplicate-different-hash always HALT/QUARANTINE per default table
    - ``cli_policy=skip`` (test-only hidden) — duplicate-same-hash internal action only;
      maps reasons to SKIP for callers that opt in.
    """
    if cli_policy is PartialFailurePolicy.HALT:
        return PolicyDecision.HALT
    if cli_policy is PartialFailurePolicy.QUARANTINE:
        return _DEFAULT_TRIGGER_TABLE[reason]
    if cli_policy is PartialFailurePolicy.SKIP:
        return PolicyDecision.SKIP
    raise ValueError(f"unknown policy: {cli_policy!r}")  # pragma: no cover


# ── Policy check functions ─────────────────────────────────────────────────────


def candle_hash(candle: CandleLike) -> str:
    """SHA-256 hex digest of the canonical OHLCV fields for a candle.

    Used by :func:`check_duplicate` to detect same-hash vs diff-hash duplicates.
    """
    payload = (
        f"{candle.ts_utc.isoformat()}"
        f"|{candle.exchange}"
        f"|{candle.symbol}"
        f"|{candle.timeframe}"
        f"|{candle.open}"
        f"|{candle.high}"
        f"|{candle.low}"
        f"|{candle.close}"
        f"|{candle.volume}"
        f"|{candle.value}"
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def check_gap(
    prev_ts: datetime | None,
    curr_ts: datetime,
    timeframe: Timeframe,
) -> QuarantineReason | None:
    """Policy check #1 — gap detection.

    Returns :attr:`QuarantineReason.GAP` when the step from *prev_ts* to
    *curr_ts* is larger than one timeframe period.  Returns ``None`` when there
    is no previous candle (first record) or when the step is exactly one period.
    """
    if prev_ts is None:
        return None
    expected_delta = timeframe.delta
    actual_delta = curr_ts - prev_ts
    if actual_delta > expected_delta:
        return QuarantineReason.GAP
    return None


def check_duplicate(
    existing_hash: str | None,
    incoming_hash: str,
) -> str | None:
    """Policy check #2/#3 — duplicate detection.

    Returns:

    - ``DUPLICATE_SAME_HASH`` (== :data:`DUPLICATE_SAME_HASH`) when the
      existing record has the same hash → caller should **skip** the write.
    - :attr:`QuarantineReason.DUPLICATE_DIFFERENT_HASH` when the existing record
      has a *different* hash for the same timestamp → caller should quarantine.
    - ``None`` when no existing record exists at that timestamp.
    """
    if existing_hash is None:
        return None
    if existing_hash == incoming_hash:
        return DUPLICATE_SAME_HASH
    return QuarantineReason.DUPLICATE_DIFFERENT_HASH


def check_value_range(candle: CandleLike) -> QuarantineReason | None:
    """Policy check #4 — OHLCV value range validation.

    Returns :attr:`QuarantineReason.VALUE_OUT_OF_RANGE` when any of the
    following invariants are violated:

    - ``open``, ``high``, ``low``, ``close``, ``volume`` must all be > 0
    - ``high >= open``, ``high >= close``
    - ``low <= open``, ``low <= close``
    - ``high >= low``

    Returns ``None`` when all checks pass.
    """
    zero = Decimal("0")
    price_fields = (candle.open, candle.high, candle.low, candle.close)
    if any(p <= zero for p in price_fields):
        return QuarantineReason.VALUE_OUT_OF_RANGE
    if candle.volume < zero:
        return QuarantineReason.VALUE_OUT_OF_RANGE
    if candle.high < candle.low:
        return QuarantineReason.VALUE_OUT_OF_RANGE
    if candle.high < candle.open or candle.high < candle.close:
        return QuarantineReason.VALUE_OUT_OF_RANGE
    if candle.low > candle.open or candle.low > candle.close:
        return QuarantineReason.VALUE_OUT_OF_RANGE
    return None


def check_schema(candle: CandleLike) -> QuarantineReason | None:
    """Policy check #5 — schema presence check (ADR-009).

    Returns :attr:`QuarantineReason.SCHEMA_MISMATCH` when any required field is
    missing (``None`` or absent) on the candle.  Returns ``None`` when all
    required fields are present.
    """
    for field in _REQUIRED_SCHEMA_FIELDS:
        value = getattr(candle, field, None)
        if value is None:
            return QuarantineReason.SCHEMA_MISMATCH
    return None
