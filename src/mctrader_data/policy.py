"""Halt / Skip / Quarantine partial failure policy (ADR-009 D5)."""

from __future__ import annotations

from enum import StrEnum


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
