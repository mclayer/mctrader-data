"""Halt/Skip/Quarantine policy resolution tests."""

from __future__ import annotations

import pytest

from mctrader_data.policy import (
    PartialFailurePolicy,
    PolicyDecision,
    QuarantineReason,
    resolve_decision,
)


def test_halt_policy_always_halt() -> None:
    for reason in QuarantineReason:
        assert resolve_decision(reason, PartialFailurePolicy.HALT) is PolicyDecision.HALT


def test_quarantine_policy_table() -> None:
    assert resolve_decision(QuarantineReason.GAP, PartialFailurePolicy.QUARANTINE) is PolicyDecision.HALT
    assert (
        resolve_decision(QuarantineReason.SCHEMA_MISMATCH, PartialFailurePolicy.QUARANTINE)
        is PolicyDecision.HALT
    )
    assert (
        resolve_decision(QuarantineReason.VALUE_OUT_OF_RANGE, PartialFailurePolicy.QUARANTINE)
        is PolicyDecision.QUARANTINE
    )
    assert (
        resolve_decision(QuarantineReason.VALUE_ABSENCE, PartialFailurePolicy.QUARANTINE)
        is PolicyDecision.QUARANTINE
    )
    assert (
        resolve_decision(QuarantineReason.DUPLICATE_DIFFERENT_HASH, PartialFailurePolicy.QUARANTINE)
        is PolicyDecision.QUARANTINE
    )


def test_skip_policy_returns_skip() -> None:
    for reason in QuarantineReason:
        assert resolve_decision(reason, PartialFailurePolicy.SKIP) is PolicyDecision.SKIP


@pytest.mark.parametrize("policy", list(PartialFailurePolicy))
def test_policy_enum_values(policy: PartialFailurePolicy) -> None:
    assert policy.value in {"halt", "quarantine", "skip"}
