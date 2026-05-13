"""Halt/Skip/Quarantine policy resolution tests."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctrader_market.candle import CandleModel
from mctrader_market.types import Symbol, Timeframe

from mctrader_data.policy import (
    DUPLICATE_SAME_HASH,
    PartialFailurePolicy,
    PolicyDecision,
    QuarantineReason,
    candle_hash,
    check_duplicate,
    check_gap,
    check_schema,
    check_value_range,
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


# MCT-92 — active-active mismatch reason
def test_active_active_mismatch_reason_exists() -> None:
    assert QuarantineReason.ACTIVE_ACTIVE_MISMATCH.value == "ACTIVE_ACTIVE_MISMATCH"


def test_active_active_mismatch_default_decision_is_quarantine() -> None:
    assert (
        resolve_decision(
            QuarantineReason.ACTIVE_ACTIVE_MISMATCH, PartialFailurePolicy.QUARANTINE
        )
        is PolicyDecision.QUARANTINE
    )


def test_active_active_mismatch_halt_policy_returns_halt() -> None:
    assert (
        resolve_decision(
            QuarantineReason.ACTIVE_ACTIVE_MISMATCH, PartialFailurePolicy.HALT
        )
        is PolicyDecision.HALT
    )


# --- MCT-109: check_* 함수 단위 테스트 ---
def _make_test_candle(**kwargs):
    defaults = {
        "ts_utc": datetime(2026, 5, 1, tzinfo=timezone.utc),
        "exchange": "bithumb",
        "symbol": Symbol(base="BTC", quote="KRW"),
        "timeframe": Timeframe.H1,
        "open": Decimal("100"),
        "high": Decimal("110"),
        "low": Decimal("90"),
        "close": Decimal("105"),
        "volume": Decimal("1"),
        "value": None,
    }
    defaults.update(kwargs)
    # model_construct bypasses pydantic validators — needed for intentionally invalid data
    return CandleModel.model_construct(**defaults)


def test_check_gap_returns_gap_reason() -> None:
    prev = datetime(2026, 5, 1, tzinfo=timezone.utc)
    curr = datetime(2026, 5, 1, 3, tzinfo=timezone.utc)  # 3h gap on H1 → GAP
    assert check_gap(prev, curr, Timeframe.H1) is QuarantineReason.GAP


def test_check_duplicate_same_hash_returns_sentinel() -> None:
    candle = _make_test_candle()
    h = candle_hash(candle)
    assert check_duplicate(h, h) == DUPLICATE_SAME_HASH


def test_check_duplicate_diff_hash_returns_quarantine_reason() -> None:
    candle = _make_test_candle()
    h1 = candle_hash(candle)
    h2 = candle_hash(_make_test_candle(close=Decimal("200")))
    assert check_duplicate(h1, h2) is QuarantineReason.DUPLICATE_DIFFERENT_HASH


def test_check_value_range_violation_returns_quarantine() -> None:
    bad = _make_test_candle(low=Decimal("200"), high=Decimal("100"))  # high < low
    assert check_value_range(bad) is QuarantineReason.VALUE_OUT_OF_RANGE


def test_check_schema_missing_field_returns_mismatch() -> None:
    # exchange=None cannot be passed to CandleModel (pydantic strict).
    # Use a simple namespace stub to simulate a candle with a missing field.
    class _StubCandle:
        ts_utc = datetime(2026, 5, 1, tzinfo=timezone.utc)
        exchange = None  # missing required field
        symbol = Symbol(base="BTC", quote="KRW")
        timeframe = Timeframe.H1
        open = Decimal("100")
        high = Decimal("110")
        low = Decimal("90")
        close = Decimal("105")
        volume = Decimal("1")
        value = None

    assert check_schema(_StubCandle()) is QuarantineReason.SCHEMA_MISMATCH  # type: ignore[arg-type]
