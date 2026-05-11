"""Provenance assignment tests (ADR-009 §D16 + ADR-026 §D3).

Epic MCT-112 Story-12 (MCT-146).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mctrader_data.provenance import (
    PROVENANCE_LEGACY_CANDLE,
    PROVENANCE_TRANSACTION_DERIVED,
    assign_provenance,
)


class TestAssignProvenance:
    def test_pre_cutoff_is_legacy_candle(self):
        ts = datetime(2026, 5, 31, 23, 59, 59, tzinfo=timezone.utc)
        assert assign_provenance(ts) == PROVENANCE_LEGACY_CANDLE
        assert assign_provenance(ts) == "legacy_candle"

    def test_at_cutoff_is_transaction_derived(self):
        """ADR-026 §D3: ``ts >= cutoff`` = transaction_derived."""
        ts = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert assign_provenance(ts) == PROVENANCE_TRANSACTION_DERIVED
        assert assign_provenance(ts) == "transaction_derived"

    def test_post_cutoff_is_transaction_derived(self):
        ts = datetime(2026, 7, 15, 12, 30, 0, tzinfo=timezone.utc)
        assert assign_provenance(ts) == PROVENANCE_TRANSACTION_DERIVED

    def test_far_history_is_legacy(self):
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert assign_provenance(ts) == PROVENANCE_LEGACY_CANDLE

    def test_naive_datetime_rejected(self):
        with pytest.raises(ValueError, match="tz-aware"):
            assign_provenance(datetime(2026, 5, 31))

    def test_provenance_constants_match_adr009_d16(self):
        """ADR-009 §D16 allowed values 정합 — string literal 박제."""
        assert PROVENANCE_LEGACY_CANDLE == "legacy_candle"
        assert PROVENANCE_TRANSACTION_DERIVED == "transaction_derived"

    def test_override_cutoff_param(self):
        override = datetime(2027, 1, 1, tzinfo=timezone.utc)
        # ts < override → legacy
        assert (
            assign_provenance(datetime(2026, 12, 31, tzinfo=timezone.utc), cutoff=override)
            == PROVENANCE_LEGACY_CANDLE
        )
        # ts >= override → derived
        assert (
            assign_provenance(datetime(2027, 1, 1, tzinfo=timezone.utc), cutoff=override)
            == PROVENANCE_TRANSACTION_DERIVED
        )
