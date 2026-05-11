"""tests for contract_metadata — immutable + SHA256 contract_id + backward compat.

ADR-025 §contract-metadata.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctrader_data.aggregation.contract_metadata import (
    ContractMetadata,
    compute_contract_id,
)


def _make_metadata(**overrides: object) -> ContractMetadata:
    base: dict[str, object] = {
        "bar_label": "vol_1000",
        "genesis_ts": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "threshold": Decimal("1000"),
        "precision": 0,
        "rounding_rule": "ROUND_HALF_EVEN",
        "source_cutoff": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "tie_breaking": "next_tick",
        "version": "info_bar.v1",
    }
    base.update(overrides)
    return ContractMetadata(**base)  # type: ignore[arg-type]


class TestImmutability:
    """frozen dataclass — runtime mutation 차단."""

    def test_cannot_mutate_field(self) -> None:
        md = _make_metadata()
        with pytest.raises(FrozenInstanceError):
            md.threshold = Decimal("999")  # type: ignore[misc]

    def test_cannot_mutate_genesis_ts(self) -> None:
        md = _make_metadata()
        with pytest.raises(FrozenInstanceError):
            md.genesis_ts = datetime(2027, 1, 1, tzinfo=timezone.utc)  # type: ignore[misc]


class TestContractIdSHA256:
    """contract_id = SHA256 16-hex prefix — deterministic + stable."""

    def test_contract_id_is_16_hex(self) -> None:
        md = _make_metadata()
        cid = compute_contract_id(md)
        assert len(cid) == 16
        assert all(c in "0123456789abcdef" for c in cid)

    def test_same_metadata_same_id(self) -> None:
        md1 = _make_metadata()
        md2 = _make_metadata()
        assert compute_contract_id(md1) == compute_contract_id(md2)

    def test_different_threshold_different_id(self) -> None:
        md1 = _make_metadata(threshold=Decimal("1000"))
        md2 = _make_metadata(threshold=Decimal("2000"))
        assert compute_contract_id(md1) != compute_contract_id(md2)

    def test_different_bar_label_different_id(self) -> None:
        md1 = _make_metadata(bar_label="vol_1000")
        md2 = _make_metadata(bar_label="time_60")
        assert compute_contract_id(md1) != compute_contract_id(md2)

    def test_different_tie_breaking_different_id(self) -> None:
        md1 = _make_metadata(tie_breaking="next_tick")
        md2 = _make_metadata(tie_breaking="current_tick")
        assert compute_contract_id(md1) != compute_contract_id(md2)

    def test_different_version_different_id(self) -> None:
        md1 = _make_metadata(version="info_bar.v1")
        md2 = _make_metadata(version="info_bar.v2")
        assert compute_contract_id(md1) != compute_contract_id(md2)


class TestContractIdProperty:
    """ContractMetadata.contract_id property — lazy + cached."""

    def test_contract_id_property(self) -> None:
        md = _make_metadata()
        assert md.contract_id == compute_contract_id(md)

    def test_contract_id_stable_across_access(self) -> None:
        md = _make_metadata()
        assert md.contract_id == md.contract_id


class TestBackwardCompatVersioning:
    """ADR-008 SemVer — version 필드 별도 박제 → 미래 v2 출현 시 v1 데이터 식별 가능."""

    def test_default_version_v1(self) -> None:
        md = _make_metadata()
        assert md.version == "info_bar.v1"

    def test_version_appears_in_contract_id_hash_input(self) -> None:
        """version 만 다른 metadata → contract_id 달라야 함 (분리 가능)."""
        v1 = _make_metadata(version="info_bar.v1")
        v2 = _make_metadata(version="info_bar.v2")
        assert v1.contract_id != v2.contract_id


class TestThresholdValidation:
    def test_zero_threshold_raises(self) -> None:
        with pytest.raises(ValueError, match="threshold"):
            _make_metadata(threshold=Decimal("0"))

    def test_negative_threshold_raises(self) -> None:
        with pytest.raises(ValueError, match="threshold"):
            _make_metadata(threshold=Decimal("-1"))


class TestTimezoneEnforcement:
    def test_genesis_ts_naive_raises(self) -> None:
        with pytest.raises(ValueError, match="UTC|timezone"):
            _make_metadata(genesis_ts=datetime(2026, 1, 1))

    def test_source_cutoff_naive_raises(self) -> None:
        with pytest.raises(ValueError, match="UTC|timezone"):
            _make_metadata(source_cutoff=datetime(2026, 1, 1))
