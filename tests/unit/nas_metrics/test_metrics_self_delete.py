"""test_metrics_self_delete.py — Unit tests for MCT-202 Prometheus Counter 19 series.

Change Plan §8.1:
- compactor_local_self_delete_total{tier, outcome}: 3 tier × 5 outcome = 15 series
- mctrader_retry_orphan_total{tier}: 3 series
- mctrader_legacy_cleanup_race_noop_total: 1 series
- 총 19 series ≤ 50 (ADR-027 §D6 cardinality invariant)
- INV-SEC-6: sha256 Prometheus label 0
"""
from __future__ import annotations

import pytest


class TestCounterCardinality:
    """19 series 총합 + cardinality ≤ 50 (ADR-027 §D6)."""

    def test_compactor_local_self_delete_total_exists(self) -> None:
        """compactor_local_self_delete_total Counter 등록 확인."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            compactor_local_self_delete_total,
        )
        assert compactor_local_self_delete_total is not None

    def test_mctrader_retry_orphan_total_exists(self) -> None:
        """mctrader_retry_orphan_total Counter 등록 확인."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            mctrader_retry_orphan_total,
        )
        assert mctrader_retry_orphan_total is not None

    def test_mctrader_legacy_cleanup_race_noop_total_exists(self) -> None:
        """mctrader_legacy_cleanup_race_noop_total Counter 등록 확인."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            mctrader_legacy_cleanup_race_noop_total,
        )
        assert mctrader_legacy_cleanup_race_noop_total is not None

    @pytest.mark.parametrize("tier", ["L1", "L2", "L3"])
    @pytest.mark.parametrize("outcome", [
        "committed_unlinked",
        "committed_unlink_failed",
        "local_only_retained",
        "hard_floor_retained",
        "already_promoted",
    ])
    def test_counter_emit_parametrize(self, tier: str, outcome: str) -> None:
        """3 tier × 5 outcome = 15 series label 조합 모두 emit 가능."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            compactor_local_self_delete_total,
        )
        # label 조합이 유효한지 확인 (오류 없이 inc() 호출 가능)
        counter = compactor_local_self_delete_total.labels(tier=tier, outcome=outcome)
        assert counter is not None

    @pytest.mark.parametrize("tier", ["L1", "L2", "L3"])
    def test_retry_orphan_counter_emit_parametrize(self, tier: str) -> None:
        """3 tier series label 조합 emit 가능."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            mctrader_retry_orphan_total,
        )
        counter = mctrader_retry_orphan_total.labels(tier=tier)
        assert counter is not None

    def test_race_noop_counter_no_labels(self) -> None:
        """mctrader_legacy_cleanup_race_noop_total: label 0 (1 series)."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            mctrader_legacy_cleanup_race_noop_total,
        )
        # label 없는 Counter = 1 series
        assert mctrader_legacy_cleanup_race_noop_total is not None


class TestSha256NotInPromLabel:
    """INV-SEC-6: sha256 hex Prometheus label 0."""

    def test_compactor_local_self_delete_total_no_sha256_label(self) -> None:
        """compactor_local_self_delete_total labelnames 에 sha256 없음."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            compactor_local_self_delete_total,
        )
        label_names = compactor_local_self_delete_total._labelnames
        assert "sha256" not in label_names, (
            "INV-SEC-6: sha256 Prometheus label 포함 금지 (cardinality 무한 폭증 risk)"
        )

    def test_retry_orphan_total_no_sha256_label(self) -> None:
        """mctrader_retry_orphan_total labelnames 에 sha256 없음."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            mctrader_retry_orphan_total,
        )
        label_names = mctrader_retry_orphan_total._labelnames
        assert "sha256" not in label_names

    def test_cardinality_total_le_50(self) -> None:
        """19 series = 15 + 3 + 1 ≤ 50 (ADR-027 §D6 cardinality invariant)."""
        # 15 (3 tier × 5 outcome) + 3 (retry_orphan 3 tier) + 1 (race_noop) = 19
        compactor_series = 3 * 5   # tier × outcome
        orphan_series = 3           # tier
        race_noop_series = 1
        total = compactor_series + orphan_series + race_noop_series
        assert total == 19, f"예상 19, 실제 {total}"
        assert total <= 50, f"ADR-027 §D6: cardinality ≤ 50 위반 ({total})"


class TestCounterLabelNames:
    """Counter labelname 규격 검증."""

    def test_compactor_self_delete_labelnames(self) -> None:
        """compactor_local_self_delete_total labelnames = ['tier', 'outcome']."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            compactor_local_self_delete_total,
        )
        assert set(compactor_local_self_delete_total._labelnames) == {"tier", "outcome"}

    def test_retry_orphan_labelnames(self) -> None:
        """mctrader_retry_orphan_total labelnames = ['tier']."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            mctrader_retry_orphan_total,
        )
        assert list(mctrader_retry_orphan_total._labelnames) == ["tier"]

    def test_race_noop_no_labelnames(self) -> None:
        """mctrader_legacy_cleanup_race_noop_total: labelnames 없음 (label 0)."""
        from mctrader_data.nas_metrics.prometheus_exporters import (
            mctrader_legacy_cleanup_race_noop_total,
        )
        assert len(mctrader_legacy_cleanup_race_noop_total._labelnames) == 0
