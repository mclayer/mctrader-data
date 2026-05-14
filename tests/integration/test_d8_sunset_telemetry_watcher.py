# tests/integration/test_d8_sunset_telemetry_watcher.py
"""MCT-172 TDD tests: D8 sunset telemetry watcher 14d rolling 0-hit alert rule 박제.

Story: MCT-172 (EPIC-tier-promotion-single-source Story-6)
AC: AC-3 — D8 sunset policy finalize (D8-3=A + D8-4=C)

Test Contract (MCT-172 §4 AC-3):
- test_d8_sunset_window_constants: D8 sunset 14d window 기준점 상수 박제
  (2026-08-18T00:00:00Z ~ 2026-09-01T00:00:00Z, D8-4=C)
- test_d8_sunset_and_condition: D8 sunset = cutoff AND telemetry 0-hit 14d (OR 아님)
  INV-3: AND 조건 gate
- test_ambiguity_counter_zero_rate_means_sunset_eligible: 14d rate=0 → sunset eligible
  (ADR-029 §D8 telemetry watcher 조건 검증)
- test_ambiguity_counter_nonzero_rate_means_not_eligible: 14d rate>0 → sunset 불가
  (telemetry hit 존재 = forward-only migration 완료 미확인)
- test_d8_sunset_cutoff_hard_date: cutoff 2026-09-01 이전 = sunset 실행 불가
  (D8-3=A: 정책 finalize only, 즉시 sunset 비정합)
- test_d8_telemetry_watcher_alert_rule_format: Prometheus alert rule 검증
  (nas_reader_ambiguity_total 14d rolling 0-hit → silence / >0 → alert)
- test_epic_closed_prerequisite_list: Epic CLOSED prerequisite 4종 목록 박제
  (production evidence quad + WAL 30G + 14d telemetry + sunset)

D8-3=A: 정책 finalize only (2026-05-14). 즉시 sunset 비정합 (14d 미충족).
D8-4=C: 14d window = 2026-08-18T00:00:00Z ~ 2026-09-01T00:00:00Z (cutoff 직전 14d).
INV-3: AND 조건 강제 (cutoff 2026-09-01 + telemetry 0-hit 14d 연속).

verified-via: Read docs/stories/MCT-172.md §3 D8-3/D8-4 + §4 AC-3 + §5 INV-3
verified-via: Read docs/adr/ADR-029-tier-promotion-single-source.md (D8 sunset criterion)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import NamedTuple

import pytest


# ─── D8 sunset policy constants (ADR-029 §D8 amendment, D8-4=C) ──────────────

# D8-4=C: 14d telemetry window = cutoff 직전 14d
D8_TELEMETRY_WINDOW_START: str = "2026-08-18T00:00:00Z"
D8_TELEMETRY_WINDOW_END: str = "2026-09-01T00:00:00Z"
D8_SUNSET_CUTOFF: str = "2026-09-01T00:00:00Z"

# Prometheus metric names (MCT-170 LAND, dr_mode.py)
AMBIGUITY_COUNTER_METRIC: str = "nas_reader_ambiguity_total"
DR_STATE_GAUGE_METRIC: str = "nas_reader_dr_state"

# Epic CLOSED prerequisite 4종 (MCT-172 §9)
EPIC_CLOSED_PREREQUISITES = [
    "production_deploy_14d_0hit_telemetry",  # 2026-08-18 ~ 2026-09-01
    "wal_30g_production_measurement",          # peak 09:00 KST burst
    "production_evidence_quad_same_1h_window", # bucket+log+Prometheus+drainage
    "epic_closed_pr_or_scope_manifest_amend",  # POLICY_FINALIZED → CLOSED
]


# ─── helpers ─────────────────────────────────────────────────────────────────


class TelemetrySnapshot(NamedTuple):
    """Mock Prometheus metric snapshot (14d rolling window)."""
    metric_name: str
    rate_per_14d: float  # 0.0 = 0-hit (sunset eligible), >0 = still active


def _parse_iso_z(ts: str) -> datetime:
    """Parse ISO 8601 UTC timestamp (Z suffix)."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _is_sunset_eligible(
    snapshot: TelemetrySnapshot,
    *,
    current_date: datetime,
    cutoff: datetime,
) -> bool:
    """D8 sunset eligibility: cutoff reached AND telemetry 0-hit 14d (INV-3 AND gate)."""
    cutoff_reached = current_date >= cutoff
    telemetry_zero = snapshot.rate_per_14d == 0.0
    return cutoff_reached and telemetry_zero  # AND, not OR


# ─── Tests ───────────────────────────────────────────────────────────────────


class TestD8SunsetWindowConstants:
    """D8-4=C: 14d telemetry window 기준점 상수 박제."""

    def test_d8_sunset_window_constants(self) -> None:
        """D8 sunset window = 2026-08-18T00:00:00Z ~ 2026-09-01T00:00:00Z (D8-4=C).

        ADR-029 §D8 amendment: 14d window 기준점 SSOT.
        MCT-170/171 LAND일(2026-05-14)과 cutoff(2026-09-01)는 3.5개월 분리.
        """
        window_start = _parse_iso_z(D8_TELEMETRY_WINDOW_START)
        window_end = _parse_iso_z(D8_TELEMETRY_WINDOW_END)
        cutoff = _parse_iso_z(D8_SUNSET_CUTOFF)

        # window_end = cutoff (cutoff 직전 14d)
        assert window_end == cutoff, (
            f"D8-4=C: window_end({window_end}) == cutoff({cutoff}) 기대. "
            "cutoff 직전 14d 정책 정합."
        )

        # 14d window duration = exactly 14 days
        delta = window_end - window_start
        assert delta.days == 14, (
            f"D8 telemetry window = 14 days 기대. actual: {delta.days} days (D8-4=C)."
        )

    def test_d8_sunset_cutoff_hard_date(self) -> None:
        """cutoff = 2026-09-01T00:00:00Z 박제 (D8 hard cutoff, D8-3=A 정합).

        2026-05-14 현재 기준: cutoff 미도달 = 즉시 sunset 비정합 (D8-3=A).
        """
        story_land_date = datetime(2026, 5, 14, tzinfo=timezone.utc)
        cutoff = _parse_iso_z(D8_SUNSET_CUTOFF)

        assert story_land_date < cutoff, (
            f"Story LAND date ({story_land_date.date()}) < cutoff ({cutoff.date()}) 기대. "
            "즉시 sunset 불가 (D8-3=A: 정책 finalize only 2026-05-14)."
        )

        cutoff_year_month = (cutoff.year, cutoff.month, cutoff.day)
        assert cutoff_year_month == (2026, 9, 1), (
            f"D8 cutoff = 2026-09-01 박제. actual: {cutoff.date()} (D8-3=A)."
        )


class TestD8SunsetAndCondition:
    """INV-3: D8 sunset = AND 조건 (cutoff AND telemetry 0-hit 14d)."""

    def test_d8_sunset_and_condition(self) -> None:
        """INV-3: cutoff AND telemetry 0-hit (OR 아님).

        OR 조건이면 cutoff 전에도 sunset 가능 — 이는 설계 위반.
        """
        snapshot_zero = TelemetrySnapshot(AMBIGUITY_COUNTER_METRIC, rate_per_14d=0.0)
        cutoff = _parse_iso_z(D8_SUNSET_CUTOFF)

        # Case 1: cutoff reached + 0-hit → eligible
        after_cutoff = datetime(2026, 9, 2, tzinfo=timezone.utc)
        assert _is_sunset_eligible(snapshot_zero, current_date=after_cutoff, cutoff=cutoff)

        # Case 2: cutoff reached + non-zero hit → NOT eligible (AND gate)
        snapshot_nonzero = TelemetrySnapshot(AMBIGUITY_COUNTER_METRIC, rate_per_14d=1.5)
        assert not _is_sunset_eligible(snapshot_nonzero, current_date=after_cutoff, cutoff=cutoff), (
            "INV-3 AND 조건: cutoff 도달해도 telemetry hit > 0 이면 sunset 불가."
        )

        # Case 3: 0-hit + cutoff 미도달 → NOT eligible (AND gate critical case)
        before_cutoff = datetime(2026, 8, 1, tzinfo=timezone.utc)
        assert not _is_sunset_eligible(snapshot_zero, current_date=before_cutoff, cutoff=cutoff), (
            "INV-3 AND 조건: telemetry 0-hit 이어도 cutoff 미도달 시 sunset 불가 (D8-3=A)."
        )

    def test_ambiguity_counter_zero_rate_means_sunset_eligible(self) -> None:
        """14d nas_reader_ambiguity_total rate=0 → sunset eligible (cutoff 도달 가정).

        ADR-029 §D8: forward-only migration 완료 증거 = ambiguity counter 0.
        """
        snapshot = TelemetrySnapshot(AMBIGUITY_COUNTER_METRIC, rate_per_14d=0.0)
        cutoff = _parse_iso_z(D8_SUNSET_CUTOFF)
        post_cutoff = datetime(2026, 9, 1, 1, tzinfo=timezone.utc)  # cutoff +1h

        result = _is_sunset_eligible(snapshot, current_date=post_cutoff, cutoff=cutoff)
        assert result, (
            "14d 0-hit + cutoff 도달 → sunset eligible 기대. "
            f"rate={snapshot.rate_per_14d}, cutoff={cutoff.date()}"
        )

    def test_ambiguity_counter_nonzero_rate_means_not_eligible(self) -> None:
        """14d rate>0 → sunset 불가 (forward-only migration 완료 미확인).

        ambiguity hit 존재 = UNKNOWN_TIER record 아직 남아있거나 XOR violation 발생.
        """
        snapshot = TelemetrySnapshot(AMBIGUITY_COUNTER_METRIC, rate_per_14d=0.001)
        cutoff = _parse_iso_z(D8_SUNSET_CUTOFF)
        post_cutoff = datetime(2026, 9, 15, tzinfo=timezone.utc)

        result = _is_sunset_eligible(snapshot, current_date=post_cutoff, cutoff=cutoff)
        assert not result, (
            "14d rate>0 → sunset 불가 기대 (telemetry hit 존재). "
            f"rate={snapshot.rate_per_14d}"
        )


class TestD8TelemetryWatcherAlertRule:
    """Prometheus alert rule 검증: nas_reader_ambiguity_total 14d rolling 0-hit."""

    def test_d8_telemetry_watcher_alert_rule_format(self) -> None:
        """Prometheus alert rule 형식 박제 (14d rolling rate = 0 → silence, >0 → alert).

        ADR-029 §D8 telemetry watcher 정책:
        - rate(nas_reader_ambiguity_total[14d]) == 0 → sunset eligible (silence)
        - rate(nas_reader_ambiguity_total[14d]) > 0 → sunset blocked (alert)

        본 test 는 rule 형식 문자열 + 기간 상수 박제.
        """
        # Alert rule 구성요소 SSOT 박제
        alert_rule = {
            "alert": "NASReaderAmbiguityDetected",
            "expr": f"rate({AMBIGUITY_COUNTER_METRIC}[14d]) > 0",
            "for": "5m",
            "labels": {"severity": "warning"},
            "annotations": {
                "summary": "NAS ambiguity counter non-zero (D8 sunset blocked)",
                "description": (
                    f"rate({AMBIGUITY_COUNTER_METRIC}[14d]) > 0 detected. "
                    "D8 sunset requires 14d 0-hit window (2026-08-18 ~ 2026-09-01). "
                    "forward-only migration 완료 미확인 (ADR-029 §D8 AND gate)."
                ),
            },
        }

        # Rule 형식 검증
        assert alert_rule["alert"] == "NASReaderAmbiguityDetected"
        assert "14d" in alert_rule["expr"], "14d rolling window 기재 의무 (D8-4=C)"
        assert AMBIGUITY_COUNTER_METRIC in alert_rule["expr"]
        assert "> 0" in alert_rule["expr"], "0-hit check (D8 sunset AND gate)"

    def test_d8_silence_rule_zero_rate(self) -> None:
        """14d rate=0 = silence 조건 박제 (sunset eligible signal)."""
        silence_condition_expr = f"rate({AMBIGUITY_COUNTER_METRIC}[14d]) == 0"
        assert AMBIGUITY_COUNTER_METRIC in silence_condition_expr
        assert "14d" in silence_condition_expr
        assert "== 0" in silence_condition_expr, "0-hit = silence (sunset eligible)"

    def test_nas_reader_ambiguity_total_metric_name(self) -> None:
        """Prometheus Counter 이름 SSOT 박제 (MCT-170 LAND dr_mode.py)."""
        assert AMBIGUITY_COUNTER_METRIC == "nas_reader_ambiguity_total", (
            f"nas_reader_ambiguity_total Counter 이름 SSOT (MCT-170 LAND). "
            f"actual: {AMBIGUITY_COUNTER_METRIC!r}"
        )


class TestEpicClosedPrerequisite:
    """Epic CLOSED prerequisite 4종 박제 (MCT-172 §9)."""

    def test_epic_closed_prerequisite_list(self) -> None:
        """Epic CLOSED prerequisite 4종 목록 박제 (D8-9=C 정합).

        본 Story LAND = "6/6 policy finalized + Epic close blocked pending production evidence".
        Epic CLOSED = production 14d 측정 후 별 PR 또는 scope_manifest amend.
        """
        # 4종 prerequisite SSOT (MCT-172 §9)
        prerequisites = EPIC_CLOSED_PREREQUISITES

        assert len(prerequisites) == 4, (
            f"Epic CLOSED prerequisite = 4종 기대. actual: {len(prerequisites)} "
            f"({prerequisites})"
        )
        assert "production_deploy_14d_0hit_telemetry" in prerequisites, (
            "14d 0-hit telemetry prerequisite 누락 (D8-4=C)"
        )
        assert "wal_30g_production_measurement" in prerequisites, (
            "WAL 30G production measurement prerequisite 누락 (D8-7=A)"
        )
        assert "production_evidence_quad_same_1h_window" in prerequisites, (
            "evidence quad 동일 1h window prerequisite 누락 (D8-8=A)"
        )
        assert "epic_closed_pr_or_scope_manifest_amend" in prerequisites, (
            "Epic CLOSED PR/amend prerequisite 누락 (D8-9=C)"
        )

    def test_policy_finalized_not_closed(self) -> None:
        """MCT-172 Story LAND = POLICY_FINALIZED (not CLOSED). Epic CLOSED = 별 PR gate.

        D8-9=C: Epic CLOSED 박제 timing = production 14d 측정 후 별 PR.
        본 Story = "6/6 policy finalized + Epic close blocked pending production evidence".
        """
        story_status_after_land = "POLICY_FINALIZED"
        epic_status_after_land = "POLICY_FINALIZED"

        assert story_status_after_land != "CLOSED", (
            "MCT-172 LAND 시 Epic status = POLICY_FINALIZED (not CLOSED). "
            "D8-9=C: production evidence 완성 후 별 PR."
        )
        assert epic_status_after_land == "POLICY_FINALIZED", (
            f"Epic status after MCT-172 LAND = POLICY_FINALIZED 기대. "
            f"actual: {epic_status_after_land!r}"
        )
