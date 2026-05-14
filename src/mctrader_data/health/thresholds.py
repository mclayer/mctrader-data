"""Health threshold definitions — MCT-165 D5=C.

정적 ±20% threshold (volume) + collector lag SLO (60s) + rolling baseline stub.
Rolling baseline = NotImplementedError (ADR-028 Reserved — follow-up PR).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ThresholdResult:
    actual: float
    expected: float | None
    verdict: Literal["PASS", "WARN", "FAIL"]
    detail: str


def static_volume_threshold(
    actual: float,
    expected: float,
    tol: float = 0.20,
) -> ThresholdResult:
    """정적 ±tol 임계값 판정 — MCT-165 D5=C.

    Args:
        actual: 실측값 (GiB 또는 bytes 단위 일관 사용).
        expected: 기대값.
        tol: 허용 편차 비율 (default 0.20 = ±20%).

    Returns:
        ThresholdResult with verdict PASS/FAIL.
    """
    deviation = float("inf") if expected == 0 else abs(actual - expected) / expected
    if deviation <= tol:
        return ThresholdResult(
            actual=actual,
            expected=expected,
            verdict="PASS",
            detail=f"deviation={deviation:.2%} <= ±{tol:.0%}",
        )
    return ThresholdResult(
        actual=actual,
        expected=expected,
        verdict="FAIL",
        detail=f"deviation={deviation:.2%} > ±{tol:.0%}",
    )


def static_lag_threshold(
    actual_seconds: float,
    slo_seconds: float = 60.0,
) -> ThresholdResult:
    """Collector WAL lag SLO 판정 — MCT-165 D5=C.

    Args:
        actual_seconds: 실측 lag (seconds).
        slo_seconds: SLO 상한 (default 60s).

    Returns:
        ThresholdResult with verdict PASS/FAIL.
    """
    verdict: Literal["PASS", "FAIL"] = "PASS" if actual_seconds <= slo_seconds else "FAIL"
    return ThresholdResult(
        actual=actual_seconds,
        expected=slo_seconds,
        verdict=verdict,
        detail=f"lag={actual_seconds:.0f}s SLO={slo_seconds:.0f}s",
    )


def rolling_threshold(actual: float, window_days: int = 7) -> ThresholdResult:
    """Rolling distribution baseline — MCT-165 D5=C 자리 예약.

    후속 ADR-028 (rolling-baseline-threshold) 진입 전까지 NotImplementedError.
    Exit code 2 contract: caller 가 NotImplementedError → sys.exit(2) 처리.
    """
    raise NotImplementedError(
        "rolling baseline reserved for follow-up ADR — "
        "see docs/adr/ADR-028-rolling-baseline-threshold.md"
    )
