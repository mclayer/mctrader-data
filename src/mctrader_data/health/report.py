"""Health report generation — MCT-165 Task 7 Step 1.

JSON/CSV/markdown 산출물 생성. 4 layer 측정 결과 → HealthReport.
INV-5: 산출물 박제 경로는 caller(CLI)가 결정.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Literal

from mctrader_data.health.thresholds import static_lag_threshold, static_volume_threshold


@dataclass
class LayerResult:
    """단일 layer 측정 결과."""

    name: str
    actual: float | int | None
    expected: float | int | None
    unit: str
    verdict: Literal["PASS", "WARN", "FAIL", "N/A"]
    detail: str


@dataclass
class HealthReport:
    """4-layer health check 보고서.

    Attributes:
        generated_at: ISO 8601 생성 시각.
        window_start: 측정 시작일.
        window_end: 측정 종료일.
        layers: 4 layer 결과 목록.
    """

    generated_at: str
    window_start: date
    window_end: date
    layers: list[LayerResult] = field(default_factory=list)

    @property
    def overall_verdict(self) -> Literal["PASS", "FAIL"]:
        return "FAIL" if any(lr.verdict == "FAIL" for lr in self.layers) else "PASS"

    def to_json(self) -> str:
        """JSON 직렬화."""
        d = {
            "generated_at": self.generated_at,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "overall_verdict": self.overall_verdict,
            "layers": [
                {
                    "layer": lr.name,
                    "actual": lr.actual,
                    "expected": lr.expected,
                    "unit": lr.unit,
                    "verdict": lr.verdict,
                    "detail": lr.detail,
                }
                for lr in self.layers
            ],
        }
        return json.dumps(d, indent=2, ensure_ascii=False)

    def to_csv(self) -> str:
        """CSV 직렬화. header = layer,actual,expected,unit,verdict,detail"""
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=["layer", "actual", "expected", "unit", "verdict", "detail"],
        )
        writer.writeheader()
        for lr in self.layers:
            writer.writerow(
                {
                    "layer": lr.name,
                    "actual": lr.actual,
                    "expected": lr.expected,
                    "unit": lr.unit,
                    "verdict": lr.verdict,
                    "detail": lr.detail,
                }
            )
        return buf.getvalue()

    def to_markdown(self) -> str:
        """Markdown 보고서 생성."""
        lines = [
            "## Health Check Report",
            "",
            f"- **Window**: {self.window_start} → {self.window_end}",
            f"- **Generated**: {self.generated_at}",
            f"- **Overall**: {self.overall_verdict}",
            "",
            "| Layer | Actual | Expected | Unit | Verdict | Detail |",
            "|-------|--------|----------|------|---------|--------|",
        ]
        for lr in self.layers:
            actual = f"{lr.actual:.3f}" if isinstance(lr.actual, float) else str(lr.actual)
            expected = f"{lr.expected:.3f}" if isinstance(lr.expected, float) else str(lr.expected)
            lines.append(
                f"| {lr.name} | {actual} | {expected} | {lr.unit} | {lr.verdict} | {lr.detail} |"
            )
        return "\n".join(lines) + "\n"


def build_report(
    *,
    volume_result,
    gap_result,
    file_count_result,
    lag_result,
    window_start: date,
    window_end: date,
    expected_volume_gib: float = 4.35,
    volume_tol: float = 0.20,
    lag_slo_seconds: float = 60.0,
) -> HealthReport:
    """4 layer 측정 결과 → HealthReport 구성.

    Args:
        volume_result: VolumeResult
        gap_result: GapResult
        file_count_result: FileCountResult
        lag_result: LagResult
        window_start: 측정 시작일.
        window_end: 측정 종료일.
        expected_volume_gib: 예상 volume (GiB).
        volume_tol: volume threshold tolerance.
        lag_slo_seconds: lag SLO (seconds).

    Returns:
        HealthReport.
    """
    generated_at = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")

    # Volume layer
    actual_gib = volume_result.total_bytes / (1024**3)
    vol_threshold = static_volume_threshold(actual_gib, expected_volume_gib, volume_tol)
    volume_layer = LayerResult(
        name="volume",
        actual=round(actual_gib, 3),
        expected=expected_volume_gib,
        unit="GiB",
        verdict=vol_threshold.verdict,
        detail=vol_threshold.detail,
    )

    # Gap layer
    gap_layer = LayerResult(
        name="gap",
        actual=gap_result.missing_count,
        expected=0,
        unit="missing_partitions",
        verdict="PASS" if gap_result.missing_count == 0 else "FAIL",
        detail=f"{gap_result.missing_count} missing of {gap_result.total_expected} expected",
    )

    # File count layer
    fc_layer = LayerResult(
        name="file_count",
        actual=file_count_result.total_files,
        expected=None,
        unit="files",
        verdict="PASS",  # Threshold TBD — presence only for MVP
        detail=f"total={file_count_result.total_files} parquet files",
    )

    # Lag layer
    max_lag = lag_result.max_lag_seconds
    if max_lag is None:
        lag_verdict: Literal["PASS", "WARN", "FAIL", "N/A"] = "N/A"
        lag_detail = "WAL not found — lag measurement unavailable"
        lag_actual: float | None = None
    else:
        lag_thresh = static_lag_threshold(max_lag, lag_slo_seconds)
        lag_verdict = lag_thresh.verdict  # type: ignore[assignment]
        lag_detail = lag_thresh.detail
        lag_actual = round(max_lag, 1)

    lag_layer = LayerResult(
        name="lag",
        actual=lag_actual,
        expected=lag_slo_seconds,
        unit="seconds",
        verdict=lag_verdict,
        detail=lag_detail,
    )

    return HealthReport(
        generated_at=generated_at,
        window_start=window_start,
        window_end=window_end,
        layers=[volume_layer, gap_layer, fc_layer, lag_layer],
    )
