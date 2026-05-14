"""Unit tests for health.report — MCT-165 Task 7 Step 1."""

import csv
import io
import json
from datetime import date

import pytest

from mctrader_data.health.report import (
    HealthReport,
    LayerResult,
    build_report,
)


@pytest.fixture()
def sample_report() -> HealthReport:
    return HealthReport(
        generated_at="2026-05-14T00:00:00Z",
        window_start=date(2026, 5, 9),
        window_end=date(2026, 5, 13),
        layers=[
            LayerResult(
                name="volume",
                actual=2.973,
                expected=4.35,
                unit="GiB",
                verdict="FAIL",
                detail="deviation=31.66% > ±20%",
            ),
            LayerResult(
                name="gap",
                actual=0,
                expected=0,
                unit="missing_partitions",
                verdict="PASS",
                detail="0 missing of 200 expected",
            ),
            LayerResult(
                name="file_count",
                actual=53500,
                expected=None,
                unit="files",
                verdict="PASS",
                detail="50 syms × 4d total",
            ),
            LayerResult(
                name="lag",
                actual=60,
                expected=60,
                unit="seconds",
                verdict="PASS",
                detail="lag=60s SLO=60s",
            ),
        ],
    )


def test_report_overall_verdict_fail_when_any_fail(sample_report: HealthReport):
    assert sample_report.overall_verdict == "FAIL"


def test_report_to_json_serializable(sample_report: HealthReport):
    j = sample_report.to_json()
    parsed = json.loads(j)
    assert parsed["overall_verdict"] == "FAIL"
    assert len(parsed["layers"]) == 4


def test_report_to_csv_has_header(sample_report: HealthReport):
    csv_str = sample_report.to_csv()
    reader = csv.DictReader(io.StringIO(csv_str))
    rows = list(reader)
    assert rows[0]["layer"] == "volume"
    assert rows[0]["verdict"] == "FAIL"


def test_report_to_markdown_contains_table(sample_report: HealthReport):
    md = sample_report.to_markdown()
    assert "| volume |" in md
    assert "FAIL" in md
    assert "## Health Check Report" in md


def test_build_report_constructs_from_results(tmp_path):
    """build_report 헬퍼 함수 — 실제 측정 결과 없이 layer_results dict 입력."""
    from mctrader_data.health.volume import VolumeResult
    from mctrader_data.health.gap import GapResult
    from mctrader_data.health.file_count import FileCountResult
    from mctrader_data.health.lag import LagResult

    vol = VolumeResult(total_bytes=int(2.973 * 1024**3))
    gap = GapResult(missing_count=0, total_expected=200)
    fc = FileCountResult(total_files=53500)
    lag = LagResult(per_exchange={"bithumb": 60.0})

    report = build_report(
        volume_result=vol,
        gap_result=gap,
        file_count_result=fc,
        lag_result=lag,
        window_start=date(2026, 5, 9),
        window_end=date(2026, 5, 13),
        expected_volume_gib=4.35,
        volume_tol=0.20,
        lag_slo_seconds=60.0,
    )
    assert isinstance(report, HealthReport)
    assert len(report.layers) == 4
