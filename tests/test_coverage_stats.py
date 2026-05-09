from __future__ import annotations

import json
from pathlib import Path

import pytest

from mctrader_data.coverage_stats import CoverageStatsWriter


def test_flush_creates_json_with_correct_schema(tmp_path: Path) -> None:
    writer = CoverageStatsWriter(root=tmp_path, node_id="NODE_A", collector_run_id="NODE_A-test")
    writer.flush()
    out = tmp_path / "market" / "manifest" / "coverage-stats.json"
    assert out.exists()
    data = json.loads(out.read_text())
    assert data["schema_version"] == "coverage_stats.v1"
    assert data["node_id"] == "NODE_A"
    assert data["collector_run_id"] == "NODE_A-test"
    assert data["stats"] == {}
    assert "generated_at" in data
    assert data["flush_interval_seconds"] == 300.0


def test_flush_atomic_on_fsync_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def bad_fsync(fd: int) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("os.fsync", bad_fsync)
    writer = CoverageStatsWriter(root=tmp_path, node_id="NODE_A", collector_run_id="test")
    writer.flush()  # must not raise
    out = tmp_path / "market" / "manifest" / "coverage-stats.json"
    tmp = out.with_suffix(".json.tmp")
    assert not out.exists()   # nothing existed before, last-good = nothing
    assert not tmp.exists()   # tmp cleaned up
