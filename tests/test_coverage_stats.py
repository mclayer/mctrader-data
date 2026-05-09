from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mctrader_data.coverage_stats import CoverageStatsWriter, GapEvent


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
    assert not out.exists()
    assert not tmp.exists()


def test_record_event_increments_row_count(tmp_path: Path) -> None:
    writer = CoverageStatsWriter(tmp_path, "NODE_A", "run-1")
    ts = datetime(2026, 5, 9, 12, 0, 0, tzinfo=timezone.utc)
    writer.record_event("KRW-BTC", "tick", ts, file_size_delta=100)
    writer.record_event("KRW-BTC", "tick", ts, file_size_delta=200)
    writer.flush()
    data = json.loads((tmp_path / "market" / "manifest" / "coverage-stats.json").read_text())
    tier = data["stats"]["KRW-BTC"]["tick"]
    assert tier["row_count_today"] == 2
    assert tier["file_size_bytes_today"] == 300
    assert tier["last_event_ts"] == "2026-05-09T12:00:00Z"


def test_record_event_different_symbols_isolated(tmp_path: Path) -> None:
    writer = CoverageStatsWriter(tmp_path, "NODE_A", "run-1")
    ts = datetime(2026, 5, 9, 12, 0, 0, tzinfo=timezone.utc)
    writer.record_event("KRW-BTC", "tick", ts)
    writer.record_event("KRW-ETH", "tick", ts)
    writer.flush()
    data = json.loads((tmp_path / "market" / "manifest" / "coverage-stats.json").read_text())
    assert data["stats"]["KRW-BTC"]["tick"]["row_count_today"] == 1
    assert data["stats"]["KRW-ETH"]["tick"]["row_count_today"] == 1


def test_record_gap_appends_to_gap_events(tmp_path: Path) -> None:
    writer = CoverageStatsWriter(tmp_path, "NODE_A", "run-1")
    gap = GapEvent(
        symbol="KRW-BTC", tier="tick",
        start_ts="2026-05-09T11:00:00Z", end_ts="2026-05-09T11:05:00Z",
        duration_seconds=300.0, cause="UNKNOWN", node_id="NODE_A", ws_reconnect_count=0,
    )
    writer.record_gap(gap)
    writer.flush()
    data = json.loads((tmp_path / "market" / "manifest" / "coverage-stats.json").read_text())
    gaps = data["stats"]["KRW-BTC"]["tick"]["gap_events"]
    assert len(gaps) == 1
    assert gaps[0]["cause"] == "UNKNOWN"
    assert gaps[0]["duration_seconds"] == 300.0


@pytest.mark.asyncio
async def test_run_final_flush_on_cancel(tmp_path: Path) -> None:
    writer = CoverageStatsWriter(tmp_path, "NODE_A", "run-1")
    ts = datetime(2026, 5, 9, 12, 0, 0, tzinfo=timezone.utc)
    writer.record_event("KRW-BTC", "tick", ts)
    task = asyncio.create_task(writer.run())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    out = tmp_path / "market" / "manifest" / "coverage-stats.json"
    assert out.exists()
    data = json.loads(out.read_text())
    assert data["stats"]["KRW-BTC"]["tick"]["row_count_today"] == 1


@pytest.mark.asyncio
async def test_run_flushes_periodically(tmp_path: Path) -> None:
    import unittest.mock

    writer = CoverageStatsWriter(tmp_path, "NODE_A", "run-1")
    writer.FLUSH_INTERVAL_SECONDS = 0.05
    with unittest.mock.patch.object(writer, "flush", wraps=writer.flush) as mock_flush:
        task = asyncio.create_task(writer.run())
        await asyncio.sleep(0.15)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    # At 0.05s interval over 0.15s: ≥2 periodic flushes + 1 cancel flush = ≥3 total
    assert mock_flush.call_count >= 2, f"expected ≥2 flush calls, got {mock_flush.call_count}"
