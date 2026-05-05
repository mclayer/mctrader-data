"""Heartbeat writer tests — atomic write + freshness + shutdown + edge cases.

Per MCT-91 Phase 2 plan, Task 1. 6 test classes covering:
- Atomic write (temp -> fsync -> rename)
- Schema v1 (11 top-level + 5 nested metrics)
- Cross-host read (two writers, file separation)
- Shutdown race (CancelledError + final flush)
- Disk full (last-good preserved on OSError)
- Schema mismatch warning (consumer best-effort parse)
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mctrader_data.heartbeat import (
    HEARTBEAT_SCHEMA_VERSION,
    HeartbeatWriter,
    read_heartbeat,
)


class TestHeartbeatAtomicWrite:
    @pytest.mark.asyncio
    async def test_atomic_write_temp_then_rename(self, tmp_path: Path) -> None:
        writer = HeartbeatWriter(root=tmp_path, node_id="NODE_A", interval_seconds=0.1)
        await writer.write_once()
        main = tmp_path / "market" / "manifest" / "heartbeat-NODE_A.json"
        temp = tmp_path / "market" / "manifest" / "heartbeat-NODE_A.json.tmp"
        assert main.exists()
        assert not temp.exists()
        data = json.loads(main.read_text(encoding="utf-8"))
        assert data["schema_version"] == HEARTBEAT_SCHEMA_VERSION
        assert data["node_id"] == "NODE_A"


class TestHeartbeatSchemaV1:
    @pytest.mark.asyncio
    async def test_schema_v1_top_level_11_field(self, tmp_path: Path) -> None:
        writer = HeartbeatWriter(
            root=tmp_path, node_id="NODE_B", interval_seconds=0.1, version="abc1234"
        )
        writer.set_collector_run_id("NODE_B-20260505T223456Z")
        await writer.write_once()
        path = tmp_path / "market" / "manifest" / "heartbeat-NODE_B.json"
        data = json.loads(path.read_text(encoding="utf-8"))

        top_level = [
            "schema_version", "node_id", "collector_run_id", "version", "started_at",
            "now", "uptime_seconds", "ws_state", "last_event_ts_per_tier",
            "queue_depth", "metrics",
        ]
        for key in top_level:
            assert key in data, f"missing top-level field: {key}"
        assert len(top_level) == 11

        metrics_nested = [
            "events_per_sec", "dup_skip_count", "quarantine_count",
            "ws_reconnect_count", "backfill_pending_seconds",
        ]
        for key in metrics_nested:
            assert key in data["metrics"], f"missing metrics nested field: {key}"
        assert len(metrics_nested) == 5

    @pytest.mark.asyncio
    async def test_ws_state_validation(self, tmp_path: Path) -> None:
        writer = HeartbeatWriter(root=tmp_path, node_id="NODE_A", interval_seconds=0.1)
        writer.ws_state = "reconnecting"
        await writer.write_once()
        data = json.loads(
            (tmp_path / "market" / "manifest" / "heartbeat-NODE_A.json").read_text()
        )
        assert data["ws_state"] == "reconnecting"

        with pytest.raises(ValueError, match="invalid ws_state"):
            writer.ws_state = "BOGUS"


class TestHeartbeatCrossHost:
    @pytest.mark.asyncio
    async def test_two_writers_different_node_ids(self, tmp_path: Path) -> None:
        wa = HeartbeatWriter(root=tmp_path, node_id="NODE_A", interval_seconds=0.1)
        wb = HeartbeatWriter(root=tmp_path, node_id="NODE_B", interval_seconds=0.1)
        await wa.write_once()
        await wb.write_once()
        manifest_dir = tmp_path / "market" / "manifest"
        files = sorted(p.name for p in manifest_dir.glob("heartbeat-*.json"))
        assert files == ["heartbeat-NODE_A.json", "heartbeat-NODE_B.json"]

    def test_consumer_reads_other_node_artifact(self, tmp_path: Path) -> None:
        manifest_dir = tmp_path / "market" / "manifest"
        manifest_dir.mkdir(parents=True)
        producer_data = {
            "schema_version": "heartbeat.v1",
            "node_id": "NODE_A",
            "now": datetime.now(timezone.utc).isoformat(),
        }
        (manifest_dir / "heartbeat-NODE_A.json").write_text(
            json.dumps(producer_data), encoding="utf-8"
        )
        data = read_heartbeat(tmp_path, "NODE_A")
        assert data["schema_version"] == "heartbeat.v1"
        assert data["node_id"] == "NODE_A"


class TestHeartbeatShutdownRace:
    @pytest.mark.asyncio
    async def test_cancel_triggers_final_flush(self, tmp_path: Path) -> None:
        writer = HeartbeatWriter(root=tmp_path, node_id="NODE_A", interval_seconds=10.0)
        task = asyncio.create_task(writer.run())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        main = tmp_path / "market" / "manifest" / "heartbeat-NODE_A.json"
        assert main.exists(), "final flush 미작동 — heartbeat task shutdown race"


class TestHeartbeatErrorHandling:
    @pytest.mark.asyncio
    async def test_disk_full_keeps_last_good(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        writer = HeartbeatWriter(root=tmp_path, node_id="NODE_A", interval_seconds=0.1)
        await writer.write_once()
        main = tmp_path / "market" / "manifest" / "heartbeat-NODE_A.json"
        last_good = main.read_text(encoding="utf-8")

        def fake_fsync_fail(*_args: object, **_kwargs: object) -> None:
            raise OSError(28, "No space left on device")

        monkeypatch.setattr("os.fsync", fake_fsync_fail)
        await writer.write_once()
        assert main.read_text(encoding="utf-8") == last_good
        temp = tmp_path / "market" / "manifest" / "heartbeat-NODE_A.json.tmp"
        assert not temp.exists(), "temp file 잔존 — cleanup 실패"
        assert writer._write_failure_count == 1


class TestHeartbeatSchemaMismatch:
    def test_consumer_warns_on_v2_artifact(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        manifest_dir = tmp_path / "market" / "manifest"
        manifest_dir.mkdir(parents=True)
        v2_data = {
            "schema_version": "heartbeat.v2",
            "node_id": "NODE_A",
            "now": "2026-05-05T22:34:56+00:00",
            "extra_v2_field": "future-only",
        }
        (manifest_dir / "heartbeat-NODE_A.json").write_text(
            json.dumps(v2_data), encoding="utf-8"
        )

        with caplog.at_level(logging.WARNING, logger="mctrader_data.heartbeat"):
            data = read_heartbeat(tmp_path, "NODE_A")
        assert data["schema_version"] == "heartbeat.v2"
        assert data["extra_v2_field"] == "future-only"
        assert any(
            "schema_version mismatch" in rec.message for rec in caplog.records
        ), "schema mismatch warning missing"
