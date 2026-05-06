"""Tests for CLI `mctrader-data status` (MCT-93 X4)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from click.testing import CliRunner

from mctrader_data.cli import main


def _hb_payload(
    *, node_id: str, now: datetime, ws_state: str = "connected",
    last_event: dict[str, str] | None = None,
    metrics: dict | None = None,
) -> dict:
    return {
        "schema_version": "heartbeat.v1",
        "node_id": node_id,
        "collector_run_id": f"{node_id}-test",
        "version": "test-v",
        "started_at": (now - timedelta(seconds=100)).isoformat(),
        "now": now.isoformat(),
        "uptime_seconds": 100,
        "ws_state": ws_state,
        "last_event_ts_per_tier": last_event or {"tick": now.isoformat()},
        "queue_depth": 0,
        "metrics": metrics or {
            "events_per_sec": 10.0, "dup_skip_count": 0,
            "quarantine_count": 0, "ws_reconnect_count": 0,
            "backfill_pending_seconds": 0,
        },
    }


def _write_hb(root: Path, payload: dict) -> None:
    hb_dir = root / "market" / "manifest"
    hb_dir.mkdir(parents=True, exist_ok=True)
    (hb_dir / f"heartbeat-{payload['node_id']}.json").write_text(
        json.dumps(payload), encoding="utf-8",
    )


def test_status_no_heartbeat_files_exit_code_2(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["status", "--root", str(tmp_path)])
    assert result.exit_code == 2
    assert "no heartbeat" in result.output.lower() or "no heartbeat" in (result.stderr or "").lower()


def test_status_one_node_green(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    _write_hb(tmp_path, _hb_payload(node_id="NODE_A", now=now))

    runner = CliRunner()
    result = runner.invoke(main, ["status", "--root", str(tmp_path), "--no-color"])
    assert result.exit_code == 0
    assert "NODE_A" in result.output


def test_status_disconnected_red(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    _write_hb(tmp_path, _hb_payload(node_id="NODE_A", now=now, ws_state="disconnected"))

    runner = CliRunner()
    result = runner.invoke(main, ["status", "--root", str(tmp_path), "--no-color"])
    assert result.exit_code == 2


def test_status_stale_freshness_red(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc) - timedelta(seconds=60)
    _write_hb(tmp_path, _hb_payload(node_id="NODE_A", now=now))

    runner = CliRunner()
    result = runner.invoke(main, ["status", "--root", str(tmp_path), "--no-color"])
    assert result.exit_code == 2


def test_status_stale_freshness_yellow(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc) - timedelta(seconds=15)
    _write_hb(tmp_path, _hb_payload(node_id="NODE_A", now=now))

    runner = CliRunner()
    result = runner.invoke(main, ["status", "--root", str(tmp_path), "--no-color"])
    assert result.exit_code == 1


def test_status_format_json(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    _write_hb(tmp_path, _hb_payload(node_id="NODE_A", now=now))

    runner = CliRunner()
    result = runner.invoke(main, ["status", "--root", str(tmp_path), "--format", "json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "nodes" in payload
    assert payload["nodes"][0]["node_id"] == "NODE_A"
    assert "worst_level" in payload


def test_status_two_nodes_worst_level_red(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    _write_hb(tmp_path, _hb_payload(node_id="NODE_A", now=now))  # green
    _write_hb(tmp_path, _hb_payload(node_id="NODE_B", now=now, ws_state="disconnected"))

    runner = CliRunner()
    result = runner.invoke(main, ["status", "--root", str(tmp_path), "--no-color", "--format", "json"])
    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["worst_level"] == 2
    assert len(payload["nodes"]) == 2


def test_status_lag_red_threshold(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    # last_event_ts 가 400s 이전 (default lag_red_seconds = 300)
    last = (now - timedelta(seconds=400)).isoformat()
    _write_hb(tmp_path, _hb_payload(
        node_id="NODE_A", now=now, last_event={"tick": last},
    ))

    runner = CliRunner()
    result = runner.invoke(main, ["status", "--root", str(tmp_path), "--no-color"])
    assert result.exit_code == 2


def test_status_lag_yellow_threshold(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    last = (now - timedelta(seconds=80)).isoformat()  # > lag_yellow=60, < lag_red=300
    _write_hb(tmp_path, _hb_payload(
        node_id="NODE_A", now=now, last_event={"tick": last},
    ))

    runner = CliRunner()
    result = runner.invoke(main, ["status", "--root", str(tmp_path), "--no-color"])
    assert result.exit_code == 1


def test_status_malformed_heartbeat_json(tmp_path: Path) -> None:
    """Malformed JSON → reported as red (worst_level 2)."""
    hb_dir = tmp_path / "market" / "manifest"
    hb_dir.mkdir(parents=True, exist_ok=True)
    (hb_dir / "heartbeat-NODE_BAD.json").write_text("{not valid json", encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(main, ["status", "--root", str(tmp_path), "--no-color"])
    assert result.exit_code == 2
