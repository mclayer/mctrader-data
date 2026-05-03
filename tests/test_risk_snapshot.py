"""RiskPolicy snapshot persistence tests (MCT-26)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mctrader_data.risk_snapshot import (
    read_risk_policy_snapshot,
    write_risk_policy_snapshot,
)


def _snapshot_dict(run_id: str = "run-001") -> dict[str, str | None]:
    return {
        "run_id": run_id,
        "policy_version": "mct-25-v1",
        "policy_hash": "abc123",
        "canonical_json": '{"max_daily_loss_hard_pct":"0.03"}',
        "locked_at_utc": "2026-05-03T12:00:00Z",
        "amendment_from": None,
    }


def test_write_creates_partition_path(tmp_path: Path) -> None:
    target = write_risk_policy_snapshot(_snapshot_dict("run-A"), root=tmp_path)
    assert target.exists()
    assert target.name == "policy.json"
    assert target.parent.name == "run_id=run-A"
    assert target.parent.parent.name == "risk_policy_snapshot"


def test_round_trip_preserves_dict(tmp_path: Path) -> None:
    payload = _snapshot_dict("run-B")
    write_risk_policy_snapshot(payload, root=tmp_path)
    restored = read_risk_policy_snapshot(root=tmp_path, run_id="run-B")
    assert restored == payload


def test_write_uses_sorted_keys(tmp_path: Path) -> None:
    write_risk_policy_snapshot(_snapshot_dict("run-C"), root=tmp_path)
    target = tmp_path / "risk_policy_snapshot" / "run_id=run-C" / "policy.json"
    raw = target.read_text(encoding="utf-8")
    data_keys = list(json.loads(raw).keys())
    assert data_keys == sorted(data_keys)


def test_write_rejects_missing_run_id(tmp_path: Path) -> None:
    bad = _snapshot_dict()
    bad["run_id"] = ""
    with pytest.raises(ValueError):
        write_risk_policy_snapshot(bad, root=tmp_path)


def test_write_rejects_non_string_run_id(tmp_path: Path) -> None:
    bad: dict[str, object] = dict(_snapshot_dict())
    bad["run_id"] = 12345
    with pytest.raises(ValueError):
        write_risk_policy_snapshot(bad, root=tmp_path)  # type: ignore[arg-type]
