"""Lineage metadata sidecar (`_lineage.json`) per snapshot directory."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def _isoformat(dt: datetime) -> str:
    return dt.astimezone(tz=dt.tzinfo).isoformat().replace("+00:00", "Z")


def write_lineage(
    *,
    partition_dir: Path,
    snapshot_id: str,
    exchange: str,
    endpoint: str,
    request_params_hash: str,
    fetched_at_utc: datetime,
    response_hash: str,
    adapter_name: str,
    adapter_version: str,
    node_id: str | None = None,
) -> Path:
    """Write a per-snapshot ``_lineage.json`` sidecar.

    File path: ``{partition_dir}/_lineage_{snapshot_id}.json`` so multiple snapshots in the
    same partition coexist without overwrite.

    MCT-91 — ``node_id`` optional kwarg. 명시 시 lineage payload 에 ``node_id`` field 추가.
    legacy (None) 는 기존 payload 형식 (backward compat).
    """
    record: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "exchange": exchange,
        "endpoint": endpoint,
        "request_params_hash": request_params_hash,
        "fetched_at_utc": _isoformat(fetched_at_utc),
        "response_hash": response_hash,
        "adapter_name": adapter_name,
        "adapter_version": adapter_version,
    }
    if node_id is not None:
        record["node_id"] = node_id
    partition_dir.mkdir(parents=True, exist_ok=True)
    target = partition_dir / f"_lineage_{snapshot_id}.json"
    with target.open("w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)
    return target


def read_lineage(path: Path) -> dict[str, Any]:
    """Load a lineage sidecar JSON."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)
