"""Collector run manifest persistence (MCT-65).

Per Codex F-21 push-back: collector run 마다 selected symbol list + run metadata 를
``<root>/market/manifest/run-{collector_run_id}.json`` 에 persist. MCT-66 reconstruction
의 coverage report 가 본 manifest 를 의무 참조 (replay reproducibility).
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

MANIFEST_SCHEMA_VERSION = "collector_manifest.v1"


class CollectorManifest(BaseModel):
    """Per-collector-run metadata. Pydantic v2 strict, file = JSON."""

    model_config = ConfigDict(strict=True, extra="forbid")

    schema_version: Literal["collector_manifest.v1"] = "collector_manifest.v1"
    collector_run_id: str
    started_at_utc: datetime
    exchange: str
    selected_symbols: list[str]
    channels: list[str]
    selection_method: Literal["explicit", "top_n_volume"]
    top_n: int | None = Field(default=None, description="present iff selection_method=top_n_volume")


def derive_collector_run_id(
    *,
    started_at_utc: datetime,
    exchange: str,
    selected_symbols: list[str],
) -> str:
    """Deterministic collector_run_id = sha256(exchange|sorted_symbols|started_iso)[:16]."""
    sorted_syms = "|".join(sorted(selected_symbols))
    payload = f"{exchange}|{sorted_syms}|{started_at_utc.astimezone(timezone.utc).isoformat()}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def manifest_path(root: Path, collector_run_id: str) -> Path:
    """``<root>/market/manifest/run-{collector_run_id}.json``"""
    return root / "market" / "manifest" / f"run-{collector_run_id}.json"


def write_manifest(root: Path, manifest: CollectorManifest) -> Path:
    """Atomically write manifest JSON. Returns final path.

    Idempotent — same content overwrite OK (same file path = same collector_run_id).
    """
    target = manifest_path(root, manifest.collector_run_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = manifest.model_dump_json(indent=2)
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(target)
    return target


def read_manifest(root: Path, collector_run_id: str) -> CollectorManifest:
    """Read + validate manifest. Raises FileNotFoundError if absent."""
    target = manifest_path(root, collector_run_id)
    return CollectorManifest.model_validate_json(target.read_text(encoding="utf-8"))


def list_manifests(root: Path) -> list[CollectorManifest]:
    """List all manifests under ``<root>/market/manifest/``. Returns sorted by started_at_utc."""
    base = root / "market" / "manifest"
    if not base.exists():
        return []
    out: list[CollectorManifest] = []
    for p in sorted(base.glob("run-*.json")):
        try:
            out.append(CollectorManifest.model_validate_json(p.read_text(encoding="utf-8")))
        except Exception:
            continue
    out.sort(key=lambda m: m.started_at_utc)
    return out
