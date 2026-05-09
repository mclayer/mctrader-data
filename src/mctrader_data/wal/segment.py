# src/mctrader_data/wal/segment.py
"""WAL segment path conventions and scan helpers."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

SEGMENT_SECONDS = 300  # 5 minutes


def segment_index(ts: float, segment_seconds: int = SEGMENT_SECONDS) -> int:
    """Return the 5-minute epoch bucket index for timestamp ts (seconds since epoch)."""
    return int(ts // segment_seconds)


def active_segment_path(
    *,
    root: Path,
    exchange: str,
    channel: str,
    symbol: str,
    date: str,
    start_idx: int,
    node_id: str,
    segment_seconds: int = SEGMENT_SECONDS,
) -> Path:
    start_ts = start_idx * segment_seconds
    dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    ts_str = dt.strftime("%Y%m%dT%H%M%SZ")
    filename = f"segment-{ts_str}-{node_id}.ndjson"
    return root / "wal" / exchange / channel / symbol / date / filename


def seal_path(active: Path) -> Path:
    return Path(str(active) + ".sealed")


def compacted_path(sealed: Path) -> Path:
    return Path(str(sealed) + ".compacted")


def is_active(p: Path) -> bool:
    name = p.name
    return name.endswith(".ndjson") and not name.endswith(".sealed")


def is_sealed(p: Path) -> bool:
    return p.name.endswith(".ndjson.sealed") and not p.name.endswith(".compacted")


def is_compacted(p: Path) -> bool:
    return p.name.endswith(".ndjson.sealed.compacted")


def scan_sealed(root: Path) -> list[Path]:
    """Return all .ndjson.sealed paths under root/wal/ that have no .compacted marker."""
    wal_root = root / "wal"
    if not wal_root.exists():
        return []
    result = []
    for p in sorted(wal_root.rglob("*.ndjson.sealed")):
        if not compacted_path(p).exists():
            result.append(p)
    return result


def parse_node_id_from_segment(sealed: Path) -> str:
    """Extract node_id from segment filename: segment-{ts}-{node_id}.ndjson.sealed"""
    stem = sealed.name  # e.g. segment-20260509T000000Z-NODE_A.ndjson.sealed
    base = stem.replace(".ndjson.sealed", "").replace(".ndjson", "")
    # base = segment-20260509T000000Z-NODE_A
    parts = base.split("-", 2)  # ["segment", "20260509T000000Z", "NODE_A"]
    return parts[2] if len(parts) >= 3 else "DEFAULT"
