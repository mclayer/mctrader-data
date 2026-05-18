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


def _strip_segment_suffixes(name: str) -> str:
    """Strip WAL segment 파일 suffix (longest-first — substring 부분소비 차단).

    WAL 3-state closure: .ndjson (active) -> .ndjson.sealed -> .ndjson.sealed.compacted.
    suffix-strip 단일 책임 — split/validate/error 는 caller 책임 (error contract 비대칭 의도).
    """
    for suffix in (".ndjson.sealed.compacted", ".ndjson.sealed", ".ndjson"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def parse_node_id_from_segment(sealed: Path) -> str:
    """Extract node_id from segment filename: segment-{ts}-{node_id}.ndjson[.sealed[.compacted]]

    suffix-strip = _strip_segment_suffixes SSOT (longest-first). split/error 는 본 함수
    책임 — len(parts)<3 시 "DEFAULT" lenient fallback 보존 (parse_ts_from_segment 의
    ValueError strict contract 와 의도적 비대칭, zero-regression — spec §3.3 / Researcher U1).
    """
    base = _strip_segment_suffixes(sealed.name)
    parts = base.split("-", 2)
    return parts[2] if len(parts) >= 3 else "DEFAULT"


def parse_ts_from_segment(sealed: Path) -> str:
    """Extract epoch ts from segment filename: segment-{YYYYMMDDTHHMMSSZ}-{node_id}.ndjson[.sealed[.compacted]]

    Symmetric with parse_node_id_from_segment — ts 위치 = parts[1].
    Returns 'YYYYMMDDTHHMMSSZ' (사전 정렬 가능 ISO 형식).

    ADR-009 §D2 Amendment N — L1 dual filename pattern 의 ts source.
    """
    stem = sealed.name
    base = (
        stem
        .replace(".ndjson.sealed.compacted", "")
        .replace(".ndjson.sealed", "")
        .replace(".ndjson", "")
    )
    parts = base.split("-", 2)
    if len(parts) < 3 or parts[0] != "segment":
        raise ValueError(
            f"Unexpected segment filename: {sealed.name!r}. "
            f"Expected 'segment-<YYYYMMDDTHHMMSSZ>-<node_id>.ndjson[.sealed[.compacted]]'."
        )
    return parts[1]
