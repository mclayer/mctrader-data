# src/mctrader_data/compactor/backfill.py
"""MCT-173 backfill module — frozen WAL iterator + BackfillManifest writer.

Design decisions implemented:
- D3=A: PIT (point-in-time) snapshot — iter_frozen_segments() snapshots sealed list
  once at call time to avoid concurrent ingester race (INV-1).
- D4=A: .compacted sentinel skip — same as normal compaction path (ADR-017 §D2,
  Kafka KIP-280 sentinel idempotency). Re-running backfill on already-processed
  segments is a no-op (INV-2).
- D5=B: BackfillManifest frontmatter YAML — partial boundary 박제 (INV-4).
  Persisted alongside backfill execution for audit.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


# ─── frozen segment iterator ─────────────────────────────────────────────────


def iter_frozen_segments(
    wal_root: Path,
    exchange: str,
    channel: str,
) -> list[Path]:
    """Return a PIT snapshot of uncompacted sealed WAL segments for (exchange, channel).

    D3=A: Snapshot-at-call — collects all sealed paths into a list before returning.
    Any new segments arriving after this call are excluded from the backfill batch,
    preventing concurrent ingester race conditions (INV-1).

    D4=A: Segments with a `.compacted` sentinel are excluded (INV-2 idempotency).
    A segment that was already compacted by the normal compaction path or by a
    previous backfill run is silently skipped.

    Args:
        wal_root: Root of the WAL tree (e.g. /data/wal)
        exchange: Exchange name (e.g. "upbit")
        channel: Channel name (e.g. "orderbooksnapshot")

    Returns:
        Sorted list of Path objects for sealed segments without .compacted marker.
        The list is a snapshot — mutations to the filesystem after this call do NOT
        affect the returned list.
    """
    channel_dir = wal_root / exchange / channel
    if not channel_dir.exists():
        log.warning("[backfill] iter_frozen_segments: channel dir not found: %s", channel_dir)
        return []

    results: list[Path] = []
    for sym_dir in sorted(channel_dir.iterdir()):
        if not sym_dir.is_dir():
            continue
        for date_dir in sorted(sym_dir.iterdir()):
            if not date_dir.is_dir():
                continue
            for f in sorted(date_dir.iterdir()):
                if not f.is_file():
                    continue
                if not f.name.endswith(".ndjson.sealed"):
                    continue
                # D4=A: skip if .compacted sentinel exists
                compacted_marker = Path(str(f) + ".compacted")
                if compacted_marker.exists():
                    continue
                results.append(f)

    log.info(
        "[backfill] iter_frozen_segments exchange=%s channel=%s found=%d segments (PIT snapshot)",
        exchange,
        channel,
        len(results),
    )
    return results


# ─── BackfillManifest ────────────────────────────────────────────────────────


@dataclass
class BackfillManifest:
    """Frontmatter YAML manifest for a backfill run (D5=B, INV-4).

    Persisted to a .yaml file alongside the backfill execution as an audit trail.
    Records the frozen WAL date range and partial boundary symbols for downstream
    consumers (backtest readers) to understand the historical coverage boundary.
    """

    exchange: str
    channel: str
    date_range_start: str  # earliest WAL date processed (e.g. "2026-05-13")
    date_range_end: str  # latest WAL date processed (e.g. "2026-05-14")
    segment_count: int  # total sealed segments targeted
    segments_processed: int  # segments that produced L1 parquets
    segments_skipped: int  # segments skipped due to .compacted sentinel
    l1_parquets_created: int  # total L1 parquet files written
    partial_boundary_symbols: list[str] = field(default_factory=list)
    # "symbol/date" pairs where WAL coverage is incomplete (e.g. onboarding day)
    created_at: str = ""  # ISO 8601 UTC
    mct_story: str = "MCT-173"
    mct166_land_date: str = "2026-05-14"  # MCT-166 fix LAND date (boundary reference)
    inv1_source_wal_immutable: bool = True  # Source WAL not modified
    inv2_idempotency: str = ".compacted sentinel (ADR-017 §D2)"
    inv3_schema_compat: str = "_ob_snapshot_dicts_to_arrow() reused (MCT-166 path B)"

    def write_manifest(self, path: Path) -> None:
        """Write YAML frontmatter manifest to *path*.

        Format:
            ---
            <YAML key: value pairs>
            ---
        """
        import yaml

        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()

        data = asdict(self)
        yaml_body = yaml.dump(data, allow_unicode=True, default_flow_style=False, sort_keys=True)
        content = f"---\n{yaml_body}---\n"

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        log.info("[backfill] manifest written to %s", path)
