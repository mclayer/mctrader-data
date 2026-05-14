#!/usr/bin/env python3
"""MCT-173 Phase 2.1 entry scan — D2=C/D9=C 실측.

PURPOSE
-------
frozen WAL path 실측 + freeze flag 상태 + pre-existing L1 inventory +
partial WAL date boundary 를 audit 한다.

USAGE
-----
# Production scan
python scripts/backfill_entry_scan.py \
    --root /var/lib/mctrader/data \
    --exchange upbit \
    --channel orderbooksnapshot \
    --output-json docs/audit/MCT-173-entry-scan.md

python scripts/backfill_entry_scan.py \
    --root /var/lib/mctrader/data \
    --exchange upbit \
    --channel orderbooksnapshot

DESIGN DECISIONS
----------------
- D2=C: Phase 2 entry 실측 결정 — frozen WAL path + freeze flag 상태 박제
- D9=C: pre-existing L1 inventory 실측 후 처리 정책 결정
  - pre-existing L1 이 존재하는 segment → idempotency (D4=A sentinel skip)
  - 신규 sealed (uncompacted) segment → backfill 대상
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import stat
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class SegmentInventory:
    exchange: str
    channel: str
    wal_root: str
    scan_at: str
    # WAL state
    total_sealed: int = 0
    total_compacted: int = 0  # .sealed.compacted sentinel
    total_active: int = 0
    uncompacted_sealed: int = 0  # backfill 대상
    # Freeze flag state (chmod basis, D2=C)
    sealed_readonly: int = 0
    sealed_writable: int = 0
    freeze_executed: bool = False  # True if any sealed segment is readonly
    # Partial WAL boundary (D9=C)
    dates_in_wal: list[str] = field(default_factory=list)
    dates_in_l1: list[str] = field(default_factory=list)
    partial_boundary_symbols: list[str] = field(default_factory=list)  # symbols with < full day coverage
    # WAL line counts per (symbol, date)
    wal_line_counts: dict[str, int] = field(default_factory=dict)  # "symbol/date" -> line count
    # L1 parquet counts per (symbol, date)
    l1_parquet_counts: dict[str, int] = field(default_factory=dict)  # "symbol/date" -> parquet count
    # Decision outcomes
    d2_decision: str = ""  # D2=C: frozen WAL path confirmed
    d9_decision: str = ""  # D9=C: pre-existing L1 处理 정책


def scan_wal_inventory(
    root: Path,
    exchange: str,
    channel: str,
) -> SegmentInventory:
    """Scan WAL segments and L1 parquet to produce entry inventory.

    Returns SegmentInventory with all counts and boundary info.
    """
    inv = SegmentInventory(
        exchange=exchange,
        channel=channel,
        wal_root=str(root / "wal" / exchange / channel),
        scan_at=datetime.now(timezone.utc).isoformat(),
    )

    wal_channel_dir = root / "wal" / exchange / channel
    if not wal_channel_dir.exists():
        log.error("[scan] WAL channel dir not found: %s", wal_channel_dir)
        return inv

    log.info("[scan] WAL dir: %s", wal_channel_dir)

    # === WAL scan ===
    dates_in_wal: set[str] = set()
    wal_line_counts: dict[str, int] = {}
    # per (symbol, date) sealed/compacted counts
    sym_date_sealed: dict[str, int] = defaultdict(int)

    for sym_dir in sorted(wal_channel_dir.iterdir()):
        if not sym_dir.is_dir():
            continue
        sym = sym_dir.name
        for date_dir in sorted(sym_dir.iterdir()):
            if not date_dir.is_dir():
                continue
            dt = date_dir.name
            dates_in_wal.add(dt)

            for f in date_dir.iterdir():
                if not f.is_file():
                    continue
                name = f.name
                if name.endswith(".ndjson.sealed.compacted"):
                    inv.total_compacted += 1
                    # Also count as sealed
                    inv.total_sealed += 1
                elif name.endswith(".ndjson.sealed"):
                    inv.total_sealed += 1
                    inv.uncompacted_sealed += 1
                    sym_date_sealed[f"{sym}/{dt}"] += 1
                    # Check freeze mode
                    mode = os.stat(f).st_mode
                    if mode & stat.S_IWUSR:
                        inv.sealed_writable += 1
                    else:
                        inv.sealed_readonly += 1
                    # Count lines (WAL records)
                    key = f"{sym}/{dt}"
                    try:
                        with open(f, encoding="utf-8") as fh:
                            lines = sum(1 for line in fh if line.strip())
                        wal_line_counts[key] = wal_line_counts.get(key, 0) + lines
                    except OSError as e:
                        log.warning("[scan] cannot read %s: %s", f, e)
                elif name.endswith(".ndjson"):
                    inv.total_active += 1

    inv.freeze_executed = inv.sealed_readonly > 0
    inv.dates_in_wal = sorted(dates_in_wal)
    inv.wal_line_counts = dict(sorted(wal_line_counts.items()))

    # Detect partial boundary symbols (< 48 segments for a full day)
    partial_syms: set[str] = set()
    for key, count in sym_date_sealed.items():
        sym, dt = key.split("/", 1)
        if count < 10:  # threshold: very few segments = partial boundary
            partial_syms.add(f"{sym}/{dt}")
    inv.partial_boundary_symbols = sorted(partial_syms)

    # D2=C decision
    inv.d2_decision = (
        f"WAL path confirmed: {inv.wal_root} | "
        f"freeze_executed={inv.freeze_executed} (sealed_readonly={inv.sealed_readonly}, "
        f"sealed_writable={inv.sealed_writable}) | "
        f"uncompacted_sealed={inv.uncompacted_sealed} segments = backfill 대상"
    )

    # === L1 scan ===
    l1_base = (
        root
        / "market"
        / channel
        / "schema_version=orderbook_snapshot.v1"
        / "tier=L1"
        / f"exchange={exchange}"
    )
    if channel == "transaction":
        # tick schema has different version
        l1_base = (
            root
            / "market"
            / channel
            / "schema_version=tick.v1"
            / "tier=L1"
            / f"exchange={exchange}"
        )

    dates_in_l1: set[str] = set()
    l1_parquet_counts: dict[str, int] = {}

    if l1_base.exists():
        for sym_dir in sorted(l1_base.iterdir()):
            if not sym_dir.is_dir():
                continue
            sym = sym_dir.name.split("=", 1)[-1]  # strip "symbol=" prefix
            for date_dir in sorted(sym_dir.iterdir()):
                if not date_dir.is_dir():
                    continue
                dt = date_dir.name.split("=", 1)[-1]  # strip "date=" prefix
                parquets = list(date_dir.rglob("*.parquet"))
                if parquets:
                    dates_in_l1.add(dt)
                    key = f"{sym}/{dt}"
                    l1_parquet_counts[key] = l1_parquet_counts.get(key, 0) + len(parquets)
    else:
        log.warning("[scan] L1 dir not found: %s", l1_base)

    inv.dates_in_l1 = sorted(dates_in_l1)
    inv.l1_parquet_counts = dict(sorted(l1_parquet_counts.items()))

    # D9=C decision: pre-existing L1 처리 정책
    if dates_in_l1:
        pre_existing_dates = sorted(set(inv.dates_in_wal) & set(inv.dates_in_l1))
        new_dates = sorted(set(inv.dates_in_wal) - set(inv.dates_in_l1))
        inv.d9_decision = (
            f"pre-existing L1 dates: {pre_existing_dates} | "
            f"new dates (no L1): {new_dates} | "
            f"policy: D4=A sentinel (.compacted) idempotency — "
            f"segments already compacted → skip, uncompacted sealed → backfill"
        )
    else:
        inv.d9_decision = (
            "No pre-existing L1 found | "
            "policy: all uncompacted sealed segments → backfill 대상"
        )

    log.info("[scan] complete: uncompacted_sealed=%d dates_wal=%s dates_l1=%s",
             inv.uncompacted_sealed, inv.dates_in_wal, inv.dates_in_l1)
    log.info("[scan] D2=C: %s", inv.d2_decision)
    log.info("[scan] D9=C: %s", inv.d9_decision)

    return inv


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MCT-173 Phase 2.1 entry scan — D2=C/D9=C WAL inventory"
    )
    parser.add_argument("--root", required=True, help="Data root (e.g. /var/lib/mctrader/data)")
    parser.add_argument("--exchange", default="upbit")
    parser.add_argument("--channel", default="orderbooksnapshot")
    parser.add_argument("--output-json", help="Write JSON result to this path")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    root = Path(args.root)
    if not root.exists():
        log.error("[scan] root not found: %s", root)
        return 1

    inv = scan_wal_inventory(root=root, exchange=args.exchange, channel=args.channel)

    output = asdict(inv)
    output_json = json.dumps(output, indent=2, ensure_ascii=False)

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output_json, encoding="utf-8")
        log.info("[scan] result written to %s", out_path)
    else:
        print(output_json)

    return 0


if __name__ == "__main__":
    sys.exit(main())
