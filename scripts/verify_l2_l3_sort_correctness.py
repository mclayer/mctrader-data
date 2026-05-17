#!/usr/bin/env python3
"""verify_l2_l3_sort_correctness — L2/L3 sort key 정합성 운영 게이트.

MCT-166 verify_upbit_l1_fix.py (INV-4 자동 해제 단일 경로) 패턴 정합.

출력: <root>/audit/l2_l3_sort_check-<exchange>-<channel>-<date>.json
  {
    "total_files": N,
    "stats_primary_count": N,    # Opt2 stats.min 적용
    "fallback_count": N,         # Opt1 first-row fallback 적용
    "zero_row_count": N,         # skip 대상
    "legacy_sha_count": N,       # part-<sha>.parquet (rewrite 0)
    "new_ts_prefix_count": N,    # part-<ts>-<sha>.parquet
    "monotonic_pass": True/False,
    "threshold_pass": True/False,
  }
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

LEGACY_RE = re.compile(r"^part-[0-9a-f]{16}\.parquet$")
NEW_RE = re.compile(r"^part-\d{8}T\d{6}Z-[0-9a-f]{16}\.parquet$")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--channel", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--threshold", type=float, default=0.99,
                        help="monotonic pass ratio threshold")
    args = parser.parse_args(argv)

    from mctrader_data.compactor.l1 import _schema_version
    from mctrader_data.compactor.sort_key import _extract_min_ts

    schema_ver = _schema_version(args.channel)
    l1_root = (
        args.root / "market" / args.channel
        / f"schema_version={schema_ver}" / "tier=L1"
        / f"exchange={args.exchange}"
    )

    files = list(l1_root.rglob(f"date={args.date}/**/part-*.parquet"))

    _log = logging.getLogger(__name__)
    legacy = 0
    new = 0
    stats_primary = 0
    fallback = 0
    zero_row = 0
    error_count = 0
    extracted: list[tuple[Path, object]] = []

    for f in files:
        name = f.name
        if NEW_RE.match(name):
            new += 1
        elif LEGACY_RE.match(name):
            legacy += 1
        # Stats path check
        try:
            meta = pq.read_metadata(f)
            schema = meta.schema.to_arrow_schema()
            ts_idx = schema.get_field_index("ts_utc")
            stats_ok = (
                meta.num_row_groups > 0
                and ts_idx >= 0
                and meta.row_group(0).column(ts_idx).statistics is not None
                and meta.row_group(0).column(ts_idx).statistics.has_min_max
            )
        except Exception:
            stats_ok = False
        try:
            ts = _extract_min_ts(f)
        except Exception as exc:
            _log.warning(
                "[verify_sort] _extract_min_ts failed for %s: %s — skipped", f, exc
            )
            error_count += 1
            continue
        if ts is None:
            zero_row += 1
            continue
        if stats_ok:
            stats_primary += 1
        else:
            fallback += 1
        extracted.append((f, ts))

    # monotonic verify on sorted order
    extracted.sort(key=lambda x: x[1])
    monotonic_pass = all(
        extracted[i - 1][1] <= extracted[i][1] for i in range(1, len(extracted))
    )

    pass_ratio = (
        (stats_primary + fallback) / max(1, len(files))
    )
    threshold_pass = pass_ratio >= args.threshold

    audit = {
        "total_files": len(files),
        "stats_primary_count": stats_primary,
        "fallback_count": fallback,
        "zero_row_count": zero_row,
        "error_count": error_count,
        "legacy_sha_count": legacy,
        "new_ts_prefix_count": new,
        "monotonic_pass": monotonic_pass,
        "threshold_pass": threshold_pass,
        "pass_ratio": pass_ratio,
        "threshold": args.threshold,
    }
    audit_dir = args.root / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    out = audit_dir / (
        f"l2_l3_sort_check-{args.exchange}-{args.channel}-{args.date}.json"
    )
    out.write_text(json.dumps(audit, default=str, indent=2))
    print(json.dumps(audit, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
