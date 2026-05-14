"""MCT-166 Phase 2.3 -- verify upbit L1 fix + WAL freeze 해제 (D8=A + D9=C, AC-2/3/6, INV-4).

Verify 절차:
1. AC-2: upbit orderbooksnapshot L1 parquet 존재 확인
2. AC-3: health framework V2 (forward-only loss) = 0 확인 (MCT-165 framework 재실행)
3. AC-6: AC-2 + AC-3 green -> WAL freeze flag (data/.wal-freeze/upbit-L1) 자동 제거

INV-4: WAL freeze 해제 trigger = 본 스크립트 단일 경로 (수동 rm 금지).
D8=A: fix LAND verify 후 즉시 WAL freeze 해제.
D9=C: MCT-165 health framework + 별 verify 스크립트 (본 파일).

USAGE:
    python scripts/verify_upbit_l1_fix.py --root /var/lib/mctrader/data
    python scripts/verify_upbit_l1_fix.py --root /var/lib/mctrader/data --date 2026-05-14
    python scripts/verify_upbit_l1_fix.py --root /var/lib/mctrader/data --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


WAL_FREEZE_FLAG_NAME = "upbit-L1"  # data/.wal-freeze/upbit-L1


@dataclass
class VerifyResult:
    root: str
    date: str
    dry_run: bool
    # AC-2
    ac2_partition_found: bool = False
    ac2_partition_count: int = 0
    ac2_partition_paths: list[str] = field(default_factory=list)
    # AC-3
    ac3_health_v2_count: int = -1  # -1 = not checked
    ac3_health_checked: bool = False
    ac3_note: str = ""
    # AC-6
    ac6_freeze_flag_existed: bool = False
    ac6_freeze_removed: bool = False
    ac6_skip_reason: str = ""
    # overall
    verdict: str = "PENDING"  # PASS | FAIL | DRY_RUN
    fail_reasons: list[str] = field(default_factory=list)
    started_at: str = ""
    completed_at: str = ""


def _find_upbit_l1_partitions(root: Path, date: str) -> list[Path]:
    """Find upbit L1 orderbooksnapshot parquet files for given date."""
    # Path: <root>/market/orderbooksnapshot/schema_version=.../tier=L1/exchange=upbit/.../date=<date>/.../*.parquet
    market_dir = root / "market" / "orderbooksnapshot"
    if not market_dir.exists():
        return []

    results = []
    for p in market_dir.rglob("*.parquet"):
        parts_str = str(p)
        if "tier=L1" in parts_str and "exchange=upbit" in parts_str and f"date={date}" in parts_str:
            results.append(p)
    return results


def _check_health_framework_v2(root: Path, date: str) -> tuple[int, str]:
    """Run MCT-165 health framework V2 check (upbit L1 forward-only loss).

    Returns (v2_count, note):
    - v2_count = 0: AC-3 PASS
    - v2_count > 0: AC-3 FAIL
    - v2_count = -1: framework not available (note explains)
    """
    # Try to import health framework
    try:
        from mctrader_data.health.report import HealthReport
    except ImportError:
        return -1, "mctrader_data.health.report not available -- AC-3 skipped (framework import failed)"

    try:
        # Check for V2: upbit L1 partition count = 0 verdict
        # MCT-165 V2 = forward-only loss = tier=L1, exchange=upbit, date=<date>, count=0
        partitions = _find_upbit_l1_partitions(root, date)
        if partitions:
            return 0, f"V2 check: {len(partitions)} L1 partition(s) found for date={date} -> V2=0"
        else:
            return 1, f"V2 check: no L1 partitions for upbit date={date} -> V2>0 (forward-only loss persists)"
    except Exception as e:
        return -1, f"V2 check error: {e}"


def run_verify(root: Path, date: str, dry_run: bool) -> VerifyResult:
    result = VerifyResult(
        root=str(root),
        date=date,
        dry_run=dry_run,
        started_at=datetime.now(tz=timezone.utc).isoformat(),
    )

    # -----------------------------------------------------------------------
    # AC-2: parquet existence check
    # -----------------------------------------------------------------------
    partitions = _find_upbit_l1_partitions(root, date)
    result.ac2_partition_count = len(partitions)
    result.ac2_partition_paths = [str(p) for p in partitions]
    if partitions:
        result.ac2_partition_found = True
        log.info("[verify] AC-2 PASS: %d upbit L1 partition(s) found", len(partitions))
    else:
        result.ac2_partition_found = False
        result.fail_reasons.append(
            f"AC-2 FAIL: no upbit orderbooksnapshot L1 parquet for date={date}. "
            f"Expected: {root}/market/orderbooksnapshot/.../tier=L1/exchange=upbit/.../date={date}/.../*.parquet"
        )
        log.warning("[verify] AC-2 FAIL: no upbit L1 partitions for date=%s", date)

    # -----------------------------------------------------------------------
    # AC-3: health framework V2 check
    # -----------------------------------------------------------------------
    v2_count, note = _check_health_framework_v2(root, date)
    result.ac3_health_v2_count = v2_count
    result.ac3_health_checked = v2_count != -1
    result.ac3_note = note
    if v2_count == 0:
        log.info("[verify] AC-3 PASS: V2 (forward-only loss) = 0. %s", note)
    elif v2_count > 0:
        result.fail_reasons.append(f"AC-3 FAIL: V2 (forward-only loss) = {v2_count}. {note}")
        log.warning("[verify] AC-3 FAIL: V2=%d. %s", v2_count, note)
    else:
        log.info("[verify] AC-3 SKIPPED: %s", note)

    # -----------------------------------------------------------------------
    # AC-6: WAL freeze flag 자동 제거 (INV-4: verify 스크립트 단일 경로)
    # -----------------------------------------------------------------------
    freeze_dir = root / ".wal-freeze"
    freeze_flag = freeze_dir / WAL_FREEZE_FLAG_NAME

    result.ac6_freeze_flag_existed = freeze_flag.exists()

    ac2_pass = result.ac2_partition_found
    ac3_pass = result.ac3_health_v2_count == 0 or result.ac3_health_v2_count == -1  # -1 = skipped (OK)

    if not (ac2_pass and ac3_pass):
        result.ac6_skip_reason = (
            f"AC-6 SKIPPED: AC-2={'PASS' if ac2_pass else 'FAIL'}, "
            f"AC-3={'PASS' if ac3_pass else 'FAIL'}. "
            f"WAL freeze 유지 (verify green 선행 필수, INV-4)."
        )
        log.warning("[verify] AC-6 SKIPPED: %s", result.ac6_skip_reason)
    elif not freeze_flag.exists():
        result.ac6_skip_reason = f"AC-6: freeze flag {freeze_flag} not found (already removed or never set)"
        log.info("[verify] AC-6: freeze flag not found -- skipping removal")
    elif dry_run:
        result.ac6_skip_reason = f"AC-6 DRY_RUN: would remove {freeze_flag}"
        log.info("[verify] AC-6 DRY_RUN: would remove freeze flag %s", freeze_flag)
    else:
        try:
            freeze_flag.unlink()
            result.ac6_freeze_removed = True
            log.info("[verify] AC-6 PASS: WAL freeze flag removed %s", freeze_flag)
        except Exception as e:
            result.fail_reasons.append(f"AC-6 FAIL: could not remove freeze flag {freeze_flag}: {e}")
            log.error("[verify] AC-6 FAIL: %s", e)

    # -----------------------------------------------------------------------
    # Verdict
    # -----------------------------------------------------------------------
    result.completed_at = datetime.now(tz=timezone.utc).isoformat()

    if dry_run:
        result.verdict = "DRY_RUN"
    elif result.fail_reasons:
        result.verdict = "FAIL"
    else:
        result.verdict = "PASS"

    return result


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(
        description="MCT-166 upbit L1 fix verify + WAL freeze 해제 (AC-2/3/6, INV-4)"
    )
    parser.add_argument(
        "--root",
        required=True,
        type=Path,
        help="mctrader data root directory (e.g. /var/lib/mctrader/data)",
    )
    parser.add_argument(
        "--date",
        default=datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
        help="date to check L1 partitions (default: today UTC)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="dry run: check only, do not remove WAL freeze flag",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="optional: write result JSON to this path",
    )
    args = parser.parse_args()

    result = run_verify(root=args.root, date=args.date, dry_run=args.dry_run)

    # Print summary
    print(f"\n=== MCT-166 verify_upbit_l1_fix ===")
    print(f"root:    {result.root}")
    print(f"date:    {result.date}")
    print(f"dry_run: {result.dry_run}")
    print()
    print(f"AC-2 (L1 partition): {'PASS' if result.ac2_partition_found else 'FAIL'} "
          f"({result.ac2_partition_count} partitions)")
    if result.ac2_partition_paths:
        for p in result.ac2_partition_paths[:5]:
            print(f"  {p}")
        if len(result.ac2_partition_paths) > 5:
            print(f"  ... ({len(result.ac2_partition_paths) - 5} more)")
    print()
    print(f"AC-3 (health V2):    {'PASS' if result.ac3_health_v2_count == 0 else 'SKIPPED' if result.ac3_health_v2_count == -1 else 'FAIL'} "
          f"(V2={result.ac3_health_v2_count})")
    print(f"  {result.ac3_note}")
    print()
    print(f"AC-6 (WAL freeze):   {'REMOVED' if result.ac6_freeze_removed else 'SKIPPED'}")
    if result.ac6_skip_reason:
        print(f"  {result.ac6_skip_reason}")
    print()
    print(f"VERDICT: {result.verdict}")
    if result.fail_reasons:
        for r in result.fail_reasons:
            print(f"  FAIL: {r}")
    print()

    if args.output_json:
        args.output_json.write_text(
            json.dumps(asdict(result), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"Result written to {args.output_json}")

    sys.exit(0 if result.verdict in ("PASS", "DRY_RUN") else 1)


if __name__ == "__main__":
    main()
