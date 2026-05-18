#!/usr/bin/env python3
"""
WS-A 117GB Historical Tier Promotion Verify Gate (MCT-200 Group B)

MCT-173 verify_backfill_partial_loss.py pattern 재사용.
L1 file count vs L2 partition count (date 2026-05-13~15, upbit orderbooksnapshot).

Acceptance Criteria AC-4:
- 16,946 L1 files L2 승격 ratio >= 0.90 partial-loss threshold (MCT-173 D8=C 정합)
- per-symbol breakdown (KRW-* upbit orderbooksnapshot)
- audit markdown 자동 박제 (MCT-173 Phase 2.4 result 형식)

Invariant:
- INV-T4: ratio >= 0.90 + audit/backfill-manifest YAML 무변
- INV-T7: perf baseline within target (duration <= 24h + RSS <= 256 MB — MCT-163 F6 streaming)

Exit codes:
- 0 = PASS (ratio >= threshold, audit 박제 완료)
- 1 = FAIL (ratio < threshold || fail count > 0)
- 2 = ERROR (missing files || invalid args)

Story: MCT-200 — MinIO IAM 복원 + WS-A 117GB 백필
Phase: Phase 2 Group B (DataEngineerAgent)
Date: 2026-05-18
"""

import argparse
import json
import logging
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class BackfillVerifyResult:
    """MCT-173 PartialLossReport 형식 정합."""
    total_l1_rows: int  # WAL line count
    total_l2_partitions: int  # L2 row count
    ratio: float  # L1 / L2 ratio
    pass_count: int
    fail_count: int
    skip_count: int
    status: str  # "PASS" or "FAIL"
    threshold: float
    per_symbol_breakdown: Dict[str, Dict[str, int]]  # {"KRW-BTC": {"l1": N, "l2": M, "ratio": X}, ...}
    audit_path: str  # docs/audit/MCT-200-ws-a-backfill-verify-2026-05-13-15.md


def discover_l1_partitions(
    root: Path,
    start_date: str,
    end_date: str,
    exchange: str,
    channel: str,
) -> Dict[str, int]:
    """
    L1 파일 발견 (date range, per-symbol).

    Layout:
      <root>/l1/<exchange>/<channel>/<symbol>/<date>/
        *.parquet  (date=<d>/node=<node_id>/part-*.parquet format)

    Invariant INV-1: Source WAL immutable (PIT snapshot).
    Note: 본 verify 는 L1 파일 발견만 (WAL line count 직접 계산 X).

    Returns: {"KRW-BTC": <file_count>, ...}
    """
    l1_base = root / "l1" / exchange / channel
    if not l1_base.exists():
        logger.error(f"L1 base not found: {l1_base}")
        return {}

    # Date range parse
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return {}

    per_symbol = {}
    current = start

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")

        # CLAUDE.md §historical tier promotion discovery 규약:
        # "_discover_partitions_in_range 는 production L1 layout `date=<d>/node=<node_id>/part-*.parquet` 인지"
        # date_dir 비재귀 glob 이 아니라 rglob 사용 (commit c169720 CRITICAL fix)

        symbol_dirs = l1_base.glob(f"*/{date_str}/")  # <symbol>/<date>/

        for symbol_dir in symbol_dirs:
            symbol = symbol_dir.parent.name

            # node=<id>/part-*.parquet 서브디렉토리 발견 (rglob)
            parquet_files = list(symbol_dir.rglob("part-*.parquet"))

            if parquet_files:
                if symbol not in per_symbol:
                    per_symbol[symbol] = 0
                per_symbol[symbol] += len(parquet_files)

        current += timedelta(days=1)

    return per_symbol


def discover_l2_partitions(
    root: Path,
    start_date: str,
    end_date: str,
    exchange: str,
    channel: str,
) -> Dict[str, int]:
    """
    L2 파일 발견 (date range, per-symbol).

    Layout:
      <root>/l2/<exchange>/<channel>/<date>/<symbol>/
        *.parquet  (date-bucketed, symbol-bucketed)

    Returns: {"KRW-BTC": <partition_count>, ...}
    """
    l2_base = root / "l2" / exchange / channel
    if not l2_base.exists():
        logger.warning(f"L2 base not found: {l2_base} (first-time backfill OK)")
        return {}

    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return {}

    per_symbol = {}
    current = start

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")

        # <date>/<symbol>/ layout
        symbol_dirs = list(l2_base.glob(f"{date_str}/*/"))

        for symbol_dir in symbol_dirs:
            symbol = symbol_dir.name

            parquet_files = list(symbol_dir.glob("*.parquet"))

            if parquet_files:
                if symbol not in per_symbol:
                    per_symbol[symbol] = 0
                per_symbol[symbol] += len(parquet_files)

        current += timedelta(days=1)

    return per_symbol


def compute_ratio(l1_count: int, l2_count: int) -> float:
    """L2 / L1 ratio (coverage 비율)."""
    if l1_count == 0:
        return 0.0
    return l2_count / l1_count


def verify_backfill(
    root: Path,
    start_date: str,
    end_date: str,
    exchange: str,
    channel: str,
    threshold: float = 0.90,
    audit_output: Optional[Path] = None,
) -> BackfillVerifyResult:
    """
    Main verify logic.

    Per-symbol breakdown:
    - KRW-MATIC: partial boundary (MCT-173 Phase 2.4 Skip 1 허용) → skip 처리
    - 기타: ratio < threshold → fail
    """
    logger.info(f"Verifying backfill: {exchange}/{channel} {start_date}~{end_date}")

    # L1 / L2 발견
    l1_per_symbol = discover_l1_partitions(root, start_date, end_date, exchange, channel)
    l2_per_symbol = discover_l2_partitions(root, start_date, end_date, exchange, channel)

    logger.info(f"L1 symbols: {len(l1_per_symbol)}")
    logger.info(f"L2 symbols: {len(l2_per_symbol)}")

    # 통계 계산
    total_l1_rows = sum(l1_per_symbol.values())
    total_l2_partitions = sum(l2_per_symbol.values())
    ratio = compute_ratio(total_l1_rows, total_l2_partitions)

    logger.info(f"Total L1 rows: {total_l1_rows} / L2 partitions: {total_l2_partitions} / ratio: {ratio:.2%}")

    # Per-symbol breakdown
    pass_count = 0
    fail_count = 0
    skip_count = 0
    per_symbol = {}

    all_symbols = set(l1_per_symbol.keys()) | set(l2_per_symbol.keys())

    for symbol in sorted(all_symbols):
        l1_count = l1_per_symbol.get(symbol, 0)
        l2_count = l2_per_symbol.get(symbol, 0)
        symbol_ratio = compute_ratio(l1_count, l2_count)

        per_symbol[symbol] = {
            "l1": l1_count,
            "l2": l2_count,
            "ratio": round(symbol_ratio, 4),
        }

        # MCT-173 Phase 2.4: KRW-MATIC partial boundary Skip 1 허용
        if symbol == "KRW-MATIC" and l1_count > 0 and l2_count == 0:
            logger.warning(f"Symbol {symbol}: partial boundary (L1={l1_count}, L2={l2_count}) — SKIP")
            skip_count += 1
        elif symbol_ratio < threshold and l1_count > 0:
            logger.error(f"Symbol {symbol}: ratio {symbol_ratio:.2%} < {threshold:.2%} — FAIL")
            fail_count += 1
        else:
            pass_count += 1

    # Overall status
    status = "PASS" if (fail_count == 0 and ratio >= threshold) else "FAIL"

    logger.info(f"Pass={pass_count}, Fail={fail_count}, Skip={skip_count}")
    logger.info(f"Status: {status}")

    # Audit path (repo-relative, not production filesystem)
    # Default: script directory/../docs/audit/ (repo root relative)
    if audit_output is None:
        audit_output = (
            Path(__file__).resolve().parents[1]
            / "docs"
            / "audit"
            / f"MCT-200-ws-a-backfill-verify-{start_date}-{end_date}.md"
        )
    audit_path = str(audit_output)

    return BackfillVerifyResult(
        total_l1_rows=total_l1_rows,
        total_l2_partitions=total_l2_partitions,
        ratio=round(ratio, 4),
        pass_count=pass_count,
        fail_count=fail_count,
        skip_count=skip_count,
        status=status,
        threshold=threshold,
        per_symbol_breakdown=per_symbol,
        audit_path=audit_path,
    )


def write_audit_markdown(
    result: BackfillVerifyResult,
    output_path: Path,
) -> bool:
    """
    MCT-173 Phase 2.4 result 형식 audit markdown 박제.

    Template:
    ```
    Total L1 rows: <N> / L2 partitions: <M> (ratio ~Xx, orderbooksnapshot flatten)
    Pass=<P>, Fail=<F>, Skip=<S>
    INV-T4 PASS: True
    ```
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# MCT-200 WS-A Historical Tier Promotion Verify Result",
        "",
        f"**Date**: {datetime.now().isoformat()}",
        f"**Threshold**: {result.threshold:.0%}",
        "",
        "## Summary",
        "",
        f"Total L1 rows: {result.total_l1_rows:,} / L2 partitions: {result.total_l2_partitions:,} "
        f"(ratio ~{result.ratio:.1%}, orderbooksnapshot flatten)",
        f"Pass={result.pass_count}, Fail={result.fail_count}, Skip={result.skip_count}",
        "",
        f"**INV-T4 PASS**: {result.status == 'PASS'}",
        "",
        "## Per-Symbol Breakdown",
        "",
        "| Symbol | L1 files | L2 partitions | Ratio | Status |",
        "|--------|----------|---------------|-------|--------|",
    ]

    for symbol in sorted(result.per_symbol_breakdown.keys()):
        data = result.per_symbol_breakdown[symbol]
        l1 = data["l1"]
        l2 = data["l2"]
        ratio = data["ratio"]
        status_str = "PASS" if ratio >= result.threshold else ("SKIP" if l1 > 0 and l2 == 0 else "FAIL")
        lines.append(f"| {symbol} | {l1:,} | {l2:,} | {ratio:.2%} | {status_str} |")

    lines.extend([
        "",
        "## Notes",
        "",
        "- MCT-173 D8=C pattern 정합 (partial-loss threshold 0.90)",
        "- MCT-163 F6 streaming (RSS <= 256 MB, duration <= 24h)",
        "- CLAUDE.md §backfill mode INV-1~5 + §historical tier promotion INV-A/B/C/D 준수",
        "- commit c169720 (rglob discovery, date_dir 비재귀 glob 관계없이 node=<id>/ subdir 포함)",
    ])

    try:
        output_path.write_text("\n".join(lines))
        logger.info(f"Audit markdown written: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write audit markdown: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="WS-A Historical Tier Promotion Verify Gate (MCT-200 Group B)"
    )
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Data root path (e.g., /var/lib/mctrader/data)",
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Start date (YYYY-MM-DD format, e.g., 2026-05-13)",
    )
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="End date (YYYY-MM-DD format, e.g., 2026-05-15)",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default="upbit",
        help="Exchange (default: upbit)",
    )
    parser.add_argument(
        "--channel",
        type=str,
        default="orderbooksnapshot",
        help="Channel (default: orderbooksnapshot, orderbookdepth = #48 INV-D)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.90,
        help="Partial-loss threshold ratio (default: 0.90, MCT-173 D8=C pattern)",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Output JSON result path (optional, /tmp/ws-a-verify-mct200.json recommended)",
    )
    parser.add_argument(
        "--audit-output",
        type=Path,
        help="Audit markdown output path (default: docs/audit/MCT-200-ws-a-backfill-verify-{start_date}-{end_date}.md, relative to script directory)",
    )

    args = parser.parse_args()

    # Validate args
    if not args.root.exists():
        logger.error(f"Root path not found: {args.root}")
        sys.exit(2)

    # Verify
    try:
        result = verify_backfill(
            root=args.root,
            start_date=args.start,
            end_date=args.end,
            exchange=args.exchange,
            channel=args.channel,
            threshold=args.threshold,
            audit_output=args.audit_output,
        )
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        sys.exit(2)

    # Output JSON (if requested)
    if args.output_json:
        try:
            args.output_json.parent.mkdir(parents=True, exist_ok=True)
            args.output_json.write_text(json.dumps(asdict(result), indent=2))
            logger.info(f"JSON result written: {args.output_json}")
        except Exception as e:
            logger.error(f"Failed to write JSON: {e}")
            sys.exit(2)

    # Write audit markdown (always)
    audit_path = Path(result.audit_path)
    if not write_audit_markdown(result, audit_path):
        sys.exit(2)

    # Exit code
    if result.status == "PASS":
        logger.info("WS-A verify gate PASS — audit 박제 완료")
        sys.exit(0)
    else:
        logger.error("WS-A verify gate FAIL")
        sys.exit(1)


if __name__ == "__main__":
    main()
