#!/usr/bin/env python3
"""WAL freeze tool — MCT-164 Phase 2 entry (INV-1, AC-1).

PURPOSE
-------
upbit WAL 의 sealed segments 를 read-only 로 전환하여 forward-only loss accumulation
을 즉시 차단. 신규 쓰기 차단 검증 포함.

INV-1: Phase 2 entry 첫 액션. forward-only loss 차단 최우선.
INV-2: production data mutation 0 — chmod read-only only, no content modification.

USAGE
-----
# Dry-run (default): 대상 파일 목록만 출력, 실제 변경 없음
python scripts/wal_freeze.py --root /var/lib/mctrader/data

# 실제 freeze 실행
python scripts/wal_freeze.py --root /var/lib/mctrader/data --execute

# 특정 exchange 지정 (기본값: upbit)
python scripts/wal_freeze.py --root /var/lib/mctrader/data --exchange upbit --execute

# 결과 JSON 저장
python scripts/wal_freeze.py --root /var/lib/mctrader/data --execute --output-json /tmp/freeze-result.json

DESIGN
------
- sealed segments (.ndjson.sealed) → chmod 0o444 (read-only)
- active segments (.ndjson, not sealed) → 그대로 (진행 중인 write 방해 안 함)
- compacted markers (.ndjson.sealed.compacted) → 포함 (이미 처리됨, read-only 유지)
- 결과: frozen_count, skipped_count, error_count 박제
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import stat
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class FreezeResult:
    exchange: str
    root: str
    executed: bool
    frozen_count: int = 0
    already_frozen_count: int = 0
    skipped_active_count: int = 0
    error_count: int = 0
    frozen_paths: list[str] = field(default_factory=list)
    error_paths: list[str] = field(default_factory=list)
    started_at: str = ""
    completed_at: str = ""

    def summary(self) -> str:
        return (
            f"FreezeResult exchange={self.exchange} "
            f"frozen={self.frozen_count} already_frozen={self.already_frozen_count} "
            f"skipped_active={self.skipped_active_count} errors={self.error_count} "
            f"executed={self.executed}"
        )


def is_readonly(path: Path) -> bool:
    """Return True if the file is read-only (not writable by owner)."""
    mode = os.stat(path).st_mode
    return not bool(mode & stat.S_IWUSR)


def freeze_wal(
    root: Path,
    exchange: str = "upbit",
    execute: bool = False,
) -> FreezeResult:
    """Freeze sealed WAL segments for the given exchange.

    Args:
        root: WAL root directory (e.g. /var/lib/mctrader/data)
        exchange: Exchange name to freeze (default: upbit)
        execute: If False, dry-run only (no filesystem changes)

    Returns:
        FreezeResult with counts and paths
    """
    result = FreezeResult(
        exchange=exchange,
        root=str(root),
        executed=execute,
        started_at=datetime.now(timezone.utc).isoformat(),
    )

    wal_exchange_dir = root / "wal" / exchange
    if not wal_exchange_dir.exists():
        log.warning("[freeze] WAL directory not found: %s", wal_exchange_dir)
        result.completed_at = datetime.now(timezone.utc).isoformat()
        return result

    log.info("[freeze] scanning %s (execute=%s)", wal_exchange_dir, execute)

    # Scan all files under wal/<exchange>/
    for p in sorted(wal_exchange_dir.rglob("*")):
        if not p.is_file():
            continue

        name = p.name

        # Active segment (.ndjson, not sealed) — skip (진행 중인 write 방해 안 함)
        if name.endswith(".ndjson") and not name.endswith(".ndjson.sealed"):
            log.debug("[freeze] skip active segment: %s", p)
            result.skipped_active_count += 1
            continue

        # Sealed or compacted segment — freeze target
        if name.endswith(".ndjson.sealed") or name.endswith(".ndjson.sealed.compacted"):
            if is_readonly(p):
                log.debug("[freeze] already frozen: %s", p)
                result.already_frozen_count += 1
                continue

            if execute:
                try:
                    os.chmod(p, 0o444)
                    log.info("[freeze] frozen: %s", p)
                    result.frozen_count += 1
                    result.frozen_paths.append(str(p))
                except OSError as e:
                    log.error("[freeze] chmod failed: %s — %s", p, e)
                    result.error_count += 1
                    result.error_paths.append(str(p))
            else:
                # Dry-run: just count
                log.info("[freeze] DRY-RUN would freeze: %s", p)
                result.frozen_count += 1
                result.frozen_paths.append(str(p))

    result.completed_at = datetime.now(timezone.utc).isoformat()
    return result


def verify_freeze(root: Path, exchange: str = "upbit") -> dict:
    """Verify that all sealed segments for exchange are read-only.

    Returns a verification report dict.
    """
    wal_exchange_dir = root / "wal" / exchange
    total_sealed = 0
    frozen = 0
    writable = 0
    writable_paths: list[str] = []

    if wal_exchange_dir.exists():
        for p in sorted(wal_exchange_dir.rglob("*.ndjson.sealed")):
            if not p.is_file():
                continue
            total_sealed += 1
            if is_readonly(p):
                frozen += 1
            else:
                writable += 1
                writable_paths.append(str(p))

    return {
        "exchange": exchange,
        "wal_dir": str(wal_exchange_dir),
        "total_sealed": total_sealed,
        "frozen": frozen,
        "writable": writable,
        "writable_paths": writable_paths,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "inv1_pass": writable == 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MCT-164 WAL freeze tool — upbit sealed segments read-only 전환 (INV-1, AC-1)"
    )
    parser.add_argument(
        "--root",
        required=True,
        help="WAL root directory (e.g. /var/lib/mctrader/data)",
    )
    parser.add_argument(
        "--exchange",
        default="upbit",
        help="Exchange to freeze (default: upbit)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually freeze (default: dry-run only)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="After freeze, verify all sealed segments are read-only",
    )
    parser.add_argument(
        "--output-json",
        help="Write result JSON to this path",
    )
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
        log.error("[freeze] root directory does not exist: %s", root)
        return 1

    if not args.execute:
        log.info("[freeze] DRY-RUN mode — no filesystem changes (pass --execute to apply)")

    result = freeze_wal(root=root, exchange=args.exchange, execute=args.execute)
    log.info("[freeze] %s", result.summary())

    output: dict = {"freeze_result": asdict(result)}

    if args.verify:
        verify_report = verify_freeze(root=root, exchange=args.exchange)
        output["verify_report"] = verify_report
        inv1_pass = verify_report["inv1_pass"]
        log.info(
            "[freeze] INV-1 verify: total_sealed=%d frozen=%d writable=%d PASS=%s",
            verify_report["total_sealed"],
            verify_report["frozen"],
            verify_report["writable"],
            inv1_pass,
        )
        if not inv1_pass:
            log.error(
                "[freeze] INV-1 FAIL — writable sealed segments remain: %s",
                verify_report["writable_paths"],
            )

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("[freeze] result written to %s", out_path)
    else:
        print(json.dumps(output, indent=2, ensure_ascii=False))

    # Exit 0 if no errors
    return 1 if result.error_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
