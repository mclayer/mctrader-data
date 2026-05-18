"""rekey_l1_migration.py — NAS l1/ → 평면 1회성 멱등 re-key CLI (U3-MIGRATE, thin wrapper).

Story: U3-MIGRATE (mctrader-data#89)
ADR: ADR-034 §결정 4 (3-step copy → 4-HEAD verify → delete)

Usage:
    python scripts/rekey_l1_migration.py \\
        --root /var/lib/mctrader/data \\
        --exchange bithumb \\
        --channel orderbooksnapshot \\
        [--dry-run | --execute --i-understand-this-is-irreversible] \\
        [--batch-size 500] [--max-partitions <int>] \\
        [--resume-from-manifest] [--threshold 0.0]

PL 결정 #3: <50 lines thin wrapper — NASUploader + RekeyOrchestrator 조립 + run() 위임만.
PL 결정 #5: NASUploader(nas_role="rekey") — NAS_MINIO_REKEY_* IAM key (DELETE+COPY only).
PL 결정 #9: --i-understand-this-is-irreversible gate (OpRiskArch §7.4.5 운영 인적 게이트).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="rekey_l1_migration",
        description="NAS l1/ prefix 객체 → 평면 1회성 re-key (U3-MIGRATE ADR-034 §결정 4)",
    )
    p.add_argument("--root", required=True, type=Path, help="data root (절대경로 의무)")
    p.add_argument(
        "--exchange",
        required=True,
        choices=["bithumb", "upbit"],
        help="exchange (allowlist: bithumb / upbit)",
    )
    p.add_argument(
        "--channel",
        default=None,
        choices=["transaction", "orderbooksnapshot", "orderbookdepth"],
        help="channel (default: all supported for exchange)",
    )
    p.add_argument("--batch-size", type=int, default=int(os.environ.get("MCTRADER_REKEY_BATCH_LIMIT", "500")))
    p.add_argument("--max-partitions", type=int, default=None)
    p.add_argument("--resume-from-manifest", action="store_true")
    p.add_argument("--threshold", type=float, default=0.0)

    mode_group = p.add_mutually_exclusive_group()
    mode_group.add_argument("--dry-run", action="store_true", default=True)
    mode_group.add_argument("--execute", action="store_true", default=False)
    p.add_argument(
        "--i-understand-this-is-irreversible",
        action="store_true",
        dest="i_understand_irreversible",
        help="운영 인적 게이트 (--execute 와 함께 의무, PL 결정 #9)",
    )
    return p


def main() -> int:
    from mctrader_data.allowlist import validate_channel_exchange
    from mctrader_data.nas_migration.rekey import RekeyOrchestrator
    from mctrader_data.nas_storage.nas_uploader import NASUploader

    parser = _build_parser()
    args = parser.parse_args()

    dry_run = not args.execute
    root = args.root.resolve()

    # channel resolution (default = all-supported-for-exchange)
    channels = [args.channel] if args.channel else _default_channels(args.exchange)

    for channel in channels:
        # ACL gate (MCT-164 정합 — upbit+orderbookdepth BLOCKED)
        try:
            validate_channel_exchange(channel, args.exchange)
        except ValueError as exc:
            print(f"[rekey] BLOCKED: {exc}", file=sys.stderr)
            return 1

        uploader = NASUploader(nas_role="rekey")
        orchestrator = RekeyOrchestrator(
            nas_uploader=uploader,
            root=root,
            exchange=args.exchange,
            channel=channel,
            batch_size=args.batch_size,
            dry_run=dry_run,
            threshold=args.threshold,
            max_partitions=args.max_partitions,
            resume_from_manifest=args.resume_from_manifest,
            i_understand_irreversible=args.i_understand_irreversible,
        )
        result = orchestrator.run()
        print(
            f"[rekey] exchange={args.exchange} channel={channel} dry_run={dry_run} "
            f"total={result.partitions_total} copied={result.copied} "
            f"deleted={result.deleted} failed={result.failed} "
            f"duration_s={result.duration_s:.2f}"
        )

    return 0


def _default_channels(exchange: str) -> list[str]:
    """exchange 별 기본 channel 목록 (allowlist 정합, MCT-166 D1=B)."""
    if exchange == "bithumb":
        return ["transaction", "orderbooksnapshot", "orderbookdepth"]
    # upbit: orderbookdepth = BLOCKED (MCT-166 D1=B + MCT-159 Issue 1)
    return ["transaction", "orderbooksnapshot"]


if __name__ == "__main__":
    sys.exit(main())
