"""verify_rpo_zero.py — RPO=0 verify CLI (CutoverVerifier 호출 + JSON output + Markdown evidence pack).

Story: MCT-155 (Stage 2 — Local GC + Secret rotation + RPO=0 verify + Stage 2 종료 gate)
Issue: mclayer/mctrader-hub#274

S8 박제 직접 owner.

Usage:
    python scripts/migration/verify_rpo_zero.py \\
        --cutover-timestamp 2026-05-13T15:00:00Z \\
        --output /tmp/rpo-zero-verify-MCT-155.md \\
        --json-output /tmp/rpo-zero-verify-MCT-155.json \\
        --local-l2-root /data/cold/L2

Exit codes:
    0 = rpo_zero_verified
    1 = drift_detected
    2 = verify_inconclusive
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from mctrader_data.nas_migration.cutover_verifier import (
    CutoverVerifier,
    RpoVerifyResult,
)
from mctrader_data.nas_migration.invariant_harness import InvariantHarness
from mctrader_data.nas_storage.nas_uploader import NASUploader

log = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RPO=0 verify CLI (MCT-155)")
    p.add_argument(
        "--cutover-timestamp",
        required=True,
        help="cutover timestamp ISO8601 (e.g. 2026-05-13T15:00:00Z)",
    )
    p.add_argument(
        "--local-l2-root",
        default="/data/cold/L2",
        help="local L2 root directory",
    )
    p.add_argument(
        "--nas-bucket",
        default="mctrader-market",
        help="NAS bucket name",
    )
    p.add_argument(
        "--nas-l2-prefix",
        default="schema_version=v1/tier=L2",
        help="NAS L2 partition prefix",
    )
    p.add_argument(
        "--output",
        default="/tmp/rpo-zero-verify-MCT-155.md",
        help="Markdown evidence pack output path",
    )
    p.add_argument(
        "--json-output",
        default="/tmp/rpo-zero-verify-MCT-155.json",
        help="JSON output path (GcRunner pre-check 직접 활용)",
    )
    p.add_argument(
        "--minio-endpoint",
        default=None,
        help="MinIO endpoint URL override (default: env MINIO_ENDPOINT)",
    )
    return p.parse_args()


def _result_to_json_dict(result: RpoVerifyResult) -> dict:
    """RpoVerifyResult -> JSON-serializable dict (GcRunner pre-check file format)."""
    return {
        "status": result.status,
        "cutover_timestamp_iso": result.cutover_timestamp_iso,
        "cutover_minus_1s_segment_count": result.cutover_minus_1s_segment_count,
        "cutover_plus_1s_nas_object_count": result.cutover_plus_1s_nas_object_count,
        "diff_segments_count": len(result.diff_segments),
        "diff_segments": result.diff_segments[:50],  # truncate for readability
        "verify_duration_ms": result.verify_duration_ms,
        "verify_error": result.verify_error,
        "invariant_status": (
            result.invariant_result.status if result.invariant_result else "n/a"
        ),
    }


def _render_markdown(result: RpoVerifyResult) -> str:
    """Render Markdown evidence pack for retro 박제 trail."""
    now_iso = datetime.now(timezone.utc).isoformat()
    lines = [
        "# RPO=0 Verify Evidence Pack — MCT-155",
        "",
        f"**Generated**: {now_iso}",
        f"**Cutover timestamp**: {result.cutover_timestamp_iso}",
        f"**Status**: `{result.status}`",
        "",
        "## §1 Overview",
        "",
        f"- cutover-1s local segment count: {result.cutover_minus_1s_segment_count}",
        f"- cutover+1s NAS object count: {result.cutover_plus_1s_nas_object_count}",
        f"- diff segments count: {len(result.diff_segments)}",
        f"- verify duration ms: {result.verify_duration_ms:.2f}",
        "",
        "## §2 Diff Segments (cutover-1s in local but missing in NAS @ +1s)",
        "",
    ]
    if result.diff_segments:
        for seg in result.diff_segments[:50]:
            lines.append(f"- `{seg}`")
        if len(result.diff_segments) > 50:
            lines.append(f"- ... ({len(result.diff_segments) - 50} more)")
    else:
        lines.append("(none — diff 0)")
    lines.extend(
        [
            "",
            "## §3 7종 invariant verify result",
            "",
        ]
    )
    if result.invariant_result is not None:
        inv = result.invariant_result
        lines.append(f"- **Status**: `{inv.status}`")
        lines.append(f"- verify latency ms: {inv.verify_latency_ms:.2f}")
        lines.append("")
        lines.append("### per-invariant")
        for name, per_result in (inv.per_invariant_results or {}).items():
            lines.append(f"- **{name}**: `{per_result.status}`")
    else:
        lines.append("(no local segments at cutover+1s — invariant verify skipped)")
    if result.verify_error:
        lines.extend(
            ["", "## §4 Verify Error", "", f"```\n{result.verify_error}\n```"]
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    import os

    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    local_l2_root = Path(args.local_l2_root)
    output_path = Path(args.output)
    json_output_path = Path(args.json_output)

    # ── NASUploader + InvariantHarness instances ──────────────────────────────
    endpoint = args.minio_endpoint or os.environ.get("NAS_MINIO_ENDPOINT", "")
    access_key = os.environ.get("NAS_MINIO_ACCESS_KEY", "")
    secret_key = os.environ.get("NAS_MINIO_SECRET_KEY", "")
    uploader = NASUploader(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket=args.nas_bucket,
    )
    harness = InvariantHarness(
        nas_uploader=uploader,
        local_root=local_l2_root,
    )
    verifier = CutoverVerifier(
        nas_uploader=uploader,
        invariant_harness=harness,
        local_l2_root=local_l2_root,
        nas_bucket=args.nas_bucket,
        nas_l2_prefix=args.nas_l2_prefix,
    )

    log.info(
        "verify_rpo_zero start cutover_timestamp=%s local_l2_root=%s",
        args.cutover_timestamp,
        local_l2_root,
    )
    result = verifier.verify_rpo_zero(args.cutover_timestamp)

    # ── Write JSON output (GcRunner pre-check file) ───────────────────────────
    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    json_output_path.write_text(json.dumps(_result_to_json_dict(result), indent=2))

    # ── Write Markdown evidence pack ──────────────────────────────────────────
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(_render_markdown(result))

    log.info(
        "verify_rpo_zero done status=%s diff_count=%d duration_ms=%.2f",
        result.status,
        len(result.diff_segments),
        result.verify_duration_ms,
    )

    if result.status == "rpo_zero_verified":
        return 0
    if result.status == "drift_detected":
        return 1
    return 2  # verify_inconclusive


if __name__ == "__main__":
    sys.exit(main())
