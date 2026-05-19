# src/mctrader_data/compactor/historical_reclaim.py
"""MCT-204 Layer 3 — historical L1 partition local reclaim.

L2 NAS HEAD verify pass → L1 local date_dir rglob unlink + sentinel .l1-promoted 멱등.
ADR-029 D1=B amendment (historical L1 reclaim verify-after pattern) carrier.

Design decisions:
- L2 HEAD verify = list_objects_v2 KeyCount > 0 + local L2 date_dir exists (per chief decision:
  per-file HEAD = 비용 폭증 회피; GATE-2 simple pattern — MCT-202 D-3 sequential
  _historical_dual_write guarantees partition-level atomic L2 PUT).
- INV-C: verify fail → L1 unlink 0 (안전망 first).
- INV-D: sentinel .l1-promoted = idempotent re-entry guard.
- INV-F: sentinel write = tempfile + os.replace atomic.
- forward in-flight check (FIX 1/3 P0 #3): .forward-processing sentinel cross-cycle race mitigation.
- monotonic now_snapshot: caller passes single date snapshot per cycle (boundary race mitigation).
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader

log = logging.getLogger(__name__)

# L2 schema version lookup (mirrors L2Compactor _CHANNEL_SCHEMA_VERSION)
_CHANNEL_SCHEMA_VERSION: dict[str, str] = {
    "transaction": "tick.v1",
    "orderbooksnapshot": "orderbook_snapshot.v1",
    "orderbookdepth": "orderbook_depth.v1",
}

ReclaimOutcomeLiteral = Literal[
    "ok",
    "skip_sentinel",
    "skip_today_window",
    "skip_forward_in_flight",
    "skip_nas_missing",
    "fail_verify",
]


@dataclass
class ReclaimOutcome:
    """Result of reclaim_partition_l1_local().

    outcome enum (6 종, FIX 1/3 P0 #3 outcome 정합):
    - ok: L2 NAS verify pass → L1 files unlinked + sentinel written.
    - skip_sentinel: sentinel .l1-promoted already exists — idempotent skip.
    - skip_today_window: date_utc >= now_snapshot-1 → forward window, skip.
    - skip_forward_in_flight: .forward-processing sentinel exists → cross-cycle race skip.
    - skip_nas_missing: L2 NAS list_objects_v2 KeyCount == 0 → L2 not yet committed.
    - fail_verify: local L2 date_dir missing → verify failed, L1 preserved.
    """

    outcome: ReclaimOutcomeLiteral
    files_unlinked: int = 0
    bytes_freed: int = 0
    errors: list[str] = field(default_factory=list)


def reclaim_partition_l1_local(
    *,
    root: Path,
    nas_uploader: "NASUploader",
    exchange: str,
    symbol: str,
    channel: str,
    date_utc: date,
    now_snapshot: date,
) -> ReclaimOutcome:
    """L2 NAS HEAD verify → L1 local unlink → sentinel write. 멱등.

    MCT-204 Layer 3 main entry point. Called per (exchange, symbol, channel, date) partition
    after 24-hour L2 dual_write + 1 L3 dual_write all committed.

    Safety invariants:
    - INV-C: L2 NAS verify fail (KeyCount=0 or local L2 missing) → return early, L1 unlink 0.
    - INV-D: sentinel.exists() → skip (멱등).
    - INV-F: sentinel write = tempfile + os.replace atomic.
    - FIX 1/3 P0 #3: monotonic now_snapshot + .forward-processing sentinel cross-cycle race guard.

    Args:
        root: data root (e.g. /var/lib/mctrader/data)
        nas_uploader: NASUploader with live boto3 client
        exchange: exchange name (e.g. "upbit")
        symbol: symbol (e.g. "KRW-BTC")
        channel: channel name (e.g. "orderbooksnapshot")
        date_utc: partition date (UTC)
        now_snapshot: caller monotonic date snapshot (single per cycle, boundary race mitigation)

    Returns:
        ReclaimOutcome with outcome enum + stats.
    """
    schema_ver = _CHANNEL_SCHEMA_VERSION.get(channel, "v1")

    # L1 date_dir: market/<channel>/schema_version=*/tier=L1/exchange=*/symbol=*/date=*/
    # We glob schema_version since it may vary.
    l1_date_dirs = list(
        root.glob(
            f"market/{channel}/schema_version=*/tier=L1"
            f"/exchange={exchange}/symbol={symbol}/date={date_utc.isoformat()}"
        )
    )
    # Use first match (there should be exactly 1 in normal operation)
    if l1_date_dirs:
        date_dir = l1_date_dirs[0]
    else:
        # No L1 dir at all — nothing to reclaim, not an error
        date_dir = (
            root
            / "market"
            / channel
            / f"schema_version={schema_ver}"
            / "tier=L1"
            / f"exchange={exchange}"
            / f"symbol={symbol}"
            / f"date={date_utc.isoformat()}"
        )

    # --- INV-D: sentinel pre-check (idempotent re-entry guard) ---
    sentinel = date_dir / ".l1-promoted"
    if sentinel.exists():
        log.debug(
            "[reclaim] skip_sentinel exchange=%s symbol=%s channel=%s date=%s",
            exchange, symbol, channel, date_utc,
        )
        return ReclaimOutcome(outcome="skip_sentinel")

    # --- forward window boundary check (FIX 1/3 P0 #3, monotonic now_snapshot) ---
    # forward window = [now_snapshot-1, now_snapshot]. historical = date < now_snapshot-1.
    if date_utc >= now_snapshot - timedelta(days=1):
        log.debug(
            "[reclaim] skip_today_window exchange=%s symbol=%s channel=%s date=%s "
            "now_snapshot=%s",
            exchange, symbol, channel, date_utc, now_snapshot,
        )
        return ReclaimOutcome(outcome="skip_today_window")

    # --- forward in-flight sentinel check (FIX 1/3 P0 #3) ---
    # forward _run_l2_for_parquet writes .forward-processing on entry, unlinks on exit.
    # If present, a forward cycle is currently processing this partition — skip reclaim.
    forward_sentinel = date_dir / ".forward-processing"
    if forward_sentinel.exists():
        log.info(
            "[reclaim] skip_forward_in_flight exchange=%s symbol=%s channel=%s date=%s",
            exchange, symbol, channel, date_utc,
        )
        return ReclaimOutcome(outcome="skip_forward_in_flight")

    # --- L2 NAS verify (INV-C: fail → L1 unlink 0) ---
    # Step 1: list_objects_v2 KeyCount > 0 (L2 partition exists on NAS)
    l2_prefix = (
        f"market/{channel}/schema_version={schema_ver}/tier=L2"
        f"/exchange={exchange}/symbol={symbol}/date={date_utc.isoformat()}/"
    )
    try:
        resp = nas_uploader._s3.list_objects_v2(
            Bucket=nas_uploader.bucket,
            Prefix=l2_prefix,
            MaxKeys=1,
        )
        key_count = resp.get("KeyCount", 0)
    except Exception:
        log.exception(
            "[reclaim] NAS list_objects_v2 failed exchange=%s symbol=%s channel=%s date=%s",
            exchange, symbol, channel, date_utc,
        )
        return ReclaimOutcome(outcome="fail_verify")

    if key_count == 0:
        log.info(
            "[reclaim] skip_nas_missing exchange=%s symbol=%s channel=%s date=%s prefix=%s",
            exchange, symbol, channel, date_utc, l2_prefix,
        )
        return ReclaimOutcome(outcome="skip_nas_missing")

    # Step 2: local L2 date_dir exists (GATE-2 simplification — chief decision)
    l2_date_dirs = list(
        root.glob(
            f"market/{channel}/schema_version=*/tier=L2"
            f"/exchange={exchange}/symbol={symbol}/date={date_utc.isoformat()}"
        )
    )
    if not l2_date_dirs:
        log.info(
            "[reclaim] fail_verify (local L2 missing) exchange=%s symbol=%s channel=%s date=%s",
            exchange, symbol, channel, date_utc,
        )
        return ReclaimOutcome(outcome="fail_verify")

    # --- L1 unlink + sentinel write ---
    if not date_dir.exists():
        # L1 dir gone (already reclaimed by other path) — write sentinel, report ok
        _write_sentinel_atomic(sentinel)
        log.info(
            "[reclaim] ok (L1 dir absent, sentinel written) exchange=%s symbol=%s "
            "channel=%s date=%s",
            exchange, symbol, channel, date_utc,
        )
        return ReclaimOutcome(outcome="ok", files_unlinked=0, bytes_freed=0)

    l1_files = list(date_dir.rglob("part-*.parquet"))
    files_unlinked = 0
    bytes_freed = 0
    errors: list[str] = []

    for f in l1_files:
        try:
            size = f.stat().st_size
            f.unlink()
            files_unlinked += 1
            bytes_freed += size
        except FileNotFoundError:
            # Already gone (race with another process) — benign
            pass
        except OSError as e:
            errors.append(f"{f}: {e}")
            log.warning("[reclaim] unlink error %s: %s", f, e)

    # INV-F: sentinel write atomic (tempfile + os.replace)
    try:
        _write_sentinel_atomic(sentinel)
    except OSError as e:
        errors.append(f"sentinel write: {e}")
        log.warning("[reclaim] sentinel write failed: %s", e)

    log.info(
        "[reclaim] ok exchange=%s symbol=%s channel=%s date=%s "
        "files_unlinked=%d bytes_freed=%d errors=%d",
        exchange, symbol, channel, date_utc,
        files_unlinked, bytes_freed, len(errors),
    )

    # Emit metric
    from mctrader_data.metrics import historical_l1_reclaim_total
    historical_l1_reclaim_total.labels(
        exchange=exchange, channel=channel, outcome="ok"
    ).inc()

    return ReclaimOutcome(
        outcome="ok",
        files_unlinked=files_unlinked,
        bytes_freed=bytes_freed,
        errors=errors,
    )


def _write_sentinel_atomic(sentinel: Path) -> None:
    """INV-F: atomic sentinel write via tempfile + os.replace (POSIX atomic)."""
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    tmp = sentinel.with_suffix(".tmp")
    tmp.write_bytes(b"")
    os.replace(tmp, sentinel)


def emit_reclaim_metric(*, exchange: str, channel: str, outcome: ReclaimOutcomeLiteral) -> None:
    """Emit historical_l1_reclaim_total counter for non-ok outcomes (caller helper)."""
    from mctrader_data.metrics import historical_l1_reclaim_total
    historical_l1_reclaim_total.labels(
        exchange=exchange, channel=channel, outcome=outcome
    ).inc()
