# src/mctrader_data/compactor/gc_daemon.py
"""WAL grace daemon — 24h normal grace + 7d extension on archive failure (MCT-141).

Story-6 (MCT-140) forwarded open question:
- Normal compaction success → 24h grace then delete (legacy ``compactor.gc.run_gc``).
- Archive failure (MinIO upload / off-host copy raised) → ``_archive_failed``
  sentinel written next to the sealed segment; grace extends to 7d so an
  operator has a window to remediate before the source data disappears.
- After 7d the sealed segment is deleted anyway (caps unbounded growth) and an
  OperationalRiskArch alert is emitted via the optional callback.

Sentinel naming:
    <segment>.ndjson.sealed._archive_failed
    (sibling of the existing <segment>.ndjson.sealed.compacted marker)

Compose-time integration: the wrapper daemon can call :func:`run_gc_with_extension`
on the same 1h cadence as the existing GC tick. The OpRisk alert delivery
mechanism (Prometheus / Sentry / Webhook) is wired by the caller via
``on_op_risk_alert``.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable
from pathlib import Path

log = logging.getLogger(__name__)


NORMAL_GRACE_SECONDS: int = 86400  # 24h (matches legacy compactor.gc.GRACE_SECONDS)
ARCHIVE_FAILED_GRACE_SECONDS: int = 86400 * 7  # 7d
ARCHIVE_FAILED_SUFFIX: str = "._archive_failed"


def archive_failed_sentinel(sealed: Path) -> Path:
    """Path of the ``_archive_failed`` sentinel for a sealed WAL segment."""
    return Path(str(sealed) + ARCHIVE_FAILED_SUFFIX)


def run_gc_with_extension(
    root: Path,
    *,
    on_op_risk_alert: Callable[[dict], None] | None = None,
    now: float | None = None,
) -> None:
    """Run a single GC pass over ``root/wal/``.

    For each ``<segment>.ndjson.sealed.compacted`` marker:
    1. If a sibling ``_archive_failed`` sentinel exists → 7d grace
       - aged > 7d → delete sealed + compacted + sentinel + emit OpRisk alert
       - aged <= 7d → leave in place
    2. Otherwise (normal compaction) → 24h grace
       - aged > 24h → delete sealed + compacted
       - aged <= 24h → leave in place

    Parameters
    ----------
    root
        Hub data root. ``root/wal/`` is scanned recursively.
    on_op_risk_alert
        Optional callback fired when a sealed segment is force-deleted at
        the 7d archive-failed expiry. Receives a dict ``{"reason", "segment", "age_seconds"}``.
    now
        Override for deterministic tests; defaults to ``time.time()``.
    """
    now = now if now is not None else time.time()
    wal_root = root / "wal"
    if not wal_root.exists():
        return

    for compacted in wal_root.rglob("*.ndjson.sealed.compacted"):
        sealed = Path(str(compacted)[: -len(".compacted")])
        sentinel = archive_failed_sentinel(sealed)

        try:
            mtime = compacted.stat().st_mtime
        except OSError:
            continue
        age = now - mtime

        if sentinel.exists():
            if age >= ARCHIVE_FAILED_GRACE_SECONDS:
                _delete_segment(sealed, compacted, sentinel)
                if on_op_risk_alert is not None:
                    try:
                        on_op_risk_alert({
                            "reason": "archive_failed_7d_expired",
                            "segment": str(sealed),
                            "age_seconds": age,
                        })
                    except Exception:  # pragma: no cover — best-effort
                        log.exception("[gc_daemon] op_risk alert handler raised")
            # else: keep, extension still in effect
        else:
            if age >= NORMAL_GRACE_SECONDS:
                _delete_segment(sealed, compacted, None)


def _delete_segment(sealed: Path, compacted: Path, sentinel: Path | None) -> None:
    """Best-effort delete of all 3 artifacts (sealed + marker + sentinel)."""
    for p in (sealed, compacted, sentinel):
        if p is None:
            continue
        try:
            if p.exists():
                p.unlink()
        except Exception:
            log.exception("[gc_daemon] failed to delete %s", p)


__all__ = [
    "ARCHIVE_FAILED_GRACE_SECONDS",
    "ARCHIVE_FAILED_SUFFIX",
    "NORMAL_GRACE_SECONDS",
    "archive_failed_sentinel",
    "run_gc_with_extension",
]
