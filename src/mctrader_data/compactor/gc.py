# src/mctrader_data/compactor/gc.py
"""GC: delete .compacted WAL segments older than 24h grace period."""
from __future__ import annotations

import logging
import time
from pathlib import Path

log = logging.getLogger(__name__)

GRACE_SECONDS = 86400  # 24 hours


def run_gc(root: Path) -> None:
    """Delete .compacted marker + original sealed segment after 24h grace."""
    now = time.time()
    wal_root = root / "wal"
    if not wal_root.exists():
        return
    for compacted in wal_root.rglob("*.ndjson.sealed.compacted"):
        if now - compacted.stat().st_mtime >= GRACE_SECONDS:
            sealed = Path(str(compacted)[: -len(".compacted")])
            try:
                if sealed.exists():
                    sealed.unlink()
                compacted.unlink()
                log.info("[gc] deleted %s", sealed.name)
            except Exception:
                log.exception("[gc] delete failed %s", compacted)
