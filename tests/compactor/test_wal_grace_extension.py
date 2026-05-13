# tests/compactor/test_wal_grace_extension.py
"""MCT-141 — WAL grace 24h → 7d extension on archive failure.

Story-6 협조 항목:
- normal path: 24h grace (.compacted marker → delete)  ← legacy behavior, unchanged
- archive_failed path: `_archive_failed` sentinel detected → 7d threshold + OpRisk alert

Sentinel: `<segment>.ndjson.sealed._archive_failed` (sibling marker)
"""
from __future__ import annotations

import os
import time
from pathlib import Path


from mctrader_data.compactor.gc_daemon import (
    ARCHIVE_FAILED_GRACE_SECONDS,
    NORMAL_GRACE_SECONDS,
    archive_failed_sentinel,
    run_gc_with_extension,
)


def _make_sealed(tmp_path: Path, age_seconds: float, archive_failed: bool = False) -> Path:
    """Create a fake .ndjson.sealed file aged `age_seconds` in the past."""
    wal_dir = tmp_path / "wal" / "bithumb" / "transaction" / "KRW-BTC" / "2026-05-12"
    wal_dir.mkdir(parents=True, exist_ok=True)
    sealed = wal_dir / "00000000.ndjson.sealed"
    sealed.write_text("{}\n", encoding="utf-8")
    compacted = Path(str(sealed) + ".compacted")
    compacted.touch()
    if archive_failed:
        sentinel = archive_failed_sentinel(sealed)
        sentinel.touch()
    # Age both files
    past = time.time() - age_seconds
    os.utime(sealed, (past, past))
    os.utime(compacted, (past, past))
    if archive_failed:
        sentinel = archive_failed_sentinel(sealed)
        os.utime(sentinel, (past, past))
    return sealed


def test_normal_grace_24h_threshold():
    assert NORMAL_GRACE_SECONDS == 86400
    assert ARCHIVE_FAILED_GRACE_SECONDS == 86400 * 7


def test_normal_path_deletes_after_24h(tmp_path: Path):
    sealed = _make_sealed(tmp_path, age_seconds=NORMAL_GRACE_SECONDS + 60)
    run_gc_with_extension(tmp_path)
    assert not sealed.exists()
    assert not Path(str(sealed) + ".compacted").exists()


def test_normal_path_keeps_before_24h(tmp_path: Path):
    sealed = _make_sealed(tmp_path, age_seconds=NORMAL_GRACE_SECONDS - 60)
    run_gc_with_extension(tmp_path)
    assert sealed.exists()


def test_archive_failed_extends_grace_to_7d(tmp_path: Path):
    """archive_failed sentinel → 24h-2d is still within extension, not deleted."""
    sealed = _make_sealed(tmp_path, age_seconds=NORMAL_GRACE_SECONDS + 60, archive_failed=True)
    run_gc_with_extension(tmp_path)
    assert sealed.exists(), "archive_failed sentinel must extend grace beyond 24h"
    sentinel = archive_failed_sentinel(sealed)
    assert sentinel.exists()


def test_archive_failed_deletes_after_7d(tmp_path: Path):
    sealed = _make_sealed(
        tmp_path, age_seconds=ARCHIVE_FAILED_GRACE_SECONDS + 60, archive_failed=True
    )
    alerts: list[dict] = []
    run_gc_with_extension(tmp_path, on_op_risk_alert=alerts.append)
    assert not sealed.exists()
    assert not archive_failed_sentinel(sealed).exists()
    # OpRisk alert emitted on 7d expiry
    assert len(alerts) == 1
    assert alerts[0]["reason"] == "archive_failed_7d_expired"


def test_archive_failed_sentinel_path():
    p = Path("/tmp/wal/x/00000000.ndjson.sealed")
    s = archive_failed_sentinel(p)
    assert s.name == "00000000.ndjson.sealed._archive_failed"
