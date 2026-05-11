# tests/wal/test_transaction_wal_disk_full.py
"""MCT-140 Story-6 — Disk-full graceful degradation.

When os.write() raises OSError(errno=ENOSPC), the WAL writer must:
1. surface the error to the caller (no silent loss)
2. NOT mark itself closed (caller may free disk and retry)
3. preserve already-fsynced records on disk (forward-only)

We simulate ENOSPC by monkey-patching os.write to raise OSError(ENOSPC).
"""
from __future__ import annotations

import errno
import os
from decimal import Decimal
from pathlib import Path

import pytest

from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.ndjson_codec import decode_line


def test_disk_full_raises_oserror_and_preserves_prior_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ing = WalIngester(
        root=tmp_path,
        exchange="bithumb",
        symbol="KRW-BTC",
        channel="transaction",
        node_id="DISK_FULL",
        fsync_window_ms=10,
        fsync_window_msgs=2,
    )
    # First two writes succeed normally.
    ing.append({"seq": 0, "price": Decimal("100")})
    ing.append({"seq": 1, "price": Decimal("101")})
    ing.flush()  # ensure both are fsynced before we break disk

    real_write = os.write
    fail_after_calls = {"n": 0}

    def fake_write(fd: int, data: bytes) -> int:
        fail_after_calls["n"] += 1
        if fail_after_calls["n"] >= 1:
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_write(fd, data)

    monkeypatch.setattr(os, "write", fake_write)

    with pytest.raises(OSError) as exc_info:
        ing.append({"seq": 2, "price": Decimal("102")})
    assert exc_info.value.errno == errno.ENOSPC

    # Writer must still report not-closed — caller may want to retry after
    # freeing space.
    assert not ing._closed  # noqa: SLF001 — explicit invariant check

    # Restore disk and close cleanly to inspect surviving records.
    monkeypatch.setattr(os, "write", real_write)
    ing.close()

    sealed = list((tmp_path / "wal").rglob("*.ndjson.sealed"))
    assert len(sealed) == 1
    seqs = sorted(decode_line(ln)["seq"] for ln in sealed[0].read_text().splitlines() if ln.strip())
    # The two pre-failure records must survive (forward-only).
    assert 0 in seqs and 1 in seqs
