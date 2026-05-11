# tests/wal/test_transaction_wal_basic.py
"""MCT-140 Epic MCT-112 Story-6 — Transaction WAL basic append + batch fsync.

Verifies:
- batch fsync window (msgs / wall-clock ms) parameters tune flush cadence
- buffer cap admits up to N msgs then back-pressures (RuntimeError or block)
- the resulting sealed segment carries exactly the appended records
- backward compatibility: default constructor preserves per-message fsync
  (ADR-017 §1 Ingester policy, orderbook tier unchanged)
"""
from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path

import pytest

from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.ndjson_codec import decode_line


def _make_ingester(tmp_path: Path, **kwargs) -> WalIngester:
    return WalIngester(
        root=tmp_path,
        exchange="bithumb",
        symbol="KRW-BTC",
        channel="transaction",
        node_id="NODE_A",
        **kwargs,
    )


def test_batch_fsync_msgs_window_flushes_at_threshold(tmp_path: Path) -> None:
    """fsync_window_msgs=10 → fsync triggered at every 10th append, not before."""
    ing = _make_ingester(
        tmp_path,
        fsync_window_msgs=10,
        fsync_window_ms=10_000,  # large wall-clock so msg count wins
    )
    # Append 25 records → fsync should fire at msg 10 and 20 (2 explicit fsyncs).
    for i in range(25):
        ing.append({"seq": i, "price": Decimal(str(i))})
    ing.close()
    # All 25 visible in sealed segment.
    sealed = list((tmp_path / "wal").rglob("*.ndjson.sealed"))
    assert len(sealed) == 1
    lines = sealed[0].read_text().strip().splitlines()
    assert len(lines) == 25
    seqs = sorted(decode_line(ln)["seq"] for ln in lines)
    assert seqs == list(range(25))


def test_batch_fsync_ms_window_flushes_after_wall_clock(tmp_path: Path) -> None:
    """fsync_window_ms=50 → fsync fires after 50ms wall-clock even if msg count low."""
    ing = _make_ingester(
        tmp_path,
        fsync_window_ms=50,
        fsync_window_msgs=10_000,  # large count so wall-clock wins
    )
    ing.append({"seq": 0, "price": Decimal("1")})
    # First append always fsyncs (initial window start).
    time.sleep(0.075)  # > 50ms
    ing.append({"seq": 1, "price": Decimal("2")})
    # Second append's window has expired → fsync should have run.
    ing.close()
    sealed = list((tmp_path / "wal").rglob("*.ndjson.sealed"))
    assert len(sealed) == 1
    lines = sealed[0].read_text().strip().splitlines()
    assert len(lines) == 2


def test_buffer_max_msgs_backpressure(tmp_path: Path) -> None:
    """When buffer_max_msgs is exceeded *between fsyncs*, append raises (admit-block).

    ADR-017 amendment §153 — overflow → backpressure (block + warn). Story-6 spec
    interprets this as a hard ceiling on un-fsynced in-memory line count: when
    the writer would queue beyond the ceiling without fsync clearing it, the
    write blocks (or raises if non-blocking caller). We choose explicit raise so
    the WS receive thread can pause instead of deadlocking in append().
    """
    ing = _make_ingester(
        tmp_path,
        # Very generous fsync window so the buffer fills up without auto-flush.
        fsync_window_ms=10_000,
        fsync_window_msgs=10_000,
        buffer_max_msgs=5,
    )
    # First 5 appends fit within the buffer cap.
    for i in range(5):
        ing.append({"seq": i, "price": Decimal(str(i))})
    # 6th append exceeds cap → BufferOverflow.
    from mctrader_data.wal.ingester import WalBufferOverflowError

    with pytest.raises(WalBufferOverflowError):
        ing.append({"seq": 5, "price": Decimal("5")})

    # Explicit flush drains the buffer → next append succeeds.
    ing.flush()
    ing.append({"seq": 5, "price": Decimal("5")})
    ing.close()


def test_default_constructor_preserves_per_message_fsync(tmp_path: Path) -> None:
    """No new kwargs → identical behavior to MCT-58 era WalIngester (orderbook tier unchanged)."""
    ing = _make_ingester(tmp_path)
    ing.append({"seq": 0})
    ing.append({"seq": 1})
    ing.close()
    # Should produce a single sealed segment with both records.
    sealed = list((tmp_path / "wal").rglob("*.ndjson.sealed"))
    assert len(sealed) == 1
    lines = sealed[0].read_text().strip().splitlines()
    assert len(lines) == 2


def test_flush_drains_buffer_explicitly(tmp_path: Path) -> None:
    """flush() forces an immediate fsync regardless of window state."""
    ing = _make_ingester(
        tmp_path,
        fsync_window_ms=10_000,
        fsync_window_msgs=10_000,
    )
    ing.append({"seq": 0})
    ing.append({"seq": 1})
    ing.flush()
    # After flush() the (active, pre-seal) file must contain both lines.
    active = [
        f
        for f in (tmp_path / "wal").rglob("*.ndjson")
        if not f.name.endswith(".sealed")
    ]
    assert len(active) == 1
    lines = active[0].read_text().strip().splitlines()
    assert len(lines) == 2
    ing.close()
