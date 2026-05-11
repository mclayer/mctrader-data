# src/mctrader_data/wal/replay.py
"""WAL replay helpers (transaction tier) — MCT-140 Epic MCT-112 Story-6.

This module exposes the *atomic write* primitive that Story-7 (Compactor
transaction-tier) and any post-restart WAL replay path may reuse. It does NOT
re-implement the L1 Compactor — that lives under
``mctrader_data.compactor.l1``.

ADR-017 amendment §196 — ParquetWriter context-manager pattern:
1. write to ``<final>.tmp``
2. fsync the temp file
3. ``os.replace(<tmp>, <final>)`` — atomic on POSIX, near-atomic on Windows.

If any step raises, both the temp and the final path are cleaned up so the
caller never observes a partially-written Parquet artifact.
"""
from __future__ import annotations

import os
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Per-process counter to guarantee unique tmp suffixes under concurrent calls.
_tmp_counter_lock = threading.Lock()
_tmp_counter = 0


def _next_tmp_suffix() -> str:
    global _tmp_counter
    with _tmp_counter_lock:
        _tmp_counter += 1
        return f".{os.getpid()}.{threading.get_ident()}.{_tmp_counter}.tmp"


def _write_table_to_parquet(table: pa.Table, path: Path) -> None:
    """Write ``table`` to ``path`` (a temp file) and fsync the resulting file.

    The argument order mirrors PyArrow's :func:`pyarrow.parquet.write_table`
    so downstream tests can monkey-patch this helper to simulate writer
    failure modes (see ``test_replay_atomic_rename``).

    Windows note: ``os.fsync`` requires a file descriptor opened for write or
    read-write. We re-open the just-closed file with ``O_RDWR`` so the fsync
    is valid on both Win32 and POSIX (POSIX accepts O_RDONLY too, but the
    cross-platform contract is RDWR for fsync).
    """
    pq.write_table(table, str(path), compression="snappy")
    fd = os.open(str(path), os.O_RDWR)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_replace_parquet(final: Path, table: pa.Table) -> None:
    """Write ``table`` as Parquet at ``final`` atomically.

    Semantics:
    - On success: ``final`` exists with the Parquet payload; no tmp siblings remain.
    - On failure: ``final`` is absent (if it existed before, it is left untouched);
      any tmp siblings created by this call are removed; the original exception
      is re-raised.
    """
    final = Path(final)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_suffix(final.suffix + _next_tmp_suffix())
    try:
        _write_table_to_parquet(table, tmp)
        os.replace(str(tmp), str(final))
    except BaseException:
        # Best-effort cleanup of both tmp and (if any) a partial final.
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:  # pragma: no cover — best-effort
            pass
        try:
            # final was never replaced if we raised before os.replace; if the
            # caller's monkey-patched writer left a stub at the final path
            # before failure, remove it.
            if final.exists():
                final.unlink()
        except OSError:  # pragma: no cover — best-effort
            pass
        raise
