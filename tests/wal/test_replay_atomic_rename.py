# tests/wal/test_replay_atomic_rename.py
"""MCT-140 Story-6 — WAL replay → Parquet atomic rename pattern.

ADR-017 amendment §196 — ParquetWriter context manager pattern
(tmp file → fsync footer → atomic rename to final). This Story-6 fixture
does NOT re-implement the full L1 Compactor (Story-7 owns that); it only
verifies that the *atomic rename helper* used during replay produces no
half-written parquet file under crash simulation.

The helper under test is ``mctrader_data.wal.replay.atomic_replace_parquet``
which wraps the MCT-132 pattern for transaction-tier writes.
"""
from __future__ import annotations

import contextlib
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.wal.replay import atomic_replace_parquet


def _tiny_table() -> pa.Table:
    return pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})


def test_atomic_replace_produces_final_parquet(tmp_path: Path) -> None:
    final = tmp_path / "part-001.parquet"
    table = _tiny_table()
    atomic_replace_parquet(final, table)
    assert final.exists()
    # Read back to verify validity.
    back = pq.read_table(final)
    assert back.column("x").to_pylist() == [1, 2, 3]


def test_atomic_replace_leaves_no_tmp_artifact_on_success(tmp_path: Path) -> None:
    final = tmp_path / "part-002.parquet"
    atomic_replace_parquet(final, _tiny_table())
    siblings = list(tmp_path.iterdir())
    assert siblings == [final]


def test_atomic_replace_no_partial_file_on_writer_failure(
    tmp_path: Path, monkeypatch
) -> None:
    """If ParquetWriter raises before commit, the final path must not exist
    AND any tmp must be cleaned up."""
    final = tmp_path / "part-003.parquet"

    class _BadTable:
        # An object that triggers an exception during pyarrow serialization.
        def to_batches(self) -> list:  # pragma: no cover — used by pyarrow internals
            raise RuntimeError("simulated writer failure")

    # Monkey-patch the write step to fail.
    import mctrader_data.wal.replay as replay_mod

    def _fail_writer(_t: pa.Table, path: Path) -> None:
        path.write_bytes(b"PAR1\x00\x00partial")  # leave a partial file
        raise RuntimeError("simulated writer failure")

    monkeypatch.setattr(replay_mod, "_write_table_to_parquet", _fail_writer)

    with contextlib.suppress(RuntimeError):
        atomic_replace_parquet(final, _tiny_table())
    # Final path must NOT be present (atomic guarantee).
    assert not final.exists()
    # Tmp siblings must be cleaned up.
    tmps = [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
    assert tmps == []


def test_concurrent_atomic_replace_no_corruption(tmp_path: Path) -> None:
    """Two threads write to distinct final paths concurrently — neither sees
    the other's tmp file as a final artifact."""
    n_threads = 5
    paths = [tmp_path / f"part-{i:03d}.parquet" for i in range(n_threads)]
    errors: list[Exception] = []

    def _worker(p: Path) -> None:
        try:
            atomic_replace_parquet(p, _tiny_table())
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=_worker, args=(p,)) for p in paths]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    for p in paths:
        assert p.exists()
        assert pq.read_table(p).column("x").to_pylist() == [1, 2, 3]
    # No stray tmp files.
    tmps = [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
    assert tmps == []
