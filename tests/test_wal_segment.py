# tests/test_wal_segment.py
"""Tests for wal/segment.py — INV-2 atomic seal."""
from __future__ import annotations

import os
from pathlib import Path


from mctrader_data.wal.segment import (
    active_segment_path,
    is_active,
    is_compacted,
    is_sealed,
    scan_sealed,
    seal_path,
    segment_index,
)


def test_segment_index_5min_boundary() -> None:
    # t=0..299 → idx 0; t=300..599 → idx 1
    assert segment_index(0.0) == 0
    assert segment_index(299.9) == 0
    assert segment_index(300.0) == 1
    assert segment_index(599.9) == 1
    assert segment_index(600.0) == 2


def test_segment_index_custom_seconds() -> None:
    assert segment_index(3600.0, segment_seconds=3600) == 1
    assert segment_index(7199.9, segment_seconds=3600) == 1


def test_active_segment_path_structure(tmp_path: Path) -> None:
    p = active_segment_path(
        root=tmp_path, exchange="bithumb", channel="transaction",
        symbol="KRW-BTC", date="2026-05-09",
        start_idx=0, node_id="NODE_A",
    )
    assert p.parent == tmp_path / "wal" / "bithumb" / "transaction" / "KRW-BTC" / "2026-05-09"
    assert p.name.startswith("segment-")
    assert p.name.endswith("-NODE_A.ndjson")


def test_seal_path_rename(tmp_path: Path) -> None:
    active = tmp_path / "segment-20260509T000000Z-NODE_A.ndjson"
    active.write_text("line\n")
    sealed = seal_path(active)
    assert sealed.name == "segment-20260509T000000Z-NODE_A.ndjson.sealed"
    os.replace(str(active), str(sealed))
    assert sealed.exists()
    assert not active.exists()


def test_is_active_sealed_compacted() -> None:
    assert is_active(Path("segment-20260509T000000Z-NODE.ndjson"))
    assert not is_active(Path("segment-20260509T000000Z-NODE.ndjson.sealed"))
    assert is_sealed(Path("segment-20260509T000000Z-NODE.ndjson.sealed"))
    assert not is_sealed(Path("segment-20260509T000000Z-NODE.ndjson"))
    assert is_compacted(Path("segment-20260509T000000Z-NODE.ndjson.sealed.compacted"))


def test_scan_sealed_returns_only_unsealed(tmp_path: Path) -> None:
    wal = tmp_path / "wal" / "bithumb" / "transaction" / "KRW-BTC" / "2026-05-09"
    wal.mkdir(parents=True)
    sealed = wal / "segment-20260509T000000Z-N.ndjson.sealed"
    compacted_sealed = wal / "segment-20260509T000500Z-N.ndjson.sealed"
    active = wal / "segment-20260509T001000Z-N.ndjson"
    for f in [sealed, compacted_sealed, active]:
        f.write_text("x\n")
    # Mark one as compacted
    (wal / "segment-20260509T000500Z-N.ndjson.sealed.compacted").write_text("")

    result = scan_sealed(tmp_path)
    assert result == [sealed]
