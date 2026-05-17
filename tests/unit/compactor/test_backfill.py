# tests/unit/compactor/test_backfill.py
"""TDD unit tests for MCT-173 backfill module.

4 test cases (D7=C TDD mandate):
1. test_iter_frozen_segments_sealed_only: .sealed only, .compacted skip (D4=A), .active skip
2. test_iter_frozen_segments_idempotency: .compacted sentinel → skip (INV-2)
3. test_write_manifest_frontmatter: BackfillManifest + YAML frontmatter (D5=B, INV-4)
4. test_schema_compat_ob_snapshot: iter output schema == L1Compactor orderbooksnapshot schema (INV-3)
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import yaml


# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_sealed_segment(
    date_dir: Path,
    ts_str: str = "20260513T120000Z",
    node_id: str = "N1",  # short node_id — Windows MAX_PATH=260 guard (ts-prefix +17 chars)
    content: str = "",
    add_compacted: bool = False,
) -> Path:
    """Create a .ndjson.sealed segment with optional .compacted marker."""
    filename = f"segment-{ts_str}-{node_id}.ndjson.sealed"
    seg = date_dir / filename
    seg.write_text(content, encoding="utf-8")
    if add_compacted:
        (date_dir / (filename + ".compacted")).touch()
    return seg


def _ob_snapshot_line(
    ts: str = "2026-05-13T12:00:00+00:00",
    exchange: str = "upbit",
    symbol: str = "KRW-BTC",
) -> str:
    """Return a minimal orderbooksnapshot NDJSON line."""
    record = {
        "ts_utc": ts,
        "received_at": ts,
        "exchange": exchange,
        "symbol": symbol,
        "bids": [{"price": "50000000", "quantity": "0.001"}],
        "asks": [{"price": "50001000", "quantity": "0.002"}],
        "raw_json": None,
    }
    return json.dumps(record)


# ─── test 1: iterator returns sealed-only, skips .compacted ──────────────────


def test_iter_frozen_segments_sealed_only(tmp_path: Path) -> None:
    """iter_frozen_segments() returns .sealed segments without .compacted marker.

    Verifies:
    - .ndjson.sealed without .compacted → included
    - .ndjson.sealed with .compacted → excluded (D4=A idempotency)
    - .ndjson active → excluded
    """
    from mctrader_data.compactor.backfill import iter_frozen_segments

    wal_root = tmp_path / "wal"
    exchange = "upbit"
    channel = "orderbooksnapshot"
    date_dir = wal_root / exchange / channel / "KRW-BTC" / "2026-05-13"
    date_dir.mkdir(parents=True)

    # sealed without compacted → should appear
    seg_target = _make_sealed_segment(date_dir, ts_str="20260513T120000Z")

    # sealed with compacted → should be skipped (D4=A)
    _make_sealed_segment(date_dir, ts_str="20260513T120500Z", add_compacted=True)

    # active (no .sealed) → should be skipped
    (date_dir / "segment-20260513T121000Z-NODE_TEST.ndjson").write_text("", encoding="utf-8")

    results = list(iter_frozen_segments(wal_root, exchange, channel))

    assert len(results) == 1, f"Expected 1 result, got {len(results)}: {results}"
    assert results[0] == seg_target


# ─── test 2: idempotency — .compacted sentinel skip ──────────────────────────


def test_iter_frozen_segments_idempotency(tmp_path: Path) -> None:
    """Repeated backfill on already-processed segments = 0 results (INV-2).

    After backfill marks a segment with .compacted, re-running iter_frozen_segments
    for the same WAL must return 0 segments for that segment.
    """
    from mctrader_data.compactor.backfill import iter_frozen_segments

    wal_root = tmp_path / "wal"
    exchange = "upbit"
    channel = "orderbooksnapshot"
    date_dir = wal_root / exchange / channel / "KRW-ETH" / "2026-05-13"
    date_dir.mkdir(parents=True)

    # Create sealed segment
    seg = _make_sealed_segment(date_dir, ts_str="20260513T130000Z")
    assert len(list(iter_frozen_segments(wal_root, exchange, channel))) == 1

    # Simulate backfill marking segment as compacted
    compacted_marker = Path(str(seg) + ".compacted")
    compacted_marker.touch()

    # Second run must return 0
    results = list(iter_frozen_segments(wal_root, exchange, channel))
    assert len(results) == 0, f"Expected 0 after compacted marker, got {results}"


# ─── test 3: manifest frontmatter YAML (D5=B, INV-4) ─────────────────────────


def test_write_manifest_frontmatter(tmp_path: Path) -> None:
    """BackfillManifest.write_manifest() produces valid YAML frontmatter (D5=B).

    Verifies:
    - YAML frontmatter present and parseable
    - Required fields: exchange, channel, date_range_start, date_range_end,
      segment_count, partial_boundary_symbols, created_at
    - INV-4: partial boundary박제 (date range = freeze start ~ MCT-166 LAND date)
    """
    from mctrader_data.compactor.backfill import BackfillManifest

    manifest = BackfillManifest(
        exchange="upbit",
        channel="orderbooksnapshot",
        date_range_start="2026-05-13",
        date_range_end="2026-05-14",
        segment_count=1922,
        segments_processed=1922,
        segments_skipped=1884,
        l1_parquets_created=1922,
        partial_boundary_symbols=["KRW-MATIC/2026-05-13"],
        created_at=datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
    )

    out_path = tmp_path / "backfill-manifest.yaml"
    manifest.write_manifest(out_path)

    content = out_path.read_text(encoding="utf-8")
    assert "---" in content, "YAML frontmatter delimiter must be present"

    # Parse YAML body
    # frontmatter is between first and second ---
    body = content.split("---")[1]
    parsed = yaml.safe_load(body)

    assert parsed["exchange"] == "upbit"
    assert parsed["channel"] == "orderbooksnapshot"
    assert parsed["date_range_start"] == "2026-05-13"
    assert parsed["date_range_end"] == "2026-05-14"
    assert parsed["segment_count"] == 1922
    assert "KRW-MATIC/2026-05-13" in parsed["partial_boundary_symbols"]
    assert "created_at" in parsed


# ─── test 4: schema compat — orderbooksnapshot (INV-3) ───────────────────────


def test_schema_compat_ob_snapshot() -> None:
    """backfill iter output processed through L1Compactor yields OB snapshot schema (INV-3).

    Verifies that a backfill-sourced sealed segment compacted via L1Compactor
    produces the same Arrow schema as the normal MCT-166 path B output.

    Uses tempfile.TemporaryDirectory for shorter root — ts-prefix adds ~17 chars to parquet
    filenames; Windows MAX_PATH=260 requires shorter base than pytest tmp_path.
    """
    from mctrader_data.compactor.backfill import iter_frozen_segments
    from mctrader_data.compactor.l1 import L1Compactor
    from mctrader_data.orderbook_snapshot_storage import _OB_SNAPSHOT_SCHEMA

    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        wal_root = root / "wal"
        exchange = "upbit"
        channel = "orderbooksnapshot"
        date_dir = wal_root / exchange / channel / "KRW-BTC" / "2026-05-13"
        date_dir.mkdir(parents=True)

        # Write a valid orderbooksnapshot sealed segment
        line = _ob_snapshot_line(
            ts="2026-05-13T12:00:00+00:00",
            exchange=exchange,
            symbol="KRW-BTC",
        )
        seg = _make_sealed_segment(date_dir, ts_str="20260513T120000Z", content=line + "\n")

        # Verify iter_frozen_segments picks it up
        segments = list(iter_frozen_segments(wal_root, exchange, channel))
        assert len(segments) == 1
        assert segments[0] == seg

        # Compact via L1Compactor — must succeed and produce correct schema
        compactor = L1Compactor(root=root)
        parquet_path = compactor.compact_segment(seg)

        assert parquet_path.exists(), "L1 parquet must be created"

        import pyarrow.parquet as pq
        # Use ParquetFile (not read_table) to avoid Hive auto-discovery conflicts (ADR-009 §D2)
        table = pq.ParquetFile(str(parquet_path)).read()
        # Schema compatibility check: all required field names must match
        expected_names = set(_OB_SNAPSHOT_SCHEMA.names)
        actual_names = set(table.schema.names)
        assert expected_names == actual_names, (
            f"Schema mismatch (INV-3). Expected={expected_names}, Got={actual_names}"
        )

        # INV-2: .compacted marker must exist after compaction
        from mctrader_data.wal.segment import compacted_path
        assert compacted_path(seg).exists(), ".compacted sentinel must be created (INV-2)"
