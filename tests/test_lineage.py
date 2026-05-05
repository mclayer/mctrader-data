"""Lineage sidecar (`_lineage_*.json`) write/read tests."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from mctrader_data.lineage import read_lineage, write_lineage


def test_write_and_read_lineage(tmp_path: Path) -> None:
    target = write_lineage(
        partition_dir=tmp_path,
        snapshot_id="snap-1",
        exchange="bithumb",
        endpoint="public/candlestick/BTC_KRW/1h",
        request_params_hash="abc123",
        fetched_at_utc=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
        response_hash="def456",
        adapter_name="mctrader-market-bithumb",
        adapter_version="0.1.0",
    )

    assert target.exists()
    assert target.name == "_lineage_snap-1.json"

    loaded = read_lineage(target)
    assert loaded["snapshot_id"] == "snap-1"
    assert loaded["exchange"] == "bithumb"
    assert loaded["endpoint"] == "public/candlestick/BTC_KRW/1h"
    assert loaded["fetched_at_utc"].endswith("Z")
    assert loaded["adapter_name"] == "mctrader-market-bithumb"
    assert loaded["adapter_version"] == "0.1.0"


def test_multiple_snapshots_coexist(tmp_path: Path) -> None:
    write_lineage(
        partition_dir=tmp_path,
        snapshot_id="snap-A",
        exchange="bithumb",
        endpoint="x",
        request_params_hash="a",
        fetched_at_utc=datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc),
        response_hash="ah",
        adapter_name="x",
        adapter_version="0.1.0",
    )
    write_lineage(
        partition_dir=tmp_path,
        snapshot_id="snap-B",
        exchange="bithumb",
        endpoint="x",
        request_params_hash="b",
        fetched_at_utc=datetime(2026, 5, 1, 1, 0, tzinfo=timezone.utc),
        response_hash="bh",
        adapter_name="x",
        adapter_version="0.1.0",
    )
    assert (tmp_path / "_lineage_snap-A.json").exists()
    assert (tmp_path / "_lineage_snap-B.json").exists()


# MCT-91 — node_id field in lineage
def test_lineage_with_node_id_field(tmp_path: Path) -> None:
    """node_id 명시 시 lineage payload 에 node_id field 포함."""
    target = write_lineage(
        partition_dir=tmp_path,
        snapshot_id="snap-ha",
        exchange="bithumb",
        endpoint="x",
        request_params_hash="a",
        fetched_at_utc=datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc),
        response_hash="ah",
        adapter_name="x",
        adapter_version="0.1.0",
        node_id="NODE_A",
    )
    loaded = read_lineage(target)
    assert loaded["node_id"] == "NODE_A"


def test_lineage_without_node_id_legacy(tmp_path: Path) -> None:
    """node_id=None (default) 시 lineage payload 에 node_id field 미포함 (legacy)."""
    target = write_lineage(
        partition_dir=tmp_path,
        snapshot_id="snap-legacy",
        exchange="bithumb",
        endpoint="x",
        request_params_hash="a",
        fetched_at_utc=datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc),
        response_hash="ah",
        adapter_name="x",
        adapter_version="0.1.0",
    )
    loaded = read_lineage(target)
    assert "node_id" not in loaded
