"""Tests for CoverageReport.node_coverage (MCT-93 X4)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mctrader_data.orderbook_replay import NodeCoverage, tier_coverage
from mctrader_data.tick_storage import TickRecord, TickWriter


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 5, 6, 0, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def _tick(seconds: int, side: str = "buy", price: str = "100000000") -> TickRecord:
    return TickRecord(
        ts_utc=_ts(seconds), received_at=_ts(seconds),
        exchange="bithumb", symbol="KRW-BTC",
        price=Decimal(price), quantity=Decimal("0.01"),
        side=side, raw_json=None,
    )


def test_tier_coverage_node_breakdown_two_nodes(tmp_path: Path) -> None:
    wa = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        snapshot_id="ign", node_id="NODE_A", collector_run_id="NODE_A-A",
    )
    wa.append(_tick(0))
    wa.append(_tick(10))
    wa.close()

    wb = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        snapshot_id="ign", node_id="NODE_B", collector_run_id="NODE_B-A",
    )
    wb.append(_tick(5))
    wb.append(_tick(15))
    wb.close()

    report = tier_coverage(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", tier="tick",
        start=_ts(0), end=_ts(60),
    )
    assert "NODE_A" in report.node_coverage
    assert "NODE_B" in report.node_coverage
    assert isinstance(report.node_coverage["NODE_A"], NodeCoverage)
    assert report.node_coverage["NODE_A"].collector_run_ids == ["NODE_A-A"]
    assert report.node_coverage["NODE_B"].collector_run_ids == ["NODE_B-A"]


def test_tier_coverage_legacy_node_default_sentinel(tmp_path: Path) -> None:
    """Legacy partition (no node= level) → node_coverage[zzz_DEFAULT]."""
    w = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        snapshot_id="legacy",
    )
    w.append(_tick(0))
    w.close()

    report = tier_coverage(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", tier="tick",
        start=_ts(0), end=_ts(60),
    )
    assert "zzz_DEFAULT" in report.node_coverage


def test_tier_coverage_backward_compat_existing_fields(tmp_path: Path) -> None:
    """기존 7 field 변경 0 + node_coverage default empty."""
    w = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        snapshot_id="legacy-s1",
    )
    w.append(_tick(0))
    w.append(_tick(10))
    w.close()

    report = tier_coverage(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", tier="tick",
        start=_ts(0), end=_ts(60),
    )
    assert report.symbol == "KRW-BTC"
    assert report.tier == "tick"
    assert report.min_ts_utc == _ts(0)
    assert report.max_ts_utc == _ts(10)
    assert report.gaps == []
    assert "legacy-s1" in report.collector_run_ids
    assert isinstance(report.node_coverage, dict)


def test_tier_coverage_empty_partition_node_coverage_empty(tmp_path: Path) -> None:
    """empty partition → node_coverage = {} (no entries)."""
    report = tier_coverage(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", tier="tick",
        start=_ts(0), end=_ts(60),
    )
    assert report.node_coverage == {}
