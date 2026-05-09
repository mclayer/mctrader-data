"""Tests for orderbook_replay (MCT-66) — scan + reconstruction + coverage."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mctrader_data.orderbook_replay import (
    CoverageReport,
    GapDetectedError,
    OrderbookSnapshot,
    ReconstructionError,
    get_orderbook_at,
    scan_orderbook_events,
    scan_ticks,
    tier_coverage,
)
from mctrader_data.orderbook_storage import OrderbookEventRecord, OrderbookWriter
from mctrader_data.tick_storage import TickRecord, TickWriter


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 5, 4, 0, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def _tick(seconds: int, side: str = "buy", price: str = "100000000") -> TickRecord:
    return TickRecord(
        ts_utc=_ts(seconds), received_at=_ts(seconds),
        exchange="bithumb", symbol="KRW-BTC",
        price=Decimal(price), quantity=Decimal("0.01"),
        side=side, raw_json=None,
    )


def _ob_snapshot_event(level: int, side: str, price: str, qty: str = "0.05", *, sec: int = 0) -> OrderbookEventRecord:
    return OrderbookEventRecord(
        ts_utc=_ts(sec), received_at=_ts(sec),
        exchange="bithumb", symbol="KRW-BTC",
        event_type="snapshot", side=side, level=level,
        price=Decimal(price), quantity=Decimal(qty),
    )


def _ob_delta_event(side: str, price: str, qty: str, *, sec: int = 1) -> OrderbookEventRecord:
    return OrderbookEventRecord(
        ts_utc=_ts(sec), received_at=_ts(sec),
        exchange="bithumb", symbol="KRW-BTC",
        event_type="delta", side=side, level=-1,
        price=Decimal(price), quantity=Decimal(qty),
    )


def _seed_ticks(tmp_path: Path, records: list[TickRecord]) -> None:
    w = TickWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="test_run")
    for r in records:
        w.append(r)
    w.close()


def _seed_orderbook(tmp_path: Path, records: list[OrderbookEventRecord]) -> None:
    w = OrderbookWriter(root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="test_run")
    w.append_many(records)
    w.close()


def test_scan_ticks_returns_records_in_window(tmp_path: Path) -> None:
    _seed_ticks(tmp_path, [_tick(0), _tick(10), _tick(20)])
    records = list(
        scan_ticks(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            start=_ts(0), end=_ts(15),
        )
    )
    assert len(records) == 2
    assert records[0].ts_utc == _ts(0)
    assert records[1].ts_utc == _ts(10)


def test_scan_ticks_simulated_clock_filters_future(tmp_path: Path) -> None:
    _seed_ticks(tmp_path, [_tick(0), _tick(10), _tick(20)])
    records = list(
        scan_ticks(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            start=_ts(0), end=_ts(60),
            simulated_clock=_ts(11),
        )
    )
    assert [r.ts_utc for r in records] == [_ts(0), _ts(10)]


def test_scan_ticks_deterministic_order(tmp_path: Path) -> None:
    _seed_ticks(tmp_path, [_tick(0), _tick(0, "sell"), _tick(0, "buy", "200000000")])
    records1 = [r.ts_utc for r in scan_ticks(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        start=_ts(0), end=_ts(60),
    )]
    records2 = [r.ts_utc for r in scan_ticks(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        start=_ts(0), end=_ts(60),
    )]
    assert records1 == records2


def test_scan_orderbook_events_returns_window(tmp_path: Path) -> None:
    events = [
        _ob_snapshot_event(0, "bid", "100000000", sec=0),
        _ob_snapshot_event(0, "ask", "100000010", sec=0),
        _ob_delta_event("bid", "100000005", "0.10", sec=5),
    ]
    _seed_orderbook(tmp_path, events)
    records = list(
        scan_orderbook_events(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            start=_ts(0), end=_ts(10),
        )
    )
    assert len(records) == 3


def test_get_orderbook_at_baseline_only(tmp_path: Path) -> None:
    events = [
        _ob_snapshot_event(0, "bid", "100000000", sec=0),
        _ob_snapshot_event(1, "bid", "99999990", sec=0),
        _ob_snapshot_event(0, "ask", "100000010", sec=0),
        _ob_snapshot_event(1, "ask", "100000020", sec=0),
    ]
    _seed_orderbook(tmp_path, events)
    snap = get_orderbook_at(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        ts_utc=_ts(0),
    )
    assert isinstance(snap, OrderbookSnapshot)
    assert snap.top_bid is not None
    assert snap.top_bid.price == Decimal("100000000")
    assert snap.top_ask is not None
    assert snap.top_ask.price == Decimal("100000010")
    assert len(snap.bids) == 2
    assert len(snap.asks) == 2


def test_get_orderbook_at_applies_delta_remove(tmp_path: Path) -> None:
    events = [
        _ob_snapshot_event(0, "bid", "100000000", sec=0),
        _ob_snapshot_event(0, "ask", "100000010", sec=0),
        _ob_delta_event("bid", "100000000", "0", sec=5),  # remove the bid level
    ]
    _seed_orderbook(tmp_path, events)
    snap = get_orderbook_at(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        ts_utc=_ts(10),
    )
    assert snap.top_bid is None  # bid removed by delta
    assert snap.top_ask is not None
    assert snap.top_ask.price == Decimal("100000010")


def test_get_orderbook_at_missing_baseline_raises(tmp_path: Path) -> None:
    # Orderbook events with only deltas — no snapshot baseline
    events = [
        _ob_delta_event("bid", "100000000", "0.01", sec=0),
    ]
    _seed_orderbook(tmp_path, events)
    with pytest.raises(ReconstructionError, match="missing baseline"):
        get_orderbook_at(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            ts_utc=_ts(10),
        )


def test_get_orderbook_at_no_events_raises(tmp_path: Path) -> None:
    with pytest.raises(ReconstructionError, match="no orderbook events"):
        get_orderbook_at(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            ts_utc=_ts(10),
        )


def test_get_orderbook_at_gap_detected_raises(tmp_path: Path) -> None:
    events = [
        _ob_snapshot_event(0, "bid", "100000000", sec=0),
        _ob_snapshot_event(0, "ask", "100000010", sec=0),
        _ob_delta_event("bid", "100000005", "0.05", sec=600),  # 10min gap
    ]
    _seed_orderbook(tmp_path, events)
    with pytest.raises(GapDetectedError, match="gap detected"):
        get_orderbook_at(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            ts_utc=_ts(700),
            gap_threshold_seconds=300.0,
        )


def test_tier_coverage_tick_basic(tmp_path: Path) -> None:
    _seed_ticks(tmp_path, [_tick(0), _tick(10), _tick(20)])
    report = tier_coverage(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", tier="tick",
        start=_ts(0), end=_ts(60),
    )
    assert isinstance(report, CoverageReport)
    assert report.symbol == "KRW-BTC"
    assert report.tier == "tick"
    assert report.min_ts_utc == _ts(0)
    assert report.max_ts_utc == _ts(20)
    assert report.gaps == []
    assert report.collector_run_ids == ["test_run"]


def test_tier_coverage_detects_gap(tmp_path: Path) -> None:
    _seed_ticks(tmp_path, [_tick(0), _tick(700)])  # 700s gap > 300s default
    report = tier_coverage(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", tier="tick",
        start=_ts(0), end=_ts(1000),
    )
    assert len(report.gaps) == 1
    assert report.gaps[0].gap_seconds == pytest.approx(700.0)


def test_tier_coverage_empty_partition(tmp_path: Path) -> None:
    report = tier_coverage(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", tier="tick",
        start=_ts(0), end=_ts(60),
    )
    assert report.min_ts_utc is None
    assert report.max_ts_utc is None
    assert report.gaps == []
    assert report.collector_run_ids == []


# MCT-92 — multi-node scan + dedup + tier_coverage 신규 file naming 호환
def test_scan_ticks_two_nodes_auto_dedup(tmp_path: Path) -> None:
    """양 node tick partition → multi-node 자동 감지 + dedup transparent."""
    wa = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ign",
        node_id="NODE_A", collector_run_id="NODE_A-A",
    )
    wa.append(_tick(0))
    wa.append(_tick(1))
    wa.close()
    wb = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ign",
        node_id="NODE_B", collector_run_id="NODE_B-A",
    )
    wb.append(_tick(0))
    wb.append(_tick(1))
    wb.close()

    result = list(scan_ticks(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        start=_ts(0), end=_ts(60),
    ))
    # dedup 후 2 row (양 node 동일 logical key idempotent skip)
    assert len(result) == 2


def test_tier_coverage_supports_new_file_naming(tmp_path: Path) -> None:
    """MCT-91 신규 file naming `{collector_run_id}-{batch_seq}.parquet` collector_run_ids harvest."""
    wa = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ign",
        node_id="NODE_A", collector_run_id="NODE_A-20260506T120000Z",
    )
    wa.append(_tick(0))
    wa.close()

    report = tier_coverage(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", tier="tick",
        start=_ts(0), end=_ts(60),
    )
    assert "NODE_A-20260506T120000Z" in report.collector_run_ids


def test_tier_coverage_legacy_part_naming_still_works(tmp_path: Path) -> None:
    """legacy part-{snapshot_id}.parquet backward compat."""
    w = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="legacy-s1",
    )
    w.append(_tick(0))
    w.close()

    report = tier_coverage(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", tier="tick",
        start=_ts(0), end=_ts(60),
    )
    assert "legacy-s1" in report.collector_run_ids


# ── §D14.7 baseline source priority tests (MCT-104) ───────────────────────────

def _write_d14_snapshot(tmp_path: Path, ts_sec: int, n_levels: int = 3) -> None:
    """Write a §D14 orderbook_snapshot.v1 parquet partition."""
    from types import SimpleNamespace
    from mctrader_data.orderbook_snapshot_storage import (
        OrderbookSnapshotWriter,
        snapshot_event_to_snapshot_records,
    )
    ts = _ts(ts_sec)

    class Sym(SimpleNamespace):
        def __str__(self) -> str:
            return "KRW-BTC"

    class Level(SimpleNamespace):
        pass

    event = SimpleNamespace(
        exchange="bithumb",
        symbol=Sym(),
        event_time=ts,
        received_at=ts,
        bids=[Level(price=Decimal(f"{118900000 - i * 100}"), quantity=Decimal("0.1")) for i in range(n_levels)],
        asks=[Level(price=Decimal(f"{119000000 + i * 100}"), quantity=Decimal("0.1")) for i in range(n_levels)],
        raw={},
    )
    records = snapshot_event_to_snapshot_records(event)
    writer = OrderbookSnapshotWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        snapshot_id=f"d14-snap-{ts_sec}",
    )
    writer.append_many(records)
    writer.close()


def test_get_orderbook_at_d14_baseline_preferred_over_d11(tmp_path: Path) -> None:
    """§D14.7: when §D14 snapshot exists, it is preferred over §D11 snapshot."""
    ob_w = OrderbookWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="d11-snap",
    )
    # §D11 snapshot at sec=0: single bid/ask level
    ob_w.append(_ob_snapshot_event(0, "bid", "100000000", sec=0))
    ob_w.append(_ob_snapshot_event(0, "ask", "101000000", sec=0))
    ob_w.append(_ob_delta_event("bid", "100100000", "0.02", sec=5))
    ob_w.close()

    # §D14 snapshot at sec=2: newer, higher prices (should win)
    _write_d14_snapshot(tmp_path, ts_sec=2, n_levels=3)

    # Request at sec=10 (after both baselines + delta)
    result = get_orderbook_at(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        ts_utc=_ts(10),
    )
    # §D14 baseline has bids starting at 118900000; §D11 has 100000000
    # If §D14 was used as baseline, top bid ≥ 118800000
    assert result.top_bid is not None
    assert result.top_bid.price >= Decimal("118800000")


def test_get_orderbook_at_d11_fallback_when_d14_absent(tmp_path: Path) -> None:
    """§D14.7: when §D14 partition absent, falls back to §D11 snapshot."""
    ob_w = OrderbookWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="d11-only",
    )
    ob_w.append(_ob_snapshot_event(0, "bid", "100000000", sec=0))
    ob_w.append(_ob_snapshot_event(0, "ask", "101000000", sec=0))
    ob_w.close()

    result = get_orderbook_at(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        ts_utc=_ts(1),
    )
    assert result.top_bid is not None
    assert result.top_bid.price == Decimal("100000000")
