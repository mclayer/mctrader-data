"""E2E active-active writer test (MCT-91 Phase 2 Task 8).

Two collector "instances" (different node_id) writing to the same MCTRADER_DATA_ROOT.
Validates Epic AC B1 (X2 부분):
- per-node partition write contention 0
- 양 partition (NODE_A / NODE_B) 모두 정상 row 보존
- heartbeat 양 file 모두 atomic write + schema_version="heartbeat.v1"
- collector_run_id format = {node_id}-{UTC_compact_ts} (manifest 양쪽 cross-reference)

mock event source = 직접 TickRecord / OrderbookEventRecord 를 buffer 에 feed.
WS / asyncio collector 전체 mock 은 Task 7 의 collector test 가 cover — 본 E2E 는 storage
layer 의 contention + integrity 만 검증.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from mctrader_data.heartbeat import HeartbeatWriter
from mctrader_data.manifest import (
    CollectorManifest,
    derive_collector_run_id,
    write_manifest,
)
from mctrader_data.orderbook_storage import OrderbookEventRecord, OrderbookWriter
from mctrader_data.tick_storage import TickRecord, TickWriter


def _ts(offset_ms: int = 0) -> datetime:
    return datetime(2026, 5, 5, 12, 0, 0, tzinfo=timezone.utc) + timedelta(
        milliseconds=offset_ms
    )


def _tick(offset_ms: int) -> TickRecord:
    """Identical event from Bithumb stream — both nodes receive same data."""
    return TickRecord(
        ts_utc=_ts(offset_ms), received_at=_ts(offset_ms),
        exchange="bithumb", symbol="KRW-BTC",
        price=Decimal("100000000") + Decimal(offset_ms),
        quantity=Decimal("0.01"),
        side="buy", raw_json=f'{{"id":{offset_ms}}}',
    )


def _ob_snapshot(level: int) -> OrderbookEventRecord:
    return OrderbookEventRecord(
        ts_utc=_ts(0), received_at=_ts(0),
        exchange="bithumb", symbol="KRW-BTC",
        event_type="snapshot", side="bid", level=level,
        price=Decimal("100000000") + Decimal(level), quantity=Decimal("0.05"),
    )


@pytest.mark.asyncio
async def test_two_writers_no_contention_t1_t2_t3(tmp_path: Path) -> None:
    """양 node 가 같은 root 에 동시 write — file collision 0 + 양 partition 정상 row."""
    started_a = datetime(2026, 5, 5, 12, 0, 0, tzinfo=timezone.utc)
    started_b = datetime(2026, 5, 5, 12, 0, 1, tzinfo=timezone.utc)

    run_a = derive_collector_run_id(
        started_at_utc=started_a, exchange="bithumb",
        selected_symbols=["KRW-BTC"], node_id="NODE_A",
    )
    run_b = derive_collector_run_id(
        started_at_utc=started_b, exchange="bithumb",
        selected_symbols=["KRW-BTC"], node_id="NODE_B",
    )
    assert run_a.startswith("NODE_A-")
    assert run_b.startswith("NODE_B-")
    assert run_a != run_b

    # T2 — two TickWriter instances (different node_id) 같은 root
    tw_a = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_A", collector_run_id=run_a,
    )
    tw_b = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_B", collector_run_id=run_b,
    )
    # 같은 source 의 5 event 를 양 writer 에 동일하게 feed
    for ms in (0, 100, 200, 300, 400):
        tw_a.append(_tick(ms))
        tw_b.append(_tick(ms))
    tw_a.close()
    tw_b.close()

    # T3 — two OrderbookWriter instances
    ow_a = OrderbookWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_A", collector_run_id=run_a,
    )
    ow_b = OrderbookWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_B", collector_run_id=run_b,
    )
    for level in range(3):
        ow_a.append(_ob_snapshot(level))
        ow_b.append(_ob_snapshot(level))
    ow_a.close()
    ow_b.close()

    # Heartbeat — 두 node 모두 atomic write
    hb_a = HeartbeatWriter(root=tmp_path, node_id="NODE_A", interval_seconds=0.1)
    hb_b = HeartbeatWriter(root=tmp_path, node_id="NODE_B", interval_seconds=0.1)
    hb_a.set_collector_run_id(run_a)
    hb_b.set_collector_run_id(run_b)
    await hb_a.write_once()
    await hb_b.write_once()

    # Manifest — 양 node 모두 persist
    manifest_a = CollectorManifest(
        collector_run_id=run_a, started_at_utc=started_a,
        exchange="bithumb", selected_symbols=["KRW-BTC"],  # type: ignore[arg-type]
        channels=["transaction", "orderbookdepth"],  # type: ignore[arg-type]
        selection_method="explicit", top_n=None, node_id="NODE_A",
    )
    manifest_b = CollectorManifest(
        collector_run_id=run_b, started_at_utc=started_b,
        exchange="bithumb", selected_symbols=["KRW-BTC"],  # type: ignore[arg-type]
        channels=["transaction", "orderbookdepth"],  # type: ignore[arg-type]
        selection_method="explicit", top_n=None, node_id="NODE_B",
    )
    write_manifest(tmp_path, manifest_a)
    write_manifest(tmp_path, manifest_b)

    # === Assertion ===

    # 1) T2 partition file separation (write contention 0)
    tick_files = sorted((tmp_path / "market" / "ticks").rglob("*.parquet"))
    assert len(tick_files) == 2
    assert any("node=NODE_A" in p.as_posix() for p in tick_files)
    assert any("node=NODE_B" in p.as_posix() for p in tick_files)

    # 2) T3 partition file separation
    ob_files = sorted((tmp_path / "market" / "orderbook").rglob("*.parquet"))
    assert len(ob_files) == 2
    assert any("node=NODE_A" in p.as_posix() for p in ob_files)
    assert any("node=NODE_B" in p.as_posix() for p in ob_files)

    # 3) 양 node 의 T2 row 갯수 동일 (5)
    for p in tick_files:
        table = pq.ParquetFile(p).read()
        assert table.num_rows == 5, f"{p} has {table.num_rows} rows (expected 5)"
    # 4) 양 node 의 T3 row 갯수 동일 (3)
    for p in ob_files:
        table = pq.ParquetFile(p).read()
        assert table.num_rows == 3, f"{p} has {table.num_rows} rows (expected 3)"

    # 5) Heartbeat 양 file
    manifest_dir = tmp_path / "market" / "manifest"
    hb_a_path = manifest_dir / "heartbeat-NODE_A.json"
    hb_b_path = manifest_dir / "heartbeat-NODE_B.json"
    assert hb_a_path.exists() and hb_b_path.exists()
    for p, expected_node, expected_run_id in [
        (hb_a_path, "NODE_A", run_a),
        (hb_b_path, "NODE_B", run_b),
    ]:
        data = json.loads(p.read_text(encoding="utf-8"))
        assert data["schema_version"] == "heartbeat.v1"
        assert data["node_id"] == expected_node
        assert data["collector_run_id"] == expected_run_id

    # 6) Manifest 양 file (cross-reference key 일관)
    manifest_files = sorted((manifest_dir).glob("run-*.json"))
    assert len(manifest_files) == 2
    for mp in manifest_files:
        loaded = CollectorManifest.model_validate_json(mp.read_text(encoding="utf-8"))
        assert loaded.node_id in {"NODE_A", "NODE_B"}
        # heartbeat collector_run_id 와 manifest collector_run_id 동일
        assert loaded.collector_run_id in {run_a, run_b}


def test_t1_byte_identical_two_node_partitions(tmp_path: Path) -> None:
    """T1 closed candle 의 byte-identical write — 양 node 가 같은 stream 에서 write 시.

    NOTE: 양 node 의 parquet 가 row level 에서 동일 값 (Decimal 보존)을 가져야 함.
    """
    from mctrader_data.storage import write_candles
    from mctrader_market.candle import CandleModel
    from mctrader_market.types import Symbol, Timeframe

    base_ts = datetime(2026, 5, 5, 12, 0, 0, tzinfo=timezone.utc)
    candles = [
        CandleModel(
            ts_utc=base_ts + timedelta(hours=i),
            exchange="bithumb",
            symbol=Symbol(base="BTC", quote="KRW"),
            timeframe=Timeframe.H1,
            open=Decimal("100000000"),
            high=Decimal("100100000"),
            low=Decimal("99900000"),
            close=Decimal("100050000"),
            volume=Decimal("1.5"),
            value=None,
        )
        for i in range(3)
    ]

    started_a = base_ts
    started_b = base_ts + timedelta(seconds=2)
    run_a = derive_collector_run_id(
        started_at_utc=started_a, exchange="bithumb",
        selected_symbols=["KRW-BTC"], node_id="NODE_A",
    )
    run_b = derive_collector_run_id(
        started_at_utc=started_b, exchange="bithumb",
        selected_symbols=["KRW-BTC"], node_id="NODE_B",
    )

    write_candles(
        candles, root=tmp_path, snapshot_id="ignored",
        node_id="NODE_A", collector_run_id=run_a, batch_seq=0,
    )
    write_candles(
        candles, root=tmp_path, snapshot_id="ignored",
        node_id="NODE_B", collector_run_id=run_b, batch_seq=0,
    )

    # 양 partition file 존재
    candle_files = sorted((tmp_path / "market" / "ohlcv").rglob("*.parquet"))
    assert len(candle_files) == 2
    assert any("node=NODE_A" in p.as_posix() for p in candle_files)
    assert any("node=NODE_B" in p.as_posix() for p in candle_files)

    # 양 partition 의 row count + 모든 column row-level equality (T1 byte-identical 의무)
    # — Codex F-4 NIT ADOPT: close 만 비교 → 전체 column equality 로 강화.
    table_a = pq.ParquetFile(
        next(p for p in candle_files if "node=NODE_A" in p.as_posix())
    ).read()
    table_b = pq.ParquetFile(
        next(p for p in candle_files if "node=NODE_B" in p.as_posix())
    ).read()
    assert table_a.num_rows == table_b.num_rows == 3
    # 모든 column (logical key + value column) row-level equality
    full_row_columns = ["ts_utc", "open", "high", "low", "close", "volume", "value", "schema_version"]
    for col in full_row_columns:
        if col not in table_a.schema.names:
            continue
        # Decimal128 + timestamp 모두 stringify 후 비교 (Decimal repr 일관)
        col_a = [str(v) if v is not None else None for v in table_a[col].to_pylist()]
        col_b = [str(v) if v is not None else None for v in table_b[col].to_pylist()]
        assert col_a == col_b, (
            f"T1 byte-identical violation: column {col!r} mismatch between NODE_A vs NODE_B "
            f"(NODE_A={col_a}, NODE_B={col_b})"
        )
