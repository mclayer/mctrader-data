"""§D14 OrderbookSnapshotWriter + snapshot_event_to_snapshot_records tests (MCT-104)."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from mctrader_data.orderbook_snapshot_storage import (
    ORDERBOOK_SNAPSHOT_SCHEMA_VERSION,
    OrderbookSnapshotRecord,
    OrderbookSnapshotWriter,
    _compute_payload_hash,
    snapshot_event_to_snapshot_records,
)


def _make_snapshot_event(
    *,
    symbol: str = "KRW-BTC",
    ts_micro: int = 1778154976506519,
    n_levels: int = 3,
) -> SimpleNamespace:
    """Build a mock OrderbookSnapshotEvent."""
    ts = datetime.fromtimestamp(ts_micro / 1_000_000, tz=timezone.utc)
    received = ts

    class Level(SimpleNamespace):
        pass

    bids = [Level(price=Decimal(f"{118900000 - i * 100}"), quantity=Decimal(f"0.{i + 1}")) for i in range(n_levels)]
    asks = [Level(price=Decimal(f"{119000000 + i * 100}"), quantity=Decimal(f"0.{i + 1}")) for i in range(n_levels)]

    class Symbol(SimpleNamespace):
        def __str__(self) -> str:
            return symbol

    return SimpleNamespace(
        exchange="bithumb",
        symbol=Symbol(),
        event_time=ts,
        received_at=received,
        bids=bids,
        asks=asks,
        raw={"type": "orderbooksnapshot", "content": {"symbol": symbol.replace("-", "_")}},
    )


class TestSnapshotEventToRecords:
    def test_60_row_atomic_write(self) -> None:
        """30 bids + 30 asks = 60 rows exactly."""
        event = _make_snapshot_event(n_levels=30)
        records = snapshot_event_to_snapshot_records(event)
        assert len(records) == 60
        bid_records = [r for r in records if r.side == "bid"]
        ask_records = [r for r in records if r.side == "ask"]
        assert len(bid_records) == 30
        assert len(ask_records) == 30

    def test_baseline_seq_is_micro_epoch(self) -> None:
        ts_micro = 1778154976506519
        event = _make_snapshot_event(ts_micro=ts_micro)
        records = snapshot_event_to_snapshot_records(event)
        assert records[0].baseline_seq == ts_micro

    def test_payload_hash_deterministic(self) -> None:
        """Same event input → same payload_hash across two calls."""
        event1 = _make_snapshot_event()
        event2 = _make_snapshot_event()
        records1 = snapshot_event_to_snapshot_records(event1)
        records2 = snapshot_event_to_snapshot_records(event2)
        assert records1[0].payload_hash == records2[0].payload_hash

    def test_payload_hash_differs_on_content_change(self) -> None:
        event1 = _make_snapshot_event(n_levels=3)
        event2 = _make_snapshot_event(n_levels=5)
        r1 = snapshot_event_to_snapshot_records(event1)
        r2 = snapshot_event_to_snapshot_records(event2)
        assert r1[0].payload_hash != r2[0].payload_hash

    def test_level_indices_zero_based(self) -> None:
        event = _make_snapshot_event(n_levels=3)
        records = snapshot_event_to_snapshot_records(event)
        bid_levels = sorted([r.level for r in records if r.side == "bid"])
        ask_levels = sorted([r.level for r in records if r.side == "ask"])
        assert bid_levels == [0, 1, 2]
        assert ask_levels == [0, 1, 2]

    def test_schema_columns_present(self) -> None:
        event = _make_snapshot_event()
        records = snapshot_event_to_snapshot_records(event)
        r = records[0]
        assert isinstance(r.ts_utc, datetime)
        assert isinstance(r.received_at, datetime)
        assert isinstance(r.exchange, str)
        assert isinstance(r.symbol, str)
        assert isinstance(r.baseline_seq, int)
        assert isinstance(r.side, str)
        assert isinstance(r.level, int)
        assert isinstance(r.price, Decimal)
        assert isinstance(r.quantity, Decimal)
        assert isinstance(r.payload_hash, str)
        assert len(r.payload_hash) == 16


class TestOrderbookSnapshotWriter:
    def test_parquet_round_trip(self, tmp_path: Path) -> None:
        """Write records → read back → schema + values match."""
        event = _make_snapshot_event(n_levels=30)
        records = snapshot_event_to_snapshot_records(event)

        writer = OrderbookSnapshotWriter(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            snapshot_id="test-snap",
        )
        writer.append_many(records)
        writer.close()

        assert writer.current_path is not None
        assert writer.current_path.exists()

        table = pq.ParquetFile(writer.current_path).read()
        assert len(table) == 60
        col_names = table.schema.names
        assert "baseline_seq" in col_names
        assert "payload_hash" in col_names
        assert "side" in col_names

    def test_compression_is_zstd(self, tmp_path: Path) -> None:
        event = _make_snapshot_event()
        records = snapshot_event_to_snapshot_records(event)

        writer = OrderbookSnapshotWriter(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            snapshot_id="test-snap",
        )
        writer.append_many(records)
        writer.close()

        pf = pq.ParquetFile(writer.current_path)
        for rg_idx in range(pf.num_row_groups):
            for col_idx in range(pf.metadata.row_group(rg_idx).num_columns):
                codec = pf.metadata.row_group(rg_idx).column(col_idx).compression
                assert codec.lower() in ("zstd", "snappy")  # zstd preferred

    def test_hive_partition_path(self, tmp_path: Path) -> None:
        event = _make_snapshot_event()
        records = snapshot_event_to_snapshot_records(event)

        writer = OrderbookSnapshotWriter(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            snapshot_id="test-snap", node_id="node-A", collector_run_id="run-001",
        )
        writer.append_many(records)
        writer.close()

        path = writer.current_path
        assert path is not None
        parts = path.parts
        assert f"schema_version={ORDERBOOK_SNAPSHOT_SCHEMA_VERSION}" in parts
        assert "exchange=bithumb" in parts
        assert "symbol=KRW-BTC" in parts
        assert "node=node-A" in parts

    def test_1sec_throttle_drops_fast_events(self, tmp_path: Path) -> None:
        """§D14.10: second event within 1 second is throttled (last-write-wins)."""
        event = _make_snapshot_event()

        writer = OrderbookSnapshotWriter(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            snapshot_id="test-snap",
        )
        t = 1000.0
        accepted1 = writer.append_event(event, monotonic_now=t)
        accepted2 = writer.append_event(event, monotonic_now=t + 0.2)  # 200ms — throttled
        accepted3 = writer.append_event(event, monotonic_now=t + 1.1)  # 1.1s — accepted

        assert accepted1 is True
        assert accepted2 is False
        assert accepted3 is True
        writer.close()

    def test_throttle_resets_after_1sec(self, tmp_path: Path) -> None:
        event = _make_snapshot_event()
        writer = OrderbookSnapshotWriter(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            snapshot_id="test-snap",
        )
        t = 0.0
        results = []
        for offset in [0.0, 0.5, 1.0, 1.5, 2.0]:
            results.append(writer.append_event(event, monotonic_now=t + offset))
        writer.close()
        # accepted: 0.0, 1.0, 2.0 → 3 accepted, 0.5 and 1.5 throttled
        assert results[0] is True   # 0.0
        assert results[1] is False  # 0.5 (within 1s)
        assert results[2] is True   # 1.0 (exactly 1s boundary — >= 1.0)
        assert results[3] is False  # 1.5 (within 1s of 1.0)
        assert results[4] is True   # 2.0 (1s after 1.0)


class TestComputePayloadHash:
    def test_deterministic(self) -> None:
        h1 = _compute_payload_hash("bithumb", "KRW-BTC", 1778154976506519, [], [])
        h2 = _compute_payload_hash("bithumb", "KRW-BTC", 1778154976506519, [], [])
        assert h1 == h2
        assert len(h1) == 16

    def test_differs_on_symbol_change(self) -> None:
        h1 = _compute_payload_hash("bithumb", "KRW-BTC", 123, [], [])
        h2 = _compute_payload_hash("bithumb", "KRW-ETH", 123, [], [])
        assert h1 != h2
