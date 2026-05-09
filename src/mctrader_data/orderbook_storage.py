"""Forward-only orderbook event Parquet append-only storage (MCT-58).

ADR-009 amendment: ``market/orderbook/schema_version=orderbook.v1/exchange=.../
symbol=.../date=YYYY-MM-DD/part-{snapshot_id}.parquet`` Hive layout.

Stores **flat orderbook events** — both snapshot and delta types are flattened
into per-level rows for efficient append + queryability:

Schema (10 columns):

* ``ts_utc`` (timestamp[ns, UTC])
* ``received_at`` (timestamp[ns, UTC])
* ``exchange`` (string)
* ``symbol`` (string)
* ``event_type`` (string: "snapshot" / "delta")
* ``side`` (string: "bid" / "ask")
* ``level`` (int32: 0-N for snapshot, -1 for delta)
* ``price`` (decimal128(38,18))
* ``quantity`` (decimal128(38,18); 0 = remove level for delta)
* ``raw_json`` (string, optional)

Reconstruction utility (MCT-59) reads this stream + replays in ts_utc order
to obtain full orderbook state at any timestamp T.
"""

from __future__ import annotations

import json
import threading
import warnings
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

ORDERBOOK_SCHEMA_VERSION = "orderbook.v1"

_OB_SCHEMA = pa.schema([
    pa.field("ts_utc", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("received_at", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("event_type", pa.string(), nullable=False),
    pa.field("side", pa.string(), nullable=False),
    pa.field("level", pa.int32(), nullable=False),
    pa.field("price", pa.decimal128(38, 18), nullable=False),
    pa.field("quantity", pa.decimal128(38, 18), nullable=False),
    pa.field("raw_json", pa.string(), nullable=True),
])


@dataclass
class OrderbookEventRecord:
    ts_utc: datetime
    received_at: datetime
    exchange: str
    symbol: str
    event_type: str  # "snapshot" or "delta"
    side: str  # "bid" or "ask"
    level: int  # 0-N for snapshot, -1 for delta
    price: Decimal
    quantity: Decimal
    raw_json: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.price, float):
            raise TypeError("float not allowed for price; use Decimal or str")
        if isinstance(self.quantity, float):
            raise TypeError("float not allowed for quantity; use Decimal or str")


class OrderbookWriter:
    """Append-only orderbook event writer with daily partition rotation."""

    def __init__(
        self,
        *,
        root: Path,
        exchange: str,
        symbol: str,
        snapshot_id: str,
        batch_size: int = 1000,
        node_id: str | None = None,
        collector_run_id: str | None = None,
    ) -> None:
        warnings.warn(
            "OrderbookWriter is deprecated since MCT-106. Use WalIngester + L1Compactor instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._root = root
        self._exchange = exchange
        self._symbol = symbol
        self._snapshot_id = snapshot_id
        self._batch_size = batch_size
        self._node_id = node_id
        self._collector_run_id = collector_run_id
        self._batch_seq = 0
        self._lock = threading.Lock()
        self._buffer: list[OrderbookEventRecord] = []
        self._current_date: str | None = None
        self._current_writer: pq.ParquetWriter | None = None
        self._current_path: Path | None = None
        self._closed = False

    def append(self, record: OrderbookEventRecord) -> None:
        if self._closed:
            raise RuntimeError("OrderbookWriter is closed")
        with self._lock:
            self._buffer.append(record)
            if len(self._buffer) >= self._batch_size:
                self._flush_locked()

    def append_many(self, records: Iterable[OrderbookEventRecord]) -> None:
        if self._closed:
            raise RuntimeError("OrderbookWriter is closed")
        with self._lock:
            for r in records:
                self._buffer.append(r)
            if len(self._buffer) >= self._batch_size:
                self._flush_locked()

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            self._flush_locked()
            if self._current_writer is not None:
                self._current_writer.close()
                self._current_writer = None
            self._closed = True

    def _flush_locked(self) -> None:
        if not self._buffer:
            return
        first = self._buffer[0]
        date_str = first.ts_utc.astimezone(timezone.utc).date().isoformat()

        if self._current_date != date_str:
            if self._current_writer is not None:
                self._current_writer.close()
            partition = self._derive_partition(first.ts_utc)
            partition.mkdir(parents=True, exist_ok=True)
            # MCT-91 — HA file naming + parquet metadata
            if self._node_id is not None and self._collector_run_id is not None:
                file_name = f"{self._collector_run_id}-{self._batch_seq}.parquet"
                self._batch_seq += 1
            else:
                file_name = f"part-{self._snapshot_id}.parquet"
            target = partition / file_name
            self._current_path = target
            schema_with_meta = _OB_SCHEMA
            if self._node_id is not None:
                meta = dict(schema_with_meta.metadata or {})
                meta[b"node_id"] = self._node_id.encode("utf-8")
                schema_with_meta = schema_with_meta.with_metadata(meta)
            self._current_writer = pq.ParquetWriter(
                target, schema_with_meta, compression="snappy"
            )
            self._current_date = date_str

        table = _records_to_arrow(self._buffer)
        assert self._current_writer is not None
        self._current_writer.write_table(table)
        self._buffer = []

    def _derive_partition(self, ts_utc: datetime) -> Path:
        date_str = ts_utc.astimezone(timezone.utc).date().isoformat()
        partition = (
            self._root
            / "market"
            / "orderbook"
            / f"schema_version={ORDERBOOK_SCHEMA_VERSION}"
            / f"exchange={self._exchange}"
            / f"symbol={self._symbol}"
            / f"date={date_str}"
        )
        if self._node_id is not None:
            partition = partition / f"node={self._node_id}"
        return partition

    @property
    def current_path(self) -> Path | None:
        return self._current_path


def _records_to_arrow(records: Iterable[OrderbookEventRecord]) -> pa.Table:
    rows = list(records)
    return pa.Table.from_pydict(
        {
            "ts_utc": [r.ts_utc for r in rows],
            "received_at": [r.received_at for r in rows],
            "exchange": [r.exchange for r in rows],
            "symbol": [r.symbol for r in rows],
            "event_type": [r.event_type for r in rows],
            "side": [r.side for r in rows],
            "level": [r.level for r in rows],
            "price": [r.price for r in rows],
            "quantity": [r.quantity for r in rows],
            "raw_json": [r.raw_json for r in rows],
        },
        schema=_OB_SCHEMA,
    )


def snapshot_event_to_records(event: Any) -> list[OrderbookEventRecord]:
    """Flatten an OrderbookSnapshotEvent into per-level records."""
    records: list[OrderbookEventRecord] = []
    common = {
        "ts_utc": event.event_time,
        "received_at": event.received_at,
        "exchange": event.exchange,
        "symbol": str(event.symbol),
        "event_type": "snapshot",
        "raw_json": json.dumps(event.raw, ensure_ascii=False) if event.raw else None,
    }
    for level, lvl in enumerate(event.bids):
        records.append(OrderbookEventRecord(
            **common, side="bid", level=level, price=lvl.price, quantity=lvl.quantity,
        ))
    for level, lvl in enumerate(event.asks):
        records.append(OrderbookEventRecord(
            **common, side="ask", level=level, price=lvl.price, quantity=lvl.quantity,
        ))
    return records


def delta_event_to_records(event: Any) -> list[OrderbookEventRecord]:
    """Flatten an OrderbookDeltaEvent (changes list) into per-change records."""
    records: list[OrderbookEventRecord] = []
    common = {
        "ts_utc": event.event_time,
        "received_at": event.received_at,
        "exchange": event.exchange,
        "symbol": str(event.symbol),
        "event_type": "delta",
        "level": -1,
        "raw_json": json.dumps(event.raw, ensure_ascii=False) if event.raw else None,
    }
    for change in event.changes:
        records.append(OrderbookEventRecord(
            **common, side=change.side, price=change.price, quantity=change.quantity,
        ))
    return records
