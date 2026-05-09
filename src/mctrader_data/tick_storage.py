"""Forward-only tick (transaction) Parquet append-only storage (MCT-58).

ADR-009 amendment: ``market/ticks/schema_version=tick.v1/exchange=.../symbol=.../
date=YYYY-MM-DD/part-{snapshot_id}.parquet`` Hive layout.

Schema (8 columns, all nullable=false):

* ``ts_utc`` (timestamp[ns, UTC])
* ``received_at`` (timestamp[ns, UTC])
* ``exchange`` (string)
* ``symbol`` (string, canonical "{quote}-{base}")
* ``price`` (decimal128(38,18))
* ``quantity`` (decimal128(38,18))
* ``side`` (string: "buy" / "sell")
* ``raw_json`` (string, optional — original WS payload for audit)

Writer is per-(symbol, date) singleton — rotates daily at UTC midnight.
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


TICK_SCHEMA_VERSION = "tick.v1"

_TICK_SCHEMA = pa.schema([
    pa.field("ts_utc", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("received_at", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("price", pa.decimal128(38, 18), nullable=False),
    pa.field("quantity", pa.decimal128(38, 18), nullable=False),
    pa.field("side", pa.string(), nullable=False),
    pa.field("raw_json", pa.string(), nullable=True),
])


@dataclass
class TickRecord:
    ts_utc: datetime
    received_at: datetime
    exchange: str
    symbol: str
    price: Decimal
    quantity: Decimal
    side: str
    raw_json: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.price, float):
            raise TypeError("float not allowed for price; use Decimal or str")
        if isinstance(self.quantity, float):
            raise TypeError("float not allowed for quantity; use Decimal or str")


class TickWriter:
    """Append-only writer that buffers in-memory and flushes per-batch.

    Flush triggers:
    * batch_size reached (default 500)
    * date rolled (UTC midnight) — flushes prior day, starts new file
    * close() called explicitly (graceful SIGTERM)
    """

    def __init__(
        self,
        *,
        root: Path,
        exchange: str,
        symbol: str,
        snapshot_id: str,
        batch_size: int = 500,
        node_id: str | None = None,
        collector_run_id: str | None = None,
    ) -> None:
        warnings.warn(
            "TickWriter is deprecated since MCT-106. Use WalIngester + L1Compactor instead.",
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
        self._buffer: list[TickRecord] = []
        self._current_date: str | None = None
        self._current_writer: pq.ParquetWriter | None = None
        self._current_path: Path | None = None
        self._closed = False

    def append(self, record: TickRecord) -> None:
        if self._closed:
            raise RuntimeError("TickWriter is closed")
        with self._lock:
            self._buffer.append(record)
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
            # MCT-91 — HA file naming when node_id + collector_run_id 모두 명시
            if self._node_id is not None and self._collector_run_id is not None:
                file_name = f"{self._collector_run_id}-{self._batch_seq}.parquet"
                self._batch_seq += 1
            else:
                file_name = f"part-{self._snapshot_id}.parquet"
            target = partition / file_name
            self._current_path = target
            # MCT-91 — parquet metadata 에 node_id 추가
            schema_with_meta = _TICK_SCHEMA
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

        # Reuse derive_partition_path with a synthetic Timeframe (1m placeholder unused)
        # — actually we need a tick-specific path. Build it manually.
        date_str = ts_utc.astimezone(timezone.utc).date().isoformat()
        partition = (
            self._root
            / "market"
            / "ticks"
            / f"schema_version={TICK_SCHEMA_VERSION}"
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


def _records_to_arrow(records: Iterable[TickRecord]) -> pa.Table:
    rows = list(records)
    return pa.Table.from_pydict(
        {
            "ts_utc": [r.ts_utc for r in rows],
            "received_at": [r.received_at for r in rows],
            "exchange": [r.exchange for r in rows],
            "symbol": [r.symbol for r in rows],
            "price": [r.price for r in rows],
            "quantity": [r.quantity for r in rows],
            "side": [r.side for r in rows],
            "raw_json": [r.raw_json for r in rows],
        },
        schema=_TICK_SCHEMA,
    )


def transaction_event_to_record(event: Any) -> TickRecord:
    """Convert a :class:`TransactionEvent` (from mctrader-market-bithumb) to a TickRecord."""
    return TickRecord(
        ts_utc=event.event_time,
        received_at=event.received_at,
        exchange=event.exchange,
        symbol=str(event.symbol),
        price=event.price,
        quantity=event.quantity,
        side=event.side,
        raw_json=json.dumps(event.raw, ensure_ascii=False) if event.raw else None,
    )
