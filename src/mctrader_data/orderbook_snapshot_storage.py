"""ADR-009 §D14 orderbook_snapshot.v1 — Parquet append-only writer.

Hive partition layout:
  market/orderbook_snapshot/schema_version=orderbook_snapshot.v1/
    exchange={ex}/symbol={sym}/date={YYYY-MM-DD}/
    node={node_id}/part-{collector_run_id}.parquet

Schema (11 columns, per §D14.1 + §D14.5 wiretap amendments 2026-05-09):

* ts_utc       (timestamp[ns, UTC])  — content.datetime micro-epoch converted
* received_at  (timestamp[ns, UTC])  — wall-clock WS receive time
* exchange     (string)
* symbol       (string)
* baseline_seq (int64)               — int64(content.datetime) per §D14.5 (1)
* side         (string: "bid"/"ask")
* level        (int32: 0-29 for 30-level snapshot)
* price        (decimal128(38,18))
* quantity     (decimal128(38,18))
* payload_hash (string)              — SHA256(canonical body)[:16]
* raw_json     (string, nullable)    — full raw WS message JSON (storage optional)

§D14.10 1-sec throttle:
  last-write-wins per symbol; WS native push (~200ms) 그대로 받되
  적재 측면만 1-sec hard cap (ADR-009 §D14.10, 2026-05-09 wiretap amendment).

§D14.6 dedup 6-tuple:
  (exchange, symbol, baseline_seq, side, level, payload_hash)
  — multi-node 중복 적재 방어 (Read API 측 dedup은 scan_orderbook_snapshots 책임).
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

ORDERBOOK_SNAPSHOT_SCHEMA_VERSION = "orderbook_snapshot.v1"

_OB_SNAPSHOT_SCHEMA = pa.schema([
    pa.field("ts_utc", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("received_at", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("baseline_seq", pa.int64(), nullable=False),
    pa.field("side", pa.string(), nullable=False),
    pa.field("level", pa.int32(), nullable=False),
    pa.field("price", pa.decimal128(38, 18), nullable=False),
    pa.field("quantity", pa.decimal128(38, 18), nullable=False),
    pa.field("payload_hash", pa.string(), nullable=False),
    pa.field("raw_json", pa.string(), nullable=True),
])


@dataclass
class OrderbookSnapshotRecord:
    ts_utc: datetime
    received_at: datetime
    exchange: str
    symbol: str
    baseline_seq: int          # int64(content.datetime) — §D14.5 (1)
    side: str                  # "bid" or "ask"
    level: int                 # 0-29 for 30-level snapshot
    price: Decimal
    quantity: Decimal
    payload_hash: str          # SHA256(canonical body)[:16]
    raw_json: str | None = None


def _compute_payload_hash(exchange: str, symbol: str, baseline_seq: int,
                           bids: list, asks: list) -> str:
    """Deterministic SHA256 hash of canonical snapshot body.

    Canonical form: JSON-sorted keys, no whitespace.
    Input bids/asks must be list of [price_str, qty_str] pairs.
    """
    canonical = json.dumps(
        {
            "exchange": exchange,
            "symbol": symbol,
            "baseline_seq": baseline_seq,
            "bids": [[str(p), str(q)] for p, q in bids],
            "asks": [[str(p), str(q)] for p, q in asks],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def snapshot_event_to_snapshot_records(event: Any) -> list[OrderbookSnapshotRecord]:
    """Flatten an OrderbookSnapshotEvent into §D14 per-level records (60 rows).

    baseline_seq = int64(content.datetime micro-epoch) per §D14.5 (1).
    payload_hash = SHA256 of canonical body [:16].
    60 rows = 30 bid levels + 30 ask levels (single-message atomic, §D14.5 (1)).
    """
    baseline_seq = int(event.event_time.timestamp() * 1_000_000)

    bids_pairs = [(lvl.price, lvl.quantity) for lvl in event.bids]
    asks_pairs = [(lvl.price, lvl.quantity) for lvl in event.asks]
    payload_hash = _compute_payload_hash(
        event.exchange, str(event.symbol), baseline_seq, bids_pairs, asks_pairs
    )

    raw_str: str | None = (
        json.dumps(event.raw, ensure_ascii=False) if event.raw else None
    )

    records: list[OrderbookSnapshotRecord] = []
    common = {
        "ts_utc": event.event_time,
        "received_at": event.received_at,
        "exchange": event.exchange,
        "symbol": str(event.symbol),
        "baseline_seq": baseline_seq,
        "payload_hash": payload_hash,
        "raw_json": raw_str,
    }
    for level, (price, qty) in enumerate(bids_pairs):
        records.append(OrderbookSnapshotRecord(
            **common, side="bid", level=level, price=price, quantity=qty,
        ))
    for level, (price, qty) in enumerate(asks_pairs):
        records.append(OrderbookSnapshotRecord(
            **common, side="ask", level=level, price=price, quantity=qty,
        ))
    return records


class OrderbookSnapshotWriter:
    """Append-only §D14 orderbook snapshot writer with §D14.10 1-sec throttle.

    §D14.10 1-sec subsample throttle: per-symbol last-write timestamp tracked;
    snapshot events arriving within 1 second of the last write for the same
    symbol are dropped (last-write-wins). The WS connection receives the native
    push (~200ms) unmodified — only the storage side is throttled.

    The writer is thread-safe (single lock per instance).
    """

    THROTTLE_SECONDS: float = 1.0  # §D14.10 hard cap

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
        self._root = root
        self._exchange = exchange
        self._symbol = symbol
        self._snapshot_id = snapshot_id
        self._batch_size = batch_size
        self._node_id = node_id
        self._collector_run_id = collector_run_id
        self._batch_seq = 0
        self._lock = threading.Lock()
        self._buffer: list[OrderbookSnapshotRecord] = []
        self._current_date: str | None = None
        self._current_writer: pq.ParquetWriter | None = None
        self._current_path: Path | None = None
        self._closed = False
        # §D14.10 — last write timestamp per symbol (monotonic seconds)
        self._last_write_ts: float | None = None

    def append_event(self, event: Any, *, monotonic_now: float | None = None) -> bool:
        """Ingest one OrderbookSnapshotEvent, applying §D14.10 1-sec throttle.

        Returns True if the event was accepted (written), False if throttled.
        ``monotonic_now`` injects the monotonic clock for testing; defaults to
        ``time.monotonic()``.
        """
        if self._closed:
            raise RuntimeError("OrderbookSnapshotWriter is closed")

        import time as _time
        now = monotonic_now if monotonic_now is not None else _time.monotonic()

        with self._lock:
            if (
                self._last_write_ts is not None
                and (now - self._last_write_ts) < self.THROTTLE_SECONDS
            ):
                return False  # throttled — last-write-wins by discarding this one

            records = snapshot_event_to_snapshot_records(event)
            for r in records:
                self._buffer.append(r)
            self._last_write_ts = now
            if len(self._buffer) >= self._batch_size:
                self._flush_locked()
        return True

    def append_many(self, records: list[OrderbookSnapshotRecord]) -> None:
        """Append pre-converted records directly (bypasses throttle — for testing/backfill)."""
        if self._closed:
            raise RuntimeError("OrderbookSnapshotWriter is closed")
        with self._lock:
            self._buffer.extend(records)
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
            if self._node_id is not None and self._collector_run_id is not None:
                file_name = f"{self._collector_run_id}-{self._batch_seq}.parquet"
                self._batch_seq += 1
            else:
                file_name = f"part-{self._snapshot_id}.parquet"
            target = partition / file_name
            self._current_path = target
            schema_with_meta = _OB_SNAPSHOT_SCHEMA
            if self._node_id is not None:
                meta = dict(schema_with_meta.metadata or {})
                meta[b"node_id"] = self._node_id.encode("utf-8")
                schema_with_meta = schema_with_meta.with_metadata(meta)
            self._current_writer = pq.ParquetWriter(
                target, schema_with_meta, compression="zstd"
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
            / "orderbook_snapshot"
            / f"schema_version={ORDERBOOK_SNAPSHOT_SCHEMA_VERSION}"
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


def _records_to_arrow(records: list[OrderbookSnapshotRecord]) -> pa.Table:
    return pa.Table.from_pydict(
        {
            "ts_utc": [r.ts_utc for r in records],
            "received_at": [r.received_at for r in records],
            "exchange": [r.exchange for r in records],
            "symbol": [r.symbol for r in records],
            "baseline_seq": [r.baseline_seq for r in records],
            "side": [r.side for r in records],
            "level": [r.level for r in records],
            "price": [r.price for r in records],
            "quantity": [r.quantity for r in records],
            "payload_hash": [r.payload_hash for r in records],
            "raw_json": [r.raw_json for r in records],
        },
        schema=_OB_SNAPSHOT_SCHEMA,
    )
