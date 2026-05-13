# src/mctrader_data/compactor/l1.py
"""L1Compactor: NDJSON sealed WAL segment → sorted Parquet with lineage (MCT-106).

Invariants enforced:
  INV-3  Idempotency  — same sealed segment → byte-identical Parquet (deterministic run_id)
  INV-4  Sort         — output rows sorted ascending by ts_utc
  INV-5  Schema       — output schema matches upstream storage module (tick.v1 for transaction)
  INV-6  Lineage      — lineage-{run_id}.json written alongside Parquet

Path layout (ADR-009 §D2 / ADR-017 — ALL components in key=value Hive format):
  <root>/market/<channel>/schema_version=<version>/tier=L1/
    exchange=<exchange>/symbol=<symbol>/date=<date>/
    node=<node_id>/part-<run_id>.parquet

WAL segment path (parsed to extract metadata):
  <root>/wal/<exchange>/<channel>/<symbol>/<date>/<filename>.ndjson.sealed
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.wal.ndjson_codec import decode_line
from mctrader_data.wal.segment import compacted_path, parse_node_id_from_segment
from mctrader_data.tick_storage import (
    TICK_SCHEMA_VERSION,
    TickRecord,
    _records_to_arrow as _tick_records_to_arrow,
    _TICK_SCHEMA,
)
from mctrader_data.orderbook_snapshot_storage import (
    ORDERBOOK_SNAPSHOT_SCHEMA_VERSION,
    OrderbookSnapshotRecord,
    _OB_SNAPSHOT_SCHEMA,
    _compute_payload_hash,
    _records_to_arrow as _ob_snapshot_records_to_arrow,
)
from mctrader_data.metrics import compactor_writer_open_count
import contextlib

# ADR-009 §D11.9.2 — orderbook_depth.v1 schema (11 column, per-level flat row)
# raw_json = pa.large_string() (LargeUtf8, i64 offset) 의무 (§D11.9.6, i32 4GB overflow 차단)
# MCT-160 D7+P1: pa.field(name, dtype, nullable=...) 명시 — raw_json만 True, 나머지 False
# MCT-162 (2026-05-13)
_ORDERBOOKDEPTH_SCHEMA = pa.schema([
    pa.field("ts_utc",           pa.timestamp("us", tz="UTC"),   nullable=False),
    pa.field("received_at",      pa.timestamp("us", tz="UTC"),   nullable=False),
    pa.field("exchange",         pa.string(),                     nullable=False),
    pa.field("symbol",           pa.string(),                     nullable=False),
    pa.field("side",             pa.string(),                     nullable=False),
    pa.field("price",            pa.decimal128(38, 18),           nullable=False),
    pa.field("quantity",         pa.decimal128(38, 18),           nullable=False),
    pa.field("raw_json",         pa.large_string(),               nullable=True),   # nullable=True
    pa.field("node_id",          pa.string(),                     nullable=False),
    pa.field("collector_run_id", pa.string(),                     nullable=False),
    pa.field("ingest_seq",       pa.int64(),                      nullable=False),
])


# Allowlist — ADR-027 D4 amendment + ADR-009 §D2.6 matrix row 정합
# MCT-162 (2026-05-13) — orderbookdepth 추가
_CHANNEL_SCHEMA_VERSION: dict[str, str] = {
    "transaction": TICK_SCHEMA_VERSION,
    "orderbooksnapshot": ORDERBOOK_SNAPSHOT_SCHEMA_VERSION,
    "orderbookdepth": "orderbook_depth.v1",  # MCT-162 신규
}


def _schema_version(channel: str) -> str:
    """Return the schema version string for *channel* (module-level helper for L2/L3).

    ADR-027 D4 amendment 정합 — fail-fast vs silent skip.
    Unsupported channel → NotImplementedError raise (silent skip 금지)
    + Prometheus counter ``compactor_unsupported_channel_total{channel}`` emit
    (cardinality bounded low — collector emit channel 종류만).
    """
    if channel not in _CHANNEL_SCHEMA_VERSION:
        from mctrader_data.nas_metrics.prometheus_exporters import (  # lazy import (circular 회피)
            compactor_unsupported_channel_total,
        )
        compactor_unsupported_channel_total.labels(channel=channel).inc()
        raise NotImplementedError(
            f"_schema_version: channel {channel!r} not supported. "
            f"Supported: {sorted(_CHANNEL_SCHEMA_VERSION.keys())}. "
            f"ADR-009 §D11.9 + ADR-027 D4 channel parity 정책 정합."
        )
    return _CHANNEL_SCHEMA_VERSION[channel]


class L1Compactor:
    """Compact a single sealed WAL segment into a tier=L1 Parquet file.

    Currently supports channel ``transaction`` (tick.v1 schema).
    Other channels raise ``NotImplementedError``.
    """

    def __init__(self, *, root: Path) -> None:
        self._root = root

    # ------------------------------------------------------------------ public

    def compact_segment(self, sealed: Path) -> Path:
        """Compact *sealed* segment → Parquet.  Idempotent: returns same path on re-run.

        Steps:
        1. Parse metadata from segment path.
        2. Derive deterministic run_id from sealed path (INV-3).
        3. Derive output Parquet path.
        4. If Parquet already exists, skip write (idempotency).
        5. Otherwise: decode NDJSON → sort by ts_utc → convert → write atomically.
        6. Write lineage JSON (idempotent: overwrites).
        7. Touch .compacted marker on sealed segment.
        """
        meta = self._parse_segment_meta(sealed)
        run_id = self._derive_run_id(sealed)
        parquet_path = self._derive_parquet_path(meta, run_id)
        parquet_path.parent.mkdir(parents=True, exist_ok=True)

        if not parquet_path.exists():
            records_raw = self._read_ndjson(sealed)
            # inject run_id as collector_run_id for channel-specific converters (e.g. orderbookdepth)
            self._current_meta = {**meta, "collector_run_id": run_id}
            table = self._convert_to_arrow(records_raw, meta["channel"])
            # Sort by ts_utc (INV-4)
            table = table.sort_by("ts_utc")
            # Atomic write: tmp file → rename
            self._write_parquet_atomic(table, parquet_path, meta)

        # INV-6: write lineage JSON (idempotent overwrite)
        self._write_lineage(sealed, parquet_path, meta, run_id)

        # Mark sealed segment as compacted
        marker = compacted_path(sealed)
        if not marker.exists():
            marker.touch()

        return parquet_path

    # ----------------------------------------------------------------- private

    def _parse_segment_meta(self, sealed: Path) -> dict:
        """Extract exchange, channel, symbol, date, node_id from the sealed segment path.

        WAL layout: <root>/wal/<exchange>/<channel>/<symbol>/<date>/<filename>
        """
        # Resolve relative to wal root
        wal_root = self._root / "wal"
        try:
            rel = sealed.relative_to(wal_root)
        except ValueError as err:
            raise ValueError(
                f"Sealed segment {sealed} is not under wal root {wal_root}"
            ) from err
        parts = rel.parts  # (exchange, channel, symbol, date, filename)
        if len(parts) < 5:
            raise ValueError(
                f"Unexpected WAL segment path structure: {sealed}. "
                f"Expected <root>/wal/<exchange>/<channel>/<symbol>/<date>/<file>"
            )
        exchange = parts[0]
        channel = parts[1]
        symbol = parts[2]
        date = parts[3]
        node_id = parse_node_id_from_segment(sealed)
        return {
            "exchange": exchange,
            "channel": channel,
            "symbol": symbol,
            "date": date,
            "node_id": node_id,
        }

    def _derive_run_id(self, sealed: Path) -> str:
        """Deterministic run_id = sha256(relative path of sealed segment)[:16] (INV-3)."""
        wal_root = self._root / "wal"
        try:
            rel = sealed.relative_to(wal_root)
        except ValueError:
            rel = sealed
        rel_str = rel.as_posix()
        return hashlib.sha256(rel_str.encode("utf-8")).hexdigest()[:16]

    def _derive_parquet_path(self, meta: dict, run_id: str) -> Path:
        """Derive the output Parquet path from metadata.

        All path components use key=value Hive format per ADR-009 §D2 and ADR-017.
        Callers reading individual files must use pq.ParquetFile(f).read() — NOT
        pq.read_table(directory) — to avoid PyArrow Hive auto-discovery conflicts.
        """
        channel = meta["channel"]
        schema_version = self._schema_version_for_channel(channel)
        exchange = meta["exchange"]
        symbol = meta["symbol"]
        date = meta["date"]
        node_id = meta["node_id"]
        return (
            self._root
            / "market"
            / channel
            / f"schema_version={schema_version}"
            / "tier=L1"
            / f"exchange={exchange}"
            / f"symbol={symbol}"
            / f"date={date}"
            / f"node={node_id}"
            / f"part-{run_id}.parquet"
        )

    def _schema_version_for_channel(self, channel: str) -> str:
        return _schema_version(channel)

    def _arrow_schema_for_channel(self, channel: str) -> pa.Schema:
        if channel == "transaction":
            return _TICK_SCHEMA
        if channel == "orderbooksnapshot":
            return _OB_SNAPSHOT_SCHEMA
        if channel == "orderbookdepth":
            return _ORDERBOOKDEPTH_SCHEMA
        raise NotImplementedError(
            f"L1Compactor: no Arrow schema for channel '{channel}'. "
            f"Supported: {sorted(_CHANNEL_SCHEMA_VERSION.keys())}."
        )

    def _read_ndjson(self, sealed: Path) -> list[dict]:
        """Read all records from the NDJSON sealed segment."""
        records: list[dict] = []
        with open(sealed, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(decode_line(line))
        return records

    def _convert_to_arrow(self, records_raw: list[dict], channel: str) -> pa.Table:
        """Convert raw dicts from NDJSON to Arrow table using the channel's schema."""
        if channel == "transaction":
            return self._tick_dicts_to_arrow(records_raw)
        if channel == "orderbooksnapshot":
            return self._ob_snapshot_dicts_to_arrow(records_raw)
        if channel == "orderbookdepth":
            return self._orderbookdepth_dicts_to_arrow(records_raw)
        raise NotImplementedError(
            f"L1Compactor._convert_to_arrow: channel '{channel}' not supported. "
            f"Supported: {sorted(_CHANNEL_SCHEMA_VERSION.keys())}."
        )

    def _ob_snapshot_dicts_to_arrow(self, records_raw: list[dict]) -> pa.Table:
        """Convert orderbooksnapshot WAL records → Arrow table via per-level rows.

        Each WAL record encodes one full snapshot (bids + asks as lists).
        This method flattens them into N bid rows + N ask rows per WAL record,
        matching the §D14 Parquet schema (OrderbookSnapshotRecord per level).

        baseline_seq = int(ts_utc.timestamp() * 1_000_000) per §D14.5 (1).
        payload_hash = SHA256(canonical body)[:16] per §D14.6.
        """
        ob_records: list[OrderbookSnapshotRecord] = []
        for d in records_raw:
            ts_utc = self._parse_ts(d["ts_utc"])
            received_at = self._parse_ts(d["received_at"])
            exchange = d["exchange"]
            symbol = d["symbol"]
            raw_json = d.get("raw_json")

            baseline_seq = int(ts_utc.timestamp() * 1_000_000)

            bids_pairs = [
                (Decimal(str(lvl["price"])), Decimal(str(lvl["quantity"])))
                for lvl in d["bids"]
            ]
            asks_pairs = [
                (Decimal(str(lvl["price"])), Decimal(str(lvl["quantity"])))
                for lvl in d["asks"]
            ]
            payload_hash = _compute_payload_hash(exchange, symbol, baseline_seq, bids_pairs, asks_pairs)

            common: dict = {
                "ts_utc": ts_utc,
                "received_at": received_at,
                "exchange": exchange,
                "symbol": symbol,
                "baseline_seq": baseline_seq,
                "payload_hash": payload_hash,
                "raw_json": raw_json,
            }
            for level, (price, qty) in enumerate(bids_pairs):
                ob_records.append(OrderbookSnapshotRecord(
                    **common, side="bid", level=level, price=price, quantity=qty,
                ))
            for level, (price, qty) in enumerate(asks_pairs):
                ob_records.append(OrderbookSnapshotRecord(
                    **common, side="ask", level=level, price=price, quantity=qty,
                ))
        return _ob_snapshot_records_to_arrow(ob_records)

    def _orderbookdepth_dicts_to_arrow(self, records_raw: list[dict]) -> pa.Table:
        """Convert orderbookdepth WAL records → Arrow table (MCT-162, ADR-009 §D11.9).

        Schema = ADR-009 §D11.9.2 (11 column, per-level flat row).
        Flat 변환 규칙: WAL frame 1개 (N levels) → parquet N rows (per-level flatten).
        row count = Σ len(frame.changes) (across all frames in segment).

        raw_json = pa.large_string() (LargeUtf8, i64 offset) — i32 4GB overflow 차단
        (MCT-156 OOM exit 137 cross-ref, ADR-009 §D11.9.6 의무).

        metadata 4 column (node_id, collector_run_id, ingest_seq, validation_status)
        = L1Compactor segment metadata 에서 inject (§D2.1 정합).
        """
        meta = self._current_meta  # set by compact_segment before _convert_to_arrow call
        node_id: str = meta["node_id"]
        collector_run_id: str = meta.get("collector_run_id", "unknown")

        # per-level flatten: 1 frame → N rows (N = len(frame.changes))
        flat_rows: list[dict] = []
        for ingest_seq, frame in enumerate(records_raw):
            ts_utc = self._parse_ts(frame["ts_utc"])
            received_at = self._parse_ts(frame["received_at"])
            for change in frame["changes"]:
                flat_rows.append({
                    "ts_utc": ts_utc,
                    "received_at": received_at,
                    "exchange": frame["exchange"],
                    "symbol": frame["symbol"],
                    "side": change["side"],
                    "price": Decimal(str(change["price"])),
                    "quantity": Decimal(str(change["quantity"])),
                    "raw_json": frame.get("raw_json"),
                    "node_id": node_id,
                    "collector_run_id": collector_run_id,
                    "ingest_seq": ingest_seq,
                })
        return pa.Table.from_pylist(flat_rows, schema=_ORDERBOOKDEPTH_SCHEMA)

    def _tick_dicts_to_arrow(self, records_raw: list[dict]) -> pa.Table:
        """Convert transaction channel dicts → Arrow table via TickRecord + _records_to_arrow."""
        tick_records: list[TickRecord] = []
        for d in records_raw:
            ts_utc = self._parse_ts(d["ts_utc"])
            received_at = self._parse_ts(d["received_at"])
            raw_json = d.get("raw_json")
            tick_records.append(
                TickRecord(
                    ts_utc=ts_utc,
                    received_at=received_at,
                    exchange=d["exchange"],
                    symbol=d["symbol"],
                    price=Decimal(str(d["price"])),
                    quantity=Decimal(str(d["quantity"])),
                    side=d["side"],
                    raw_json=raw_json,
                )
            )
        return _tick_records_to_arrow(tick_records)

    @staticmethod
    def _parse_ts(value: object) -> datetime:
        """Parse ts_utc / received_at from NDJSON value to timezone-aware datetime."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        if isinstance(value, str):
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        raise TypeError(f"Cannot parse datetime from {value!r} (type {type(value).__name__})")

    def _write_parquet_atomic(
        self, table: pa.Table, target: Path, meta: dict
    ) -> None:
        """Write Parquet to a tmp file then atomically rename to target."""
        schema_with_meta = self._arrow_schema_for_channel(meta["channel"])
        node_id = meta.get("node_id")
        if node_id is not None:
            existing_meta = dict(schema_with_meta.metadata or {})
            existing_meta[b"node_id"] = node_id.encode("utf-8")
            schema_with_meta = schema_with_meta.with_metadata(existing_meta)

        tmp_dir = target.parent
        fd, tmp_path = tempfile.mkstemp(dir=tmp_dir, suffix=".parquet.tmp")
        try:
            os.close(fd)
            # MCT-133 A1: use context manager to guarantee writer.close() on
            # exception paths (e.g. write_table raising) — prevents file handle
            # leaks under memory pressure / partition errors.
            # MCT-134 A2 Task 7: track currently-open ParquetWriter instances per
            # tier via compactor_writer_open_count Gauge (inc before open, dec
            # in finally — paired across both success and exception paths).
            compactor_writer_open_count.labels(tier="L1").inc()
            try:
                with pq.ParquetWriter(
                    tmp_path, schema_with_meta, compression="snappy"
                ) as writer:
                    writer.write_table(table)
            finally:
                compactor_writer_open_count.labels(tier="L1").dec()
            os.replace(tmp_path, str(target))
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise

    def _write_lineage(
        self, sealed: Path, parquet_path: Path, meta: dict, run_id: str
    ) -> None:
        """Write lineage-{run_id}.json alongside the Parquet file (INV-6)."""
        lineage = {
            "run_id": run_id,
            "node_id": meta["node_id"],
            "exchange": meta["exchange"],
            "channel": meta["channel"],
            "symbol": meta["symbol"],
            "date": meta["date"],
            "compacted_from": sealed.name,
            "compacted_from_abs": str(sealed),
            "output": str(parquet_path),
            "tier": "L1",
            "schema_version": self._schema_version_for_channel(meta["channel"]),
            "compacted_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        lineage_path = parquet_path.parent / f"lineage-{run_id}.json"
        lineage_path.write_text(json.dumps(lineage, indent=2), encoding="utf-8")
