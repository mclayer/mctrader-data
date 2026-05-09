# src/mctrader_data/compactor/l1.py
"""L1Compactor: NDJSON sealed WAL segment → sorted Parquet with lineage (MCT-106).

Invariants enforced:
  INV-3  Idempotency  — same sealed segment → byte-identical Parquet (deterministic run_id)
  INV-4  Sort         — output rows sorted ascending by ts_utc
  INV-5  Schema       — output schema matches upstream storage module (tick.v1 for transaction)
  INV-6  Lineage      — lineage-{run_id}.json written alongside Parquet

Path layout:
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
        except ValueError:
            raise ValueError(
                f"Sealed segment {sealed} is not under wal root {wal_root}"
            )
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
        """Derive the output Parquet path from metadata."""
        channel = meta["channel"]
        schema_version = self._schema_version_for_channel(channel)
        # Note: exchange, symbol, date use plain directory names (not key=value Hive format)
        # to avoid PyArrow schema merge errors when pq.read_table auto-discovers partitions.
        # tier=L1 and node=NODE_X use key=value format as required by the path spec.
        return (
            self._root
            / "market"
            / channel
            / f"schema_version={schema_version}"
            / "tier=L1"
            / meta["exchange"]
            / meta["symbol"]
            / meta["date"]
            / f"node={meta['node_id']}"
            / f"part-{run_id}.parquet"
        )

    def _schema_version_for_channel(self, channel: str) -> str:
        if channel == "transaction":
            return TICK_SCHEMA_VERSION
        raise NotImplementedError(
            f"L1Compactor does not yet support channel '{channel}'. "
            "Only 'transaction' is implemented."
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
        raise NotImplementedError(
            f"L1Compactor._convert_to_arrow: channel '{channel}' not supported"
        )

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
        schema_with_meta = _TICK_SCHEMA
        node_id = meta.get("node_id")
        if node_id is not None:
            existing_meta = dict(schema_with_meta.metadata or {})
            existing_meta[b"node_id"] = node_id.encode("utf-8")
            schema_with_meta = schema_with_meta.with_metadata(existing_meta)

        tmp_dir = target.parent
        fd, tmp_path = tempfile.mkstemp(dir=tmp_dir, suffix=".parquet.tmp")
        try:
            os.close(fd)
            writer = pq.ParquetWriter(tmp_path, schema_with_meta, compression="snappy")
            writer.write_table(table)
            writer.close()
            os.replace(tmp_path, str(target))
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
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
