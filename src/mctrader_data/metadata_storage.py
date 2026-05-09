"""ADR-009 §D13 exchange_metadata.v1 — Parquet append-only writer + REST poller.

Hive partition layout:
  market/exchange_metadata/schema_version=exchange_metadata.v1/
    exchange={ex}/fetched_date={YYYY-MM-DD}/
    node={node_id}/part-{collector_run_id}.parquet

Schema (14 columns, §D13.1 + §D13.10 Public-fillable subset, 2026-05-09):

Non-nullable (public-fillable, Phase 2):
  exchange, symbol, fetched_date, fetched_at, source_snapshot_id, data_hash,
  asset_status, acc_trade_value_24h

Nullable (private/unconfirmed, Phase 2 = NULL, Live Epic채움):
  tick_size, min_order_qty, fee_maker, fee_taker, min_order_notional_krw

§D13.5 dedup: logical key (exchange, symbol, fetched_date, source_snapshot_id).
  data_hash = SHA256 of non-NULL columns only (NULL → hash skip per §D13.1 amendment).

§D13.7 rate-limit: Bithumb public REST 150 req/sec (v2.1 official).
  Daily cadence (UTC midnight + 1min grace) — collector daemon scheduler triggers.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

EXCHANGE_METADATA_SCHEMA_VERSION = "exchange_metadata.v1"

_DECIMAL_38_18_QUANTUM = Decimal("1e-18")


def _to_decimal_38_18(raw: str) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 50
        return Decimal(raw).quantize(_DECIMAL_38_18_QUANTUM, rounding=ROUND_HALF_EVEN)

_META_SCHEMA = pa.schema([
    # Non-nullable public-fillable columns (§D13.10)
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("fetched_date", pa.date32(), nullable=False),
    pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("source_snapshot_id", pa.string(), nullable=False),
    pa.field("data_hash", pa.string(), nullable=False),
    pa.field("asset_status", pa.string(), nullable=False),
    pa.field("acc_trade_value_24h", pa.decimal128(38, 18), nullable=False),
    # Nullable private/unconfirmed (Phase 2 = NULL, Live Epic fills)
    pa.field("tick_size", pa.decimal128(38, 18), nullable=True),
    pa.field("min_order_qty", pa.decimal128(38, 18), nullable=True),
    pa.field("fee_maker", pa.decimal128(38, 18), nullable=True),
    pa.field("fee_taker", pa.decimal128(38, 18), nullable=True),
    pa.field("min_order_notional_krw", pa.decimal128(38, 18), nullable=True),
    # Forward-compat available_from_ts (ADR-005 §path-c)
    pa.field("available_from_ts", pa.timestamp("us", tz="UTC"), nullable=False),
])


@dataclass
class ExchangeMetadataRecord:
    exchange: str
    symbol: str
    fetched_date: date
    fetched_at: datetime          # ADR-005: available_from_ts := fetched_at
    source_snapshot_id: str       # SHA256[:16] of (endpoint + response_hash)
    data_hash: str                # SHA256 of non-NULL columns
    asset_status: str             # "1" = active, "0" = suspended
    acc_trade_value_24h: Decimal  # 24h KRW traded value (from /ticker/ALL_KRW)
    # Nullable — Phase 2 = None
    tick_size: Decimal | None = None
    min_order_qty: Decimal | None = None
    fee_maker: Decimal | None = None
    fee_taker: Decimal | None = None
    min_order_notional_krw: Decimal | None = None

    def __post_init__(self) -> None:
        _decimal_fields = (
            ("acc_trade_value_24h", self.acc_trade_value_24h),
            ("tick_size", self.tick_size),
            ("min_order_qty", self.min_order_qty),
            ("fee_maker", self.fee_maker),
            ("fee_taker", self.fee_taker),
            ("min_order_notional_krw", self.min_order_notional_krw),
        )
        for name, val in _decimal_fields:
            if isinstance(val, float):
                raise TypeError(f"float not allowed for {name}; use Decimal or str")

    @property
    def available_from_ts(self) -> datetime:
        """ADR-005 path-c lookahead guard: available_from_ts := fetched_at."""
        return self.fetched_at


def compute_data_hash(record: ExchangeMetadataRecord) -> str:
    """SHA256 of non-NULL column values (NULL columns skipped per §D13.1 amendment).

    Canonical form: JSON-sorted keys, no whitespace, Decimal as string.
    """
    non_null: dict[str, str] = {
        "exchange": record.exchange,
        "symbol": record.symbol,
        "fetched_date": record.fetched_date.isoformat(),
        "fetched_at": record.fetched_at.isoformat(),
        "source_snapshot_id": record.source_snapshot_id,
        "asset_status": record.asset_status,
        "acc_trade_value_24h": str(record.acc_trade_value_24h),
    }
    if record.tick_size is not None:
        non_null["tick_size"] = str(record.tick_size)
    if record.min_order_qty is not None:
        non_null["min_order_qty"] = str(record.min_order_qty)
    if record.fee_maker is not None:
        non_null["fee_maker"] = str(record.fee_maker)
    if record.fee_taker is not None:
        non_null["fee_taker"] = str(record.fee_taker)
    if record.min_order_notional_krw is not None:
        non_null["min_order_notional_krw"] = str(record.min_order_notional_krw)

    canonical = json.dumps(non_null, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:32]


def build_source_snapshot_id(endpoint: str, response_hash: str) -> str:
    """Deterministic source_snapshot_id from endpoint + response hash."""
    combined = f"{endpoint}|{response_hash}"
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


class ExchangeMetadataWriter:
    """Append-only §D13 exchange metadata writer with logical key dedup.

    §D13.5 logical key dedup: (exchange, symbol, fetched_date, source_snapshot_id).
    Duplicate key with same data_hash → idempotent skip.
    Duplicate key with different data_hash → quarantine signal (§D13.5 mismatch).

    Thread-safe. Daily partition rotation on fetched_date change.
    """

    def __init__(
        self,
        *,
        root: Path,
        exchange: str,
        node_id: str | None = None,
        collector_run_id: str | None = None,
        snapshot_id: str | None = None,
    ) -> None:
        self._root = root
        self._exchange = exchange
        self._node_id = node_id
        self._collector_run_id = collector_run_id
        self._snapshot_id = snapshot_id or "default"
        self._batch_seq = 0
        self._lock = threading.Lock()
        self._buffer: list[ExchangeMetadataRecord] = []
        self._current_date: str | None = None
        self._current_writer: pq.ParquetWriter | None = None
        self._current_path: Path | None = None
        self._closed = False
        # dedup cache: (symbol, fetched_date.isoformat(), source_snapshot_id) → data_hash
        self._seen: dict[tuple[str, str, str], str] = {}
        # quarantine events: list of mismatch dicts
        self._quarantine: list[dict[str, Any]] = []

    def append(self, record: ExchangeMetadataRecord) -> str:
        """Append one record. Returns: 'written' | 'skipped' | 'quarantine'."""
        if self._closed:
            raise RuntimeError("ExchangeMetadataWriter is closed")
        key = (record.symbol, record.fetched_date.isoformat(), record.source_snapshot_id)
        with self._lock:
            if key in self._seen:
                existing_hash = self._seen[key]
                if existing_hash == record.data_hash:
                    return "skipped"  # idempotent — same content
                # Content mismatch — quarantine emit (§D13.5 mismatch)
                self._quarantine.append({
                    "type": "metadata_content_mismatch",
                    "exchange": record.exchange,
                    "symbol": record.symbol,
                    "fetched_date": record.fetched_date.isoformat(),
                    "source_snapshot_id": record.source_snapshot_id,
                    "existing_hash": existing_hash,
                    "new_hash": record.data_hash,
                })
                log.warning(
                    "[metadata] content mismatch quarantine: symbol=%s fetched_date=%s "
                    "existing_hash=%s new_hash=%s",
                    record.symbol, record.fetched_date, existing_hash, record.data_hash,
                )
                return "quarantine"
            self._seen[key] = record.data_hash
            self._buffer.append(record)
            if len(self._buffer) >= 500:
                self._flush_locked()
        return "written"

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

    @property
    def quarantine_events(self) -> list[dict[str, Any]]:
        return list(self._quarantine)

    @property
    def current_path(self) -> Path | None:
        return self._current_path

    def _flush_locked(self) -> None:
        if not self._buffer:
            return
        first = self._buffer[0]
        date_str = first.fetched_date.isoformat()

        if self._current_date != date_str:
            if self._current_writer is not None:
                self._current_writer.close()
            partition = self._derive_partition(first.fetched_date)
            partition.mkdir(parents=True, exist_ok=True)
            if self._node_id is not None and self._collector_run_id is not None:
                file_name = f"{self._collector_run_id}-{self._batch_seq}.parquet"
                self._batch_seq += 1
            else:
                file_name = f"part-{self._snapshot_id}.parquet"
            target = partition / file_name
            self._current_path = target
            schema_with_meta = _META_SCHEMA
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

    def _derive_partition(self, fetched_date: date) -> Path:
        date_str = fetched_date.isoformat()
        partition = (
            self._root
            / "market"
            / "exchange_metadata"
            / f"schema_version={EXCHANGE_METADATA_SCHEMA_VERSION}"
            / f"exchange={self._exchange}"
            / f"fetched_date={date_str}"
        )
        if self._node_id is not None:
            partition = partition / f"node={self._node_id}"
        return partition


def _records_to_arrow(records: list[ExchangeMetadataRecord]) -> pa.Table:
    return pa.Table.from_pydict(
        {
            "exchange": [r.exchange for r in records],
            "symbol": [r.symbol for r in records],
            "fetched_date": [r.fetched_date for r in records],
            "fetched_at": [r.fetched_at for r in records],
            "source_snapshot_id": [r.source_snapshot_id for r in records],
            "data_hash": [r.data_hash for r in records],
            "asset_status": [r.asset_status for r in records],
            "acc_trade_value_24h": [r.acc_trade_value_24h for r in records],
            "tick_size": [r.tick_size for r in records],
            "min_order_qty": [r.min_order_qty for r in records],
            "fee_maker": [r.fee_maker for r in records],
            "fee_taker": [r.fee_taker for r in records],
            "min_order_notional_krw": [r.min_order_notional_krw for r in records],
            "available_from_ts": [r.available_from_ts for r in records],
        },
        schema=_META_SCHEMA,
    )


# ── REST poller ────────────────────────────────────────────────────────────────

async def fetch_exchange_metadata_records(
    *,
    exchange: str,
    node_id: str | None = None,
    collector_run_id: str | None = None,
) -> list[ExchangeMetadataRecord]:
    """Fetch §D13 exchange metadata from Bithumb public REST endpoints.

    Sources (§D13.10 Public-fillable subset):
    - /public/ticker/ALL_KRW → symbol list + acc_trade_value_24H
    - /public/assetsstatus/multichain/ALL → asset_status per currency

    Nullable columns (tick_size / min_order_qty / fee_maker / fee_taker /
    min_order_notional_krw) are set to None in Phase 2 — Live Epic fills.

    Returns one ExchangeMetadataRecord per KRW symbol.
    """
    import httpx

    fetched_at = datetime.now(timezone.utc)
    fetched_date = fetched_at.date()

    ticker_url = "https://api.bithumb.com/public/ticker/ALL_KRW"
    assets_url = "https://api.bithumb.com/public/assetsstatus/multichain/ALL"

    async with httpx.AsyncClient(timeout=10.0) as client:
        ticker_resp = await client.get(ticker_url)
        ticker_resp.raise_for_status()
        ticker_payload = ticker_resp.json()

        assets_resp = await client.get(assets_url)
        assets_resp.raise_for_status()
        assets_payload = assets_resp.json()

    if ticker_payload.get("status") != "0000":
        raise RuntimeError(
            f"Bithumb ticker/ALL_KRW status={ticker_payload.get('status')!r}"
        )
    if assets_payload.get("status") != "0000":
        raise RuntimeError(
            f"Bithumb assetsstatus/ALL status={assets_payload.get('status')!r}"
        )

    # Build asset_status lookup: currency → "1"/"0"
    asset_status_map: dict[str, str] = {}
    for item in (assets_payload.get("data") or []):
        if not isinstance(item, dict):
            continue
        currency = item.get("currency")
        deposit = str(item.get("depositStatus", "0"))
        withdrawal = str(item.get("withdrawalStatus", "0"))
        combined = "1" if deposit == "1" and withdrawal == "1" else "0"
        if isinstance(currency, str) and currency:
            asset_status_map[currency] = combined

    ticker_data = ticker_payload.get("data", {})

    # Deterministic source_snapshot_id from response content
    ticker_hash = hashlib.sha256(
        json.dumps(ticker_data, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assets_hash = hashlib.sha256(
        json.dumps(assets_payload.get("data"), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    source_snapshot_id = build_source_snapshot_id(
        f"{ticker_url}|{assets_url}",
        f"{ticker_hash}|{assets_hash}",
    )

    records: list[ExchangeMetadataRecord] = []
    for code, ticker in ticker_data.items():
        if code == "date" or not isinstance(ticker, dict):
            continue
        try:
            acc_value = _to_decimal_38_18(str(ticker.get("acc_trade_value_24H", "0")))
        except Exception:
            acc_value = _to_decimal_38_18("0")
        symbol = f"KRW-{code}"
        asset_status = asset_status_map.get(code, "0")

        rec = ExchangeMetadataRecord(
            exchange=exchange,
            symbol=symbol,
            fetched_date=fetched_date,
            fetched_at=fetched_at,
            source_snapshot_id=source_snapshot_id,
            data_hash="",  # filled below
            asset_status=asset_status,
            acc_trade_value_24h=acc_value,
        )
        rec.data_hash = compute_data_hash(rec)
        records.append(rec)

    return records
