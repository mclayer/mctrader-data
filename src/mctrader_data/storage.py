"""Parquet/DuckDB write + read for ADR-009 v1 OHLCV schema.

MCT-20 extension:
- ``scan_candles(..., mode=...)`` filter (``"historical"`` default = legacy no-mode +
  ``mode=historical/`` partitions, ``"paper"`` = ``mode=paper/`` only).
- See :mod:`mctrader_data.paper_storage` for paper-mode writes.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Literal, cast

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_market.candle import CandleLike, CandleModel
from mctrader_market.types import Symbol, Timeframe

from mctrader_data.dedup import NODE_PRIORITY_DEFAULT_SENTINEL
from mctrader_data.metadata_storage import (
    EXCHANGE_METADATA_SCHEMA_VERSION,
    ExchangeMetadataRecord,
)
from mctrader_data.orderbook_snapshot_storage import (
    ORDERBOOK_SNAPSHOT_SCHEMA_VERSION,
    OrderbookSnapshotRecord,
)
from mctrader_data.path import Mode, derive_partition_path, to_duckdb_glob
from mctrader_data.schema import SCHEMA_VERSION

ScanMode = Literal["historical", "paper"]

# ── §D13/§D14 Read API forward declarations ────────────────────────────────────
# Full implementations at end of module to avoid circular imports.


def _candles_to_arrow(candles: Sequence[CandleLike]) -> pa.Table:
    """Build a PyArrow Table with ADR-009 v1 column order + types."""
    rows = {
        "ts_utc": [c.ts_utc for c in candles],
        "exchange": [c.exchange for c in candles],
        "symbol": [str(c.symbol) for c in candles],
        "timeframe": [c.timeframe.value for c in candles],
        "open": [str(c.open) for c in candles],
        "high": [str(c.high) for c in candles],
        "low": [str(c.low) for c in candles],
        "close": [str(c.close) for c in candles],
        "volume": [str(c.volume) for c in candles],
        "value": [str(c.value) if c.value is not None else None for c in candles],
        "schema_version": [SCHEMA_VERSION] * len(candles),
    }
    schema = pa.schema(
        [
            ("ts_utc", pa.timestamp("us", tz="UTC")),
            ("exchange", pa.string()),
            ("symbol", pa.string()),
            ("timeframe", pa.string()),
            ("open", pa.decimal128(38, 18)),
            ("high", pa.decimal128(38, 18)),
            ("low", pa.decimal128(38, 18)),
            ("close", pa.decimal128(38, 18)),
            ("volume", pa.decimal128(38, 18)),
            ("value", pa.decimal128(38, 18)),
            ("schema_version", pa.string()),
        ]
    )
    decimal_cols = ("open", "high", "low", "close", "volume", "value")
    arrays: list[pa.Array] = []
    for field in schema:
        col_data = rows[field.name]
        if field.name in decimal_cols:
            arrays.append(
                pa.array(
                    [Decimal(v) if v is not None else None for v in col_data],
                    type=field.type,
                )
            )
        elif field.name == "ts_utc":
            arrays.append(pa.array(col_data, type=field.type))
        else:
            arrays.append(pa.array(col_data, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def write_candles(
    candles: Sequence[CandleLike],
    *,
    root: Path,
    snapshot_id: str,
    mode: Mode | None = None,
    node_id: str | None = None,
    collector_run_id: str | None = None,
    batch_seq: int | None = None,
) -> Path:
    """Write a batch of candles as a single Parquet file under the canonical Hive partition.

    All candles MUST share ``(exchange, symbol, timeframe)``.
    The partition date is derived from the first candle's ``ts_utc.day``.
    Returns the partition directory path.

    ``mode`` is forwarded to :func:`derive_partition_path`. The default keeps the legacy
    no-mode layout for backward compatibility; paper writers should call
    :func:`mctrader_data.paper_storage.write_paper_candles`.

    MCT-91 — HA active-active kwargs (all optional):

    - ``node_id``: 명시 시 partition path 에 ``node={node_id}`` Hive level 추가
      (ADR-009 §D2.1). parquet metadata 에 ``node_id`` field 도 추가.
    - ``collector_run_id`` + ``batch_seq``: 양 값 명시 시 file name =
      ``{collector_run_id}-{batch_seq}.parquet`` (HA convention). 미명시 시 기존
      ``part-{snapshot_id}.parquet`` 유지 (legacy backfill compat).
    """
    if not candles:
        raise ValueError("write_candles: empty candles batch")
    head = candles[0]
    partition = derive_partition_path(
        root=root,
        exchange=head.exchange,
        symbol=head.symbol,
        timeframe=head.timeframe,
        ts_utc=head.ts_utc,
        mode=mode,
        node_id=node_id,
    )
    partition.mkdir(parents=True, exist_ok=True)
    table = _candles_to_arrow(candles)

    # MCT-91 — parquet metadata 에 node_id 추가 (logical key 영향 0, file footer)
    if node_id is not None:
        existing_meta = table.schema.metadata or {}
        new_meta = dict(existing_meta)
        new_meta[b"node_id"] = node_id.encode("utf-8")
        table = table.replace_schema_metadata(new_meta)

    # MCT-91 — file naming: HA convention vs legacy
    if collector_run_id is not None and batch_seq is not None:
        file_name = f"{collector_run_id}-{batch_seq}.parquet"
    else:
        file_name = f"part-{snapshot_id}.parquet"

    target = partition / file_name
    pq.write_table(table, target, compression="snappy")
    return partition


def _resolve_scan_paths(
    *,
    root: Path,
    exchange: str,
    symbol: Symbol,
    timeframe: Timeframe,
    modes: Sequence[ScanMode],
) -> list[str]:
    """Return DuckDB-friendly glob strings for the requested ``modes``.

    For ``historical``, both legacy no-mode and ``mode=historical/`` partitions are
    returned so existing 0.1.0 datasets remain readable.

    MCT-92 — recursive `**/*.parquet` glob already catches both legacy partitions
    (no `node=` directory) and post-HA partitions (`node=NODE_A`). De-dup paths
    via ``set`` to avoid redundant DuckDB scans (Codex F-5 fix).
    """
    schema_root = root / "market" / "ohlcv" / f"schema_version={SCHEMA_VERSION}"
    relative_tail = (
        f"exchange={exchange}/symbol={symbol}/timeframe={timeframe.value}/**/*.parquet"
    )
    candidates: list[Path] = []
    for mode in modes:
        if mode == "historical":
            candidates.append(schema_root)  # legacy no-mode
            candidates.append(schema_root / "mode=historical")
        elif mode == "paper":
            candidates.append(schema_root / "mode=paper")
    globs: set[str] = set()
    for base in candidates:
        if not base.exists():
            continue
        full = base / relative_tail
        globs.add(to_duckdb_glob(full))
    return sorted(globs)


def scan_candles(
    *,
    exchange: str,
    symbol: Symbol,
    timeframe: Timeframe,
    start: datetime,
    end: datetime,
    root: Path,
    mode: ScanMode | Sequence[ScanMode] = "historical",
) -> Iterable[CandleModel]:
    """Read candles for ``[start, end)`` half-open interval.

    ``mode`` semantics:

    - ``"historical"`` (default) — read legacy no-mode partitions and ``mode=historical/``.
    - ``"paper"`` — read only ``mode=paper/`` partitions.
    - ``["historical", "paper"]`` — read both explicitly.

    Returns an iterable of :class:`CandleModel` sorted ASC by ``ts_utc``.
    """
    if isinstance(mode, str):
        modes: tuple[ScanMode, ...] = (mode,)
    else:
        modes = tuple(mode)

    globs = _resolve_scan_paths(
        root=root,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        modes=modes,
    )
    if not globs:
        return

    # MCT-92 — Architect 결정 #6: hive_partitioning=false + filename=true 로
    # mixed legacy (no `node=` directory) + post-HA (`node=NODE_A/`) 의 strict
    # hive 충돌 회피. caller-side regex 로 file path 에서 node 추출 — DuckDB
    # version 의존 0 + Python 측 sentinel substitution 자유.
    union_select = " UNION ALL BY NAME ".join(
        f"SELECT * FROM read_parquet('{g}', hive_partitioning=false, filename=true)"
        for g in globs
    )

    con = duckdb.connect(":memory:", read_only=False)
    try:
        # `filename` column 으로 file path 노출 → Python regex 에서 node 추출.
        rel = con.sql(
            f"""
            SELECT ts_utc, exchange, symbol, timeframe,
                   open::VARCHAR AS open, high::VARCHAR AS high,
                   low::VARCHAR AS low, close::VARCHAR AS close,
                   volume::VARCHAR AS volume,
                   value::VARCHAR AS value,
                   filename
            FROM ({union_select}) AS combined
            WHERE ts_utc >= ? AND ts_utc < ?
            ORDER BY ts_utc ASC
            """,
            params=[start, end],
        )
        rows = rel.fetchall()
    finally:
        con.close()

    # MCT-92 — multi-node mode 자동 감지 + dedup
    # 1. wrap row 들에 node_id 부여 (file path 에서 regex 로 추출)
    # 2. distinct node_id ≥ 2 면 multi_node mode (dedup 적용)
    # 3. dedup 결과를 CandleModel 로 yield (caller transparent)
    #
    # **T1 hybrid late correction limitation (Codex F-4 acknowledged)**:
    # candle parquet schema 에 `received_at` column 부재 → scan path 에서 양 row 의
    # received_at 이 항상 ts_utc fallback (동일 값). 결과: hybrid 의 "received_at MAX"
    # phase 는 사실상 무의미 + tie-break (node priority alphabetical) 만 작동.
    # dedup module 자체는 hybrid 지원 (test_dedup.py::TestT1HybridLateCorrection 검증) —
    # 향후 candle schema 에 received_at 추가 (별도 ADR amendment) 시 자동 적용.
    import re
    from types import SimpleNamespace

    from mctrader_data.dedup import deduplicate_candles

    node_re = re.compile(r"[/\\]node=([^/\\]+)[/\\]")

    def _extract_node_id(filename: str) -> str:
        m = node_re.search(filename)
        return m.group(1) if m else NODE_PRIORITY_DEFAULT_SENTINEL

    wrapped_rows = []
    for row in rows:
        ts_utc, ex, sym_str, tf_str, o, h, lo, cl, vol, val, filename = row
        ts_normalized = cast(datetime, ts_utc)
        if ts_normalized.tzinfo is None:
            ts_normalized = ts_normalized.replace(tzinfo=timezone.utc)
        else:
            ts_normalized = ts_normalized.astimezone(timezone.utc)
        node_id = _extract_node_id(filename)
        wrapped = SimpleNamespace(
            ts_utc=ts_normalized,
            exchange=ex,
            symbol=Symbol.from_string(sym_str),
            timeframe=Timeframe(tf_str),
            open=Decimal(o),
            high=Decimal(h),
            low=Decimal(lo),
            close=Decimal(cl),
            volume=Decimal(vol),
            value=Decimal(val) if val is not None else None,
            received_at=ts_normalized,  # T1 candle: received_at = ts_utc fallback
            node_id=node_id,
        )
        wrapped_rows.append(wrapped)

    # multi_node = distinct node_id (sentinel 포함) ≥ 2
    distinct_nodes = {w.node_id for w in wrapped_rows}
    multi_node = len(distinct_nodes) >= 2

    if multi_node:
        result = deduplicate_candles(wrapped_rows, multi_node=True)
        emitted = result.emitted
    else:
        emitted = sorted(wrapped_rows, key=lambda r: r.ts_utc)

    for r in emitted:
        yield CandleModel(
            ts_utc=r.ts_utc,
            exchange=r.exchange,
            symbol=r.symbol,
            timeframe=r.timeframe,
            open=r.open,
            high=r.high,
            low=r.low,
            close=r.close,
            volume=r.volume,
            value=r.value,
        )


# ── §D13 Read API: scan_exchange_metadata ──────────────────────────────────────

def scan_exchange_metadata(
    *,
    root: Path,
    exchange: str,
    symbol: str,
    ts_utc: datetime,
) -> ExchangeMetadataRecord | None:
    """Return the most recent §D13 metadata record where ``fetched_at <= ts_utc``.

    Implements ADR-005 path-c lookahead guard: only rows with
    ``available_from_ts (= fetched_at) <= ts_utc`` are eligible.
    Returns ``None`` if no eligible row exists.

    Partition scan: fetched_date from oldest available up to ts_utc.date().
    """
    import pyarrow.parquet as _pq
    from datetime import date as _date

    meta_root = (
        root
        / "market"
        / "exchange_metadata"
        / f"schema_version={EXCHANGE_METADATA_SCHEMA_VERSION}"
        / f"exchange={exchange}"
    )
    if not meta_root.exists():
        return None

    ts_utc_aware = ts_utc.replace(tzinfo=timezone.utc) if ts_utc.tzinfo is None else ts_utc
    cutoff_date = ts_utc_aware.astimezone(timezone.utc).date()

    # Scan all fetched_date= partitions up to cutoff, collect eligible rows
    best: ExchangeMetadataRecord | None = None
    for date_dir in sorted(meta_root.iterdir()):
        dir_name = date_dir.name  # e.g. "fetched_date=2026-05-09"
        if not dir_name.startswith("fetched_date="):
            continue
        date_str = dir_name[len("fetched_date="):]
        try:
            partition_date = _date.fromisoformat(date_str)
        except ValueError:
            continue
        if partition_date > cutoff_date:
            break  # sorted ascending, stop early

        for fp in sorted(date_dir.rglob("*.parquet")):
            try:
                # Use ParquetFile.read() to avoid Hive partition schema merge issues
                pf = _pq.ParquetFile(fp)
                table = pf.read()
            except Exception:
                continue
            for row in table.to_pylist():
                row_symbol = str(row.get("symbol", "")) if row.get("symbol") is not None else ""
                if row_symbol != symbol:
                    continue
                fetched_at = row["fetched_at"]
                if isinstance(fetched_at, datetime):
                    if fetched_at.tzinfo is None:
                        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
                    else:
                        fetched_at = fetched_at.astimezone(timezone.utc)
                # ADR-005 lookahead guard
                if fetched_at > ts_utc_aware:
                    continue
                if best is None or fetched_at > best.fetched_at:
                    from decimal import Decimal as Dec
                    raw_date = row["fetched_date"]
                    parsed_date = (
                        raw_date if hasattr(raw_date, "year")
                        else _date.fromisoformat(str(raw_date))
                    )
                    best = ExchangeMetadataRecord(
                        exchange=str(row["exchange"]),
                        symbol=str(row["symbol"]),
                        fetched_date=parsed_date,
                        fetched_at=fetched_at,
                        source_snapshot_id=str(row["source_snapshot_id"]),
                        data_hash=str(row["data_hash"]),
                        asset_status=str(row["asset_status"]),
                        acc_trade_value_24h=Dec(str(row["acc_trade_value_24h"])),
                        tick_size=(
                            Dec(str(row["tick_size"]))
                            if row.get("tick_size") is not None else None
                        ),
                        min_order_qty=(
                            Dec(str(row["min_order_qty"]))
                            if row.get("min_order_qty") is not None else None
                        ),
                        fee_maker=(
                            Dec(str(row["fee_maker"]))
                            if row.get("fee_maker") is not None else None
                        ),
                        fee_taker=(
                            Dec(str(row["fee_taker"]))
                            if row.get("fee_taker") is not None else None
                        ),
                        min_order_notional_krw=(
                            Dec(str(row["min_order_notional_krw"]))
                            if row.get("min_order_notional_krw") is not None else None
                        ),
                    )
    return best


# ── §D14 Read API: scan_orderbook_snapshots ────────────────────────────────────

def scan_orderbook_snapshots(
    *,
    root: Path,
    exchange: str,
    symbol: str,
    start: datetime,
    end: datetime,
    simulated_clock: datetime | None = None,
) -> Iterable[OrderbookSnapshotRecord]:
    """Scan §D14 orderbook_snapshot.v1 partitions for ``[start, end)`` half-open.

    ADR-005 lookahead guard: ``received_at <= simulated_clock`` filter when
    ``simulated_clock`` is provided.
    Sort: ``(ts_utc ASC, baseline_seq ASC)``.

    Returns an iterable of :class:`OrderbookSnapshotRecord`.
    """
    import pyarrow.parquet as _pq
    from datetime import timedelta as _td

    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start/end must be timezone-aware UTC")

    snap_root = (
        root
        / "market"
        / "orderbook_snapshot"
        / f"schema_version={ORDERBOOK_SNAPSHOT_SCHEMA_VERSION}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
    )
    if not snap_root.exists():
        return

    # Iterate date partitions in range
    cur_date = start.astimezone(timezone.utc).date()
    end_date = end.astimezone(timezone.utc).date()

    rows_all: list[tuple[datetime, int, dict]] = []
    while cur_date <= end_date:
        date_str = cur_date.isoformat()
        date_dir = snap_root / f"date={date_str}"
        if date_dir.exists():
            for fp in sorted(date_dir.rglob("*.parquet")):
                try:
                    pf = _pq.ParquetFile(fp)
                    table = pf.read()
                except Exception:
                    continue
                for row in table.to_pylist():
                    ts = row["ts_utc"]
                    received = row["received_at"]
                    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    if not (start <= ts < end):
                        continue
                    if simulated_clock is not None:
                        if hasattr(received, "tzinfo") and received.tzinfo is None:
                            received = received.replace(tzinfo=timezone.utc)
                        if received > simulated_clock:
                            continue
                    rows_all.append((ts, row["baseline_seq"], row))
        cur_date += _td(days=1)

    rows_all.sort(key=lambda x: (x[0], x[1]))

    from decimal import Decimal as Dec
    for ts, baseline_seq, row in rows_all:
        received = row["received_at"]
        if hasattr(received, "tzinfo") and received.tzinfo is None:
            received = received.replace(tzinfo=timezone.utc)
        yield OrderbookSnapshotRecord(
            ts_utc=ts,
            received_at=received,
            exchange=row["exchange"],
            symbol=row["symbol"],
            baseline_seq=int(baseline_seq),
            side=row["side"],
            level=int(row["level"]),
            price=Dec(str(row["price"])),
            quantity=Dec(str(row["quantity"])),
            payload_hash=row["payload_hash"],
            raw_json=row.get("raw_json"),
        )
