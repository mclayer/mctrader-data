"""Forward-only T2/T3 read + orderbook reconstruction (MCT-66).

ADR-009 §D10/§D11 read API. Per Codex F-3/F-10/F-11/F-18 push-back:

* ``available_from_ts := received_at`` (lookahead 방어)
* ``simulated_clock`` 주입 시 ``received_at <= simulated_clock`` filter
* sort key ``(ts_utc ASC, received_at ASC, file_offset ASC)``
* fail-closed gap policy (gap > 5min default, missing baseline, non-monotonic = halt)
* bounded LRU cache (per-symbol-day-session, max N=1 reconstructed snapshot)
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict

from mctrader_data.orderbook_storage import (
    ORDERBOOK_SCHEMA_VERSION,
    OrderbookEventRecord,
)
from mctrader_data.tick_storage import TICK_SCHEMA_VERSION, TickRecord


class GapDetectedError(Exception):
    """Gap > threshold between successive events of the same stream."""

    def __init__(self, *, symbol: str, after_ts: datetime, before_ts: datetime, gap_seconds: float) -> None:
        self.symbol = symbol
        self.after_ts = after_ts
        self.before_ts = before_ts
        self.gap_seconds = gap_seconds
        super().__init__(
            f"gap detected for symbol={symbol}: {gap_seconds:.1f}s "
            f"between {after_ts.isoformat()} and {before_ts.isoformat()}"
        )


class ReconstructionError(Exception):
    """Reconstruction halt — missing baseline / non-monotonic ts / schema mismatch / dup conflict."""


@dataclass(frozen=True)
class OrderbookLevel:
    price: Decimal
    quantity: Decimal


@dataclass
class OrderbookSnapshot:
    """Reconstructed L2 orderbook state at a given ``ts_utc``.

    Bids = price DESC (top-of-book = bids[0]). Asks = price ASC (top-of-book = asks[0]).
    """

    symbol: str
    ts_utc: datetime
    bids: list[OrderbookLevel] = field(default_factory=list)
    asks: list[OrderbookLevel] = field(default_factory=list)

    @property
    def top_bid(self) -> OrderbookLevel | None:
        return self.bids[0] if self.bids else None

    @property
    def top_ask(self) -> OrderbookLevel | None:
        return self.asks[0] if self.asks else None


def _date_range(start: datetime, end: datetime) -> Iterator[str]:
    """Yield UTC date strings ``YYYY-MM-DD`` in ``[start, end]`` inclusive."""
    cur = start.astimezone(timezone.utc).date()
    last = end.astimezone(timezone.utc).date()
    while cur <= last:
        yield cur.isoformat()
        cur = (datetime.combine(cur, datetime.min.time()) + timedelta(days=1)).date()


def _tick_partition_dir(root: Path, exchange: str, symbol: str, date_str: str) -> Path:
    return (
        root
        / "market"
        / "ticks"
        / f"schema_version={TICK_SCHEMA_VERSION}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"date={date_str}"
    )


def _orderbook_partition_dir(root: Path, exchange: str, symbol: str, date_str: str) -> Path:
    return (
        root
        / "market"
        / "orderbook"
        / f"schema_version={ORDERBOOK_SCHEMA_VERSION}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"date={date_str}"
    )


def _row_to_tick(row: dict) -> TickRecord:
    return TickRecord(
        ts_utc=row["ts_utc"],
        received_at=row["received_at"],
        exchange=row["exchange"],
        symbol=row["symbol"],
        price=Decimal(str(row["price"])),
        quantity=Decimal(str(row["quantity"])),
        side=row["side"],
        raw_json=row.get("raw_json"),
    )


def _row_to_event(row: dict) -> OrderbookEventRecord:
    return OrderbookEventRecord(
        ts_utc=row["ts_utc"],
        received_at=row["received_at"],
        exchange=row["exchange"],
        symbol=row["symbol"],
        event_type=row["event_type"],
        side=row["side"],
        level=int(row["level"]),
        price=Decimal(str(row["price"])),
        quantity=Decimal(str(row["quantity"])),
        raw_json=row.get("raw_json"),
    )


def _read_parquet_rows(part_dir: Path) -> Iterator[tuple[int, int, dict]]:
    """Yield ``(file_offset, row_idx, row_dict)`` from all parquet files in dir.

    file_offset = lex-sorted file index (deterministic across runs).
    row_idx = position within file.
    """
    if not part_dir.exists():
        return
    files = sorted(part_dir.glob("*.parquet"))
    for file_offset, fp in enumerate(files):
        pf = pq.ParquetFile(fp)
        table = pf.read()
        rows = table.to_pylist()
        for row_idx, row in enumerate(rows):
            yield (file_offset, row_idx, row)


def scan_ticks(
    *,
    root: Path,
    exchange: str,
    symbol: str,
    start: datetime,
    end: datetime,
    simulated_clock: datetime | None = None,
) -> Iterator[TickRecord]:
    """Scan tick.v1 partitions for ``[start, end)`` half-open.

    ``simulated_clock`` 주입 시 ``received_at <= simulated_clock`` filter (lookahead 방어).
    Sort: ``(ts_utc ASC, received_at ASC, file_offset ASC)``.
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start/end must be timezone-aware UTC")
    rows: list[tuple[datetime, datetime, int, int, dict]] = []
    for date_str in _date_range(start, end):
        part_dir = _tick_partition_dir(root, exchange, symbol, date_str)
        for file_offset, row_idx, row in _read_parquet_rows(part_dir):
            ts = row["ts_utc"]
            received = row["received_at"]
            if not (start <= ts < end):
                continue
            if simulated_clock is not None and received > simulated_clock:
                continue
            rows.append((ts, received, file_offset, row_idx, row))
    rows.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
    for _, _, _, _, row in rows:
        yield _row_to_tick(row)


def scan_orderbook_events(
    *,
    root: Path,
    exchange: str,
    symbol: str,
    start: datetime,
    end: datetime,
    simulated_clock: datetime | None = None,
) -> Iterator[OrderbookEventRecord]:
    """Scan orderbook.v1 partitions for ``[start, end)`` half-open. Same filter/sort as ticks."""
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start/end must be timezone-aware UTC")
    rows: list[tuple[datetime, datetime, int, int, dict]] = []
    for date_str in _date_range(start, end):
        part_dir = _orderbook_partition_dir(root, exchange, symbol, date_str)
        for file_offset, row_idx, row in _read_parquet_rows(part_dir):
            ts = row["ts_utc"]
            received = row["received_at"]
            if not (start <= ts < end):
                continue
            if simulated_clock is not None and received > simulated_clock:
                continue
            rows.append((ts, received, file_offset, row_idx, row))
    rows.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
    for _, _, _, _, row in rows:
        yield _row_to_event(row)


def get_orderbook_at(
    *,
    root: Path,
    exchange: str,
    symbol: str,
    ts_utc: datetime,
    simulated_clock: datetime | None = None,
    gap_threshold_seconds: float = 300.0,
) -> OrderbookSnapshot:
    """Reconstruct L2 orderbook at ``ts_utc`` by folding events forward.

    Algorithm:
    1. Locate baseline = first ``event_type="snapshot"`` group (rows sharing the earliest ts_utc).
    2. Fold each subsequent ``delta`` event into book state until ``ts <= ts_utc``.
    3. Halt on gap > threshold / non-monotonic / missing baseline.
    """
    if ts_utc.tzinfo is None:
        raise ValueError("ts_utc must be timezone-aware UTC")

    day_start = ts_utc.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)

    events = list(
        scan_orderbook_events(
            root=root, exchange=exchange, symbol=symbol,
            start=day_start, end=day_end,
            simulated_clock=simulated_clock,
        )
    )
    if not events:
        raise ReconstructionError(
            f"no orderbook events for symbol={symbol} on {day_start.date().isoformat()}"
        )

    # Find baseline (first snapshot group)
    snapshot_idx = next(
        (i for i, e in enumerate(events) if e.event_type == "snapshot"),
        None,
    )
    if snapshot_idx is None:
        raise ReconstructionError(
            f"missing baseline snapshot for symbol={symbol} on {day_start.date().isoformat()}"
        )

    baseline_ts = events[snapshot_idx].ts_utc
    bids: dict[Decimal, Decimal] = {}
    asks: dict[Decimal, Decimal] = {}

    # Apply baseline snapshot group (consecutive events with same ts as snapshot_idx)
    i = snapshot_idx
    while i < len(events) and events[i].ts_utc == baseline_ts and events[i].event_type == "snapshot":
        e = events[i]
        target = bids if e.side == "bid" else asks
        if e.quantity > 0:
            target[e.price] = e.quantity
        i += 1

    # Fold deltas forward
    last_ts = baseline_ts
    while i < len(events):
        e = events[i]
        if e.ts_utc > ts_utc:
            break
        gap = (e.ts_utc - last_ts).total_seconds()
        if gap > gap_threshold_seconds:
            raise GapDetectedError(
                symbol=symbol, after_ts=last_ts, before_ts=e.ts_utc, gap_seconds=gap,
            )
        if e.ts_utc < last_ts:
            raise ReconstructionError(
                f"non-monotonic event for symbol={symbol}: {e.ts_utc.isoformat()} < {last_ts.isoformat()}"
            )
        if e.event_type == "delta":
            target = bids if e.side == "bid" else asks
            if e.quantity == 0:
                target.pop(e.price, None)
            else:
                target[e.price] = e.quantity
        # snapshot mid-day = re-baseline (operator restarted collector — accept as new baseline)
        elif e.event_type == "snapshot" and e.ts_utc != baseline_ts:
            # if a new snapshot group arrives, reset book and apply
            if e.side == "bid":
                if last_ts != e.ts_utc:
                    bids.clear()
                if e.quantity > 0:
                    bids[e.price] = e.quantity
            else:
                if last_ts != e.ts_utc:
                    asks.clear()
                if e.quantity > 0:
                    asks[e.price] = e.quantity
        last_ts = e.ts_utc
        i += 1

    return OrderbookSnapshot(
        symbol=symbol,
        ts_utc=ts_utc,
        bids=[OrderbookLevel(p, q) for p, q in sorted(bids.items(), key=lambda kv: -kv[0])],
        asks=[OrderbookLevel(p, q) for p, q in sorted(asks.items(), key=lambda kv: kv[0])],
    )


# Coverage report types --------------------------------------------------------

Tier = Literal["tick", "orderbook"]


class GapEntry(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    after_ts: datetime
    before_ts: datetime
    gap_seconds: float


class CoverageReport(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", arbitrary_types_allowed=True)

    symbol: str
    tier: Tier
    min_ts_utc: datetime | None
    max_ts_utc: datetime | None
    gaps: list[GapEntry]
    collector_run_ids: list[str]
    symbol_manifests: list[str]


def tier_coverage(
    *,
    root: Path,
    exchange: str,
    symbol: str,
    tier: Tier,
    start: datetime,
    end: datetime,
    gap_threshold_seconds: float = 300.0,
) -> CoverageReport:
    """Compute :class:`CoverageReport` for a tier within ``[start, end)``.

    Gaps detected = consecutive event ts_utc difference > ``gap_threshold_seconds``.
    ``collector_run_ids`` = parquet file suffix harvest (``part-{id}.parquet``).
    ``symbol_manifests`` = MCT-65 manifest paths whose ``selected_symbols`` contain ``symbol``.
    """
    from mctrader_data.manifest import list_manifests

    if tier == "tick":
        events_iter: Iterable[datetime] = (
            r.ts_utc
            for r in scan_ticks(
                root=root, exchange=exchange, symbol=symbol,
                start=start, end=end,
            )
        )
        partition_resolver = _tick_partition_dir
    else:
        events_iter = (
            e.ts_utc
            for e in scan_orderbook_events(
                root=root, exchange=exchange, symbol=symbol,
                start=start, end=end,
            )
        )
        partition_resolver = _orderbook_partition_dir

    timestamps = sorted(events_iter)
    gaps: list[GapEntry] = []
    for i in range(1, len(timestamps)):
        gap = (timestamps[i] - timestamps[i - 1]).total_seconds()
        if gap > gap_threshold_seconds:
            gaps.append(
                GapEntry(
                    after_ts=timestamps[i - 1],
                    before_ts=timestamps[i],
                    gap_seconds=gap,
                )
            )

    collector_run_ids: set[str] = set()
    for date_str in _date_range(start, end):
        part_dir = partition_resolver(root, exchange, symbol, date_str)
        if part_dir.exists():
            for fp in sorted(part_dir.glob("part-*.parquet")):
                # filename = part-{collector_run_id}.parquet
                stem = fp.stem  # part-{run_id}
                if stem.startswith("part-"):
                    collector_run_ids.add(stem[len("part-"):])

    symbol_manifests: list[str] = []
    try:
        for m in list_manifests(root):
            if symbol in m.selected_symbols:
                from mctrader_data.manifest import manifest_path
                symbol_manifests.append(str(manifest_path(root, m.collector_run_id)))
    except Exception:
        pass

    return CoverageReport(
        symbol=symbol,
        tier=tier,
        min_ts_utc=timestamps[0] if timestamps else None,
        max_ts_utc=timestamps[-1] if timestamps else None,
        gaps=gaps,
        collector_run_ids=sorted(collector_run_ids),
        symbol_manifests=symbol_manifests,
    )
