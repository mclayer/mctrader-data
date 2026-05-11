"""Shared fixtures for Cold path tests.

Synthesises transaction Parquet files at the canonical Hive layout
(``market/transaction/schema_version=tick.v1/tier=L1/exchange=.../symbol=.../
date=.../node=.../part-<run_id>.parquet``) using the same
:class:`mctrader_data.compactor.l1.L1Compactor` pipeline that production
collectors emit. This keeps test inputs byte-identical to production Parquet —
no schema drift between test fixtures and the real Cold path read path.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from collections.abc import Iterable

import pytest

from mctrader_data.compactor.l1 import L1Compactor
from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed


@dataclass(frozen=True)
class TickSpec:
    ts_utc: datetime
    price: str
    quantity: str
    side: str = "buy"
    exchange: str = "bithumb"
    symbol: str = "KRW-BTC"


def _tick_to_wal_record(spec: TickSpec) -> dict:
    return {
        "ts_utc": spec.ts_utc.isoformat(),
        "received_at": spec.ts_utc.isoformat(),
        "exchange": spec.exchange,
        "symbol": spec.symbol,
        "price": Decimal(spec.price),
        "quantity": Decimal(spec.quantity),
        "side": spec.side,
        "raw_json": None,
        "channel": "transaction",
    }


def write_parquet_fixture(
    root: Path,
    ticks: Iterable[TickSpec],
    *,
    node_id: str = "NODE_A",
) -> list[Path]:
    """Append all ticks via WAL → compact L1 → return resulting Parquet paths.

    Groups by ``(exchange, symbol)`` so the WAL writer's per-symbol-channel
    invariant holds, but uses a single WAL session per group so all ticks for
    one symbol end up in the same sealed segment (one Parquet file per symbol).
    """
    by_key: dict[tuple[str, str], list[TickSpec]] = {}
    for spec in ticks:
        by_key.setdefault((spec.exchange, spec.symbol), []).append(spec)

    for (exch, sym), specs in by_key.items():
        ing = WalIngester(
            root=root,
            exchange=exch,
            symbol=sym,
            channel="transaction",
            node_id=node_id,
            segment_seconds=86_400,  # don't auto-seal mid-test
        )
        try:
            for spec in specs:
                ing.append(_tick_to_wal_record(spec))
        finally:
            ing.close()

    compactor = L1Compactor(root=root)
    out: list[Path] = []
    for sealed in scan_sealed(root):
        out.append(compactor.compact_segment(sealed))
    return out


@pytest.fixture
def parquet_root(tmp_path: Path) -> Path:
    """Returns the L1 root path (callers populate it via ``write_parquet_fixture``)."""
    return tmp_path


@pytest.fixture
def write_ticks(tmp_path: Path):
    """Convenience callable: write a list of TickSpec → return Parquet paths."""

    def _write(ticks: Iterable[TickSpec], *, node_id: str = "NODE_A") -> list[Path]:
        return write_parquet_fixture(tmp_path, ticks, node_id=node_id)

    return _write


@pytest.fixture
def utc():
    """``datetime`` factory in UTC — avoids ``tzinfo=`` boilerplate in test bodies."""

    def _utc(year=2026, month=1, day=1, hour=0, minute=0, second=0, microsecond=0):
        return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=timezone.utc)

    return _utc
