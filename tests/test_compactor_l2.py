# tests/test_compactor_l2.py
"""INV-7: L1×12 → L2 preserves all records."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from mctrader_data.compactor.l1 import L1Compactor
from mctrader_data.compactor.l2 import L2Compactor
from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed


def _write_and_compact_l1(tmp_path: Path, n_records: int, node_id: str = "N") -> None:
    ing = WalIngester(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        channel="transaction", node_id=node_id, segment_seconds=86400,
    )
    for i in range(n_records):
        ts = datetime(2026, 5, 9, 0, 0, i, tzinfo=timezone.utc)
        ing.append({
            "ts_utc": ts.isoformat(), "received_at": ts.isoformat(),
            "exchange": "bithumb", "symbol": "KRW-BTC",
            "price": Decimal("100000"), "quantity": Decimal("0.01"),
            "side": "buy", "raw_json": None, "channel": "transaction",
        })
    ing.close()
    for s in scan_sealed(tmp_path):
        L1Compactor(root=tmp_path).compact_segment(s)


def test_l2_merges_l1_files(tmp_path: Path) -> None:
    _write_and_compact_l1(tmp_path, 20)
    compactor = L2Compactor(root=tmp_path)
    result = compactor.compact_hour(
        exchange="bithumb", symbol="KRW-BTC",
        channel="transaction",
        hour_utc=datetime(2026, 5, 9, 0, 0, tzinfo=timezone.utc),
    )
    assert result is not None
    tbl = pq.ParquetFile(result).read()
    assert tbl.num_rows == 20
    parts = result.parts
    assert "tier=L2" in parts


def test_l2_row_count_equals_l1_total(tmp_path: Path) -> None:
    """INV-7: L2 row count == sum of all L1 rows for that hour."""
    _write_and_compact_l1(tmp_path, 50)
    l1_files = list((tmp_path / "market").rglob("*/tier=L1/**/*.parquet"))
    l1_total = sum(pq.ParquetFile(f).read().num_rows for f in l1_files)

    compactor = L2Compactor(root=tmp_path)
    result = compactor.compact_hour(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction",
        hour_utc=datetime(2026, 5, 9, 0, 0, tzinfo=timezone.utc),
    )
    assert pq.ParquetFile(result).read().num_rows == l1_total
