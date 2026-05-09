# tests/integration/test_ingester_compactor_roundtrip.py
"""§8.4 Integration: Ingester → WAL → L1Compactor → Parquet → verify 1:1 records."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed
from mctrader_data.compactor.l1 import L1Compactor


def test_roundtrip_100_records(tmp_path: Path) -> None:
    ing = WalIngester(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        channel="transaction", node_id="INT_TEST", segment_seconds=86400,
    )
    n = 100
    for i in range(n):
        ts = datetime(2026, 5, 9, 0, 0, i % 60, tzinfo=timezone.utc)
        ing.append({
            "ts_utc": ts.isoformat(), "received_at": ts.isoformat(),
            "exchange": "bithumb", "symbol": "KRW-BTC",
            "price": Decimal(str(100000 + i)),
            "quantity": Decimal("0.01"),
            "side": "buy", "raw_json": None, "channel": "transaction",
        })
    ing.close()

    compactor = L1Compactor(root=tmp_path)
    for sealed in scan_sealed(tmp_path):
        compactor.compact_segment(sealed)

    parquet_files = list((tmp_path / "market").rglob("*/tier=L1/**/*.parquet"))
    assert len(parquet_files) >= 1
    total_rows = sum(pq.ParquetFile(f).read().num_rows for f in parquet_files)
    assert total_rows == n


def test_multi_exchange_isolation(tmp_path: Path) -> None:
    """§8.4: two exchanges write to separate WAL trees."""
    for exchange in ("bithumb", "upbit"):
        ing = WalIngester(
            root=tmp_path, exchange=exchange, symbol="KRW-BTC",
            channel="transaction", node_id=f"NODE_{exchange.upper()}",
            segment_seconds=86400,
        )
        for i in range(10):
            ing.append({
                "ts_utc": f"2026-05-09T00:00:{i:02d}+00:00",
                "received_at": f"2026-05-09T00:00:{i:02d}+00:00",
                "exchange": exchange, "symbol": "KRW-BTC",
                "price": Decimal("100000"), "quantity": Decimal("0.01"),
                "side": "buy", "raw_json": None,
            })
        ing.close()

    bithumb_sealed = list((tmp_path / "wal" / "bithumb").rglob("*.ndjson.sealed"))
    upbit_sealed = list((tmp_path / "wal" / "upbit").rglob("*.ndjson.sealed"))
    assert len(bithumb_sealed) == 1
    assert len(upbit_sealed) == 1
