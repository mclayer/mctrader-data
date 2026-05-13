# tests/test_compactor_l3.py
"""INV-8: L3 reprocessing is monotone."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from mctrader_data.compactor.l1 import L1Compactor
from mctrader_data.compactor.l2 import L2Compactor
from mctrader_data.compactor.l3 import L3Compactor
from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed

# Use today's date so that WalIngester's wall-clock-based partitioning
# matches the compact_* calls' date.
_TODAY = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _setup_l2(tmp_path: Path, n: int) -> None:
    ing = WalIngester(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        channel="transaction", node_id="N", segment_seconds=86400,
    )
    for i in range(n):
        ts = _TODAY.replace(second=i)
        ing.append({
            "ts_utc": ts.isoformat(), "received_at": ts.isoformat(),
            "exchange": "bithumb", "symbol": "KRW-BTC",
            "price": Decimal("100000"), "quantity": Decimal("0.01"),
            "side": "buy", "raw_json": None, "channel": "transaction",
        })
    ing.close()
    for s in scan_sealed(tmp_path):
        L1Compactor(root=tmp_path).compact_segment(s)
    # MCT-160 D2: date_utc=date, hour_utc=int
    L2Compactor(root=tmp_path).compact_hour(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction",
        date_utc=_TODAY.date(),
        hour_utc=_TODAY.hour,
    )


def test_l3_produces_daily_parquet(tmp_path: Path) -> None:
    _setup_l2(tmp_path, 10)
    compactor = L3Compactor(root=tmp_path)
    result = compactor.compact_day(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction",
        date_utc=_TODAY.date(),
    )
    assert result is not None
    assert "tier=L3" in result.parts
    assert pq.ParquetFile(result).read().num_rows == 10


def test_l3_reprocessing_monotone(tmp_path: Path) -> None:
    """INV-8: compact same day twice → row count non-decreasing."""
    _setup_l2(tmp_path, 10)
    compactor = L3Compactor(root=tmp_path)
    d = _TODAY.date()
    r1 = compactor.compact_day(exchange="bithumb", symbol="KRW-BTC", channel="transaction", date_utc=d)
    r2 = compactor.compact_day(exchange="bithumb", symbol="KRW-BTC", channel="transaction", date_utc=d)
    assert pq.ParquetFile(r2).read().num_rows >= pq.ParquetFile(r1).read().num_rows
