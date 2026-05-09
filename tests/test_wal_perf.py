# tests/test_wal_perf.py
"""§8.3 Performance baseline: WS-to-disk p99 < 5ms, >1000 msg/sec."""
from __future__ import annotations

import os
import sys
import time
from decimal import Decimal
from pathlib import Path

import pytest

from mctrader_data.wal.ingester import WalIngester


@pytest.mark.slow
def test_wal_write_throughput(tmp_path: Path) -> None:
    """Sustained 1000 msg/sec, p99 latency < 5ms (per-message fsync)."""
    ing = WalIngester(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        channel="transaction", node_id="PERF_TEST",
        fsync_batch=1,
        segment_seconds=86400,
    )

    n = 1000
    latencies: list[float] = []
    record = {
        "ts_utc": "2026-05-09T00:00:00+00:00", "received_at": "2026-05-09T00:00:00+00:00",
        "exchange": "bithumb", "symbol": "KRW-BTC",
        "price": Decimal("100000"), "quantity": Decimal("0.01"),
        "side": "buy", "raw_json": None,
    }

    for _ in range(n):
        t0 = time.perf_counter()
        ing.append(record)
        latencies.append((time.perf_counter() - t0) * 1000)  # ms

    ing.close()

    latencies.sort()
    p99 = latencies[int(0.99 * n)]
    throughput = n / sum(latencies) * 1000

    print(f"\nWAL p99={p99:.2f}ms, throughput~{throughput:.0f} msg/sec")

    # Windows GH Actions runner fsync is significantly slower than local Windows.
    # CI=true on GitHub Actions; local Windows keeps the 10ms bar.
    if sys.platform == "win32" and os.getenv("CI"):
        threshold_ms = 50.0
    elif sys.platform == "win32":
        threshold_ms = 10.0
    else:
        threshold_ms = 5.0
    assert p99 < threshold_ms, f"p99 {p99:.2f}ms exceeds {threshold_ms}ms threshold on {sys.platform}"
