"""Unit tests for health.lag — MCT-165 Task 6 Step 3.

Lag = now - max(WAL segment mtime) per exchange.
WAL layout: <root>/wal/{exchange}/orderbookdepth/{symbol}/{YYYY-MM-DD}/segment-*.ndjson
"""

from datetime import datetime, timezone
from pathlib import Path

from mctrader_data.health.lag import measure_lag


def test_lag_returns_seconds_from_latest_wal(tmp_path: Path, monkeypatch):
    """WAL segment mtime 기반 lag 계산."""
    # Setup WAL structure
    wal_dir = tmp_path / "wal" / "bithumb" / "orderbookdepth" / "KRW-BTC" / "2026-05-14"
    wal_dir.mkdir(parents=True)
    seg = wal_dir / "segment-20260514T001500Z-NODE_A.ndjson"
    seg.write_bytes(b"data")

    # mtime을 2026-05-14T00:15:00 UTC로 설정
    import os, time
    ts = datetime(2026, 5, 14, 0, 15, 0, tzinfo=timezone.utc).timestamp()
    os.utime(seg, (ts, ts))

    # now = 2026-05-14T00:16:00 UTC (lag = 60s)
    fixed_now = datetime(2026, 5, 14, 0, 16, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "mctrader_data.health.lag._now_utc",
        lambda: fixed_now,
    )

    result = measure_lag(
        root=tmp_path,
        exchanges=["bithumb"],
    )
    assert result.per_exchange["bithumb"] == pytest.approx(60.0, abs=1.0)


def test_lag_no_wal_returns_none(tmp_path: Path):
    """WAL 없으면 exchange lag = None."""
    result = measure_lag(
        root=tmp_path,
        exchanges=["bithumb"],
    )
    assert result.per_exchange.get("bithumb") is None


import pytest
