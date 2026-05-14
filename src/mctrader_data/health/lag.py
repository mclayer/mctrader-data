"""Lag layer measurement — MCT-165 Task 6 Step 3.

Measures collector write lag from WAL segment mtime.
WAL layout: <root>/wal/{exchange}/orderbookdepth/{symbol}/{YYYY-MM-DD}/segment-*.ndjson

Read-only fs walk (INV-1). SLO = 60s (MCT-165 D5=C).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


def _now_utc() -> datetime:
    """현재 UTC 시각 반환 — monkeypatch 대상."""
    return datetime.now(tz=timezone.utc)


@dataclass
class LagResult:
    """Lag layer 측정 결과.

    Attributes:
        per_exchange: {exchange: lag_seconds | None} 맵.
            None = WAL 파일 없음 (측정 불가).
        max_lag_seconds: 전체 exchange 중 최대 lag (None if all missing).
    """

    per_exchange: dict[str, float | None] = field(default_factory=dict)

    @property
    def max_lag_seconds(self) -> float | None:
        vals = [v for v in self.per_exchange.values() if v is not None]
        return max(vals) if vals else None


def measure_lag(
    root: Path,
    exchanges: list[str],
) -> LagResult:
    """WAL segment mtime 기반 collector lag 측정 — read-only (INV-1).

    WAL layout: <root>/wal/{exchange}/orderbookdepth/**/*.ndjson
    lag = now_utc - max(mtime of .ndjson files)

    Args:
        root: MCTRADER_DATA_ROOT.
        exchanges: exchange 이름 목록.

    Returns:
        LagResult with per_exchange lag in seconds.
    """
    result = LagResult()
    now = _now_utc()

    for exchange in exchanges:
        wal_base = root / "wal" / exchange / "orderbookdepth"
        if not wal_base.is_dir():
            result.per_exchange[exchange] = None
            continue

        latest_mtime: float | None = None
        for seg in wal_base.rglob("*.ndjson"):
            try:
                mtime = seg.stat().st_mtime
            except OSError:
                continue
            if latest_mtime is None or mtime > latest_mtime:
                latest_mtime = mtime

        if latest_mtime is None:
            result.per_exchange[exchange] = None
        else:
            latest_dt = datetime.fromtimestamp(latest_mtime, tz=timezone.utc)
            lag_seconds = (now - latest_dt).total_seconds()
            result.per_exchange[exchange] = max(0.0, lag_seconds)

    return result
