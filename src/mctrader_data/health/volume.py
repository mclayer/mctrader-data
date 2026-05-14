"""Volume layer measurement — MCT-165 Task 5.

Read-only fs walk (INV-1 detective only). No writes.
INV-2: start_date = 2026-05-09 cut-in default.

Actual storage layout (reconciled Task 5 Step 3):
    <root>/market/orderbookdepth/schema_version=orderbook_depth.v1/
    tier={L1|L2|L3}/exchange={exchange}/symbol={symbol}/
    date={YYYY-MM-DD}/[hour={H}/][node={node}/]part-*.parquet

The layout uses Hive-style partitioning with 'symbol=' prefix in directory names
(e.g., symbol=KRW-BTC), not bare symbol names.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

SCHEMA_VERSION = "orderbook_depth.v1"
_MARKET_BASE = "market/orderbookdepth"


@dataclass
class VolumeResult:
    """Volume measurement result.

    Attributes:
        total_bytes: 전체 합산 bytes.
        per_sym: {"{exchange}/{symbol}": bytes} 맵.
        per_day: {date: bytes} 맵.
        missing_symbols: 측정 기간 내 데이터 없는 (exchange, symbol) 쌍.
    """

    total_bytes: int = 0
    per_sym: dict[str, int] = field(default_factory=dict)
    per_day: dict[date, int] = field(default_factory=dict)
    missing_symbols: list[tuple[str, str]] = field(default_factory=list)


def measure_volume(
    root: Path,
    exchanges: list[str],
    symbols: list[str],
    tiers: list[str],
    start_date: date,
    end_date: date,
) -> VolumeResult:
    """Parquet 부피 측정 — read-only fs walk (INV-1 detective only).

    Layout (reconciled from actual NAS):
        <root>/market/orderbookdepth/schema_version=<ver>/
        tier=<tier>/exchange=<exchange>/symbol=<symbol>/
        date=<YYYY-MM-DD>/[hour=<H>/][node=<node>/]part-*.parquet

    Args:
        root: MCTRADER_DATA_ROOT (e.g., /var/lib/mctrader/data).
        exchanges: exchange 이름 목록 (e.g., ["bithumb", "upbit"]).
        symbols: symbol 이름 목록 (e.g., ["KRW-BTC", "KRW-ETH"]).
        tiers: tier 이름 목록 (e.g., ["L1", "L2", "L3"]).
        start_date: INV-2 cut-in (2026-05-09 이상).
        end_date: 측정 종료일 (inclusive).

    Returns:
        VolumeResult with total_bytes / per_sym / per_day / missing_symbols.
    """
    result = VolumeResult()
    schema_base = root / _MARKET_BASE / f"schema_version={SCHEMA_VERSION}"

    cur = start_date
    while cur <= end_date:
        day_total = 0
        date_str = cur.isoformat()

        for tier in tiers:
            for exchange in exchanges:
                for symbol in symbols:
                    sym_key = f"{exchange}/{symbol}"
                    sym_day = (
                        schema_base
                        / f"tier={tier}"
                        / f"exchange={exchange}"
                        / f"symbol={symbol}"
                        / f"date={date_str}"
                    )
                    if not sym_day.is_dir():
                        continue
                    # Recursive glob — handles hour=H/node=N/part-*.parquet nesting
                    sym_bytes = sum(
                        f.stat().st_size
                        for f in sym_day.rglob("*.parquet")
                        if f.is_file()
                    )
                    result.per_sym[sym_key] = result.per_sym.get(sym_key, 0) + sym_bytes
                    day_total += sym_bytes

        if day_total > 0:
            result.per_day[cur] = result.per_day.get(cur, 0) + day_total
        result.total_bytes += day_total
        cur += timedelta(days=1)

    # missing_symbols: 전체 기간 동안 데이터가 전혀 없는 (exchange, symbol)
    for exchange in exchanges:
        for symbol in symbols:
            sym_key = f"{exchange}/{symbol}"
            if result.per_sym.get(sym_key, 0) == 0:
                result.missing_symbols.append((exchange, symbol))

    return result
