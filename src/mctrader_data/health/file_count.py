"""File count layer measurement — MCT-165 Task 6 Step 2.

Counts parquet files per (exchange, symbol, date) partition.
Read-only fs walk (INV-1). INV-2 start_date applies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

SCHEMA_VERSION = "orderbook_depth.v1"
_MARKET_BASE = "market/orderbookdepth"


@dataclass
class FileCountResult:
    """File count layer 측정 결과.

    Attributes:
        total_files: 기간 내 전체 parquet 파일 수.
        per_sym_day: {(exchange, symbol, date): file_count} 맵.
    """

    total_files: int = 0
    per_sym_day: dict[tuple[str, str, date], int] = field(default_factory=dict)


def measure_file_count(
    root: Path,
    exchanges: list[str],
    symbols: list[str],
    tiers: list[str],
    start_date: date,
    end_date: date,
) -> FileCountResult:
    """파티션 내 parquet 파일 수 측정 — read-only (INV-1).

    Args:
        root: MCTRADER_DATA_ROOT.
        exchanges: exchange 이름 목록.
        symbols: symbol 이름 목록.
        tiers: tier 이름 목록.
        start_date: 검증 시작일.
        end_date: 검증 종료일 (inclusive).

    Returns:
        FileCountResult.
    """
    result = FileCountResult()
    schema_base = root / _MARKET_BASE / f"schema_version={SCHEMA_VERSION}"

    cur = start_date
    while cur <= end_date:
        date_str = cur.isoformat()
        for tier in tiers:
            for exchange in exchanges:
                for symbol in symbols:
                    sym_day = (
                        schema_base
                        / f"tier={tier}"
                        / f"exchange={exchange}"
                        / f"symbol={symbol}"
                        / f"date={date_str}"
                    )
                    if not sym_day.is_dir():
                        continue
                    count = sum(1 for _ in sym_day.rglob("*.parquet"))
                    if count > 0:
                        key = (exchange, symbol, cur)
                        result.per_sym_day[key] = result.per_sym_day.get(key, 0) + count
                        result.total_files += count
        cur += timedelta(days=1)

    return result
