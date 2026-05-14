"""Gap layer measurement — MCT-165 Task 6 Step 1.

Detects missing daily partitions for expected (exchange, symbol, tier, date) combinations.
Read-only fs walk (INV-1). INV-2 start_date applies.

Layout: <root>/market/orderbookdepth/schema_version=orderbook_depth.v1/
        tier={tier}/exchange={exchange}/symbol={symbol}/date={YYYY-MM-DD}/
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

SCHEMA_VERSION = "orderbook_depth.v1"
_MARKET_BASE = "market/orderbookdepth"


@dataclass(frozen=True)
class MissingPartition:
    """단일 누락 파티션 레코드."""

    exchange: str
    symbol: str
    tier: str
    missing_date: date


@dataclass
class GapResult:
    """Gap layer 측정 결과.

    Attributes:
        missing_count: 총 누락 파티션 수.
        missing_partitions: MissingPartition 목록.
        total_expected: 기대 파티션 총수 (exchanges × symbols × tiers × days).
    """

    missing_count: int = 0
    missing_partitions: list[MissingPartition] = field(default_factory=list)
    total_expected: int = 0


def _has_any_parquet(path: Path) -> bool:
    """디렉터리 내 (재귀적으로) parquet 파일 1개 이상 존재 확인."""
    return any(True for _ in path.rglob("*.parquet"))


def measure_gap(
    root: Path,
    exchanges: list[str],
    symbols: list[str],
    tiers: list[str],
    start_date: date,
    end_date: date,
) -> GapResult:
    """일별 파티션 존재 여부 검사 — read-only (INV-1).

    expected = exchanges × symbols × tiers × [start_date, end_date] inclusive.
    missing = expected 중 디렉터리가 없거나 parquet 파일이 0개인 파티션.

    Args:
        root: MCTRADER_DATA_ROOT.
        exchanges: exchange 이름 목록.
        symbols: symbol 이름 목록.
        tiers: tier 이름 목록.
        start_date: 검증 시작일 (INV-2 cut-in 이상으로 caller가 보장).
        end_date: 검증 종료일 (inclusive).

    Returns:
        GapResult.
    """
    result = GapResult()
    schema_base = root / _MARKET_BASE / f"schema_version={SCHEMA_VERSION}"

    cur = start_date
    while cur <= end_date:
        date_str = cur.isoformat()
        for tier in tiers:
            for exchange in exchanges:
                for symbol in symbols:
                    result.total_expected += 1
                    sym_day = (
                        schema_base
                        / f"tier={tier}"
                        / f"exchange={exchange}"
                        / f"symbol={symbol}"
                        / f"date={date_str}"
                    )
                    # 디렉터리 없거나 parquet 없으면 gap
                    if not sym_day.is_dir() or not _has_any_parquet(sym_day):
                        result.missing_count += 1
                        result.missing_partitions.append(
                            MissingPartition(
                                exchange=exchange,
                                symbol=symbol,
                                tier=tier,
                                missing_date=cur,
                            )
                        )
        cur += timedelta(days=1)

    return result
