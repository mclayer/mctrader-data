"""Unit tests for health.volume — MCT-165 Task 5.

Layout: <root>/market/orderbookdepth/schema_version=orderbook_depth.v1/
        tier={L1|L2|L3}/exchange={exchange}/symbol={symbol}/
        date={YYYY-MM-DD}/[hour={H}/][node={node}/]part-*.parquet
"""

from datetime import date
from pathlib import Path

import pytest

from mctrader_data.health.volume import measure_volume


def _make_parquet(path: Path, size_bytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size_bytes)


def test_measure_volume_sums_parquet_sizes(tmp_path: Path):
    """L1 파티션 내 parquet 파일 크기 합산."""
    base = tmp_path / "market" / "orderbookdepth" / "schema_version=orderbook_depth.v1"
    sym_day = base / "tier=L1" / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-10" / "node=TEST"
    _make_parquet(sym_day / "part-aaa.parquet", 1 * 1024 * 1024)   # 1 MiB
    _make_parquet(sym_day / "part-bbb.parquet", 2 * 1024 * 1024)   # 2 MiB
    _make_parquet(sym_day / "part-ccc.parquet", 3 * 1024 * 1024)   # 3 MiB

    result = measure_volume(
        root=tmp_path,
        exchanges=["bithumb"],
        symbols=["KRW-BTC"],
        tiers=["L1"],
        start_date=date(2026, 5, 10),
        end_date=date(2026, 5, 10),
    )
    assert result.total_bytes == 6 * 1024 * 1024
    assert result.per_sym["bithumb/KRW-BTC"] == 6 * 1024 * 1024
    assert result.per_day[date(2026, 5, 10)] == 6 * 1024 * 1024


def test_measure_volume_respects_cutin_2026_05_09(tmp_path: Path):
    """INV-2: 2026-05-09 이전 데이터 skip."""
    base = tmp_path / "market" / "orderbookdepth" / "schema_version=orderbook_depth.v1"
    # 2026-05-08 파티션 생성 (cut-in 이전)
    sym_day_before = base / "tier=L1" / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-08" / "node=TEST"
    _make_parquet(sym_day_before / "part-before.parquet", 1 * 1024 * 1024)
    # 2026-05-09 파티션 생성 (cut-in 당일)
    sym_day_cutin = base / "tier=L1" / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-09" / "node=TEST"
    _make_parquet(sym_day_cutin / "part-cutin.parquet", 2 * 1024 * 1024)

    result = measure_volume(
        root=tmp_path,
        exchanges=["bithumb"],
        symbols=["KRW-BTC"],
        tiers=["L1"],
        start_date=date(2026, 5, 9),
        end_date=date(2026, 5, 14),
    )
    # 2026-05-08 데이터 제외, 2026-05-09 포함
    assert result.total_bytes == 2 * 1024 * 1024


def test_measure_volume_multi_tier(tmp_path: Path):
    """L1 + L2 합산 측정."""
    base = tmp_path / "market" / "orderbookdepth" / "schema_version=orderbook_depth.v1"
    for tier in ["L1", "L2"]:
        sym_day = base / f"tier={tier}" / "exchange=bithumb" / "symbol=KRW-ETH" / "date=2026-05-10" / "node=TEST"
        _make_parquet(sym_day / "part-x.parquet", 1 * 1024 * 1024)

    result = measure_volume(
        root=tmp_path,
        exchanges=["bithumb"],
        symbols=["KRW-ETH"],
        tiers=["L1", "L2"],
        start_date=date(2026, 5, 10),
        end_date=date(2026, 5, 10),
    )
    assert result.total_bytes == 2 * 1024 * 1024
