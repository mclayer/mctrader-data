"""Unit tests for health.gap — MCT-165 Task 6 Step 1.

Gap layer: expected daily partition 존재 여부 검사 (INV-2 cut-in 적용).
"""

from datetime import date
from pathlib import Path

from mctrader_data.health.gap import measure_gap, MissingPartition


def _make_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    # parquet 없이 디렉터리만 (gap check는 디렉터리 존재만 봄)
    (path / "part-dummy.parquet").write_bytes(b"dummy")


def test_gap_no_missing_all_present(tmp_path: Path):
    """모든 날짜 파티션 존재 → missing_partitions 빈 리스트."""
    base = tmp_path / "market" / "orderbookdepth" / "schema_version=orderbook_depth.v1"
    for d in ["2026-05-10", "2026-05-11", "2026-05-12", "2026-05-13"]:
        path = base / "tier=L1" / "exchange=bithumb" / "symbol=KRW-BTC" / f"date={d}"
        _make_dir(path)

    result = measure_gap(
        root=tmp_path,
        exchanges=["bithumb"],
        symbols=["KRW-BTC"],
        tiers=["L1"],
        start_date=date(2026, 5, 10),
        end_date=date(2026, 5, 13),
    )
    assert result.missing_count == 0
    assert result.missing_partitions == []


def test_gap_missing_partition_detected(tmp_path: Path):
    """2026-05-11 파티션 누락 → missing_partitions에 포함."""
    base = tmp_path / "market" / "orderbookdepth" / "schema_version=orderbook_depth.v1"
    # 2026-05-11 제외하고 생성
    for d in ["2026-05-10", "2026-05-12", "2026-05-13"]:
        path = base / "tier=L1" / "exchange=bithumb" / "symbol=KRW-BTC" / f"date={d}"
        _make_dir(path)

    result = measure_gap(
        root=tmp_path,
        exchanges=["bithumb"],
        symbols=["KRW-BTC"],
        tiers=["L1"],
        start_date=date(2026, 5, 10),
        end_date=date(2026, 5, 13),
    )
    assert result.missing_count == 1
    mp = result.missing_partitions[0]
    assert isinstance(mp, MissingPartition)
    assert mp.exchange == "bithumb"
    assert mp.symbol == "KRW-BTC"
    assert mp.missing_date == date(2026, 5, 11)


def test_gap_cutin_respected(tmp_path: Path):
    """INV-2: start_date 이전 파티션 누락은 gap으로 카운트하지 않음."""
    base = tmp_path / "market" / "orderbookdepth" / "schema_version=orderbook_depth.v1"
    # 2026-05-09 이전은 없어도 OK, 2026-05-10만 생성
    path = base / "tier=L1" / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-10"
    _make_dir(path)

    result = measure_gap(
        root=tmp_path,
        exchanges=["bithumb"],
        symbols=["KRW-BTC"],
        tiers=["L1"],
        start_date=date(2026, 5, 9),   # cut-in: 2026-05-09
        end_date=date(2026, 5, 10),
    )
    # 2026-05-09 missing + 2026-05-10 present
    # gap은 expected > 0 이지만 start_date로 범위 제한 → 2026-05-09도 expected
    assert result.missing_count == 1
    assert result.missing_partitions[0].missing_date == date(2026, 5, 9)
