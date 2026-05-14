"""Unit tests for health.file_count — MCT-165 Task 6 Step 2."""

from datetime import date
from pathlib import Path

from mctrader_data.health.file_count import measure_file_count


def _make_parquet(path: Path, count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    for i in range(count):
        (path.parent / f"part-{i:04d}.parquet").write_bytes(b"x" * 100)


def test_file_count_returns_actual_count(tmp_path: Path):
    """parquet 파일 개수 실측."""
    base = tmp_path / "market" / "orderbookdepth" / "schema_version=orderbook_depth.v1"
    path = base / "tier=L1" / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-10" / "node=TEST" / "dummy.parquet"
    _make_parquet(path, 5)

    result = measure_file_count(
        root=tmp_path,
        exchanges=["bithumb"],
        symbols=["KRW-BTC"],
        tiers=["L1"],
        start_date=date(2026, 5, 10),
        end_date=date(2026, 5, 10),
    )
    assert result.total_files == 5
    assert result.per_sym_day[("bithumb", "KRW-BTC", date(2026, 5, 10))] == 5


def test_file_count_zero_for_absent_partition(tmp_path: Path):
    """파티션 없는 날 → file_count=0."""
    result = measure_file_count(
        root=tmp_path,
        exchanges=["bithumb"],
        symbols=["KRW-BTC"],
        tiers=["L1"],
        start_date=date(2026, 5, 10),
        end_date=date(2026, 5, 10),
    )
    assert result.total_files == 0
    assert result.per_sym_day == {}
