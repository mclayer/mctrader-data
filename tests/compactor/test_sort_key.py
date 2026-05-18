"""_extract_min_ts — content-derived sort key (ADR-017 Amendment 3).

Primary: pq.read_metadata(path).row_group(N).column(ts_utc_idx).statistics.min
  (multi-row-group 시 min(rg.min for rg in row_groups))
Fallback: stats 부재/null 시 iter_batches(batch_size=1) first-row
Edge: 0-row file → None (skip + warning)
"""
import io
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.sort_key import _extract_min_ts


def _write_parquet(path: Path, ts_values: list[datetime], *, write_statistics: bool = True) -> None:
    table = pa.table(
        {
            "ts_utc": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
            "value": pa.array([1] * len(ts_values), type=pa.int64()),
        }
    )
    pq.write_table(table, str(path), write_statistics=write_statistics)


def test_stats_primary(tmp_path: Path) -> None:
    p = tmp_path / "a.parquet"
    ts0 = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 13, 12, 0, 5, tzinfo=timezone.utc)
    _write_parquet(p, [ts0, ts1])
    assert _extract_min_ts(p) == ts0


def test_stats_absent_fallback_to_first_row(tmp_path: Path) -> None:
    p = tmp_path / "no_stats.parquet"
    ts0 = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 13, 12, 0, 5, tzinfo=timezone.utc)
    _write_parquet(p, [ts0, ts1], write_statistics=False)
    # L1 intra-file mono 보장 (l1.py sort_by 'ts_utc') → first row = file_min
    assert _extract_min_ts(p) == ts0


def test_multi_row_group_aggregates_min(tmp_path: Path) -> None:
    p = tmp_path / "multi_rg.parquet"
    ts_late = datetime(2026, 5, 13, 15, 0, 0, tzinfo=timezone.utc)
    ts_early = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)
    # 두 row-group: 첫 rg = late, 두번째 rg = early (의도적 비정상 순서 — file-level min 검증)
    schema = pa.schema([
        ("ts_utc", pa.timestamp("us", tz="UTC")),
        ("value", pa.int64()),
    ])
    with pq.ParquetWriter(str(p), schema, write_statistics=True) as w:
        w.write_table(pa.table({"ts_utc": [ts_late], "value": [1]}, schema=schema))
        w.write_table(pa.table({"ts_utc": [ts_early], "value": [2]}, schema=schema))
    assert _extract_min_ts(p) == ts_early


def test_zero_row_file_returns_none(tmp_path: Path) -> None:
    p = tmp_path / "empty.parquet"
    schema = pa.schema([
        ("ts_utc", pa.timestamp("us", tz="UTC")),
        ("value", pa.int64()),
    ])
    pq.write_table(pa.table({"ts_utc": [], "value": []}, schema=schema), str(p))
    assert _extract_min_ts(p) is None


def test_stats_primary_via_bytesio(tmp_path: Path) -> None:
    """BytesIO stream input (NAS GET stream 경로) — stats present case."""
    p = tmp_path / "s.parquet"
    ts0 = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 13, 12, 0, 5, tzinfo=timezone.utc)
    _write_parquet(p, [ts0, ts1])
    stream = io.BytesIO(p.read_bytes())
    assert _extract_min_ts(stream) == ts0
