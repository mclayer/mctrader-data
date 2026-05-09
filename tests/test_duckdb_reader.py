import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mctrader_data.duckdb_reader import DuckDBReader, MinioConfig


@pytest.fixture
def sample_parquet(tmp_path) -> Path:
    """Write a tiny Parquet file with transaction schema for tests."""
    table = pa.table({
        "ts_utc": pa.array(["2026-05-09T00:00:00+00:00", "2026-05-09T00:01:00+00:00"]),
        "exchange": pa.array(["bithumb", "bithumb"]),
        "symbol": pa.array(["KRW-BTC", "KRW-BTC"]),
        "price": pa.array([135000000, 136000000], type=pa.int64()),
        "quantity": pa.array([0.001, 0.002]),
        "side": pa.array(["bid", "ask"]),
    })
    out = tmp_path / "market" / "transaction" / "part-0.parquet"
    out.parent.mkdir(parents=True)
    pq.write_table(table, str(out))
    return tmp_path


def test_open_returns_connection(sample_parquet):
    reader = DuckDBReader(root=sample_parquet)
    with reader.open() as conn:
        assert conn is not None


def test_query_local_parquet(sample_parquet):
    reader = DuckDBReader(root=sample_parquet)
    with reader.open() as conn:
        result = conn.execute(
            "SELECT COUNT(*) as cnt FROM read_parquet(?)",
            [str(sample_parquet / "market" / "transaction" / "part-0.parquet")],
        ).fetchone()
    assert result[0] == 2


def test_query_glob_pattern(sample_parquet):
    reader = DuckDBReader(root=sample_parquet)
    with reader.open() as conn:
        result = conn.execute(
            "SELECT symbol, price FROM read_parquet(?) ORDER BY price",
            [str(sample_parquet / "market" / "transaction" / "*.parquet")],
        ).fetchall()
    assert len(result) == 2
    assert result[0][1] == 135000000


def test_minio_config_sets_s3_vars():
    cfg = MinioConfig(
        endpoint="http://minio:9000",
        access_key="test",
        secret_key="secret",
        bucket="mctrader-market",
    )
    reader = DuckDBReader(root=Path("/data"), minio=cfg)
    with reader.open() as conn:
        result = conn.execute("SELECT current_setting('s3_endpoint')").fetchone()
        assert result[0] == "minio:9000"


def test_reader_context_manager(sample_parquet):
    reader = DuckDBReader(root=sample_parquet)
    with reader.open() as conn:
        rows = conn.execute("SELECT 1 as x").fetchall()
    assert rows[0][0] == 1
