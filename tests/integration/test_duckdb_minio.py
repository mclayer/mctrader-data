"""Integration test: DuckDB reads Parquet from running MinIO.
Requires: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY env vars
          and at least one Parquet uploaded by compactor.
"""
import os

import pytest

from mctrader_data.duckdb_reader import DuckDBReader, MinioConfig


@pytest.mark.integration
def test_duckdb_reads_from_minio():
    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")
    if not (endpoint and access_key and secret_key):
        pytest.skip("MINIO_* env vars not set")

    cfg = MinioConfig(endpoint=endpoint, access_key=access_key, secret_key=secret_key)
    reader = DuckDBReader(root=None, minio=cfg)  # type: ignore[arg-type]
    with reader.open() as conn:
        result = conn.execute(
            "SELECT COUNT(*) FROM read_parquet('s3://mctrader-market/**/*.parquet', hive_partitioning=true)"
        ).fetchone()
    assert result[0] >= 0
