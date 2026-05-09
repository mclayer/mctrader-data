from __future__ import annotations

import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str = "mctrader-market"


class DuckDBReader:
    """Factory that opens a DuckDB in-memory connection, optionally configuring httpfs for MinIO."""

    def __init__(self, root: Path, minio: MinioConfig | None = None) -> None:
        self._root = root
        self._minio = minio

    @contextlib.contextmanager
    def open(self) -> Generator:
        import duckdb

        conn = duckdb.connect(database=":memory:")
        try:
            if self._minio is not None:
                self._configure_httpfs(conn, self._minio)
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _configure_httpfs(conn, cfg: MinioConfig) -> None:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        parsed = urlparse(cfg.endpoint)
        endpoint_host = parsed.netloc
        conn.execute(f"SET s3_endpoint='{endpoint_host}'")
        conn.execute(f"SET s3_access_key_id='{cfg.access_key}'")
        conn.execute(f"SET s3_secret_access_key='{cfg.secret_key}'")
        conn.execute("SET s3_use_ssl=false")
        conn.execute("SET s3_url_style='path'")
