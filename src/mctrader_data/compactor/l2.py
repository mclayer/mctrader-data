# src/mctrader_data/compactor/l2.py
"""L2Compactor: merge tier=L1 Parquet files for one UTC hour → tier=L2 Parquet."""
from __future__ import annotations

import contextlib
import hashlib
import os
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.l1 import _schema_version


class L2Compactor:
    def __init__(self, root: Path) -> None:
        self._root = root

    def compact_hour(
        self,
        *,
        exchange: str,
        symbol: str,
        channel: str,
        hour_utc: datetime,
    ) -> Path | None:
        """Merge all tier=L1 Parquet for (exchange, symbol, channel, hour) → tier=L2."""
        date_str = hour_utc.strftime("%Y-%m-%d")
        schema_ver = _schema_version(channel)
        l1_dir = (
            self._root / "market" / channel
            / f"schema_version={schema_ver}" / "tier=L1"
            / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
        )
        # Read individual files (not directory) to avoid Hive auto-discovery conflict
        l1_files = sorted(l1_dir.rglob("part-*.parquet")) if l1_dir.exists() else []
        if not l1_files:
            return None

        tables = [pq.ParquetFile(f).read() for f in l1_files]
        merged = pa.concat_tables(tables).sort_by("ts_utc")

        run_id = hashlib.sha256(
            "|".join(str(f) for f in l1_files).encode()
        ).hexdigest()[:16]

        out_dir = (
            self._root / "market" / channel
            / f"schema_version={schema_ver}" / "tier=L2"
            / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
            / f"hour={hour_utc.strftime('%H')}" / "node=MERGED"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"part-{run_id}.parquet"
        tmp = out_dir / f"part-tmp-{os.getpid()}.tmp"
        # MCT-133 A1 Task 6a: use ParquetWriter as context manager so writer.close()
        # runs even when write_table raises (e.g. under memory pressure). On any
        # exception, clean the tmp file to prevent leftover *.tmp accumulation.
        try:
            with pq.ParquetWriter(
                str(tmp), merged.schema, compression="snappy"
            ) as writer:
                writer.write_table(merged)
            os.replace(str(tmp), str(out_path))
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(str(tmp))
            raise
        return out_path
