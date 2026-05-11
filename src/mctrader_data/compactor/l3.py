# src/mctrader_data/compactor/l3.py
"""L3Compactor: merge tier=L2 Parquet files for one UTC day → tier=L3 Parquet."""
from __future__ import annotations

import contextlib
import hashlib
import os
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.l1 import _schema_version
from mctrader_data.metrics import compactor_writer_open_count


class L3Compactor:
    def __init__(self, root: Path) -> None:
        self._root = root

    def compact_day(
        self,
        *,
        exchange: str,
        symbol: str,
        channel: str,
        date_utc: date,
    ) -> Path | None:
        date_str = date_utc.isoformat()
        schema_ver = _schema_version(channel)
        l2_dir = (
            self._root / "market" / channel
            / f"schema_version={schema_ver}" / "tier=L2"
            / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
        )
        # Read individual files to avoid Hive auto-discovery conflict
        l2_files = sorted(l2_dir.rglob("part-*.parquet")) if l2_dir.exists() else []
        if not l2_files:
            return None

        tables = [pq.ParquetFile(f).read() for f in l2_files]
        merged = pa.concat_tables(tables).sort_by("ts_utc")

        run_id = hashlib.sha256(
            "|".join(str(f) for f in l2_files).encode()
        ).hexdigest()[:16]

        out_dir = (
            self._root / "market" / channel
            / f"schema_version={schema_ver}" / "tier=L3"
            / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
            / "node=MERGED"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"part-{run_id}.parquet"
        tmp = out_dir / f"part-tmp-{os.getpid()}.tmp"
        # MCT-133 A1 Task 6b: use ParquetWriter as context manager so writer.close()
        # runs even when write_table raises (e.g. under memory pressure). On any
        # exception, clean the tmp file to prevent leftover *.tmp accumulation.
        # MCT-134 A2 Task 7: track open ParquetWriter instances per tier
        # (inc before open, dec in finally — paired across success + exception).
        try:
            compactor_writer_open_count.labels(tier="L3").inc()
            try:
                with pq.ParquetWriter(
                    str(tmp), merged.schema, compression="snappy"
                ) as writer:
                    writer.write_table(merged)
            finally:
                compactor_writer_open_count.labels(tier="L3").dec()
            os.replace(str(tmp), str(out_path))
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(str(tmp))
            raise
        return out_path
