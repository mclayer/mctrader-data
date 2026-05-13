# src/mctrader_data/compactor/l2.py
"""L2Compactor: merge tier=L1 Parquet files for one UTC hour → tier=L2 Parquet."""
from __future__ import annotations

import contextlib
import hashlib
import os
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

from mctrader_data.compactor.l1 import _schema_version
from mctrader_data.metrics import compactor_writer_open_count


class L2Compactor:
    def __init__(self, root: Path) -> None:
        self._root = root

    def compact_hour(
        self,
        *,
        exchange: str,
        symbol: str,
        channel: str,
        date_utc: date,      # MCT-160 D2: caller-explicit date (KST→UTC roll silent skip 차단)
        hour_utc: int,        # 0-23
    ) -> Path | None:
        """MCT-160: caller-explicit date + chunk streaming + monotonic verify + quarantine.

        D2: date_utc 명시 (KST→UTC roll silent skip 차단)
        D3: pa.concat_tables 제거, ParquetWriter chunk write + row_group_size=100_000
        D4: post-write monotonic verify, 위반 시 quarantine
        """
        date_str = date_utc.isoformat()
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

        # Pre-read first file to extract schema (D7: nullability preserved)
        first_pf = pq.ParquetFile(str(l1_files[0]))
        schema = first_pf.schema_arrow

        run_id = hashlib.sha256(
            "|".join(str(f) for f in l1_files).encode()
        ).hexdigest()[:16]

        out_dir = (
            self._root / "market" / channel
            / f"schema_version={schema_ver}" / "tier=L2"
            / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
            / f"hour={hour_utc:02d}" / "node=MERGED"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"part-{run_id}.parquet"
        tmp = out_dir / f"part-tmp-{os.getpid()}.tmp"

        # D3: streaming chunk write with row_group_size=100_000
        # D4: monotonic verify (chunk-level, no full-table sort in memory)
        last_ts = None
        monotonic_violation = False
        try:
            compactor_writer_open_count.labels(tier="L2").inc()
            try:
                with pq.ParquetWriter(str(tmp), schema, compression="snappy") as writer:
                    for f in l1_files:
                        tbl = pq.ParquetFile(str(f)).read()
                        # D4: monotonic verify per chunk
                        ts_col = tbl.column("ts_utc")
                        for i in range(tbl.num_rows):
                            cur = ts_col[i].as_py()
                            if last_ts is not None and cur < last_ts:
                                monotonic_violation = True
                                break
                            last_ts = cur
                        if monotonic_violation:
                            break
                        writer.write_table(tbl, row_group_size=100_000)
            finally:
                compactor_writer_open_count.labels(tier="L2").dec()

            if monotonic_violation:
                from mctrader_data.compactor.quarantine import quarantine_l2
                from mctrader_data.nas_metrics.prometheus_exporters import compactor_quarantine_total
                quarantine_l2(tmp, channel=channel, date_utc=date_utc, reason="monotonic_violation")
                compactor_quarantine_total.labels(tier="L2", reason="monotonic_violation").inc()
                return None

            os.replace(str(tmp), str(out_path))
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(str(tmp))
            raise
        return out_path
