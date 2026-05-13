# src/mctrader_data/compactor/l3.py
"""L3Compactor: merge tier=L2 Parquet files for one UTC day → tier=L3 Parquet."""
from __future__ import annotations

import contextlib
import hashlib
import os
from datetime import date
from pathlib import Path

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
        """MCT-160 D1+D3+D4: L2 동형 streaming pattern (D3 chunk write + D4 monotonic verify).

        D1: L3 = 1day window (hour_utc 인자 없음, L2 모든 hour 병합)
        D2: date_utc caller 명시
        D3: pa.concat_tables 제거, ParquetWriter chunk write + row_group_size=100_000
        D4: post-write monotonic verify, 위반 시 quarantine
        """
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

        # Pre-read first file to extract schema (D7: nullability preserved)
        first_pf = pq.ParquetFile(str(l2_files[0]))
        schema = first_pf.schema_arrow

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

        # D3: streaming chunk write with row_group_size=100_000
        # D4: monotonic verify (chunk-level, no full-table sort in memory)
        last_ts = None
        monotonic_violation = False
        try:
            compactor_writer_open_count.labels(tier="L3").inc()
            try:
                with pq.ParquetWriter(str(tmp), schema, compression="snappy") as writer:
                    for f in l2_files:
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
                compactor_writer_open_count.labels(tier="L3").dec()

            if monotonic_violation:
                from mctrader_data.compactor.quarantine import quarantine_l3
                from mctrader_data.nas_metrics.prometheus_exporters import compactor_quarantine_total
                quarantine_l3(tmp, channel=channel, date_utc=date_utc, reason="monotonic_violation")
                compactor_quarantine_total.labels(tier="L3", reason="monotonic_violation").inc()
                return None

            os.replace(str(tmp), str(out_path))
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(str(tmp))
            raise
        return out_path
