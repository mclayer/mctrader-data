# src/mctrader_data/compactor/l3.py
"""L3Compactor: merge tier=L2 Parquet files for one UTC day → tier=L3 Parquet.

MCT-163 F6 (D4=A, D5=A) — L2 동형:
- pq.ParquetFile(f).read() → iter_batches(batch_size=1024) per-batch (D4=A)
- writer.write_table → writer.write_batch per batch (D5=A, true streaming)
- INV-4: peak RSS+tracemalloc delta ≤ 256 MB (1 GiB+ L2 input)
- INV-5: iter_batches per-batch 산출물 schema == 기존 L3 schema (forward-only)
"""
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

        # MCT-163 F6: iter_batches per-batch streaming (D4=A batch_size=1024, D5=A write_batch)
        # replaces: pq.ParquetFile(f).read() fully-load + write_table (L2 동형)
        # INV-4: peak ≤ 256 MB (per-batch, not full-table in memory)
        # INV-5: schema preserved (first file schema propagated via pq.ParquetWriter)
        last_ts = None
        monotonic_violation = False
        try:
            compactor_writer_open_count.labels(tier="L3").inc()
            try:
                with pq.ParquetWriter(str(tmp), schema, compression="snappy") as writer:
                    for f in l2_files:
                        pf = pq.ParquetFile(str(f))
                        # D4=A: iter_batches(batch_size=1024) — OLAP standard, per-batch memory
                        for batch in pf.iter_batches(batch_size=1024):
                            # D4 (MCT-160): monotonic verify per batch
                            ts_col = batch.column("ts_utc")
                            for i in range(len(ts_col)):
                                cur = ts_col[i].as_py()
                                if last_ts is not None and cur < last_ts:
                                    monotonic_violation = True
                                    break
                                last_ts = cur
                            if monotonic_violation:
                                break
                            # D5=A: write_batch (true streaming, no full-table concat)
                            writer.write_batch(batch)
                        if monotonic_violation:
                            break
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
