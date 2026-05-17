# src/mctrader_data/compactor/l2.py
"""L2Compactor: merge tier=L1 Parquet files for one UTC hour → tier=L2 Parquet.

MCT-163 F6 (D4=A, D5=A):
- pq.ParquetFile(f).read() → iter_batches(batch_size=1024) per-batch (D4=A)
- writer.write_table → writer.write_batch per batch (D5=A, true streaming)
- INV-4: peak RSS+tracemalloc delta ≤ 256 MB (1 GiB+ L1 input)
- INV-5: iter_batches per-batch 산출물 schema == 기존 L2 schema (forward-only)

MCT-169 (ADR-029 D3=C, INV-3):
- nas_uploader inject 시: L1 source = NAS GET stream (get_streaming) — local Path open 0
- nas_uploader=None: local fallback (backward compat — test/local dev 호환)
"""
from __future__ import annotations

import contextlib
import hashlib
import os
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from mctrader_data.compactor.l1 import _schema_version
from mctrader_data.metrics import compactor_writer_open_count

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader


class L2Compactor:
    def __init__(self, root: Path, *, nas_uploader: NASUploader | None = None) -> None:
        self._root = root
        self._nas_uploader = nas_uploader  # MCT-169: NAS GET source (D3=C, INV-3)

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
        # MCT-169 INV-3: NAS GET source (nas_uploader inject 시) or local fallback
        if self._nas_uploader is not None:
            # NAS GET path: NAS key list (l1/ prefix) → get_streaming()
            return self._compact_hour_nas(
                exchange=exchange, symbol=symbol, channel=channel,
                date_str=date_str, schema_ver=schema_ver,
                hour_utc=hour_utc, out_dir_prefix=None,
            )

        # Local fallback (backward compat — nas_uploader=None)
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

        # MCT-163 F6: iter_batches per-batch streaming (D4=A batch_size=1024, D5=A write_batch)
        # replaces: pq.ParquetFile(f).read() fully-load + write_table
        # INV-4: peak ≤ 256 MB (per-batch, not full-table in memory)
        # INV-5: schema preserved (first file schema propagated via pq.ParquetWriter)
        last_ts = None
        monotonic_violation = False
        try:
            compactor_writer_open_count.labels(tier="L2").inc()
            try:
                with pq.ParquetWriter(str(tmp), schema, compression="snappy") as writer:
                    for f in l1_files:
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

    def _compact_hour_nas(
        self,
        *,
        exchange: str,
        symbol: str,
        channel: str,
        date_str: str,
        schema_ver: str,
        hour_utc: int,
        out_dir_prefix: str | None,
    ) -> Path | None:
        """MCT-169 D3=C INV-3: NAS GET source path (nas_uploader inject 시).

        NAS key prefix: single SSOT helper (ADR-034 §결정 2, U2-HELPER SSOT-4).
        §11.2-A Option A dual-prefix list union — flat (평면) + legacy (l1/) 양쪽 GET.
        NASUploader._list_objects(prefix) → NAS key list → get_streaming() 순서.
        pq.ParquetFile(BytesIO stream) 로 읽기 (local Path open 0, INV-3).
        INV-9: run_id hash input = flat_keys ONLY (legacy_keys 제외) — cutover-stable determinism.
        """
        from mctrader_data.nas_storage.get_streaming import get_streaming
        from mctrader_data.nas_storage.nas_key import build_l1_prefix, build_legacy_l1_prefix
        from mctrader_data.nas_metrics.prometheus_exporters import nas_key_helper_call_total

        flat_prefix = build_l1_prefix(
            channel=channel, schema_ver=schema_ver, exchange=exchange,
            symbol=symbol, date_str=date_str,
        )
        legacy_prefix = build_legacy_l1_prefix(
            channel=channel, schema_ver=schema_ver, exchange=exchange,
            symbol=symbol, date_str=date_str,
        )
        nas_key_helper_call_total.labels(caller="l2_compactor_get_source", tier="L1").inc()

        try:
            flat_keys = sorted(
                k for k in self._nas_uploader._list_objects(flat_prefix)  # type: ignore[union-attr]
                if k.endswith(".parquet")
            )
            legacy_keys = sorted(
                k for k in self._nas_uploader._list_objects(legacy_prefix)  # type: ignore[union-attr]
                if k.endswith(".parquet")
            )
            # §11.2-A Option A union — 평면 우선, legacy fallback (dual-read 윈도우)
            nas_keys = sorted(set(flat_keys) | set(legacy_keys))
        except Exception:
            import logging
            logging.getLogger(__name__).warning(
                "[L2Compactor] NAS _list_objects failed flat=%s legacy=%s — skip (INV-3)",
                flat_prefix,
                legacy_prefix,
            )
            return None

        if not nas_keys:
            return None

        # Pre-read first object for schema
        first_stream = get_streaming(nas_uploader=self._nas_uploader, nas_key=nas_keys[0])  # type: ignore[arg-type]
        first_pf = pq.ParquetFile(first_stream)
        schema = first_pf.schema_arrow

        # INV-9 (FIX iteration 1 Finding 3 = Option (b)) — run_id cutover-stable determinism:
        # run_id hash input = flat_keys ONLY (legacy_keys 제외).
        # 동일 partition L1 PUT set 이 고정인 한 U3-MIGRATE delete 진행 (legacy_keys shrink)
        # 와 무관하게 동일 run_id → output filename drift 0 → re-compaction trigger 차단.
        # legacy_keys = pure content GET fallback only, run_id input 아님.
        run_id = hashlib.sha256("|".join(flat_keys).encode()).hexdigest()[:16]

        out_dir = (
            self._root / "market" / channel
            / f"schema_version={schema_ver}" / "tier=L2"
            / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
            / f"hour={hour_utc:02d}" / "node=MERGED"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"part-{run_id}.parquet"
        tmp = out_dir / f"part-tmp-{os.getpid()}.tmp"

        last_ts = None
        monotonic_violation = False
        try:
            compactor_writer_open_count.labels(tier="L2").inc()
            try:
                with pq.ParquetWriter(str(tmp), schema, compression="snappy") as writer:
                    # Re-read first key (already consumed for schema)
                    for nas_key in nas_keys:
                        stream = get_streaming(nas_uploader=self._nas_uploader, nas_key=nas_key)  # type: ignore[arg-type]
                        pf = pq.ParquetFile(stream)
                        for batch in pf.iter_batches(batch_size=1024):
                            ts_col = batch.column("ts_utc")
                            for i in range(len(ts_col)):
                                cur = ts_col[i].as_py()
                                if last_ts is not None and cur < last_ts:
                                    monotonic_violation = True
                                    break
                                last_ts = cur
                            if monotonic_violation:
                                break
                            writer.write_batch(batch)
                        if monotonic_violation:
                            break
            finally:
                compactor_writer_open_count.labels(tier="L2").dec()

            if monotonic_violation:
                from mctrader_data.compactor.quarantine import quarantine_l2
                from mctrader_data.nas_metrics.prometheus_exporters import compactor_quarantine_total
                from datetime import date as date_type
                date_utc = date_type.fromisoformat(date_str)
                quarantine_l2(tmp, channel=channel, date_utc=date_utc, reason="monotonic_violation")
                compactor_quarantine_total.labels(tier="L2", reason="monotonic_violation").inc()
                return None

            os.replace(str(tmp), str(out_path))
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(str(tmp))
            raise
        return out_path
