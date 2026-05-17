# src/mctrader_data/compactor/l3.py
"""L3Compactor: merge tier=L2 Parquet files for one UTC day → tier=L3 Parquet.

MCT-163 F6 (D4=A, D5=A) — L2 동형:
- pq.ParquetFile(f).read() → iter_batches(batch_size=1024) per-batch (D4=A)
- writer.write_table → writer.write_batch per batch (D5=A, true streaming)
- INV-4: peak RSS+tracemalloc delta ≤ 256 MB (1 GiB+ L2 input)
- INV-5: iter_batches per-batch 산출물 schema == 기존 L3 schema (forward-only)

MCT-169 (ADR-029 D3=C, INV-3):
- nas_uploader inject 시: L2 source = NAS GET stream (get_streaming) — local Path open 0
- nas_uploader=None: local fallback (backward compat — test/local dev 호환)
"""
from __future__ import annotations

import contextlib
import hashlib
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from mctrader_data.compactor.l1 import _schema_version
from mctrader_data.compactor.sort_key import _extract_min_ts
from mctrader_data.metrics import compactor_writer_open_count

_log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader


class L3Compactor:
    def __init__(self, root: Path, *, nas_uploader: NASUploader | None = None) -> None:
        self._root = root
        self._nas_uploader = nas_uploader  # MCT-169: NAS GET source (D3=C, INV-3)

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
        # MCT-169 INV-3: NAS GET source (nas_uploader inject 시) or local fallback
        if self._nas_uploader is not None:
            return self._compact_day_nas(
                exchange=exchange, symbol=symbol, channel=channel,
                date_str=date_str, schema_ver=schema_ver,
            )

        # Local fallback (backward compat — nas_uploader=None)
        # Read individual files to avoid Hive auto-discovery conflict
        # ADR-017 Amendment 3: L3 도 content-derived sort key (defensive — uniform API)
        if l2_dir.exists():
            candidates = list(l2_dir.rglob("part-*.parquet"))
            keyed = []
            for p in candidates:
                ts = _extract_min_ts(p)
                if ts is None:
                    _log.warning("[L3Compactor] skip 0-row L2 file: %s", p)
                    continue
                keyed.append((p, ts))
            l2_files = [p for p, _ts in sorted(keyed, key=lambda x: x[1])]
        else:
            l2_files = []
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

    def _compact_day_nas(
        self,
        *,
        exchange: str,
        symbol: str,
        channel: str,
        date_str: str,
        schema_ver: str,
    ) -> Path | None:
        """MCT-169 D3=C INV-3: NAS GET source path (nas_uploader inject 시).

        NAS key prefix: single SSOT helper (ADR-034 §결정 2, U2-HELPER SSOT-6).
        NASUploader._list_objects(prefix) → NAS key list → get_streaming().
        pq.ParquetFile(BytesIO stream) 로 읽기 (local Path open 0, INV-3).

        ADR-017 Amendment 3 (PR #96): NAS GET path 도 content-derived sort key (L2 동형).
        """
        from mctrader_data.nas_storage.get_streaming import get_streaming
        from mctrader_data.nas_storage.nas_key import build_nas_prefix
        from mctrader_data.nas_metrics.prometheus_exporters import nas_key_helper_call_total

        nas_prefix = build_nas_prefix(
            tier="L2", channel=channel, schema_ver=schema_ver,
            exchange=exchange, symbol=symbol, date_str=date_str,
        )
        nas_key_helper_call_total.labels(caller="l3_compactor_get_source", tier="L2").inc()

        # ADR-017 Amendment 3: NAS GET path 도 content-derived sort key (L2 동형)
        try:
            candidate_keys = [
                k for k in self._nas_uploader._list_objects(nas_prefix)  # type: ignore[union-attr]
                if k.endswith(".parquet")
            ]
        except Exception:
            _log.warning(
                "[L3Compactor] NAS _list_objects failed prefix=%s — skip (INV-3)",
                nas_prefix,
            )
            return None

        if not candidate_keys:
            return None

        # content-derived sort: get_streaming → _extract_min_ts stats.min
        keyed: list[tuple[str, datetime]] = []
        for k in candidate_keys:
            stream = get_streaming(nas_uploader=self._nas_uploader, nas_key=k)  # type: ignore[arg-type]
            ts = _extract_min_ts(stream)
            if ts is None:
                _log.warning("[L3Compactor] skip 0-row NAS L2 key: %s", k)
                continue
            keyed.append((k, ts))

        nas_keys = [k for k, _ts in sorted(keyed, key=lambda x: x[1])]
        if not nas_keys:
            return None

        # Pre-read first object for schema
        first_stream = get_streaming(nas_uploader=self._nas_uploader, nas_key=nas_keys[0])  # type: ignore[arg-type]
        first_pf = pq.ParquetFile(first_stream)
        schema = first_pf.schema_arrow

        run_id = hashlib.sha256("|".join(nas_keys).encode()).hexdigest()[:16]

        out_dir = (
            self._root / "market" / channel
            / f"schema_version={schema_ver}" / "tier=L3"
            / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
            / "node=MERGED"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"part-{run_id}.parquet"
        tmp = out_dir / f"part-tmp-{os.getpid()}.tmp"

        last_ts = None
        monotonic_violation = False
        try:
            compactor_writer_open_count.labels(tier="L3").inc()
            try:
                with pq.ParquetWriter(str(tmp), schema, compression="snappy") as writer:
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
                compactor_writer_open_count.labels(tier="L3").dec()

            if monotonic_violation:
                from mctrader_data.compactor.quarantine import quarantine_l3
                from mctrader_data.nas_metrics.prometheus_exporters import compactor_quarantine_total
                from datetime import date as date_type
                date_utc = date_type.fromisoformat(date_str)
                quarantine_l3(tmp, channel=channel, date_utc=date_utc, reason="monotonic_violation")
                compactor_quarantine_total.labels(tier="L3", reason="monotonic_violation").inc()
                return None

            os.replace(str(tmp), str(out_path))
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(str(tmp))
            raise
        return out_path
