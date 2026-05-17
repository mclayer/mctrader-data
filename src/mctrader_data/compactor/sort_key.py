"""Content-derived sort key for L2/L3 compactor (ADR-017 Amendment 3).

Primary: pq.read_metadata(path).row_group(N).column(ts_utc_idx).statistics.min
  (multi-row-group 시 file-level min 명시 집계)
Fallback: stats 부재/null 시 iter_batches(batch_size=1) first-row
  (L1 intra-file mono 보장 활용 — l1.py compact_segment step 5 sort_by('ts_utc'))
Edge: 0-row file → None (caller skip + warning emit)

파일명은 untrusted — sorted(rglob(...)) byte-order 또는 mtime 금지.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import IO, Union

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

PathOrStream = Union[Path, str, IO[bytes]]  # BytesIO acceptable for NAS GET


def _extract_min_ts(path_or_stream: PathOrStream) -> datetime | None:
    """Return file-level minimum ts_utc, or None for 0-row file.

    Primary: row-group statistics.min 집계 (read I/O ≈ 0, metadata footer만).
    Fallback: stats 부재 시 iter_batches(batch_size=1) first-row.

    Raises:
        KeyError: ts_utc 컬럼 부재 (schema 위반)
    """
    # Read metadata once at entry
    if isinstance(path_or_stream, (str, Path)):
        meta = pq.read_metadata(path_or_stream)
    else:
        meta = pq.ParquetFile(path_or_stream).metadata

    # 0-row guard — explicit contract, PyArrow-version-independent
    if meta.num_rows == 0:
        return None

    schema = meta.schema.to_arrow_schema()
    ts_idx = schema.get_field_index("ts_utc")
    if ts_idx < 0:
        raise KeyError("ts_utc column not found in parquet schema")

    # Primary — try metadata stats (metadata footer only, no data I/O)
    mins = []
    for rg_idx in range(meta.num_row_groups):
        col_meta = meta.row_group(rg_idx).column(ts_idx)
        stats = col_meta.statistics
        if stats is None or not stats.has_min_max:
            mins = []  # stats 부재 — fallback 으로
            break
        mins.append(stats.min)

    if mins:
        return min(mins)

    # Fallback — first row via iter_batches (num_rows >= 1 guaranteed above)
    pf = pq.ParquetFile(path_or_stream)
    first_batch = next(pf.iter_batches(batch_size=1))
    return first_batch.column("ts_utc")[0].as_py()
