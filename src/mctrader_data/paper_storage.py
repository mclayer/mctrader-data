"""Paper-mode write API (MCT-20). Separate from canonical historical writers."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

from mctrader_market.candle import CandleLike

from mctrader_data.path import derive_partition_path
from mctrader_data.paper_lineage import PaperLineage
from mctrader_data.storage import _candles_to_arrow

import pyarrow.parquet as pq


def write_paper_candles(
    candles: Sequence[CandleLike],
    *,
    root: Path,
    run_id: str,
    snapshot_id: str,
    lineage: PaperLineage,
) -> Path:
    """Append a closed-bar batch under ``schema_version=ohlcv.v1/mode=paper/...``.

    ADR-009 v1 16-column schema is preserved (paper provenance lives in path + lineage
    sidecars, not in row schema). Each call writes one Parquet file plus a JSON sidecar
    ``_paper_lineage_{snapshot_id}.json`` per snapshot.
    """
    if not candles:
        raise ValueError("write_paper_candles: empty candles batch")
    if lineage.run_id != run_id:
        raise ValueError(
            f"PaperLineage.run_id={lineage.run_id!r} mismatches argument run_id={run_id!r}"
        )
    if lineage.snapshot_id != snapshot_id:
        raise ValueError(
            f"PaperLineage.snapshot_id={lineage.snapshot_id!r} "
            f"mismatches argument snapshot_id={snapshot_id!r}"
        )

    head = candles[0]
    partition = derive_partition_path(
        root=root,
        exchange=head.exchange,
        symbol=head.symbol,
        timeframe=head.timeframe,
        ts_utc=head.ts_utc,
        mode="paper",
    )
    partition.mkdir(parents=True, exist_ok=True)
    table = _candles_to_arrow(candles)
    parquet_target = partition / f"part-{snapshot_id}.parquet"
    pq.write_table(table, parquet_target, compression="snappy")

    sidecar_target = partition / f"_paper_lineage_{snapshot_id}.json"
    with sidecar_target.open("w", encoding="utf-8") as f:
        json.dump(lineage.model_dump(mode="json"), f, ensure_ascii=False, indent=2)

    return partition
