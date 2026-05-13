# tests/compactor/test_transaction_tier_streaming.py
"""MCT-141 — TransactionTierCompactor streaming row groups for 4-8 GB process limit.

MCT-132 mem_limit pattern답습:
- write_table_streaming(rows_iter, ...) writes via row-group batches
  so peak RSS is bounded by the row-group size, not the full table.
- row_group_size default = 50_000 (~ tens of MB for tick.v1.1 → keeps RSS << 8 GB).
- output file is readable as a single Parquet with N rows.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.schema_upgrade import TICK_V1_1_SCHEMA
from mctrader_data.compactor.transaction_tier import (
    TRANSACTION_L1_ROW_GROUP_SIZE,
    TransactionTierCompactor,
)


def _row_batch(start: int, n: int) -> pa.RecordBatch:
    ts = [datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc)] * n
    return pa.RecordBatch.from_pydict(
        {
            "ts_utc": ts,
            "received_at": ts,
            "exchange": ["bithumb"] * n,
            "symbol": ["KRW-BTC"] * n,
            "price": [Decimal("100000000")] * n,
            "quantity": [Decimal("0.001")] * n,
            "side": ["buy"] * n,
            "raw_json": [None] * n,
            "ingest_seq": [None] * n,
            "payload_hash": [None] * n,
            "validation_status": ["OK"] * n,
        },
        schema=TICK_V1_1_SCHEMA,
    )


def test_row_group_size_default():
    assert TRANSACTION_L1_ROW_GROUP_SIZE >= 1000
    # Sanity: not absurdly large (>1 M)
    assert TRANSACTION_L1_ROW_GROUP_SIZE <= 1_000_000


def test_write_table_streaming_writes_all_batches(tmp_path: Path):
    comp = TransactionTierCompactor(root=tmp_path)

    def batches() -> Iterator[pa.RecordBatch]:
        for i in range(3):
            yield _row_batch(i * 100, 100)

    out = comp.write_table_streaming(
        batches(),
        exchange="bithumb", symbol="KRW-BTC",
        date_utc="2026-05-12", node_id="NODE_A", run_id="stream1",
    )
    assert out.exists()
    pf = pq.ParquetFile(str(out))
    assert pf.metadata.num_rows == 300
    # multi-row-group expected when batch > row_group_size; for small test we
    # accept any positive count (writer may compact small batches).
    assert pf.metadata.num_row_groups >= 1


def test_streaming_cleanup_on_exception(tmp_path: Path):
    comp = TransactionTierCompactor(root=tmp_path)

    def batches() -> Iterator[pa.RecordBatch]:
        yield _row_batch(0, 10)
        raise RuntimeError("upstream feed broken")

    import pytest as _pytest
    with _pytest.raises(RuntimeError, match="upstream feed broken"):
        comp.write_table_streaming(
            batches(),
            exchange="bithumb", symbol="KRW-BTC",
            date_utc="2026-05-12", node_id="NODE_A", run_id="stream_fail",
        )
    # No partial parquet
    final = comp.derive_partition_path(
        exchange="bithumb", symbol="KRW-BTC",
        date_utc="2026-05-12", node_id="NODE_A", run_id="stream_fail",
    )
    assert not final.exists()
    if final.parent.exists():
        tmps = [p for p in final.parent.iterdir() if ".tmp" in p.name]
        assert tmps == [], f"tmp leftovers: {tmps}"
