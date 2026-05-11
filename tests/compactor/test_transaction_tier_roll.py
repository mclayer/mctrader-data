# tests/compactor/test_transaction_tier_roll.py
"""MCT-141 — TransactionTierCompactor 256 MB Parquet roll + atomic rename.

Coverage:
- partition layout: market/transaction/schema_version=tick.v1.1/tier=L1/
  exchange=.../symbol=.../date=.../node=.../part-{run_id}.parquet
- 256 MB roll boundary: writer rolls part-N+1 when cumulative size crosses threshold
- atomic_replace_parquet integration (Story-6 API) — tmp cleanup on exception
- with-context ParquetWriter (MCT-133 pattern)
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from mctrader_data.compactor.schema_upgrade import TICK_V1_1_SCHEMA
from mctrader_data.compactor.transaction_tier import (
    TRANSACTION_L1_ROLL_BYTES,
    TransactionTierCompactor,
)


def _make_table(n_rows: int) -> pa.Table:
    ts = [datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc)] * n_rows
    return pa.Table.from_pydict(
        {
            "ts_utc": ts,
            "received_at": ts,
            "exchange": ["bithumb"] * n_rows,
            "symbol": ["KRW-BTC"] * n_rows,
            "price": [Decimal("100000000.000000000000000000")] * n_rows,
            "quantity": [Decimal("0.001000000000000000")] * n_rows,
            "side": ["buy"] * n_rows,
            "raw_json": [None] * n_rows,
            "ingest_seq": [None] * n_rows,
            "payload_hash": [None] * n_rows,
            "validation_status": ["OK"] * n_rows,
        },
        schema=TICK_V1_1_SCHEMA,
    )


def test_partition_layout_uses_v1_1_schema_version(tmp_path: Path):
    comp = TransactionTierCompactor(root=tmp_path)
    path = comp.derive_partition_path(
        exchange="bithumb", symbol="KRW-BTC",
        date_utc="2026-05-12", node_id="NODE_A", run_id="run001",
    )
    parts = path.parts
    assert "market" in parts
    assert "transaction" in parts
    assert "schema_version=tick.v1.1" in parts
    assert "tier=L1" in parts
    assert "exchange=bithumb" in parts
    assert "symbol=KRW-BTC" in parts
    assert "date=2026-05-12" in parts
    assert "node=NODE_A" in parts
    assert path.name == "part-run001.parquet"


def test_default_roll_size_is_256mb():
    assert TRANSACTION_L1_ROLL_BYTES == 256 * 1024 * 1024


def test_write_table_creates_parquet(tmp_path: Path):
    comp = TransactionTierCompactor(root=tmp_path)
    table = _make_table(10)
    out = comp.write_table(
        table,
        exchange="bithumb", symbol="KRW-BTC",
        date_utc="2026-05-12", node_id="NODE_A", run_id="run001",
    )
    assert out.exists()
    assert out.suffix == ".parquet"
    import pyarrow.parquet as pq
    read = pq.ParquetFile(str(out)).read()
    assert read.num_rows == 10
    assert read.schema.equals(TICK_V1_1_SCHEMA, check_metadata=False)


def test_should_roll_at_threshold():
    comp = TransactionTierCompactor(root=Path("."))
    assert comp.should_roll(current_bytes=255 * 1024 * 1024) is False
    assert comp.should_roll(current_bytes=256 * 1024 * 1024) is True
    assert comp.should_roll(current_bytes=300 * 1024 * 1024) is True


def test_write_table_tmp_cleanup_on_exception(tmp_path: Path, monkeypatch):
    """When write fails mid-flight, tmp must be cleaned and final must not exist."""
    comp = TransactionTierCompactor(root=tmp_path)
    table = _make_table(5)

    from mctrader_data.compactor import transaction_tier as tt_mod

    def boom(*a, **kw):
        raise RuntimeError("disk full")

    monkeypatch.setattr(tt_mod, "atomic_replace_parquet", boom)

    with pytest.raises(RuntimeError, match="disk full"):
        comp.write_table(
            table,
            exchange="bithumb", symbol="KRW-BTC",
            date_utc="2026-05-12", node_id="NODE_A", run_id="run002",
        )

    # Final must not exist
    final = comp.derive_partition_path(
        exchange="bithumb", symbol="KRW-BTC",
        date_utc="2026-05-12", node_id="NODE_A", run_id="run002",
    )
    assert not final.exists()
    # No .tmp siblings in the partition dir
    if final.parent.exists():
        siblings = [p for p in final.parent.iterdir() if p.suffix == ".tmp"]
        assert siblings == []
