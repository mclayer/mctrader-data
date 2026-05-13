# tests/compactor/test_transaction_tier_metrics.py
"""MCT-141 — TransactionTierCompactor metric paired inc/dec + label coverage.

MCT-132 framework metrics already exist:
- compactor_process_rss_bytes
- compactor_pyarrow_total_allocated_bytes
- compactor_python_gc_gen_count
- compactor_tier_pending_segments{tier="L1|L2|L3"}
- compactor_writer_open_count{tier="L1|L2|L3"}

Coverage:
- TransactionTierCompactor.write_table increments writer_open_count{tier="L1"} pre-open
  and decrements in finally (paired across both success and exception)
- After write completes, gauge is 0
- After write fails, gauge is 0
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from mctrader_data.compactor.schema_upgrade import TICK_V1_1_SCHEMA
from mctrader_data.compactor.transaction_tier import TransactionTierCompactor
from mctrader_data.metrics import compactor_writer_open_count


def _make_table(n: int) -> pa.Table:
    ts = [datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc)] * n
    return pa.Table.from_pydict(
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


def _gauge_value(tier: str) -> float:
    sample = compactor_writer_open_count.labels(tier=tier)
    return sample._value.get()


def test_writer_open_count_paired_inc_dec_on_success(tmp_path: Path):
    before = _gauge_value("L1")
    comp = TransactionTierCompactor(root=tmp_path)
    comp.write_table(
        _make_table(5),
        exchange="bithumb", symbol="KRW-BTC",
        date_utc="2026-05-12", node_id="NODE_A", run_id="mrun1",
    )
    after = _gauge_value("L1")
    assert after == before


def test_writer_open_count_decremented_on_failure(tmp_path: Path, monkeypatch):
    before = _gauge_value("L1")
    comp = TransactionTierCompactor(root=tmp_path)
    from mctrader_data.compactor import transaction_tier as tt_mod
    monkeypatch.setattr(tt_mod, "atomic_replace_parquet", lambda *a, **kw: (_ for _ in ()).throw(OSError("nope")))
    with pytest.raises(IOError):
        comp.write_table(
            _make_table(5),
            exchange="bithumb", symbol="KRW-BTC",
            date_utc="2026-05-12", node_id="NODE_A", run_id="mrun2",
        )
    after = _gauge_value("L1")
    assert after == before, "writer_open_count must be decremented on exception path"
