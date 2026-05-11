# tests/compactor/test_l1_writer_close.py
"""Verify L1Compactor closes ParquetWriter and cleans tmp on exception path.

MCT-133 A1 Task 5: prevent ParquetWriter handle leak when write_table raises.

The pre-fix code path in L1Compactor._write_parquet_atomic was:

    writer = pq.ParquetWriter(...)
    writer.write_table(table)   # <-- if this raises, writer.close() never runs
    writer.close()

This test exercises that exception path with a mock writer that raises on
write_table, and asserts that:
  1. The writer is closed (via context manager __exit__ or explicit close).
  2. The .parquet.tmp file is cleaned up (no leftover tmp).
  3. The exception propagates.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mctrader_data.compactor.l1 import L1Compactor
from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed


def _make_tick(ts_offset: int = 0) -> dict:
    ts = datetime(2026, 5, 11, 0, 0, ts_offset, tzinfo=timezone.utc)
    return {
        "ts_utc": ts.isoformat(),
        "received_at": ts.isoformat(),
        "exchange": "bithumb",
        "symbol": "KRW-BTC",
        "price": Decimal("100000"),
        "quantity": Decimal("0.01"),
        "side": "buy",
        "raw_json": None,
        "channel": "transaction",
    }


def _write_sealed(tmp_path: Path) -> Path:
    ing = WalIngester(
        root=tmp_path,
        exchange="bithumb",
        symbol="KRW-BTC",
        channel="transaction",
        node_id="NODE_A",
        segment_seconds=86400,
    )
    ing.append(_make_tick(0))
    ing.close()
    sealed = scan_sealed(tmp_path)
    assert len(sealed) == 1
    return sealed[0]


def test_l1_closes_writer_on_exception(tmp_path: Path) -> None:
    """If writer.write_table raises, the writer must still be closed (__exit__ or close)."""
    sealed = _write_sealed(tmp_path)
    compactor = L1Compactor(root=tmp_path)

    fake_writer = MagicMock(name="ParquetWriter")
    # Make the mock usable as a context manager *and* track close().
    fake_writer.__enter__ = MagicMock(return_value=fake_writer)
    fake_writer.__exit__ = MagicMock(return_value=False)
    fake_writer.write_table.side_effect = RuntimeError("boom")

    with patch(
        "mctrader_data.compactor.l1.pq.ParquetWriter", return_value=fake_writer
    ) as ctor:
        with pytest.raises(RuntimeError, match="boom"):
            compactor.compact_segment(sealed)
        assert ctor.called, "ParquetWriter should have been constructed"

    # Either __exit__ (with-pattern) or close() (try/finally pattern) is acceptable.
    closed_via_context = fake_writer.__exit__.called
    closed_via_explicit = fake_writer.close.called
    assert closed_via_context or closed_via_explicit, (
        "ParquetWriter was not closed on exception — handle leak. "
        f"__exit__.called={closed_via_context}, close.called={closed_via_explicit}"
    )


def test_l1_cleans_tmp_on_exception(tmp_path: Path) -> None:
    """If write_table raises, no leftover .parquet.tmp files remain in target dir."""
    sealed = _write_sealed(tmp_path)
    compactor = L1Compactor(root=tmp_path)

    fake_writer = MagicMock(name="ParquetWriter")
    fake_writer.__enter__ = MagicMock(return_value=fake_writer)
    fake_writer.__exit__ = MagicMock(return_value=False)
    fake_writer.write_table.side_effect = RuntimeError("boom")

    with patch(
        "mctrader_data.compactor.l1.pq.ParquetWriter", return_value=fake_writer
    ):
        with pytest.raises(RuntimeError, match="boom"):
            compactor.compact_segment(sealed)

    # Find any leftover .parquet.tmp anywhere under market/ tree
    market_root = tmp_path / "market"
    leftover = list(market_root.rglob("*.parquet.tmp*")) if market_root.exists() else []
    assert not leftover, f"tmp files not cleaned up: {leftover}"
