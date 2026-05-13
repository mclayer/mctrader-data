# tests/compactor/test_l2_writer_close.py
"""Verify L2Compactor closes ParquetWriter and cleans tmp on exception path.

MCT-133 A1 Task 6a: same pattern as L1 (test_l1_writer_close.py) — prevent
ParquetWriter handle leak when write_table raises during L1 → L2 merge.

The pre-fix code path in L2Compactor.compact_hour used pq.write_table() which
on exception leaves the tmp file behind and provides no explicit close hook.
Post-fix: use pq.ParquetWriter as a context manager and clean tmp on exception.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mctrader_data.compactor.l1 import L1Compactor
from mctrader_data.compactor.l2 import L2Compactor
from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed


def _seed_l1(tmp_path: Path, n: int = 5) -> None:
    ing = WalIngester(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        channel="transaction", node_id="N", segment_seconds=86400,
    )
    for i in range(n):
        ts = datetime(2026, 5, 11, 0, 0, i, tzinfo=timezone.utc)
        ing.append({
            "ts_utc": ts.isoformat(), "received_at": ts.isoformat(),
            "exchange": "bithumb", "symbol": "KRW-BTC",
            "price": Decimal("100000"), "quantity": Decimal("0.01"),
            "side": "buy", "raw_json": None, "channel": "transaction",
        })
    ing.close()
    for s in scan_sealed(tmp_path):
        L1Compactor(root=tmp_path).compact_segment(s)


def test_l2_closes_writer_on_exception(tmp_path: Path) -> None:
    """If writer.write_table raises, the writer must still be closed (__exit__ or close)."""
    _seed_l1(tmp_path)
    compactor = L2Compactor(root=tmp_path)

    fake_writer = MagicMock(name="ParquetWriter")
    fake_writer.__enter__ = MagicMock(return_value=fake_writer)
    fake_writer.__exit__ = MagicMock(return_value=False)
    fake_writer.write_table.side_effect = RuntimeError("boom")

    with patch(
        "mctrader_data.compactor.l2.pq.ParquetWriter", return_value=fake_writer
    ) as ctor:
        with pytest.raises(RuntimeError, match="boom"):
            compactor.compact_hour(
                exchange="bithumb", symbol="KRW-BTC", channel="transaction",
                hour_utc=datetime(2026, 5, 11, 0, 0, tzinfo=timezone.utc),
            )
        assert ctor.called, "ParquetWriter should have been constructed"

    closed_via_context = fake_writer.__exit__.called
    closed_via_explicit = fake_writer.close.called
    assert closed_via_context or closed_via_explicit, (
        "ParquetWriter was not closed on exception — handle leak. "
        f"__exit__.called={closed_via_context}, close.called={closed_via_explicit}"
    )


def test_l2_cleans_tmp_on_exception(tmp_path: Path) -> None:
    """If write_table raises, no leftover .tmp files remain in L2 target dir."""
    _seed_l1(tmp_path)
    compactor = L2Compactor(root=tmp_path)

    fake_writer = MagicMock(name="ParquetWriter")
    fake_writer.__enter__ = MagicMock(return_value=fake_writer)
    fake_writer.__exit__ = MagicMock(return_value=False)
    fake_writer.write_table.side_effect = RuntimeError("boom")

    with patch(
        "mctrader_data.compactor.l2.pq.ParquetWriter", return_value=fake_writer
    ), pytest.raises(RuntimeError, match="boom"):
        compactor.compact_hour(
            exchange="bithumb", symbol="KRW-BTC", channel="transaction",
            hour_utc=datetime(2026, 5, 11, 0, 0, tzinfo=timezone.utc),
        )

    market_root = tmp_path / "market"
    leftover = (
        list(market_root.rglob("*.tmp")) + list(market_root.rglob("*.parquet.tmp*"))
        if market_root.exists() else []
    )
    # Filter to L2 tier only
    leftover = [p for p in leftover if "tier=L2" in str(p)]
    assert not leftover, f"L2 tmp files not cleaned up: {leftover}"
