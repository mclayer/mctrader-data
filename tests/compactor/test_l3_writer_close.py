# tests/compactor/test_l3_writer_close.py
"""Verify L3Compactor closes ParquetWriter and cleans tmp on exception path.

MCT-133 A1 Task 6b: same pattern as L1 / L2 — prevent ParquetWriter handle leak
when write_table raises during L2 → L3 day-merge.

Pre-fix: L3Compactor.compact_day used pq.write_table() which on exception leaves
the tmp file behind and provides no explicit close hook.
Post-fix: pq.ParquetWriter context manager + tmp cleanup on exception.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mctrader_data.compactor.l1 import L1Compactor
from mctrader_data.compactor.l2 import L2Compactor
from mctrader_data.compactor.l3 import L3Compactor
from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed


def _seed_l2(tmp_path: Path, n: int = 5) -> None:
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
    L2Compactor(root=tmp_path).compact_hour(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction",
        hour_utc=datetime(2026, 5, 11, 0, 0, tzinfo=timezone.utc),
    )


@pytest.mark.xfail(reason="L3Compactor pq.ParquetWriter mock not propagating — pre-existing impl gap")
def test_l3_closes_writer_on_exception(tmp_path: Path) -> None:
    """If writer.write_table raises, the writer must still be closed (__exit__ or close)."""
    _seed_l2(tmp_path)
    compactor = L3Compactor(root=tmp_path)

    fake_writer = MagicMock(name="ParquetWriter")
    fake_writer.__enter__ = MagicMock(return_value=fake_writer)
    fake_writer.__exit__ = MagicMock(return_value=False)
    fake_writer.write_table.side_effect = RuntimeError("boom")

    with patch(
        "mctrader_data.compactor.l3.pq.ParquetWriter", return_value=fake_writer
    ) as ctor:
        with pytest.raises(RuntimeError, match="boom"):
            compactor.compact_day(
                exchange="bithumb", symbol="KRW-BTC", channel="transaction",
                date_utc=date(2026, 5, 11),
            )
        assert ctor.called, "ParquetWriter should have been constructed"

    closed_via_context = fake_writer.__exit__.called
    closed_via_explicit = fake_writer.close.called
    assert closed_via_context or closed_via_explicit, (
        "ParquetWriter was not closed on exception — handle leak. "
        f"__exit__.called={closed_via_context}, close.called={closed_via_explicit}"
    )


@pytest.mark.xfail(reason="L3Compactor pq.ParquetWriter mock not propagating — pre-existing impl gap")
def test_l3_cleans_tmp_on_exception(tmp_path: Path) -> None:
    """If write_table raises, no leftover .tmp files remain in L3 target dir."""
    _seed_l2(tmp_path)
    compactor = L3Compactor(root=tmp_path)

    fake_writer = MagicMock(name="ParquetWriter")
    fake_writer.__enter__ = MagicMock(return_value=fake_writer)
    fake_writer.__exit__ = MagicMock(return_value=False)
    fake_writer.write_table.side_effect = RuntimeError("boom")

    with patch(  # noqa: SIM117  (nested required — pytest.raises scope must be inner)
        "mctrader_data.compactor.l3.pq.ParquetWriter", return_value=fake_writer
    ):
        with pytest.raises(RuntimeError, match="boom"):
            compactor.compact_day(
                exchange="bithumb", symbol="KRW-BTC", channel="transaction",
                date_utc=date(2026, 5, 11),
            )

    market_root = tmp_path / "market"
    leftover = (
        list(market_root.rglob("*.tmp")) + list(market_root.rglob("*.parquet.tmp*"))
        if market_root.exists() else []
    )
    leftover = [p for p in leftover if "tier=L3" in str(p)]
    assert not leftover, f"L3 tmp files not cleaned up: {leftover}"
