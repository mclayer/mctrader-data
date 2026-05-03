"""paper_storage + scan mode filter tests (MCT-20 Phase 2)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mctrader_market.candle import CandleModel
from mctrader_market.types import Symbol, Timeframe

from mctrader_data.paper_lineage import PaperLineage, canonical_jsonl_hash
from mctrader_data.paper_storage import write_paper_candles
from mctrader_data.storage import scan_candles, write_candles


def _make_candle(ts: datetime, close: Decimal) -> CandleModel:
    return CandleModel(
        ts_utc=ts,
        exchange="bithumb",
        symbol=Symbol(base="BTC", quote="KRW"),
        timeframe=Timeframe.H1,
        open=close,
        high=close + Decimal("100000"),
        low=close - Decimal("100000"),
        close=close,
        volume=Decimal("1.0"),
        value=None,
    )


def _make_lineage(run_id: str, snapshot_id: str, fetched_at: datetime) -> PaperLineage:
    return PaperLineage(
        snapshot_id=snapshot_id,
        run_id=run_id,
        exchange="bithumb",
        endpoint="wss://pubwss.bithumb.com/pub/ws",
        request_params_hash="subhash-1",
        fetched_at_utc=fetched_at,
        response_hash=canonical_jsonl_hash([{"a": 1}, {"b": 2}]),
        adapter_name="mctrader-market-bithumb-ws",
        adapter_version="0.2.0",
    )


def test_write_paper_candles_creates_mode_paper_partition(tmp_path: Path) -> None:
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    candles = [_make_candle(base_ts + timedelta(hours=i), Decimal("100000000")) for i in range(3)]
    lineage = _make_lineage(run_id="run-A", snapshot_id="snap-1", fetched_at=base_ts)

    partition = write_paper_candles(
        candles, root=tmp_path, run_id="run-A", snapshot_id="snap-1", lineage=lineage
    )

    assert "mode=paper" in partition.as_posix()
    assert "schema_version=ohlcv.v1" in partition.as_posix()
    assert (partition / "part-snap-1.parquet").exists()
    assert (partition / "_paper_lineage_snap-1.json").exists()


def test_write_paper_candles_lineage_run_id_mismatch_raises(tmp_path: Path) -> None:
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    candles = [_make_candle(base_ts, Decimal("100000000"))]
    lineage = _make_lineage(run_id="run-A", snapshot_id="snap-1", fetched_at=base_ts)
    with pytest.raises(ValueError, match="run_id"):
        write_paper_candles(
            candles, root=tmp_path, run_id="run-B", snapshot_id="snap-1", lineage=lineage
        )


def test_scan_default_historical_excludes_paper(tmp_path: Path) -> None:
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    historical = [_make_candle(base_ts + timedelta(hours=i), Decimal("100000000")) for i in range(3)]
    paper = [_make_candle(base_ts + timedelta(hours=10 + i), Decimal("110000000")) for i in range(3)]

    write_candles(historical, root=tmp_path, snapshot_id="hist-1")  # legacy no-mode
    lineage = _make_lineage(run_id="run-A", snapshot_id="paper-1", fetched_at=base_ts)
    write_paper_candles(paper, root=tmp_path, run_id="run-A", snapshot_id="paper-1", lineage=lineage)

    historical_only = list(
        scan_candles(
            exchange="bithumb",
            symbol=Symbol(base="BTC", quote="KRW"),
            timeframe=Timeframe.H1,
            start=base_ts,
            end=base_ts + timedelta(hours=20),
            root=tmp_path,
        )
    )
    assert len(historical_only) == 3
    assert all(c.close == Decimal("100000000") for c in historical_only)


def test_scan_mode_paper_returns_only_paper(tmp_path: Path) -> None:
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    historical = [_make_candle(base_ts + timedelta(hours=i), Decimal("100000000")) for i in range(3)]
    paper = [_make_candle(base_ts + timedelta(hours=10 + i), Decimal("110000000")) for i in range(3)]

    write_candles(historical, root=tmp_path, snapshot_id="hist-1")
    lineage = _make_lineage(run_id="run-A", snapshot_id="paper-1", fetched_at=base_ts)
    write_paper_candles(paper, root=tmp_path, run_id="run-A", snapshot_id="paper-1", lineage=lineage)

    paper_only = list(
        scan_candles(
            exchange="bithumb",
            symbol=Symbol(base="BTC", quote="KRW"),
            timeframe=Timeframe.H1,
            start=base_ts,
            end=base_ts + timedelta(hours=20),
            root=tmp_path,
            mode="paper",
        )
    )
    assert len(paper_only) == 3
    assert all(c.close == Decimal("110000000") for c in paper_only)


def test_scan_both_modes_returns_union(tmp_path: Path) -> None:
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    historical = [_make_candle(base_ts + timedelta(hours=i), Decimal("100000000")) for i in range(2)]
    paper = [_make_candle(base_ts + timedelta(hours=10 + i), Decimal("110000000")) for i in range(2)]

    write_candles(historical, root=tmp_path, snapshot_id="hist-1")
    lineage = _make_lineage(run_id="run-A", snapshot_id="paper-1", fetched_at=base_ts)
    write_paper_candles(paper, root=tmp_path, run_id="run-A", snapshot_id="paper-1", lineage=lineage)

    union = list(
        scan_candles(
            exchange="bithumb",
            symbol=Symbol(base="BTC", quote="KRW"),
            timeframe=Timeframe.H1,
            start=base_ts,
            end=base_ts + timedelta(hours=20),
            root=tmp_path,
            mode=["historical", "paper"],
        )
    )
    assert len(union) == 4


def test_canonical_jsonl_hash_deterministic() -> None:
    a = canonical_jsonl_hash([{"x": 1, "y": 2}, {"a": 3}])
    b = canonical_jsonl_hash([{"y": 2, "x": 1}, {"a": 3}])  # different key order, same content
    assert a == b


def test_canonical_jsonl_hash_order_preserving() -> None:
    a = canonical_jsonl_hash([{"x": 1}, {"y": 2}])
    b = canonical_jsonl_hash([{"y": 2}, {"x": 1}])  # reversed message order
    assert a != b
