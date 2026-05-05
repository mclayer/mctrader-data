"""DuckDB/Parquet roundtrip tests (cross-platform Linux + Windows lane critical)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mctrader_market.candle import CandleModel
from mctrader_market.types import Symbol, Timeframe

from mctrader_data.storage import scan_candles, write_candles


def _make_candle(ts_utc: datetime, close: Decimal) -> CandleModel:
    return CandleModel(
        ts_utc=ts_utc,
        exchange="bithumb",
        symbol=Symbol(base="BTC", quote="KRW"),
        timeframe=Timeframe.H1,
        open=close,
        high=close + Decimal("100000"),
        low=close - Decimal("100000"),
        close=close,
        volume=Decimal("1.5"),
        value=None,
    )


def test_write_and_scan_roundtrip(tmp_path: Path) -> None:
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    candles = [
        _make_candle(base_ts + timedelta(hours=i), Decimal("100000000") + Decimal(i * 1000))
        for i in range(5)
    ]
    write_candles(candles, root=tmp_path, snapshot_id="snap-test-1")

    result = list(
        scan_candles(
            exchange="bithumb",
            symbol=Symbol(base="BTC", quote="KRW"),
            timeframe=Timeframe.H1,
            start=base_ts,
            end=base_ts + timedelta(hours=10),
            root=tmp_path,
        )
    )

    assert len(result) == 5
    for original, restored in zip(candles, result, strict=True):
        assert restored.ts_utc == original.ts_utc
        assert restored.symbol == original.symbol
        assert restored.timeframe == original.timeframe
        assert restored.close == original.close
        assert restored.volume == original.volume


def test_scan_half_open_interval(tmp_path: Path) -> None:
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    candles = [
        _make_candle(base_ts + timedelta(hours=i), Decimal("100000000") + Decimal(i * 1000))
        for i in range(5)
    ]
    write_candles(candles, root=tmp_path, snapshot_id="snap-test-2")

    result = list(
        scan_candles(
            exchange="bithumb",
            symbol=Symbol(base="BTC", quote="KRW"),
            timeframe=Timeframe.H1,
            start=base_ts + timedelta(hours=1),
            end=base_ts + timedelta(hours=4),
            root=tmp_path,
        )
    )

    assert len(result) == 3
    expected_ts = [base_ts + timedelta(hours=i) for i in (1, 2, 3)]
    actual_ts = [c.ts_utc for c in result]
    assert actual_ts == expected_ts


def test_decimal_precision_preserved_through_roundtrip(tmp_path: Path) -> None:
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    precise = Decimal("100000000.123456789012345678")
    candles = [_make_candle(base_ts, precise)]
    write_candles(candles, root=tmp_path, snapshot_id="snap-test-3")

    result = list(
        scan_candles(
            exchange="bithumb",
            symbol=Symbol(base="BTC", quote="KRW"),
            timeframe=Timeframe.H1,
            start=base_ts,
            end=base_ts + timedelta(hours=1),
            root=tmp_path,
        )
    )

    assert len(result) == 1
    assert result[0].close == precise


def test_empty_scan_returns_empty(tmp_path: Path) -> None:
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    candles = [_make_candle(base_ts, Decimal("100000000"))]
    write_candles(candles, root=tmp_path, snapshot_id="snap-test-4")

    result = list(
        scan_candles(
            exchange="bithumb",
            symbol=Symbol(base="BTC", quote="KRW"),
            timeframe=Timeframe.H1,
            start=base_ts + timedelta(days=10),
            end=base_ts + timedelta(days=11),
            root=tmp_path,
        )
    )

    assert result == []


# MCT-91 — HA writer (node= partition + new file naming + parquet metadata)
def test_write_candles_node_id_partition_and_filename(tmp_path: Path) -> None:
    """node_id + collector_run_id + batch_seq 명시 시 ADR-009 §D2.1 layout."""
    import pyarrow.parquet as pq
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    candles = [_make_candle(base_ts, Decimal("100000000"))]
    partition = write_candles(
        candles, root=tmp_path, snapshot_id="ignored",
        node_id="NODE_A",
        collector_run_id="NODE_A-20260505T223456Z",
        batch_seq=0,
    )
    assert "node=NODE_A" in partition.parts
    parquet_files = list(partition.glob("*.parquet"))
    assert len(parquet_files) == 1
    assert parquet_files[0].name == "NODE_A-20260505T223456Z-0.parquet"


def test_write_candles_parquet_metadata_node_id(tmp_path: Path) -> None:
    """node_id 명시 시 parquet metadata 에 node_id field."""
    import pyarrow.parquet as pq
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    candles = [_make_candle(base_ts, Decimal("100000000"))]
    partition = write_candles(
        candles, root=tmp_path, snapshot_id="ignored",
        node_id="NODE_B",
        collector_run_id="NODE_B-20260505T120000Z",
        batch_seq=3,
    )
    parquet = next(partition.glob("*.parquet"))
    # ParquetFile 로 file 자체 schema 의 metadata 만 검증 (Hive partition merge 회피)
    pf = pq.ParquetFile(parquet)
    meta = pf.schema_arrow.metadata or {}
    assert meta.get(b"node_id") == b"NODE_B"


def test_write_candles_legacy_no_node_id(tmp_path: Path) -> None:
    """node_id 미명시 시 기존 part-{snapshot_id}.parquet (backward compat)."""
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    candles = [_make_candle(base_ts, Decimal("100000000"))]
    partition = write_candles(candles, root=tmp_path, snapshot_id="legacy-snap-1")
    assert not any("node=" in p for p in partition.parts), \
        f"legacy write should not have node= partition: {partition}"
    parquet_files = list(partition.glob("*.parquet"))
    assert len(parquet_files) == 1
    assert parquet_files[0].name == "part-legacy-snap-1.parquet"


def test_write_candles_batch_seq_resets_per_collector_run_id(tmp_path: Path) -> None:
    """다른 collector_run_id 마다 batch_seq=0 재시작 가능 (file collision 0)."""
    base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
    candles = [_make_candle(base_ts, Decimal("100000000"))]
    p1 = write_candles(
        candles, root=tmp_path, snapshot_id="ignored",
        node_id="NODE_A", collector_run_id="NODE_A-20260505T100000Z", batch_seq=0,
    )
    p2 = write_candles(
        candles, root=tmp_path, snapshot_id="ignored",
        node_id="NODE_A", collector_run_id="NODE_A-20260505T120000Z", batch_seq=0,
    )
    assert p1 == p2  # same partition path
    files = sorted(p.name for p in p1.glob("*.parquet"))
    assert files == [
        "NODE_A-20260505T100000Z-0.parquet",
        "NODE_A-20260505T120000Z-0.parquet",
    ]
