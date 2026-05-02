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
