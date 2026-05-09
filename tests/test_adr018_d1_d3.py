"""ADR-018 D1/D2/D3 패턴 위반 감사 테스트 (MCT-115)."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError as PydanticValidationError

from mctrader_data.manifest import CollectorManifest
from mctrader_data.metadata_storage import ExchangeMetadataRecord
from mctrader_data.orderbook_snapshot_storage import OrderbookSnapshotRecord
from mctrader_data.orderbook_storage import OrderbookEventRecord
from mctrader_data.schema import OhlcvRow
from mctrader_data.tick_storage import TickRecord
from mctrader_market.types import Symbol, Timeframe


_TS = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)


# ─── D1: TickRecord float 거부 ────────────────────────────────────────────────

def test_tick_record_rejects_float_price() -> None:
    with pytest.raises(TypeError, match="float"):
        TickRecord(
            ts_utc=_TS, received_at=_TS,
            exchange="bithumb", symbol="KRW-BTC",
            price=1.5,  # type: ignore[arg-type]  # intentional: testing runtime rejection
            quantity=Decimal("0.001"), side="buy",
        )


def test_tick_record_rejects_float_quantity() -> None:
    with pytest.raises(TypeError, match="float"):
        TickRecord(
            ts_utc=_TS, received_at=_TS,
            exchange="bithumb", symbol="KRW-BTC",
            price=Decimal("100000000"), quantity=0.001,  # type: ignore[arg-type]
            side="buy",
        )


def test_tick_record_accepts_decimal() -> None:
    r = TickRecord(
        ts_utc=_TS, received_at=_TS,
        exchange="bithumb", symbol="KRW-BTC",
        price=Decimal("100000000"), quantity=Decimal("0.001"), side="buy",
    )
    assert r.price == Decimal("100000000")


# ─── D1: OrderbookEventRecord float 거부 ─────────────────────────────────────

def test_orderbook_event_record_rejects_float_price() -> None:
    with pytest.raises(TypeError, match="float"):
        OrderbookEventRecord(
            ts_utc=_TS, received_at=_TS,
            exchange="bithumb", symbol="KRW-BTC",
            event_type="snapshot", side="bid", level=0,
            price=100.5,  # type: ignore[arg-type]
            quantity=Decimal("1.0"),
        )


def test_orderbook_event_record_rejects_float_quantity() -> None:
    with pytest.raises(TypeError, match="float"):
        OrderbookEventRecord(
            ts_utc=_TS, received_at=_TS,
            exchange="bithumb", symbol="KRW-BTC",
            event_type="snapshot", side="bid", level=0,
            price=Decimal("100000000"), quantity=0.5,  # type: ignore[arg-type]
        )


def test_orderbook_event_record_accepts_decimal() -> None:
    r = OrderbookEventRecord(
        ts_utc=_TS, received_at=_TS,
        exchange="bithumb", symbol="KRW-BTC",
        event_type="snapshot", side="bid", level=0,
        price=Decimal("100000000"), quantity=Decimal("1.0"),
    )
    assert r.price == Decimal("100000000")


# ─── D1: OrderbookSnapshotRecord float 거부 ──────────────────────────────────

def test_orderbook_snapshot_record_rejects_float_price() -> None:
    with pytest.raises(TypeError, match="float"):
        OrderbookSnapshotRecord(
            ts_utc=_TS, received_at=_TS,
            exchange="bithumb", symbol="KRW-BTC",
            baseline_seq=1234567890,
            side="bid", level=0,
            price=99.9,  # type: ignore[arg-type]
            quantity=Decimal("10.0"),
            payload_hash="abc123",
        )


def test_orderbook_snapshot_record_rejects_float_quantity() -> None:
    with pytest.raises(TypeError, match="float"):
        OrderbookSnapshotRecord(
            ts_utc=_TS, received_at=_TS,
            exchange="bithumb", symbol="KRW-BTC",
            baseline_seq=1234567890,
            side="bid", level=0,
            price=Decimal("99000000"), quantity=9.9,  # type: ignore[arg-type]
            payload_hash="abc123",
        )


def test_orderbook_snapshot_record_accepts_decimal() -> None:
    r = OrderbookSnapshotRecord(
        ts_utc=_TS, received_at=_TS,
        exchange="bithumb", symbol="KRW-BTC",
        baseline_seq=1234567890,
        side="bid", level=0,
        price=Decimal("99000000"), quantity=Decimal("10.0"),
        payload_hash="abc123",
    )
    assert r.price == Decimal("99000000")


# ─── D1: ExchangeMetadataRecord float 거부 ───────────────────────────────────

def test_exchange_metadata_record_rejects_float_acc_trade_value() -> None:
    with pytest.raises(TypeError, match="float"):
        ExchangeMetadataRecord(
            exchange="bithumb", symbol="KRW-BTC",
            fetched_date=date(2026, 5, 1),
            fetched_at=_TS,
            source_snapshot_id="abc123",
            data_hash="def456",
            asset_status="1",
            acc_trade_value_24h=12345.6,  # type: ignore[arg-type]
        )


def test_exchange_metadata_record_rejects_float_tick_size() -> None:
    with pytest.raises(TypeError, match="float"):
        ExchangeMetadataRecord(
            exchange="bithumb", symbol="KRW-BTC",
            fetched_date=date(2026, 5, 1),
            fetched_at=_TS,
            source_snapshot_id="abc123",
            data_hash="def456",
            asset_status="1",
            acc_trade_value_24h=Decimal("12345000000"),
            tick_size=1.0,  # type: ignore[arg-type]
        )


def test_exchange_metadata_record_accepts_none_optionals() -> None:
    r = ExchangeMetadataRecord(
        exchange="bithumb", symbol="KRW-BTC",
        fetched_date=date(2026, 5, 1),
        fetched_at=_TS,
        source_snapshot_id="abc123",
        data_hash="def456",
        asset_status="1",
        acc_trade_value_24h=Decimal("12345000000"),
    )
    assert r.tick_size is None
    assert r.acc_trade_value_24h == Decimal("12345000000")


# ─── D2: CollectorManifest frozen + tuple ─────────────────────────────────────

def test_collector_manifest_frozen() -> None:
    m = CollectorManifest(
        collector_run_id="run-001",
        started_at_utc=_TS,
        exchange="bithumb",
        selected_symbols=("KRW-BTC", "KRW-ETH"),
        channels=("transaction",),
        selection_method="explicit",
    )
    with pytest.raises(PydanticValidationError):
        m.collector_run_id = "tampered"  # type: ignore[misc]


def test_collector_manifest_selected_symbols_is_tuple() -> None:
    m = CollectorManifest(
        collector_run_id="run-001",
        started_at_utc=_TS,
        exchange="bithumb",
        selected_symbols=["KRW-BTC", "KRW-ETH"],  # type: ignore[arg-type]  # testing BeforeValidator coercion
        channels=["transaction"],  # type: ignore[arg-type]
        selection_method="explicit",
    )
    assert isinstance(m.selected_symbols, tuple)
    assert m.selected_symbols == ("KRW-BTC", "KRW-ETH")


def test_collector_manifest_channels_is_tuple() -> None:
    m = CollectorManifest(
        collector_run_id="run-001",
        started_at_utc=_TS,
        exchange="bithumb",
        selected_symbols=["KRW-BTC"],  # type: ignore[arg-type]
        channels=["transaction", "orderbook"],  # type: ignore[arg-type]  # testing BeforeValidator coercion
        selection_method="explicit",
    )
    assert isinstance(m.channels, tuple)
    assert m.channels == ("transaction", "orderbook")


def test_collector_manifest_roundtrip_json_preserves_tuple() -> None:
    m = CollectorManifest(
        collector_run_id="run-001",
        started_at_utc=_TS,
        exchange="bithumb",
        selected_symbols=("KRW-BTC",),
        channels=("transaction",),
        selection_method="explicit",
    )
    json_str = m.model_dump_json()
    m2 = CollectorManifest.model_validate_json(json_str)
    assert isinstance(m2.selected_symbols, tuple)
    assert isinstance(m2.channels, tuple)


# ─── D3: OhlcvRow cross-field invariant ──────────────────────────────────────

_SYM = Symbol(base="BTC", quote="KRW")


def test_ohlcv_row_rejects_low_gt_high() -> None:
    with pytest.raises(PydanticValidationError):
        OhlcvRow(
            ts_utc=_TS, exchange="bithumb",
            symbol=_SYM, timeframe=Timeframe.H1,
            open=Decimal("100000000"),
            high=Decimal("99000000"),    # high < low → invalid
            low=Decimal("100500000"),
            close=Decimal("100000000"),
            volume=Decimal("1.5"),
        )


def test_ohlcv_row_rejects_open_below_low() -> None:
    with pytest.raises(PydanticValidationError):
        OhlcvRow(
            ts_utc=_TS, exchange="bithumb",
            symbol=_SYM, timeframe=Timeframe.H1,
            open=Decimal("98000000"),    # open < low → invalid
            high=Decimal("101000000"),
            low=Decimal("99000000"),
            close=Decimal("100000000"),
            volume=Decimal("1.5"),
        )


def test_ohlcv_row_rejects_close_above_high() -> None:
    with pytest.raises(PydanticValidationError):
        OhlcvRow(
            ts_utc=_TS, exchange="bithumb",
            symbol=_SYM, timeframe=Timeframe.H1,
            open=Decimal("100000000"),
            high=Decimal("100500000"),
            low=Decimal("99500000"),
            close=Decimal("101000000"),  # close > high → invalid
            volume=Decimal("1.5"),
        )


def test_ohlcv_row_rejects_negative_volume() -> None:
    with pytest.raises(PydanticValidationError):
        OhlcvRow(
            ts_utc=_TS, exchange="bithumb",
            symbol=_SYM, timeframe=Timeframe.H1,
            open=Decimal("100000000"),
            high=Decimal("100500000"),
            low=Decimal("99500000"),
            close=Decimal("100200000"),
            volume=Decimal("-1"),  # negative volume → invalid
        )


def test_ohlcv_row_accepts_valid_candle() -> None:
    row = OhlcvRow(
        ts_utc=_TS, exchange="bithumb",
        symbol=_SYM, timeframe=Timeframe.H1,
        open=Decimal("100000000"),
        high=Decimal("100500000"),
        low=Decimal("99500000"),
        close=Decimal("100200000"),
        volume=Decimal("1.5"),
    )
    assert row.high >= row.low


def test_ohlcv_row_accepts_doji_candle() -> None:
    """open == close == high == low (점형 봉) 은 유효."""
    v = Decimal("100000000")
    row = OhlcvRow(
        ts_utc=_TS, exchange="bithumb",
        symbol=_SYM, timeframe=Timeframe.H1,
        open=v, high=v, low=v, close=v,
        volume=Decimal("0"),
    )
    assert row.open == row.close
