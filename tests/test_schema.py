"""ADR-009 v1 schema validation tests."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from mctrader_market.types import Symbol, Timeframe

from mctrader_data.schema import OHLCV_COLUMNS, SCHEMA_VERSION, OhlcvRow


def test_schema_version_constant() -> None:
    assert SCHEMA_VERSION == "ohlcv.v1"


def test_ohlcv_columns_count_16() -> None:
    assert len(OHLCV_COLUMNS) == 16


def test_ohlcv_row_minimal() -> None:
    row = OhlcvRow(
        ts_utc=datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc),
        exchange="bithumb",
        symbol=Symbol(base="BTC", quote="KRW"),
        timeframe=Timeframe.H1,
        open=Decimal("100000000"),
        high=Decimal("100500000"),
        low=Decimal("99500000"),
        close=Decimal("100200000"),
        volume=Decimal("1.5"),
    )
    assert row.schema_version == "ohlcv.v1"
    assert row.is_complete is True
    assert row.value is None


def test_ohlcv_row_rejects_naive_datetime() -> None:
    with pytest.raises(ValidationError):
        OhlcvRow(
            ts_utc=datetime(2026, 5, 1, 0, 0),  # naive
            exchange="bithumb",
            symbol=Symbol(base="BTC", quote="KRW"),
            timeframe=Timeframe.H1,
            open=Decimal("100000000"),
            high=Decimal("100500000"),
            low=Decimal("99500000"),
            close=Decimal("100200000"),
            volume=Decimal("1.5"),
        )


def test_ohlcv_row_frozen() -> None:
    row = OhlcvRow(
        ts_utc=datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc),
        exchange="bithumb",
        symbol=Symbol(base="BTC", quote="KRW"),
        timeframe=Timeframe.H1,
        open=Decimal("100000000"),
        high=Decimal("100500000"),
        low=Decimal("99500000"),
        close=Decimal("100200000"),
        volume=Decimal("1.5"),
    )
    with pytest.raises(ValidationError):
        row.open = Decimal("999")  # type: ignore[misc]
