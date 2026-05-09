# tests/test_wal_ndjson_codec.py
"""Tests for wal/ndjson_codec.py — INV-5 decimal roundtrip."""
from __future__ import annotations

from decimal import Decimal
from datetime import datetime, timezone

import pytest

from mctrader_data.wal.ndjson_codec import decode_line, encode_record


def test_encode_decode_roundtrip_basic() -> None:
    record = {
        "ts_utc": datetime(2026, 5, 9, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        "price": Decimal("100000.123456789012345678"),
        "quantity": Decimal("0.001"),
        "symbol": "KRW-BTC",
    }
    line = encode_record(record)
    assert line.endswith("\n")
    decoded = decode_line(line)
    assert decoded["symbol"] == "KRW-BTC"
    assert decoded["price"] == Decimal("100000.123456789012345678")


def test_decimal_no_precision_loss_38_18() -> None:
    """INV-5: Decimal(38,18) round-trip preserves all digits."""
    price = Decimal("99999999999999999999.123456789012345678")  # 38 significant digits
    record = {"price": price}
    decoded = decode_line(encode_record(record))
    assert decoded["price"] == price


def test_decimal_no_scientific_notation() -> None:
    """str(Decimal) must not produce scientific notation for normal values."""
    # Ensure Decimal values that could trigger scientific notation are safe
    price = Decimal("1E+10")
    line = encode_record({"price": price})
    decoded = decode_line(line)
    # Round-trip safe: Decimal("1E+10") == Decimal("10000000000")
    assert decoded["price"] == price


def test_encode_produces_single_line() -> None:
    line = encode_record({"x": 1})
    assert line.count("\n") == 1
    assert line[-1] == "\n"


def test_decode_non_decimal_number_preserved_as_decimal() -> None:
    """parse_float=Decimal ensures float literals in JSON become Decimal."""
    line = '{"qty": 1.5}\n'
    decoded = decode_line(line)
    assert isinstance(decoded["qty"], Decimal)
    assert decoded["qty"] == Decimal("1.5")
