"""tests for scaled_int — KRW boundary drift 방지 utility.

ADR-025 (Aggregation Core Lib Contract) §scaled-int boundary.
KRW = 원 단위 = naturally integer-friendly (precision=0 default).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from mctrader_data.aggregation.scaled_int import from_scaled, to_scaled


class TestKRWDefault:
    """precision=0 default — KRW 원 단위."""

    def test_to_scaled_integer_value(self) -> None:
        assert to_scaled(Decimal("12345")) == 12345

    def test_to_scaled_zero(self) -> None:
        assert to_scaled(Decimal("0")) == 0

    def test_to_scaled_large_krw(self) -> None:
        # 1조 원
        assert to_scaled(Decimal("1000000000000")) == 1_000_000_000_000

    def test_from_scaled_returns_decimal(self) -> None:
        result = from_scaled(12345)
        assert isinstance(result, Decimal)
        assert result == Decimal("12345")

    def test_round_trip_krw(self) -> None:
        original = Decimal("987654321")
        assert from_scaled(to_scaled(original)) == original


class TestPositivePrecision:
    """precision>0 — sub-unit precision (예: USD cent → precision=2)."""

    def test_to_scaled_precision_2(self) -> None:
        # $12.34 → 1234 cents
        assert to_scaled(Decimal("12.34"), precision=2) == 1234

    def test_to_scaled_precision_8(self) -> None:
        # 0.00000001 BTC → 1 satoshi
        assert to_scaled(Decimal("0.00000001"), precision=8) == 1

    def test_from_scaled_precision_2(self) -> None:
        assert from_scaled(1234, precision=2) == Decimal("12.34")

    def test_round_trip_high_precision(self) -> None:
        original = Decimal("0.12345678")
        assert from_scaled(to_scaled(original, precision=8), precision=8) == original


class TestBoundaryDriftPrevention:
    """ADR-025 핵심 — boundary drift 방지. 정수 산술만 사용."""

    def test_no_drift_repeated_addition(self) -> None:
        """0.1 + 0.1 + 0.1 == 0.3 (Decimal 정확) → scaled 정수 산술 유지."""
        a = to_scaled(Decimal("0.1"), precision=1)
        b = to_scaled(Decimal("0.1"), precision=1)
        c = to_scaled(Decimal("0.1"), precision=1)
        total_scaled = a + b + c
        assert from_scaled(total_scaled, precision=1) == Decimal("0.3")

    def test_no_drift_repeated_multiplication(self) -> None:
        """price × quantity threshold — dollar bar 핵심 산술."""
        price_scaled = to_scaled(Decimal("100"))  # KRW
        quantity = 1000
        # notional = price × quantity = 100000 KRW
        notional_scaled = price_scaled * quantity
        assert from_scaled(notional_scaled) == Decimal("100000")


class TestNegativeAndEdges:
    def test_negative_value(self) -> None:
        # signed scaled int 허용 (PnL 계산용)
        assert to_scaled(Decimal("-500")) == -500
        assert from_scaled(-500) == Decimal("-500")

    def test_invalid_precision_negative(self) -> None:
        with pytest.raises(ValueError, match="precision must be"):
            to_scaled(Decimal("1"), precision=-1)

    def test_invalid_precision_negative_from(self) -> None:
        with pytest.raises(ValueError, match="precision must be"):
            from_scaled(1, precision=-1)

    def test_fractional_at_precision_zero_raises(self) -> None:
        """precision=0 인데 fractional input — explicit error (silent truncation 금지)."""
        with pytest.raises(ValueError, match="loses precision|fractional"):
            to_scaled(Decimal("12.5"))


class TestBackwardCompat:
    """ADR-008 SemVer — default precision=0 stable signature."""

    def test_default_precision_is_zero(self) -> None:
        # 호출 시 precision 인자 생략 → 0 (KRW 기본)
        assert to_scaled(Decimal("100")) == 100
        assert from_scaled(100) == Decimal("100")
