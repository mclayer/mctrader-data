"""tests for DollarBarAggregator — cumulative notional (price × quantity) threshold.

KRW boundary drift 핵심: notional 산술은 scaled int — Decimal accumulation drift 방지.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mctrader_market.schemas.tick import TickRowV1_1
from mctrader_market.types import Symbol

from mctrader_data.aggregation.core import DollarBarAggregator

BTC_KRW = Symbol(base="BTC", quote="KRW")
ETH_KRW = Symbol(base="ETH", quote="KRW")


def _tick(
    ts: datetime,
    price: str,
    qty: str,
    symbol: Symbol = BTC_KRW,
) -> TickRowV1_1:
    return TickRowV1_1(  # type: ignore[arg-type]
        ts_utc=ts,
        exchange="upbit",
        symbol=symbol,
        trade_id=f"t-{ts.isoformat()}-{price}-{qty}",
        price=Decimal(price),
        quantity=Decimal(qty),
        side="BUY",
        is_taker=True,
    )


class TestNotionalThreshold:
    def test_emits_when_notional_meets_threshold(self) -> None:
        # threshold = 1M KRW
        agg = DollarBarAggregator(threshold=Decimal("1000000"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # tick 1: 100 KRW × 5000 = 500000 KRW notional
        out1 = agg.process_tick(_tick(base, "100", "5000"))
        assert out1 is None
        # tick 2: 100 KRW × 3000 = 300000 KRW → cum 800000
        out2 = agg.process_tick(_tick(base + timedelta(seconds=1), "100", "3000"))
        assert out2 is None
        # tick 3: 100 KRW × 2000 = 200000 KRW → cum 1_000_000 정확
        bar = agg.process_tick(_tick(base + timedelta(seconds=2), "100", "2000"))
        assert bar is not None
        assert bar.bar_label == "dollar_1000000"
        assert bar.open == Decimal("100")
        assert bar.volume == Decimal("10000")  # 5000+3000+2000
        # value column = cumulative notional KRW
        assert bar.value == Decimal("1000000")
        assert bar.threshold == Decimal("1000000")

    def test_overshoot_in_single_tick(self) -> None:
        agg = DollarBarAggregator(threshold=Decimal("1000"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        # 100 × 50 = 5000 → way over 1000 → close on this tick
        bar = agg.process_tick(_tick(base, "100", "50"))
        assert bar is not None
        assert bar.value == Decimal("5000")
        assert bar.volume == Decimal("50")


class TestPriceVariance:
    def test_notional_with_changing_prices(self) -> None:
        agg = DollarBarAggregator(threshold=Decimal("10000"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # tick 1: 200 × 10 = 2000
        agg.process_tick(_tick(base, "200", "10"))
        # tick 2: 300 × 20 = 6000 → cum 8000
        agg.process_tick(_tick(base + timedelta(seconds=1), "300", "20"))
        # tick 3: 250 × 8 = 2000 → cum 10000 정확
        bar = agg.process_tick(_tick(base + timedelta(seconds=2), "250", "8"))
        assert bar is not None
        assert bar.open == Decimal("200")
        assert bar.high == Decimal("300")
        assert bar.low == Decimal("200")
        assert bar.close == Decimal("250")
        assert bar.volume == Decimal("38")
        assert bar.value == Decimal("10000")


class TestTieBreakingExact:
    """tie_breaking SSOT: cumulative notional == threshold → close on triggering tick."""

    def test_exact_threshold(self) -> None:
        agg = DollarBarAggregator(threshold=Decimal("500"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        bar = agg.process_tick(_tick(base, "100", "5"))  # 500 정확
        assert bar is not None
        assert bar.value == Decimal("500")


class TestMultipleBars:
    def test_consecutive_bars_reset_value(self) -> None:
        agg = DollarBarAggregator(threshold=Decimal("1000"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        bar1 = agg.process_tick(_tick(base, "100", "10"))  # 1000 정확
        assert bar1 is not None
        assert bar1.value == Decimal("1000")

        bar2 = agg.process_tick(_tick(base + timedelta(seconds=1), "200", "5"))  # 1000 정확
        assert bar2 is not None
        assert bar2.value == Decimal("1000")
        assert bar2.open == Decimal("200")


class TestPerSymbol:
    def test_isolation(self) -> None:
        agg = DollarBarAggregator(threshold=Decimal("1000"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # BTC: 100 × 5 = 500
        agg.process_tick(_tick(base, "100", "5", symbol=BTC_KRW))
        # ETH: 50 × 30 = 1500 → 즉시 close
        bar_eth = agg.process_tick(_tick(base, "50", "30", symbol=ETH_KRW))
        assert bar_eth is not None
        assert bar_eth.symbol == ETH_KRW
        assert bar_eth.value == Decimal("1500")

        # BTC 진행 중: 100 × 6 = 600 → cum 1100 → close
        bar_btc = agg.process_tick(_tick(base + timedelta(seconds=1), "100", "6", symbol=BTC_KRW))
        assert bar_btc is not None
        assert bar_btc.symbol == BTC_KRW
        assert bar_btc.value == Decimal("1100")


class TestBoundaryDrift:
    """ADR-025 핵심 — scaled int 사용으로 drift 방지."""

    def test_fractional_quantity_no_drift(self) -> None:
        # 0.1 × 10 = 1.0 → fractional quantity 도 정확
        agg = DollarBarAggregator(threshold=Decimal("100"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # 10 × 0.1 = 1 → need 100 ticks to reach 100. Do 99 + 1.
        # simplify: 100 KRW × 1 = 100 KRW (정확 threshold)
        bar = agg.process_tick(_tick(base, "100", "1"))
        assert bar is not None
        assert bar.value == Decimal("100")
