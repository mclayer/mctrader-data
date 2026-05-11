"""tests for TickBarAggregator — fixed trade count threshold."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mctrader_market.schemas.tick import TickRowV1_1
from mctrader_market.types import Symbol

from mctrader_data.aggregation.core import TickBarAggregator

BTC_KRW = Symbol(base="BTC", quote="KRW")
ETH_KRW = Symbol(base="ETH", quote="KRW")


def _tick(ts: datetime, price: str, qty: str = "1", symbol: Symbol = BTC_KRW) -> TickRowV1_1:
    return TickRowV1_1(  # type: ignore[arg-type]
        ts_utc=ts,
        exchange="upbit",
        symbol=symbol,
        trade_id=f"t-{ts.isoformat()}-{price}",
        price=Decimal(price),
        quantity=Decimal(qty),
        side="BUY",
        is_taker=True,
    )


class TestThresholdReached:
    def test_emits_when_tick_count_meets_threshold(self) -> None:
        agg = TickBarAggregator(threshold=3)
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        assert agg.process_tick(_tick(base, "100")) is None
        assert agg.process_tick(_tick(base + timedelta(seconds=1), "110")) is None
        bar = agg.process_tick(_tick(base + timedelta(seconds=2), "120"))

        assert bar is not None
        assert bar.bar_label == "tick_3"
        assert bar.open == Decimal("100")
        assert bar.high == Decimal("120")
        assert bar.low == Decimal("100")
        assert bar.close == Decimal("120")
        assert bar.volume == Decimal("3")  # qty=1 × 3 ticks
        assert bar.threshold == Decimal("3")

    def test_single_tick_threshold(self) -> None:
        agg = TickBarAggregator(threshold=1)
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        bar = agg.process_tick(_tick(base, "100", "1"))
        assert bar is not None
        assert bar.bar_label == "tick_1"
        assert bar.open == bar.high == bar.low == bar.close == Decimal("100")


class TestConsecutiveBars:
    def test_state_resets(self) -> None:
        agg = TickBarAggregator(threshold=2)
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        agg.process_tick(_tick(base, "100"))
        bar1 = agg.process_tick(_tick(base + timedelta(seconds=1), "110"))
        assert bar1 is not None
        assert bar1.open == Decimal("100")

        agg.process_tick(_tick(base + timedelta(seconds=2), "120"))
        bar2 = agg.process_tick(_tick(base + timedelta(seconds=3), "130"))
        assert bar2 is not None
        assert bar2.open == Decimal("120")
        assert bar2.close == Decimal("130")


class TestOHLCInvariants:
    def test_high_low_track_correctly(self) -> None:
        agg = TickBarAggregator(threshold=5)
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        prices = ["100", "150", "80", "120", "90"]
        bar = None
        for i, p in enumerate(prices):
            bar = agg.process_tick(_tick(base + timedelta(seconds=i), p))

        assert bar is not None
        assert bar.open == Decimal("100")
        assert bar.high == Decimal("150")
        assert bar.low == Decimal("80")
        assert bar.close == Decimal("90")


class TestPerSymbol:
    def test_isolation(self) -> None:
        agg = TickBarAggregator(threshold=3)
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        agg.process_tick(_tick(base, "100", symbol=BTC_KRW))
        agg.process_tick(_tick(base, "50", symbol=ETH_KRW))
        agg.process_tick(_tick(base + timedelta(seconds=1), "110", symbol=BTC_KRW))
        agg.process_tick(_tick(base + timedelta(seconds=1), "55", symbol=ETH_KRW))
        bar_btc = agg.process_tick(_tick(base + timedelta(seconds=2), "120", symbol=BTC_KRW))
        bar_eth = agg.process_tick(_tick(base + timedelta(seconds=2), "60", symbol=ETH_KRW))

        assert bar_btc is not None
        assert bar_btc.symbol == BTC_KRW
        assert bar_btc.open == Decimal("100")
        assert bar_eth is not None
        assert bar_eth.symbol == ETH_KRW
        assert bar_eth.open == Decimal("50")


class TestInvalidThreshold:
    def test_zero_threshold_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="threshold"):
            TickBarAggregator(threshold=0)

    def test_negative_threshold_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="threshold"):
            TickBarAggregator(threshold=-1)
