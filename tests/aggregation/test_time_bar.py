"""tests for TimeBarAggregator — boundary [start, end) inclusion edge."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Literal

from mctrader_market.schemas.tick import TickRowV1_1
from mctrader_market.types import Symbol

from mctrader_data.aggregation.core import TimeBarAggregator

BTC_KRW = Symbol(base="BTC", quote="KRW")
ETH_KRW = Symbol(base="ETH", quote="KRW")


def _tick(
    ts: datetime,
    price: str,
    qty: str,
    symbol: Symbol = BTC_KRW,
    exchange: str = "upbit",
    side: Literal["BUY", "SELL"] = "BUY",
) -> TickRowV1_1:
    return TickRowV1_1(  # type: ignore[arg-type]
        ts_utc=ts,
        exchange=exchange,
        symbol=symbol,
        trade_id=f"t-{ts.isoformat()}",
        price=Decimal(price),
        quantity=Decimal(qty),
        side=side,
        is_taker=True,
    )


class TestTimeBarBoundary:
    """timeframe=1 min — [start, end) inclusion."""

    def test_emits_bar_when_minute_boundary_crossed(self) -> None:
        agg = TimeBarAggregator(timeframe=timedelta(minutes=1))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # 1st minute window [00:00, 00:01)
        out1 = agg.process_tick(_tick(base, "100", "1"))
        assert out1 is None  # first tick → bar in progress
        out2 = agg.process_tick(_tick(base + timedelta(seconds=30), "110", "2"))
        assert out2 is None

        # tick at exact 00:01:00 → triggers close of [00:00, 00:01) bar
        out3 = agg.process_tick(_tick(base + timedelta(minutes=1), "120", "1"))
        assert out3 is not None
        assert out3.bar_label == "time_60"
        assert out3.genesis_ts == base
        assert out3.ts_close == base + timedelta(minutes=1)
        assert out3.open == Decimal("100")
        assert out3.high == Decimal("110")
        assert out3.low == Decimal("100")
        assert out3.close == Decimal("110")
        assert out3.volume == Decimal("3")

    def test_left_inclusive_right_exclusive(self) -> None:
        """tick at exactly genesis_ts (start) included; tick at exactly end excluded."""
        agg = TimeBarAggregator(timeframe=timedelta(minutes=1))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        # tick @ 00:00:00 (boundary start) → included in first bar
        assert agg.process_tick(_tick(base, "100", "1")) is None
        # tick @ 00:01:00 (boundary end of first bar = boundary start of next) →
        #   triggers close + starts new bar
        bar = agg.process_tick(_tick(base + timedelta(minutes=1), "200", "1"))
        assert bar is not None
        # first bar contained only the 00:00:00 tick
        assert bar.open == Decimal("100")
        assert bar.close == Decimal("100")
        assert bar.volume == Decimal("1")


class TestMultipleBarsSequence:
    def test_two_consecutive_bars(self) -> None:
        agg = TimeBarAggregator(timeframe=timedelta(minutes=1))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        agg.process_tick(_tick(base + timedelta(seconds=10), "100", "1"))
        agg.process_tick(_tick(base + timedelta(seconds=50), "105", "1"))
        bar1 = agg.process_tick(_tick(base + timedelta(seconds=70), "110", "1"))
        assert bar1 is not None
        assert bar1.genesis_ts == base
        assert bar1.close == Decimal("105")

        agg.process_tick(_tick(base + timedelta(seconds=100), "115", "1"))
        bar2 = agg.process_tick(_tick(base + timedelta(seconds=130), "120", "1"))
        assert bar2 is not None
        assert bar2.genesis_ts == base + timedelta(minutes=1)
        assert bar2.open == Decimal("110")
        assert bar2.close == Decimal("115")


class TestEmptyBarSkipped:
    """No emit when no tick in a window. ADR-025 — emit only on bar close with content."""

    def test_no_emit_until_tick_in_next_window(self) -> None:
        agg = TimeBarAggregator(timeframe=timedelta(minutes=1))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        agg.process_tick(_tick(base, "100", "1"))

        # gap — no ticks in 00:01-00:03 → 다음 tick 도착 시 첫 bar 만 close
        bar = agg.process_tick(_tick(base + timedelta(minutes=3), "200", "1"))
        assert bar is not None
        assert bar.genesis_ts == base
        assert bar.ts_close == base + timedelta(minutes=1)
        # 그 다음 tick 은 새 bar 시작 (gap 안 empty bar 발급 X)


class TestPerSymbolState:
    """Multi-symbol — state isolation."""

    def test_two_symbols_independent_state(self) -> None:
        agg = TimeBarAggregator(timeframe=timedelta(minutes=1))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        agg.process_tick(_tick(base, "100", "1", symbol=BTC_KRW))
        agg.process_tick(_tick(base, "50", "2", symbol=ETH_KRW))

        bar_btc = agg.process_tick(_tick(base + timedelta(minutes=1), "200", "1", symbol=BTC_KRW))
        assert bar_btc is not None
        assert bar_btc.symbol == BTC_KRW
        assert bar_btc.open == Decimal("100")

        bar_eth = agg.process_tick(_tick(base + timedelta(minutes=1), "60", "1", symbol=ETH_KRW))
        assert bar_eth is not None
        assert bar_eth.symbol == ETH_KRW
        assert bar_eth.open == Decimal("50")


class TestTimeframeLabel:
    def test_5min_bar_label(self) -> None:
        agg = TimeBarAggregator(timeframe=timedelta(minutes=5))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        agg.process_tick(_tick(base, "100", "1"))
        bar = agg.process_tick(_tick(base + timedelta(minutes=5), "200", "1"))
        assert bar is not None
        assert bar.bar_label == "time_300"

    def test_1s_bar_label(self) -> None:
        agg = TimeBarAggregator(timeframe=timedelta(seconds=1))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        agg.process_tick(_tick(base, "100", "1"))
        bar = agg.process_tick(_tick(base + timedelta(seconds=1), "200", "1"))
        assert bar is not None
        assert bar.bar_label == "time_1"


class TestThresholdInBar:
    def test_threshold_equals_timeframe_seconds(self) -> None:
        agg = TimeBarAggregator(timeframe=timedelta(minutes=1))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        agg.process_tick(_tick(base, "100", "1"))
        bar = agg.process_tick(_tick(base + timedelta(minutes=1), "200", "1"))
        assert bar is not None
        assert bar.threshold == Decimal("60")
