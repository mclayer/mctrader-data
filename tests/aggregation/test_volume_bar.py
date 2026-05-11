"""tests for VolumeBarAggregator — cumulative base-volume threshold."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mctrader_market.schemas.tick import TickRowV1_1
from mctrader_market.types import Symbol

from mctrader_data.aggregation.core import VolumeBarAggregator

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
        trade_id=f"t-{ts.isoformat()}-{qty}",
        price=Decimal(price),
        quantity=Decimal(qty),
        side="BUY",
        is_taker=True,
    )


class TestThresholdReached:
    def test_emits_when_cumulative_volume_meets_threshold(self) -> None:
        agg = VolumeBarAggregator(threshold=Decimal("10"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        out1 = agg.process_tick(_tick(base, "100", "3"))
        assert out1 is None  # 3 < 10
        out2 = agg.process_tick(_tick(base + timedelta(seconds=1), "110", "3"))
        assert out2 is None  # 6 < 10
        out3 = agg.process_tick(_tick(base + timedelta(seconds=2), "120", "4"))
        # cumulative = 10 → exactly meets threshold → close
        assert out3 is not None
        assert out3.bar_label == "vol_10"
        assert out3.open == Decimal("100")
        assert out3.high == Decimal("120")
        assert out3.low == Decimal("100")
        assert out3.close == Decimal("120")
        assert out3.volume == Decimal("10")
        assert out3.threshold == Decimal("10")

    def test_overshoot_in_single_tick(self) -> None:
        """Single tick that overshoots threshold — closes bar with full tick volume."""
        agg = VolumeBarAggregator(threshold=Decimal("10"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        out = agg.process_tick(_tick(base, "100", "15"))
        # tie-breaking SSOT: cumulative >= threshold → close, includes the triggering tick
        assert out is not None
        assert out.volume == Decimal("15")
        assert out.bar_label == "vol_10"


class TestTieBreakingExact:
    """tie_breaking SSOT: cumulative == threshold → close on the triggering tick."""

    def test_exact_threshold_includes_tick(self) -> None:
        agg = VolumeBarAggregator(threshold=Decimal("10"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        agg.process_tick(_tick(base, "100", "5"))
        bar = agg.process_tick(_tick(base + timedelta(seconds=1), "110", "5"))
        # cumulative 5+5=10 정확 == 10 → 본 tick 포함하여 close
        assert bar is not None
        assert bar.volume == Decimal("10")
        assert bar.close == Decimal("110")


class TestMultipleBars:
    def test_consecutive_bars_reset_state(self) -> None:
        agg = VolumeBarAggregator(threshold=Decimal("5"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        agg.process_tick(_tick(base, "100", "2"))
        bar1 = agg.process_tick(_tick(base + timedelta(seconds=1), "110", "3"))
        assert bar1 is not None
        assert bar1.open == Decimal("100")
        assert bar1.close == Decimal("110")
        assert bar1.volume == Decimal("5")

        agg.process_tick(_tick(base + timedelta(seconds=2), "120", "2"))
        bar2 = agg.process_tick(_tick(base + timedelta(seconds=3), "130", "3"))
        assert bar2 is not None
        assert bar2.open == Decimal("120")
        assert bar2.close == Decimal("130")
        assert bar2.volume == Decimal("5")
        assert bar2.genesis_ts == base + timedelta(seconds=2)


class TestGenesisTs:
    def test_genesis_ts_is_first_tick_in_bar(self) -> None:
        agg = VolumeBarAggregator(threshold=Decimal("10"))
        t1 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc)
        agg.process_tick(_tick(t1, "100", "3"))
        bar = agg.process_tick(_tick(t2, "110", "7"))
        assert bar is not None
        assert bar.genesis_ts == t1
        assert bar.ts_close == t2


class TestPerSymbolIsolation:
    def test_independent_volume_state(self) -> None:
        agg = VolumeBarAggregator(threshold=Decimal("10"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        agg.process_tick(_tick(base, "100", "8", symbol=BTC_KRW))
        agg.process_tick(_tick(base, "50", "5", symbol=ETH_KRW))

        # BTC 8 → close on next 2+
        bar_btc = agg.process_tick(_tick(base + timedelta(seconds=1), "200", "2", symbol=BTC_KRW))
        assert bar_btc is not None
        assert bar_btc.symbol == BTC_KRW

        # ETH still 5 < 10 — no emit
        out_eth = agg.process_tick(_tick(base + timedelta(seconds=1), "55", "3", symbol=ETH_KRW))
        assert out_eth is None


class TestThresholdLabel:
    def test_label_includes_int_threshold(self) -> None:
        agg = VolumeBarAggregator(threshold=Decimal("1000"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        bar = agg.process_tick(_tick(base, "100", "1500"))
        assert bar is not None
        assert bar.bar_label == "vol_1000"

    def test_label_threshold_fractional(self) -> None:
        agg = VolumeBarAggregator(threshold=Decimal("0.5"))
        base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        bar = agg.process_tick(_tick(base, "100", "1"))
        assert bar is not None
        # label uses string-form threshold (canonical)
        assert bar.bar_label == "vol_0.5"
