"""tests for determinism — same input → same output (Hot/Cold consistency premise).

ADR-025 §determinism: random/threading/wall-clock 금지 → golden replay 가능.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mctrader_market.schemas.tick import TickRowV1_1
from mctrader_market.types import Symbol

from mctrader_data.aggregation.core import (
    DollarBarAggregator,
    TickBarAggregator,
    TimeBarAggregator,
    VolumeBarAggregator,
)

BTC_KRW = Symbol(base="BTC", quote="KRW")
ETH_KRW = Symbol(base="ETH", quote="KRW")


def _golden_ticks() -> list[TickRowV1_1]:
    """Deterministic fixture — 50 ticks across 5 minutes, BTC/KRW + ETH/KRW interleaved."""
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    ticks: list[TickRowV1_1] = []
    for i in range(50):
        symbol = BTC_KRW if i % 2 == 0 else ETH_KRW
        # deterministic price walk
        price = 100 + (i % 11)  # 100..110 cycling
        qty = 1 + (i % 5)  # 1..5 cycling
        ts = base + timedelta(seconds=6 * i)  # 6s apart
        ticks.append(
            TickRowV1_1(  # type: ignore[arg-type]
                ts_utc=ts,
                exchange="upbit",
                symbol=symbol,
                trade_id=f"golden-{i}",
                price=Decimal(price),
                quantity=Decimal(qty),
                side="BUY" if i % 3 != 0 else "SELL",
                is_taker=True,
            )
        )
    return ticks


class TestTimeBarDeterminism:
    def test_two_runs_identical_output(self) -> None:
        ticks = _golden_ticks()

        agg1 = TimeBarAggregator(timeframe=timedelta(minutes=1))
        agg2 = TimeBarAggregator(timeframe=timedelta(minutes=1))

        out1 = [agg1.process_tick(t) for t in ticks]
        out2 = [agg2.process_tick(t) for t in ticks]

        # bar 객체 비교 (Pydantic frozen → __eq__ field-wise)
        assert out1 == out2

    def test_emits_expected_count(self) -> None:
        ticks = _golden_ticks()
        agg = TimeBarAggregator(timeframe=timedelta(minutes=1))
        bars = [b for t in ticks if (b := agg.process_tick(t)) is not None]
        # 50 tick × 6s = 300s = 5 min → at least 4 closed bars (per symbol)
        # BTC and ETH interleaved → expected min 4 bars total (lower bound — deterministic upper from impl)
        assert len(bars) >= 4


class TestVolumeBarDeterminism:
    def test_replay_identical(self) -> None:
        ticks = _golden_ticks()
        agg1 = VolumeBarAggregator(threshold=Decimal("20"))
        agg2 = VolumeBarAggregator(threshold=Decimal("20"))
        out1 = [agg1.process_tick(t) for t in ticks]
        out2 = [agg2.process_tick(t) for t in ticks]
        assert out1 == out2


class TestTickBarDeterminism:
    def test_replay_identical(self) -> None:
        ticks = _golden_ticks()
        agg1 = TickBarAggregator(threshold=5)
        agg2 = TickBarAggregator(threshold=5)
        out1 = [agg1.process_tick(t) for t in ticks]
        out2 = [agg2.process_tick(t) for t in ticks]
        assert out1 == out2


class TestDollarBarDeterminism:
    def test_replay_identical(self) -> None:
        ticks = _golden_ticks()
        agg1 = DollarBarAggregator(threshold=Decimal("2000"))
        agg2 = DollarBarAggregator(threshold=Decimal("2000"))
        out1 = [agg1.process_tick(t) for t in ticks]
        out2 = [agg2.process_tick(t) for t in ticks]
        assert out1 == out2


class TestCrossAggregatorBoundaryStability:
    """4 aggregator 같은 입력 → 각자 일관된 output (다른 알고리즘이지만 input determinism 동일)."""

    def test_all_four_aggregators_replay_stable(self) -> None:
        ticks = _golden_ticks()

        time_agg = TimeBarAggregator(timeframe=timedelta(minutes=1))
        vol_agg = VolumeBarAggregator(threshold=Decimal("10"))
        tick_agg = TickBarAggregator(threshold=3)
        dollar_agg = DollarBarAggregator(threshold=Decimal("1500"))

        results: dict[str, list[object]] = {
            "time": [],
            "vol": [],
            "tick": [],
            "dollar": [],
        }
        for t in ticks:
            results["time"].append(time_agg.process_tick(t))
            results["vol"].append(vol_agg.process_tick(t))
            results["tick"].append(tick_agg.process_tick(t))
            results["dollar"].append(dollar_agg.process_tick(t))

        # 2회 replay 일치
        time_agg2 = TimeBarAggregator(timeframe=timedelta(minutes=1))
        vol_agg2 = VolumeBarAggregator(threshold=Decimal("10"))
        tick_agg2 = TickBarAggregator(threshold=3)
        dollar_agg2 = DollarBarAggregator(threshold=Decimal("1500"))

        results2: dict[str, list[object]] = {
            "time": [],
            "vol": [],
            "tick": [],
            "dollar": [],
        }
        for t in ticks:
            results2["time"].append(time_agg2.process_tick(t))
            results2["vol"].append(vol_agg2.process_tick(t))
            results2["tick"].append(tick_agg2.process_tick(t))
            results2["dollar"].append(dollar_agg2.process_tick(t))

        for key in ("time", "vol", "tick", "dollar"):
            assert results[key] == results2[key], f"non-deterministic {key} replay"
