"""Dedup module tests — tier별 logical key + node priority + T1 hybrid + T2/T3 mismatch.

Per MCT-92 Phase 3 Task 1 (TDD).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any


from mctrader_data.dedup import (
    DEDUP_WINDOW_MS,
    NODE_PRIORITY_DEFAULT_SENTINEL,
    candle_logical_key,
    deduplicate_candles,
    deduplicate_orderbook_events,
    deduplicate_ticks,
    node_priority,
    orderbook_logical_key,
    tick_logical_key,
)


# ------- Test fixtures (lightweight row stubs) -----------------------

@dataclass
class _CandleRow:
    exchange: str
    symbol: str
    timeframe: str
    ts_utc: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    received_at: datetime | None = None
    node_id: str | None = None


@dataclass
class _TickRow:
    exchange: str
    symbol: str
    ts_utc: datetime
    received_at: datetime
    price: Decimal
    quantity: Decimal
    side: str
    raw_json: str
    node_id: str | None = None


@dataclass
class _OBRow:
    exchange: str
    symbol: str
    ts_utc: datetime
    received_at: datetime
    event_type: str
    side: str
    level: int
    price: Decimal
    quantity: Decimal
    raw_json: str = ""
    node_id: str | None = None


def _ts(offset_ms: int = 0) -> datetime:
    return datetime(2026, 5, 6, 12, 0, 0, tzinfo=timezone.utc) + timedelta(milliseconds=offset_ms)


# ====================================================================
# Logical key extractors
# ====================================================================

class TestLogicalKey:
    def test_t1_candle_4_key(self) -> None:
        row = _CandleRow(
            exchange="bithumb", symbol="KRW-BTC", timeframe="1h", ts_utc=_ts(),
            open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
            close=Decimal("100"), volume=Decimal("1"),
        )
        key = candle_logical_key(row)
        assert key == ("bithumb", "KRW-BTC", "1h", _ts())
        assert len(key) == 4

    def test_t2_tick_6_tuple(self) -> None:
        row = _TickRow(
            exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(0), received_at=_ts(0),
            price=Decimal("100"), quantity=Decimal("0.5"), side="buy", raw_json="{}",
        )
        key = tick_logical_key(row)
        assert key == ("bithumb", "KRW-BTC", _ts(0), Decimal("100"), Decimal("0.5"), "buy")
        assert len(key) == 6

    def test_t3_orderbook_8_tuple(self) -> None:
        row = _OBRow(
            exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(0), received_at=_ts(0),
            event_type="snapshot", side="bid", level=0,
            price=Decimal("100"), quantity=Decimal("1.5"),
        )
        key = orderbook_logical_key(row)
        assert key == ("bithumb", "KRW-BTC", _ts(0), "snapshot", "bid", 0,
                       Decimal("100"), Decimal("1.5"))
        assert len(key) == 8


# ====================================================================
# Node priority (Architect 결정 #2)
# ====================================================================

class TestNodePriority:
    def test_alphabetical_node_a_wins_over_node_b(self) -> None:
        assert node_priority("NODE_A") < node_priority("NODE_B")

    def test_explicit_node_wins_over_zzz_default(self) -> None:
        # NODE_A < zzz_DEFAULT (uppercase ASCII < lowercase)
        assert node_priority("NODE_A") < node_priority(None)
        assert node_priority(None) == NODE_PRIORITY_DEFAULT_SENTINEL

    def test_zzz_default_loses_to_any_explicit(self) -> None:
        assert node_priority("NODE_A") < NODE_PRIORITY_DEFAULT_SENTINEL
        assert node_priority("NODE_Z") < NODE_PRIORITY_DEFAULT_SENTINEL


# ====================================================================
# T1 candle — hybrid late correction (Architect 결정 #1)
# ====================================================================

class TestT1HybridLateCorrection:
    def test_received_at_max_first(self) -> None:
        early = _CandleRow(
            exchange="bithumb", symbol="KRW-BTC", timeframe="1h", ts_utc=_ts(),
            open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
            close=Decimal("100"), volume=Decimal("1"),
            received_at=_ts(0), node_id="NODE_A",
        )
        late = _CandleRow(
            exchange="bithumb", symbol="KRW-BTC", timeframe="1h", ts_utc=_ts(),
            open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
            close=Decimal("101"),  # corrected close (late correction)
            volume=Decimal("1"),
            received_at=_ts(50), node_id="NODE_B",
        )
        result = deduplicate_candles([early, late], multi_node=True)
        assert len(result.emitted) == 1
        # received_at MAX win (late row 의 close=101)
        assert result.emitted[0].close == Decimal("101")
        assert result.dup_skip_count == 1

    def test_tie_breaks_to_node_priority(self) -> None:
        # Same received_at — alphabetical node priority
        a = _CandleRow(
            exchange="bithumb", symbol="KRW-BTC", timeframe="1h", ts_utc=_ts(),
            open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
            close=Decimal("100"), volume=Decimal("1"),
            received_at=_ts(0), node_id="NODE_B",
        )
        b = _CandleRow(
            exchange="bithumb", symbol="KRW-BTC", timeframe="1h", ts_utc=_ts(),
            open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
            close=Decimal("100"), volume=Decimal("1"),
            received_at=_ts(0), node_id="NODE_A",  # alphabetical 우선
        )
        result = deduplicate_candles([a, b], multi_node=True)
        assert len(result.emitted) == 1
        assert result.emitted[0].node_id == "NODE_A"  # tie-break win

    def test_t1_no_quarantine_on_value_mismatch(self) -> None:
        """ADR-009 §D5: T1 mismatch = late correction, NOT quarantine."""
        a = _CandleRow(
            exchange="bithumb", symbol="KRW-BTC", timeframe="1h", ts_utc=_ts(),
            open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
            close=Decimal("100"), volume=Decimal("1"),
            received_at=_ts(0), node_id="NODE_A",
        )
        b = _CandleRow(
            exchange="bithumb", symbol="KRW-BTC", timeframe="1h", ts_utc=_ts(),
            open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
            close=Decimal("999"),  # different value — late correction, no quarantine
            volume=Decimal("1"),
            received_at=_ts(50), node_id="NODE_B",
        )
        result = deduplicate_candles([a, b], multi_node=True)
        assert result.quarantine_count == 0
        assert len(result.quarantine_records) == 0

    def test_single_node_passthrough(self) -> None:
        """multi_node=False — no dedup, pass-through."""
        rows = [
            _CandleRow(
                exchange="bithumb", symbol="KRW-BTC", timeframe="1h",
                ts_utc=_ts(i * 1000),
                open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
                close=Decimal("100"), volume=Decimal("1"),
                node_id="NODE_A",
            )
            for i in range(3)
        ]
        result = deduplicate_candles(rows, multi_node=False)
        assert len(result.emitted) == 3
        assert result.dup_skip_count == 0


# ====================================================================
# T2/T3 content mismatch
# ====================================================================

class TestT2T3ContentMismatch:
    def test_t2_logical_key_match_no_mismatch(self) -> None:
        """logical key 6-tuple + raw_json 일치 → idempotent skip."""
        a = _TickRow(
            exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(0), received_at=_ts(0),
            price=Decimal("100"), quantity=Decimal("0.5"), side="buy", raw_json='{"x":1}',
            node_id="NODE_A",
        )
        b = _TickRow(
            exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(0), received_at=_ts(50),
            price=Decimal("100"), quantity=Decimal("0.5"), side="buy", raw_json='{"x":1}',  # same content
            node_id="NODE_B",
        )
        result = deduplicate_ticks([a, b], multi_node=True)
        assert len(result.emitted) == 1
        assert result.dup_skip_count == 1
        assert result.quarantine_count == 0

    def test_t2_logical_key_match_value_mismatch_quarantines(self) -> None:
        """logical key 동일, raw_json 다름 → quarantine."""
        a = _TickRow(
            exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(0), received_at=_ts(0),
            price=Decimal("100"), quantity=Decimal("0.5"), side="buy", raw_json='{"x":1}',
            node_id="NODE_A",
        )
        b = _TickRow(
            exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(0), received_at=_ts(0),
            price=Decimal("100"), quantity=Decimal("0.5"), side="buy", raw_json='{"x":2}',  # mismatch
            node_id="NODE_B",
        )
        result = deduplicate_ticks([a, b], multi_node=True)
        assert result.quarantine_count == 1
        assert len(result.quarantine_records) == 1
        assert result.quarantine_records[0].reason == "ACTIVE_ACTIVE_MISMATCH"
        assert result.quarantine_records[0].tier == "tick"

    def test_t3_logical_key_match_value_mismatch_quarantines(self) -> None:
        """T3 8-tuple match + content mismatch → quarantine (Codex F-3 fix)."""
        a = _OBRow(
            exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(0), received_at=_ts(0),
            event_type="snapshot", side="bid", level=0,
            price=Decimal("100"), quantity=Decimal("1.5"),
            raw_json='{"a":1}', node_id="NODE_A",
        )
        b = _OBRow(
            exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(0), received_at=_ts(0),
            event_type="snapshot", side="bid", level=0,
            price=Decimal("100"), quantity=Decimal("1.5"),
            raw_json='{"a":2}',  # mismatch
            node_id="NODE_B",
        )
        result = deduplicate_orderbook_events([a, b], multi_node=True)
        assert result.quarantine_count == 1
        assert result.quarantine_records[0].tier == "orderbook"


# ====================================================================
# Counter sink protocol (Codex F-2 — heartbeat metric wiring)
# ====================================================================

class _StubSink:
    def __init__(self) -> None:
        self.dup_skip = 0
        self.quarantine = 0

    def increment_dup_skip(self, n: int = 1) -> None:
        self.dup_skip += n

    def increment_quarantine(self, n: int = 1) -> None:
        self.quarantine += n


class TestCounterSink:
    def test_t1_dedup_increments_dup_skip_sink(self) -> None:
        sink = _StubSink()
        rows = [
            _CandleRow(
                exchange="bithumb", symbol="KRW-BTC", timeframe="1h", ts_utc=_ts(),
                open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
                close=Decimal("100"), volume=Decimal("1"),
                received_at=_ts(i * 10), node_id=f"NODE_{chr(65 + i)}",
            )
            for i in range(3)
        ]
        deduplicate_candles(rows, multi_node=True, sink=sink)
        assert sink.dup_skip == 2  # 3 rows, 1 emitted, 2 skipped

    def test_t2_quarantine_increments_both_sinks(self) -> None:
        sink = _StubSink()
        a = _TickRow(
            exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(0), received_at=_ts(0),
            price=Decimal("100"), quantity=Decimal("0.5"), side="buy", raw_json='{"x":1}',
            node_id="NODE_A",
        )
        b = _TickRow(
            exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(0), received_at=_ts(0),
            price=Decimal("100"), quantity=Decimal("0.5"), side="buy", raw_json='{"x":2}',
            node_id="NODE_B",
        )
        deduplicate_ticks([a, b], multi_node=True, sink=sink)
        assert sink.quarantine == 1


# ====================================================================
# Quarantine backpressure (Architect 결정 #8 + Codex F-6 fix)
# ====================================================================

class TestQuarantineBackpressure:
    def test_quarantine_count_includes_batched_mismatches(self) -> None:
        """artifact 가 batching 되더라도 quarantine_count 는 모든 mismatch 합."""
        # 200 mismatch (cap=100/sec → 100 individual + 100 batched)
        rows: list[Any] = []
        for i in range(200):
            rows.append(_TickRow(
                exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(i),
                received_at=_ts(i),
                price=Decimal("100"), quantity=Decimal("0.5"), side="buy",
                raw_json=f'{{"a":{i}}}', node_id="NODE_A",
            ))
            rows.append(_TickRow(
                exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(i),
                received_at=_ts(i),
                price=Decimal("100"), quantity=Decimal("0.5"), side="buy",
                raw_json=f'{{"b":{i}}}',  # mismatch
                node_id="NODE_B",
            ))
        result = deduplicate_ticks(rows, multi_node=True)
        # 200 mismatch (각 ts 마다 NODE_A vs NODE_B raw_json 다름)
        assert result.quarantine_count == 200
        # artifact 는 backpressure 적용 — emitted 갯수는 cap 기반
        # records 갯수는 admit 된 + flushed batch 합
        assert len(result.quarantine_records) <= 200  # batch + immediate emission

    def test_no_mismatch_no_quarantine_records(self) -> None:
        rows = [
            _TickRow(
                exchange="bithumb", symbol="KRW-BTC", ts_utc=_ts(i), received_at=_ts(i),
                price=Decimal("100"), quantity=Decimal("0.5"), side="buy", raw_json="{}",
                node_id="NODE_A",
            )
            for i in range(5)
        ]
        result = deduplicate_ticks(rows, multi_node=True)
        assert result.quarantine_count == 0
        assert result.quarantine_records == []


# ====================================================================
# Constants check
# ====================================================================

def test_dedup_window_ms_is_200() -> None:
    """Architect 결정 #5 — 200ms safety margin."""
    assert DEDUP_WINDOW_MS == 200
