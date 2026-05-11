# tests/wal/test_dedup_fallback_tuple.py
"""MCT-140 Story-6 — Fallback tuple dedup for tick.v1.1.

ADR-009 §D10.8 amends §D10.7 — fallback dedup key extends from the 6-tuple
``(exchange, symbol, ts_utc, price, quantity, side)`` to the 8-tuple
``(exchange, symbol, ts_utc, price, quantity, side, raw_json_hash, ingest_seq)``
(``raw_json_hash`` is the tick.v1.1 ``payload_hash`` column).

Active-active dedup contract (Compactor scan + reader scan):
- byte-identical pair → idempotent skip (dup_skip_count += 1)
- logical key match + payload_hash mismatch → quarantine emit
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from mctrader_data.dedup import (
    DedupResult,
    deduplicate_ticks_v1_1,
    tick_v1_1_logical_key,
)


@dataclass
class _TickV1_1Row:
    """Minimal duck-typed row matching the persisted tick.v1.1 schema."""

    exchange: str
    symbol: str
    ts_utc: datetime
    price: Decimal
    quantity: Decimal
    side: str
    payload_hash: str | None
    ingest_seq: int | None
    raw_json: str | None = None
    node_id: str | None = None
    validation_status: str = "OK"


def _row(
    *,
    payload_hash: str = "deadbeef00000000",
    ingest_seq: int = 1,
    raw_json: str | None = '{"a":1}',
    node_id: str = "NODE_A",
    side: str = "BUY",
) -> _TickV1_1Row:
    return _TickV1_1Row(
        exchange="bithumb",
        symbol="KRW-BTC",
        ts_utc=datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc),
        price=Decimal("100000.000000000000000000"),
        quantity=Decimal("0.010000000000000000"),
        side=side,
        payload_hash=payload_hash,
        ingest_seq=ingest_seq,
        raw_json=raw_json,
        node_id=node_id,
    )


def test_8_tuple_key_includes_payload_hash_and_ingest_seq() -> None:
    row = _row(payload_hash="abc1234567890def", ingest_seq=42)
    key = tick_v1_1_logical_key(row)
    assert len(key) == 8
    assert key == (
        "bithumb",
        "KRW-BTC",
        row.ts_utc,
        row.price,
        row.quantity,
        "BUY",
        "abc1234567890def",
        42,
    )


def test_dedup_same_trade_two_nodes_collapse_to_single_row() -> None:
    """Same trade observed on NODE_A + NODE_B (identical payload_hash) → 1 row."""
    a = _row(node_id="NODE_A", payload_hash="hash_x", ingest_seq=10)
    b = _row(node_id="NODE_B", payload_hash="hash_x", ingest_seq=10)
    result = deduplicate_ticks_v1_1([a, b], multi_node=True)
    assert isinstance(result, DedupResult)
    assert len(result.emitted) == 1
    assert result.dup_skip_count == 1
    assert result.quarantine_count == 0


def test_dedup_single_node_pass_through() -> None:
    """multi_node=False → no dedup; preserve all rows."""
    a = _row(node_id="NODE_A", ingest_seq=1)
    b = _row(node_id="NODE_A", ingest_seq=2, payload_hash="hash_y")
    result = deduplicate_ticks_v1_1([a, b], multi_node=False)
    assert len(result.emitted) == 2
    assert result.dup_skip_count == 0


def test_dedup_different_ingest_seq_treated_as_distinct_rows() -> None:
    """ingest_seq mismatch (otherwise identical) → 2 distinct rows."""
    a = _row(node_id="NODE_A", payload_hash="hash_x", ingest_seq=1)
    b = _row(node_id="NODE_B", payload_hash="hash_x", ingest_seq=2)
    result = deduplicate_ticks_v1_1([a, b], multi_node=True)
    assert len(result.emitted) == 2
    assert result.dup_skip_count == 0
