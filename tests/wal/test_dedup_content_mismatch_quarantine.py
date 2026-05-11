# tests/wal/test_dedup_content_mismatch_quarantine.py
"""MCT-140 Story-6 — content-mismatch quarantine emit.

ADR-009 §D10.7/§D10.8 — when two rows share the same 6-tuple body
``(exchange, symbol, ts_utc, price, quantity, side)`` BUT differ on
payload_hash or ingest_seq, the writer must emit a quarantine record
(``reason="ACTIVE_ACTIVE_MISMATCH"``).

This is the v1.1 variant of the existing T2 6-tuple quarantine test in
``tests/test_dedup.py`` — kept in tests/wal/ alongside the WAL-specific fixtures.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from mctrader_data.dedup import deduplicate_ticks_v1_1


@dataclass
class _TickV1_1Row:
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


def _base(**override) -> _TickV1_1Row:
    base = {
        "exchange": "bithumb",
        "symbol": "KRW-BTC",
        "ts_utc": datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc),
        "price": Decimal("100000.000000000000000000"),
        "quantity": Decimal("0.010000000000000000"),
        "side": "BUY",
        "payload_hash": "hash_X",
        "ingest_seq": 1,
        "raw_json": '{"a":1}',
        "node_id": "NODE_A",
        "validation_status": "OK",
    }
    base.update(override)
    return _TickV1_1Row(**base)


def test_payload_hash_mismatch_same_6tuple_quarantines() -> None:
    """6-tuple body identical, payload_hash differs → quarantine."""
    a = _base(node_id="NODE_A", payload_hash="hash_X", ingest_seq=1)
    b = _base(node_id="NODE_B", payload_hash="hash_Y", ingest_seq=1)
    result = deduplicate_ticks_v1_1([a, b], multi_node=True)
    assert result.quarantine_count == 1
    assert len(result.quarantine_records) == 1
    qr = result.quarantine_records[0]
    assert qr.reason == "ACTIVE_ACTIVE_MISMATCH"
    assert qr.tier == "tick"


def test_ingest_seq_mismatch_with_matching_payload_does_not_quarantine() -> None:
    """payload_hash agrees → byte-identical; different ingest_seq merely means
    distinct trade events (separate appends), not a mismatch."""
    a = _base(node_id="NODE_A", payload_hash="hash_X", ingest_seq=1)
    b = _base(node_id="NODE_B", payload_hash="hash_X", ingest_seq=2)
    result = deduplicate_ticks_v1_1([a, b], multi_node=True)
    # 8-tuple differs by ingest_seq → 2 distinct rows, 0 quarantine.
    assert result.quarantine_count == 0
    assert len(result.emitted) == 2


def test_quarantine_records_carry_both_rows() -> None:
    a = _base(node_id="NODE_A", payload_hash="hash_X")
    b = _base(node_id="NODE_B", payload_hash="hash_Y")
    result = deduplicate_ticks_v1_1([a, b], multi_node=True)
    qr = result.quarantine_records[0]
    assert len(qr.rows) == 2
