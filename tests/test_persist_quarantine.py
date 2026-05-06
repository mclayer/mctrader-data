"""Tests for dedup.persist_quarantine_records (MCT-93 X4)."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from mctrader_data.dedup import QuarantineRecord, persist_quarantine_records


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 5, 6, 12, 34, 56, tzinfo=timezone.utc)


def test_persist_quarantine_records_atomic_write(tmp_path: Path) -> None:
    record = QuarantineRecord(
        reason="ACTIVE_ACTIVE_MISMATCH",
        tier="tick",
        logical_key=("bithumb", "KRW-BTC", _ts(0), Decimal("100"), Decimal("0.01"), "buy"),
        rows=[],
        detected_at=_ts(0),
    )
    paths = persist_quarantine_records(tmp_path, [record])
    assert len(paths) == 1
    assert paths[0].exists()
    assert paths[0].parent == tmp_path / "market" / "manifest" / "quarantine"
    payload = json.loads(paths[0].read_text(encoding="utf-8"))
    assert payload["tier"] == "tick"
    assert payload["count"] == 1
    assert len(payload["records"]) == 1
    assert payload["records"][0]["reason"] == "ACTIVE_ACTIVE_MISMATCH"


def test_persist_quarantine_records_path_format(tmp_path: Path) -> None:
    record = QuarantineRecord(
        reason="ACTIVE_ACTIVE_MISMATCH",
        tier="orderbook",
        logical_key=(),
        rows=[],
        detected_at=_ts(0),
    )
    paths = persist_quarantine_records(tmp_path, [record])
    name = paths[0].name
    assert name.startswith("orderbook-")
    assert name.endswith(".json")
    assert re.match(r"orderbook-\d{8}T\d{6}Z-\d{6}\.json", name)


def test_persist_quarantine_records_decimal_serialization(tmp_path: Path) -> None:
    record = QuarantineRecord(
        reason="ACTIVE_ACTIVE_MISMATCH",
        tier="tick",
        logical_key=("bithumb", "KRW-BTC", _ts(0), Decimal("100.5"), Decimal("0.01"), "buy"),
        rows=[],
        detected_at=_ts(0),
    )
    paths = persist_quarantine_records(tmp_path, [record])
    payload = json.loads(paths[0].read_text(encoding="utf-8"))
    logical_key_str = " ".join(payload["records"][0]["logical_key"])
    assert "100.5" in logical_key_str


def test_persist_quarantine_records_empty_list(tmp_path: Path) -> None:
    paths = persist_quarantine_records(tmp_path, [])
    assert paths == []
    # 디렉토리도 생성되지 않음 (no-op fast path)
    assert not (tmp_path / "market" / "manifest" / "quarantine").exists()


def test_persist_quarantine_records_collision_increments_seq(tmp_path: Path) -> None:
    record = QuarantineRecord(
        reason="ACTIVE_ACTIVE_MISMATCH",
        tier="tick",
        logical_key=(),
        rows=[],
        detected_at=_ts(0),
    )
    p1 = persist_quarantine_records(tmp_path, [record])
    p2 = persist_quarantine_records(tmp_path, [record])
    assert p1[0] != p2[0]  # different batch_seq
    assert "000000.json" in p1[0].name
    assert "000001.json" in p2[0].name


def test_persist_quarantine_records_groups_by_tier(tmp_path: Path) -> None:
    r_tick = QuarantineRecord(
        reason="ACTIVE_ACTIVE_MISMATCH", tier="tick",
        logical_key=(), rows=[], detected_at=_ts(0),
    )
    r_ob = QuarantineRecord(
        reason="ACTIVE_ACTIVE_MISMATCH", tier="orderbook",
        logical_key=(), rows=[], detected_at=_ts(0),
    )
    paths = persist_quarantine_records(tmp_path, [r_tick, r_ob])
    assert len(paths) == 2
    names = {p.name for p in paths}
    assert any(n.startswith("tick-") for n in names)
    assert any(n.startswith("orderbook-") for n in names)
