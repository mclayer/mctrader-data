"""Tests for diagnostic.classify_gap (MCT-93 X4)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from mctrader_data.diagnostic import GapCause, classify_gap
from mctrader_data.orderbook_replay import GapEntry


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 5, 6, 0, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def _gap(seconds: int = 700) -> GapEntry:
    return GapEntry(after_ts=_ts(0), before_ts=_ts(seconds), gap_seconds=float(seconds))


def test_classify_gap_likely_node_down_disconnected() -> None:
    now = datetime.now(timezone.utc)
    heartbeats = {
        "NODE_A": {"now": now.isoformat(), "ws_state": "connected"},
        "NODE_B": {"now": now.isoformat(), "ws_state": "disconnected"},
    }
    assert classify_gap(_gap(), heartbeats) == GapCause.LIKELY_NODE_DOWN


def test_classify_gap_likely_node_down_stale_heartbeat() -> None:
    now = datetime.now(timezone.utc)
    heartbeats = {
        "NODE_A": {"now": now.isoformat(), "ws_state": "connected"},
        "NODE_B": {
            "now": (now - timedelta(seconds=60)).isoformat(),
            "ws_state": "connected",
        },
    }
    assert classify_gap(_gap(), heartbeats) == GapCause.LIKELY_NODE_DOWN


def test_classify_gap_unknown_all_connected_fresh() -> None:
    now = datetime.now(timezone.utc)
    heartbeats = {
        "NODE_A": {"now": now.isoformat(), "ws_state": "connected"},
        "NODE_B": {"now": now.isoformat(), "ws_state": "connected"},
    }
    assert classify_gap(_gap(), heartbeats) == GapCause.UNKNOWN


def test_classify_gap_no_heartbeats_unknown() -> None:
    assert classify_gap(_gap(), {}) == GapCause.UNKNOWN


def test_classify_gap_threshold_override() -> None:
    """fresh_red_seconds 5 → 6s 도 stale."""
    now = datetime.now(timezone.utc)
    heartbeats = {
        "NODE_A": {
            "now": (now - timedelta(seconds=6)).isoformat(),
            "ws_state": "connected",
        },
    }
    assert classify_gap(_gap(), heartbeats, fresh_red_seconds=5.0) == GapCause.LIKELY_NODE_DOWN
    assert classify_gap(_gap(), heartbeats, fresh_red_seconds=10.0) == GapCause.UNKNOWN


def test_classify_gap_malformed_heartbeat_skipped() -> None:
    """malformed `now` field — single node skipped, others 정상 → UNKNOWN."""
    now = datetime.now(timezone.utc)
    heartbeats = {
        "NODE_A": {"now": "not-a-date", "ws_state": "connected"},
        "NODE_B": {"now": now.isoformat(), "ws_state": "connected"},
    }
    assert classify_gap(_gap(), heartbeats) == GapCause.UNKNOWN
