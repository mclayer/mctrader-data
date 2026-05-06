"""Heartbeat-aware gap cause classifier (MCT-93 X4 of MCT-89).

Codex F-3 PUSH-BACK fix applied — conservative current-state-only.
Full gap classifier with history ring-buffer is a follow-up minor.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from mctrader_data.orderbook_replay import GapEntry


class GapCause(StrEnum):
    LIKELY_NODE_DOWN = "LIKELY_NODE_DOWN"
    UNKNOWN = "UNKNOWN"


def classify_gap(
    gap: GapEntry,
    heartbeats_now: dict[str, dict[str, Any]],
    *,
    fresh_red_seconds: float = 30.0,
) -> GapCause:
    """Conservative current-state-only gap cause classifier.

    Returns LIKELY_NODE_DOWN if any node 의:
      - heartbeat['ws_state'] == 'disconnected', OR
      - (now_wall - heartbeat['now']) >= fresh_red_seconds (stale heartbeat)

    Otherwise UNKNOWN (heartbeat 만으론 단정 불가).

    LIKELY_BITHUMB_OUTAGE 분류는 X4 미도입 (cumulative ws_reconnect_count 의
    history ring-buffer 가 prerequisite — 후속 minor).
    """
    del gap  # gap window 자체는 X4 conservative 정책에서 사용 안 함
    if not heartbeats_now:
        return GapCause.UNKNOWN
    now_wall = datetime.now(timezone.utc)
    for hb in heartbeats_now.values():
        if hb.get("ws_state") == "disconnected":
            return GapCause.LIKELY_NODE_DOWN
        try:
            now_str = hb["now"]
            hb_now = datetime.fromisoformat(now_str.replace("Z", "+00:00"))
            staleness = (now_wall - hb_now).total_seconds()
            if staleness >= fresh_red_seconds:
                return GapCause.LIKELY_NODE_DOWN
        except (KeyError, ValueError, AttributeError):
            continue
    return GapCause.UNKNOWN
