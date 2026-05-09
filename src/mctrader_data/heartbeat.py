"""Atomic heartbeat JSON writer for collector HA active-active.

Contract: docs/domain-knowledge/contracts/heartbeat-schema.v1.md (mctrader-hub)
Path: <root>/market/manifest/heartbeat-{node_id}.json
Atomic write: write-temp -> fsync -> os.replace.

Per MCT-91 (X2 of MCT-89). 5s default interval. Each node writes its own file
(cross-host write contention 0). Consumer reads via read_heartbeat() with
schema_version best-effort parse + warning on mismatch.

MCT-93 (X4 of MCT-89): HeartbeatCounterSink concrete adapter for
DedupCounterSink Protocol — composition + threading.Lock for cross-thread
safety (collector asyncio loop ↔ scan caller sync).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

logger = logging.getLogger(__name__)

HEARTBEAT_SCHEMA_VERSION = "heartbeat.v1"

WsState = Literal["connected", "reconnecting", "disconnected"]
_VALID_WS_STATES = {"connected", "reconnecting", "disconnected"}


@dataclass
class HeartbeatMetrics:
    events_per_sec: float = 0.0
    dup_skip_count: int = 0
    quarantine_count: int = 0
    ws_reconnect_count: int = 0
    backfill_pending_seconds: int = 0


class HeartbeatWriter:
    def __init__(
        self,
        root: Path | str,
        node_id: str,
        interval_seconds: float = 5.0,
        version: str = "unknown",
    ):
        self.root = Path(root)
        self.node_id = node_id
        self.interval = interval_seconds
        self.version = version
        self.started_at = datetime.now(timezone.utc)
        self.collector_run_id: str | None = None
        self._ws_state: WsState = "connected"
        self.last_event_ts_per_tier: dict[str, str] = {}
        self.queue_depth: int = 0
        self.metrics = HeartbeatMetrics()
        self._write_failure_count = 0
        self.last_heartbeat_ts: datetime | None = None

    @property
    def ws_state(self) -> WsState:
        return self._ws_state

    @ws_state.setter
    def ws_state(self, value: str) -> None:
        if value not in _VALID_WS_STATES:
            raise ValueError(
                f"invalid ws_state: {value!r}, must be one of {sorted(_VALID_WS_STATES)}"
            )
        self._ws_state = value  # type: ignore[assignment]

    def set_collector_run_id(self, value: str) -> None:
        self.collector_run_id = value

    def update_tier_event_ts(self, tier: str, ts: datetime) -> None:
        self.last_event_ts_per_tier[tier] = ts.isoformat()

    def _payload(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        return {
            "schema_version": HEARTBEAT_SCHEMA_VERSION,
            "node_id": self.node_id,
            "collector_run_id": self.collector_run_id or "",
            "version": self.version,
            "started_at": self.started_at.isoformat(),
            "now": now.isoformat(),
            "uptime_seconds": int((now - self.started_at).total_seconds()),
            "ws_state": self._ws_state,
            "last_event_ts_per_tier": dict(self.last_event_ts_per_tier),
            "queue_depth": self.queue_depth,
            "metrics": {
                "events_per_sec": self.metrics.events_per_sec,
                "dup_skip_count": self.metrics.dup_skip_count,
                "quarantine_count": self.metrics.quarantine_count,
                "ws_reconnect_count": self.metrics.ws_reconnect_count,
                "backfill_pending_seconds": self.metrics.backfill_pending_seconds,
            },
        }

    def _file_path(self) -> Path:
        return self.root / "market" / "manifest" / f"heartbeat-{self.node_id}.json"

    async def write_once(self) -> None:
        """Write heartbeat artifact atomically. last-good preserved on failure."""
        path = self._file_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + ".tmp")
        try:
            with temp.open("w", encoding="utf-8") as f:
                json.dump(self._payload(), f, ensure_ascii=False, indent=None)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp, path)
            self.last_heartbeat_ts = datetime.now(timezone.utc)
        except OSError as exc:
            self._write_failure_count += 1
            logger.warning(
                "heartbeat write failed for node=%s (last-good preserved, failure_count=%d): %s",
                self.node_id, self._write_failure_count, exc,
            )
            if temp.exists():
                with contextlib.suppress(OSError):
                    temp.unlink()

    async def run(self) -> None:
        """5s loop until cancelled. Final atomic write on cancel."""
        try:
            while True:
                await self.write_once()
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            await self.write_once()
            raise


def read_heartbeat(root: Path | str, node_id: str) -> dict[str, Any]:
    """Consumer-side read with schema_version best-effort parse + mismatch warning."""
    path = Path(root) / "market" / "manifest" / f"heartbeat-{node_id}.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != HEARTBEAT_SCHEMA_VERSION:
        logger.warning(
            "heartbeat schema_version mismatch for node=%s: got %r (expected %r)",
            node_id, data.get("schema_version"), HEARTBEAT_SCHEMA_VERSION,
        )
    return data


class HeartbeatCounterSink:
    """DedupCounterSink Protocol concrete impl, wrapping HeartbeatWriter.

    MCT-93 X4 — composition (not inheritance) over HeartbeatWriter.
    threading.Lock chosen (not asyncio.Lock) because dedup callers are
    synchronous and may run outside the collector's asyncio event loop.
    """

    def __init__(self, writer: HeartbeatWriter):
        self._writer = writer
        self._lock = threading.Lock()

    def increment_dup_skip(self, n: int = 1) -> None:
        with self._lock:
            self._writer.metrics.dup_skip_count += n

    def increment_quarantine(self, n: int = 1) -> None:
        with self._lock:
            self._writer.metrics.quarantine_count += n
