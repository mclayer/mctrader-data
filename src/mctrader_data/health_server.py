"""HTTP /health endpoint for Docker HEALTHCHECK (Pilot, Amendment 1).

Lightweight stdlib http.server in a daemon thread — zero dep, asyncio-loop-free.
Reads HeartbeatWriter._ws_state + heartbeat staleness for liveness signal:
  - heartbeat_writer is None → 503 unhealthy ("heartbeat unavailable")
  - now - last heartbeat write > MAX_STALE_SECONDS → 503 ("stale_heartbeat")
  - ws_state in {connected, reconnecting} → 200 ok
  - ws_state == disconnected → 503 unhealthy

MAX_STALE_SECONDS is configurable via MCTRADER_HEALTH_STALE_SECONDS env var (default 60).
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

DEFAULT_PORT = 8080
DEFAULT_MAX_STALE_SECONDS = 60
HEALTHY_WS_STATES = frozenset({"connected", "reconnecting"})


def resolve_max_stale_seconds() -> int:
    """Read MCTRADER_HEALTH_STALE_SECONDS env, default 60. Invalid value falls back."""
    raw = os.environ.get("MCTRADER_HEALTH_STALE_SECONDS")
    if not raw:
        return DEFAULT_MAX_STALE_SECONDS
    try:
        return int(raw)
    except ValueError:
        return DEFAULT_MAX_STALE_SECONDS


def resolve_port() -> int:
    """Read MCTRADER_HEALTH_PORT env, default 8080. Invalid value falls back."""
    raw = os.environ.get("MCTRADER_HEALTH_PORT")
    if not raw:
        return DEFAULT_PORT
    try:
        return int(raw)
    except ValueError:
        return DEFAULT_PORT


def _build_response(
    heartbeat_writer: Any,
    max_stale_seconds: int = DEFAULT_MAX_STALE_SECONDS,
) -> tuple[int, dict[str, Any]]:
    """Return (http_status, body) given heartbeat writer state and staleness.

    Staleness check: reads ``heartbeat_writer.last_heartbeat_ts`` (a
    ``datetime`` in UTC) set by the writer on each successful atomic flush.
    If the timestamp is absent or older than *max_stale_seconds*, the
    endpoint returns 503 with ``reason="stale_heartbeat"`` regardless of
    ``ws_state``.  This catches the case where the writer object exists but
    the collector loop has silently died.
    """
    if heartbeat_writer is None:
        return 503, {"status": "unhealthy", "reason": "heartbeat unavailable"}

    now = datetime.now(timezone.utc)

    # --- Staleness check (primary liveness signal) ---
    last_ts: datetime | None = getattr(heartbeat_writer, "last_heartbeat_ts", None)
    if last_ts is None:
        # Writer exists but has never flushed — treat as stale
        stale_seconds: float = float("inf")
    else:
        stale_seconds = (now - last_ts).total_seconds()

    if stale_seconds > max_stale_seconds:
        return 503, {
            "status": "unhealthy",
            "reason": "stale_heartbeat",
            "stale_seconds": round(stale_seconds, 1) if stale_seconds != float("inf") else None,
            "max_stale_seconds": max_stale_seconds,
        }

    # --- ws_state check (secondary signal) ---
    ws_state = getattr(heartbeat_writer, "ws_state", "unknown")
    node_id = getattr(heartbeat_writer, "node_id", "unknown")
    started_at = getattr(heartbeat_writer, "started_at", None)
    uptime = (
        int((now - started_at).total_seconds())
        if isinstance(started_at, datetime)
        else 0
    )
    body: dict[str, Any] = {
        "ws_state": ws_state,
        "node_id": node_id,
        "uptime_seconds": uptime,
    }
    if ws_state in HEALTHY_WS_STATES:
        body["status"] = "ok"
        return 200, body
    body["status"] = "unhealthy"
    body["reason"] = f"ws_state={ws_state}"
    return 503, body


class _HealthHandler(BaseHTTPRequestHandler):
    """Subclassed per-server with `heartbeat_writer` and `max_stale_seconds` bound at start()."""

    heartbeat_writer: Any = None
    max_stale_seconds: int = DEFAULT_MAX_STALE_SECONDS

    def do_GET(self) -> None:  # noqa: N802 (stdlib API)
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return
        status, body = _build_response(self.heartbeat_writer, self.max_stale_seconds)
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 (stdlib API)
        # Silence default stderr access logs.
        return


class HealthServer:
    """Daemon-thread HTTP server exposing GET /health.

    Lifecycle:
        server = HealthServer(heartbeat_writer=writer)
        server.start()
        ...
        server.stop()
    """

    def __init__(
        self,
        heartbeat_writer: Any | None,
        port: int | None = None,
        max_stale_seconds: int | None = None,
    ):
        self._heartbeat_writer = heartbeat_writer
        self._port = port if port is not None else resolve_port()
        self._max_stale_seconds = (
            max_stale_seconds if max_stale_seconds is not None else resolve_max_stale_seconds()
        )
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return self._port

    def start(self) -> None:
        writer = self._heartbeat_writer
        stale = self._max_stale_seconds

        class _BoundHandler(_HealthHandler):
            heartbeat_writer = writer
            max_stale_seconds = stale

        self._httpd = ThreadingHTTPServer(("0.0.0.0", self._port), _BoundHandler)
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="mctrader-health-server",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._httpd = None
        self._thread = None
