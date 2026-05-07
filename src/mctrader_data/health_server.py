"""HTTP /health endpoint for Docker HEALTHCHECK (Pilot, Amendment 1).

Lightweight stdlib http.server in a daemon thread — zero dep, asyncio-loop-free.
Reads HeartbeatWriter._ws_state for liveness signal:
  - heartbeat_writer is None → 503 unhealthy ("heartbeat unavailable")
  - ws_state in {connected, reconnecting} → 200 ok
  - ws_state == disconnected → 503 unhealthy
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

DEFAULT_PORT = 8080
HEALTHY_WS_STATES = frozenset({"connected", "reconnecting"})


def resolve_port() -> int:
    """Read MCTRADER_HEALTH_PORT env, default 8080. Invalid value falls back."""
    raw = os.environ.get("MCTRADER_HEALTH_PORT")
    if not raw:
        return DEFAULT_PORT
    try:
        return int(raw)
    except ValueError:
        return DEFAULT_PORT


def _build_response(heartbeat_writer: Any) -> tuple[int, dict[str, Any]]:
    """Return (http_status, body) given heartbeat writer state."""
    if heartbeat_writer is None:
        return 503, {"status": "unhealthy", "reason": "heartbeat unavailable"}
    ws_state = getattr(heartbeat_writer, "ws_state", "unknown")
    node_id = getattr(heartbeat_writer, "node_id", "unknown")
    started_at = getattr(heartbeat_writer, "started_at", None)
    uptime = (
        int((datetime.now(timezone.utc) - started_at).total_seconds())
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
    """Subclassed per-server with `heartbeat_writer` bound at start()."""

    heartbeat_writer: Any = None

    def do_GET(self) -> None:  # noqa: N802 (stdlib API)
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return
        status, body = _build_response(self.heartbeat_writer)
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

    def __init__(self, heartbeat_writer: Any | None, port: int | None = None):
        self._heartbeat_writer = heartbeat_writer
        self._port = port if port is not None else resolve_port()
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return self._port

    def start(self) -> None:
        writer = self._heartbeat_writer

        class _BoundHandler(_HealthHandler):
            heartbeat_writer = writer

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
