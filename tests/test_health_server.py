"""TDD for HealthServer HTTP /health endpoint (Pilot, Amendment 1).

4 scenarios:
1. heartbeat_writer is None → 503 + JSON {"status":"unhealthy","reason":"heartbeat unavailable"}
2. ws_state="connected" → 200 + JSON {"status":"ok","ws_state":"connected", ...}
3. ws_state="disconnected" → 503 + JSON {"status":"unhealthy","reason":"ws_state=disconnected", ...}
4. resolve_port() reads MCTRADER_HEALTH_PORT env (default 8080)
"""
from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from mctrader_data.health_server import HealthServer, resolve_port
from mctrader_data.heartbeat import HeartbeatWriter


def _free_port() -> int:
    """Allocate an ephemeral port for test isolation."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _get_health(port: int) -> tuple[int, dict[str, Any]]:
    """GET /health, return (status_code, body_json) for both 2xx and 5xx."""
    req = urllib.request.Request(f"http://127.0.0.1:{port}/health")
    try:
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def test_health_503_when_heartbeat_writer_missing() -> None:
    port = _free_port()
    server = HealthServer(heartbeat_writer=None, port=port)
    server.start()
    try:
        time.sleep(0.2)  # allow bind
        code, body = _get_health(port)
        assert code == 503
        assert body["status"] == "unhealthy"
        assert "heartbeat unavailable" in body["reason"]
    finally:
        server.stop()


def test_health_200_when_ws_connected(tmp_path: Path) -> None:
    writer = HeartbeatWriter(root=tmp_path, node_id="test-node")
    writer.ws_state = "connected"
    port = _free_port()
    server = HealthServer(heartbeat_writer=writer, port=port)
    server.start()
    try:
        time.sleep(0.2)
        code, body = _get_health(port)
        assert code == 200
        assert body["status"] == "ok"
        assert body["ws_state"] == "connected"
        assert body["node_id"] == "test-node"
    finally:
        server.stop()


def test_health_503_when_ws_disconnected(tmp_path: Path) -> None:
    writer = HeartbeatWriter(root=tmp_path, node_id="test-node")
    writer.ws_state = "disconnected"
    port = _free_port()
    server = HealthServer(heartbeat_writer=writer, port=port)
    server.start()
    try:
        time.sleep(0.2)
        code, body = _get_health(port)
        assert code == 503
        assert body["status"] == "unhealthy"
        assert "ws_state=disconnected" in body["reason"]
    finally:
        server.stop()


def test_resolve_port_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """resolve_port() reads MCTRADER_HEALTH_PORT env, default 8080."""
    monkeypatch.setenv("MCTRADER_HEALTH_PORT", "9090")
    assert resolve_port() == 9090
    monkeypatch.delenv("MCTRADER_HEALTH_PORT")
    assert resolve_port() == 8080
    # invalid value falls back to default
    monkeypatch.setenv("MCTRADER_HEALTH_PORT", "not-an-int")
    assert resolve_port() == 8080
