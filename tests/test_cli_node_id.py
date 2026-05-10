"""Unit tests for _resolve_node_id priority: --node-id > env var > hostname. MCT-129."""
from __future__ import annotations

import pytest


def test_explicit_arg_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MCTRADER_NODE_ID", "ENV_NODE")
    from mctrader_data.cli import _resolve_node_id
    assert _resolve_node_id("CLI_NODE") == "CLI_NODE"


def test_env_var_wins_over_hostname(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MCTRADER_NODE_ID", raising=False)
    monkeypatch.setenv("MCTRADER_NODE_ID", "ENV_NODE")
    from mctrader_data.cli import _resolve_node_id
    assert _resolve_node_id(None) == "ENV_NODE"


def test_hostname_fallback_when_no_env(monkeypatch: pytest.MonkeyPatch) -> None:
    import socket
    monkeypatch.delenv("MCTRADER_NODE_ID", raising=False)
    from mctrader_data.cli import _resolve_node_id
    assert _resolve_node_id(None) == socket.gethostname()
