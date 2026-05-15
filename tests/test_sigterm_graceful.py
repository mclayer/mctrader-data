"""Tests for SIGTERM graceful shutdown handler (MCT-176 / ADR-030 §D4)."""

from __future__ import annotations

import signal

import pytest

import mctrader_data.cli as cli_module
from mctrader_data.cli import _register_signal_handlers, _sigterm_handler


def test_sigterm_handler_sets_shutdown_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calling _sigterm_handler directly sets _SHUTDOWN_REQUESTED to True."""
    monkeypatch.setattr(cli_module, "_SHUTDOWN_REQUESTED", False)
    assert cli_module._SHUTDOWN_REQUESTED is False

    _sigterm_handler(signal.SIGTERM, None)

    assert cli_module._SHUTDOWN_REQUESTED is True


def test_sigint_handler_sets_shutdown_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calling _sigterm_handler with SIGINT sets _SHUTDOWN_REQUESTED to True."""
    monkeypatch.setattr(cli_module, "_SHUTDOWN_REQUESTED", False)

    _sigterm_handler(signal.SIGINT, None)

    assert cli_module._SHUTDOWN_REQUESTED is True


def test_register_signal_handlers_registers_sigterm(monkeypatch: pytest.MonkeyPatch) -> None:
    """_register_signal_handlers installs _sigterm_handler for SIGTERM."""
    monkeypatch.setattr(cli_module, "_SHUTDOWN_REQUESTED", False)
    _register_signal_handlers()

    # Verify the handler is installed by querying signal.getsignal
    installed = signal.getsignal(signal.SIGTERM)
    assert installed is _sigterm_handler


def test_register_signal_handlers_registers_sigint(monkeypatch: pytest.MonkeyPatch) -> None:
    """_register_signal_handlers installs _sigterm_handler for SIGINT."""
    monkeypatch.setattr(cli_module, "_SHUTDOWN_REQUESTED", False)
    _register_signal_handlers()

    installed = signal.getsignal(signal.SIGINT)
    assert installed is _sigterm_handler


def test_register_signal_handlers_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Calling _register_signal_handlers multiple times does not raise."""
    monkeypatch.setattr(cli_module, "_SHUTDOWN_REQUESTED", False)
    _register_signal_handlers()
    _register_signal_handlers()  # second call must not raise

    # Handler must still be correct after second registration
    assert signal.getsignal(signal.SIGTERM) is _sigterm_handler
