"""Tests for collector.py (MCT-58 — daemon-level)."""

from __future__ import annotations

from pathlib import Path

import pytest

from mctrader_data.collector import (
    CollectorDaemon,
    _default_snapshot_id,
)
from mctrader_market.types import Symbol


def test_default_snapshot_id_is_deterministic_per_day() -> None:
    a = _default_snapshot_id("bithumb", Symbol.from_string("KRW-BTC"))
    b = _default_snapshot_id("bithumb", Symbol.from_string("KRW-BTC"))
    assert a == b
    assert len(a) == 16


def test_default_snapshot_id_differs_per_symbol() -> None:
    a = _default_snapshot_id("bithumb", Symbol.from_string("KRW-BTC"))
    b = _default_snapshot_id("bithumb", Symbol.from_string("KRW-ETH"))
    assert a != b


def test_collector_rejects_unknown_exchange(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="bithumb"):
        CollectorDaemon(
            root=tmp_path, exchange="upbit",
            symbol=Symbol.from_string("KRW-BTC"),
        )


@pytest.mark.asyncio
async def test_collector_run_no_channels_raises(tmp_path: Path) -> None:
    d = CollectorDaemon(
        root=tmp_path, exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
        include_transactions=False, include_orderbook=False,
    )
    with pytest.raises(ValueError, match="at least one"):
        await d.run()


@pytest.mark.asyncio
async def test_collector_cancel_event_triggers_exit(tmp_path: Path, monkeypatch) -> None:
    """Cancel-before-run should not raise; run-then-cancel exits gracefully."""
    d = CollectorDaemon(
        root=tmp_path, exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
    )

    # Pre-set cancel event so the inner run loop exits immediately when WS opens.
    # We monkeypatch BithumbWebSocketStream to a stub that yields nothing.
    class _StubStream:
        def __init__(self, **kwargs):  # noqa: ARG002
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def messages(self):
            if False:
                yield None  # async generator type hint, never runs

    monkeypatch.setattr(
        "mctrader_data.collector.BithumbWebSocketStream", _StubStream,
    )

    await d.cancel()
    await d.run()  # should return cleanly without writing anything
