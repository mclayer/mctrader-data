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


# MCT-91 — node_id + collector_run_id propagation
def test_collector_accepts_node_id_and_collector_run_id(tmp_path: Path) -> None:
    """CollectorDaemon 가 node_id + collector_run_id 인자 받음."""
    d = CollectorDaemon(
        root=tmp_path, exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
        node_id="NODE_A",
        collector_run_id="NODE_A-20260505T120000Z",
    )
    assert d._node_id == "NODE_A"
    assert d._collector_run_id == "NODE_A-20260505T120000Z"


@pytest.mark.asyncio
async def test_multi_collector_heartbeat_writer_lifecycle(tmp_path: Path, monkeypatch) -> None:
    """MultiSymbolCollector 의 heartbeat_writer = task spawn + cancel + final flush."""
    import asyncio
    from mctrader_data.collector import MultiSymbolCollector
    from mctrader_data.heartbeat import HeartbeatWriter

    # heartbeat writer
    hb = HeartbeatWriter(root=tmp_path, node_id="NODE_A", interval_seconds=10.0)

    # Stub daemon — run() 즉시 cancel 하면 종료
    class _StubDaemon:
        def __init__(self):
            self._cancelled = False

        async def run(self):
            try:
                await asyncio.sleep(60)  # blocking 으로 대기
            except asyncio.CancelledError:
                self._cancelled = True
                raise

        async def cancel(self):
            self._cancelled = True

    stub = _StubDaemon()
    collector = MultiSymbolCollector(
        daemons=[stub],  # type: ignore[list-item]
        heartbeat_writer=hb,
    )

    task = asyncio.create_task(collector.run())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task

    # heartbeat task final flush 후 main file 존재
    main_path = tmp_path / "market" / "manifest" / "heartbeat-NODE_A.json"
    assert main_path.exists(), "heartbeat final flush 미작동 — shutdown ordering broken"
