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
        include_transactions=False, include_orderbook=False, include_orderbook_snapshot=False,
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


def test_collector_event_propagates_to_heartbeat_tier_ts(tmp_path: Path) -> None:
    """Codex F-1/F-5 ADOPT — daemon._handle_event 가 heartbeat tier timestamp wiring."""
    from datetime import datetime, timezone
    from decimal import Decimal
    from mctrader_data.heartbeat import HeartbeatWriter
    from mctrader_market_bithumb.ws_events import TransactionEvent

    hb = HeartbeatWriter(root=tmp_path, node_id="NODE_A", interval_seconds=5.0)
    d = CollectorDaemon(
        root=tmp_path, exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
        node_id="NODE_A",
        collector_run_id="NODE_A-20260505T120000Z",
        heartbeat_writer=hb,
    )
    # Manually wire up writers (run() 거치지 않고 _handle_event 만 검증)
    from mctrader_data.tick_storage import TickWriter
    d._tick_writer = TickWriter(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC", snapshot_id="ignored",
        node_id="NODE_A", collector_run_id="NODE_A-20260505T120000Z",
    )

    event_ts = datetime(2026, 5, 5, 12, 0, 0, tzinfo=timezone.utc)
    sym = Symbol.from_string("KRW-BTC")
    txn = TransactionEvent(
        exchange="bithumb", symbol=sym,
        event_time=event_ts, received_at=event_ts, raw={"contPrice": "100000000"},
        price=Decimal("100000000"), quantity=Decimal("0.01"),
        side="buy",
    )
    d._handle_event(txn)

    # heartbeat 의 tier timestamp 가 update 됨 (transaction_event_to_record 의 ts_utc = event_time)
    assert hb.last_event_ts_per_tier.get("tick") == event_ts.isoformat()
    d._tick_writer.close()


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


# MCT-104 — orderbooksnapshot channel + §D14 routing tests

def test_collector_daemon_include_orderbook_snapshot_default_true(tmp_path: Path) -> None:
    """include_orderbook_snapshot defaults to True."""
    d = CollectorDaemon(
        root=tmp_path, exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
    )
    assert d._include_orderbook_snapshot is True


def test_collector_daemon_include_orderbook_snapshot_false(tmp_path: Path) -> None:
    d = CollectorDaemon(
        root=tmp_path, exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
        include_orderbook_snapshot=False,
    )
    assert d._include_orderbook_snapshot is False


@pytest.mark.asyncio
async def test_collector_orderbook_snapshot_routes_to_d14_writer(
    tmp_path: Path, monkeypatch
) -> None:
    """OrderbookSnapshotEvent must be routed to §D14 writer, NOT §D11 writer."""
    from decimal import Decimal as _D
    from datetime import datetime, timezone as _tz

    from mctrader_market_bithumb.ws_events import _OrderbookLevel, OrderbookSnapshotEvent

    ts = datetime(2026, 5, 9, 0, 0, 0, tzinfo=_tz.utc)
    snapshot_event = OrderbookSnapshotEvent(
        exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
        event_time=ts,
        received_at=ts,
        bids=[_OrderbookLevel(price=_D("118900000"), quantity=_D("0.1"))],
        asks=[_OrderbookLevel(price=_D("119000000"), quantity=_D("0.1"))],
        raw={},
    )

    d14_written: list = []
    d11_written: list = []

    class _StubStream:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def messages(self):
            yield snapshot_event

    monkeypatch.setattr("mctrader_data.collector.BithumbWebSocketStream", _StubStream)

    daemon = CollectorDaemon(
        root=tmp_path, exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
        include_transactions=False,
        include_orderbook=True,
        include_orderbook_snapshot=True,
        snapshot_id="test-run",
    )
    await daemon.run()

    # §D14 partition must exist
    d14_root = tmp_path / "market" / "orderbook_snapshot"
    d11_root = tmp_path / "market" / "orderbook"
    assert d14_root.exists(), "§D14 partition not created"
    # §D11 orderbook partition should NOT contain snapshot rows (snapshot-only event)
    if d11_root.exists():
        parquets = list(d11_root.rglob("*.parquet"))
        import pyarrow.parquet as pq
        for p in parquets:
            table = pq.read_table(p)
            rows = table.to_pylist()
            for row in rows:
                assert row.get("event_type") != "snapshot", (
                    "§D11 partition must not receive orderbooksnapshot events"
                )
