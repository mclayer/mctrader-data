"""Upbit collector -> WAL 통합 테스트.

실제 Upbit WebSocket 연결 없이 adapters.get_ws_stream을 mock해
CollectorDaemon -> WalIngester 파이프라인만 검증.
"""
import asyncio
import contextlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from mctrader_market.types import Symbol

from mctrader_data.collector import CollectorDaemon

BTC_KRW = Symbol(base="BTC", quote="KRW")


@pytest.fixture
def tmp_root(tmp_path: Path) -> Path:
    return tmp_path


@pytest.mark.asyncio
async def test_upbit_collector_writes_to_wal(tmp_root: Path) -> None:
    """Upbit stream trade 이벤트가 transaction WAL에 기록되는지 확인."""
    from mctrader_market_upbit.ws_mapping import normalize_message

    now = datetime.now(tz=timezone.utc)
    raw = {
        "type": "trade",
        "code": "KRW-BTC",
        "trade_price": 55000000.0,
        "trade_volume": 0.001,
        "ask_bid": "BID",
        "trade_timestamp": int(now.timestamp() * 1000),
    }
    trade_event = normalize_message(raw, now)
    assert trade_event is not None, "normalize_message returned None"

    async def fake_messages():
        yield trade_event

    mock_stream = AsyncMock()
    mock_stream.__aenter__ = AsyncMock(return_value=mock_stream)
    mock_stream.__aexit__ = AsyncMock(return_value=None)
    mock_stream.messages = fake_messages

    with patch("mctrader_data.adapters.get_ws_stream", return_value=mock_stream):
        daemon = CollectorDaemon(
            root=tmp_root,
            exchange="upbit",
            symbol=BTC_KRW,
            include_transactions=True,
            include_orderbook=False,
            include_orderbook_snapshot=False,
        )

        async def run_and_stop() -> None:
            task = asyncio.create_task(daemon.run())
            await asyncio.sleep(0.1)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        await run_and_stop()

    wal_root = tmp_root / "wal" / "upbit"
    assert wal_root.exists(), f"WAL directory not created: {wal_root}"

    # WAL 파일은 .ndjson 또는 .ndjson.sealed 확장자
    wal_files = list(wal_root.rglob("*.ndjson*"))
    assert len(wal_files) > 0, f"No WAL files written. Directory contents: {list(wal_root.rglob('*'))}"


@pytest.mark.asyncio
async def test_upbit_collector_no_orderbookdepth_wal(tmp_root: Path) -> None:
    """Upbit는 include_orderbook=True여도 orderbookdepth WAL ingester를 생성하지 않아야 한다."""
    daemon = CollectorDaemon(
        root=tmp_root,
        exchange="upbit",
        symbol=BTC_KRW,
        include_transactions=True,
        include_orderbook=True,   # Upbit에서는 orderbookdepth WAL 생성 안 함
        include_orderbook_snapshot=True,
    )
    assert "orderbookdepth" not in daemon._wal_ingesters
    assert "orderbooksnapshot" in daemon._wal_ingesters
