"""Forward-only WebSocket collector daemon (MCT-58).

Subscribes to Bithumb public WebSocket for N symbols × {transaction, orderbook}
and persists every event as Parquet append-only via :class:`TickWriter` and
:class:`OrderbookWriter`. Designed for 24/7 systemd operation.

Lifecycle:

1. ``CollectorDaemon.__init__`` — store config (no I/O)
2. ``await run()`` — open WS, subscribe, write events until cancel
3. SIGTERM handling = caller (systemd ExecStop) cancels the asyncio.Task

Symbol selection:

* explicit ``--symbols KRW-BTC,KRW-ETH,...`` list
* OR ``--top-n 10`` queries Bithumb public ticker at startup, sorts by 24h volume
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

from mctrader_market.types import Symbol
from mctrader_market_bithumb.ws_client import BithumbWebSocketStream
from mctrader_market_bithumb.ws_events import (
    OrderbookDeltaEvent,
    OrderbookSnapshotEvent,
    TransactionEvent,
)

from mctrader_data.lineage import write_lineage
from mctrader_data.manifest import CollectorManifest
from mctrader_data.orderbook_storage import (
    OrderbookWriter,
    delta_event_to_records,
    snapshot_event_to_records,
)
from mctrader_data.tick_storage import TickWriter, transaction_event_to_record

log = logging.getLogger(__name__)


class CollectorDaemon:
    """One symbol = one CollectorDaemon (independent WS + writers)."""

    def __init__(
        self,
        *,
        root: Path,
        exchange: str,
        symbol: Symbol,
        include_transactions: bool = True,
        include_orderbook: bool = True,
        snapshot_id: str | None = None,
        node_id: str | None = None,
        collector_run_id: str | None = None,
    ) -> None:
        if exchange != "bithumb":
            raise ValueError(f"only 'bithumb' exchange supported in v1, got {exchange!r}")
        self._root = root
        self._exchange = exchange
        self._symbol = symbol
        self._include_transactions = include_transactions
        self._include_orderbook = include_orderbook
        self._snapshot_id = snapshot_id or _default_snapshot_id(exchange, symbol)
        self._node_id = node_id
        self._collector_run_id = collector_run_id
        self._tick_writer: TickWriter | None = None
        self._ob_writer: OrderbookWriter | None = None
        self._cancel_event = asyncio.Event()

    async def run(self) -> None:
        from mctrader_market_bithumb.ws_subscribe import Channel

        channels: list[Channel] = []
        if self._include_transactions:
            channels.append("transaction")
        if self._include_orderbook:
            channels.append("orderbookdepth")
        if not channels:
            raise ValueError("at least one of transactions/orderbook must be included")

        self._tick_writer = TickWriter(
            root=self._root, exchange=self._exchange,
            symbol=str(self._symbol), snapshot_id=self._snapshot_id,
            node_id=self._node_id, collector_run_id=self._collector_run_id,
        ) if self._include_transactions else None

        self._ob_writer = OrderbookWriter(
            root=self._root, exchange=self._exchange,
            symbol=str(self._symbol), snapshot_id=self._snapshot_id,
            node_id=self._node_id, collector_run_id=self._collector_run_id,
        ) if self._include_orderbook else None

        log.info("[collector] symbol=%s channels=%s root=%s", self._symbol, channels, self._root)

        stream = BithumbWebSocketStream(symbol=self._symbol, channels=channels)
        try:
            async with stream:
                async for event in stream.messages():
                    if self._cancel_event.is_set():
                        break
                    self._handle_event(event)
        except asyncio.CancelledError:
            log.info("[collector] cancelled — flushing buffers")
            raise
        finally:
            self._finalize()

    async def cancel(self) -> None:
        self._cancel_event.set()

    def _handle_event(self, event) -> None:  # type: ignore[no-untyped-def]
        if isinstance(event, TransactionEvent) and self._tick_writer is not None:
            self._tick_writer.append(transaction_event_to_record(event))
        elif isinstance(event, OrderbookSnapshotEvent) and self._ob_writer is not None:
            self._ob_writer.append_many(snapshot_event_to_records(event))
        elif isinstance(event, OrderbookDeltaEvent) and self._ob_writer is not None:
            self._ob_writer.append_many(delta_event_to_records(event))
        # TickerEvent ignored — diagnostic only, not persisted

    def _finalize(self) -> None:
        try:
            if self._tick_writer is not None:
                self._tick_writer.close()
                if self._tick_writer.current_path is not None:
                    self._write_lineage(self._tick_writer.current_path, "tick")
        except Exception:
            log.exception("[collector] tick writer close failed")
        try:
            if self._ob_writer is not None:
                self._ob_writer.close()
                if self._ob_writer.current_path is not None:
                    self._write_lineage(self._ob_writer.current_path, "orderbook")
        except Exception:
            log.exception("[collector] orderbook writer close failed")

    def _write_lineage(self, parquet_path: Path, kind: str) -> None:
        partition = parquet_path.parent
        try:
            write_lineage(
                partition_dir=partition,
                snapshot_id=self._snapshot_id,
                exchange=self._exchange,
                endpoint="wss://pubwss.bithumb.com/pub/ws",
                request_params_hash=hashlib.sha256(
                    f"{self._symbol}|{kind}|collector".encode()
                ).hexdigest(),
                fetched_at_utc=datetime.now(timezone.utc),
                response_hash="forward-only-stream",
                adapter_name="mctrader-market-bithumb-ws",
                adapter_version="0.3.0",
                node_id=self._node_id,
            )
        except Exception:
            log.exception("[collector] lineage write failed for %s", parquet_path)


class MultiSymbolCollector:
    """Run N :class:`CollectorDaemon` concurrently (one per symbol).

    On startup, persists a :class:`CollectorManifest` under
    ``<root>/market/manifest/run-{collector_run_id}.json`` (MCT-65, F-21).

    MCT-91 — HA active-active 지원: ``heartbeat_writer`` 인자 명시 시 별도 async task 로
    spawn 하고 main collector task 종료 시 cancel + final atomic flush 보장.
    """

    def __init__(
        self,
        daemons: list[CollectorDaemon],
        *,
        manifest: CollectorManifest | None = None,
        manifest_root: Path | None = None,
        heartbeat_writer: object | None = None,
    ) -> None:
        self._daemons = daemons
        self._manifest = manifest
        self._manifest_root = manifest_root
        self._heartbeat_writer = heartbeat_writer

    async def run(self) -> None:
        if self._manifest is not None and self._manifest_root is not None:
            from mctrader_data.manifest import write_manifest

            write_manifest(self._manifest_root, self._manifest)
            log.info(
                "[collector] manifest persisted: run_id=%s symbols=%d",
                self._manifest.collector_run_id,
                len(self._manifest.selected_symbols),
            )

        # MCT-91 — heartbeat task spawn
        heartbeat_task: asyncio.Task[None] | None = None
        if self._heartbeat_writer is not None:
            if self._manifest is not None:
                self._heartbeat_writer.set_collector_run_id(  # type: ignore[attr-defined]
                    self._manifest.collector_run_id
                )
            heartbeat_task = asyncio.create_task(
                self._heartbeat_writer.run()  # type: ignore[attr-defined]
            )
            log.info("[collector] heartbeat task spawned")

        tasks = [asyncio.create_task(d.run()) for d in self._daemons]
        try:
            await asyncio.gather(*tasks, return_exceptions=False)
        except (asyncio.CancelledError, KeyboardInterrupt):
            log.info("[collector] aggregate cancel — propagating to children")
            for d in self._daemons:
                await d.cancel()
            for t in tasks:
                t.cancel()
            for t in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t
            raise
        finally:
            # MCT-91 — heartbeat task graceful shutdown (cancel + final atomic flush)
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await heartbeat_task
                log.info("[collector] heartbeat task shutdown complete")


async def fetch_top_n_krw_symbols(n: int = 10) -> list[Symbol]:
    """Query Bithumb public ticker, sort by 24h volume, return top N KRW pairs.

    Uses ``GET https://api.bithumb.com/public/ticker/ALL_KRW``.
    """
    import httpx

    url = "https://api.bithumb.com/public/ticker/ALL_KRW"
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url)
        r.raise_for_status()
        payload = r.json()

    if payload.get("status") != "0000":
        raise RuntimeError(f"Bithumb ticker API error: status={payload.get('status')}")

    data = payload["data"]
    rows = []
    for code, ticker in data.items():
        if code == "date":
            continue
        try:
            volume = float(ticker.get("acc_trade_value_24H", 0))
        except (ValueError, TypeError):
            continue
        rows.append((code, volume))

    rows.sort(key=lambda x: x[1], reverse=True)
    top = rows[:n]
    return [Symbol.from_string(f"KRW-{code}") for code, _ in top]


def _default_snapshot_id(exchange: str, symbol: Symbol) -> str:
    """Deterministic per-(exchange, symbol, startup-day) snapshot id."""
    today = datetime.now(timezone.utc).date().isoformat()
    return hashlib.sha256(
        f"{exchange}|{symbol}|{today}|collector".encode()
    ).hexdigest()[:16]
