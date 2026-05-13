"""Forward-only WebSocket collector daemon (MCT-58, MCT-106).

Subscribes to Bithumb public WebSocket for N symbols × {transaction, orderbook}
and persists every event to a WAL (Write-Ahead Log) via :class:`WalIngester`.
Designed for 24/7 systemd operation.

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
import json
import logging
import os
import socket
from datetime import datetime, timezone
from pathlib import Path

from mctrader_market.types import Symbol

from mctrader_data import adapters
from mctrader_data.manifest import CollectorManifest
from mctrader_data.wal.ingester import WalIngester

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
        include_orderbook_snapshot: bool = True,
        snapshot_id: str | None = None,
        node_id: str | None = None,
        collector_run_id: str | None = None,
        heartbeat_writer: object | None = None,
        coverage_stats_writer: object | None = None,
        redis_publisher: object | None = None,
    ) -> None:
        self._root = root
        self._exchange = exchange
        self._symbol = symbol
        self._include_transactions = include_transactions
        self._include_orderbook = include_orderbook
        self._include_orderbook_snapshot = include_orderbook_snapshot
        self._snapshot_id = snapshot_id or _default_snapshot_id(exchange, symbol)
        self._node_id = node_id
        self._collector_run_id = collector_run_id
        self._heartbeat_writer = heartbeat_writer
        self._coverage_stats_writer = coverage_stats_writer
        self._redis_publisher = redis_publisher
        self._resolved_node_id = node_id or os.environ.get("MCTRADER_NODE_ID") or socket.gethostname()
        self._wal_ingesters: dict[str, WalIngester] = self._build_ingesters()
        self._cancel_event = asyncio.Event()

    def _build_ingesters(self) -> dict[str, WalIngester]:
        ingesters: dict[str, WalIngester] = {}
        if self._include_transactions:
            ingesters["transaction"] = WalIngester(
                root=self._root, exchange=self._exchange, symbol=str(self._symbol),
                channel="transaction", node_id=self._resolved_node_id,
            )
        if self._include_orderbook and self._exchange == "bithumb":
            ingesters["orderbookdepth"] = WalIngester(
                root=self._root, exchange=self._exchange, symbol=str(self._symbol),
                channel="orderbookdepth", node_id=self._resolved_node_id,
            )
        if self._include_orderbook_snapshot:
            ingesters["orderbooksnapshot"] = WalIngester(
                root=self._root, exchange=self._exchange, symbol=str(self._symbol),
                channel="orderbooksnapshot", node_id=self._resolved_node_id,
            )
        return ingesters

    async def run(self) -> None:
        if self._wal_ingesters and any(i._closed for i in self._wal_ingesters.values()):
            self._wal_ingesters = self._build_ingesters()
        log.info("[collector] exchange=%s symbol=%s root=%s", self._exchange, self._symbol, self._root)

        stream = adapters.get_ws_stream(
            self._exchange, self._symbol,
            include_transactions=self._include_transactions,
            include_orderbook=self._include_orderbook,
            include_orderbook_snapshot=self._include_orderbook_snapshot,
        )
        try:
            async with stream:  # type: ignore[attr-defined]
                async for event in stream.messages():  # type: ignore[attr-defined]
                    if self._cancel_event.is_set():
                        break
                    self._emit_to_wal(event)
        except asyncio.CancelledError:
            log.info("[collector] cancelled — flushing buffers")
            raise
        finally:
            for channel, ingester in self._wal_ingesters.items():
                try:
                    ingester.close()
                except Exception:
                    log.exception("[collector] wal ingester close failed channel=%s", channel)

    async def cancel(self) -> None:
        self._cancel_event.set()

    def _emit_to_wal(self, event) -> None:  # type: ignore[no-untyped-def]
        raw = getattr(event, "raw", None)
        if event.kind == "transaction":
            ingester = self._wal_ingesters.get("transaction")
            if ingester is not None:
                record = {
                    "ts_utc": event.event_time,
                    "received_at": event.received_at,
                    "exchange": self._exchange,
                    "symbol": str(self._symbol),
                    "price": event.price,
                    "quantity": event.quantity,
                    "side": event.side,
                    "raw_json": json.dumps(raw, ensure_ascii=False) if raw else None,
                    "channel": "transaction",
                }
                ingester.append(record)
                from mctrader_data.metrics import record_ingester_event
                record_ingester_event(exchange=self._exchange, symbol=str(event.symbol), channel="transaction")
                if self._redis_publisher is not None:
                    self._redis_publisher.publish_transaction(  # type: ignore[attr-defined]
                        exchange=self._exchange,
                        symbol=str(event.symbol),
                        record=record,
                    )
                if self._heartbeat_writer is not None:
                    self._heartbeat_writer.update_tier_event_ts(  # type: ignore[attr-defined]
                        "tick", event.event_time
                    )
                if self._coverage_stats_writer is not None:
                    self._coverage_stats_writer.record_event(  # type: ignore[attr-defined]
                        str(self._symbol), "tick", event.event_time
                    )
        elif event.kind == "orderbook_snapshot":
            ingester = self._wal_ingesters.get("orderbooksnapshot")
            if ingester is not None:
                record = {
                    "ts_utc": event.event_time,
                    "received_at": event.received_at,
                    "exchange": self._exchange,
                    "symbol": str(self._symbol),
                    "bids": [
                        {"price": lvl.price, "quantity": lvl.quantity}
                        for lvl in event.bids
                    ],
                    "asks": [
                        {"price": lvl.price, "quantity": lvl.quantity}
                        for lvl in event.asks
                    ],
                    "raw_json": json.dumps(raw, ensure_ascii=False) if raw else None,
                    "channel": "orderbooksnapshot",
                }
                ingester.append(record)
                from mctrader_data.metrics import record_ingester_event
                record_ingester_event(exchange=self._exchange, symbol=str(event.symbol), channel="orderbooksnapshot")
                if self._redis_publisher is not None:
                    self._redis_publisher.publish_orderbook_snapshot(  # type: ignore[attr-defined]
                        exchange=self._exchange,
                        symbol=str(event.symbol),
                        record=record,
                    )
                if self._heartbeat_writer is not None:
                    self._heartbeat_writer.update_tier_event_ts(  # type: ignore[attr-defined]
                        "orderbook_snapshot", event.received_at
                    )
                if self._coverage_stats_writer is not None:
                    self._coverage_stats_writer.record_event(  # type: ignore[attr-defined]
                        str(self._symbol), "orderbook", event.received_at
                    )
        elif event.kind == "orderbook_delta":
            ingester = self._wal_ingesters.get("orderbookdepth")
            if ingester is not None:
                record = {
                    "ts_utc": event.event_time,
                    "received_at": event.received_at,
                    "exchange": self._exchange,
                    "symbol": str(self._symbol),
                    "changes": [
                        {"side": ch.side, "price": ch.price, "quantity": ch.quantity}
                        for ch in event.changes
                    ],
                    "raw_json": json.dumps(raw, ensure_ascii=False) if raw else None,
                    "channel": "orderbookdepth",
                }
                ingester.append(record)
                from mctrader_data.metrics import record_ingester_event
                record_ingester_event(exchange=self._exchange, symbol=str(event.symbol), channel="orderbookdepth")
                if self._heartbeat_writer is not None and event.changes:
                    self._heartbeat_writer.update_tier_event_ts(  # type: ignore[attr-defined]
                        "orderbook", event.event_time
                    )
                if self._coverage_stats_writer is not None:
                    self._coverage_stats_writer.record_event(  # type: ignore[attr-defined]
                        str(self._symbol), "orderbook", event.event_time
                    )
        # TickerEvent / 기타 kind 는 무시 (diagnostic only)

    # backward-compatibility alias
    _handle_event = _emit_to_wal


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
        health_server: object | None = None,
        coverage_stats_writer: object | None = None,
    ) -> None:
        self._daemons = daemons
        self._manifest = manifest
        self._manifest_root = manifest_root
        self._heartbeat_writer = heartbeat_writer
        self._health_server = health_server
        self._coverage_stats_writer = coverage_stats_writer

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

        # Coverage stats task (mirrors heartbeat_task pattern)
        coverage_task: asyncio.Task[None] | None = None
        if self._coverage_stats_writer is not None:
            coverage_task = asyncio.create_task(
                self._coverage_stats_writer.run()  # type: ignore[attr-defined]
            )
            log.info("[collector] coverage-stats task spawned")

        # CFP-128 / ADR-033 Pilot — HealthServer (HTTP /health) for Docker HEALTHCHECK
        if self._health_server is not None:
            self._health_server.start()  # type: ignore[attr-defined]
            log.info(
                "[collector] health server started on port %s",
                getattr(self._health_server, "port", "?"),
            )

        # Per-symbol backoff delays (index = restart attempt number, capped at last)
        _RESTART_BACKOFF = [5, 30, 120]  # noqa: N806

        async def _run_with_restart(daemon: CollectorDaemon) -> None:
            """Run a single daemon, restarting on non-cancel errors with backoff."""
            attempt = 0
            while True:
                try:
                    await daemon.run()
                    return  # clean exit (cancel_event set)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    delay = _RESTART_BACKOFF[min(attempt, len(_RESTART_BACKOFF) - 1)]
                    log.error(
                        "[collector] symbol=%s task error (attempt=%d), restarting in %ds: %r",
                        daemon._symbol, attempt, delay, exc,
                    )
                    attempt += 1
                    await asyncio.sleep(delay)

        tasks = [asyncio.create_task(_run_with_restart(d)) for d in self._daemons]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
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
            # Coverage stats graceful shutdown
            if coverage_task is not None:
                coverage_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await coverage_task
                log.info("[collector] coverage-stats task shutdown complete")
            # CFP-128 / ADR-033 — HealthServer graceful stop
            if self._health_server is not None:
                with contextlib.suppress(Exception):
                    self._health_server.stop()  # type: ignore[attr-defined]
                log.info("[collector] health server shutdown complete")


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


class MetadataRefreshScheduler:
    """Daily §D13 exchange metadata refresh scheduler.

    Fires once at startup + once per UTC calendar day (at UTC midnight + 1min grace).
    Each fire fetches /ticker/ALL_KRW + /assetsstatus/multichain/ALL, builds
    ExchangeMetadataRecord list, and writes via ExchangeMetadataWriter.

    Runs as a separate asyncio task, cancelled when the collector daemon shuts down.
    """

    GRACE_SECONDS: float = 60.0  # UTC midnight + 1min grace per §D13

    def __init__(
        self,
        *,
        root: Path,
        exchange: str,
        node_id: str | None = None,
        collector_run_id: str | None = None,
    ) -> None:
        self._root = root
        self._exchange = exchange
        self._node_id = node_id
        self._collector_run_id = collector_run_id

    async def run(self) -> None:
        """Run until cancelled. First refresh fires immediately at startup."""
        from mctrader_data.metadata_storage import (
            ExchangeMetadataWriter,
        )

        writer = ExchangeMetadataWriter(
            root=self._root,
            exchange=self._exchange,
            node_id=self._node_id,
            collector_run_id=self._collector_run_id,
        )
        try:
            # Initial fetch at startup
            await self._do_refresh(writer)
            while True:
                # Sleep until next UTC midnight + grace
                now = datetime.now(timezone.utc)
                from datetime import timedelta as _td
                next_midnight = datetime(
                    *(now.date() + _td(days=1)).timetuple()[:3],
                    tzinfo=timezone.utc
                )
                wait = (next_midnight - now).total_seconds() + self.GRACE_SECONDS
                log.info(
                    "[metadata] next refresh in %.0fs (UTC %s + grace)",
                    wait, next_midnight.isoformat()
                )
                await asyncio.sleep(wait)
                await self._do_refresh(writer)
        except asyncio.CancelledError:
            log.info("[metadata] scheduler cancelled — flushing")
            writer.close()
            raise

    async def _do_refresh(self, writer: object) -> None:
        from mctrader_data.metadata_storage import fetch_exchange_metadata_records
        try:
            records = await fetch_exchange_metadata_records(
                exchange=self._exchange,
                node_id=self._node_id,
                collector_run_id=self._collector_run_id,
            )
            written = skipped = quarantine = 0
            for rec in records:
                status = writer.append(rec)  # type: ignore[attr-defined]
                if status == "written":
                    written += 1
                elif status == "skipped":
                    skipped += 1
                else:
                    quarantine += 1
            writer.flush()  # type: ignore[attr-defined]
            log.info(
                "[metadata] refresh done: written=%d skipped=%d quarantine=%d",
                written, skipped, quarantine,
            )
        except Exception:
            log.exception("[metadata] refresh failed — will retry next cycle")
