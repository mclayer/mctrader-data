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
from mctrader_data.orderbook_snapshot_storage import OrderbookSnapshotWriter
from mctrader_data.orderbook_storage import (
    OrderbookWriter,
    delta_event_to_records,
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
        include_orderbook_snapshot: bool = True,
        snapshot_id: str | None = None,
        node_id: str | None = None,
        collector_run_id: str | None = None,
        heartbeat_writer: object | None = None,
    ) -> None:
        if exchange != "bithumb":
            raise ValueError(f"only 'bithumb' exchange supported in v1, got {exchange!r}")
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
        self._tick_writer: TickWriter | None = None
        self._ob_writer: OrderbookWriter | None = None
        self._ob_snapshot_writer: OrderbookSnapshotWriter | None = None
        self._cancel_event = asyncio.Event()

    async def run(self) -> None:
        from mctrader_market_bithumb.ws_subscribe import Channel

        channels: list[Channel] = []
        if self._include_transactions:
            channels.append("transaction")
        if self._include_orderbook:
            channels.append("orderbookdepth")
        if self._include_orderbook_snapshot:
            channels.append("orderbooksnapshot")
        if not channels:
            raise ValueError(
                "at least one of transactions/orderbook/orderbook_snapshot must be included"
            )

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

        self._ob_snapshot_writer = OrderbookSnapshotWriter(
            root=self._root, exchange=self._exchange,
            symbol=str(self._symbol), snapshot_id=self._snapshot_id,
            node_id=self._node_id, collector_run_id=self._collector_run_id,
        ) if self._include_orderbook_snapshot else None

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
            record = transaction_event_to_record(event)
            self._tick_writer.append(record)
            # MCT-91 — heartbeat tier timestamp wiring (Codex F-1/F-5 ADOPT)
            if self._heartbeat_writer is not None:
                self._heartbeat_writer.update_tier_event_ts(  # type: ignore[attr-defined]
                    "tick", record.ts_utc
                )
        elif isinstance(event, OrderbookSnapshotEvent) and self._ob_snapshot_writer is not None:
            # MCT-104 §D14 — orderbooksnapshot goes to §D14 partition (NOT §D11)
            accepted = self._ob_snapshot_writer.append_event(event)
            if accepted and self._heartbeat_writer is not None:
                self._heartbeat_writer.update_tier_event_ts(  # type: ignore[attr-defined]
                    "orderbook_snapshot", event.received_at
                )
        elif isinstance(event, OrderbookDeltaEvent) and self._ob_writer is not None:
            # §D11 partition: delta-only (snapshot separated to §D14 above)
            records = delta_event_to_records(event)
            self._ob_writer.append_many(records)
            if self._heartbeat_writer is not None and records:
                self._heartbeat_writer.update_tier_event_ts(  # type: ignore[attr-defined]
                    "orderbook", records[0].ts_utc
                )
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
        try:
            if self._ob_snapshot_writer is not None:
                self._ob_snapshot_writer.close()
                if self._ob_snapshot_writer.current_path is not None:
                    self._write_lineage(
                        self._ob_snapshot_writer.current_path, "orderbook_snapshot"
                    )
        except Exception:
            log.exception("[collector] orderbook snapshot writer close failed")

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
        health_server: object | None = None,
    ) -> None:
        self._daemons = daemons
        self._manifest = manifest
        self._manifest_root = manifest_root
        self._heartbeat_writer = heartbeat_writer
        self._health_server = health_server

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

        # CFP-128 / ADR-033 Pilot — HealthServer (HTTP /health) for Docker HEALTHCHECK
        if self._health_server is not None:
            self._health_server.start()  # type: ignore[attr-defined]
            log.info(
                "[collector] health server started on port %s",
                getattr(self._health_server, "port", "?"),
            )

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
            fetch_exchange_metadata_records,
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
                tomorrow = (now.date().isoformat())
                from datetime import date as _date, timedelta as _td
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
