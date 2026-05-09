from __future__ import annotations

import time

from prometheus_client import Counter, Gauge

ingester_events_total = Counter(
    "mctrader_ingester_events_total",
    "Total events written to WAL",
    ["exchange", "symbol", "channel"],
)

wal_write_lag_seconds = Gauge(
    "mctrader_wal_write_lag_seconds",
    "Seconds since last WAL write per (exchange, symbol)",
    ["exchange", "symbol"],
)

compactor_last_l3_timestamp = Gauge(
    "mctrader_compactor_last_l3_timestamp_seconds",
    "Unix timestamp of most recent successful L3 compaction",
    ["exchange", "symbol", "channel"],
)

compactor_l3_runs_total = Counter(
    "mctrader_compactor_l3_runs_total",
    "Total L3 compaction runs completed",
    ["exchange", "symbol", "channel"],
)


def record_ingester_event(*, exchange: str, symbol: str, channel: str) -> None:
    ingester_events_total.labels(exchange=exchange, symbol=symbol, channel=channel).inc()


def record_l3_compaction(*, exchange: str, symbol: str, channel: str) -> None:
    now = time.time()
    compactor_last_l3_timestamp.labels(
        exchange=exchange, symbol=symbol, channel=channel
    ).set(now)
    compactor_l3_runs_total.labels(
        exchange=exchange, symbol=symbol, channel=channel
    ).inc()
