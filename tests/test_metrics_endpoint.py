import threading
import time
import urllib.request

import pytest

from mctrader_data.metrics import (
    ingester_events_total,
    compactor_last_l3_timestamp,
    record_ingester_event,
    record_l3_compaction,
)
from mctrader_data.health_server import HealthServer


def test_record_ingester_event_increments_counter():
    before = ingester_events_total.labels(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction"
    )._value.get()
    record_ingester_event(exchange="bithumb", symbol="KRW-BTC", channel="transaction")
    after = ingester_events_total.labels(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction"
    )._value.get()
    assert after == before + 1


def test_record_l3_compaction_sets_gauge():
    record_l3_compaction(exchange="bithumb", symbol="KRW-BTC", channel="transaction")
    ts = compactor_last_l3_timestamp.labels(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction"
    )._value.get()
    assert ts > 0


def test_health_server_exposes_metrics_endpoint():
    server = HealthServer(heartbeat_writer=None, port=18181)
    server.start()
    time.sleep(0.1)
    try:
        with urllib.request.urlopen("http://localhost:18181/metrics") as resp:
            assert resp.status == 200
            content_type = resp.headers.get("Content-Type", "")
            assert "text/plain" in content_type
            body = resp.read().decode()
            assert "mctrader_ingester_events_total" in body
    finally:
        server.stop()
