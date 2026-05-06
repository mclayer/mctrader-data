"""Tests for HeartbeatCounterSink (MCT-93 X4)."""
from __future__ import annotations

import threading
from pathlib import Path

from mctrader_data.dedup import DedupCounterSink
from mctrader_data.heartbeat import HeartbeatCounterSink, HeartbeatWriter


def test_heartbeat_counter_sink_increments_dup_skip(tmp_path: Path) -> None:
    writer = HeartbeatWriter(root=tmp_path, node_id="NODE_A")
    sink = HeartbeatCounterSink(writer)
    sink.increment_dup_skip(5)
    assert writer.metrics.dup_skip_count == 5


def test_heartbeat_counter_sink_increments_quarantine(tmp_path: Path) -> None:
    writer = HeartbeatWriter(root=tmp_path, node_id="NODE_A")
    sink = HeartbeatCounterSink(writer)
    sink.increment_quarantine(3)
    assert writer.metrics.quarantine_count == 3


def test_heartbeat_counter_sink_default_increment_is_one(tmp_path: Path) -> None:
    writer = HeartbeatWriter(root=tmp_path, node_id="NODE_A")
    sink = HeartbeatCounterSink(writer)
    sink.increment_dup_skip()
    sink.increment_quarantine()
    assert writer.metrics.dup_skip_count == 1
    assert writer.metrics.quarantine_count == 1


def test_heartbeat_counter_sink_thread_safety(tmp_path: Path) -> None:
    """10 threads × 1000 increments = 10000 (no lost updates)."""
    writer = HeartbeatWriter(root=tmp_path, node_id="NODE_A")
    sink = HeartbeatCounterSink(writer)

    def _worker() -> None:
        for _ in range(1000):
            sink.increment_dup_skip()

    threads = [threading.Thread(target=_worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert writer.metrics.dup_skip_count == 10000


def test_heartbeat_counter_sink_protocol_compliance(tmp_path: Path) -> None:
    """HeartbeatCounterSink must satisfy DedupCounterSink Protocol."""
    writer = HeartbeatWriter(root=tmp_path, node_id="NODE_A")
    sink: DedupCounterSink = HeartbeatCounterSink(writer)
    sink.increment_dup_skip()
    sink.increment_quarantine()
    assert writer.metrics.dup_skip_count == 1
    assert writer.metrics.quarantine_count == 1
