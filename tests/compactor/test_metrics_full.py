"""Verify all 5 compactor metrics are exposed via /metrics."""
from __future__ import annotations

import urllib.request

import pytest

from mctrader_data.compactor.metrics_server import start_metrics_server, stop_metrics_server


REQUIRED_METRICS = [
    "compactor_process_rss_bytes",
    "compactor_pyarrow_total_allocated_bytes",
    "compactor_python_gc_gen_count",
    "compactor_tier_pending_segments",
    "compactor_writer_open_count",
]


@pytest.fixture
def metrics_port(unused_tcp_port):
    start_metrics_server(port=unused_tcp_port)
    yield unused_tcp_port
    stop_metrics_server()


def test_all_required_metrics_exposed(metrics_port):
    resp = urllib.request.urlopen(f"http://127.0.0.1:{metrics_port}/metrics", timeout=2)
    body = resp.read().decode("utf-8")
    missing = [m for m in REQUIRED_METRICS if m not in body]
    assert not missing, f"missing metrics: {missing}"
