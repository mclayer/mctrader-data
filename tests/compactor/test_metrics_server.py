from __future__ import annotations

import urllib.request

import pytest

from mctrader_data.compactor.metrics_server import start_metrics_server, stop_metrics_server


@pytest.fixture
def metrics_port(unused_tcp_port):
    start_metrics_server(port=unused_tcp_port)
    yield unused_tcp_port
    stop_metrics_server()


def test_metrics_endpoint_responds_200(metrics_port):
    resp = urllib.request.urlopen(f"http://127.0.0.1:{metrics_port}/metrics", timeout=2)
    assert resp.status == 200
    body = resp.read().decode("utf-8")
    assert "compactor_process_rss_bytes" in body


def test_rss_gauge_is_positive(metrics_port):
    resp = urllib.request.urlopen(f"http://127.0.0.1:{metrics_port}/metrics", timeout=2)
    body = resp.read().decode("utf-8")
    rss_lines = [
        line for line in body.splitlines()
        if line.startswith("compactor_process_rss_bytes ")
    ]
    assert len(rss_lines) == 1
    value = float(rss_lines[0].split()[1])
    assert value > 0
