"""Compactor /metrics HTTP server + observer thread."""
from __future__ import annotations

import logging
import threading

from prometheus_client import start_http_server

from mctrader_data.metrics import observe_compactor_rss, observe_compactor_runtime

log = logging.getLogger(__name__)

_OBSERVER_INTERVAL_SECONDS = 5.0
_observer_thread: threading.Thread | None = None
_observer_stop = threading.Event()
_server_started = False


def _observer_loop() -> None:
    while not _observer_stop.is_set():
        try:
            observe_compactor_rss()
            observe_compactor_runtime()
        except Exception:
            log.exception("[metrics_server] observer tick error")
        _observer_stop.wait(_OBSERVER_INTERVAL_SECONDS)


def start_metrics_server(port: int = 8080) -> None:
    """Start prometheus_client HTTP server + observer thread.

    Idempotent: subsequent calls log a warning and no-op. The HTTP server is a
    daemon thread owned by ``prometheus_client``; production process lifetime is
    controlled by the container. Tests should call :func:`stop_metrics_server`
    to terminate the observer between cases.
    """
    global _observer_thread, _server_started
    if _server_started:
        log.warning("[metrics_server] already started, skipping")
        return
    start_http_server(port)
    _server_started = True
    # Prime gauges once synchronously so the first scrape after start observes values.
    try:
        observe_compactor_rss()
        observe_compactor_runtime()
    except Exception:
        log.exception("[metrics_server] initial observe error")
    # Prime tier labels so /metrics shows 0 instead of "no data" before first cycle.
    try:
        from mctrader_data.metrics import (
            compactor_tier_pending_segments,
            compactor_writer_open_count,
        )
        for tier in ("L1", "L2", "L3"):
            compactor_writer_open_count.labels(tier=tier).set(0)
            compactor_tier_pending_segments.labels(tier=tier).set(0)
    except Exception:
        log.exception("[metrics_server] tier label prime error")
    _observer_stop.clear()
    _observer_thread = threading.Thread(
        target=_observer_loop, name="compactor-metrics-observer", daemon=True
    )
    _observer_thread.start()
    log.info("[metrics_server] started port=%d", port)


def stop_metrics_server() -> None:
    """Test helper. Production process is killed via container stop."""
    global _observer_thread, _server_started
    _observer_stop.set()
    if _observer_thread is not None:
        _observer_thread.join(timeout=3.0)
        _observer_thread = None
    _server_started = False
