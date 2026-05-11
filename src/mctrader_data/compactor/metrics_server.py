"""Compactor /metrics HTTP server + observer thread."""
from __future__ import annotations

import logging
import threading

from prometheus_client import start_http_server

from mctrader_data.metrics import observe_compactor_rss

log = logging.getLogger(__name__)

_OBSERVER_INTERVAL_SECONDS = 5.0
_observer_thread: threading.Thread | None = None
_observer_stop = threading.Event()
_server_started = False


def _observer_loop() -> None:
    while not _observer_stop.is_set():
        try:
            observe_compactor_rss()
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
    # Prime gauge once synchronously so the first scrape after start observes a value.
    try:
        observe_compactor_rss()
    except Exception:
        log.exception("[metrics_server] initial observe_compactor_rss error")
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
