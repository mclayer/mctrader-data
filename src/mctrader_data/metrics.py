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

compactor_last_l2_timestamp = Gauge(
    "mctrader_compactor_last_l2_timestamp_seconds",
    "Unix timestamp of most recent successful L2 compaction",
    ["exchange", "symbol", "channel"],
)

compactor_l2_runs_total = Counter(
    "mctrader_compactor_l2_runs_total",
    "Total L2 compaction runs completed (MCT-156)",
    ["exchange", "symbol", "channel"],
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


def record_l2_compaction(*, exchange: str, symbol: str, channel: str) -> None:
    """MCT-156: L2 compaction 완료 시 timestamp + counter emit."""
    now = time.time()
    compactor_last_l2_timestamp.labels(
        exchange=exchange, symbol=symbol, channel=channel
    ).set(now)
    compactor_l2_runs_total.labels(
        exchange=exchange, symbol=symbol, channel=channel
    ).inc()


def record_l3_compaction(*, exchange: str, symbol: str, channel: str) -> None:
    now = time.time()
    compactor_last_l3_timestamp.labels(
        exchange=exchange, symbol=symbol, channel=channel
    ).set(now)
    compactor_l3_runs_total.labels(
        exchange=exchange, symbol=symbol, channel=channel
    ).inc()


compactor_process_rss_bytes = Gauge(
    "compactor_process_rss_bytes",
    "Compactor process resident set size (RSS) in bytes",
)


def observe_compactor_rss() -> None:
    """Sample current RSS via /proc/self/status. Called by metrics_server observer thread.

    Linux production path reads ``/proc/self/status:VmRSS``. macOS/Windows
    development fallback uses ``resource`` (Unix) or a ``ctypes`` Win32 call.
    psutil 도입 검토는 Task 7.
    """
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = int(line.split()[1])
                    compactor_process_rss_bytes.set(kb * 1024)
                    return
    except FileNotFoundError:
        pass

    # macOS/Linux non-procfs fallback — resource.getrusage
    try:
        import resource  # type: ignore[import-not-found]

        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # type: ignore[attr-defined]
        compactor_process_rss_bytes.set(rss_kb * 1024)
        return
    except ImportError:
        pass

    # Windows fallback — ctypes GetProcessMemoryInfo (psapi)
    try:
        import ctypes
        from ctypes import wintypes

        class _PMC(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        psapi = ctypes.windll.psapi  # type: ignore[attr-defined]
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE, ctypes.POINTER(_PMC), wintypes.DWORD
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL

        counters = _PMC()
        counters.cb = ctypes.sizeof(_PMC)
        if psapi.GetProcessMemoryInfo(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        ):
            compactor_process_rss_bytes.set(counters.WorkingSetSize)
            return
    except Exception:
        pass

    # Last-resort sentinel so /metrics still emits a sample point.
    compactor_process_rss_bytes.set(0)


compactor_pyarrow_total_allocated_bytes = Gauge(
    "compactor_pyarrow_total_allocated_bytes",
    "Bytes allocated by PyArrow default memory pool",
)

compactor_python_gc_gen_count = Gauge(
    "compactor_python_gc_gen_count",
    "Python GC collections count per generation",
    ["generation"],
)

compactor_tier_pending_segments = Gauge(
    "compactor_tier_pending_segments",
    "Pending sealed segments awaiting compaction per tier",
    ["tier"],
)

compactor_writer_open_count = Gauge(
    "compactor_writer_open_count",
    "Currently open ParquetWriter instances per tier",
    ["tier"],
)


def observe_compactor_runtime() -> None:
    """Sample pyarrow + Python gc stats. Called by metrics_server observer thread."""
    try:
        import pyarrow as pa

        pool = pa.default_memory_pool()
        compactor_pyarrow_total_allocated_bytes.set(pool.bytes_allocated())
    except Exception:
        pass

    import gc

    for gen, count in enumerate(gc.get_count()):
        compactor_python_gc_gen_count.labels(generation=str(gen)).set(count)


# ---------------------------------------------------------------------------
# MCT-166 Phase 2 -- collector/compactor unsupported channel counters (AC-4/5)
# ADR-027 Amendment 2 + INV-1 (fail-fast, silent-skip 차단)
# Counter objects are defined in allowlist.py to avoid circular imports.
# These helpers provide a metrics.py-level access point for documentation.
# ---------------------------------------------------------------------------

def record_collector_unsupported_channel(*, exchange: str, channel: str) -> None:
    """Emit collector_unsupported_channel_total counter (AC-4, INV-1).

    Called by allowlist.validate_channel_exchange() on unsupported combo.
    Do not call directly -- use allowlist.validate_channel_exchange() instead.
    """
    from mctrader_data.allowlist import collector_unsupported_channel_total
    collector_unsupported_channel_total.labels(exchange=exchange, channel=channel).inc()


def record_compactor_unsupported_source(*, tier: str, exchange: str, channel: str) -> None:
    """Emit compactor_unsupported_source_total counter (AC-5, INV-1).

    Called by allowlist.validate_compactor_source() on unsupported combo.
    Do not call directly -- use allowlist.validate_compactor_source() instead.
    """
    from mctrader_data.allowlist import compactor_unsupported_source_total
    compactor_unsupported_source_total.labels(tier=tier, exchange=exchange, channel=channel).inc()
