"""prometheus_exporters.py — 4종 Prometheus metric export for NAS MinIO uploader.

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253
ADR: ADR-027 (D5/D6) + S11 IOPS baseline

Design decisions (§6.2.3 Change Plan 박제):
- Metric prefix: nas_uploader_* (NFR-4 freeze — MCT-151 nas_invariant_* prefix-disjoint)
- 4종 metric:
  - nas_uploader_success_count (Counter, labels: bucket)
  - nas_uploader_fail_count (Counter, labels: bucket, reason)
  - nas_uploader_latency_seconds (Histogram, labels: bucket, operation [put|head])
  - nas_uploader_queue_depth (Gauge, labels: queue_path)
- reason label = generic enum ONLY (§6.3 SecurityArch: raw boto3 exception message embed 금지)
  Allowed values: endpoint_unreachable | auth_failed | quota_exceeded | unknown
- bucket label = simple bucket name (not URL, not endpoint)
- IOPS baseline = node_exporter scrape (host disk I/O) + cAdvisor (container metrics)
  본 module = naming + label 박제만, 측정은 Prometheus scrape job.

SecurityArch (§6.3):
- NAS endpoint URL: Prometheus metric naming 에 포함 금지
- reason label 화이트리스트 = 4 enum
"""
from __future__ import annotations

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    REGISTRY,
)

# §6.3: reason label 화이트리스트 (cardinality=4, SecurityArch 박제)
ALLOWED_REASONS = frozenset(
    ["endpoint_unreachable", "auth_failed", "quota_exceeded", "unknown"]
)

# §8.3: latency histogram buckets (0.05s=50ms ... 10s, §6.2.3 박제)
_LATENCY_BUCKETS = (0.05, 0.1, 0.5, 1.0, 3.0, 10.0)


class PrometheusExporter:
    """4종 Prometheus metric export + IOPS baseline integration.

    Metric prefix (NFR-4 freeze): "nas_uploader_*"
    MCT-151 invariant harness: "nas_invariant_*" prefix-disjoint.

    Metrics:
        - nas_uploader_success_count (Counter, labels: bucket)
        - nas_uploader_fail_count (Counter, labels: bucket, reason)
        - nas_uploader_latency_seconds (Histogram, labels: bucket, operation [put|head])
        - nas_uploader_queue_depth (Gauge, labels: queue_path)

    IOPS baseline = node_exporter 가 scrape — 본 module 측정 0.
    node_exporter metrics: node_disk_io_time_seconds_total / node_disk_read_bytes_total /
    node_disk_write_bytes_total / container_fs_io_time_seconds_total (cAdvisor).
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        _reg = registry if registry is not None else REGISTRY

        self._success_count = Counter(
            "nas_uploader_success_count",
            "Number of successful NAS MinIO PUT operations",
            ["bucket"],
            registry=_reg,
        )

        self._fail_count = Counter(
            "nas_uploader_fail_count",
            "Number of failed NAS MinIO PUT/HEAD operations",
            ["bucket", "reason"],
            registry=_reg,
        )

        self._latency = Histogram(
            "nas_uploader_latency_seconds",
            "Latency of NAS MinIO PUT/HEAD operations in seconds",
            ["bucket", "operation"],
            buckets=_LATENCY_BUCKETS,
            registry=_reg,
        )

        self._queue_depth = Gauge(
            "nas_uploader_queue_depth",
            "Current retry queue backlog segment count",
            ["queue_path"],
            registry=_reg,
        )

    def emit_success(self, bucket: str, latency_s: float) -> None:
        """성공 카운터 +1 + latency histogram record (operation='put')."""
        self._success_count.labels(bucket=bucket).inc()
        self._latency.labels(bucket=bucket, operation="put").observe(latency_s)

    def emit_fail(self, bucket: str, reason: str, latency_s: float) -> None:
        """실패 카운터 +1 (reason label = enum only, SecurityArch §6.3).

        reason 이 ALLOWED_REASONS 외의 값이면 'unknown' 으로 강제 변환.
        """
        safe_reason = reason if reason in ALLOWED_REASONS else "unknown"
        self._fail_count.labels(bucket=bucket, reason=safe_reason).inc()
        self._latency.labels(bucket=bucket, operation="put").observe(latency_s)

    def emit_head(self, bucket: str, latency_s: float) -> None:
        """HEAD 요청 latency record (operation='head')."""
        self._latency.labels(bucket=bucket, operation="head").observe(latency_s)

    def set_queue_depth(self, queue_path: str, depth: int) -> None:
        """retry queue depth gauge 설정. RetryQueue.depth() 호출 후 emit."""
        self._queue_depth.labels(queue_path=queue_path).set(depth)
