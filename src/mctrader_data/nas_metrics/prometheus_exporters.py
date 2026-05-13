"""prometheus_exporters.py — 5종 Prometheus metric export for NAS MinIO uploader.

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253
ADR: ADR-027 (D5/D6) + S11 IOPS baseline

Design decisions (§6.2.3 Change Plan 박제, FIX#2 P1-1 갱신):
- Metric prefix: nas_uploader_* (NFR-4 freeze — MCT-151 nas_invariant_* prefix-disjoint)
- 5종 metric (FIX#2: queue_bytes 추가):
  - nas_uploader_success_count (Counter, labels: bucket)
  - nas_uploader_fail_count (Counter, labels: bucket, reason)
  - nas_uploader_latency_seconds (Histogram, labels: bucket, operation [put|head])
  - nas_uploader_queue_depth (Gauge, labels: queue_path)
  - nas_uploader_queue_bytes (Gauge, labels: queue_path) — FIX#2 P1-1 신규
- reason label = generic enum ONLY (§6.3 SecurityArch: raw boto3 exception message embed 금지)
  Allowed values: endpoint_unreachable | auth_failed | quota_exceeded | unknown
- bucket label = simple bucket name (not URL, not endpoint)
- IOPS baseline = node_exporter scrape (host disk I/O) + cAdvisor (container metrics)
  본 module = naming + label 박제만, 측정은 Prometheus scrape job.
- AC-4 10GB alert: nas_uploader_queue_bytes > 10737418240 → NASUploaderBacklogBytesHigh
  (Prometheus rule: configs/prometheus/nas_uploader_rules.yml 정합)

SecurityArch (§6.3):
- NAS endpoint URL: Prometheus metric naming 에 포함 금지
- reason label 화이트리스트 = 4 enum
"""
from __future__ import annotations

from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    REGISTRY,
)

if TYPE_CHECKING:
    pass

# §6.3: reason label 화이트리스트 (cardinality=4, SecurityArch 박제)
ALLOWED_REASONS = frozenset(
    ["endpoint_unreachable", "auth_failed", "quota_exceeded", "unknown"]
)

# §8.3: latency histogram buckets (0.05s=50ms ... 10s, §6.2.3 박제)
_LATENCY_BUCKETS = (0.05, 0.1, 0.5, 1.0, 3.0, 10.0)


class PrometheusExporter:
    """5종 Prometheus metric export + IOPS baseline integration.

    Metric prefix (NFR-4 freeze): "nas_uploader_*"
    MCT-151 invariant harness: "nas_invariant_*" prefix-disjoint.

    Metrics:
        - nas_uploader_success_count (Counter, labels: bucket)
        - nas_uploader_fail_count (Counter, labels: bucket, reason)
        - nas_uploader_latency_seconds (Histogram, labels: bucket, operation [put|head])
        - nas_uploader_queue_depth (Gauge, labels: queue_path)
        - nas_uploader_queue_bytes (Gauge, labels: queue_path) — FIX#2 P1-1

    IOPS baseline = node_exporter 가 scrape — 본 module 측정 0.
    node_exporter metrics: node_disk_io_time_seconds_total / node_disk_read_bytes_total /
    node_disk_write_bytes_total / container_fs_io_time_seconds_total (cAdvisor).
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        _reg = registry if registry is not None else REGISTRY
        # Store registry reference for lazy emit_invariant_* init (MCT-151)
        self._registry_ref = _reg

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

        # P1-1 FIX#2: queue bytes Gauge (10GB alert 정합)
        self._queue_bytes = Gauge(
            "nas_uploader_queue_bytes",
            "Current retry queue backlog total bytes",
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

    def set_queue_bytes(self, queue_path: str, bytes_total: int) -> None:
        """retry queue bytes gauge 설정 (P1-1 FIX#2).

        RetryQueue._total_bytes() 정기 호출 또는 enqueue/drain trigger 시 update.
        Prometheus rule: nas_uploader_queue_bytes > 10737418240 → NASUploaderBacklogBytesHigh.
        AC-4 wording 정합: 10GB threshold OR 1000seg threshold.
        """
        self._queue_bytes.labels(queue_path=queue_path).set(bytes_total)

    # ─── MCT-151 신규 method (nas_invariant_* prefix — MCT-150 nas_uploader_* prefix-disjoint) ──

    def _ensure_invariant_metrics(self) -> None:
        """Lazy-initialize nas_invariant_* metrics (NFR-4 prefix-disjoint 의무).

        MCT-150 nas_uploader_* prefix 와 collision 0 보장.
        emit_invariant_* method 최초 호출 시 1회 초기화.
        """
        if hasattr(self, "_invariant_initialized"):
            return

        _reg = self.__dict__.get("_registry_ref", REGISTRY)

        # nas_invariant_dual_write_*
        self._inv_dw_status = Counter(
            "nas_invariant_dual_write_status_count",
            "DualWriter.write() result status count (MCT-151 §6.2.1)",
            ["status", "nas_key_prefix"],
            registry=_reg,
        )
        self._inv_dw_latency = Histogram(
            "nas_invariant_dual_write_latency_seconds",
            "DualWriter.write() latency in seconds (MCT-151)",
            ["status"],
            buckets=(0.05, 0.1, 0.5, 1.0, 3.0, 10.0),
            registry=_reg,
        )

        # nas_invariant_compaction_barrier_*
        self._inv_cb_status = Counter(
            "nas_invariant_compaction_barrier_status_count",
            "CompactionBarrier.drain_and_block() result status count (MCT-151 §6.2.2)",
            ["status"],
            registry=_reg,
        )
        self._inv_cb_drain_wait = Histogram(
            "nas_invariant_compaction_barrier_drain_wait_seconds",
            "CompactionBarrier drain wait time in seconds (MCT-151)",
            [],
            buckets=(0.1, 1.0, 10.0, 60.0, 600.0, 3600.0, 86400.0),
            registry=_reg,
        )
        self._inv_cb_in_flight = Gauge(
            "nas_invariant_compaction_barrier_in_flight_remaining",
            "CompactionBarrier in-flight tasks remaining after drain_timeout (MCT-151)",
            [],
            registry=_reg,
        )

        # nas_invariant_verify_*
        self._inv_verify_status = Counter(
            "nas_invariant_status_count",
            "InvariantHarness.verify() result status count (MCT-151 §6.2.3)",
            ["status"],
            registry=_reg,
        )
        self._inv_verify_latency = Histogram(
            "nas_invariant_verify_latency_seconds",
            "InvariantHarness.verify() latency in seconds (MCT-151)",
            ["status"],
            buckets=(0.1, 1.0, 5.0, 30.0, 60.0, 600.0),
            registry=_reg,
        )
        self._inv_sha256_match = Counter(
            "nas_invariant_sha256_match_count",
            "sha256 invariant match count (MCT-151, D6 source)",
            ["partition"],
            registry=_reg,
        )
        self._inv_object_count = Gauge(
            "nas_invariant_object_count_match",
            "object_count invariant gauge (MCT-151, D6 source)",
            ["partition", "type"],
            registry=_reg,
        )
        self._inv_row_count = Counter(
            "nas_invariant_row_count_match_count",
            "row_count invariant match count (MCT-151, D6 source)",
            ["partition"],
            registry=_reg,
        )
        self._inv_schema_drift = Counter(
            "nas_invariant_schema_drift_count",
            "schema drift count by drift_type (MCT-151, S5 신규 4종)",
            ["partition", "drift_type"],
            registry=_reg,
        )

        self._invariant_initialized = True

    def emit_invariant_dual_write(
        self, status: str, nas_key_prefix: str, latency_s: float
    ) -> None:
        """DualWriter.write() emit — DualWriteResult.status enum 3종 별 Counter + Histogram.

        Metrics (nas_invariant_* prefix — MCT-150 nas_uploader_* prefix-disjoint):
        - nas_invariant_dual_write_status_count (Counter, labels: status, nas_key_prefix)
        - nas_invariant_dual_write_latency_seconds (Histogram, labels: status)

        §6.8 Wording SSOT: status ∈ {"committed", "local_only", "hard_floor_blocked"}.
        """
        self._ensure_invariant_metrics()
        self._inv_dw_status.labels(status=status, nas_key_prefix=nas_key_prefix).inc()
        self._inv_dw_latency.labels(status=status).observe(latency_s)

    def emit_invariant_compaction_barrier(
        self, status: str, drain_wait_s: float, in_flight_remaining: int
    ) -> None:
        """CompactionBarrier.drain_and_block() emit — BarrierResult.status 3종 별 Counter + Histogram + Gauge.

        Metrics (nas_invariant_* prefix):
        - nas_invariant_compaction_barrier_status_count (Counter, labels: status)
        - nas_invariant_compaction_barrier_drain_wait_seconds (Histogram)
        - nas_invariant_compaction_barrier_in_flight_remaining (Gauge)

        §6.8 Wording SSOT: status ∈ {"ok", "drain_timeout", "barrier_violated"}.
        """
        self._ensure_invariant_metrics()
        self._inv_cb_status.labels(status=status).inc()
        self._inv_cb_drain_wait.observe(drain_wait_s)
        self._inv_cb_in_flight.set(in_flight_remaining)

    def emit_invariant_verify(
        self,
        status: str,
        partition: str,
        latency_s: float,
        per_invariant_results: dict,
    ) -> None:
        """InvariantHarness.verify() emit — InvariantResult.status 8종 + per-invariant 측정값.

        Metrics (nas_invariant_* prefix):
        - nas_invariant_status_count (Counter, labels: status)
        - nas_invariant_verify_latency_seconds (Histogram, labels: status)
        - nas_invariant_sha256_match_count (Counter, labels: partition) — sha256 PASS 시
        - nas_invariant_object_count_match (Gauge, labels: partition, type)
        - nas_invariant_row_count_match_count (Counter, labels: partition) — row_count PASS 시
        - nas_invariant_schema_drift_count (Counter, labels: partition, drift_type) — FAIL 시

        §6.8 Wording SSOT: status ∈ {"all_pass", "sha256_fail", "object_count_fail",
        "row_count_fail", "column_count_fail", "column_order_fail", "dtype_fail", "schema_version_fail"}.
        """
        self._ensure_invariant_metrics()
        self._inv_verify_status.labels(status=status).inc()
        self._inv_verify_latency.labels(status=status).observe(latency_s)

        # sha256 match count (pass only)
        sha256_result = per_invariant_results.get("sha256")
        if sha256_result and getattr(sha256_result, "status", None) == "pass":
            self._inv_sha256_match.labels(partition=partition).inc()

        # object_count gauge
        oc_result = per_invariant_results.get("object_count")
        if oc_result:
            local_count = getattr(oc_result, "measured_local", 0) or 0
            nas_count = getattr(oc_result, "measured_nas", 0) or 0
            self._inv_object_count.labels(partition=partition, type="local").set(local_count)
            self._inv_object_count.labels(partition=partition, type="nas").set(nas_count)

        # row_count match count (pass only)
        rc_result = per_invariant_results.get("row_count")
        if rc_result and getattr(rc_result, "status", None) == "pass":
            self._inv_row_count.labels(partition=partition).inc()

        # schema_drift_count (fail only — S5 신규 4종)
        for drift_type in ("column_count", "column_order", "dtype", "schema_version"):
            inv_result = per_invariant_results.get(drift_type)
            if inv_result and getattr(inv_result, "status", None) == "fail":
                self._inv_schema_drift.labels(partition=partition, drift_type=drift_type).inc()
