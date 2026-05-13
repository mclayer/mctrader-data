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

# MCT-156: DualWriter status/tier Counter (Stage 3 wiring, AC-G)
# status ∈ {committed, local_only, hard_floor_blocked}, tier ∈ {L2, L3}
# ADR-027 D5 amendment caller contract — caller (CompactorRunner._dispatch_dual_write) emit.
dual_write_result_total = Counter(
    "mctrader_dual_write_result_total",
    "DualWriter put() result count by status and tier (MCT-156)",
    ["status", "tier"],  # status ∈ {committed, local_only, hard_floor_blocked}, tier ∈ {L2, L3}
)

# MCT-162 (ADR-027 D4 amendment, 2026-05-13)
# L1Compactor unsupported channel encounter = fail-fast invariant (silent skip 차단)
# cardinality bounded low — collector emit channel 종류만 (attacker-controlled injection path 0)
compactor_unsupported_channel_total = Counter(
    "mctrader_compactor_unsupported_channel_total",
    "L1Compactor unsupported channel encountered (MCT-162 fail-fast, silent skip 차단)",
    ["channel"],  # cardinality bounded low — collector emit channel 종류만
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

        # nas_invariant_verify_* (MCT-159 FIX Iter 1: channel+tier label 추가)
        self._inv_verify_status = Counter(
            "nas_invariant_status_count",
            "InvariantHarness.verify() result status count (MCT-151 §6.2.3, MCT-159 channel-aware)",
            ["status", "channel", "tier"],
            registry=_reg,
        )
        self._inv_verify_latency = Histogram(
            "nas_invariant_verify_latency_seconds",
            "InvariantHarness.verify() latency in seconds (MCT-151)",
            ["status"],
            buckets=(0.1, 1.0, 5.0, 30.0, 60.0, 600.0),
            registry=_reg,
        )
        # mctrader_invariant_verify_total{status, channel, tier} — ADR-027 §D6.1 channel-aware Counter
        # MCT-159 FIX Iter 1: channel label 추가 (cardinality: status×channel×tier)
        self._invariant_verify_total = Counter(
            "mctrader_invariant_verify_total",
            "InvariantHarness.verify() total count by status, channel, and tier (MCT-159 §D6.1)",
            ["status", "channel", "tier"],
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
        channel: str = "unknown",
        tier: str = "unknown",
    ) -> None:
        """InvariantHarness.verify() emit — InvariantResult.status 8종 + per-invariant 측정값.

        MCT-159 FIX Iter 1: channel + tier label 추가 (ADR-027 §D6.1 channel-aware contract).

        Metrics (nas_invariant_* prefix):
        - nas_invariant_status_count (Counter, labels: status, channel, tier) — MCT-159 channel label 추가
        - nas_invariant_verify_latency_seconds (Histogram, labels: status)
        - nas_invariant_sha256_match_count (Counter, labels: partition) — sha256 PASS 시
        - nas_invariant_object_count_match (Gauge, labels: partition, type)
        - nas_invariant_row_count_match_count (Counter, labels: partition) — row_count PASS 시
        - nas_invariant_schema_drift_count (Counter, labels: partition, drift_type) — FAIL 시
        - mctrader_invariant_verify_total (Counter, labels: status, channel, tier) — MCT-159 신규

        §6.8 Wording SSOT: status ∈ {"all_pass", "sha256_fail", "object_count_fail",
        "row_count_fail", "column_count_fail", "column_order_fail", "dtype_fail", "schema_version_fail"}.
        """
        self._ensure_invariant_metrics()
        self._inv_verify_status.labels(status=status, channel=channel, tier=tier).inc()
        self._invariant_verify_total.labels(status=status, channel=channel, tier=tier).inc()
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

    # ─── MCT-152 신규 method (nas_dual_write_window_* prefix — NFR-4 prefix-disjoint) ──

    def _ensure_dual_write_window_metrics(self) -> None:
        """Lazy-initialize nas_dual_write_window_* metrics (NFR-4 prefix-disjoint 의무).

        MCT-150 nas_uploader_* + MCT-151 nas_invariant_* prefix 와 collision 0 보장.
        emit_dual_write_window_* method 최초 호출 시 1회 초기화.

        Metrics (§6.8.4 박제, variant 금지):
        - nas_dual_write_window_status_count (Counter, labels: status [5 enum])
        - nas_dual_write_window_cycle_duration_seconds (Histogram, buckets [60,300,1800,3600,7200])
        - nas_dual_write_window_iops_delta_pct (Gauge, labels: metric_type [p99|read_iops|write_iops])
        - nas_dual_write_window_sop_trigger_count (Counter, labels: sop_state [3 enum])
        """
        if hasattr(self, "_dww_initialized"):
            return

        _reg = self.__dict__.get("_registry_ref", REGISTRY)

        self._dww_status_count = Counter(
            "nas_dual_write_window_status_count",
            "DualWriteWindowRunner.run() result status count (MCT-152 §6.8.4)",
            ["status"],
            registry=_reg,
        )

        self._dww_cycle_duration = Histogram(
            "nas_dual_write_window_cycle_duration_seconds",
            "DualWriteWindowRunner.run() cycle duration in seconds (MCT-152 §8.3)",
            [],
            buckets=(60.0, 300.0, 1800.0, 3600.0, 7200.0),
            registry=_reg,
        )

        self._dww_iops_delta_pct = Gauge(
            "nas_dual_write_window_iops_delta_pct",
            "IOPS delta percentage vs MCT-148 T2 baseline (MCT-152 S11 박제)",
            ["metric_type"],
            registry=_reg,
        )

        self._dww_sop_trigger_count = Counter(
            "nas_dual_write_window_sop_trigger_count",
            "SOPRunner state trigger count during dual-write window (MCT-152 S10 박제)",
            ["sop_state"],
            registry=_reg,
        )

        self._dww_initialized = True

    def emit_dual_write_window_status(self, status: str) -> None:
        """DualWriteWindowResult.status emit — 5 enum Counter.

        Metrics (nas_dual_write_window_* prefix — NFR-4 freeze):
        - nas_dual_write_window_status_count (Counter, labels: status)

        §6.8.1 Wording SSOT: status ∈ {"healthy", "drift_detected", "barrier_drain_timeout",
        "sop_manual_gate", "iops_gate_breached"}.
        variant 사용 금지.
        """
        self._ensure_dual_write_window_metrics()
        self._dww_status_count.labels(status=status).inc()

    def emit_dual_write_window_cycle_duration(self, duration_s: float) -> None:
        """DualWriteWindowRunner.run() cycle duration emit — Histogram.

        Metrics:
        - nas_dual_write_window_cycle_duration_seconds (Histogram)

        §8.3 Perf Baseline: buckets [60, 300, 1800, 3600, 7200] (1min/5min/30min/1h/2h).
        """
        self._ensure_dual_write_window_metrics()
        self._dww_cycle_duration.observe(duration_s)

    def emit_dual_write_window_iops_delta(
        self, p99_pct: float, read_iops: float, write_iops: float
    ) -> None:
        """IOPS during delta % emit — Gauge (3 metric_type labels).

        Metrics:
        - nas_dual_write_window_iops_delta_pct (Gauge, labels: metric_type)
          metric_type ∈ {"p99", "read_iops", "write_iops"}

        S11 박제: MCT-150 pre vs MCT-152 during delta 비교 표 source.
        NFR cross-reference: ±15% gate (MCT-148 T2 baseline 50MB p99 = 2870.65ms).
        """
        self._ensure_dual_write_window_metrics()
        self._dww_iops_delta_pct.labels(metric_type="p99").set(p99_pct)
        self._dww_iops_delta_pct.labels(metric_type="read_iops").set(read_iops)
        self._dww_iops_delta_pct.labels(metric_type="write_iops").set(write_iops)

    def emit_dual_write_window_sop_trigger(self, sop_state: str) -> None:
        """SOPRunner state trigger count emit — Counter.

        Metrics:
        - nas_dual_write_window_sop_trigger_count (Counter, labels: sop_state)
          sop_state ∈ {"auto_resume", "threshold_breached", "manual_gate"} (§6.8.3 SOPState SSOT)

        S10 박제: SOP 실전 가동 evidence-rich source.
        per-trigger evidence: timestamp (evidence pack) + sop_state counter (Prometheus).
        """
        self._ensure_dual_write_window_metrics()
        self._dww_sop_trigger_count.labels(sop_state=sop_state).inc()

    # ─── MCT-153 신규 method (nas_backfill_* prefix — NFR-4 prefix-disjoint) ──

    def _ensure_backfill_metrics(self) -> None:
        """Lazy-initialize nas_backfill_* metrics (NFR-4 prefix-disjoint 의무).

        MCT-150 nas_uploader_* + MCT-151 nas_invariant_* + MCT-152 nas_dual_write_window_*
        prefix 와 collision 0 보장.
        emit_backfill_* method 최초 호출 시 1회 초기화.

        Metrics (§6.4 chief decision 박제, variant 금지):
        - nas_backfill_chunks_total (Counter)
        - nas_backfill_chunks_completed_total (Counter, labels: status [5 enum])
        - nas_backfill_put_latency_seconds (Histogram, buckets [0.5,1.0,2.0,5.0,10.0,30.0])
        - nas_backfill_legacy_node_default_count (Counter)
        - nas_backfill_quarantine_count (Counter, labels: fail_invariant [7 enum])
        - nas_backfill_resumable_count (Counter)
        """
        if hasattr(self, "_backfill_initialized"):
            return

        _reg = self.__dict__.get("_registry_ref", REGISTRY)

        self._bf_chunks_total = Counter(
            "nas_backfill_chunks_total",
            "Total chunks discovered by BackfillOrchestrator partition discovery (MCT-153 §6.2.1)",
            registry=_reg,
        )

        self._bf_chunks_completed = Counter(
            "nas_backfill_chunks_completed_total",
            "BackfillOrchestrator per-chunk processing result count (MCT-153 §6.2.1)",
            ["status"],
            registry=_reg,
        )

        self._bf_put_latency = Histogram(
            "nas_backfill_put_latency_seconds",
            "NASUploader.put() latency per chunk during backfill (MCT-153 AC-2)",
            [],
            buckets=(0.5, 1.0, 2.0, 5.0, 10.0, 30.0),
            registry=_reg,
        )

        self._bf_legacy_node_default = Counter(
            "nas_backfill_legacy_node_default_count",
            "Partition count where node= prefix was absent and node=DEFAULT was inserted (MCT-153 S6, AC-3)",
            registry=_reg,
        )

        self._bf_quarantine = Counter(
            "nas_backfill_quarantine_count",
            "Chunks quarantined after 3 invariant verify retries (MCT-153 AC-4)",
            ["fail_invariant"],
            registry=_reg,
        )

        self._bf_resumable = Counter(
            "nas_backfill_resumable_count",
            "Chunks in pending/in_flight/sop_skipped state at BackfillOrchestrator.run() exit (MCT-153 AC-5)",
            registry=_reg,
        )

        self._backfill_initialized = True

    def emit_backfill_chunks_total(self, count: int) -> None:
        """Partition discovery 결과 총 chunk 수 emit — Counter.

        Metrics (nas_backfill_* prefix — NFR-4 freeze):
        - nas_backfill_chunks_total (Counter)

        Phase B (partition discovery) 완료 시점 1회 호출.
        §6.8 Wording SSOT: BackfillOrchestrator Phase B exit 시점 박제.
        """
        self._ensure_backfill_metrics()
        self._bf_chunks_total.inc(count)

    def emit_backfill_chunks_completed(self, status: str) -> None:
        """Per-chunk processing result emit — Counter with status label.

        Metrics (nas_backfill_* prefix — NFR-4 freeze):
        - nas_backfill_chunks_completed_total (Counter, labels: status)

        §6.8 Wording SSOT: status ∈ {
            "chunk_verified", "chunk_skipped_resumed", "chunk_quarantined",
            "chunk_blocked", "chunk_sop_skipped"
        } (ChunkResult.status 5 enum).
        variant 사용 금지.
        Per-chunk Phase D 완료 시점 1회 호출.
        """
        self._ensure_backfill_metrics()
        self._bf_chunks_completed.labels(status=status).inc()

    def emit_backfill_put_latency(self, latency_s: float) -> None:
        """NASUploader.put() per-chunk latency emit — Histogram.

        Metrics (nas_backfill_* prefix — NFR-4 freeze):
        - nas_backfill_put_latency_seconds (Histogram, buckets [0.5,1.0,2.0,5.0,10.0,30.0])

        §6.2.1 Metric emission 박제.
        NFR-3 cross-reference: MCT-148 T2 baseline 50MB p99=2870.65ms (2.87s).
        per-chunk PUT 완료 시점 호출 (PutResult.latency_ms 로 환산).
        """
        self._ensure_backfill_metrics()
        self._bf_put_latency.observe(latency_s)

    def emit_backfill_legacy_node_default(self) -> None:
        """Legacy node= 부재 partition 의 node=DEFAULT 삽입 event emit — Counter.

        Metrics (nas_backfill_* prefix — NFR-4 freeze):
        - nas_backfill_legacy_node_default_count (Counter)

        S6 박제 enforcement marker.
        ADR-009 §D2.1: node= 부재 legacy partition → NAS PUT 시 node=DEFAULT 명시 삽입.
        _build_chunk_spec() 에서 is_legacy_node=True 검출 시 호출.
        """
        self._ensure_backfill_metrics()
        self._bf_legacy_node_default.inc()

    def emit_backfill_quarantine(self, fail_invariant: str) -> None:
        """Chunk quarantine event emit — Counter with fail_invariant label.

        Metrics (nas_backfill_* prefix — NFR-4 freeze):
        - nas_backfill_quarantine_count (Counter, labels: fail_invariant)

        §6.8 Wording SSOT: fail_invariant ∈ {
            "sha256_fail", "object_count_fail", "row_count_fail",
            "column_count_fail", "column_order_fail", "dtype_fail", "schema_version_fail"
        } (InvariantResult.status 7종 fail enum — MCT-151 §6.8 SSOT).
        variant 사용 금지.
        quarantine 결정 시점 (3 retry 소진 후) 호출.
        Alert consume: NASInvariantSchemaDriftDetected (MCT-151 land nas_invariant_rules.yml).
        """
        self._ensure_backfill_metrics()
        self._bf_quarantine.labels(fail_invariant=fail_invariant).inc()

    def emit_backfill_resumable(self, count: int) -> None:
        """Phase E exit 시점 pending+in_flight+sop_skipped chunk 수 emit — Counter.

        Metrics (nas_backfill_* prefix — NFR-4 freeze):
        - nas_backfill_resumable_count (Counter)

        AC-5 chaos test resumability evidence marker.
        BackfillOrchestrator.run() Phase E exit 시점 1회 호출.
        count > 0 이면 BackfillResult.status="checkpoint_resumable" 확인 의무.
        """
        self._ensure_backfill_metrics()
        self._bf_resumable.inc(count)
