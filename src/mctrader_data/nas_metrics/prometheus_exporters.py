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
# status ∈ {committed, local_only, hard_floor_blocked}, tier ∈ {L1, L2, L3}
# ADR-027 D5 amendment caller contract — caller (CompactorRunner._dispatch_dual_write) emit.
# MCT-168: tier label 확장 — L1 추가 (ADR-029 D1+D2, AC-6 정합)
dual_write_result_total = Counter(
    "mctrader_dual_write_result_total",
    "DualWriter put() result count by status and tier (MCT-156, MCT-168 L1 추가)",
    ["status", "tier"],  # status ∈ {committed, local_only, hard_floor_blocked}, tier ∈ {L1, L2, L3}
)

# MCT-168: L1 NAS PUT latency histogram (AC-8 NFR: p99 < 1500ms)
# buckets: (0.1, 0.25, 0.5, 1.0, 1.5, 3.0, 10.0) — 1500ms 버킷 포함 (NFR gate 정합)
# ADR-029 D1=B + D2=B wiring evidence — DualWriter.put_l1() caller emit.
dual_write_l1_latency_seconds = Histogram(
    "mctrader_dual_write_l1_latency_seconds",
    "L1 NAS DualWriter PUT latency in seconds (MCT-168 AC-8, NFR p99 < 1500ms)",
    buckets=(0.1, 0.25, 0.5, 1.0, 1.5, 3.0, 10.0),
)

# MCT-162 (ADR-027 D4 amendment, 2026-05-13)
# L1Compactor unsupported channel encounter = fail-fast invariant (silent skip 차단)
# cardinality bounded low — collector emit channel 종류만 (attacker-controlled injection path 0)
compactor_unsupported_channel_total = Counter(
    "mctrader_compactor_unsupported_channel_total",
    "L1Compactor unsupported channel encountered (MCT-162 fail-fast, silent skip 차단)",
    ["channel"],  # cardinality bounded low — collector emit channel 종류만
)

# MCT-160 D4 + R-EXTRA: L2/L3 post-write monotonic verify 실패 시 quarantine event Counter
# tier ∈ {L2, L3}, reason ∈ {monotonic_violation, ...}
compactor_quarantine_total = Counter(
    "mctrader_compactor_quarantine_total",
    "L2/L3 quarantine events by tier and reason (MCT-160 D4)",
    ["tier", "reason"],  # tier ∈ {L2, L3}, reason ∈ {monotonic_violation}
)

# MCT-160 F4 fix: AC-6/D7 malformed orderbookdepth frame validation Counter
# channel ∈ {orderbookdepth, ...}, exchange ∈ {bithumb, ...}
compactor_malformed_frame_total = Counter(
    "mctrader_compactor_malformed_frame_total",
    "L1Compactor malformed frame encountered (MCT-160 AC-6)",
    ["channel", "exchange"],
)

# U2-HELPER: nas_key helper call counter (ADR-034 §결정 6 Monitoring SSOT, AC-EMIT)
# caller label: 6 caller sites verbatim (active 10 / max 18 cardinality, Amendment 4 박제)
# caller ∈ {dual_writer_put_l1, runner_dispatch_dual_write, runner_cleanup,
#            runner_historical_dual_write, l2_compactor_get_source, l3_compactor_get_source}
# tier ∈ {L1, L2, L3, unknown} (unknown = malformed-path safety sentinel, F-codex-3 박제)
nas_key_helper_call_total = Counter(
    "mctrader_nas_key_helper_call_total",
    "Number of nas_key helper invocations (ADR-034 §Monitoring SSOT compliance, U2-HELPER)",
    labelnames=("caller", "tier"),
)

# INCIDENT-2026-05-17 amendment (ADR-027 §D5 amend): NAS PUT 4xx operational alert
# 4xx (auth/policy/quota 영구 오류) silent fallback 차단 surface — retry_queue 흡수 금지 의무.
# tier ∈ {L1, L2, L3, unknown} (unknown = nas_uploader 직접 호출 경로의 default sentinel)
# reason ∈ {auth_failed, policy_denied, quota_exceeded, bucket_missing} bounded low cardinality
nas_put_operational_alert_total = Counter(
    "mctrader_nas_put_operational_alert_total",
    "NAS PUT 4xx operational alert (ADR-027 INCIDENT-2026-05-17 amendment, silent fallback 차단)",
    labelnames=("tier", "reason"),
)

if TYPE_CHECKING:
    pass


# ─── U3-MIGRATE: l1/ re-key Prometheus metrics (ADR-034 §결정 4 Monitoring + §9.4.6 SSOT) ──
# cardinality budget ≤ 50 (active 24: 2 exchange × 3 channel × 4 head_check + sparse others)
# INV-L carrier: mctrader_l1_rekey_* prefix disjoint from nas_uploader_* / nas_invariant_* / nas_backfill_*

# Counter (5 신설)
# mode label ∈ {dry_run, live} — R-DM-4 carrier (dry-run Counter false-positive 차단)
l1_rekey_copied_total = Counter(
    "mctrader_l1_rekey_copied_total",
    "Number of l1/ objects copied to flat layout (Step A complete, U3-MIGRATE ADR-034 §결정 4)",
    labelnames=("exchange", "channel", "mode"),
)

# head_check ∈ {etag, version_id, sha256, content_length} — 4-HEAD verify per-axis Counter
# active 24 cardinality: 2 exchange × 3 channel × 4 head_check = 24 (INV-L VERIFIED budget)
l1_rekey_verified_total = Counter(
    "mctrader_l1_rekey_verified_total",
    "Number of 4-HEAD verify passes (ADR-034 §결정 4 Step B, per head_check axis)",
    labelnames=("exchange", "channel", "head_check"),
)

l1_rekey_deleted_total = Counter(
    "mctrader_l1_rekey_deleted_total",
    "Number of l1/ objects deleted (Step C complete, U3-MIGRATE ADR-034 §결정 4)",
    labelnames=("exchange", "channel", "mode"),
)

l1_rekey_skipped_already_migrated_total = Counter(
    "mctrader_l1_rekey_skipped_already_migrated_total",
    "Number of partitions skipped due to sentinel hit (idempotent re-run, U3-MIGRATE INV-C)",
    labelnames=("exchange", "channel"),
)

# reason enum (9종 — P2-2 advisory: active subset sparse, ADR-046 active vs declared cross-ref 의무):
# versioning_not_enabled / head1_etag_mismatch / head2_versionid_absent /
# head3_sha256_mismatch / head4_contentlength_mismatch / legacy_no_sha256 /
# concurrent_lock / disk_full / boto3_error
l1_rekey_failed_total = Counter(
    "mctrader_l1_rekey_failed_total",
    "Number of partitions failed during re-key (U3-MIGRATE AC-7 silent-skip 0)",
    labelnames=("exchange", "channel", "reason"),
)

# Gauge (1 신설 — O-R1 / INV-F carrier)
# > 0 for > 5 min = P0 alert (Grafana: mctrader_l1_rekey_partial_state_count > 0 for 5m)
l1_rekey_partial_state_count = Gauge(
    "mctrader_l1_rekey_partial_state_count",
    # > 0 for 5min = P0 alert (U3-MIGRATE INV-F / O-R1)
    "Partitions in partial_state (copy done, delete pending) — P0 if > 5 min",
    labelnames=("exchange", "channel"),
)

# Histogram (1 신설 — perf baseline carrier, per-batch p99 < 60s SLO)
l1_rekey_batch_duration_seconds = Histogram(
    "mctrader_l1_rekey_batch_duration_seconds",
    "Per-batch duration in seconds — p99 < 60s SLO (U3-MIGRATE §13.C PROVISIONAL gate carrier)",
    labelnames=("exchange", "channel"),
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
)

# §6.3: reason label 화이트리스트 (cardinality=4, SecurityArch 박제)
ALLOWED_REASONS = frozenset(
    ["endpoint_unreachable", "auth_failed", "quota_exceeded", "unknown"]
)

# ─── MCT-171 신규 global metrics (capacity + invariant violation + ingest blocked) ─────
# AC-5 cardinality 제한: label enum hardcoded, free-form label 금지

# mctrader_capacity_usage_bytes{layer=<4 enum>} Gauge
# layer enum = LAYER_NAMES from capacity_probe.py SSOT
mctrader_capacity_usage_bytes = Gauge(
    "mctrader_capacity_usage_bytes",
    "4 layer capacity usage in bytes (MCT-171 D11 ADR-029)",
    ["layer"],  # 4 enum: WAL_local / L1_local / NAS_bucket / Host_disk
)

# mctrader_capacity_threshold_ratio{layer=<4 enum>} Gauge
mctrader_capacity_threshold_ratio = Gauge(
    "mctrader_capacity_threshold_ratio",
    "4 layer capacity threshold ratio 0.0-1.0 (MCT-171 D11)",
    ["layer"],  # 4 enum: WAL_local / L1_local / NAS_bucket / Host_disk
)

# mctrader_invariant_violation_total{invariant_name=<8 enum>} Counter
# invariant_name enum = _INVARIANT_NAMES from invariant_harness.py (8종)
mctrader_invariant_violation_total = Counter(
    "mctrader_invariant_violation_total",
    "Invariant violation count by invariant name (MCT-171 8종 통합)",
    # 8 enum: sha256/object_count/row_count/column_count/column_order/dtype/schema_version/ambiguity
    ["invariant_name"],
)

# mctrader_invariant_check_latency_ms Histogram (no label — latency distribution)
mctrader_invariant_check_latency_ms = Histogram(
    "mctrader_invariant_check_latency_ms",
    "InvariantHarness.verify() latency in ms (MCT-171 AC-1)",
    buckets=[1, 5, 10, 50, 100, 500, 1000, 5000, 30000],
)

# mctrader_ingest_blocked_total{reason=<3 enum>} Counter
# reason enum: wal_full / l1_full / nas_unreachable
mctrader_ingest_blocked_total = Counter(
    "mctrader_ingest_blocked_total",
    "Ingest block count by reason (MCT-171 D5 + D7-5=B)",
    ["reason"],  # 3 enum: wal_full / l1_full / nas_unreachable
)

# Cardinality 제한 enforce (AC-5 §D7-6)
_ALLOWED_LAYERS = frozenset(["WAL_local", "L1_local", "NAS_bucket", "Host_disk"])
_ALLOWED_INVARIANT_NAMES = frozenset([
    "sha256", "object_count", "row_count", "column_count",
    "column_order", "dtype", "schema_version", "ambiguity",
])
_ALLOWED_BLOCK_REASONS = frozenset(["wal_full", "l1_full", "nas_unreachable"])

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

    # ─── MCT-171 신규 method (capacity + invariant violation + ingest blocked) ──
    # AC-5: Prometheus metric 4종 + cardinality 제한 enforce (D7-6)

    def emit_capacity_usage(self, layer: str, bytes_val: int) -> None:
        """mctrader_capacity_usage_bytes{layer} Gauge set (MCT-171 AC-5).

        Args:
            layer: LAYER_NAMES enum (WAL_local/L1_local/NAS_bucket/Host_disk)
            bytes_val: usage in bytes

        Cardinality 제한: layer ∉ _ALLOWED_LAYERS → fail-fast assertion (R4 mitigation).
        """
        assert layer in _ALLOWED_LAYERS, (
            f"capacity_usage: invalid layer label '{layer}'. "
            f"Allowed: {_ALLOWED_LAYERS} (cardinality limit enforce, AC-5)"
        )
        mctrader_capacity_usage_bytes.labels(layer=layer).set(bytes_val)

    def emit_capacity_ratio(self, layer: str, ratio: float) -> None:
        """mctrader_capacity_threshold_ratio{layer} Gauge set (MCT-171 AC-5).

        Args:
            layer: LAYER_NAMES enum
            ratio: 0.0 ~ 1.0+ (usage / hard_limit)

        Cardinality 제한: layer ∉ _ALLOWED_LAYERS → fail-fast.
        """
        assert layer in _ALLOWED_LAYERS, (
            f"capacity_ratio: invalid layer label '{layer}'. "
            f"Allowed: {_ALLOWED_LAYERS} (cardinality limit enforce, AC-5)"
        )
        mctrader_capacity_threshold_ratio.labels(layer=layer).set(ratio)

    def emit_invariant_violation(self, invariant_name: str) -> None:
        """mctrader_invariant_violation_total{invariant_name} Counter +1 (MCT-171 AC-1).

        Args:
            invariant_name: _INVARIANT_NAMES 8 enum

        Cardinality 제한: invariant_name ∉ _ALLOWED_INVARIANT_NAMES → fail-fast.
        """
        assert invariant_name in _ALLOWED_INVARIANT_NAMES, (
            f"invariant_violation: invalid invariant_name '{invariant_name}'. "
            f"Allowed: {_ALLOWED_INVARIANT_NAMES} (cardinality limit enforce, AC-5)"
        )
        mctrader_invariant_violation_total.labels(invariant_name=invariant_name).inc()

    def emit_invariant_check_latency(self, latency_ms: float) -> None:
        """mctrader_invariant_check_latency_ms Histogram observe (MCT-171 AC-1).

        Args:
            latency_ms: InvariantHarness.verify() latency in ms
        """
        mctrader_invariant_check_latency_ms.observe(latency_ms)

    def emit_ingest_blocked(self, reason: str) -> None:
        """mctrader_ingest_blocked_total{reason} Counter +1 (MCT-171 AC-3).

        Args:
            reason: block reason enum (wal_full/l1_full/nas_unreachable)

        Cardinality 제한: reason ∉ _ALLOWED_BLOCK_REASONS → fail-fast.
        """
        assert reason in _ALLOWED_BLOCK_REASONS, (
            f"ingest_blocked: invalid reason '{reason}'. "
            f"Allowed: {_ALLOWED_BLOCK_REASONS} (cardinality limit enforce, AC-5)"
        )
        mctrader_ingest_blocked_total.labels(reason=reason).inc()
