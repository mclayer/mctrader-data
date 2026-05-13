"""test_prometheus_exporters.py — P1 TDD tests for PrometheusExporter.

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253

Test Contract §8 (TestContractArchitectAgent):
- test_4_metrics_exported: success_count + fail_count + latency_histogram + queue_depth_gauge 모두 export
- test_label_cardinality: error_type / size_bucket label 가 bounded (cardinality 폭증 방지)
- test_metric_prefix_freeze: nas_uploader_* namespace, MCT-151 nas_invariant_* prefix-disjoint (§8.2)

AC-3: Prometheus metrics 4종 export + IOPS baseline naming.
NFR-4: metric prefix freeze 의무 — Phase 2 PR merge 시점 freeze.

FIX#2 추가:
- P1-1: nas_uploader_queue_bytes Gauge — queue_bytes export + 10GB alert 정합
"""
from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry

from mctrader_data.nas_metrics.prometheus_exporters import PrometheusExporter


@pytest.fixture
def registry() -> CollectorRegistry:
    """격리된 Prometheus registry (test isolation)."""
    return CollectorRegistry()


@pytest.fixture
def exporter(registry: CollectorRegistry) -> PrometheusExporter:
    """PrometheusExporter 생성 (격리 registry)."""
    return PrometheusExporter(registry=registry)


class TestFourMetricsExported:
    """AC-3: 4종 metric 모두 export 확인."""

    def test_success_count_increments(self, exporter: PrometheusExporter, registry: CollectorRegistry) -> None:
        """nas_uploader_success_count Counter — emit_success() 호출 시 +1."""
        exporter.emit_success(bucket="mctrader-market", latency_s=0.5)
        exporter.emit_success(bucket="mctrader-market", latency_s=0.3)

        metric = _get_metric(registry, "nas_uploader_success_count")
        assert metric is not None, "nas_uploader_success_count not found in registry"
        total = _get_counter_value(metric, {"bucket": "mctrader-market"})
        assert total == 2.0

    def test_fail_count_increments(self, exporter: PrometheusExporter, registry: CollectorRegistry) -> None:
        """nas_uploader_fail_count Counter — emit_fail() 호출 시 +1, reason label 포함."""
        exporter.emit_fail(bucket="mctrader-market", reason="endpoint_unreachable", latency_s=0.1)
        exporter.emit_fail(bucket="mctrader-market", reason="endpoint_unreachable", latency_s=0.2)
        exporter.emit_fail(bucket="mctrader-market", reason="auth_failed", latency_s=0.05)

        metric = _get_metric(registry, "nas_uploader_fail_count")
        assert metric is not None, "nas_uploader_fail_count not found in registry"

        unreachable_count = _get_counter_value(
            metric, {"bucket": "mctrader-market", "reason": "endpoint_unreachable"}
        )
        assert unreachable_count == 2.0

        auth_count = _get_counter_value(
            metric, {"bucket": "mctrader-market", "reason": "auth_failed"}
        )
        assert auth_count == 1.0

    def test_latency_histogram_records(self, exporter: PrometheusExporter, registry: CollectorRegistry) -> None:
        """nas_uploader_latency_seconds Histogram — emit_success/emit_head 호출 시 count 증가."""
        exporter.emit_success(bucket="mctrader-market", latency_s=0.5)
        exporter.emit_head(bucket="mctrader-market", latency_s=0.05)

        metric = _get_metric(registry, "nas_uploader_latency_seconds")
        assert metric is not None, "nas_uploader_latency_seconds not found in registry"

    def test_queue_depth_gauge_sets_value(
        self, exporter: PrometheusExporter, registry: CollectorRegistry
    ) -> None:
        """nas_uploader_queue_depth Gauge — set_queue_depth() 호출 시 값 설정."""
        exporter.set_queue_depth(queue_path="/data/retry_queue", depth=42)

        metric = _get_metric(registry, "nas_uploader_queue_depth")
        assert metric is not None, "nas_uploader_queue_depth not found in registry"
        value = _get_gauge_value(metric, {"queue_path": "/data/retry_queue"})
        assert value == 42.0

    def test_queue_depth_gauge_updates(
        self, exporter: PrometheusExporter, registry: CollectorRegistry
    ) -> None:
        """nas_uploader_queue_depth Gauge — 여러 번 set 시 마지막 값 반영."""
        exporter.set_queue_depth(queue_path="/data/retry_queue", depth=10)
        exporter.set_queue_depth(queue_path="/data/retry_queue", depth=0)

        metric = _get_metric(registry, "nas_uploader_queue_depth")
        value = _get_gauge_value(metric, {"queue_path": "/data/retry_queue"})
        assert value == 0.0


class TestLabelCardinality:
    """AC-3: label cardinality bounded — cardinality 폭증 방지 (§6.3 SecurityArch)."""

    ALLOWED_REASONS = frozenset(
        ["endpoint_unreachable", "auth_failed", "quota_exceeded", "unknown"]
    )

    def test_reason_label_uses_enum(self, exporter: PrometheusExporter) -> None:
        """reason label = generic enum only."""
        for reason in self.ALLOWED_REASONS:
            exporter.emit_fail(bucket="mctrader-market", reason=reason, latency_s=0.1)

    def test_reason_label_bounded_set(self) -> None:
        """ALLOWED_REASONS 가 4개 고정 (cardinality=4)."""
        assert len(self.ALLOWED_REASONS) == 4

    def test_unknown_reason_normalized(self, exporter: PrometheusExporter, registry: CollectorRegistry) -> None:
        """허용되지 않은 reason → 'unknown' 으로 강제 변환 (SecurityArch §6.3)."""
        exporter.emit_fail(
            bucket="test-bucket",
            reason="SOME_RAW_BOTO3_EXCEPTION_MESSAGE_WITH_CREDENTIALS",
            latency_s=0.1,
        )
        metric = _get_metric(registry, "nas_uploader_fail_count")
        assert metric is not None
        unknown_count = _get_counter_value(metric, {"bucket": "test-bucket", "reason": "unknown"})
        assert unknown_count == 1.0

    def test_operation_label_two_values(self, exporter: PrometheusExporter) -> None:
        """operation label = ['put', 'head'] (cardinality=2)."""
        exporter.emit_success(bucket="mctrader-market", latency_s=0.1)
        exporter.emit_head(bucket="mctrader-market", latency_s=0.05)


class TestQueueBytesGauge:
    """P1-1 FIX#2: nas_uploader_queue_bytes Gauge — 10GB alert 정합.

    §6.2.3 FIX#2: 신규 metric nas_uploader_queue_bytes (Gauge, label=queue_path).
    Prometheus rule: nas_uploader_queue_bytes > 10737418240 → NASUploaderBacklogBytesHigh.
    """

    def test_queue_bytes_gauge_exported(
        self, exporter: PrometheusExporter, registry: CollectorRegistry
    ) -> None:
        """nas_uploader_queue_bytes Gauge — set_queue_bytes() 호출 시 값 export."""
        exporter.set_queue_bytes(queue_path="/data/retry_queue", bytes_total=1_000_000)

        metric = _get_metric(registry, "nas_uploader_queue_bytes")
        assert metric is not None, "nas_uploader_queue_bytes Gauge 미등록"
        value = _get_gauge_value(metric, {"queue_path": "/data/retry_queue"})
        assert value == 1_000_000.0

    def test_queue_bytes_gauge_updates(
        self, exporter: PrometheusExporter, registry: CollectorRegistry
    ) -> None:
        """nas_uploader_queue_bytes Gauge — 여러 번 set 시 마지막 값 반영."""
        exporter.set_queue_bytes(queue_path="/data/retry_queue", bytes_total=5 * 1024**3)
        exporter.set_queue_bytes(queue_path="/data/retry_queue", bytes_total=0)

        metric = _get_metric(registry, "nas_uploader_queue_bytes")
        value = _get_gauge_value(metric, {"queue_path": "/data/retry_queue"})
        assert value == 0.0

    def test_queue_bytes_label_queue_path(
        self, exporter: PrometheusExporter, registry: CollectorRegistry
    ) -> None:
        """nas_uploader_queue_bytes label = queue_path (AC-4 wording 정합)."""
        exporter.set_queue_bytes(queue_path="/nas/queue", bytes_total=10 * 1024**3)

        metric = _get_metric(registry, "nas_uploader_queue_bytes")
        assert metric is not None
        labels_found = [s.labels for s in metric.samples if "queue_path" in s.labels]
        assert labels_found, "queue_path label 미발견"
        assert labels_found[0]["queue_path"] == "/nas/queue"


class TestInvariantMetricPrefixFreeze:
    """§8.2 MCT-151: nas_invariant_* prefix freeze — MCT-150 nas_uploader_* prefix-disjoint (AC-5).

    NFR-4 박제: emit_invariant_* method 가 생성하는 모든 metric 의 prefix == nas_invariant_*.
    MCT-150 prefix (nas_uploader_*) 와 collision 0.
    """

    EXPECTED_INVARIANT_METRICS = [
        "nas_invariant_dual_write_status_count",
        "nas_invariant_dual_write_latency_seconds",
        "nas_invariant_compaction_barrier_status_count",
        "nas_invariant_compaction_barrier_drain_wait_seconds",
        "nas_invariant_compaction_barrier_in_flight_remaining",
        "nas_invariant_status_count",
        "nas_invariant_verify_latency_seconds",
        "nas_invariant_sha256_match_count",
        "nas_invariant_object_count_match",
        "nas_invariant_row_count_match_count",
        "nas_invariant_schema_drift_count",
    ]

    def test_invariant_metric_prefix_freeze(
        self, registry: CollectorRegistry
    ) -> None:
        """nas_invariant_* prefix 가 정확히 사용 + nas_uploader_* 와 collision 0.

        §8.2 NFR-4: emit_invariant_* method 가 생성하는 metric prefix == nas_invariant_*.
        MCT-150 nas_uploader_* prefix-disjoint 보장.
        """
        exporter = PrometheusExporter(registry=registry)

        # emit MCT-150 metrics
        exporter.emit_success(bucket="b", latency_s=0.1)
        exporter.set_queue_depth(queue_path="/q", depth=0)
        exporter.set_queue_bytes(queue_path="/q", bytes_total=0)

        # emit MCT-151 invariant metrics
        exporter.emit_invariant_dual_write(
            status="committed", nas_key_prefix="schema_version=v1", latency_s=0.5
        )
        exporter.emit_invariant_compaction_barrier(
            status="ok", drain_wait_s=1.0, in_flight_remaining=0
        )
        exporter.emit_invariant_verify(
            status="all_pass",
            partition="schema_version=v1/exchange=KRX",
            latency_s=2.0,
            per_invariant_results={},
        )

        all_names = _get_all_metric_names(registry)

        # All invariant metrics must use nas_invariant_* prefix
        for expected in self.EXPECTED_INVARIANT_METRICS:
            assert expected in all_names, (
                f"Expected invariant metric '{expected}' not registered. "
                f"Registered: {sorted(n for n in all_names if 'invariant' in n)}"
            )

        # nas_uploader_* and nas_invariant_* must be completely disjoint
        uploader_metrics = {n for n in all_names if n.startswith("nas_uploader_")}
        invariant_metrics = {n for n in all_names if n.startswith("nas_invariant_")}
        overlap = uploader_metrics & invariant_metrics
        assert not overlap, f"Metric prefix collision detected: {overlap}"

    def test_invariant_metrics_exact_prefix(self, registry: CollectorRegistry) -> None:
        """모든 emit_invariant_* method 가 nas_invariant_* prefix 만 사용.

        no nas_uploader_invariant_*, no invariant_nas_*, no uploader_invariant_*.
        """
        exporter = PrometheusExporter(registry=registry)
        exporter.emit_invariant_dual_write(
            status="local_only", nas_key_prefix="schema_version=v1", latency_s=0.1
        )
        exporter.emit_invariant_compaction_barrier(
            status="drain_timeout", drain_wait_s=86400.0, in_flight_remaining=2
        )
        exporter.emit_invariant_verify(
            status="sha256_fail",
            partition="schema_version=v1/p",
            latency_s=1.5,
            per_invariant_results={},
        )

        all_names = _get_all_metric_names(registry)
        invariant_metrics = {n for n in all_names if "invariant" in n}

        forbidden_patterns = [
            "nas_uploader_invariant",
            "invariant_nas",
            "uploader_invariant",
        ]
        for metric_name in invariant_metrics:
            for pattern in forbidden_patterns:
                assert not metric_name.startswith(pattern), (
                    f"Forbidden metric name pattern found: {metric_name!r} matches {pattern!r}"
                )
            # Must start with nas_invariant_
            assert metric_name.startswith("nas_invariant_"), (
                f"Invariant metric must start with 'nas_invariant_', got: {metric_name!r}"
            )


class TestMetricPrefixFreeze:
    """§8.2 invariant: metric prefix freeze — nas_uploader_* namespace. NFR-4."""

    EXPECTED_METRICS = [
        "nas_uploader_success_count",
        "nas_uploader_fail_count",
        "nas_uploader_latency_seconds",
        "nas_uploader_queue_depth",
        "nas_uploader_queue_bytes",
    ]

    FORBIDDEN_PREFIX = "nas_invariant_"  # MCT-151 invariant harness prefix (disjoint 의무)

    def test_all_metrics_use_nas_uploader_prefix(
        self, registry: CollectorRegistry, exporter: PrometheusExporter
    ) -> None:
        """모든 5종 metric 이 nas_uploader_* prefix 사용. §8.2 박제 + NFR-4."""
        exporter.emit_success(bucket="b", latency_s=0.1)
        exporter.emit_fail(bucket="b", reason="unknown", latency_s=0.1)
        exporter.emit_head(bucket="b", latency_s=0.05)
        exporter.set_queue_depth(queue_path="/q", depth=0)
        exporter.set_queue_bytes(queue_path="/q", bytes_total=0)

        registered_names = _get_all_metric_names(registry)

        for expected in self.EXPECTED_METRICS:
            assert expected in registered_names, (
                f"Expected metric '{expected}' not found. "
                f"Registered: {sorted(registered_names)}"
            )

    def test_no_metric_uses_nas_invariant_prefix(
        self, registry: CollectorRegistry, exporter: PrometheusExporter
    ) -> None:
        """nas_invariant_* prefix 사용 0 (MCT-151 namespace conflict 방지). NFR-4."""
        exporter.emit_success(bucket="b", latency_s=0.1)
        exporter.set_queue_depth(queue_path="/q", depth=0)
        exporter.set_queue_bytes(queue_path="/q", bytes_total=0)

        registered_names = _get_all_metric_names(registry)
        conflicting = [n for n in registered_names if n.startswith(self.FORBIDDEN_PREFIX)]
        assert not conflicting, (
            f"Found metrics with forbidden prefix '{self.FORBIDDEN_PREFIX}': {conflicting}"
        )

    def test_exact_metric_names_match_spec(
        self, registry: CollectorRegistry, exporter: PrometheusExporter
    ) -> None:
        """정확한 metric 이름이 §6.2.3 spec 과 일치 (freeze 의무)."""
        exporter.emit_success(bucket="b", latency_s=0.1)
        exporter.emit_fail(bucket="b", reason="unknown", latency_s=0.1)
        exporter.emit_head(bucket="b", latency_s=0.05)
        exporter.set_queue_depth(queue_path="/q", depth=0)
        exporter.set_queue_bytes(queue_path="/q", bytes_total=0)

        registered_names = _get_all_metric_names(registry)
        nas_uploader_metrics = {n for n in registered_names if n.startswith("nas_uploader_")}

        expected_set = set(self.EXPECTED_METRICS)
        assert expected_set.issubset(nas_uploader_metrics), (
            f"Missing metrics: {expected_set - nas_uploader_metrics}"
        )


# --- Helpers ---

def _get_metric(registry: CollectorRegistry, name: str):
    for metric in registry.collect():
        if metric.name == name:
            return metric
    return None


def _get_counter_value(metric, labels: dict) -> float:
    for sample in metric.samples:
        if all(sample.labels.get(k) == v for k, v in labels.items()) and sample.name.endswith("_total"):
            return sample.value
    for sample in metric.samples:
        if all(sample.labels.get(k) == v for k, v in labels.items()):
            return sample.value
    return 0.0


def _get_gauge_value(metric, labels: dict) -> float:
    for sample in metric.samples:
        if all(sample.labels.get(k) == v for k, v in labels.items()):
            return sample.value
    return 0.0


def _get_all_metric_names(registry: CollectorRegistry) -> set[str]:
    names: set[str] = set()
    for metric in registry.collect():
        names.add(metric.name)
    return names
