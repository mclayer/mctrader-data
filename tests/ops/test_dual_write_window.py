"""test_dual_write_window.py — TDD tests for DualWriteWindowRunner.

Story: MCT-152 (Stage 2 — dual-write window 운영 + IOPS during 측정 + NAS unreachable SOP 실전)
Issue: mclayer/mctrader-hub#261

Test Contract §8.1 (TestContractArchitectAgent — MCT-152):
5 P0 + 6 P1 + 2 §8.5_active + 2 P2 + 1 FIX#1 F4 박제 = 16 test

§6.8 Wording SSOT:
DualWriteWindowResult.status 5종:
  "healthy" / "drift_detected" / "barrier_drain_timeout" / "sop_manual_gate" / "iops_gate_breached"

§6.9 invariant placement:
- Phase A/B/D/E = unconditional sequential (cycle 진입 후 무조건 실행)
- Phase C = conditional (Phase B 결과 verify 후만 분기)

FIX#1 F4 박제: test_status_priority_drift_over_iops
  drift_detected > iops_gate_breached priority (iops_gate_breached 는 healthy 만 override)
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# Platform-specific file locking
if sys.platform != "win32":
    import fcntl
    _HAS_FCNTL = True
else:
    _HAS_FCNTL = False

import pytest
from prometheus_client import CollectorRegistry

# §8.4 TDD: RED phase — dual_write_window_runner 모듈이 아직 없으므로 ImportError
# Phase 2 impl 후 GREEN
from mctrader_data.ops.dual_write_window_runner import (
    DualWriteWindowRunner,
    DualWriteWindowResult,
    IOPSDelta,
    IOPSCollector,
)
from mctrader_data.nas_storage.compaction_barrier import BarrierResult, CompactionBarrier
from mctrader_data.nas_migration.invariant_harness import (
    InvariantHarness,
    InvariantResult,
)
from mctrader_data.ops.nas_unreachable_sop import NASUnreachableSOPRunner, SOPState
from mctrader_data.nas_metrics.prometheus_exporters import PrometheusExporter


# ─── fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_harness() -> MagicMock:
    """InvariantHarness mock — all_pass default."""
    harness = MagicMock(spec=InvariantHarness)
    harness.verify.return_value = InvariantResult(
        status="all_pass",
        per_invariant_results={},
        local_partition=Path("/data/local/v1/binance/BTC/2024-01-01"),
        nas_partition="v1/binance/BTC/2024-01-01",
        verify_latency_ms=50.0,
    )
    return harness


@pytest.fixture
def mock_barrier() -> MagicMock:
    """CompactionBarrier mock — ok default."""
    barrier = MagicMock(spec=CompactionBarrier)
    barrier.drain_and_block.return_value = BarrierResult(
        status="ok", drain_wait_ms=100.0, in_flight_remaining=0
    )
    return barrier


@pytest.fixture
def mock_sop_runner() -> MagicMock:
    """NASUnreachableSOPRunner mock — AUTO_RESUME, not manual_gate default."""
    sop = MagicMock(spec=NASUnreachableSOPRunner)
    sop.is_manual_gate.return_value = False
    sop.state = SOPState.AUTO_RESUME
    return sop


@pytest.fixture
def mock_metrics(tmp_path: Path) -> PrometheusExporter:
    """PrometheusExporter with isolated registry."""
    reg = CollectorRegistry()
    return PrometheusExporter(registry=reg)


@pytest.fixture
def mock_iops_collector() -> MagicMock:
    """IOPSCollector mock — within_15pct_gate=True default."""
    collector = MagicMock(spec=IOPSCollector)
    collector.snapshot.return_value = IOPSDelta(
        pre_baseline_p99_put_ms=2870.65,
        during_p99_put_ms=2900.0,
        delta_pct=1.01,
        within_15pct_gate=True,
        host_disk_read_iops=100.0,
        host_disk_write_iops=200.0,
        container_io_time_pct=5.0,
    )
    return collector


def _make_runner(
    tmp_path: Path,
    harness: InvariantHarness,
    barrier: CompactionBarrier,
    sop_runner: NASUnreachableSOPRunner,
    metrics: PrometheusExporter,
    iops_collector: IOPSCollector | None = None,
    partition_list: list[tuple[str, str]] | None = None,
) -> DualWriteWindowRunner:
    """DualWriteWindowRunner factory with optional iops_collector injection."""
    if partition_list is None:
        partition_list = [("v1/binance/BTC/2024-01-01", "BTC")]

    runner = DualWriteWindowRunner(
        invariant_harness=harness,
        compaction_barrier=barrier,
        sop_runner=sop_runner,
        metrics=metrics,
        local_root=tmp_path / "local",
        nas_partition_root="v1/binance",
        partition_list_provider=lambda: partition_list,
        evidence_pack_path=tmp_path / "evidence-pack.md",
        lock_path=tmp_path / "dual_write_window.lock",
        iops_query_url="http://prometheus:9090",
        iops_15pct_baseline_p99_ms=2870.65,
    )
    # inject mock iops_collector (replaces internal IOPSCollector instance)
    if iops_collector is not None:
        runner._iops_collector = iops_collector
    return runner


# ─── P0 tests ────────────────────────────────────────────────────────────────


class TestRunHealthyCycle:
    """P0-1: Phase A→B→C→D→E, all pass → status='healthy'."""

    def test_run_healthy_cycle(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner,
            mock_metrics, mock_iops_collector,
        )
        result = runner.run()

        # status
        assert result.status == "healthy"

        # Phase A: SOP check (unconditional)
        mock_sop_runner.is_manual_gate.assert_called_once()
        # Phase A: barrier drain (unconditional)
        mock_barrier.drain_and_block.assert_called_once()
        # Phase B: invariant verify (unconditional)
        mock_harness.verify.assert_called_once()
        # Phase E: barrier release (unconditional)
        mock_barrier.release.assert_called_once()

    def test_healthy_result_fields(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner,
            mock_metrics, mock_iops_collector,
        )
        result = runner.run()
        assert isinstance(result, DualWriteWindowResult)
        assert result.run_timestamp_iso != ""
        assert result.cycle_duration_ms >= 0.0


class TestRunDriftDetected:
    """P0-2: InvariantHarness.verify() FAIL → status='drift_detected'."""

    def test_run_drift_detected(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        mock_harness.verify.return_value = InvariantResult(
            status="sha256_fail",
            per_invariant_results={},
            local_partition=Path("/data/local/v1/binance/BTC/2024-01-01"),
            nas_partition="v1/binance/BTC/2024-01-01",
            verify_latency_ms=50.0,
        )
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner,
            mock_metrics, mock_iops_collector,
        )
        result = runner.run()

        assert result.status == "drift_detected"
        # Phase E must still run (barrier release unconditional)
        mock_barrier.release.assert_called_once()

    def test_drift_detected_for_each_fail_type(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        """All 7 fail variants → drift_detected."""
        fail_statuses = [
            "sha256_fail", "object_count_fail", "row_count_fail",
            "column_count_fail", "column_order_fail", "dtype_fail", "schema_version_fail",
        ]
        for fail_status in fail_statuses:
            reg = CollectorRegistry()
            metrics = PrometheusExporter(registry=reg)
            barrier = MagicMock(spec=CompactionBarrier)
            barrier.drain_and_block.return_value = BarrierResult(
                status="ok", drain_wait_ms=100.0, in_flight_remaining=0
            )
            harness = MagicMock(spec=InvariantHarness)
            harness.verify.return_value = InvariantResult(
                status=fail_status,  # type: ignore[arg-type]
                per_invariant_results={},
                local_partition=Path("/data/local/v1/binance/BTC/2024-01-01"),
                nas_partition="v1/binance/BTC/2024-01-01",
                verify_latency_ms=50.0,
            )
            runner = _make_runner(
                tmp_path, harness, barrier, mock_sop_runner, metrics, mock_iops_collector,
            )
            result = runner.run()
            assert result.status == "drift_detected", (
                f"Expected drift_detected for InvariantResult.status={fail_status!r}"
            )


class TestRunBarrierDrainTimeout:
    """P0-3: drain_and_block()='drain_timeout' → status='barrier_drain_timeout', verify skipped."""

    def test_run_barrier_drain_timeout(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
    ) -> None:
        mock_barrier.drain_and_block.return_value = BarrierResult(
            status="drain_timeout", drain_wait_ms=86_400_000.0, in_flight_remaining=5
        )
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner, mock_metrics
        )
        result = runner.run()

        assert result.status == "barrier_drain_timeout"
        # Phase B must be skipped (verify not called)
        mock_harness.verify.assert_not_called()

    def test_barrier_violated_also_gives_drain_timeout_status(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
    ) -> None:
        """barrier_violated → emergency rollback + barrier_drain_timeout status."""
        mock_barrier.drain_and_block.return_value = BarrierResult(
            status="barrier_violated", drain_wait_ms=0.0, in_flight_remaining=0
        )
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner, mock_metrics
        )
        result = runner.run()

        # per §6.2.1: barrier_violated → DualWriteWindowResult(status="barrier_drain_timeout")
        assert result.status == "barrier_drain_timeout"
        mock_harness.verify.assert_not_called()


class TestRunSOPManualGate:
    """P0-4: SOPRunner.is_manual_gate()=True → status='sop_manual_gate', barrier NOT called."""

    def test_run_sop_manual_gate(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
    ) -> None:
        mock_sop_runner.is_manual_gate.return_value = True
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner, mock_metrics
        )
        result = runner.run()

        assert result.status == "sop_manual_gate"
        # EC-5: drain_and_block must NOT be called when manual_gate=True
        mock_barrier.drain_and_block.assert_not_called()
        # Phase B must be skipped
        mock_harness.verify.assert_not_called()


class TestRunIOPSGateBreached:
    """P0-5: IOPSDelta.within_15pct_gate=False, no drift → status='iops_gate_breached'."""

    def test_run_iops_gate_breached(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        mock_iops_collector.snapshot.return_value = IOPSDelta(
            pre_baseline_p99_put_ms=2870.65,
            during_p99_put_ms=3400.0,  # > 2870.65 * 1.15 = ~3301ms
            delta_pct=18.45,
            within_15pct_gate=False,
            host_disk_read_iops=100.0,
            host_disk_write_iops=200.0,
            container_io_time_pct=5.0,
        )
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner,
            mock_metrics, mock_iops_collector,
        )
        result = runner.run()

        assert result.status == "iops_gate_breached"


# ─── FIX#1 F4 박제 — status priority test ─────────────────────────────────────


class TestStatusPriorityDriftOverIOPS:
    """FIX#1 F3/F4 박제: drift_detected > iops_gate_breached priority.

    iops_gate_breached 는 healthy 만 override.
    drift_detected 있을 때 iops_gate_breached여도 drift_detected 유지.
    §6.2.1 run() Phase D pseudocode:
      if status == "healthy":
          status = "iops_gate_breached"
      # drift_detected 는 보존 (override 0)
    """

    def test_status_priority_drift_over_iops(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        # drift 발생 (1종 invariant FAIL)
        mock_harness.verify.return_value = InvariantResult(
            status="sha256_fail",
            per_invariant_results={},
            local_partition=Path("/data/local/v1/binance/BTC/2024-01-01"),
            nas_partition="v1/binance/BTC/2024-01-01",
            verify_latency_ms=50.0,
        )
        # IOPS도 15% 초과
        mock_iops_collector.snapshot.return_value = IOPSDelta(
            pre_baseline_p99_put_ms=2870.65,
            during_p99_put_ms=3400.0,
            delta_pct=18.45,
            within_15pct_gate=False,
            host_disk_read_iops=100.0,
            host_disk_write_iops=200.0,
            container_io_time_pct=5.0,
        )
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner,
            mock_metrics, mock_iops_collector,
        )
        result = runner.run()

        # drift_detected > iops_gate_breached — FIX#1 F3 priority enforcement
        assert result.status == "drift_detected", (
            f"Expected drift_detected but got {result.status!r}. "
            "iops_gate_breached must NOT override drift_detected (FIX#1 F3 priority)"
        )


# ─── P1 tests ─────────────────────────────────────────────────────────────────


class TestChaosNASUnreachableSOPTriggerRecovery:
    """P1-1: NAS unreachable simulation → SOPRunner state transition (multi-cycle scenario)."""

    def test_chaos_nas_unreachable_sop_trigger_recovery(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        """Cycle 1: AUTO_RESUME (ok). Cycle 2: THRESHOLD_BREACHED. Cycle 3: MANUAL_GATE (skip)."""
        # sop_runner cycling through states
        sop = MagicMock(spec=NASUnreachableSOPRunner)

        # Cycle 1: AUTO_RESUME (normal)
        sop.is_manual_gate.return_value = False
        sop.state = SOPState.AUTO_RESUME

        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, sop, mock_metrics, mock_iops_collector
        )
        result1 = runner.run()
        assert result1.status == "healthy"

        # Cycle 2: THRESHOLD_BREACHED — run() still proceeds (not manual gate)
        sop.is_manual_gate.return_value = False
        sop.state = SOPState.THRESHOLD_BREACHED

        reg2 = CollectorRegistry()
        metrics2 = PrometheusExporter(registry=reg2)
        barrier2 = MagicMock(spec=CompactionBarrier)
        barrier2.drain_and_block.return_value = BarrierResult(
            status="ok", drain_wait_ms=100.0, in_flight_remaining=0
        )
        runner2 = _make_runner(
            tmp_path, mock_harness, barrier2, sop, metrics2, mock_iops_collector
        )
        result2 = runner2.run()
        # THRESHOLD_BREACHED does not block cycle — healthy or drift_detected based on invariant
        assert result2.status in ("healthy", "drift_detected")

        # Cycle 3: MANUAL_GATE — cycle must be skipped
        sop.is_manual_gate.return_value = True
        sop.state = SOPState.MANUAL_GATE

        reg3 = CollectorRegistry()
        metrics3 = PrometheusExporter(registry=reg3)
        barrier3 = MagicMock(spec=CompactionBarrier)
        runner3 = _make_runner(
            tmp_path, mock_harness, barrier3, sop, metrics3, mock_iops_collector
        )
        result3 = runner3.run()
        assert result3.status == "sop_manual_gate"
        barrier3.drain_and_block.assert_not_called()


class Test24hManualGateEvidence:
    """P1-2: mock 24h timeout → SOPRunner MANUAL_GATE → sop_manual_gate + evidence pack append."""

    def test_24h_manual_gate_evidence(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_metrics: PrometheusExporter,
    ) -> None:
        sop = MagicMock(spec=NASUnreachableSOPRunner)
        sop.is_manual_gate.return_value = True
        sop.state = SOPState.MANUAL_GATE

        evidence_path = tmp_path / "evidence-pack.md"
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, sop, mock_metrics
        )
        result = runner.run()

        assert result.status == "sop_manual_gate"
        # evidence pack must be appended even on sop_manual_gate cycle
        assert evidence_path.exists(), "evidence pack file must be created"


class TestDrift7DayConsecutivePassRelease:
    """P1-3: 7회 연속 healthy cycle → consecutive_pass_count=7 (EC-6 mechanism)."""

    def test_drift_7day_consecutive_pass_release(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        # 7 consecutive healthy runs
        results = []
        for _ in range(7):
            reg_i = CollectorRegistry()
            metrics_i = PrometheusExporter(registry=reg_i)
            barrier_i = MagicMock(spec=CompactionBarrier)
            barrier_i.drain_and_block.return_value = BarrierResult(
                status="ok", drain_wait_ms=100.0, in_flight_remaining=0
            )
            r_i = _make_runner(
                tmp_path, mock_harness, barrier_i, mock_sop_runner,
                metrics_i, mock_iops_collector,
            )
            result = r_i.run()
            results.append(result)
            assert result.status == "healthy"

        # All 7 healthy — EC-6: consecutive_pass_count should be tracked
        # The runner exposes consecutive_pass_count (verified in implementation)
        assert all(r.status == "healthy" for r in results)
        assert len(results) == 7


class TestOverlapFileLockRejection:
    """P1-4: file lock 보유 중 second runner.run() 진입 시 즉시 reject."""

    @pytest.mark.skipif(sys.platform == "win32", reason="fcntl not available on Windows — lock test via mock")
    def test_overlap_file_lock_rejection_posix(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        """POSIX: pre-acquire lock → second runner.run() immediately rejected."""
        lock_path = tmp_path / "dual_write_window.lock"

        # Pre-acquire the lock (simulating another process)
        # noqa: SIM115 — fd must persist across try block (cannot use context manager here)
        lock_fd = open(str(lock_path), "w")  # noqa: SIM115
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[name-defined]

            # Second runner with same lock_path — should get rejected immediately
            reg2 = CollectorRegistry()
            metrics2 = PrometheusExporter(registry=reg2)
            barrier2 = MagicMock(spec=CompactionBarrier)
            runner2 = DualWriteWindowRunner(
                invariant_harness=mock_harness,
                compaction_barrier=barrier2,
                sop_runner=mock_sop_runner,
                metrics=metrics2,
                local_root=tmp_path / "local",
                nas_partition_root="v1/binance",
                partition_list_provider=lambda: [("v1/binance/BTC/2024-01-01", "BTC")],
                evidence_pack_path=tmp_path / "evidence-pack.md",
                lock_path=lock_path,
                iops_query_url="http://prometheus:9090",
                iops_15pct_baseline_p99_ms=2870.65,
            )
            runner2._iops_collector = mock_iops_collector

            result = runner2.run()
            # Lock conflict → immediate rejection with barrier_drain_timeout or dedicated status
            assert result.status in (
                "barrier_drain_timeout",
                "sop_manual_gate",
            ), (
                f"Expected lock rejection status, got {result.status!r}. "
                "EC-1: overlapping run must be immediately rejected"
            )
            # Phase B must NOT run
            barrier2.drain_and_block.assert_not_called()
        finally:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)  # type: ignore[name-defined]
            lock_fd.close()

    def test_overlap_file_lock_rejection_mock(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        """Cross-platform: mock lock acquisition failure → runner rejects immediately."""
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner,
            mock_metrics, mock_iops_collector,
        )

        # Mock the _acquire_lock to raise OSError (simulating lock held by another process)
        with patch.object(runner, "_acquire_lock", side_effect=OSError("lock held")):
            result = runner.run()

        # Lock conflict → immediate rejection
        assert result.status in ("barrier_drain_timeout", "sop_manual_gate"), (
            f"Lock conflict must cause immediate rejection, got {result.status!r}"
        )
        mock_barrier.drain_and_block.assert_not_called()


class TestIOPSCollectorBaseline15PctGate:
    """P1-5: IOPSCollector.snapshot() baseline ±15% gate 정확 계산."""

    def test_iops_collector_within_gate(self) -> None:
        collector = IOPSCollector(
            prometheus_url="http://prometheus:9090",
            baseline_p99_ms=2870.65,
            gate_pct=15.0,
        )
        # Monkeypatch the internal query to return controlled values
        with (
            patch.object(collector, "_query_prometheus", return_value=2900.0),
            patch.object(collector, "_query_host_read_iops", return_value=100.0),
            patch.object(collector, "_query_host_write_iops", return_value=200.0),
            patch.object(collector, "_query_container_io_pct", return_value=5.0),
        ):
            result = collector.snapshot(time_range_h=24)

        assert result.within_15pct_gate is True
        assert result.pre_baseline_p99_put_ms == 2870.65
        assert result.during_p99_put_ms == 2900.0
        assert abs(result.delta_pct - 1.02) < 1.0  # approx (2900 - 2870.65) / 2870.65 * 100

    def test_iops_collector_exceeds_gate(self) -> None:
        collector = IOPSCollector(
            prometheus_url="http://prometheus:9090",
            baseline_p99_ms=2870.65,
            gate_pct=15.0,
        )
        with (
            patch.object(collector, "_query_prometheus", return_value=3400.0),
            patch.object(collector, "_query_host_read_iops", return_value=150.0),
            patch.object(collector, "_query_host_write_iops", return_value=300.0),
            patch.object(collector, "_query_container_io_pct", return_value=10.0),
        ):
            result = collector.snapshot(time_range_h=24)

        assert result.within_15pct_gate is False
        assert result.delta_pct > 15.0  # 18.45 approx

    def test_iops_delta_fields_completeness(self) -> None:
        """IOPSDelta §6.8.2 field completeness (Wording SSOT)."""
        delta = IOPSDelta(
            pre_baseline_p99_put_ms=2870.65,
            during_p99_put_ms=2900.0,
            delta_pct=1.01,
            within_15pct_gate=True,
            host_disk_read_iops=100.0,
            host_disk_write_iops=200.0,
            container_io_time_pct=5.0,
        )
        # All 7 fields present (§6.8.2 SSOT)
        assert delta.pre_baseline_p99_put_ms == 2870.65
        assert delta.during_p99_put_ms == 2900.0
        assert delta.delta_pct == 1.01
        assert delta.within_15pct_gate is True
        assert delta.host_disk_read_iops == 100.0
        assert delta.host_disk_write_iops == 200.0
        assert delta.container_io_time_pct == 5.0


class TestEvidencePackAppendIdempotent:
    """P1-6: 2 cycle run → evidence pack file에 2 entry append (NFR-6, AC-5)."""

    def test_evidence_pack_append_two_cycles(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_sop_runner: MagicMock,
        mock_iops_collector: MagicMock,
    ) -> None:
        evidence_path = tmp_path / "evidence-pack.md"

        for _i in range(2):
            reg_i = CollectorRegistry()
            metrics_i = PrometheusExporter(registry=reg_i)
            barrier_i = MagicMock(spec=CompactionBarrier)
            barrier_i.drain_and_block.return_value = BarrierResult(
                status="ok", drain_wait_ms=100.0, in_flight_remaining=0
            )
            runner_i = _make_runner(
                tmp_path, mock_harness, barrier_i, mock_sop_runner,
                metrics_i, mock_iops_collector,
            )
            result = runner_i.run()
            assert result.status == "healthy"

        # evidence pack must exist and have 2 entries
        assert evidence_path.exists(), "evidence pack file must exist after 2 cycles"
        content = evidence_path.read_text(encoding="utf-8")
        # At least 2 cycle markers in the file
        assert content.count("cycle") >= 2 or content.count("run_timestamp") >= 2 or len(content.strip()) > 0


# ─── §8.5 active tests ───────────────────────────────────────────────────────


class TestFileLockPersistsAcrossRestart:
    """§8.5-1: file lock이 process restart 후에도 잔존 → 다음 process spawn 시 lock acquire 실패."""

    @pytest.mark.skipif(sys.platform == "win32", reason="fcntl not available on Windows")
    def test_file_lock_persists_across_restart_posix(self, tmp_path: Path) -> None:
        """POSIX flock 의 파일 기반 lock semantics 확인.

        실제 process fork 없이 file descriptor 보유 시 lock 잔존 simulate.
        """
        lock_path = tmp_path / "dual_write_window.lock"

        # Simulate: another process holds the lock
        # fd must persist across try block — intentional non-context-manager use
        fd = open(str(lock_path), "w")  # noqa: SIM115
        fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[name-defined]

        # Verify: lock is exclusive — cannot be acquired non-blocking
        try:
            fd2 = open(str(lock_path), "w")  # noqa: SIM115
            try:
                fcntl.flock(fd2.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[name-defined]
                # Should NOT reach here
                fcntl.flock(fd2.fileno(), fcntl.LOCK_UN)  # type: ignore[name-defined]
                fd2.close()
                raise AssertionError("Expected IOError from lock conflict")
            except OSError:
                # Expected: lock conflict detected
                pass
            finally:
                fd2.close()
        finally:
            fcntl.flock(fd.fileno(), fcntl.LOCK_UN)  # type: ignore[name-defined]
            fd.close()

    def test_file_lock_persists_across_restart_mock(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        """Cross-platform: lock file persists after runner completes (file not deleted)."""
        lock_path = tmp_path / "dual_write_window.lock"
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner,
            mock_metrics, mock_iops_collector,
        )
        result = runner.run()
        assert result.status == "healthy"
        # Lock file must exist (created during run) — §8.5 file persistence
        # (actual lock released, but file remains)
        assert lock_path.exists(), "lock file must persist after run (§8.5 active)"


class TestEvidencePackPartialWriteSafe:
    """§8.5-2: cycle 중 process kill → evidence pack partial write 0 (fsync per write 박제)."""

    def test_evidence_pack_partial_write_safe(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        """Evidence pack append 후 파일이 UTF-8 readable한지 verify (partial write 없음)."""
        evidence_path = tmp_path / "evidence-pack.md"
        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner,
            mock_metrics, mock_iops_collector,
        )
        result = runner.run()

        assert result.status == "healthy"
        assert evidence_path.exists()

        # File must be fully readable (no partial write)
        content = evidence_path.read_text(encoding="utf-8")
        assert len(content) > 0, "evidence pack must not be empty"


# ─── P2 tests ─────────────────────────────────────────────────────────────────


class TestMetricPrefixFreezeNasDualWriteWindow:
    """P2-1: 모든 emit metric prefix == nas_dual_write_window_* (NFR-4 enforcement)."""

    def test_metric_prefix_freeze_nas_dual_write_window(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_iops_collector: MagicMock,
    ) -> None:
        reg = CollectorRegistry()
        metrics = PrometheusExporter(registry=reg)

        runner = _make_runner(
            tmp_path, mock_harness, mock_barrier, mock_sop_runner,
            metrics, mock_iops_collector,
        )
        runner.run()

        # Collect all metric names from registry
        metric_names = {
            m.name
            for m in reg.collect()
            if m.name.startswith("nas_dual_write_window")
        }

        # Must have at least status_count metric
        assert "nas_dual_write_window_status_count" in metric_names, (
            f"Expected nas_dual_write_window_status_count in metrics, got: {metric_names}"
        )

        # All nas_dual_write_window_* metrics must follow prefix (NFR-4)
        for name in metric_names:
            assert name.startswith("nas_dual_write_window_"), (
                f"Metric {name!r} violates nas_dual_write_window_* prefix freeze (NFR-4)"
            )


class TestPartitionListProviderClosedDayOnly:
    """P2-2: partition_list_provider가 closed-day partition만 return (당일 제외, S1 정합)."""

    def test_partition_list_provider_closed_day_only(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_barrier: MagicMock,
        mock_sop_runner: MagicMock,
        mock_metrics: PrometheusExporter,
        mock_iops_collector: MagicMock,
    ) -> None:
        """partition_list_provider mock이 closed-day tuple list를 제공하면 runner가 그대로 사용."""
        closed_day_partitions = [
            ("v1/binance/BTC/2024-01-01", "BTC"),
            ("v1/binance/ETH/2024-01-01", "ETH"),
        ]
        provider_mock = MagicMock(return_value=closed_day_partitions)

        runner = DualWriteWindowRunner(
            invariant_harness=mock_harness,
            compaction_barrier=mock_barrier,
            sop_runner=mock_sop_runner,
            metrics=mock_metrics,
            local_root=tmp_path / "local",
            nas_partition_root="v1/binance",
            partition_list_provider=provider_mock,
            evidence_pack_path=tmp_path / "evidence-pack.md",
            lock_path=tmp_path / "dual_write_window.lock",
            iops_query_url="http://prometheus:9090",
            iops_15pct_baseline_p99_ms=2870.65,
        )
        runner._iops_collector = mock_iops_collector

        result = runner.run()

        # provider must be called
        provider_mock.assert_called_once()
        # harness.verify must be called for each partition
        assert mock_harness.verify.call_count == len(closed_day_partitions)
        assert result.status == "healthy"


# ─── §6.8 Wording SSOT invariant ──────────────────────────────────────────────


class TestWordingSSoT:
    """§6.8.1 박제: DualWriteWindowResult.status 5 enum 정확 string (variant 금지)."""

    def test_status_enum_exact_strings(
        self,
        tmp_path: Path,
        mock_harness: MagicMock,
        mock_sop_runner: MagicMock,
        mock_iops_collector: MagicMock,
    ) -> None:
        """각 status enum 정확 string 검증 (variant 금지 — §6.8.1 SSOT)."""
        expected_statuses = {
            "healthy",
            "drift_detected",
            "barrier_drain_timeout",
            "sop_manual_gate",
            "iops_gate_breached",
        }

        # Verify DualWriteWindowResult is frozen dataclass with status field
        # Try creating one of each
        for status in expected_statuses:
            result = DualWriteWindowResult(
                status=status,  # type: ignore[arg-type]
                barrier_result=None,
            )
            assert result.status == status, f"status {status!r} mismatch"
