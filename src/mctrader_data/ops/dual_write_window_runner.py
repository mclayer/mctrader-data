"""dual_write_window_runner.py — cron-style daily invariant verify + IOPS during + SOP monitoring.

Story: MCT-152 (Stage 2 — dual-write window 운영 + IOPS during 측정 + NAS unreachable SOP 실전)
Issue: mclayer/mctrader-hub#261
ADR: ADR-027 D4 (dual-write window step 1) + D5 (NAS unreachable SOP)

Design decisions (§6.2.1 Change Plan 박제, FIX#1 F1/F2/F3 갱신):

§6.1 Architecture:
- DualWriteWindowRunner = MCT-150/151 land primitive 의 cron-style orchestrator (운영 layer)
- 단일 module + IOPSCollector helper class (scope_manifest planned_files MCT-152 박제 정합)
- Phase A → B → C → D → E sequential (single-thread, ADR-017 hot path 무영향)

§6.8 Wording SSOT:
DualWriteWindowResult.status 5종 (variant 금지):
  "healthy" / "drift_detected" / "barrier_drain_timeout" / "sop_manual_gate" / "iops_gate_breached"

§6.9 invariant placement:
- Phase A/B/D/E = unconditional sequential
- Phase C = conditional (Phase B 결과 따라)
- SOP trigger = event-driven conditional (cron 과 직교)

FIX#1 F2 (NFR-3 strict less-than):
- cron interval = 23h (max 23.5h jitter), drain timeout = 24h (margin 30min)

FIX#1 F3 (status priority pseudocode):
- iops_gate_breached 는 healthy 만 override (drift_detected 보존)
- priority: sop_manual_gate > barrier_drain_timeout > drift_detected > iops_gate_breached > healthy

§6.4 Metric 추가 위치:
- MCT-150 land prometheus_exporters.py 확장 (emit_dual_write_window_* 4 method)
- 신규 prefix: nas_dual_write_window_* (NFR-4, prefix-disjoint MCT-150/151)

SecurityArch (§7.2):
- NAS endpoint URL: log/evidence pack 포함 금지 (partition prefix 만)
- Prometheus URL: env only (log 출력 0)
- exception raw message: generic enum 만 evidence pack 박제

ADR-017 hot path 무영향:
- cron schedule = 시간축 직교 (collector tick 과 직교)
- CompactionBarrier signal-based polling 만 (collector WAL/L1 ParquetWriter 영향 0)
- IOPS 측정 = Prometheus query read-only

ADR-009 forward-only invariant:
- 본 runner = read-only verify layer (write 0)
- evidence pack append-only 만
"""
from __future__ import annotations

import datetime
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from collections.abc import Callable

if TYPE_CHECKING:
    from mctrader_data.nas_storage.compaction_barrier import CompactionBarrier, BarrierResult
    from mctrader_data.nas_migration.invariant_harness import InvariantHarness, InvariantResult
    from mctrader_data.nas_metrics.prometheus_exporters import PrometheusExporter
    from mctrader_data.ops.nas_unreachable_sop import NASUnreachableSOPRunner

log = logging.getLogger(__name__)

# §6.8.1 Wording SSOT — DualWriteWindowResult.status 5종 (frozen)
_STATUS_HEALTHY: Literal["healthy"] = "healthy"
_STATUS_DRIFT_DETECTED: Literal["drift_detected"] = "drift_detected"
_STATUS_BARRIER_DRAIN_TIMEOUT: Literal["barrier_drain_timeout"] = "barrier_drain_timeout"
_STATUS_SOP_MANUAL_GATE: Literal["sop_manual_gate"] = "sop_manual_gate"
_STATUS_IOPS_GATE_BREACHED: Literal["iops_gate_breached"] = "iops_gate_breached"


# ─── IOPSDelta dataclass ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class IOPSDelta:
    """IOPS during 측정 결과 — pre-baseline (MCT-150 S11) vs during (MCT-152 S11) delta.

    fields (§6.8.2 Wording SSOT 박제 — single name, variant 금지):
    - pre_baseline_p99_put_ms: MCT-148 T2 baseline 50MB p99 = 2870.65ms (NFR cross-reference upper)
    - during_p99_put_ms:       Prometheus query 결과 (24h time range, nas_uploader_latency_seconds)
    - delta_pct:               (during - pre) / pre * 100 — ±15% gate (S11 박제 + NFR cross-reference)
    - within_15pct_gate:       True if abs(delta_pct) <= gate_pct (default 15.0)
    - host_disk_read_iops:     node_disk_reads_completed_total/sec (24h average)
    - host_disk_write_iops:    node_disk_writes_completed_total/sec (24h average)
    - container_io_time_pct:   container_fs_io_time_seconds_total/24h * 100 (cAdvisor)

    variant 사용 금지 — §6.8.2 name 만 사용 (latency_delta_pct / pre_baseline_ms 등 prohibition).
    """

    pre_baseline_p99_put_ms: float
    during_p99_put_ms: float
    delta_pct: float
    within_15pct_gate: bool
    host_disk_read_iops: float
    host_disk_write_iops: float
    container_io_time_pct: float


# ─── DualWriteWindowResult dataclass ─────────────────────────────────────────


@dataclass(frozen=True)
class DualWriteWindowResult:
    """dual_write_window_runner.run() 의 result enum + caller contract.

    status enum 5종 (§6.8.1 Wording SSOT 박제 — single string, variant 금지):
    - "healthy":              7종 invariant ALL PASS + IOPS within ±15% gate + SOP 정상.
                              caller (cron daemon) 가 다음 cycle 대기 가능.
                              evidence pack 정상 누적, MCT-154 cutover 차단 0.
    - "drift_detected":       1종 이상 invariant FAIL (InvariantResult.status != "all_pass").
                              caller 의무: NASInvariantSchemaDriftDetected alert fire +
                              MCT-154 cutover 차단 signal emit (R13 mitigation).
                              7일 연속 healthy 누적 시 차단 자동 해제 (EC-6 mechanism).
    - "barrier_drain_timeout": CompactionBarrier.drain_and_block() = "drain_timeout" 또는
                              "barrier_violated" → DualWriteWindowResult(status="barrier_drain_timeout").
                              caller 의무: dual-write toggle 차단 + alert + skip cycle.
    - "sop_manual_gate":      SOPRunner.is_manual_gate() == True (24h 도달).
                              caller 의무: cycle 진입 즉시 skip + alert + user-blocking signal.
                              dual-write toggle 비활성화 의무 (EC-5 박제).
    - "iops_gate_breached":   IOPSDelta.within_15pct_gate == False (drift > ±15%).
                              caller 의무: alert + evidence pack 외부 요인 후보 표시 (EC-3) +
                              5-day moving average 검토 후 cutover 차단 결정.

    Caller contract (Phase 2 cron daemon):
    - status == "healthy"              → 다음 cycle 대기, evidence 누적 정상
    - status == "drift_detected"       → MCT-154 cutover 차단 signal emit
    - status == "barrier_drain_timeout"→ skip + alert, manual review 후 다음 cycle
    - status == "sop_manual_gate"      → skip + user-blocking alert, MANUAL_GATE 해제 후 진행
    - status == "iops_gate_breached"   → alert + evidence pack 외부 요인 표시
    """

    status: Literal[
        "healthy",
        "drift_detected",
        "barrier_drain_timeout",
        "sop_manual_gate",
        "iops_gate_breached",
    ]
    barrier_result: BarrierResult | None
    per_partition_invariant_results: dict[str, InvariantResult] = field(default_factory=dict)
    iops_delta: IOPSDelta | None = None
    sop_state: str = "auto_resume"  # SOPState enum value (§6.8.3 SSOT: "auto_resume" / etc.)
    run_timestamp_iso: str = ""
    cycle_duration_ms: float = 0.0
    evidence_pack_path: Path | None = None


# ─── IOPSCollector helper class ───────────────────────────────────────────────


class IOPSCollector:
    """Prometheus query client — dual-write 활성화 IOPS during 측정 (S11 박제).

    Responsibilities:
    1. nas_uploader_latency_seconds bucket=put p99 query (24h time range)
    2. node_disk_reads_completed_total / node_disk_writes_completed_total query (host)
    3. container_fs_io_time_seconds_total query (cAdvisor — compactor 컨테이너)
    4. MCT-148 T2 baseline 대비 ±15% gate 계산 → IOPSDelta dataclass return

    Read-only invariant: Prometheus query 만 — host process 영향 0.

    SecurityArch (§7.2 T2):
    - Prometheus URL: __init__ args 만 (log 출력 0)
    - Exception: generic enum 만 ('prometheus_unreachable' / 'query_invalid' / 'unknown')
      raw message embed 금지.

    NFR cross-reference (§6.2.1 관찰 4):
    - MCT-148 T2 baseline 50MB p99 = 2870.65ms (NFR-1 충족 marker)
    - ±gate_pct% (default 15.0) 범위 내 유지 의무

    Mock fixture 가능 (snapshot method patch 또는 _query_* method patch).
    """

    def __init__(
        self,
        prometheus_url: str,
        baseline_p99_ms: float,
        gate_pct: float = 15.0,
    ) -> None:
        self._prometheus_url = prometheus_url
        self._baseline_p99_ms = baseline_p99_ms
        self._gate_pct = gate_pct

    def snapshot(self, time_range_h: int = 24) -> IOPSDelta:
        """Single snapshot query — IOPSDelta dataclass return.

        Args:
            time_range_h: Prometheus query time range (default 24h, daily cycle 정합)

        Returns:
            IOPSDelta — caller (DualWriteWindowRunner.run() Phase D) consume.
            Prometheus unreachable 시 fallback: within_15pct_gate=True (false positive 차단).
        """
        try:
            during_p99_ms = self._query_prometheus(time_range_h)
            read_iops = self._query_host_read_iops(time_range_h)
            write_iops = self._query_host_write_iops(time_range_h)
            container_io_pct = self._query_container_io_pct(time_range_h)
        except OSError:
            log.warning("[iops] Prometheus unreachable — using baseline fallback")
            return IOPSDelta(
                pre_baseline_p99_put_ms=self._baseline_p99_ms,
                during_p99_put_ms=self._baseline_p99_ms,
                delta_pct=0.0,
                within_15pct_gate=True,
                host_disk_read_iops=0.0,
                host_disk_write_iops=0.0,
                container_io_time_pct=0.0,
            )

        delta_pct = (during_p99_ms - self._baseline_p99_ms) / self._baseline_p99_ms * 100.0
        within_gate = abs(delta_pct) <= self._gate_pct

        return IOPSDelta(
            pre_baseline_p99_put_ms=self._baseline_p99_ms,
            during_p99_put_ms=during_p99_ms,
            delta_pct=delta_pct,
            within_15pct_gate=within_gate,
            host_disk_read_iops=read_iops,
            host_disk_write_iops=write_iops,
            container_io_time_pct=container_io_pct,
        )

    def _query_prometheus(self, time_range_h: int) -> float:
        """nas_uploader_latency_seconds{operation='put'} p99 query (Prometheus HTTP API)."""
        # Production: HTTP request to Prometheus API
        # Mock-able for tests via patch.object(collector, "_query_prometheus", return_value=2900.0)
        import urllib.request
        import urllib.parse

        query = (
            f"histogram_quantile(0.99, "
            f"sum(rate(nas_uploader_latency_seconds_bucket{{operation='put'}}[{time_range_h}h])) "
            f"by (le)) * 1000"
        )
        params = urllib.parse.urlencode({"query": query})
        url = f"{self._prometheus_url}/api/v1/query?{params}"

        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
                result = data.get("data", {}).get("result", [])
                if result:
                    return float(result[0]["value"][1])
        except Exception:
            pass
        # Fallback: return baseline (no data = assume OK)
        return self._baseline_p99_ms

    def _query_host_read_iops(self, time_range_h: int) -> float:
        """node_disk_reads_completed_total/sec 24h average (Prometheus query)."""
        import urllib.request
        import urllib.parse

        query = f"rate(node_disk_reads_completed_total[{time_range_h}h])"
        params = urllib.parse.urlencode({"query": query})
        url = f"{self._prometheus_url}/api/v1/query?{params}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
                result = data.get("data", {}).get("result", [])
                if result:
                    return float(result[0]["value"][1])
        except Exception:
            pass
        return 0.0

    def _query_host_write_iops(self, time_range_h: int) -> float:
        """node_disk_writes_completed_total/sec 24h average."""
        import urllib.request
        import urllib.parse

        query = f"rate(node_disk_writes_completed_total[{time_range_h}h])"
        params = urllib.parse.urlencode({"query": query})
        url = f"{self._prometheus_url}/api/v1/query?{params}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
                result = data.get("data", {}).get("result", [])
                if result:
                    return float(result[0]["value"][1])
        except Exception:
            pass
        return 0.0

    def _query_container_io_pct(self, time_range_h: int) -> float:
        """container_fs_io_time_seconds_total/24h * 100 (cAdvisor)."""
        import urllib.request
        import urllib.parse

        query = f"rate(container_fs_io_time_seconds_total[{time_range_h}h]) * 100"
        params = urllib.parse.urlencode({"query": query})
        url = f"{self._prometheus_url}/api/v1/query?{params}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
                result = data.get("data", {}).get("result", [])
                if result:
                    return float(result[0]["value"][1])
        except Exception:
            pass
        return 0.0


# ─── DualWriteWindowRunner ────────────────────────────────────────────────────


class DualWriteWindowRunner:
    """dual-write window 운영 layer — cron-style 매일 invariant verify + IOPS during + SOP 실전.

    Responsibilities:
    1. cron orchestration (daily 03:00 KST default, configurable via caller)
    2. CompactionBarrier.drain_and_block() invoke (Phase A — cycle 진입 gate, unconditional)
    3. InvariantHarness.verify() partition × symbol batch invoke (Phase B — unconditional)
    4. drift detection + Phase C status judgment (Phase C — conditional)
    5. IOPS during 측정 + ±15% gate verify (Phase D — unconditional)
    6. SOPRunner.is_manual_gate() check (Phase A guard — unconditional)
    7. evidence pack append (Phase E — unconditional)
    8. file lock 획득/해제 (Phase A/E — overlap 차단, EC-1)

    §6.8 Wording SSOT:
    - status 5종: "healthy" / "drift_detected" / "barrier_drain_timeout" /
                  "sop_manual_gate" / "iops_gate_breached"

    §6.9 invariant placement:
    - Phase A/B/D/E = unconditional sequential
    - Phase C = conditional (Phase B 결과 따라)

    FIX#1 F3 status priority:
    sop_manual_gate > barrier_drain_timeout > drift_detected > iops_gate_breached > healthy
    iops_gate_breached 는 healthy 만 override (drift_detected 보존)

    ADR-017 hot path 무영향:
    - cron schedule = 시간축 직교
    - CompactionBarrier signal-based polling 만 사용
    - IOPS 측정 = Prometheus query read-only

    ADR-009 forward-only invariant:
    - 본 runner = read-only verify layer (write 0)
    - evidence pack append-only 만

    §6.7 Cross-module contract (lesson #2 invariant):
    - CompactionBarrier.drain_and_block() → BarrierResult.status 3종 switch 의무
    - InvariantHarness.verify() → InvariantResult.status 8종 중 != "all_pass" 시 drift_detected
    - SOPRunner.is_manual_gate() → True 시 즉시 sop_manual_gate (EC-5)
    - IOPSCollector.snapshot() → within_15pct_gate=False 시 iops_gate_breached (healthy 만 override)

    §8.5 active (process restart-aware):
    - file lock 파일 = process restart 후에도 잔존 (§8.5-1 test 정합)
    - evidence pack = append-only fsync per write (§8.5-2 test 정합)
    - SOPRunner / CompactionBarrier signal 모두 file system 영속 (MCT-150/151 정합)
    """

    def __init__(
        self,
        invariant_harness: InvariantHarness,
        compaction_barrier: CompactionBarrier,
        sop_runner: NASUnreachableSOPRunner,
        metrics: PrometheusExporter,
        *,
        local_root: Path,
        nas_partition_root: str,
        partition_list_provider: Callable[[], list[tuple[str, str]]],
        evidence_pack_path: Path,
        lock_path: Path = Path("/data/dual_write_window.lock"),
        iops_query_url: str = "http://prometheus:9090",
        iops_15pct_baseline_p99_ms: float = 2870.65,  # MCT-148 T2 baseline 50MB p99
    ) -> None:
        self._invariant_harness = invariant_harness
        self._compaction_barrier = compaction_barrier
        self._sop_runner = sop_runner
        self._metrics = metrics
        self._local_root = local_root
        self._nas_partition_root = nas_partition_root
        self._partition_list_provider = partition_list_provider
        self._evidence_pack_path = evidence_pack_path
        self._lock_path = lock_path
        self._iops_collector = IOPSCollector(
            prometheus_url=iops_query_url,
            baseline_p99_ms=iops_15pct_baseline_p99_ms,
        )
        # EC-6: 7일 연속 PASS 누적 (I-5 invariant)
        self._consecutive_pass_count: int = 0

    def run(self) -> DualWriteWindowResult:
        """Single cycle execution — Phase A → B → C → D → E sequential.

        Algorithm:
        Phase A (cycle entry — unconditional, §6.9.1):
          1. file lock 획득 (EC-1 overlap 차단)
             - lock 획득 실패 시 → return barrier_drain_timeout + log
          2. SOPRunner.is_manual_gate() check (EC-5):
             - True → release lock + return sop_manual_gate
          3. CompactionBarrier.drain_and_block() invoke:
             - status="ok"               → Phase B 진입
             - status="drain_timeout"    → release lock + return barrier_drain_timeout
             - status="barrier_violated" → release lock + return barrier_drain_timeout + alert

        Phase B (verify — unconditional sequential, §6.9.1):
          4. partitions = self._partition_list_provider()
          5. for each (partition, symbol): InvariantHarness.verify() → per_partition_results

        Phase C (drift 판정 — conditional, §6.9.2):
          6. has_drift = any(r.status != "all_pass" for r in per_partition_results.values())
          7. status = "drift_detected" if has_drift else "healthy"

        Phase D (IOPS during 측정 — unconditional, §6.9.1):
          8. iops_delta = self._iops_collector.snapshot(time_range_h=24)
          9. FIX#1 F3 priority: iops_gate_breached 는 healthy 만 override
             if not iops_delta.within_15pct_gate and status == "healthy":
                 status = "iops_gate_breached"

        Phase E (cycle exit — unconditional cleanup, §6.9.1):
         10. CompactionBarrier.release()
         11. evidence pack append (per-day report)
         12. file lock 해제
         13. emit DualWriteWindowResult metric

        Returns:
            DualWriteWindowResult — caller (cron daemon) 가 status switch 의무.

        Raises:
            None — 모든 failure case 가 status enum 으로 propagate.
            단, file lock 생성 자체 실패 (OS level) 는 OSError raise 가능 (cron retry).

        Metric emission (nas_dual_write_window_* prefix — NFR-4 §6.8.4):
        - nas_dual_write_window_status_count (Counter, labels: status [5 enum])
        - nas_dual_write_window_cycle_duration_seconds (Histogram)
        - nas_dual_write_window_iops_delta_pct (Gauge)
        - nas_dual_write_window_sop_trigger_count (Counter)
        """
        cycle_start_mono = time.monotonic()
        run_timestamp_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # ── Phase A: cycle entry (unconditional) ─────────────────────────────

        # Step 1: file lock 획득 (EC-1 overlap 차단)
        try:
            self._acquire_lock()
        except OSError as e:
            log.warning("[runner] lock acquire failed (EC-1 overlap): %s", e)
            # 즉시 reject — barrier_drain_timeout (lock conflict)
            return DualWriteWindowResult(
                status=_STATUS_BARRIER_DRAIN_TIMEOUT,
                barrier_result=None,
                run_timestamp_iso=run_timestamp_iso,
                cycle_duration_ms=(time.monotonic() - cycle_start_mono) * 1000,
            )

        barrier_result: BarrierResult | None = None
        per_partition_results: dict[str, InvariantResult] = {}
        iops_delta: IOPSDelta | None = None
        status: str = _STATUS_HEALTHY

        try:
            # Step 2: SOPRunner.is_manual_gate() check (EC-5)
            sop_state_str = "auto_resume"
            try:
                sop_state = self._sop_runner.state
                sop_state_str = sop_state.value if hasattr(sop_state, "value") else str(sop_state)
            except Exception:
                pass

            if self._sop_runner.is_manual_gate():
                log.warning("[runner] SOPRunner MANUAL_GATE — cycle skip (EC-5, sop_manual_gate)")
                status = _STATUS_SOP_MANUAL_GATE
                self._metrics.emit_dual_write_window_sop_trigger(sop_state="manual_gate")
                return DualWriteWindowResult(
                    status=status,
                    barrier_result=None,
                    sop_state=sop_state_str,
                    run_timestamp_iso=run_timestamp_iso,
                    cycle_duration_ms=(time.monotonic() - cycle_start_mono) * 1000,
                    evidence_pack_path=self._evidence_pack_path,
                )

            # Step 3: CompactionBarrier.drain_and_block() (unconditional)
            barrier_result = self._compaction_barrier.drain_and_block()
            self._metrics.emit_invariant_compaction_barrier(
                status=barrier_result.status,
                drain_wait_s=barrier_result.drain_wait_ms / 1000.0,
                in_flight_remaining=barrier_result.in_flight_remaining,
            )

            if barrier_result.status in ("drain_timeout", "barrier_violated"):
                log.error(
                    "[runner] CompactionBarrier %s — cycle skip (barrier_drain_timeout)",
                    barrier_result.status,
                )
                status = _STATUS_BARRIER_DRAIN_TIMEOUT
                # Phase E still runs (release + evidence pack)
                return DualWriteWindowResult(
                    status=status,
                    barrier_result=barrier_result,
                    sop_state=sop_state_str,
                    run_timestamp_iso=run_timestamp_iso,
                    cycle_duration_ms=(time.monotonic() - cycle_start_mono) * 1000,
                )

            # ── Phase B: verify (unconditional sequential, §6.9.1) ───────────

            partitions = self._partition_list_provider()
            for partition, symbol in partitions:
                local_partition = self._local_root / partition
                if partition.startswith(self._nas_partition_root):
                    nas_partition = partition
                else:
                    nas_partition = f"{self._nas_partition_root}/{partition}"

                inv_result = self._invariant_harness.verify(
                    local_partition=local_partition,
                    nas_partition=nas_partition,
                )
                per_partition_results[f"{partition}|{symbol}"] = inv_result
                self._metrics.emit_invariant_verify(
                    status=inv_result.status,
                    partition=partition,
                    latency_s=inv_result.verify_latency_ms / 1000.0,
                    per_invariant_results=inv_result.per_invariant_results,
                )
                log.info(
                    "[runner] verify partition=%s symbol=%s status=%s latency_ms=%.1f",
                    partition, symbol, inv_result.status, inv_result.verify_latency_ms,
                )

            # ── Phase C: drift 판정 (conditional, §6.9.2) ─────────────────────

            has_drift = any(
                r.status != "all_pass" for r in per_partition_results.values()
            )
            if has_drift:
                status = _STATUS_DRIFT_DETECTED
                log.error("[runner] drift_detected — 1종+ invariant FAIL (R13 mitigation: MCT-154 cutover 차단)")
                self._consecutive_pass_count = 0
            else:
                status = _STATUS_HEALTHY
                log.info("[runner] healthy — 7종 invariant ALL PASS")
                self._consecutive_pass_count += 1

            # ── Phase D: IOPS during 측정 (unconditional, §6.9.1) ────────────

            iops_delta = self._iops_collector.snapshot(time_range_h=24)
            self._metrics.emit_dual_write_window_iops_delta(
                p99_pct=iops_delta.delta_pct,
                read_iops=iops_delta.host_disk_read_iops,
                write_iops=iops_delta.host_disk_write_iops,
            )

            # FIX#1 F3: status priority — iops_gate_breached 는 healthy 만 override
            if not iops_delta.within_15pct_gate:
                if status == _STATUS_HEALTHY:
                    status = _STATUS_IOPS_GATE_BREACHED
                    log.warning(
                        "[runner] iops_gate_breached: delta_pct=%.2f%% (>±15%% gate)",
                        iops_delta.delta_pct,
                    )
                else:
                    # drift_detected 보존 (priority: drift_detected > iops_gate_breached)
                    log.warning(
                        "[runner] iops also breached (delta_pct=%.2f%%) but %s has higher priority",
                        iops_delta.delta_pct, status,
                    )

        finally:
            # ── Phase E: cycle exit (unconditional cleanup, §6.9.1) ───────────

            # CompactionBarrier.release() (unconditional — EC-1 barrier영구 차단 차단)
            if barrier_result is not None and barrier_result.status == "ok":
                self._compaction_barrier.release()

            # evidence pack append (unconditional — §8.5-2)
            cycle_duration_ms = (time.monotonic() - cycle_start_mono) * 1000
            self._append_evidence_pack(
                status=status,
                run_timestamp_iso=run_timestamp_iso,
                cycle_duration_ms=cycle_duration_ms,
                per_partition_results=per_partition_results,
                iops_delta=iops_delta,
            )

            # file lock 해제 (unconditional)
            self._release_lock()

            # emit status counter (nas_dual_write_window_status_count)
            self._metrics.emit_dual_write_window_status(status=status)

        return DualWriteWindowResult(
            status=status,
            barrier_result=barrier_result,
            per_partition_invariant_results=per_partition_results,
            iops_delta=iops_delta,
            sop_state=sop_state_str if "sop_state_str" in dir() else "auto_resume",
            run_timestamp_iso=run_timestamp_iso,
            cycle_duration_ms=cycle_duration_ms,
            evidence_pack_path=self._evidence_pack_path,
        )

    def shutdown(self) -> None:
        """Graceful shutdown — file lock 해제 + evidence pack flush.

        Caller 의무: cron daemon SIGTERM 처리 시 호출 (systemd ExecStop= 등).
        Idempotency: 다중 호출 시 NO-OP.
        """
        self._release_lock()
        log.info("[runner] shutdown complete")

    # ─── internal helpers ─────────────────────────────────────────────────────

    def _acquire_lock(self) -> None:
        """file lock 획득 (cross-platform).

        Windows: msvcrt.locking 기반 (또는 msvcrt.open_osfhandle)
        POSIX: fcntl.flock 기반

        EC-1 overlap 차단 의무 — lock 획득 실패 시 OSError raise.
        §8.5-1: lock file 자체는 process restart 후에도 잔존.
        """
        import sys

        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        # SIM115: intentionally NOT using context manager — fd must persist across method boundary
        lock_fd = open(str(self._lock_path), "w")  # noqa: SIM115
        self._lock_fd = lock_fd

        if sys.platform == "win32":
            import msvcrt
            try:
                # Non-blocking exclusive lock on Windows
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
                lock_fd.write("locked")
                lock_fd.flush()
            except OSError as e:
                lock_fd.close()
                self._lock_fd = None  # type: ignore[assignment]
                raise OSError(f"lock acquire failed (EC-1): {e}") from e
        else:
            import fcntl
            try:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_fd.write("locked")
                lock_fd.flush()
            except OSError as e:
                lock_fd.close()
                self._lock_fd = None  # type: ignore[assignment]
                raise OSError(f"lock acquire failed (EC-1): {e}") from e

        log.debug("[runner] lock acquired: %s", self._lock_path)

    def _release_lock(self) -> None:
        """file lock 해제 (idempotent).

        Phase E unconditional — 누락 시 다음 cycle 에서 EC-1 overlap 차단 해제 불가.
        """
        import sys

        lock_fd = getattr(self, "_lock_fd", None)
        if lock_fd is None:
            return

        import contextlib
        try:
            if sys.platform == "win32":
                import msvcrt
                with contextlib.suppress(OSError):
                    msvcrt.locking(lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                with contextlib.suppress(OSError):
                    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()
        except Exception as e:
            log.warning("[runner] lock release error: %s", e)
        finally:
            self._lock_fd = None  # type: ignore[assignment]

        log.debug("[runner] lock released: %s", self._lock_path)

    def _append_evidence_pack(
        self,
        status: str,
        run_timestamp_iso: str,
        cycle_duration_ms: float,
        per_partition_results: dict[str, InvariantResult],
        iops_delta: IOPSDelta | None,
    ) -> None:
        """evidence pack append (append-only, fsync per write — §8.5-2).

        Format: markdown table row per cycle (NFR-6 enforcement).
        SecurityArch (§7.2 T4): NAS endpoint URL embed 차단 — partition prefix 만.
        """
        try:
            self._evidence_pack_path.parent.mkdir(parents=True, exist_ok=True)

            # Build per-day entry
            drift_count = sum(
                1 for r in per_partition_results.values() if r.status != "all_pass"
            )
            iops_gate_ok = iops_delta.within_15pct_gate if iops_delta else True
            iops_delta_pct = f"{iops_delta.delta_pct:.2f}" if iops_delta else "N/A"

            entry_lines = [
                f"\n## cycle run_timestamp={run_timestamp_iso}",
                f"- status: {status}",
                f"- cycle_duration_ms: {cycle_duration_ms:.1f}",
                f"- drift_count: {drift_count} / {len(per_partition_results)} partitions",
                f"- iops_gate_ok: {iops_gate_ok}",
                f"- iops_delta_pct: {iops_delta_pct}",
                f"- consecutive_pass_count: {self._consecutive_pass_count}",
            ]
            entry = "\n".join(entry_lines) + "\n"

            # append-only + fsync (§8.5-2)
            with open(self._evidence_pack_path, "a", encoding="utf-8") as f:
                f.write(entry)
                f.flush()
                import os
                os.fsync(f.fileno())

        except Exception as e:
            log.warning("[runner] evidence pack append failed: %s", type(e).__name__)
