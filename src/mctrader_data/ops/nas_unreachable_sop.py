"""nas_unreachable_sop.py — NAS unreachable SOP runner (S10).

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253
ADR: ADR-027 D5 (NAS unreachable failure mode — concrete SOP impl)

Design decisions (§6.2.4 Change Plan 박제):
- S10 SOP state machine:
  AUTO_RESUME (default): periodic NAS HEAD ping. Recovered → drain retry_queue.
  THRESHOLD_BREACHED: backlog ≥ max_segments OR ≥ max_bytes → Prometheus alert fire.
  MANUAL_GATE: 24h 지속 unreachable → user-blocking signal (SOP runbook step 박제).
- ADR-017 archive failure 7d grace tie-in:
  본 SOP 실행 중 WAL grace 연장 신호 emit (MCT-152 dual_write_window_runner 가 consume).
- 임계값 (S10 박제 / EC-3 증거):
  threshold_segments=1000 (~50GB at 50MB/seg)
  threshold_bytes=10GB
  manual_gate_after_hours=24 (일상 transient NAS 재부팅 ~5min 흡수 buffer)
  ping_interval_seconds=30

§6.6 Amendment 보류:
- ADR-027 D5 amendment = mandatory:false → MCT-152 실전 운영 검증 후 재검토.
- 본 모듈 docstring 에 S10 박제 wording 직접 인용 (§6.6 박제 의무).
"""
from __future__ import annotations

import enum
import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mctrader_data.nas_metrics.prometheus_exporters import PrometheusExporter
    from mctrader_data.nas_storage.nas_uploader import NASUploader
    from mctrader_data.nas_storage.retry_queue import RetryQueue

log = logging.getLogger(__name__)


class SOPState(enum.Enum):
    """NAS unreachable SOP state machine states (§6.2.4 박제)."""

    AUTO_RESUME = "auto_resume"
    THRESHOLD_BREACHED = "threshold_breached"
    MANUAL_GATE = "manual_gate"


class NASUnreachableSOPRunner:
    """S10 SOP impl — auto-resume + threshold gate + 24h manual gate.

    State machine (ADR-027 D5 구체화 — §6.6 amendment 보류 pending MCT-152 실전):
    - AUTO_RESUME (default): periodic NAS HEAD ping. Recovered → drain retry_queue.
    - THRESHOLD_BREACHED: backlog ≥ 1000 seg OR ≥ 10GB → Prometheus alert fire +
      Grafana dashboard `mctrader/Cold Writer Health` 상태 변경.
    - MANUAL_GATE: 24h 지속 unreachable → user-blocking signal (SOP runbook step
      "NAS restart 또는 endpoint failover" 박제).

    ADR-017 archive failure 7d grace tie-in:
    본 SOP 실행 중 WAL grace 연장 신호 archive_grace_extend(days=7) emit
    (별 module hook — MCT-152 dual_write_window_runner 가 consume).
    """

    def __init__(
        self,
        uploader: NASUploader,
        retry_queue: RetryQueue,
        metrics: PrometheusExporter,
        threshold_segments: int = 1000,
        threshold_bytes: int = 10 * 1024**3,
        manual_gate_after_hours: int = 24,
        ping_interval_seconds: int = 30,
    ) -> None:
        self._uploader = uploader
        self._retry_queue = retry_queue
        self._metrics = metrics
        self.threshold_segments = threshold_segments
        self.threshold_bytes = threshold_bytes
        self.manual_gate_after_hours = manual_gate_after_hours
        self.ping_interval_seconds = ping_interval_seconds

        self._state = SOPState.AUTO_RESUME
        self._unreachable_since: float | None = None  # monotonic timestamp
        self._grace_extended = False  # ADR-017 7d grace 신호 emit 여부 (idempotent guard)
        # P2-3 FIX#2: grace_extension_id monotonic counter — idempotent emit semantics
        # consumer (MCT-152 dual_write_window_runner) 측 dedup 의무 박제
        self._grace_extension_id: int = 0

    def _ping_nas(self) -> bool:
        """NAS HEAD ping — 응답 성공 시 True, 실패 시 False."""
        from botocore.exceptions import EndpointConnectionError, ClientError

        try:
            client = self._uploader._get_client()
            client.head_object(Bucket=self._uploader.bucket, Key="__sop_ping__")
            return True  # 404 포함 — endpoint 자체는 응답
        except ClientError:
            return True   # ClientError (404 포함) = NAS 도달 가능
        except EndpointConnectionError:
            return False  # endpoint 자체 unreachable
        except Exception:
            return False

    def _check_threshold_breached(self) -> bool:
        """retry queue 가 threshold 초과 여부 확인 (§6.2.4, EC-3, P1-NEW-1 FIX#3 갱신).

        P1-NEW-1 FIX#3: depth(include_quarantined=True) + _total_bytes(include_quarantined=True)
        사용 — quarantined segments 는 여전히 disk 점유 중이므로 actual disk pressure 반영.
        기존 pending-only 기준은 false negative 발생 위험 (quarantined 증가 무시).
        """
        # P1-NEW-1 FIX#3: pending + quarantined total (actual drain target)
        depth = self._retry_queue.depth(include_quarantined=True)
        if depth >= self.threshold_segments:
            log.warning(
                "[sop] threshold breached: segments=%d/%d (pending+quarantined total)",
                depth, self.threshold_segments,
            )
            return True
        total_bytes = self._retry_queue._total_bytes(include_quarantined=True)
        if total_bytes >= self.threshold_bytes:
            log.warning(
                "[sop] threshold breached: bytes=%d/%d (pending+quarantined total)",
                total_bytes, self.threshold_bytes,
            )
            return True
        return False

    def _emit_grace_extension(self) -> None:
        """ADR-017 archive failure 7d grace tie-in — 신호 emit (idempotent).

        P2-3 FIX#2 idempotent emit semantics:
        - grace_extension_id monotonic counter: emit 마다 increment + signal 에 id 포함
        - consumer (MCT-152 dual_write_window_runner) 측 id 기반 dedup 의무 박제
        - 본 Story scope: emit 측 idempotent semantics 만 보장
          (실제 grace extension logic = MCT-152 scope)

        _grace_extended flag: run_once() 내 1-time emit guard.
        reset 시 (NAS recovered) _grace_extended=False 로 초기화 → 다음 unreachable 시 재 emit.
        grace_extension_id 는 단조 증가 (reset 없음 — monotonic counter).
        """
        if not self._grace_extended:
            self._grace_extension_id += 1
            log.warning(
                "[sop] ADR-017 archive failure 7d grace extension signal emitted"
                " grace_extension_id=%d"
                " — WAL grace extended (MCT-152 dual_write_window_runner consume, dedup by id)",
                self._grace_extension_id,
            )
            self._grace_extended = True

    def run_once(self) -> SOPState:
        """Single state machine tick. Returns current SOPState enum.

        §6.2.4 박제:
        - AUTO_RESUME: ping → recovered → drain / unreachable → check threshold
        - THRESHOLD_BREACHED: backlog > threshold → alert + grace extension
        - MANUAL_GATE: >24h → user-blocking signal
        """
        now = time.monotonic()
        nas_reachable = self._ping_nas()

        if nas_reachable:
            if self._state in (SOPState.THRESHOLD_BREACHED, SOPState.AUTO_RESUME) and self._retry_queue.depth() > 0:
                log.info("[sop] NAS recovered — draining retry queue")
                self._retry_queue.drain(self._uploader)
            self._state = SOPState.AUTO_RESUME
            self._unreachable_since = None
            self._grace_extended = False
            log.info(
                "[sop] NAS reachable — queue_depth=%d", self._retry_queue.depth()
            )
            self._metrics.set_queue_depth(
                queue_path=str(self._retry_queue.path),
                depth=self._retry_queue.depth(),
            )
            return self._state

        # NAS unreachable
        if self._unreachable_since is None:
            self._unreachable_since = now
            log.warning("[sop] NAS became unreachable — entering AUTO_RESUME mode")
            self._emit_grace_extension()  # ADR-017 7d grace tie-in

        unreachable_duration_hours = (now - self._unreachable_since) / 3600

        # threshold check
        if self._check_threshold_breached():
            self._state = SOPState.THRESHOLD_BREACHED
            log.error(
                "[sop] THRESHOLD_BREACHED — backlog alert fired (>= %d seg OR >= %d GB)",
                self.threshold_segments, self.threshold_bytes // (1024**3),
            )

        # 24h manual gate
        if unreachable_duration_hours >= self.manual_gate_after_hours:
            self._state = SOPState.MANUAL_GATE
            log.critical(
                "[sop] MANUAL_GATE: NAS unreachable > %dh — user intervention required. "
                "SOP runbook: NAS restart OR endpoint failover (ADR-027 D5).",
                self.manual_gate_after_hours,
            )

        # update gauge
        self._metrics.set_queue_depth(
            queue_path=str(self._retry_queue.path),
            depth=self._retry_queue.depth(),
        )

        return self._state

    def is_manual_gate(self) -> bool:
        """현재 state 가 MANUAL_GATE 여부."""
        return self._state == SOPState.MANUAL_GATE

    @property
    def state(self) -> SOPState:
        return self._state
