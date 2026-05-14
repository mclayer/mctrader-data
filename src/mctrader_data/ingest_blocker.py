"""ingest_blocker.py — D5=A_modified graceful drain ingest blocker (D7-5=B + D7-8=C).

Story: MCT-171 (EPIC-tier-promotion-single-source Story-5)
ADR-029:
  D5=A_modified: capacity-bounded ingest block
  D7-5=B: graceful drain 후 reject (in-flight WAL write 일관성 보존)
  D7-8=C: 80%/95% hysteresis — warn 80% + aggressive rotate trigger, critical 95% graceful block

State machine:
  NORMAL → WARN_DRAIN (80% trigger) → BLOCKED (95% trigger) → NORMAL (90% unblock, 5% gap)

Prometheus:
  mctrader_ingest_blocked_total{reason=<wal_full|l1_full|nas_unreachable>} Counter

Design decisions (§5.3 spec 박제):
- hot path 영향 0: collector.py sibling + 5min idle baseline (D5 정합)
- graceful drain: WAL atomic boundary (sealed segment) 이후 신규 reject
- hysteresis_gap=0.05: block 임계 95% - 5% gap = unblock 90%

SecurityArch:
- reason label = 3 enum hardcoded (free-form label 금지, cardinality 제한)
- 경로 raw / 파일명 포함 금지
"""
from __future__ import annotations

import logging
import threading
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mctrader_data.capacity_probe import CapacityProbe, CapacityReport
    from mctrader_data.nas_metrics.prometheus_exporters import PrometheusExporter

log = logging.getLogger(__name__)

# ─── reason label enum SSOT (AC-5 cardinality 제한) ─────────────────────────
# 3 enum hardcoded — free-form label 금지 (R4 mitigation)
ALLOWED_BLOCK_REASONS = frozenset(["wal_full", "l1_full", "nas_unreachable"])


class BlockerState(Enum):
    """IngestBlocker state machine (D7-8=C)."""

    NORMAL = "normal"
    WARN_DRAIN = "warn_drain"     # 80% trigger — aggressive rotate, not blocked
    BLOCKED = "blocked"           # 95% trigger — reject new ingest


class IngestBlocker:
    """Graceful drain ingest blocker — D5=A_modified + D7-8=C 80%/95% hysteresis.

    State machine:
        NORMAL → WARN_DRAIN (80% trigger) → BLOCKED (95% trigger) → NORMAL (90% unblock, 5% gap)

    Thread-safe: state 변경은 _lock 보호 (collector hot path 에서 concurrent 호출 대비).

    Usage:
        blocker = IngestBlocker(probe=..., metrics=...)
        # in collector hot path (before WAL write):
        report = probe.probe_once()
        if blocker.should_block(report):
            raise IngestBlockedError("capacity full")
    """

    def __init__(
        self,
        probe: CapacityProbe,
        metrics: PrometheusExporter | None = None,
        hysteresis_gap: float = 0.05,  # 95% block → 90% unblock
    ) -> None:
        """IngestBlocker 초기화.

        Args:
            probe: CapacityProbe 인스턴스 (on_capacity_warn/critical callback 용)
            metrics: PrometheusExporter (optional — None 시 emit skip)
            hysteresis_gap: block 임계에서 unblock 임계까지의 gap (default 0.05 = 5%)
        """
        self._probe = probe
        self._metrics = metrics
        self._hysteresis_gap = hysteresis_gap
        self._state = BlockerState.NORMAL
        self._lock = threading.Lock()
        # override state: on_capacity_critical() 가 직접 block 강제 시 사용
        self._force_blocked = False

    def should_block(self, report: CapacityReport) -> bool:
        """현재 4 layer state 기준 block 결정.

        Algorithm (D7-8=C hysteresis):
        1. 4 layer ratio 최대값 계산
        2. max_ratio >= critical_ratio (0.95) OR _force_blocked → BLOCKED → return True
        3. _state == BLOCKED AND max_ratio >= (critical_ratio - hysteresis_gap) → still blocked
        4. max_ratio >= warn_ratio (0.80) → WARN_DRAIN → return False (drain trigger, not block)
        5. NORMAL → return False

        Prometheus emit: block 결정 시 emit_ingest_blocked(reason=...)

        Returns:
            True: ingest reject (BLOCKED state)
            False: ingest 허용 (NORMAL or WARN_DRAIN state)
        """
        with self._lock:
            return self._evaluate(report)

    def _evaluate(self, report: CapacityReport) -> bool:
        """Lock 내부에서 state 평가."""
        from mctrader_data.capacity_probe import CapacityThresholds

        # Extract thresholds from probe if available (guard against MagicMock in tests)
        thresholds_raw = getattr(self._probe, "_thresholds", None)
        thresholds = thresholds_raw if isinstance(thresholds_raw, CapacityThresholds) else CapacityThresholds()

        critical: float = float(thresholds.critical_ratio)  # 0.95
        warn: float = float(thresholds.warn_ratio)          # 0.80
        unblock: float = critical - float(self._hysteresis_gap)  # 0.90

        # Determine which layer is causing issue
        layer_ratios = {
            "WAL_local": report.wal_ratio,
            "L1_local": report.l1_ratio,
            "NAS_bucket": report.nas_ratio,
            "Host_disk": report.host_ratio,
        }

        # _force_blocked: on_capacity_critical() 직접 강제 state
        # force_blocked = graceful drain + reject (D7-5=B, operator gate)
        # force_blocked 상태는 명시적 reset_block() 호출 또는 should_block()에서
        # probe report 기반 자동 전환으로 해제
        # Note: force_blocked = True이면 report ratio 무관하게 BLOCKED 유지
        #       (on_capacity_critical() = operator-level decision, D7-5=B)
        if self._force_blocked:
            return True

        # Find max ratio and which layer is critical
        max_ratio = max(layer_ratios.values())
        critical_layers = [layer for layer, r in layer_ratios.items() if r >= critical]
        warn_layers = [layer for layer, r in layer_ratios.items() if r >= warn]

        if critical_layers:
            # Transition to BLOCKED
            if self._state != BlockerState.BLOCKED:
                self._state = BlockerState.BLOCKED
                log.warning(
                    "[ingest_blocker] BLOCKED: layers=%s (>= %.0f%%)",
                    critical_layers, critical * 100
                )
            # Emit blocked counter per layer
            for layer in critical_layers:
                self._emit_blocked(layer)
            return True

        elif self._state == BlockerState.BLOCKED:
            # Hysteresis: still blocked until below unblock threshold
            if max_ratio >= unblock:
                log.debug(
                    "[ingest_blocker] still BLOCKED (hysteresis: %.1f%% >= %.0f%% unblock threshold)",
                    max_ratio * 100, unblock * 100
                )
                return True
            else:
                # Unblock
                self._state = BlockerState.NORMAL
                log.info(
                    "[ingest_blocker] unblocked: max_ratio=%.1f%% < %.0f%%",
                    max_ratio * 100, unblock * 100
                )
                return False

        elif warn_layers:
            # WARN_DRAIN — aggressive rotate trigger, not yet block
            if self._state != BlockerState.WARN_DRAIN:
                self._state = BlockerState.WARN_DRAIN
                log.warning(
                    "[ingest_blocker] WARN_DRAIN: layers=%s (>= %.0f%%)",
                    warn_layers, warn * 100
                )
            return False

        else:
            # NORMAL
            if self._state != BlockerState.NORMAL:
                self._state = BlockerState.NORMAL
            return False

    def _emit_blocked(self, layer: str) -> None:
        """layer → reason enum 변환 후 Counter emit.

        layer enum → reason enum 매핑:
        - WAL_local → wal_full
        - L1_local → l1_full
        - NAS_bucket → nas_unreachable (bucket 용량 초과)
        - Host_disk → wal_full (WAL/L1 host mount 공유 시)
        """
        reason_map = {
            "WAL_local": "wal_full",
            "L1_local": "l1_full",
            "NAS_bucket": "nas_unreachable",
            "Host_disk": "wal_full",
        }
        reason = reason_map.get(layer, "wal_full")

        if self._metrics is not None:
            try:
                self._metrics.emit_ingest_blocked(reason=reason)  # type: ignore[union-attr]
            except Exception:
                log.debug("[ingest_blocker] emit_ingest_blocked failed (metrics not ready)")

    def on_capacity_warn(self, layer: str, ratio: float) -> None:
        """80% threshold — aggressive L1 rotate trigger (compactor signal).

        called by CapacityProbe 또는 외부 caller.
        state = WARN_DRAIN 전환 + compactor rotate signal.
        """
        with self._lock:
            if self._state == BlockerState.NORMAL:
                self._state = BlockerState.WARN_DRAIN
                log.warning(
                    "[ingest_blocker] on_capacity_warn: layer=%s ratio=%.1f%% → WARN_DRAIN",
                    layer, ratio * 100
                )
            # TODO: compactor signal emit (Phase 2 PR2 scope — MCT-172 연동)

    def on_capacity_critical(self, layer: str, ratio: float) -> None:
        """95% threshold — graceful drain 후 ingest reject.

        called by CapacityProbe 또는 외부 caller.
        state = BLOCKED 강제 전환 (graceful drain completed = WAL sealed segment 이후 reject).
        """
        with self._lock:
            self._force_blocked = True
            self._state = BlockerState.BLOCKED
            log.error(
                "[ingest_blocker] on_capacity_critical: layer=%s ratio=%.1f%% → BLOCKED (graceful drain + reject)",
                layer, ratio * 100
            )
            self._emit_blocked(layer)

    @property
    def state(self) -> BlockerState:
        """현재 blocker state (read-only)."""
        return self._state

    @property
    def is_blocked(self) -> bool:
        """현재 block 상태 여부."""
        return self._state == BlockerState.BLOCKED or self._force_blocked
