"""capacity_probe.py — 4 layer capacity hybrid probe (D7-2=A + D7-4=C).

Story: MCT-171 (EPIC-tier-promotion-single-source Story-5)
ADR-029 D11: WAL 30G / L1 20G / NAS 500G target + 1TB hard / Host 200G hard
             warn 80% / critical 95%

Design decisions (§5.2 spec 박제):
- D7-2=A: capacity_probe.py 단독 module (collector.py sibling, hot path / exporter 의존 0)
- D7-4=C: hybrid loop — 5min default + 임의 layer >= 80% 근접 시 15s continuous 전이
- D11: CapacityThresholds SSOT — ADR-029 D11 박제분 상수

Layer probe 방식:
- WAL_local / L1_local: shutil.disk_usage(root) — directory 내 파일 크기 합산 또는 mountpoint usage
- NAS_bucket: boto3 list_objects_v2 paginator (mc admin du fallback)
- Host_disk: shutil.disk_usage(host_mount) fallback

SecurityArch:
- NAS endpoint URL: metric label 에 포함 금지
- 용량 측정값만 emit (파일명 / 경로 raw 포함 금지)
"""
from __future__ import annotations

import logging
import shutil
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader
    from mctrader_data.nas_metrics.prometheus_exporters import PrometheusExporter

log = logging.getLogger(__name__)

# ─── Prometheus label enum SSOT (AC-5 cardinality 제한) ─────────────────────
# free-form label 금지 — 4 enum 고정
LAYER_NAMES: tuple[str, ...] = ("WAL_local", "L1_local", "NAS_bucket", "Host_disk")

# Probe interval constants (D7-4=C hybrid)
_NORMAL_INTERVAL_S: float = 300.0   # 5min default
_APPROACH_INTERVAL_S: float = 15.0  # 15s continuous when >= 80%
_APPROACH_THRESHOLD: float = 0.80   # switch to continuous when any layer >= 80%


# ─── ADR-029 D11 상수 SSOT ───────────────────────────────────────────────────


@dataclass(frozen=True)
class CapacityThresholds:
    """ADR-029 D11 4 layer capacity 제한 상수 SSOT.

    WAL 30G / L1 20G / NAS 500G target + 1TB hard / Host 200G hard
    warn 80% / critical 95%

    frozen=True: 불변 SSOT (runtime 수정 금지).
    """

    wal_local_hard_gib: int = 30       # ADR-029 D11
    l1_local_hard_gib: int = 20        # ADR-029 D11
    nas_bucket_target_gib: int = 500   # ADR-029 D11 (soft target)
    nas_bucket_hard_gib: int = 1024    # ADR-029 D11 (1TB hard)
    host_disk_hard_gib: int = 200      # ADR-029 D11

    warn_ratio: float = 0.80    # 80% warn → aggressive rotate trigger
    critical_ratio: float = 0.95  # 95% graceful block

    def wal_hard_bytes(self) -> int:
        return self.wal_local_hard_gib * (1024 ** 3)

    def l1_hard_bytes(self) -> int:
        return self.l1_local_hard_gib * (1024 ** 3)

    def nas_hard_bytes(self) -> int:
        return self.nas_bucket_hard_gib * (1024 ** 3)

    def host_hard_bytes(self) -> int:
        return self.host_disk_hard_gib * (1024 ** 3)


# ─── CapacityReport ──────────────────────────────────────────────────────────


@dataclass
class CapacityReport:
    """4 layer capacity 측정 결과.

    layer 별 usage_bytes + hard_bytes → ratio = usage / hard.
    Prometheus emit: mctrader_capacity_usage_bytes{layer} + mctrader_capacity_threshold_ratio{layer}
    """

    wal_usage_bytes: int
    l1_usage_bytes: int
    nas_usage_bytes: int
    host_usage_bytes: int

    wal_hard_bytes: int
    l1_hard_bytes: int
    nas_hard_bytes: int
    host_hard_bytes: int

    @property
    def wal_ratio(self) -> float:
        """WAL usage ratio (0.0 ~ 1.0+)."""
        return self.wal_usage_bytes / self.wal_hard_bytes if self.wal_hard_bytes > 0 else 0.0

    @property
    def l1_ratio(self) -> float:
        return self.l1_usage_bytes / self.l1_hard_bytes if self.l1_hard_bytes > 0 else 0.0

    @property
    def nas_ratio(self) -> float:
        return self.nas_usage_bytes / self.nas_hard_bytes if self.nas_hard_bytes > 0 else 0.0

    @property
    def host_ratio(self) -> float:
        return self.host_usage_bytes / self.host_hard_bytes if self.host_hard_bytes > 0 else 0.0

    def max_ratio(self) -> float:
        """4 layer 중 최대 ratio."""
        return max(self.wal_ratio, self.l1_ratio, self.nas_ratio, self.host_ratio)

    def layer_ratio(self, layer: str) -> float:
        """layer name → ratio."""
        mapping = {
            "WAL_local": self.wal_ratio,
            "L1_local": self.l1_ratio,
            "NAS_bucket": self.nas_ratio,
            "Host_disk": self.host_ratio,
        }
        return mapping.get(layer, 0.0)


# ─── CapacityProbe ───────────────────────────────────────────────────────────


class CapacityProbe:
    """4 layer capacity hybrid probe — 5min audit sample + threshold approach continuous.

    Layer: WAL_local / L1_local / NAS_bucket / Host_disk
    Threshold: warn 80% / critical 95% / hard limit 100%

    D7-4=C hybrid:
    - 정상 시 5min 간격 poll
    - 임의 layer >= 80% 근접 → 15s continuous 전이
    - WAL/L1: directory 재귀 합산 (small overhead) 또는 shutil.disk_usage(root)
    - Host: shutil.disk_usage(host_mount)
    - NAS: boto3 list_objects_v2 paginator (ContentLength 합산)
    """

    def __init__(
        self,
        wal_root: Path,
        l1_root: Path,
        nas_uploader: NASUploader,
        host_mount: Path,
        thresholds: CapacityThresholds,
        metrics: PrometheusExporter | None = None,
    ) -> None:
        """4 layer capacity probe 초기화.

        Args:
            wal_root: WAL root directory (mctrader-data/data/wal/)
            l1_root: L1 root directory (mctrader-data/data/l1/)
            nas_uploader: NASUploader 인스턴스 (NAS bucket size probe)
            host_mount: host mount path (shutil.disk_usage fallback)
            thresholds: CapacityThresholds SSOT (ADR-029 D11 정합)
            metrics: PrometheusExporter (optional — None 시 emit skip)
        """
        self._wal_root = wal_root
        self._l1_root = l1_root
        self._nas_uploader = nas_uploader
        self._host_mount = host_mount
        self._thresholds = thresholds
        self._metrics = metrics
        self._stop_event = threading.Event()

    def probe_once(self) -> CapacityReport:
        """4 layer probe 1회 실행 — CapacityReport 반환 + Gauge emit.

        측정 방식:
        - WAL/L1: directory walk 합산 (작은 dir 기준 빠름)
        - NAS: boto3 list_objects_v2 paginator → ContentLength 합산 (시간 소요 가능)
        - Host: shutil.disk_usage(host_mount) — 가장 빠름, fallback

        SecurityArch: 경로 raw 포함 금지, 용량 바이트만 emit.
        """
        wal_bytes = self._probe_dir_bytes(self._wal_root)
        l1_bytes = self._probe_dir_bytes(self._l1_root)
        nas_bytes = self._probe_nas_bytes()
        host_bytes = self._probe_host_bytes()

        wal_hard = self._thresholds.wal_hard_bytes()
        l1_hard = self._thresholds.l1_hard_bytes()
        nas_hard = self._thresholds.nas_hard_bytes()
        host_hard = self._thresholds.host_hard_bytes()

        report = CapacityReport(
            wal_usage_bytes=wal_bytes,
            l1_usage_bytes=l1_bytes,
            nas_usage_bytes=nas_bytes,
            host_usage_bytes=host_bytes,
            wal_hard_bytes=wal_hard,
            l1_hard_bytes=l1_hard,
            nas_hard_bytes=nas_hard,
            host_hard_bytes=host_hard,
        )

        # Emit Prometheus metrics (AC-5)
        if self._metrics is not None:
            self._emit_metrics(report)

        return report

    def probe_loop(self, *, stop_event: threading.Event | None = None) -> None:
        """D7-4=C hybrid loop: 5min default + threshold approach 15s continuous.

        Args:
            stop_event: threading.Event — 외부에서 루프 종료 신호 (None 시 내부 _stop_event 사용)

        Loop 전략:
        - max_ratio < 80%: 5min 간격 (normal)
        - max_ratio >= 80%: 15s 간격 (approach continuous)
        - stop_event.set() → 루프 종료
        """
        _stop = stop_event or self._stop_event
        log.info("[capacity_probe] probe_loop started (normal_interval=%.0fs, approach_interval=%.0fs)",
                 _NORMAL_INTERVAL_S, _APPROACH_INTERVAL_S)

        while not _stop.is_set():
            try:
                report = self.probe_once()
                max_ratio = report.max_ratio()

                if max_ratio >= _APPROACH_THRESHOLD:
                    interval = _APPROACH_INTERVAL_S
                    log.warning(
                        "[capacity_probe] approach state: max_ratio=%.1f%% → 15s interval",
                        max_ratio * 100,
                    )
                else:
                    interval = _NORMAL_INTERVAL_S

            except Exception:
                log.exception("[capacity_probe] probe_once() error — retry in %.0fs", _NORMAL_INTERVAL_S)
                interval = _NORMAL_INTERVAL_S

            _stop.wait(timeout=interval)

        log.info("[capacity_probe] probe_loop stopped")

    def stop(self) -> None:
        """probe_loop() 종료 신호."""
        self._stop_event.set()

    # ─── internal probes ─────────────────────────────────────────────────────

    def _probe_dir_bytes(self, root: Path) -> int:
        """Directory 재귀 파일 크기 합산.

        directory 가 존재하지 않으면 0 반환 (graceful).
        """
        if not root.exists():
            return 0
        total = 0
        try:
            for p in root.rglob("*"):
                if p.is_file():
                    try:
                        total += p.stat().st_size
                    except OSError:
                        pass
        except Exception:
            log.warning("[capacity_probe] _probe_dir_bytes error for %s", root)
        return total

    def _probe_nas_bytes(self) -> int:
        """NAS bucket size probe: boto3 list_objects_v2 paginator ContentLength 합산.

        NAS unreachable / 권한 오류 시 0 반환 (availability > accuracy — probe 실패가 ingest 차단 X).
        """
        try:
            client = self._nas_uploader._get_client()  # type: ignore[attr-defined]
            bucket = self._nas_uploader.bucket
            paginator = client.get_paginator("list_objects_v2")

            total = 0
            for page in paginator.paginate(Bucket=bucket):
                for obj in page.get("Contents", []):
                    total += obj.get("Size", 0)
            return total
        except Exception:
            log.warning(
                "[capacity_probe] _probe_nas_bytes failed (NAS unreachable?) — returning 0"
            )
            return 0

    def _probe_host_bytes(self) -> int:
        """Host disk usage probe: shutil.disk_usage(host_mount).used.

        host_mount 부재 시 0 반환 (graceful).
        """
        try:
            usage = shutil.disk_usage(self._host_mount)
            return usage.used
        except Exception:
            log.warning("[capacity_probe] _probe_host_bytes shutil.disk_usage failed — returning 0")
            return 0

    def _emit_metrics(self, report: CapacityReport) -> None:
        """Prometheus Gauge emit (AC-5).

        mctrader_capacity_usage_bytes{layer} × 4
        mctrader_capacity_threshold_ratio{layer} × 4
        """
        layers_usage = [
            ("WAL_local", report.wal_usage_bytes),
            ("L1_local", report.l1_usage_bytes),
            ("NAS_bucket", report.nas_usage_bytes),
            ("Host_disk", report.host_usage_bytes),
        ]
        layers_ratio = [
            ("WAL_local", report.wal_ratio),
            ("L1_local", report.l1_ratio),
            ("NAS_bucket", report.nas_ratio),
            ("Host_disk", report.host_ratio),
        ]

        try:
            # emit usage bytes
            for layer, bytes_val in layers_usage:
                self._metrics.emit_capacity_usage(layer=layer, bytes_val=bytes_val)  # type: ignore[union-attr]
        except AttributeError:
            # PrometheusExporter에 emit_capacity_usage 없으면 스킵 (graceful degradation)
            pass

        try:
            # emit threshold ratio
            for layer, ratio in layers_ratio:
                self._metrics.emit_capacity_ratio(layer=layer, ratio=ratio)  # type: ignore[union-attr]
        except AttributeError:
            pass
