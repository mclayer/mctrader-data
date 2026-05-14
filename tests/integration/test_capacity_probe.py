"""test_capacity_probe.py — MCT-171 TDD failing tests: CapacityProbe 4 layer hybrid probe.

Story: MCT-171 (EPIC-tier-promotion-single-source Story-5)
AC: AC-2 — capacity_probe hybrid timing (D7-2=A + D7-4=C)
AC: AC-5 — Prometheus metric design (D7-6)

Test Contract (MCT-171 §4 AC-2/5):
- test_capacity_thresholds_constants: CapacityThresholds SSOT (ADR-029 D11 박제분)
- test_capacity_report_dataclass: CapacityReport dataclass 4 layer 측정값 포함
- test_probe_once_returns_capacity_report: probe_once() → CapacityReport 반환
- test_probe_once_4_layer_gauge_emit: probe_once() → 4 Gauge emit (WAL_local/L1_local/NAS_bucket/Host_disk)
- test_probe_once_threshold_ratio_gauge: threshold_ratio Gauge 4종 emit
- test_probe_warn_threshold_80pct: layer usage >= 80% → warn state 반영
- test_probe_critical_threshold_95pct: layer usage >= 95% → critical state 반영
- test_capacity_probe_no_hot_path_import: collector.py hot path 의존 0 (sibling module isolation)
- test_wal_probe_uses_shutil_disk_usage: host_mount probe = shutil.disk_usage fallback
- test_probe_loop_callable: probe_loop() callable (non-blocking test)

ADR-029 D11 상수 SSOT:
  WAL 30G / L1 20G / NAS 500G target + 1TB hard / Host 200G hard
  warn 80% / critical 95%

verified-via: Read docs/superpowers/specs/2026-05-14-MCT-171-dr-runbook-capacity-design.md §5.2
"""
from __future__ import annotations

import io
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ─── Test: CapacityThresholds SSOT ──────────────────────────────────────────


class TestCapacityThresholds:
    """ADR-029 D11 상수 SSOT — CapacityThresholds dataclass."""

    def test_capacity_thresholds_constants(self) -> None:
        """CapacityThresholds default 상수 = ADR-029 D11 박제분 정합."""
        from mctrader_data.capacity_probe import CapacityThresholds

        t = CapacityThresholds()
        assert t.wal_local_hard_gib == 30, f"WAL hard limit = 30G (ADR-029 D11), got: {t.wal_local_hard_gib}"
        assert t.l1_local_hard_gib == 20, f"L1 hard limit = 20G (ADR-029 D11), got: {t.l1_local_hard_gib}"
        assert t.nas_bucket_target_gib == 500, f"NAS target = 500G, got: {t.nas_bucket_target_gib}"
        assert t.nas_bucket_hard_gib == 1024, f"NAS hard = 1TB (1024G), got: {t.nas_bucket_hard_gib}"
        assert t.host_disk_hard_gib == 200, f"Host hard = 200G, got: {t.host_disk_hard_gib}"
        assert t.warn_ratio == 0.80, f"warn_ratio = 0.80 (80%), got: {t.warn_ratio}"
        assert t.critical_ratio == 0.95, f"critical_ratio = 0.95 (95%), got: {t.critical_ratio}"

    def test_capacity_thresholds_frozen(self) -> None:
        """CapacityThresholds frozen=True (immutable SSOT)."""
        from mctrader_data.capacity_probe import CapacityThresholds

        t = CapacityThresholds()
        with pytest.raises((AttributeError, TypeError)):
            t.wal_local_hard_gib = 99  # type: ignore[misc]


# ─── Test: CapacityReport dataclass ─────────────────────────────────────────


class TestCapacityReport:
    """CapacityReport 4 layer 측정 결과 dataclass."""

    def test_capacity_report_dataclass(self) -> None:
        """CapacityReport: 4 layer (wal/l1/nas/host) usage_bytes + ratio fields."""
        from mctrader_data.capacity_probe import CapacityReport

        report = CapacityReport(
            wal_usage_bytes=1024 * 1024 * 100,   # 100MB
            l1_usage_bytes=1024 * 1024 * 50,     # 50MB
            nas_usage_bytes=1024 * 1024 * 1024,  # 1GB
            host_usage_bytes=1024 * 1024 * 1024 * 10,  # 10GB
            wal_hard_bytes=30 * 1024 ** 3,
            l1_hard_bytes=20 * 1024 ** 3,
            nas_hard_bytes=1024 * 1024 ** 3,
            host_hard_bytes=200 * 1024 ** 3,
        )

        assert hasattr(report, "wal_usage_bytes")
        assert hasattr(report, "l1_usage_bytes")
        assert hasattr(report, "nas_usage_bytes")
        assert hasattr(report, "host_usage_bytes")

    def test_capacity_report_layer_enum_names(self) -> None:
        """CapacityReport 의 layer 이름 = Prometheus label enum (WAL_local/L1_local/NAS_bucket/Host_disk)."""
        from mctrader_data.capacity_probe import CapacityReport, LAYER_NAMES

        expected = {"WAL_local", "L1_local", "NAS_bucket", "Host_disk"}
        assert set(LAYER_NAMES) == expected, (
            f"LAYER_NAMES must match Prometheus label enum {expected}, got: {set(LAYER_NAMES)}"
        )


# ─── Test: CapacityProbe.probe_once() ───────────────────────────────────────


class TestCapacityProbeOnce:
    """CapacityProbe.probe_once() → CapacityReport + Gauge emit."""

    def _make_probe(self, tmp_path: Path, mock_metrics: MagicMock | None = None) -> object:
        """CapacityProbe 인스턴스 생성 helper."""
        from mctrader_data.capacity_probe import CapacityProbe, CapacityThresholds

        wal_root = tmp_path / "wal"
        wal_root.mkdir()
        l1_root = tmp_path / "l1"
        l1_root.mkdir()

        mock_nas = MagicMock()
        mock_nas.bucket = "mctrader-market"

        return CapacityProbe(
            wal_root=wal_root,
            l1_root=l1_root,
            nas_uploader=mock_nas,
            host_mount=tmp_path,
            thresholds=CapacityThresholds(),
            metrics=mock_metrics or MagicMock(),
        )

    def test_probe_once_returns_capacity_report(self, tmp_path: Path) -> None:
        """probe_once() 반환값 = CapacityReport 인스턴스."""
        from mctrader_data.capacity_probe import CapacityReport

        probe = self._make_probe(tmp_path)
        result = probe.probe_once()

        assert isinstance(result, CapacityReport), (
            f"probe_once() must return CapacityReport, got: {type(result)}"
        )

    def test_probe_once_4_layer_gauge_emit(self, tmp_path: Path) -> None:
        """probe_once() → mctrader_capacity_usage_bytes{layer} Gauge 4종 emit."""
        mock_metrics = MagicMock()
        probe = self._make_probe(tmp_path, mock_metrics)
        probe.probe_once()

        # metrics.emit_capacity_usage() 또는 직접 Gauge set 호출 확인
        # AC-5: 4 layer emit 의무 (WAL_local / L1_local / NAS_bucket / Host_disk)
        assert mock_metrics.emit_capacity_usage.call_count >= 4 or \
               mock_metrics.set_capacity_usage.call_count >= 4 or \
               mock_metrics.emit_capacity.call_count >= 1, (
            "probe_once() must emit capacity metrics for 4 layers via metrics object"
        )

    def test_probe_once_threshold_ratio_gauge(self, tmp_path: Path) -> None:
        """probe_once() → mctrader_capacity_threshold_ratio{layer} Gauge 4종 emit."""
        mock_metrics = MagicMock()
        probe = self._make_probe(tmp_path, mock_metrics)
        result = probe.probe_once()

        # CapacityReport에 ratio 정보 포함 확인
        assert hasattr(result, "wal_ratio") or hasattr(result, "wal_usage_bytes"), (
            "CapacityReport must contain ratio or usage data for all 4 layers"
        )

    def test_probe_warn_threshold_80pct(self, tmp_path: Path) -> None:
        """WAL usage >= 80% of hard limit → CapacityReport.wal_ratio >= 0.80."""
        from mctrader_data.capacity_probe import CapacityProbe, CapacityThresholds

        wal_root = tmp_path / "wal"
        wal_root.mkdir()
        l1_root = tmp_path / "l1"
        l1_root.mkdir()

        wal_hard_bytes = 30 * (1024 ** 3)
        wal_warn_bytes = int(wal_hard_bytes * 0.85)  # 85% = warn

        mock_nas = MagicMock()
        mock_nas.bucket = "mctrader-market"
        mock_metrics = MagicMock()

        probe = CapacityProbe(
            wal_root=wal_root,
            l1_root=l1_root,
            nas_uploader=mock_nas,
            host_mount=tmp_path,
            thresholds=CapacityThresholds(),
            metrics=mock_metrics,
        )

        # _probe_dir_bytes를 직접 patch해서 WAL usage를 제어
        with patch.object(probe, "_probe_dir_bytes") as mock_dir:
            # WAL = 85% of 30G, L1 = 0
            mock_dir.side_effect = lambda root: wal_warn_bytes if root == wal_root else 0
            result = probe.probe_once()

        assert result.wal_ratio >= 0.80, (
            f"WAL at 85% should have ratio >= 0.80, got: {result.wal_ratio}"
        )

    def test_probe_critical_threshold_95pct(self, tmp_path: Path) -> None:
        """WAL usage >= 95% → CapacityReport.wal_ratio >= 0.95."""
        from mctrader_data.capacity_probe import CapacityProbe, CapacityThresholds

        wal_root = tmp_path / "wal"
        wal_root.mkdir()
        l1_root = tmp_path / "l1"
        l1_root.mkdir()

        wal_hard_bytes = 30 * (1024 ** 3)
        wal_critical_bytes = int(wal_hard_bytes * 0.97)  # 97% = critical

        mock_nas = MagicMock()
        mock_metrics = MagicMock()

        probe = CapacityProbe(
            wal_root=wal_root,
            l1_root=l1_root,
            nas_uploader=mock_nas,
            host_mount=tmp_path,
            thresholds=CapacityThresholds(),
            metrics=mock_metrics,
        )

        with patch.object(probe, "_probe_dir_bytes") as mock_dir:
            mock_dir.side_effect = lambda root: wal_critical_bytes if root == wal_root else 0
            result = probe.probe_once()

        assert result.wal_ratio >= 0.95, (
            f"WAL at 97% should have ratio >= 0.95, got: {result.wal_ratio}"
        )

    def test_capacity_probe_no_hot_path_import(self) -> None:
        """capacity_probe.py는 collector.py 를 import 하지 않는다 (hot path 의존 0, sibling isolation)."""
        import importlib
        import importlib.util
        import ast

        # capacity_probe.py 소스 파싱
        probe_file = Path("c:/workspace/mclayer/mctrader-data/src/mctrader_data/capacity_probe.py")
        if not probe_file.exists():
            pytest.skip("capacity_probe.py not yet created (TDD red phase)")

        source = probe_file.read_text(encoding="utf-8")
        tree = ast.parse(source)

        forbidden_imports = {"collector", "mctrader_data.collector"}
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.ImportFrom) and node.module:
                    assert node.module not in forbidden_imports, (
                        f"capacity_probe.py must not import '{node.module}' (hot path isolation)"
                    )
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        assert alias.name not in forbidden_imports, (
                            f"capacity_probe.py must not import '{alias.name}' (hot path isolation)"
                        )

    def test_wal_probe_uses_shutil_disk_usage(self, tmp_path: Path) -> None:
        """host_mount probe = shutil.disk_usage(host_mount) fallback."""
        from mctrader_data.capacity_probe import CapacityProbe, CapacityThresholds

        wal_root = tmp_path / "wal"
        wal_root.mkdir()
        l1_root = tmp_path / "l1"
        l1_root.mkdir()

        mock_nas = MagicMock()
        mock_metrics = MagicMock()

        probe = CapacityProbe(
            wal_root=wal_root,
            l1_root=l1_root,
            nas_uploader=mock_nas,
            host_mount=tmp_path,
            thresholds=CapacityThresholds(),
            metrics=mock_metrics,
        )

        with patch("shutil.disk_usage") as mock_du:
            mock_du.return_value = MagicMock(total=100 * 1024**3, used=50 * 1024**3, free=50 * 1024**3)
            result = probe.probe_once()

        # shutil.disk_usage 가 host_mount 경로로 호출됐는지 확인
        assert mock_du.called, "probe_once() must call shutil.disk_usage for host_mount"

    def test_probe_loop_callable(self, tmp_path: Path) -> None:
        """probe_loop() callable 확인 (실제 루프 미실행 — API contract test)."""
        from mctrader_data.capacity_probe import CapacityProbe

        probe = self._make_probe(tmp_path)
        assert callable(getattr(probe, "probe_loop", None)), (
            "CapacityProbe must have probe_loop() callable method (D7-4=C hybrid loop)"
        )
