"""MCT-179 D5 — measure_wal_baseline.py unit tests.

AC-1: paper-synthetic mode → exit 0 + JSON stdout (WAL <= 30G)
AC-2: mock 45G WAL → exit 7 (EXCEED, ADR-029 D11 hard_limit trigger)
AC-3: probe fail (permission error) → exit 99

Tests use monkeypatching against scripts/measure_wal_baseline._probe_dir_bytes
to avoid filesystem dependency.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

import pytest

# ─── dynamic import: scripts/ は src/ 外のため importlib で直接ロード ──────────
_SCRIPT_PATH = Path(__file__).parent.parent / "scripts" / "measure_wal_baseline.py"
_spec = importlib.util.spec_from_file_location("measure_wal_baseline", _SCRIPT_PATH)
assert _spec is not None
_measure_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_measure_mod)  # type: ignore[union-attr]


# ─── helper ──────────────────────────────────────────────────────────────────

def _run_main(env_overrides: dict, mock_bytes: int | None = None, probe_raises: bool = False):
    """measure_wal_baseline.main() 실행 + (exit_code, stdout_lines, stderr_lines) 반환.

    Args:
        env_overrides: os.environ override dict
        mock_bytes: _probe_dir_bytes 반환값 (None 시 실제 실행)
        probe_raises: True 시 _probe_dir_bytes 에서 Exception raise
    """
    import io

    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    def _fake_probe(root: Path) -> int:
        if probe_raises:
            raise PermissionError("mocked probe fail")
        assert mock_bytes is not None
        return mock_bytes

    with (
        patch.dict("os.environ", env_overrides, clear=False),
        patch.object(_measure_mod, "_probe_dir_bytes", side_effect=_fake_probe),
        patch("sys.stdout", stdout_buf),
        patch("sys.stderr", stderr_buf),
    ):
        exit_code = _measure_mod.main()

    return exit_code, stdout_buf.getvalue(), stderr_buf.getvalue()


# ─── AC-1: paper-synthetic mode, WAL 5G (mock) → exit 0 + JSON ──────────────

class TestPaperSyntheticPass:
    """AC-1 verify: MEASURE_MODE=paper-synthetic, WAL 5G mock → exit 0."""

    def test_exit_code_0(self):
        # 5 GiB = 5 * 1024**3
        exit_code, stdout, _ = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            mock_bytes=5 * (1024 ** 3),
        )
        assert exit_code == 0, f"expected exit 0, got {exit_code}"

    def test_json_stdout_valid(self):
        exit_code, stdout, _ = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            mock_bytes=5 * (1024 ** 3),
        )
        data = json.loads(stdout)
        assert data["mode"] == "paper-synthetic"
        assert data["verdict"] == "PASS"
        assert data["hard_limit_gb"] == 30
        assert data["wal_peak_gb"] == pytest.approx(5.0, abs=0.01)

    def test_json_contains_required_keys(self):
        _, stdout, _ = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            mock_bytes=1 * (1024 ** 3),
        )
        data = json.loads(stdout)
        required_keys = {"mode", "wal_root", "wal_bytes", "wal_peak_gb", "hard_limit_gb", "verdict", "hypothesis_range"}
        assert required_keys.issubset(data.keys()), f"missing keys: {required_keys - data.keys()}"

    def test_wal_bytes_in_json(self):
        mock_b = 5 * (1024 ** 3)
        _, stdout, _ = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            mock_bytes=mock_b,
        )
        data = json.loads(stdout)
        assert data["wal_bytes"] == mock_b


# ─── AC-2: mock 45G WAL → exit 7 (EXCEED) ───────────────────────────────────

class TestExceedExit7:
    """AC-2 verify: WAL 45G mock → exit 7 (ADR-029 D11 hard_limit amendment trigger)."""

    def test_exit_code_7(self):
        # 45 GiB = 45 * 1024**3
        exit_code, stdout, _ = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            mock_bytes=45 * (1024 ** 3),
        )
        assert exit_code == 7, f"expected exit 7 (EXCEED), got {exit_code}"

    def test_verdict_exceed(self):
        exit_code, stdout, _ = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            mock_bytes=45 * (1024 ** 3),
        )
        data = json.loads(stdout)
        assert data["verdict"] == "EXCEED"

    def test_stderr_contains_amendment_mention(self):
        _, _, stderr = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            mock_bytes=45 * (1024 ** 3),
        )
        assert "ADR-029" in stderr, f"expected 'ADR-029' in stderr, got: {stderr!r}"
        assert "D11" in stderr

    def test_json_wal_peak_gb_45(self):
        _, stdout, _ = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            mock_bytes=45 * (1024 ** 3),
        )
        data = json.loads(stdout)
        assert data["wal_peak_gb"] == pytest.approx(45.0, abs=0.01)

    def test_boundary_exactly_30g_is_pass(self):
        """ADR-029 D11: WAL == hard_limit → PASS (엄격 초과만 EXCEED)."""
        exit_code, stdout, _ = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            mock_bytes=30 * (1024 ** 3),
        )
        assert exit_code == 0
        data = json.loads(stdout)
        assert data["verdict"] == "PASS"

    def test_custom_hard_limit_env(self):
        """WAL_HARD_LIMIT_GB env override: limit=10G, WAL=15G → exit 7."""
        exit_code, stdout, _ = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic", "WAL_HARD_LIMIT_GB": "10"},
            mock_bytes=15 * (1024 ** 3),
        )
        assert exit_code == 7
        data = json.loads(stdout)
        assert data["hard_limit_gb"] == 10
        assert data["verdict"] == "EXCEED"


# ─── AC-3: probe fail → exit 99 ──────────────────────────────────────────────

class TestProbeFailExit99:
    """probe 실패 → exit 99."""

    def test_exit_code_99_on_probe_error(self):
        exit_code, _, stderr = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            probe_raises=True,
        )
        assert exit_code == 99, f"expected exit 99 (probe fail), got {exit_code}"

    def test_stderr_contains_error_on_probe_fail(self):
        _, _, stderr = _run_main(
            env_overrides={"MEASURE_MODE": "paper-synthetic"},
            probe_raises=True,
        )
        assert "ERROR" in stderr.upper(), f"expected ERROR in stderr: {stderr!r}"

    def test_invalid_hard_limit_env_exit99(self):
        """WAL_HARD_LIMIT_GB=bad → exit 99 (parse error)."""
        exit_code, _, stderr = _run_main(
            env_overrides={"WAL_HARD_LIMIT_GB": "not_an_int"},
            mock_bytes=1,
        )
        assert exit_code == 99


# ─── Production mode note ─────────────────────────────────────────────────────

class TestProductionMode:
    """production mode: JSON 내 production_note 포함."""

    def test_production_mode_json_contains_note(self):
        _, stdout, _ = _run_main(
            env_overrides={"MEASURE_MODE": "production"},
            mock_bytes=5 * (1024 ** 3),
        )
        data = json.loads(stdout)
        assert data["mode"] == "production"
        assert "production_note" in data, "production mode should include production_note key"

    def test_production_mode_still_exit0_when_pass(self):
        exit_code, _, _ = _run_main(
            env_overrides={"MEASURE_MODE": "production"},
            mock_bytes=5 * (1024 ** 3),
        )
        assert exit_code == 0


# ─── capacity_probe.measure_wal_bytes + emit_wal_capacity_gauge ──────────────

class TestCapacityProbeWalBytes:
    """MCT-179 D5: CapacityProbe.measure_wal_bytes() + emit_wal_capacity_gauge()."""

    def test_measure_wal_bytes_returns_dir_size(self, tmp_path):
        """measure_wal_bytes() = wal_root 재귀 bytes 합산."""
        from mctrader_data.capacity_probe import CapacityProbe, CapacityThresholds
        from unittest.mock import MagicMock

        wal_root = tmp_path / "wal"
        wal_root.mkdir()
        # 1 KB 파일 3개 = 3072 bytes
        for i in range(3):
            (wal_root / f"seg_{i:03d}.parquet").write_bytes(b"x" * 1024)

        probe = CapacityProbe(
            wal_root=wal_root,
            l1_root=tmp_path / "l1",
            nas_uploader=MagicMock(),
            host_mount=tmp_path,
            thresholds=CapacityThresholds(),
            metrics=None,
        )
        result = probe.measure_wal_bytes()
        assert result == 3 * 1024, f"expected 3072 bytes, got {result}"

    def test_measure_wal_bytes_empty_dir(self, tmp_path):
        """빈 WAL root → 0."""
        from mctrader_data.capacity_probe import CapacityProbe, CapacityThresholds
        from unittest.mock import MagicMock

        wal_root = tmp_path / "wal"
        wal_root.mkdir()
        probe = CapacityProbe(
            wal_root=wal_root,
            l1_root=tmp_path / "l1",
            nas_uploader=MagicMock(),
            host_mount=tmp_path,
            thresholds=CapacityThresholds(),
            metrics=None,
        )
        assert probe.measure_wal_bytes() == 0

    def test_measure_wal_bytes_missing_dir(self, tmp_path):
        """WAL root 없음 → 0 (graceful)."""
        from mctrader_data.capacity_probe import CapacityProbe, CapacityThresholds
        from unittest.mock import MagicMock

        probe = CapacityProbe(
            wal_root=tmp_path / "nonexistent_wal",
            l1_root=tmp_path / "l1",
            nas_uploader=MagicMock(),
            host_mount=tmp_path,
            thresholds=CapacityThresholds(),
            metrics=None,
        )
        assert probe.measure_wal_bytes() == 0

    def test_emit_wal_capacity_gauge_calls_exporter(self, tmp_path):
        """emit_wal_capacity_gauge → metrics.emit_capacity_usage + emit_capacity_ratio 호출."""
        from mctrader_data.capacity_probe import CapacityProbe, CapacityThresholds
        from unittest.mock import MagicMock

        wal_root = tmp_path / "wal"
        wal_root.mkdir()
        (wal_root / "seg.parquet").write_bytes(b"x" * 512)

        mock_metrics = MagicMock()
        probe = CapacityProbe(
            wal_root=wal_root,
            l1_root=tmp_path / "l1",
            nas_uploader=MagicMock(),
            host_mount=tmp_path,
            thresholds=CapacityThresholds(),
            metrics=mock_metrics,
        )
        probe.emit_wal_capacity_gauge()

        mock_metrics.emit_capacity_usage.assert_called_once_with(layer="WAL_local", bytes_val=512)
        mock_metrics.emit_capacity_ratio.assert_called_once()
        _, kwargs = mock_metrics.emit_capacity_ratio.call_args
        assert kwargs["layer"] == "WAL_local"
        assert kwargs["ratio"] == pytest.approx(512 / (30 * 1024 ** 3), rel=1e-6)

    def test_emit_wal_capacity_gauge_no_metrics(self, tmp_path):
        """metrics=None → emit skip (no exception)."""
        from mctrader_data.capacity_probe import CapacityProbe, CapacityThresholds
        from unittest.mock import MagicMock

        probe = CapacityProbe(
            wal_root=tmp_path / "wal",
            l1_root=tmp_path / "l1",
            nas_uploader=MagicMock(),
            host_mount=tmp_path,
            thresholds=CapacityThresholds(),
            metrics=None,
        )
        probe.emit_wal_capacity_gauge()  # must not raise
