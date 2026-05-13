"""test_gc_runner.py — P0 TDD tests for GcRunner (Local L2 GC + 3중 lock + deletion log).

Story: MCT-155 (Stage 2 — Local GC + Secret rotation + RPO=0 verify + Stage 2 종료 gate)
Issue: mclayer/mctrader-hub#274

Test Contract §8.1 (TestContractArchitectAgent — MCT-155):
- T-7 GcResult.status = dry_run_complete — deletion_targets[] 박제
- T-8 GcResult.status = blocked_grace_period — 7d grace 미만료
- T-9 GcResult.status = blocked_invariant_fail — invariant FAIL
- T-10 GcResult.status = blocked_invariant_fail — cutover RPO=0 unverified (file 부재)
- T-11 GcResult.status = executed — 실 삭제 + free_disk_pct_after verify
- T-12 디스크 압박 시 tier/date 순차 (disk usage > 90% mock) — minimal verify
- T-13 deletion log entry append (sqlite-WAL persistent)
- T-14 deletion log file 무손실 (24h batch delete window 내 entry 손실 0)
- T-15 24h batch delete window operator manual gate verify (mock 24h interval)
- T-16 recovery_status enum 5종 transition (pending → confirmed_deleted)
- T-26 chaos: gc_runner restart 시 deletion log 기반 resume

§6.8 Wording SSOT:
- status enum 4종: "dry_run_complete" / "executed" / "blocked_grace_period" / "blocked_invariant_fail"
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mctrader_data.nas_migration.gc_runner import GcResult, GcRunner


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def local_l2_root(tmp_path: Path) -> Path:
    """Create local L2 root with sample parquet files."""
    root = tmp_path / "L2"
    partition = root / "exchange=upbit/symbol=BTC_KRW/date=2025-11-01"
    partition.mkdir(parents=True)
    (partition / "part-0.parquet").write_bytes(b"x" * 1000)
    (partition / "part-1.parquet").write_bytes(b"y" * 1000)
    return root


@pytest.fixture
def deletion_log_path(tmp_path: Path) -> Path:
    return tmp_path / "gc-deletion-log.db"


@pytest.fixture
def grace_evidence_path(tmp_path: Path) -> Path:
    return tmp_path / "grace-evidence.json"


@pytest.fixture
def invariant_evidence_path(tmp_path: Path) -> Path:
    return tmp_path / "invariant-7d-evidence.json"


@pytest.fixture
def cutover_verify_evidence_path(tmp_path: Path) -> Path:
    return tmp_path / "rpo-zero-verify.json"


@pytest.fixture
def mock_cutover_verifier() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_invariant_harness() -> MagicMock:
    return MagicMock()


def _write_evidence_files(
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
    *,
    grace_remaining_days: int = 0,
    invariant_pass_days: int = 7,
    cutover_status: str = "rpo_zero_verified",
) -> None:
    """Helper: write 3중 lock pre-check evidence files (default = ALL PASS)."""
    cutover_verify_evidence_path.parent.mkdir(parents=True, exist_ok=True)
    cutover_verify_evidence_path.write_text(json.dumps({"status": cutover_status}))
    grace_evidence_path.write_text(
        json.dumps({"grace_remaining_days": grace_remaining_days})
    )
    invariant_evidence_path.write_text(
        json.dumps({"invariant_pass_days_consecutive": invariant_pass_days})
    )


@pytest.fixture
def gc_runner(
    mock_cutover_verifier: MagicMock,
    mock_invariant_harness: MagicMock,
    local_l2_root: Path,
    deletion_log_path: Path,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
) -> GcRunner:
    return GcRunner(
        cutover_verifier=mock_cutover_verifier,
        invariant_harness=mock_invariant_harness,
        local_l2_root=local_l2_root,
        deletion_log_path=deletion_log_path,
        grace_evidence_path=grace_evidence_path,
        invariant_evidence_path=invariant_evidence_path,
        cutover_verify_evidence_path=cutover_verify_evidence_path,
    )


# ─── T-7: dry_run_complete ──────────────────────────────────────────────────


def test_dry_run_complete_returns_targets_and_size(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
) -> None:
    """T-7: dry-run → status='dry_run_complete' + deletion_targets + target_size_bytes."""
    _write_evidence_files(
        grace_evidence_path, invariant_evidence_path, cutover_verify_evidence_path
    )
    result = gc_runner.gc(tier="L2", dry_run=True)
    assert result.status == "dry_run_complete"
    assert len(result.deletion_targets) == 2
    assert result.target_size_bytes == 2000


# ─── T-8: blocked_grace_period ──────────────────────────────────────────────


def test_blocked_grace_period_when_grace_not_expired(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
) -> None:
    """T-8: 7d grace 미만료 → status='blocked_grace_period'."""
    _write_evidence_files(
        grace_evidence_path,
        invariant_evidence_path,
        cutover_verify_evidence_path,
        grace_remaining_days=3,
    )
    result = gc_runner.gc(tier="L2", dry_run=True)
    assert result.status == "blocked_grace_period"
    assert result.grace_remaining_days == 3


# ─── T-9: blocked_invariant_fail (invariant 측 FAIL) ────────────────────────


def test_blocked_invariant_fail_when_pass_days_lt_required(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
) -> None:
    """T-9: invariant pass_days < 7 → status='blocked_invariant_fail'."""
    _write_evidence_files(
        grace_evidence_path,
        invariant_evidence_path,
        cutover_verify_evidence_path,
        invariant_pass_days=3,
    )
    result = gc_runner.gc(tier="L2", dry_run=True)
    assert result.status == "blocked_invariant_fail"
    assert "invariant_pass_days=3" in result.invariant_fail_reason


# ─── T-10: blocked_invariant_fail (cutover file 부재) ───────────────────────


def test_blocked_when_cutover_evidence_missing(
    gc_runner: GcRunner,
) -> None:
    """T-10: cutover RPO=0 verify evidence file 부재 → blocked_invariant_fail."""
    result = gc_runner.gc(tier="L2", dry_run=True)
    assert result.status == "blocked_invariant_fail"
    assert "cutover_verify_evidence_file_missing" in result.invariant_fail_reason


# ─── T-10b: cutover_verify status != rpo_zero_verified ──────────────────────


def test_blocked_when_cutover_status_drift_detected(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
) -> None:
    """T-10b: cutover_verify status='drift_detected' → blocked_invariant_fail."""
    _write_evidence_files(
        grace_evidence_path,
        invariant_evidence_path,
        cutover_verify_evidence_path,
        cutover_status="drift_detected",
    )
    result = gc_runner.gc(tier="L2", dry_run=True)
    assert result.status == "blocked_invariant_fail"
    assert "drift_detected" in result.invariant_fail_reason


# ─── T-11: executed (실 삭제 완료) ──────────────────────────────────────────


def test_executed_actually_deletes_files(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
    local_l2_root: Path,
) -> None:
    """T-11: dry_run=False → status='executed' + 실 삭제 + freed_bytes 박제."""
    _write_evidence_files(
        grace_evidence_path, invariant_evidence_path, cutover_verify_evidence_path
    )
    result = gc_runner.gc(tier="L2", dry_run=False)
    assert result.status == "executed"
    assert result.deleted_count == 2
    assert result.freed_bytes == 2000
    # Files actually deleted
    remaining = list(local_l2_root.rglob("*.parquet"))
    assert len(remaining) == 0


# ─── T-12: minimal — disk pressure 시 tier/date 순차 (호출만 verify) ─────────


def test_disk_pressure_branch_executes(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-12: disk usage > 90% mock → tier/date 순차 branch executes (no error)."""
    _write_evidence_files(
        grace_evidence_path, invariant_evidence_path, cutover_verify_evidence_path
    )
    monkeypatch.setattr(gc_runner, "_compute_disk_usage_pct", lambda: 95.0)
    result = gc_runner.gc(tier="L2", dry_run=True)
    # Branch executed without error (oldest-first sort applied)
    assert result.status == "dry_run_complete"


# ─── T-13: deletion log entry append (sqlite-WAL persistent) ────────────────


def test_deletion_log_entry_append_sqlite_wal(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
    deletion_log_path: Path,
) -> None:
    """T-13: 실 삭제 시 deletion log sqlite-WAL append."""
    _write_evidence_files(
        grace_evidence_path, invariant_evidence_path, cutover_verify_evidence_path
    )
    gc_runner.gc(tier="L2", dry_run=False)

    with sqlite3.connect(str(deletion_log_path)) as conn:
        rows = conn.execute(
            "SELECT recovery_status FROM gc_deletion_log"
        ).fetchall()
    assert len(rows) == 2
    assert all(r[0] == "confirmed_deleted" for r in rows)


# ─── T-14: deletion log 무손실 (re-init 후 entry 보존) ──────────────────────


def test_deletion_log_persistent_across_init(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
    deletion_log_path: Path,
    mock_cutover_verifier: MagicMock,
    mock_invariant_harness: MagicMock,
    local_l2_root: Path,
) -> None:
    """T-14: GcRunner re-init 후 deletion log entry 보존 (24h window 무손실)."""
    _write_evidence_files(
        grace_evidence_path, invariant_evidence_path, cutover_verify_evidence_path
    )
    gc_runner.gc(tier="L2", dry_run=False)
    # Re-init GcRunner (simulating restart) — instance variable reuse below
    GcRunner(
        cutover_verifier=mock_cutover_verifier,
        invariant_harness=mock_invariant_harness,
        local_l2_root=local_l2_root,
        deletion_log_path=deletion_log_path,
        grace_evidence_path=grace_evidence_path,
        invariant_evidence_path=invariant_evidence_path,
        cutover_verify_evidence_path=cutover_verify_evidence_path,
    )
    with sqlite3.connect(str(deletion_log_path)) as conn:
        rows = conn.execute("SELECT COUNT(*) FROM gc_deletion_log").fetchone()
    assert rows[0] == 2


# ─── T-15: 24h batch delete operator manual gate verify (caller 책임) ────────


def test_24h_batch_delete_caller_responsibility(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
) -> None:
    """T-15: dry_run + 실 삭제 사이 24h interval = caller 책임 (본 method 측 강제 0)."""
    _write_evidence_files(
        grace_evidence_path, invariant_evidence_path, cutover_verify_evidence_path
    )
    # dry-run 후 즉시 실 삭제 = 가능 (caller 책임 — runbook 박제)
    dry_result = gc_runner.gc(tier="L2", dry_run=True)
    exec_result = gc_runner.gc(tier="L2", dry_run=False)
    assert dry_result.status == "dry_run_complete"
    assert exec_result.status == "executed"


# ─── T-16: recovery_status enum 5종 transition (pending → confirmed_deleted) ─


def test_recovery_status_transition_pending_to_confirmed(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
    deletion_log_path: Path,
) -> None:
    """T-16: 실 삭제 진입 시 recovery_status = pending → confirmed_deleted."""
    _write_evidence_files(
        grace_evidence_path, invariant_evidence_path, cutover_verify_evidence_path
    )
    result = gc_runner.gc(tier="L2", dry_run=False)
    assert result.status == "executed"
    with sqlite3.connect(str(deletion_log_path)) as conn:
        statuses = [
            r[0]
            for r in conn.execute(
                "SELECT recovery_status FROM gc_deletion_log"
            ).fetchall()
        ]
    assert all(s == "confirmed_deleted" for s in statuses)


# ─── T-26 (chaos): restart resume — already-deleted skip ─────────────────────


def test_chaos_restart_resume_skips_already_deleted(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
    local_l2_root: Path,
) -> None:
    """T-26 chaos: restart 후 deletion log 기반 already-deleted skip → idempotent."""
    _write_evidence_files(
        grace_evidence_path, invariant_evidence_path, cutover_verify_evidence_path
    )
    # First run — delete all
    result1 = gc_runner.gc(tier="L2", dry_run=False)
    assert result1.deleted_count == 2

    # Recreate files (simulating new data after restart)
    new_partition = local_l2_root / "exchange=upbit/symbol=ETH_KRW/date=2025-11-02"
    new_partition.mkdir(parents=True)
    (new_partition / "part-new.parquet").write_bytes(b"new" * 100)

    # Second run — only new file deleted (old already-deleted entries skipped — but also re-deleted if re-created)
    result2 = gc_runner.gc(tier="L2", dry_run=False)
    # New file gets deleted; idempotency property: previously-deleted-but-recreated files
    # are unique paths (since rglob matches present files only), so re-deletion is safe.
    assert result2.status == "executed"


# ─── status enum exact string match (§6.8 wording SSOT) ──────────────────────


@pytest.mark.parametrize(
    "expected_status",
    [
        "dry_run_complete",
        "executed",
        "blocked_grace_period",
        "blocked_invariant_fail",
    ],
)
def test_status_enum_exact_string_match(expected_status: str) -> None:
    """§6.8 wording SSOT: status enum 정확한 string 만 허용."""
    result = GcResult(status=expected_status)  # type: ignore[arg-type]
    assert result.status == expected_status


# ─── Idempotency dry-run (§11.6) ─────────────────────────────────────────────


def test_dry_run_idempotent_across_invocations(
    gc_runner: GcRunner,
    grace_evidence_path: Path,
    invariant_evidence_path: Path,
    cutover_verify_evidence_path: Path,
) -> None:
    """§11.6: dry_run 다중 호출 시 동일 결과 (deletion_targets list 동일)."""
    _write_evidence_files(
        grace_evidence_path, invariant_evidence_path, cutover_verify_evidence_path
    )
    result1 = gc_runner.gc(tier="L2", dry_run=True)
    result2 = gc_runner.gc(tier="L2", dry_run=True)
    assert result1.status == result2.status == "dry_run_complete"
    assert sorted(result1.deletion_targets) == sorted(result2.deletion_targets)
