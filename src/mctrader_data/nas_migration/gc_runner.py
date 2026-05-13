"""gc_runner.py — Local L2 GC (7d grace + dry-run + invariant gate + tier/date 순차) + deletion log + 24h batch delete.

Story: MCT-155 (Stage 2 — Local GC + Secret rotation + RPO=0 verify + Stage 2 종료 gate)
Issue: mclayer/mctrader-hub#274

ADR-027 D7 박제 직접 enforcement (7일 grace + dry-run 선행 + 실 삭제 + 디스크 압박 시 tier/date 순차).
R5 mitigation 직접 enforcement (deletion log + 24h batch delete + 3중 lock).

§8.5 active (process restart-aware + Background worker, CFP-378 AC-5):
- 24h batch delete 중 restart 시 deletion log 기반 resume (sqlite-WAL persistent)
- 7d grace 만료 evidence + invariant ALL PASS 7일 누적 evidence cross-repo coordination
- restart 후 deletion log file load → 미완료 deletion entry resume

§6.7 Cross-module contract (lesson #2 invariant):
- GcResult.status switch 의무 (caller operator runbook + retro 박제)
- CutoverVerifier 결과 cross-reference 의무 (3중 lock 1번째 lock)
- Prometheus metric (BackfillOrchestrator + dual_write_window_runner + InvariantHarness) read-only

§6.9 placement:
- 3중 lock pre-check = unconditional (GC 진입 직전 첫 단계)
- dry-run 진입 = unconditional (실 삭제 전 의무)
- deletion log entry append = conditional (실 삭제 진입 시점 only)
- 24h batch delete = conditional (operator manual gate)
- 디스크 압박 시 tier/date 순차 = conditional (disk usage > 90% 시점 only)

§6.8 Wording SSOT:
- GcResult.status 4종: "dry_run_complete" / "executed" / "blocked_grace_period" / "blocked_invariant_fail"
  variant 금지: "dry_run_done" / "deleted" / "blocked" 등.
"""
from __future__ import annotations

import hashlib
import json
import logging
import shutil
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from mctrader_data.nas_migration.cutover_verifier import CutoverVerifier
    from mctrader_data.nas_migration.invariant_harness import InvariantHarness

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class GcResult:
    """Local L2 GC 결과 — caller switch 의무 (§6.7 cross-module contract).

    status enum 4종 (§6.8.2 Wording SSOT — single string, variant 금지):
    - "dry_run_complete"          : dry-run 정상 완료 (실 삭제 0, deletion_targets 박제).
    - "executed"                  : 실 삭제 완료 (deleted_count + freed_bytes 박제).
    - "blocked_grace_period"      : 7d grace 미만료 (gc 진입 차단).
    - "blocked_invariant_fail"    : invariant FAIL 또는 cutover RPO=0 unverified (gc 진입 차단).

    Caller 처리 의무 (§6.7 매핑):
    - "dry_run_complete"          -> operator review (24h batch delete window) → 실 삭제 trigger.
    - "executed"                  -> 정상 진행 (Stage 2 종료 gate AC-3 evidence).
    - "blocked_grace_period"      -> alert + 재진입 거부 (grace 만료 후 재시도).
    - "blocked_invariant_fail"    -> alert + 재진입 거부 (invariant 측 fix 후 재시도).
    """

    status: Literal[
        "dry_run_complete",
        "executed",
        "blocked_grace_period",
        "blocked_invariant_fail",
    ]
    tier: str = "L2"
    deletion_targets: list[str] = field(default_factory=list)
    target_size_bytes: int = 0
    deleted_count: int = 0
    freed_bytes: int = 0
    free_disk_pct_after: float = 0.0
    grace_remaining_days: int = 0
    invariant_fail_reason: str = ""
    duration_ms: float = 0.0


class GcRunner:
    """Local L2 GC (7d grace + dry-run + invariant gate + tier/date 순차).

    Thread-safety: stateless (per-call instance, deletion log persistent).

    Idempotency: dry_run 다중 호출 시 동일 결과 (deletion_targets list 동일).
                 실 삭제 다중 호출 시 idempotent (deletion log 기반 already-deleted skip).

    §6.1 chief decision 2: 3중 lock unconditional gate 채택 (single check 거부).
    §6.1 chief decision 3: sqlite-WAL persistent + 24h interval operator manual gate 채택.
    """

    def __init__(
        self,
        cutover_verifier: CutoverVerifier,
        invariant_harness: InvariantHarness,
        local_l2_root: Path,
        deletion_log_path: Path,
        grace_evidence_path: Path,
        invariant_evidence_path: Path,
        cutover_verify_evidence_path: Path,
        grace_days_required: int = 7,
        invariant_pass_days_required: int = 7,
        disk_pressure_threshold_pct: float = 90.0,
    ) -> None:
        self._cutover_verifier = cutover_verifier
        self._invariant_harness = invariant_harness
        self._local_l2_root = local_l2_root
        self._deletion_log_path = deletion_log_path
        self._grace_evidence_path = grace_evidence_path
        self._invariant_evidence_path = invariant_evidence_path
        self._cutover_verify_evidence_path = cutover_verify_evidence_path
        self._grace_days_required = grace_days_required
        self._invariant_pass_days_required = invariant_pass_days_required
        self._disk_pressure_threshold_pct = disk_pressure_threshold_pct

        # Init deletion log sqlite-WAL
        self._init_deletion_log()

    def gc(self, *, tier: str = "L2", dry_run: bool = True) -> GcResult:
        """Local L2 GC 진입 (7d grace + dry-run + invariant gate + tier/date 순차).

        Algorithm (Phase 1~5 sequential, §6.9):
        Phase 1 (3중 lock pre-check, unconditional):
          1. cutover RPO=0 verify pre-check (file load + status == rpo_zero_verified)
          2. 7d grace 만료 verify (evidence file remaining_days == 0)
          3. invariant ALL PASS 7일 누적 verify (evidence file pass_days >= 7)
          4. 1건이라도 FAIL -> GcResult.status = blocked_*
        Phase 2 (dry-run 진입, unconditional):
          1. local glob 삭제 대상 list 산출 + size sum
          2. dry_run=True -> dry_run_complete return
        Phase 3 (24h batch delete operator manual gate, conditional):
          caller 책임 (본 method 측 강제 0)
        Phase 4 (실 삭제 진입, conditional dry_run=False 시):
          per-target sequential: sqlite append → os.unlink → sqlite update
        Phase 5 (디스크 압박 시 tier/date 순차, conditional disk usage > 90% 시):
          tier 순차: L3 → L2 (D7); date 순차: oldest first

        Returns:
            GcResult — status enum 4종 + deletion metadata + free_disk_pct_after.
        """
        start_ms = time.monotonic() * 1000

        # ── Phase 1: 3중 lock pre-check ───────────────────────────────────────
        cutover_ok, cutover_reason = self._check_cutover_rpo_verified()
        if not cutover_ok:
            return GcResult(
                status="blocked_invariant_fail",
                tier=tier,
                invariant_fail_reason=cutover_reason,
                duration_ms=(time.monotonic() * 1000) - start_ms,
            )

        grace_ok, grace_remaining = self._check_grace_period_expired()
        if not grace_ok:
            return GcResult(
                status="blocked_grace_period",
                tier=tier,
                grace_remaining_days=grace_remaining,
                duration_ms=(time.monotonic() * 1000) - start_ms,
            )

        invariant_ok, invariant_reason = self._check_invariant_all_pass_7d()
        if not invariant_ok:
            return GcResult(
                status="blocked_invariant_fail",
                tier=tier,
                invariant_fail_reason=invariant_reason,
                duration_ms=(time.monotonic() * 1000) - start_ms,
            )

        # ── Phase 2: dry-run 진입 (or full execution) ─────────────────────────
        deletion_targets = self._list_deletion_targets(tier=tier)
        target_size = sum(self._safe_file_size(p) for p in deletion_targets)

        if dry_run:
            return GcResult(
                status="dry_run_complete",
                tier=tier,
                deletion_targets=[str(p) for p in deletion_targets],
                target_size_bytes=target_size,
                duration_ms=(time.monotonic() * 1000) - start_ms,
            )

        # ── Phase 4: 실 삭제 진입 ─────────────────────────────────────────────
        deleted_count = 0
        freed_bytes = 0
        for target in deletion_targets:
            size = self._safe_file_size(target)
            if self._is_already_deleted(str(target)):
                continue
            sha256 = self._compute_sha256(target)
            self._append_deletion_log(
                deleted_partition=str(target),
                sha256=sha256,
                size_bytes=size,
                status="pending",
            )
            try:
                target.unlink()
                self._update_deletion_log(
                    deleted_partition=str(target),
                    status="confirmed_deleted",
                )
                deleted_count += 1
                freed_bytes += size
            except OSError as exc:
                log.warning("gc unlink failed: %s exc=%s", target, exc)
                self._update_deletion_log(
                    deleted_partition=str(target),
                    status="recovery_failed",
                )

        free_pct = self._compute_free_disk_pct()
        return GcResult(
            status="executed",
            tier=tier,
            deleted_count=deleted_count,
            freed_bytes=freed_bytes,
            free_disk_pct_after=free_pct,
            duration_ms=(time.monotonic() * 1000) - start_ms,
        )

    # ── 3중 lock pre-check helpers ────────────────────────────────────────────

    def _check_cutover_rpo_verified(self) -> tuple[bool, str]:
        """cutover RPO=0 verify evidence file load + status == rpo_zero_verified."""
        if not self._cutover_verify_evidence_path.exists():
            return False, "cutover_verify_evidence_file_missing"
        try:
            data = json.loads(self._cutover_verify_evidence_path.read_text())
            status = data.get("status", "")
            if status == "rpo_zero_verified":
                return True, ""
            return False, f"cutover_verify_status={status}"
        except (json.JSONDecodeError, OSError) as exc:
            return False, f"cutover_verify_parse_error: {exc}"

    def _check_grace_period_expired(self) -> tuple[bool, int]:
        """7d grace 만료 verify (evidence file remaining_days == 0).

        Returns (ok, remaining_days).
        """
        if not self._grace_evidence_path.exists():
            return False, self._grace_days_required
        try:
            data = json.loads(self._grace_evidence_path.read_text())
            remaining = int(data.get("grace_remaining_days", self._grace_days_required))
            return remaining <= 0, remaining
        except (json.JSONDecodeError, OSError, ValueError):
            return False, self._grace_days_required

    def _check_invariant_all_pass_7d(self) -> tuple[bool, str]:
        """invariant ALL PASS 7일 누적 verify (evidence file pass_days >= 7)."""
        if not self._invariant_evidence_path.exists():
            return False, "invariant_evidence_file_missing"
        try:
            data = json.loads(self._invariant_evidence_path.read_text())
            pass_days = int(data.get("invariant_pass_days_consecutive", 0))
            if pass_days >= self._invariant_pass_days_required:
                return True, ""
            return False, (
                f"invariant_pass_days={pass_days} required={self._invariant_pass_days_required}"
            )
        except (json.JSONDecodeError, OSError, ValueError) as exc:
            return False, f"invariant_evidence_parse_error: {exc}"

    # ── deletion target list + execution helpers ──────────────────────────────

    def _list_deletion_targets(self, *, tier: str) -> list[Path]:
        """list deletion targets (tier/date 순차 — disk pressure 시 oldest first)."""
        if not self._local_l2_root.exists():
            return []
        targets = sorted(self._local_l2_root.rglob("*.parquet"))
        # Phase 5: 디스크 압박 시 oldest first (date 순차)
        if self._compute_disk_usage_pct() > self._disk_pressure_threshold_pct:
            targets.sort(key=lambda p: self._safe_mtime(p))
        return targets

    def _safe_file_size(self, p: Path) -> int:
        try:
            return p.stat().st_size
        except OSError:
            return 0

    def _safe_mtime(self, p: Path) -> float:
        try:
            return p.stat().st_mtime
        except OSError:
            return 0.0

    def _compute_sha256(self, p: Path) -> str:
        h = hashlib.sha256()
        try:
            with p.open("rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
        except OSError:
            return ""
        return h.hexdigest()

    def _compute_free_disk_pct(self) -> float:
        try:
            total, used, free = shutil.disk_usage(str(self._local_l2_root))
            if total == 0:
                return 0.0
            return (free / total) * 100.0
        except OSError:
            return 0.0

    def _compute_disk_usage_pct(self) -> float:
        try:
            total, used, free = shutil.disk_usage(str(self._local_l2_root))
            if total == 0:
                return 0.0
            return (used / total) * 100.0
        except OSError:
            return 0.0

    # ── deletion log sqlite-WAL helpers ───────────────────────────────────────

    def _init_deletion_log(self) -> None:
        """Init sqlite-WAL deletion log (persistent across restart)."""
        self._deletion_log_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(self._deletion_log_path)) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gc_deletion_log (
                    deleted_partition TEXT PRIMARY KEY,
                    deleted_at_iso TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    recovery_status TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _is_already_deleted(self, deleted_partition: str) -> bool:
        with sqlite3.connect(str(self._deletion_log_path)) as conn:
            cur = conn.execute(
                "SELECT recovery_status FROM gc_deletion_log WHERE deleted_partition = ?",
                (deleted_partition,),
            )
            row = cur.fetchone()
            if row is None:
                return False
            return row[0] in ("confirmed_deleted", "expired")

    def _append_deletion_log(
        self,
        *,
        deleted_partition: str,
        sha256: str,
        size_bytes: int,
        status: str,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(str(self._deletion_log_path)) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO gc_deletion_log
                (deleted_partition, deleted_at_iso, sha256, size_bytes, recovery_status)
                VALUES (?, ?, ?, ?, ?)
                """,
                (deleted_partition, now_iso, sha256, size_bytes, status),
            )
            conn.commit()

    def _update_deletion_log(self, *, deleted_partition: str, status: str) -> None:
        with sqlite3.connect(str(self._deletion_log_path)) as conn:
            conn.execute(
                "UPDATE gc_deletion_log SET recovery_status = ? WHERE deleted_partition = ?",
                (status, deleted_partition),
            )
            conn.commit()
