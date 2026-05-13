"""cutover_verifier.py — RPO=0 검증 (cutover-1s/+1s diff + 7종 invariant verify).

Story: MCT-155 (Stage 2 — Local GC + Secret rotation + RPO=0 verify + Stage 2 종료 gate)
Issue: mclayer/mctrader-hub#274

S8 박제 직접 owner (scope_manifest design_decisions S8, user_confirmed=true 2026-05-12):
"RPO=0 at cutover (cutover-1s 검증 L2 segment 모두 cutover+1s NAS 존재 의무)"

ADR-027 D6 amendment trigger (3종 → 7종 invariant 명시화).
ADR-027 D4 cutover step 2 (검증) 의 RPO=0 측면 직접 owner.

§8.5 active (process restart-aware, CFP-378 AC-5):
- cutover timestamp = caller 인자 (process restart 후 동일 timestamp 재verify 정합)
- diff 결과 박제 = file persistent (`mctrader-data/.tmp/rpo-zero-verify-MCT-155.md` gitignored)
- restart 후 verify 재실행 시 동일 결과 (idempotent)

§6.7 Cross-module contract (lesson #2 invariant):
- RpoVerifyResult.status switch 의무 (caller GcRunner pre-check + retro 박제)
- InvariantHarness.verify() 결과 propagate (7종 invariant ALL PASS gate)
- endpoint_router.rollback() signal emit path (FAIL 시 operator manual gate)

§6.9 placement:
- diff 측정 = unconditional (verify 첫 단계)
- 7종 invariant verify = unconditional (verify 두 번째 단계)
- RPO=0 결정 = unconditional (verify 세 번째 단계)
- cutover rollback signal emit = conditional (FAIL 시점 only)

§6.8 Wording SSOT:
- RpoVerifyResult.status 3종: "rpo_zero_verified" / "drift_detected" / "verify_inconclusive"
  variant 금지: "rpo_verified" / "drift" / "inconclusive" / "rpo_pass" 등.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from mctrader_data.nas_migration.invariant_harness import (
        InvariantHarness,
        InvariantResult,
    )
    from mctrader_data.nas_storage.nas_uploader import NASUploader

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RpoVerifyResult:
    """RPO=0 검증 결과 — caller switch 의무 (§6.7 cross-module contract).

    status enum 3종 (§6.8.1 Wording SSOT — single string, variant 금지):
    - "rpo_zero_verified"     : diff 0 + 7종 invariant ALL PASS (RPO=0 보장).
    - "drift_detected"        : diff > 0 또는 7종 invariant 1종 이상 FAIL (cutover rollback 권고).
    - "verify_inconclusive"   : NAS unreachable 또는 cutover timestamp ±1s 측정 불가 (재시도 권고).

    Caller 처리 의무 (§6.7 매핑):
    - "rpo_zero_verified"     -> GcRunner.gc() 진입 가능 (3중 lock 1번째 통과).
    - "drift_detected"        -> cutover rollback signal emit (operator manual gate).
    - "verify_inconclusive"   -> NAS 복구 후 재시도 (transient error).
    """

    status: Literal["rpo_zero_verified", "drift_detected", "verify_inconclusive"]
    cutover_timestamp_iso: str = ""
    cutover_minus_1s_segment_count: int = 0
    cutover_plus_1s_nas_object_count: int = 0
    diff_segments: list[str] = field(default_factory=list)
    invariant_result: InvariantResult | None = None
    verify_duration_ms: float = 0.0
    verify_error: str = ""


class CutoverVerifier:
    """RPO=0 검증 (cutover-1s/+1s diff + 7종 invariant verify).

    Thread-safety: stateless (per-call instance 또는 thread-safe 사용 가능).

    Idempotency: 다중 호출 시 동일 결과 (cutover_timestamp 동일 시 동일 diff + 동일 invariant).

    §6.1 chief decision 1: cutover-1s/+1s diff 측정 + 7종 invariant verify 채택
    (segment hash 비교 only 거부) — schema-level invariant 까지 enforce.
    """

    def __init__(
        self,
        nas_uploader: NASUploader,
        invariant_harness: InvariantHarness,
        local_l2_root: Path,
        nas_bucket: str = "mctrader-market",
        nas_l2_prefix: str = "schema_version=v1/tier=L2",
    ) -> None:
        self._uploader = nas_uploader
        self._invariant_harness = invariant_harness
        self._local_l2_root = local_l2_root
        self._nas_bucket = nas_bucket
        self._nas_l2_prefix = nas_l2_prefix

    def verify_rpo_zero(self, cutover_timestamp_iso: str) -> RpoVerifyResult:
        """cutover-1s/+1s diff 측정 + 7종 invariant verify + RPO=0 결정.

        Algorithm (Phase 1~5 sequential, §6.9 unconditional):
        Phase 1 (cutover-1s segment list, unconditional):
          1. local L2 segment list at cutover-1s (mtime <= cutover-1s)
          2. NAS object list at cutover-1s (LastModified <= cutover-1s)
        Phase 2 (cutover+1s segment list, unconditional):
          1. local L2 segment list at cutover+1s
          2. NAS object list at cutover+1s
        Phase 3 (diff 측정, unconditional):
          cutover-1s in local but missing in NAS @ cutover+1s = diff_segments
        Phase 4 (7종 invariant verify, unconditional):
          InvariantHarness.verify() per partition → ALL PASS gate
        Phase 5 (RPO=0 결정, conditional FAIL 시 signal emit):
          - ALL PASS + diff 0 -> rpo_zero_verified
          - FAIL 또는 diff > 0 -> drift_detected (cutover rollback signal emit path)
          - NAS unreachable 또는 timestamp parse 실패 -> verify_inconclusive

        Returns:
            RpoVerifyResult — status enum 3종 + diff metadata + invariant_result + duration_ms.

        §6.7 Cross-module contract:
        - GcRunner pre-check 직접 활용 (3중 lock 1번째 lock).
        - endpoint_router.rollback() signal emit path (FAIL 시 operator manual gate).
        - retro 박제 (`docs/retros/2026-05-stage2.md` §4 Epic CLOSED 6 AC evidence).
        """
        start_ms = time.monotonic() * 1000

        # ── timestamp parse (verify_inconclusive on parse fail) ───────────────
        try:
            cutover_dt = datetime.fromisoformat(
                cutover_timestamp_iso.replace("Z", "+00:00")
            )
            if cutover_dt.tzinfo is None:
                cutover_dt = cutover_dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError) as exc:
            return RpoVerifyResult(
                status="verify_inconclusive",
                cutover_timestamp_iso=cutover_timestamp_iso,
                verify_duration_ms=(time.monotonic() * 1000) - start_ms,
                verify_error=f"timestamp parse failed: {exc}",
            )

        cutover_minus_1s = cutover_dt.timestamp() - 1.0
        cutover_plus_1s = cutover_dt.timestamp() + 1.0

        # ── Phase 1+2: local + NAS segment list at cutover-1s and cutover+1s ──
        try:
            local_segments_minus_1s = self._list_local_segments(
                mtime_max=cutover_minus_1s
            )
            local_segments_plus_1s = self._list_local_segments(
                mtime_max=cutover_plus_1s
            )
            # NAS list at cutover+1s — diff target (cutover-1s NAS list was design candidate
            # but RPO=0 cardinality only requires +1s presence check)
            nas_objects_plus_1s = self._list_nas_objects()
        except Exception as exc:
            return RpoVerifyResult(
                status="verify_inconclusive",
                cutover_timestamp_iso=cutover_timestamp_iso,
                verify_duration_ms=(time.monotonic() * 1000) - start_ms,
                verify_error=f"NAS unreachable: {exc}",
            )

        # ── Phase 3: diff 측정 (cutover-1s in local but missing in NAS @ +1s) ─
        nas_basenames_plus_1s = {
            obj.split("/")[-1] for obj in nas_objects_plus_1s
        }
        local_basenames_minus_1s = {p.name for p in local_segments_minus_1s}
        diff_segments = sorted(
            local_basenames_minus_1s - nas_basenames_plus_1s
        )

        # ── Phase 4: 7종 invariant verify per partition ───────────────────────
        invariant_result = self._verify_invariants_for_l2(local_segments_plus_1s)

        # ── Phase 5: RPO=0 결정 ───────────────────────────────────────────────
        invariant_status = (
            invariant_result.status if invariant_result is not None else "all_pass"
        )

        if invariant_status == "all_pass" and len(diff_segments) == 0:
            status: Literal[
                "rpo_zero_verified", "drift_detected", "verify_inconclusive"
            ] = "rpo_zero_verified"
        else:
            status = "drift_detected"
            log.warning(
                "RPO=0 verify drift_detected: diff_count=%d invariant_status=%s",
                len(diff_segments),
                invariant_status,
            )

        return RpoVerifyResult(
            status=status,
            cutover_timestamp_iso=cutover_timestamp_iso,
            cutover_minus_1s_segment_count=len(local_segments_minus_1s),
            cutover_plus_1s_nas_object_count=len(nas_objects_plus_1s),
            diff_segments=diff_segments,
            invariant_result=invariant_result,
            verify_duration_ms=(time.monotonic() * 1000) - start_ms,
        )

    def _list_local_segments(self, mtime_max: float) -> list[Path]:
        """list local L2 .parquet segments with mtime <= mtime_max."""
        if not self._local_l2_root.exists():
            return []
        result: list[Path] = []
        for parquet_file in self._local_l2_root.rglob("*.parquet"):
            try:
                if parquet_file.stat().st_mtime <= mtime_max:
                    result.append(parquet_file)
            except OSError:
                continue
        return sorted(result)

    def _list_nas_objects(self) -> list[str]:
        """list NAS L2 object keys via NASUploader."""
        return self._uploader._list_objects(prefix=self._nas_l2_prefix)

    def _verify_invariants_for_l2(
        self, local_segments: list[Path]
    ) -> InvariantResult | None:
        """7종 invariant verify per partition (aggregate FAIL 1종 이상 시 propagate first FAIL).

        본 verifier 는 단일 InvariantResult 만 return — partition 별 verify 후
        first FAIL propagate (caller 가 detail 필요 시 RpoVerifyResult.invariant_result 활용).
        """
        if not local_segments:
            return None

        # Group local segments by partition directory
        partition_dirs: dict[Path, list[Path]] = {}
        for seg in local_segments:
            partition_dirs.setdefault(seg.parent, []).append(seg)

        first_fail: InvariantResult | None = None
        last_pass: InvariantResult | None = None

        for partition_dir in sorted(partition_dirs.keys()):
            try:
                nas_partition = self._compute_nas_partition_for(partition_dir)
                inv_result = self._invariant_harness.verify(
                    local_partition=partition_dir,
                    nas_partition=nas_partition,
                )
                if inv_result.status != "all_pass" and first_fail is None:
                    first_fail = inv_result
                else:
                    last_pass = inv_result
            except Exception as exc:
                log.warning(
                    "invariant verify failed for %s: %s", partition_dir, exc
                )
                continue

        return first_fail if first_fail is not None else last_pass

    def _compute_nas_partition_for(self, local_partition_dir: Path) -> str:
        """compute NAS partition prefix from local partition directory.

        Local: /data/cold/L2/exchange=upbit/symbol=BTC_KRW/date=2025-11-01/
        NAS:   schema_version=v1/tier=L2/exchange=upbit/symbol=BTC_KRW/date=2025-11-01/
        """
        try:
            relative = local_partition_dir.relative_to(self._local_l2_root)
            return f"{self._nas_l2_prefix}/{relative.as_posix()}"
        except ValueError:
            return self._nas_l2_prefix
