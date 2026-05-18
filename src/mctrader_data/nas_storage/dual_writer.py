"""dual_writer.py — Dual-write atomic primitive (local + NAS 동시 PUT, 2-phase commit semantic).

Story: MCT-151 (Stage 2 — dual-write atomic primitives + 7종 invariant harness)
       MCT-168 (L1 NAS DualWriter wiring — put_l1() 신규 method, ADR-029 D1=B + D2=B)
Issue: mclayer/mctrader-hub#257

Design decisions (§6.2.1 Change Plan 박제, MCT-150 lesson 4 invariants 적용):

§6.9 invariant placement:
- Phase 1 (unconditional): sha256 verify — NAS PUT 호출 전 진입 직후 무조건 실행.
  mismatch 시 즉시 ValueError raise (NAS PUT 0 + local tmp 0).
- Phase 2 (conditional): PutResult.status switch — NASUploader.put() return 후 분기.

§6.8 Wording SSOT (single source):
- DualWriteResult.status 3종: "committed" / "local_only" / "hard_floor_blocked"
  variant 사용 금지: "committed_atomic" / "local_partial" / "hard_floor_breached" 등.

§6.7 Cross-module contract (MCT-150 §6.7 caller contract 그대로 propagation):
- NASUploader.put() status ∈ {"uploaded", "skipped_idempotent", "skipped_etag_overwrite"}
  → DualWriteResult.status = "committed" (caller source 삭제 가능)
- NASUploader.put() status == "queued"
  → DualWriteResult.status = "local_only" (caller source 삭제 가능 — retry_queue 후속 drain)
- NASUploader.put() status == "hard_floor_blocked"
  → DualWriteResult.status = "hard_floor_blocked" (caller source retain 의무, RPO=0)

MCT-168 put_l1() (ADR-029 D1=B + D2=B):
- put_l1(path): 평면 NAS key = build_nas_key(path, local_root, tier="L1") (U2-HELPER ADR-034 §결정 1)
  - sha256 streaming 계산 (8MB chunk, read_bytes 0 — INV-4 정합)
  - Prometheus: dual_write_result_total{tier="L1"} + dual_write_l1_latency_seconds
  - NAS PUT fail → local_only 반환 (compactor 정상 종료, INV-4 L1 local 보존)
  - INV-5: status enum 3종 (committed/local_only/hard_floor_blocked) 정확 1개 반환

ADR-017 hot path 무영향: collector WAL/L1 ParquetWriter 침범 0 (별 process / 별 file path).
Forward-only invariant (ADR-009 §D12.2): 양쪽 신규 row append-only.

Caller (MCT-152 dual_write_window_runner or compactor/runner.py) 가 본 primitive inject
후 source 삭제 결정 — 본 Story scope = primitive 정의, 실 caller 통합은 MCT-152 scope.

SecurityArch (§6.3):
- sha256 hex 는 Prometheus label 에 포함 금지 (cardinality 폭증)
- nas_key 는 Hive prefix 만 (credential 0)
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult

if TYPE_CHECKING:
    from mctrader_data.nas_metrics.prometheus_exporters import PrometheusExporter

log = logging.getLogger(__name__)

# §6.8 Wording SSOT — DualWriteResult.status enum 3종 (frozen, variant 추가 금지)
_COMMITTED_STATUSES: frozenset[str] = frozenset(
    ["uploaded", "skipped_idempotent", "skipped_etag_overwrite"]
)
_QUEUED_STATUS = "queued"
_HARD_FLOOR_STATUS = "hard_floor_blocked"


@dataclass(frozen=True)
class DualWriteResult:
    """Dual-write atomic primitive 의 result enum + caller contract.

    status enum 3종 (§6.8 Wording SSOT 박제 — single string 박제, variant 추가 금지):
    - "committed":          local + NAS 양쪽 atomic visible.
                            NASUploader.put() status ∈ {"uploaded", "skipped_idempotent",
                            "skipped_etag_overwrite"} 시 local rename + committed return.
                            caller source 삭제 가능 (양쪽 영속화 보장, RPO=0).
    - "local_only":         local atomic visible + NAS PUT → retry_queue persistent enqueue.
                            NASUploader.put() status == "queued" 시.
                            caller source 삭제 가능 (local atomic + retry_queue 영속화 보장).
    - "hard_floor_blocked": NASUploader.put() status == "hard_floor_blocked" 시.
                            retry_queue Stage 1 unconditional guard 차단.
                            양쪽 영속화 0 (NAS PUT 0 + local tmp rollback).
                            **caller source retain 의무** (RPO=0 보존, S8 user_confirmed).

    nas_put_result: NASUploader.put() 의 raw PutResult propagate (debugging + callerswitch 용).
    caller 는 status 만 switch 의무 (nas_put_result.status 직접 switch 금지 — wording desync risk).
    """

    status: Literal["committed", "local_only", "hard_floor_blocked"]
    nas_put_result: PutResult
    local_path: Path
    nas_key: str
    sha256: str
    latency_ms: float


class DualWriter:
    """Dual-write atomic primitive — local + NAS 동시 PUT, 2-phase commit semantic.

    Responsibilities:
    - Phase 1 (prepare, unconditional):
        1. sha256 verify (§6.9 #1 unconditional): caller-supplied sha256 vs data — mismatch 시 raise.
        2. local tmp write (write-then-rename pattern, rename 은 phase 2).
        3. NASUploader.put(nas_key, data, sha256=sha256, suppress_enqueue=False).

    - Phase 2 (commit/rollback, conditional on NASUploader result):
        NASUploader.put() PutResult.status switch (§6.9 #2 conditional):
        - "uploaded" / "skipped_idempotent" / "skipped_etag_overwrite"
            → tmp_path.rename(local_path) → DualWriteResult(status="committed").
        - "queued"
            → tmp_path.rename(local_path) → DualWriteResult(status="local_only").
        - "hard_floor_blocked"
            → tmp_path.unlink(missing_ok=True) → DualWriteResult(status="hard_floor_blocked").

    §6.7 Cross-module contract (MCT-150 §6.7 직접 propagation):
    - committed → MCT-189 D-2 A: DualWriter self-delete (caller 0건 재발 차단).
                  source(data as Path) promote_l1() 4중 verify 후 삭제.
    - local_only → caller source 삭제 가능.
    - hard_floor_blocked → caller source retain 의무 (RPO=0, S8 user_confirmed).

    ADR-017 hot path 무영향: 본 writer 가 L3 cold tier callsite 만 담당, collector WAL/L1
    ParquetWriter (ADR-017 hot path) 침범 0 (별 process / 별 file path).

    Forward-only (ADR-009 §D12.2): 양쪽 신규 row append-only, NASUploader HEAD-then-PUT
    idempotency 가 sha256 match 시 skip (skipped_idempotent → committed, 위반 0).
    """

    def __init__(
        self,
        nas_uploader: NASUploader,
        local_root: Path,
        metrics: PrometheusExporter | None = None,
    ) -> None:
        self._uploader = nas_uploader
        self._local_root = local_root
        self._metrics = metrics

    def write(
        self,
        *,
        local_path: Path,
        nas_key: str,
        data: bytes | Path,
        sha256: str,
        source_to_delete: Path | None = None,   # MCT-202 D-1: caller-side explicit cascade intent
    ) -> DualWriteResult:
        """2-phase commit semantic dual-write.

        Phase 1 (unconditional, §6.9 #1):
        - sha256 verify: hashlib.sha256(data) vs caller-supplied sha256.
          mismatch → raise ValueError (NAS PUT 0, local tmp 0).
        - local tmp write: local_path.with_suffix(".tmp_dw")
        - NASUploader.put(suppress_enqueue=False) → PutResult.

        Phase 2 (conditional, §6.9 #2):
        - PutResult.status switch → DualWriteResult.status propagation.

        Returns:
            DualWriteResult — caller must switch on .status before deleting source.

        Raises:
            ValueError: sha256 mismatch (§6.9 #1, unconditional, before NAS PUT).
        """
        start_ms = time.monotonic() * 1000

        # ── MCT-163 F3: streaming path (read_bytes 0, INV-4) ──────────────────
        # data: Path → streaming sha256 verify (chunk-wise, no read_bytes) + put_streaming
        # data: bytes → legacy path (caller already has bytes, put() backward compat)
        if isinstance(data, Path):
            # Streaming sha256 verify — chunk-wise (read_bytes() 호출 0, INV-4)
            _sha256_obj = hashlib.sha256()
            with data.open("rb") as _fv:
                for _chunk in iter(lambda: _fv.read(8 * 1024 * 1024), b""):
                    _sha256_obj.update(_chunk)
            actual_sha256 = _sha256_obj.hexdigest()
            if actual_sha256 != sha256:
                raise ValueError(
                    f"sha256 mismatch: caller supplied {sha256!r}, "
                    f"actual {actual_sha256!r}. NAS PUT aborted (§6.9 #1 unconditional)."
                )

            # ── Phase 1: local tmp write (atomic rename pattern, streaming copy) ──
            tmp_path = local_path.with_suffix(local_path.suffix + ".tmp_dw")
            try:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                # Streaming copy via shutil (no full-load, read_bytes 0)
                import shutil  # noqa: PLC0415
                shutil.copy2(str(data), str(tmp_path))
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise

            # ── Phase 1: NAS streaming upload (F3: put_streaming, D1=B) ───────
            try:
                nas_put_result = self._uploader.put_streaming(
                    data,       # Path → upload_fileobj (no read_bytes in NAS path)
                    nas_key,
                    sha256,
                )
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise

        else:
            # bytes path — backward compat (INV-2), unchanged
            payload: bytes = data

            # ── Phase 1 §6.9 #1: sha256 unconditional verify ─────────────────
            actual_sha256 = hashlib.sha256(payload).hexdigest()
            if actual_sha256 != sha256:
                raise ValueError(
                    f"sha256 mismatch: caller supplied {sha256!r}, "
                    f"actual {actual_sha256!r}. NAS PUT aborted (§6.9 #1 unconditional)."
                )

            # ── Phase 1: local tmp write ──────────────────────────────────────
            tmp_path = local_path.with_suffix(local_path.suffix + ".tmp_dw")
            try:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                tmp_path.write_bytes(payload)
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise

            # ── Phase 1: NAS PUT (bytes path, legacy) ────────────────────────
            try:
                nas_put_result = self._uploader.put(
                    nas_key,
                    payload,
                    sha256=sha256,
                    suppress_enqueue=False,
                )
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise

        # ── Phase 2 §6.9 #2: PutResult.status switch (conditional) ───────────
        latency_ms = time.monotonic() * 1000 - start_ms

        if nas_put_result.status in _COMMITTED_STATUSES:
            # Both sides committed → atomic visible (local rename)
            # os.replace() is used instead of Path.rename() for cross-platform
            # compatibility: on Windows, Path.rename() raises FileExistsError when
            # the target already exists (e.g. data==local_path idempotent re-write).
            # os.replace() is atomic and silently replaces on both Linux and Windows.
            os.replace(str(tmp_path), str(local_path))
            # MCT-189 D-2 A: DualWriter self-delete (caller 0건 재발 차단)
            # source(data as Path) 를 promote_l1() 4중 verify 후 삭제 (Path 입력 한정)
            dwr_status: Literal["committed", "local_only", "hard_floor_blocked"] = "committed"
            # MCT-202 D-1: caller-side explicit cascade intent (옵션 B)
            # source_to_delete 우선 처리 → MCT-189 backward compat (data != local_path) 보존
            if source_to_delete is not None:
                _promote_status = self._promote_after_nas_put(source_to_delete, nas_key, sha256)
                # P1-1: already_promoted → committed normalize (DualWriteResult.status 3-enum SSOT 보존)
                dwr_status = "committed" if _promote_status == "already_promoted" else _promote_status  # type: ignore[assignment]
            elif isinstance(data, Path) and data != local_path:
                # MCT-189 D-2 A: backward compat (source_to_delete=None 시 기존 분기 유지)
                _promote_status = self._promote_after_nas_put(data, nas_key, sha256)
                dwr_status = "committed" if _promote_status == "already_promoted" else _promote_status  # type: ignore[assignment]

        elif nas_put_result.status == _QUEUED_STATUS:
            # NAS queued (retry_queue persistent) → local visible, caller source safe to delete
            os.replace(str(tmp_path), str(local_path))
            dwr_status = "local_only"
            # MCT-202: source_to_delete 있으면 local_only_retained Counter emit
            if source_to_delete is not None:
                from mctrader_data.nas_metrics.prometheus_exporters import compactor_local_self_delete_total  # noqa: PLC0415
                compactor_local_self_delete_total.labels(
                    tier=self._tier_label_from_key(nas_key), outcome="local_only_retained"
                ).inc()

        elif nas_put_result.status == _HARD_FLOOR_STATUS:
            # hard_floor_blocked → rollback local tmp, caller MUST retain source
            tmp_path.unlink(missing_ok=True)
            dwr_status = "hard_floor_blocked"
            log.error(
                "DualWriter hard_floor_blocked: caller source retain 의무 (RPO=0 S8 user_confirmed). "
                "nas_key=%r sha256=%r",
                nas_key, sha256,
            )
            # MCT-202: source_to_delete 있으면 hard_floor_retained Counter emit
            if source_to_delete is not None:
                from mctrader_data.nas_metrics.prometheus_exporters import compactor_local_self_delete_total  # noqa: PLC0415
                compactor_local_self_delete_total.labels(
                    tier=self._tier_label_from_key(nas_key), outcome="hard_floor_retained"
                ).inc()

        else:
            # Unknown NASUploader status → rollback and raise (defensive)
            tmp_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"Unknown NASUploader.put() status: {nas_put_result.status!r}. "
                f"Expected one of: uploaded/skipped_idempotent/skipped_etag_overwrite/queued/hard_floor_blocked."
            )

        result = DualWriteResult(
            status=dwr_status,
            nas_put_result=nas_put_result,
            local_path=local_path,
            nas_key=nas_key,
            sha256=sha256,
            latency_ms=latency_ms,
        )

        # ── Metrics emit (optional) ────────────────────────────────────────────
        if self._metrics is not None:
            # Extract partition prefix from nas_key for label (cardinality control)
            # Use first 3 path segments of nas_key as prefix (e.g., schema_version=v1/exchange=KRX/symbol=005930)
            parts = nas_key.split("/")
            key_prefix = "/".join(parts[:3]) if len(parts) >= 3 else nas_key
            self._metrics.emit_invariant_dual_write(
                status=dwr_status,
                nas_key_prefix=key_prefix,
                latency_s=latency_ms / 1000.0,
            )

        return result

    def _tier_label_from_key(self, nas_key: str) -> str:
        """NAS key 에서 tier label 추출 (e.g. 'tier=L2' → 'L2').

        ADR-034 flat layout: market/<channel>/schema_version=*/tier=L{1,2,3}/...
        tier 컴포넌트 parse. 부재 시 'unknown' (defensive sentinel, cardinality 안전).
        """
        for part in nas_key.split("/"):
            if part.startswith("tier="):
                return part.split("=", 1)[1]
        return "unknown"

    def _promote_after_nas_put(
        self,
        source: Path,
        nas_key: str,
        sha256: str,
    ) -> Literal["committed", "local_only", "already_promoted"]:
        """NAS PUT commit 후 source self-delete (MCT-189 D-2 A + MCT-202 3-tier 확장).

        promote_l1() 4중 verify 후 local 삭제. 예외 분기:
        - PromotionVerifyError: retry_queue enqueue + "local_only" 반환 (orphan 방지)
        - FileNotFoundError: concurrent unlink graceful → "already_promoted" 반환 (MCT-202 §3.8 §11.6 Case 2)
        - OSError (unlink 실패): "committed_unlink_failed" Counter + source retain → "committed" 변환

        Counter 5 outcome emit (compactor_local_self_delete_total{tier,outcome}):
        - committed_unlinked: NAS commit + 4-HEAD pass + unlink success
        - committed_unlink_failed: NAS commit + 4-HEAD pass + unlink OSError
        - local_only_retained: NAS status='queued' (retry_queue enqueue)
        - hard_floor_retained: NAS status='hard_floor_blocked'
        - already_promoted: restart recovery / concurrent unlink (source 부재)

        MCT-202 Amendment (2026-05-18):
        - 반환 enum 확장: Literal["committed","local_only","already_promoted"]
        - already_promoted semantic = INV-D XOR 만족 (NAS object 존재 + local source 부재)
        - 2 callsite normalize: already_promoted → committed (P1-1, DualWriteResult.status 3-enum SSOT 보존)
        """
        from mctrader_data.compactor.promotion import (  # noqa: PLC0415
            promote_l1,
            PromotionVerifyError,
        )
        from mctrader_data.nas_metrics.prometheus_exporters import (  # noqa: PLC0415
            compactor_local_self_delete_total,
            mctrader_retry_orphan_total,
        )
        tier_label = self._tier_label_from_key(nas_key)
        try:
            promote_l1(
                local_path=source,
                nas_uploader=self._uploader,
                nas_key=nas_key,
                segment_id=nas_key,
            )
            # committed_unlinked: promote_l1 내부에서 unlink 완료
            compactor_local_self_delete_total.labels(tier=tier_label, outcome="committed_unlinked").inc()
            log.debug(
                "[dual_writer] promote_l1 committed_unlinked tier=%s source=%s",
                tier_label, source.name,
            )
            return "committed"
        except PromotionVerifyError as e:
            log.warning(
                "[dual_writer] promote_l1 verify failed — enqueue retry_queue, status=local_only: %s",
                e,
            )
            self._uploader.enqueue_retry(key=nas_key, data=source, sha256=sha256)
            # MCT-202 D-5: orphan visibility — sweep cycle 자연 회수까지 추적
            mctrader_retry_orphan_total.labels(tier=tier_label).inc()
            compactor_local_self_delete_total.labels(tier=tier_label, outcome="local_only_retained").inc()
            return "local_only"
        except FileNotFoundError:
            # MCT-202 §3.8 §11.6 Case 2: source 부재 (concurrent unlink / restart recovery)
            # already_promoted semantic = INV-D XOR 만족 (NAS object 존재 + local source 부재)
            log.debug(
                "[dual_writer] promote_l1 ENOENT — already_promoted (concurrent unlink or restart recovery), "
                "tier=%s source=%s",
                tier_label, source.name,
            )
            compactor_local_self_delete_total.labels(tier=tier_label, outcome="already_promoted").inc()
            return "already_promoted"
        except OSError as e:
            # MCT-202 P0-1 FIX: non-FileNotFoundError OSError (PermissionError / IOError 등)
            # promotion.py:200 unlink(missing_ok=False) raw-propagates OSError — caller catch 의무
            # source retain (unlink 실패 = source 보존, sweep fallback 회수 예정)
            # INV-D: NAS object 존재 + local source 잔존 = committed semantic 유지
            #   (NAS-SoT 격상: NAS 상태가 SoT, local 잔존은 sweep fallback 이 회수)
            # INV-G: log.error (P0 alarm trigger — operator 관측 의무)
            log.error(
                "[dual_writer] promote_l1 OSError(unlink failed) — source retain, "
                "committed_unlink_failed, sweep fallback 회수 예정. "
                "tier=%s source=%s err=%s",
                tier_label, source.name, e,
            )
            compactor_local_self_delete_total.labels(tier=tier_label, outcome="committed_unlink_failed").inc()
            # DualWriteResult.status 3-enum SSOT: committed_unlink_failed 는 Counter label 전용
            # NAS-SoT 격상 → committed (source 잔존은 sweep fallback 예정, 기능 상 commit 완료)
            return "committed"

    def put_l1(self, path: Path) -> DualWriteResult:
        """L1 NAS PUT — L1 ParquetWriter atomic rename 직후 호출 (ADR-029 D1=B, MCT-168).

        L1 PUT 은 이미 atomic rename 완료된 파일에 대한 NAS upload — write() 의
        local tmp copy 단계를 생략하고 NASUploader.put_streaming() 직접 호출.
        (write() 는 local_path 신규 생성 용도; L1 PUT 은 기존 parquet 재upload)

        nas_key = build_nas_key(path, local_root, tier="L1") (ADR-034 §결정 1, U2-HELPER).
        sha256 = streaming 계산 (8MB chunk, read_bytes 0 — INV-4: L1 local SSOT 보존).

        INV-4: NAS PUT fail → local_only 반환 (L1 local file 보존, compactor 정상 종료).
        INV-5: status enum 3종 (committed/local_only/hard_floor_blocked) 정확 1개 반환.

        Prometheus emit (AC-6 + AC-8):
        - mctrader_dual_write_result_total{tier="L1", status} — AC-6
        - mctrader_dual_write_l1_latency_seconds — AC-8 NFR p99 < 1500ms

        Args:
            path: L1 Parquet 파일 절대 경로 (local_root 하위 의무)

        Returns:
            DualWriteResult — status ∈ {"committed", "local_only", "hard_floor_blocked"}

        Raises:
            ValueError: path 가 local_root 하위가 아닌 경우
        """
        from mctrader_data.nas_metrics.prometheus_exporters import (
            dual_write_result_total,
            dual_write_l1_latency_seconds,
        )
        import time

        start_ms = time.monotonic() * 1000

        # tier prefix enforce (R-3 mitigation: l1/ 명시, l2/l3/ 와 명시적 분리)
        # boundary check + nas_key = single SSOT helper (ADR-034 §결정 2, U2-HELPER)
        try:
            path.relative_to(self._local_root)  # boundary pre-check (test compat: "not under local_root")
        except ValueError as err:
            raise ValueError(
                f"put_l1: path {path!r} is not under local_root {self._local_root!r}. "
                f"L1 NAS PUT requires path within local_root (ADR-029 D1=B)."
            ) from err
        from mctrader_data.nas_storage.nas_key import build_nas_key
        from mctrader_data.nas_metrics.prometheus_exporters import nas_key_helper_call_total

        nas_key = build_nas_key(path, self._local_root, tier="L1")
        nas_key_helper_call_total.labels(caller="dual_writer_put_l1", tier="L1").inc()

        # sha256 streaming 계산 (8MB chunk, read_bytes 0 — MCT-163 INV-4 정합)
        _sha256_obj = hashlib.sha256()
        with path.open("rb") as _fv:
            for _chunk in iter(lambda: _fv.read(8 * 1024 * 1024), b""):
                _sha256_obj.update(_chunk)
        sha256 = _sha256_obj.hexdigest()

        # NASUploader.put_streaming() 직접 호출 (D2=B retry_queue 재사용)
        # L1 PUT = 이미 atomic rename 완료된 파일 → local tmp copy 단계 불필요
        # suppress_enqueue=False → NAS unreachable 시 retry_queue 흡수 (INV-4 보장)
        nas_put_result = self._uploader.put_streaming(path, nas_key, sha256)

        latency_ms = time.monotonic() * 1000 - start_ms

        # PutResult.status → DualWriteResult.status 변환 (§6.7 Cross-module contract)
        if nas_put_result.status in _COMMITTED_STATUSES:
            # MCT-189 D-2 A: DualWriter self-delete (caller 0건 재발 차단)
            # put_l1() = path 가 source이자 NAS object → promote_l1() 4중 verify 후 삭제
            # MCT-202 P1-1: already_promoted → committed normalize (DualWriteResult.status 3-enum SSOT 보존)
            _l1_promote_status = self._promote_after_nas_put(path, nas_key, sha256)
            dwr_status: Literal["committed", "local_only", "hard_floor_blocked"] = (
                "committed" if _l1_promote_status == "already_promoted" else _l1_promote_status  # type: ignore[assignment]
            )
        elif nas_put_result.status == _QUEUED_STATUS:
            dwr_status = "local_only"
        elif nas_put_result.status == _HARD_FLOOR_STATUS:
            dwr_status = "hard_floor_blocked"
            log.error(
                "[dual_writer] L1 NAS PUT hard_floor_blocked: %r — SOP MANUAL_GATE escalation 의무 (ADR-029 D2=B)",
                nas_key,
            )
        else:
            # Unknown status — defensive: local_only 처리 (INV-4 보장 우선)
            log.error(
                "[dual_writer] L1 NAS PUT unknown status %r → local_only fallback (INV-4). nas_key=%r",
                nas_put_result.status, nas_key,
            )
            dwr_status = "local_only"

        result = DualWriteResult(
            status=dwr_status,
            nas_put_result=nas_put_result,
            local_path=path,
            nas_key=nas_key,
            sha256=sha256,
            latency_ms=latency_ms,
        )

        # Prometheus emit (AC-6 + AC-8)
        dual_write_result_total.labels(status=dwr_status, tier="L1").inc()
        dual_write_l1_latency_seconds.observe(latency_ms / 1000.0)

        log.info(
            "[dual_writer] L1 NAS PUT %s: status=%s latency_ms=%.1f (ADR-029 D1=B)",
            nas_key, dwr_status, latency_ms,
        )

        return result
