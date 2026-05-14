"""dual_writer.py — Dual-write atomic primitive (local + NAS 동시 PUT, 2-phase commit semantic).

Story: MCT-151 (Stage 2 — dual-write atomic primitives + 7종 invariant harness)
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
    - committed/local_only → caller source 삭제 가능.
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
            tmp_path.rename(local_path)
            dwr_status: Literal["committed", "local_only", "hard_floor_blocked"] = "committed"

        elif nas_put_result.status == _QUEUED_STATUS:
            # NAS queued (retry_queue persistent) → local visible, caller source safe to delete
            tmp_path.rename(local_path)
            dwr_status = "local_only"

        elif nas_put_result.status == _HARD_FLOOR_STATUS:
            # hard_floor_blocked → rollback local tmp, caller MUST retain source
            tmp_path.unlink(missing_ok=True)
            dwr_status = "hard_floor_blocked"
            log.error(
                "DualWriter hard_floor_blocked: caller source retain 의무 (RPO=0 S8 user_confirmed). "
                "nas_key=%r sha256=%r",
                nas_key, sha256,
            )

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
