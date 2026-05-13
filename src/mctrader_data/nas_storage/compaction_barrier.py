"""compaction_barrier.py — L1 compaction drain + barrier (dual-write toggle gate, S2 박제).

Story: MCT-151 (Stage 2 — dual-write atomic primitives + 7종 invariant harness)
Issue: mclayer/mctrader-hub#257

Design decisions (§6.2.2 Change Plan 박제):

S2 박제 (scope_manifest design_decisions S2):
"drain + barrier 필수 (dual-write toggle gate) — toggle 이전 compaction output 이
local-only land 방지 — silent row loss 차단"

§6.9 invariant placement:
- drain wait timeout: unconditional — timeout 도달 시 즉시 drain_timeout return (조건 없이).
  MCT-150 SOPRunner.manual_gate_after_hours (24h) 패턴 정합.
- drain_and_block() return: "ok" or "drain_timeout" only (barrier_violated 는 verify step).

§6.8 Wording SSOT:
- BarrierResult.status 3종: "ok" / "drain_timeout" / "barrier_violated"
  variant 금지: "barrier_ok" / "drain_complete" / "timed_out" / "violated"

ADR-017 hot path 무영향 invariant:
- drain wait = L1 compactor 측 signal 수신만 (collector WAL append + L1 ParquetWriter fsync 영향 0)
- barrier = compactor pause signal file emit (cold tier path 만 영향, hot path 침범 0)
- 별 process / 별 file path — collector WAL / L1 ParquetWriter 와 shared state 0

signal-based polling mechanism (§6.2.2 결정):
- L1 compactor 가 in-flight task count 를 compaction_state_path (JSON) 에 expose
- 본 barrier 가 poll_interval_ms 간격으로 polling
- barrier_signal_path 파일 존재 시 L1 compactor 가 신규 task spawn 차단

§8.5 active: process restart-aware (barrier_signal_path 파일 잔존 — graceful shutdown 시 release() 의무)

Caller (MCT-152 dual_write_window_runner) 가 본 primitive inject 후 dual-write toggle 직전 호출.
본 Story scope = primitive 정의, 실 caller 통합은 MCT-152 scope.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

log = logging.getLogger(__name__)

# §6.8 Wording SSOT — BarrierResult.status enum 3종
_STATUS_OK: Literal["ok"] = "ok"
_STATUS_DRAIN_TIMEOUT: Literal["drain_timeout"] = "drain_timeout"
_STATUS_BARRIER_VIOLATED: Literal["barrier_violated"] = "barrier_violated"


@dataclass(frozen=True)
class BarrierResult:
    """L1 compaction drain barrier 의 result enum + caller contract.

    status enum 3종 (§6.8 Wording SSOT — single string 박제, variant 추가 금지):
    - "ok":               drain 완료 (in_flight=0) + barrier 적용 (신규 compaction 차단).
                          caller (dual_write_window_runner) 가 dual-write toggle 활성화 가능.
                          toggle 종료 후 release() 호출 의무.
    - "drain_timeout":    drain wait 가 drain_timeout_seconds 초과. in_flight > 0 잔존.
                          barrier 미적용 (신규 compaction 차단 0).
                          **caller 의무**: dual-write toggle 활성화 차단 + alert + manual gate + release().
                          §6.9 unconditional — timeout 도달 즉시 return (조건 0).
    - "barrier_violated": barrier signal emit 후 신규 compaction task 검출 (hook 미적용 시).
                          **caller 의무**: emergency rollback (release() + dual-write toggle 중단 + alert).
                          drain_and_block() 에서는 미반환 — verify_barrier_intact() 가 검출.

    drain_wait_ms: drain wait 실측 ms (monotonic, NFR-2 latency budget 검증 input).
    in_flight_remaining: timeout 시 잔존 in-flight task 수 (drain_timeout 시 > 0, ok 시 0).
    """

    status: Literal["ok", "drain_timeout", "barrier_violated"]
    drain_wait_ms: float
    in_flight_remaining: int


class CompactionBarrier:
    """L1 compaction drain + barrier — dual-write toggle gate (S2 박제).

    Responsibilities:
    1. drain_and_block(): barrier signal emit → drain wait loop (polling) → BarrierResult.
    2. release(): barrier signal unlink (idempotent, missing_ok=True).
    3. verify_barrier_intact(): signal 잔존 + in_flight=0 검증 → barrier_violated 검출.

    Signal-based polling mechanism:
    - compaction_state_path (JSON): {"in_flight_count": N} — L1 compactor 가 기록.
    - barrier_signal_path: file 존재 시 L1 compactor 신규 task spawn 차단 의무.
    - poll_interval_ms: polling 간격 (default 100ms).
    - drain_timeout_seconds: drain wait 최대 대기 시간 (default 86400s = 24h, EC-2 박제).

    ADR-017 hot path 무영향:
    - 본 barrier 가 collector WAL / L1 ParquetWriter 파일에 접근 0.
    - compaction_state_path + barrier_signal_path 만 접근.
    - 별 process / 별 file path invariant 보존.

    §8.5 active (process restart-aware):
    - drain_and_block() 이 생성하는 barrier_signal_path 파일은 file system 에 영속.
    - process restart 후에도 signal 잔존 → L1 compactor 신규 task 차단 유지.
    - caller 가 dual-write window 종료 시 release() 호출 의무 (누락 시 영구 차단).

    Idempotency:
    - drain_and_block(): 이미 barrier 적용 상태 (in_flight=0) 에서 재호출 시 즉시 "ok" return.
    - release(): signal 부재 시 missing_ok=True NO-OP.
    """

    def __init__(
        self,
        drain_timeout_seconds: int = 86400,
        poll_interval_ms: int = 100,
        compaction_state_path: str = "/data/compaction_state.json",
        barrier_signal_path: str = "/data/compaction_barrier.signal",
    ) -> None:
        self._drain_timeout_seconds = drain_timeout_seconds
        self._poll_interval_s = poll_interval_ms / 1000.0
        self._compaction_state_path = Path(compaction_state_path)
        self._barrier_signal_path = Path(barrier_signal_path)

    # ─── public API ──────────────────────────────────────────────────────────

    def drain_and_block(self) -> BarrierResult:
        """L1 compaction drain + barrier 적용 (single-shot, dual-write toggle 직전 호출).

        Algorithm:
        1. emit barrier signal: barrier_signal_path 생성 (touch, idempotent overwrite).
           L1 compactor 가 file watch 또는 주기적 poll 로 신규 task spawn 차단.
        2. drain wait loop (polling, §6.9 unconditional timeout):
           while monotonic elapsed < drain_timeout_seconds:
               read compaction_state_path → in_flight_count
               if in_flight_count == 0 → return BarrierResult(status="ok", in_flight_remaining=0)
               sleep(poll_interval_ms)
           # timeout 도달 — unconditional, 즉시 return (§6.9 drain timeout invariant)
           return BarrierResult(status="drain_timeout", in_flight_remaining=N)

        Returns:
            BarrierResult.status ∈ {"ok", "drain_timeout"}
            (barrier_violated 는 verify_barrier_intact() 에서만 반환 — §6.2.2)

        Idempotency: 이미 barrier 상태 (in_flight=0) 에서 재호출 시 즉시 "ok" return.
        barrier_signal_path write 는 idempotent (동일 content overwrite).

        Hot path 무영향: compaction_state_path (read only) + barrier_signal_path (write only)
        — collector WAL / L1 ParquetWriter 접근 0.
        """
        start_ms = time.monotonic() * 1000

        # Step 1: emit barrier signal (§6.2.2 Algorithm)
        self._emit_barrier_signal()

        # Step 2: drain wait loop (§6.9 unconditional timeout)
        deadline = time.monotonic() + self._drain_timeout_seconds

        while True:
            in_flight = self._read_in_flight_count()
            elapsed_ms = time.monotonic() * 1000 - start_ms

            if in_flight == 0:
                log.info(
                    "CompactionBarrier drain complete: in_flight=0, elapsed_ms=%.1f", elapsed_ms
                )
                return BarrierResult(
                    status=_STATUS_OK,
                    drain_wait_ms=elapsed_ms,
                    in_flight_remaining=0,
                )

            if time.monotonic() >= deadline:
                # §6.9 unconditional: timeout 도달 시 조건 없이 즉시 return
                log.error(
                    "CompactionBarrier drain timeout: in_flight=%d after %.1fs. "
                    "dual-write toggle activation BLOCKED (S2 박제). "
                    "caller MUST NOT activate toggle — alert + manual gate 의무.",
                    in_flight,
                    self._drain_timeout_seconds,
                )
                return BarrierResult(
                    status=_STATUS_DRAIN_TIMEOUT,
                    drain_wait_ms=elapsed_ms,
                    in_flight_remaining=in_flight,
                )

            time.sleep(self._poll_interval_s)

    def release(self) -> None:
        """Barrier 해제 — 신규 compaction 재개 가능.

        Algorithm: barrier_signal_path unlink (missing_ok=True — idempotent).
        L1 compactor 가 signal 부재 검출 후 신규 task spawn 재개.

        Idempotent: 이미 release 상태 (signal 부재) 에서 재호출 시 NO-OP (raise 0).

        Caller 의무: dual-write window 종료 시점 release() 호출 의무.
        누락 시 L1 compaction 영구 차단 → hot path 영향 (L1 → L2 promotion 중단).
        §8.5 active: process restart 후에도 signal 잔존 → caller 가 restart 후 release() 의무.
        """
        try:
            self._barrier_signal_path.unlink(missing_ok=True)
            log.info("CompactionBarrier released: signal_path=%s", self._barrier_signal_path)
        except OSError as e:
            log.warning("CompactionBarrier release failed (signal_path=%s): %s", self._barrier_signal_path, e)

    def verify_barrier_intact(self) -> BarrierResult:
        """Barrier 적용 후 신규 compaction task 검출 (FIX#1 F3 박제).

        drain_and_block() 호출 후 dual-write toggle 활성화 직전 (또는 이후) 에 caller 가
        호출하여 barrier 유효성 검증.

        Returns:
            BarrierResult(status="ok") — in_flight=0 + signal_path 존재.
            BarrierResult(status="barrier_violated") — in_flight > 0 (signal 우회 또는 race).

        Caller 의무 (barrier_violated 시): emergency rollback — release() + dual-write toggle 즉시 중단 + alert.
        """
        # Check signal still present
        signal_present = self._barrier_signal_path.exists()
        in_flight = self._read_in_flight_count()

        if not signal_present or in_flight > 0:
            log.error(
                "CompactionBarrier VIOLATED: signal_present=%s, in_flight=%d. "
                "Emergency rollback 의무 (release() + toggle 중단 + alert).",
                signal_present, in_flight,
            )
            return BarrierResult(
                status=_STATUS_BARRIER_VIOLATED,
                drain_wait_ms=0.0,
                in_flight_remaining=in_flight,
            )

        return BarrierResult(
            status=_STATUS_OK,
            drain_wait_ms=0.0,
            in_flight_remaining=0,
        )

    # ─── internal helpers ────────────────────────────────────────────────────

    def _emit_barrier_signal(self) -> None:
        """Write barrier signal file (idempotent, L1 compactor watches this)."""
        try:
            self._barrier_signal_path.parent.mkdir(parents=True, exist_ok=True)
            self._barrier_signal_path.write_text("barrier_active", encoding="utf-8")
            log.info("CompactionBarrier signal emitted: %s", self._barrier_signal_path)
        except OSError as e:
            log.error("CompactionBarrier signal emit failed: %s", e)
            raise

    def _read_in_flight_count(self) -> int:
        """Read in_flight_count from compaction_state_path (JSON).

        Returns 0 if file absent (no compactor running) or parse error.
        File format: {"in_flight_count": N}
        """
        try:
            content = self._compaction_state_path.read_text(encoding="utf-8")
            state = json.loads(content)
            return int(state.get("in_flight_count", 0))
        except FileNotFoundError:
            # No state file → compactor not running → in_flight=0
            return 0
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            log.warning(
                "CompactionBarrier: failed to parse compaction_state_path=%s: %s. Assuming in_flight=0.",
                self._compaction_state_path, e,
            )
            return 0
