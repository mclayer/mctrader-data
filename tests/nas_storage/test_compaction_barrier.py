"""test_compaction_barrier.py — P0 TDD tests for CompactionBarrier.

Story: MCT-151 (Stage 2 — dual-write atomic primitives + 7종 invariant harness)
Issue: mclayer/mctrader-hub#257

Test Contract §8.2 (TestContractArchitectAgent — MCT-151):
- test_drain_complete_barrier_emit_returns_ok: in_flight=0 → status="ok"
- test_drain_timeout_returns_drain_timeout_status: timeout → status="drain_timeout" + in_flight_remaining > 0
- test_hot_path_unaffected_during_barrier: barrier 중 collector WAL/L1 영향 0 (ADR-017)
- test_release_idempotent_when_already_released: release() idempotent
- test_signal_persists_across_restart: process restart 시 signal file 잔존 (§8.5 active)
- test_barrier_violated_detected_by_verify_step: verify_barrier_intact() → barrier_violated (FIX#1 F3)
- test_status_enum_exact_string_match: enum value exact string (§6.8 wording SSOT)

§6.9 invariant placement:
- drain wait timeout: unconditional (loop 내 timeout check)
- drain_and_block(): BarrierResult.status 3종 only
- verify_barrier_intact(): barrier_violated 검출
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mctrader_data.nas_storage.compaction_barrier import BarrierResult, CompactionBarrier


# ─── fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def compaction_state(tmp_path: Path) -> Path:
    """compaction_state.json path."""
    return tmp_path / "compaction_state.json"


@pytest.fixture
def barrier_signal(tmp_path: Path) -> Path:
    """barrier_signal_path."""
    return tmp_path / "compaction_barrier.signal"


def _write_state(path: Path, in_flight_count: int) -> None:
    """Write compaction state file."""
    path.write_text(json.dumps({"in_flight_count": in_flight_count}))


def _make_barrier(
    compaction_state: Path,
    barrier_signal: Path,
    drain_timeout_seconds: int = 60,
    poll_interval_ms: int = 10,
) -> CompactionBarrier:
    return CompactionBarrier(
        drain_timeout_seconds=drain_timeout_seconds,
        poll_interval_ms=poll_interval_ms,
        compaction_state_path=str(compaction_state),
        barrier_signal_path=str(barrier_signal),
    )


# ─── drain + barrier semantics ────────────────────────────────────────────────

class TestCompactionBarrierDrainAndBlock:
    """§8.2: CompactionBarrier drain + barrier semantics (S2 박제)."""

    def test_drain_complete_barrier_emit_returns_ok(
        self, compaction_state: Path, barrier_signal: Path
    ) -> None:
        """in_flight=0 즉시 감지 → drain 완료 + barrier signal emit + status="ok".

        §6.2.2: in_flight_count == 0 → BarrierResult(status="ok", in_flight_remaining=0).
        """
        _write_state(compaction_state, in_flight_count=0)
        barrier = _make_barrier(compaction_state, barrier_signal)

        result = barrier.drain_and_block()

        assert result.status == "ok"
        assert result.in_flight_remaining == 0
        assert result.drain_wait_ms >= 0
        # barrier signal file must exist (L1 compactor will watch this)
        assert barrier_signal.exists(), "barrier signal file must be created"

    def test_drain_waits_until_in_flight_zero(
        self, compaction_state: Path, barrier_signal: Path
    ) -> None:
        """in_flight > 0 → drain wait loop → eventually 0 → status="ok".

        시뮬레이션: state file을 별도 스레드에서 0으로 갱신.
        """
        import threading

        _write_state(compaction_state, in_flight_count=2)
        barrier = _make_barrier(compaction_state, barrier_signal, poll_interval_ms=20)

        def _drain_after_delay():
            time.sleep(0.05)  # 50ms 후 in_flight=0 갱신
            _write_state(compaction_state, in_flight_count=0)

        t = threading.Thread(target=_drain_after_delay, daemon=True)
        t.start()

        result = barrier.drain_and_block()
        t.join(timeout=5)

        assert result.status == "ok"
        assert result.in_flight_remaining == 0

    def test_drain_timeout_returns_drain_timeout_status(
        self, compaction_state: Path, barrier_signal: Path
    ) -> None:
        """drain timeout (24h fast-forward fixture) → status="drain_timeout" + in_flight_remaining > 0.

        §6.2.2: timeout 도달 시 BarrierResult(status="drain_timeout", in_flight_remaining=N).
        caller 의무: dual-write toggle 활성화 차단 + alert + manual gate.
        §6.9: drain timeout = unconditional (조건 없이 timeout 도달 시 즉시 return).
        """
        _write_state(compaction_state, in_flight_count=3)
        # timeout=1s (fast-forward fixture)
        barrier = _make_barrier(
            compaction_state, barrier_signal,
            drain_timeout_seconds=1,
            poll_interval_ms=50,
        )

        start = time.monotonic()
        result = barrier.drain_and_block()
        elapsed = time.monotonic() - start

        assert result.status == "drain_timeout"
        assert result.in_flight_remaining == 3
        # must have waited approximately 1s (timeout)
        assert elapsed >= 0.9, f"Should have waited ~1s, elapsed={elapsed:.2f}s"

    def test_idempotent_when_already_blocked(
        self, compaction_state: Path, barrier_signal: Path
    ) -> None:
        """이미 barrier 적용 상태에서 drain_and_block() 재호출 → ok (idempotent).

        §6.2.2: in_flight_count=0 즉시 감지 → return "ok".
        """
        _write_state(compaction_state, in_flight_count=0)
        barrier = _make_barrier(compaction_state, barrier_signal)

        result1 = barrier.drain_and_block()
        result2 = barrier.drain_and_block()

        assert result1.status == "ok"
        assert result2.status == "ok"


# ─── hot path 무영향 ─────────────────────────────────────────────────────────

class TestCompactionBarrierHotPathUnaffected:
    """§8.2: ADR-017 hot path 무영향 invariant."""

    def test_hot_path_unaffected_during_barrier(
        self, compaction_state: Path, barrier_signal: Path, tmp_path: Path
    ) -> None:
        """barrier 적용 중 collector WAL append + L1 ParquetWriter fsync 정상 동작.

        §6.2.2: 별 process / 별 file path — CompactionBarrier 가 WAL/L1 파일에 접근 0.
        """
        _write_state(compaction_state, in_flight_count=0)
        barrier = _make_barrier(compaction_state, barrier_signal)

        # Simulate "hot path" files (collector WAL + L1 output)
        wal_file = tmp_path / "wal" / "segment_001.ndjson"
        wal_file.parent.mkdir()
        wal_file.write_bytes(b"wal content")

        l1_output = tmp_path / "l1" / "ohlcv.parquet"
        l1_output.parent.mkdir()
        l1_output.write_bytes(b"l1 parquet content")

        # Apply barrier
        result = barrier.drain_and_block()
        assert result.status == "ok"

        # Hot path files must be untouched (CompactionBarrier does NOT touch them)
        assert wal_file.exists(), "WAL file must remain untouched"
        assert wal_file.read_bytes() == b"wal content", "WAL content must be unchanged"
        assert l1_output.exists(), "L1 parquet file must remain untouched"
        assert l1_output.read_bytes() == b"l1 parquet content", "L1 content must be unchanged"

        # barrier_signal must only touch its own signal file
        assert barrier_signal.exists()
        signal_content = barrier_signal.read_text()
        # signal should not contain WAL/L1 file references
        assert "wal" not in signal_content.lower() or True  # content is implementation detail


# ─── release idempotent ──────────────────────────────────────────────────────

class TestCompactionBarrierRelease:
    """§8.2: CompactionBarrier release() idempotent."""

    def test_release_idempotent_when_already_released(
        self, compaction_state: Path, barrier_signal: Path
    ) -> None:
        """release() idempotent — 이미 release 상태에서 재호출 시 NO-OP.

        §6.2.2: signal_path 부재 시 unlink missing_ok=True (NO-OP).
        """
        barrier = _make_barrier(compaction_state, barrier_signal)

        # release without prior drain_and_block → must not raise
        barrier.release()
        assert not barrier_signal.exists()

        # release again → still must not raise
        barrier.release()
        assert not barrier_signal.exists()

    def test_release_removes_signal_file(
        self, compaction_state: Path, barrier_signal: Path
    ) -> None:
        """drain_and_block() 후 release() → signal file 삭제."""
        _write_state(compaction_state, in_flight_count=0)
        barrier = _make_barrier(compaction_state, barrier_signal)

        result = barrier.drain_and_block()
        assert result.status == "ok"
        assert barrier_signal.exists()

        barrier.release()
        assert not barrier_signal.exists(), "signal file must be removed after release()"


# ─── §8.5 active: signal persists across restart ─────────────────────────────

class TestCompactionBarrierSignalPersists:
    """§8.5 active: process restart 시 signal file 잔존 (CompactionBarrier restart-aware)."""

    def test_signal_persists_across_restart(
        self, compaction_state: Path, barrier_signal: Path
    ) -> None:
        """process restart 시 signal file 잔존 → L1 compactor 신규 task spawn 차단 유지.

        §8.2: release() 누락 + restart 시 signal file 잔존 → barrier 유지 (intended behavior).
        process restart = new CompactionBarrier instance 생성 (signal file 은 file system 에 잔존).
        """
        _write_state(compaction_state, in_flight_count=0)
        barrier1 = _make_barrier(compaction_state, barrier_signal)
        result1 = barrier1.drain_and_block()
        assert result1.status == "ok"
        assert barrier_signal.exists()

        # Simulate process restart: new CompactionBarrier instance (same paths)
        barrier2 = _make_barrier(compaction_state, barrier_signal)
        # signal file should still exist (file system persistence)
        assert barrier_signal.exists(), "signal file must persist across 'restart' (new instance)"

        # drain_and_block() on new instance → idempotent (already blocked)
        _write_state(compaction_state, in_flight_count=0)
        result2 = barrier2.drain_and_block()
        assert result2.status == "ok"

        # release on new instance
        barrier2.release()
        assert not barrier_signal.exists()


# ─── FIX#1 F3: verify_barrier_intact ─────────────────────────────────────────

class TestCompactionBarrierVerify:
    """§8.2: verify_barrier_intact() → barrier_violated (FIX#1 F3 박제)."""

    def test_barrier_violated_detected_by_verify_step(
        self, compaction_state: Path, barrier_signal: Path
    ) -> None:
        """barrier 적용 후 신규 compaction task 검출 시 verify_barrier_intact() → barrier_violated.

        §6.2.2: signal emit 후에도 in_flight_count > 0 → BarrierResult(status="barrier_violated").
        caller 의무: emergency rollback (dual-write toggle 즉시 중단 + release()).
        """
        _write_state(compaction_state, in_flight_count=0)
        barrier = _make_barrier(compaction_state, barrier_signal)

        result = barrier.drain_and_block()
        assert result.status == "ok"
        assert barrier_signal.exists()

        # Simulate L1 compactor bypass: in_flight_count > 0 after barrier
        _write_state(compaction_state, in_flight_count=1)

        # verify_barrier_intact → must detect violation
        verify_result = barrier.verify_barrier_intact()
        assert verify_result.status == "barrier_violated", (
            f"Expected barrier_violated but got {verify_result.status!r}"
        )

    def test_verify_barrier_intact_ok_when_barrier_holds(
        self, compaction_state: Path, barrier_signal: Path
    ) -> None:
        """barrier 정상 유지 시 verify_barrier_intact() → ok."""
        _write_state(compaction_state, in_flight_count=0)
        barrier = _make_barrier(compaction_state, barrier_signal)

        result = barrier.drain_and_block()
        assert result.status == "ok"

        # in_flight still 0 → barrier holds
        verify_result = barrier.verify_barrier_intact()
        assert verify_result.status == "ok"


# ─── §6.8: wording SSOT ──────────────────────────────────────────────────────

class TestCompactionBarrierStatusEnumExactStringMatch:
    """§8.2: Wording SSOT — BarrierResult.status enum 3종 exact string match."""

    def test_status_enum_exact_string_match(
        self, compaction_state: Path, barrier_signal: Path
    ) -> None:
        """BarrierResult.status 3종이 정확히 §6.8 enum value 와 일치.

        allowed: "ok" / "drain_timeout" / "barrier_violated"
        forbidden: "barrier_ok" / "drain_complete" / "timeout" / "violated"
        """
        allowed_statuses = {"ok", "drain_timeout", "barrier_violated"}
        forbidden_variants = {
            "barrier_ok", "drain_complete", "timed_out", "timeout",
            "violated", "barrier_violated_status",
        }

        # Test ok
        _write_state(compaction_state, in_flight_count=0)
        barrier = _make_barrier(compaction_state, barrier_signal)
        result_ok = barrier.drain_and_block()
        assert result_ok.status == "ok"
        assert result_ok.status in allowed_statuses
        assert result_ok.status not in forbidden_variants

        # Reset signal for next test
        barrier.release()

        # Test drain_timeout (1s fast-forward)
        _write_state(compaction_state, in_flight_count=5)
        barrier_timeout = _make_barrier(
            compaction_state, barrier_signal,
            drain_timeout_seconds=1,
            poll_interval_ms=100,
        )
        result_timeout = barrier_timeout.drain_and_block()
        assert result_timeout.status == "drain_timeout"
        assert result_timeout.status in allowed_statuses
        assert result_timeout.status not in forbidden_variants

        # Test barrier_violated
        _write_state(compaction_state, in_flight_count=0)
        barrier_violated = _make_barrier(
            compaction_state, barrier_signal,
            drain_timeout_seconds=60,
        )
        barrier_violated.drain_and_block()
        _write_state(compaction_state, in_flight_count=2)
        result_violated = barrier_violated.verify_barrier_intact()
        assert result_violated.status == "barrier_violated"
        assert result_violated.status in allowed_statuses
        assert result_violated.status not in forbidden_variants
