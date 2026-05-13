"""test_dual_writer.py — P0 TDD tests for DualWriter atomic primitive.

Story: MCT-151 (Stage 2 — dual-write atomic primitives + 7종 invariant harness)
Issue: mclayer/mctrader-hub#257

Test Contract §8.2 (TestContractArchitectAgent — MCT-151):
- test_phase2_commit_atomic_visible: phase 2 commit 시 양쪽 atomic visible
- test_phase2_rollback_on_hard_floor_blocked: hard_floor_blocked 시 local tmp rollback
- test_propagate_put_result_uploaded_to_committed: uploaded → committed propagation
- test_propagate_put_result_queued_to_local_only: queued → local_only propagation
- test_propagate_put_result_hard_floor_blocked_to_hard_floor_blocked: hard_floor_blocked propagation
- test_idempotent_skip_on_match_no_overwrite: skipped_idempotent → committed (forward-only)
- test_sha256_mismatch_raises_before_nas_put: sha256 mismatch → raise before NAS PUT
- test_status_enum_exact_string_match: enum value exact string (wording SSOT §6.8)
- test_chaos_nas_unreachable_l1_inflight_segment_drop_zero: chaos test (AC-4)

MCT-150 lesson 4 invariants 적용:
- §6.9 #1: sha256 verify unconditional (phase 1 진입 직후, NAS PUT 전)
- §6.9 #2: PutResult switch conditional (phase 2, NASUploader return 후)
- §6.8: wording SSOT — "committed" / "local_only" / "hard_floor_blocked" exact string
- §6.7: cross-module contract 표 그대로 propagation
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mctrader_data.nas_storage.dual_writer import DualWriter
from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult


# ─── fixtures ────────────────────────────────────────────────────────────────

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_put_result(status: str, etag: str = "abc123") -> PutResult:
    return PutResult(status=status, object_etag=etag, latency_ms=1.0)


def _make_uploader(put_status: str, etag: str = "abc123") -> NASUploader:
    """NASUploader mock: put() returns given status."""
    mock = MagicMock(spec=NASUploader)
    mock.put.return_value = _make_put_result(put_status, etag)
    return mock


@pytest.fixture
def local_root(tmp_path: Path) -> Path:
    root = tmp_path / "local_root"
    root.mkdir()
    return root


@pytest.fixture
def payload() -> bytes:
    return b"OHLCV parquet payload content"


@pytest.fixture
def payload_sha256(payload: bytes) -> str:
    return _sha256(payload)


# ─── AC-1: atomic write semantics ────────────────────────────────────────────

class TestDualWriterPhase2Commit:
    """§8.2: DualWriter atomic write semantics — phase 2 commit path."""

    def test_phase2_commit_atomic_visible(
        self, tmp_path: Path, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """phase 2 commit: uploaded → local rename atomic visible + DualWriteResult.status='committed'.

        §6.2.1: status ∈ {"uploaded", "skipped_idempotent", "skipped_etag_overwrite"}
        → tmp_path.rename(local_path) → DualWriteResult(status="committed").
        """
        uploader = _make_uploader("uploaded")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        local_path = local_root / "schema_version=v1" / "exchange=KRX" / "seg.parquet"
        local_path.parent.mkdir(parents=True)

        result = writer.write(
            local_path=local_path,
            nas_key="schema_version=v1/exchange=KRX/seg.parquet",
            data=payload,
            sha256=payload_sha256,
        )

        # local file must be visible at target path (atomic rename done)
        assert local_path.exists(), "local_path must exist after committed"
        assert result.status == "committed"
        assert result.nas_key == "schema_version=v1/exchange=KRX/seg.parquet"
        assert result.sha256 == payload_sha256

    def test_phase2_rollback_on_hard_floor_blocked(
        self, tmp_path: Path, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """phase 2 rollback: hard_floor_blocked → local tmp deleted + status='hard_floor_blocked'.

        §6.2.1: status == "hard_floor_blocked" → tmp_path.unlink() → DualWriteResult(status="hard_floor_blocked").
        양쪽 영속화 0 — caller source retain 의무 (RPO=0 보존).
        """
        uploader = _make_uploader("hard_floor_blocked")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        local_path = local_root / "seg.parquet"

        result = writer.write(
            local_path=local_path,
            nas_key="seg.parquet",
            data=payload,
            sha256=payload_sha256,
        )

        # local tmp must be cleaned up (rollback)
        assert not local_path.exists(), "local_path must NOT exist after hard_floor_blocked rollback"
        assert result.status == "hard_floor_blocked"

    def test_local_only_makes_file_visible(
        self, tmp_path: Path, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """queued → local rename (atomic visible) + status='local_only'.

        §6.2.1: status == "queued" (NAS unreachable + retry_queue enqueue)
        → tmp_path.rename(local_path) → DualWriteResult(status="local_only").
        caller source 삭제 가능 (local atomic visible + retry_queue 영속화 보장).
        """
        uploader = _make_uploader("queued")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        local_path = local_root / "seg_queued.parquet"

        result = writer.write(
            local_path=local_path,
            nas_key="seg_queued.parquet",
            data=payload,
            sha256=payload_sha256,
        )

        # local file visible even when NAS queued
        assert local_path.exists(), "local_path must exist after local_only (retry_queue persistent)"
        assert result.status == "local_only"


# ─── §6.7: PutResult propagation ─────────────────────────────────────────────

class TestDualWriterPutResultPropagation:
    """§8.2: DualWriter PutResult propagation — MCT-150 §6.7 caller contract 표 그대로."""

    def test_propagate_put_result_uploaded_to_committed(
        self, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """uploaded → committed propagation."""
        uploader = _make_uploader("uploaded")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)
        local_path = local_root / "seg.parquet"
        result = writer.write(local_path=local_path, nas_key="k", data=payload, sha256=payload_sha256)
        assert result.status == "committed"
        assert result.nas_put_result.status == "uploaded"

    def test_propagate_put_result_queued_to_local_only(
        self, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """queued → local_only propagation."""
        uploader = _make_uploader("queued")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)
        local_path = local_root / "seg2.parquet"
        result = writer.write(local_path=local_path, nas_key="k2", data=payload, sha256=payload_sha256)
        assert result.status == "local_only"
        assert result.nas_put_result.status == "queued"

    def test_propagate_put_result_hard_floor_blocked_to_hard_floor_blocked(
        self, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """hard_floor_blocked → hard_floor_blocked propagation (no status change)."""
        uploader = _make_uploader("hard_floor_blocked")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)
        local_path = local_root / "seg3.parquet"
        result = writer.write(local_path=local_path, nas_key="k3", data=payload, sha256=payload_sha256)
        assert result.status == "hard_floor_blocked"
        assert result.nas_put_result.status == "hard_floor_blocked"

    def test_propagate_put_result_skipped_idempotent_to_committed(
        self, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """skipped_idempotent → committed (forward-only invariant, NAS HEAD-then-PUT idempotency)."""
        uploader = _make_uploader("skipped_idempotent")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)
        local_path = local_root / "seg4.parquet"
        result = writer.write(local_path=local_path, nas_key="k4", data=payload, sha256=payload_sha256)
        assert result.status == "committed"

    def test_propagate_put_result_skipped_etag_overwrite_to_committed(
        self, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """skipped_etag_overwrite → committed propagation."""
        uploader = _make_uploader("skipped_etag_overwrite")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)
        local_path = local_root / "seg5.parquet"
        result = writer.write(local_path=local_path, nas_key="k5", data=payload, sha256=payload_sha256)
        assert result.status == "committed"

    def test_idempotent_skip_on_match_no_overwrite(
        self, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """forward-only invariant (ADR-009 §D12.2): skipped_idempotent → committed, no overwrite."""
        uploader = _make_uploader("skipped_idempotent")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)
        local_path = local_root / "idempotent.parquet"

        # first call
        result1 = writer.write(local_path=local_path, nas_key="idem_key", data=payload, sha256=payload_sha256)
        assert result1.status == "committed"

        # second call with same key (file already exists — NASUploader idempotent)
        uploader2 = _make_uploader("skipped_idempotent")
        writer2 = DualWriter(nas_uploader=uploader2, local_root=local_root)
        local_path2 = local_root / "idempotent2.parquet"
        result2 = writer2.write(local_path=local_path2, nas_key="idem_key", data=payload, sha256=payload_sha256)
        assert result2.status == "committed"
        # NAS put called once per writer
        uploader2.put.assert_called_once()


# ─── §6.9 #1: sha256 unconditional verify ────────────────────────────────────

class TestDualWriterSha256Verify:
    """§8.2: sha256 unconditional verify — phase 1 진입 직후, NAS PUT 전."""

    def test_sha256_mismatch_raises_before_nas_put(
        self, local_root: Path, payload: bytes
    ) -> None:
        """§6.9 #1: sha256 mismatch → raise immediately, NAS PUT 호출 0.

        caller 측 sha256 가 data 와 불일치 → phase 1 진입 직후 unconditional check.
        """
        uploader = _make_uploader("uploaded")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)
        local_path = local_root / "bad_sha.parquet"

        wrong_sha256 = "a" * 64  # 64-hex string, wrong hash

        with pytest.raises(ValueError, match="sha256"):
            writer.write(
                local_path=local_path,
                nas_key="bad_key",
                data=payload,
                sha256=wrong_sha256,
            )

        # NAS PUT must NOT have been called (unconditional, before NAS)
        uploader.put.assert_not_called()

    def test_sha256_correct_proceeds_to_nas_put(
        self, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """sha256 정합 시 NAS PUT 정상 진행."""
        uploader = _make_uploader("uploaded")
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)
        local_path = local_root / "ok_sha.parquet"

        result = writer.write(
            local_path=local_path,
            nas_key="ok_key",
            data=payload,
            sha256=payload_sha256,
        )

        assert result.status == "committed"
        uploader.put.assert_called_once()


# ─── §6.8: wording SSOT ──────────────────────────────────────────────────────

class TestDualWriterStatusEnumExactStringMatch:
    """§8.2: Wording SSOT — DualWriteResult.status enum 3종 exact string match.

    MCT-150 FIX#4 P1-NEW-3 lesson: variant 사용 금지
    (committed_atomic / local_partial / hard_floor_breached 등).
    """

    def test_status_enum_exact_string_match(
        self, local_root: Path, payload: bytes, payload_sha256: str
    ) -> None:
        """DualWriteResult.status 3종이 정확히 §6.8 enum value 와 일치."""
        allowed_statuses = {"committed", "local_only", "hard_floor_blocked"}
        forbidden_variants = {
            "committed_atomic", "committed_dual", "local_partial", "nas_only",
            "hard_floor_breached", "hard_floor_blocked_status", "blocked",
        }

        for put_status, expected_dwr_status in [
            ("uploaded", "committed"),
            ("skipped_idempotent", "committed"),
            ("skipped_etag_overwrite", "committed"),
            ("queued", "local_only"),
            ("hard_floor_blocked", "hard_floor_blocked"),
        ]:
            uploader = _make_uploader(put_status)
            writer = DualWriter(nas_uploader=uploader, local_root=local_root)
            local_path = local_root / f"enum_test_{put_status}.parquet"
            result = writer.write(
                local_path=local_path,
                nas_key=f"enum_{put_status}",
                data=payload,
                sha256=payload_sha256,
            )
            assert result.status == expected_dwr_status, (
                f"put_status={put_status!r} → expected DualWriteResult.status={expected_dwr_status!r}, "
                f"got {result.status!r}"
            )
            assert result.status in allowed_statuses, f"Unknown status: {result.status!r}"
            assert result.status not in forbidden_variants, f"Forbidden variant used: {result.status!r}"


# ─── AC-4: chaos test ────────────────────────────────────────────────────────

class TestChaosNasUnreachableL1InFlight:
    """§8.2 AC-4: NAS unreachable + L1 in-flight 동시 → segment drop 0.

    RPO=0 invariant 보존:
    - NAS unreachable → retry_queue persistent enqueue (status="local_only")
    - hard_floor 도달 → caller source retain (status="hard_floor_blocked")
    - segment drop 0 (어느 경우에도 데이터 유실 0)
    """

    @pytest.mark.integration
    def test_chaos_nas_unreachable_l1_inflight_segment_drop_zero(
        self, tmp_path: Path, local_root: Path
    ) -> None:
        """NAS unreachable + L1 in-flight 동시 → segment drop 0 (chaos fixture).

        RPO=0: NAS 단절 시 retry_queue enqueue → local_only (source 삭제 가능).
        hard_floor 시 hard_floor_blocked → source retain (drop 0).
        """
        # simulate NAS unreachable → queued (retry_queue enqueue successful)
        segments = [
            (f"segment_{i}.parquet", f"content_{i}".encode())
            for i in range(5)
        ]

        committed_count = 0
        local_only_count = 0
        hard_floor_count = 0

        # Mix of status to simulate chaos: 3 queued + 1 hard_floor + 1 uploaded
        statuses = ["queued", "queued", "hard_floor_blocked", "queued", "uploaded"]

        for (key, data), put_status in zip(segments, statuses, strict=False):
            sha = hashlib.sha256(data).hexdigest()
            uploader = _make_uploader(put_status)
            writer = DualWriter(nas_uploader=uploader, local_root=local_root)
            local_path = local_root / key

            result = writer.write(
                local_path=local_path,
                nas_key=key,
                data=data,
                sha256=sha,
            )

            if result.status == "committed":
                committed_count += 1
                # source can be deleted (both sides persisted)
            elif result.status == "local_only":
                local_only_count += 1
                # local persisted, retry_queue will drain — source can be deleted
                assert local_path.exists(), f"local_path must exist for local_only: {key}"
            elif result.status == "hard_floor_blocked":
                hard_floor_count += 1
                # caller source MUST be retained (RPO=0)
                assert not local_path.exists(), f"local_path must NOT exist for hard_floor_blocked: {key}"

        # Segment drop 0: all segments either committed, local_only (retry_queue), or hard_floor (source retained)
        total = committed_count + local_only_count + hard_floor_count
        assert total == len(segments), f"Segment drop detected! total={total}, expected={len(segments)}"
        # hard_floor_blocked requires caller to retain source → RPO=0 preserved
        assert hard_floor_count == 1  # exactly 1 hard_floor in our scenario
        assert local_only_count == 3  # exactly 3 queued
        assert committed_count == 1   # exactly 1 uploaded
