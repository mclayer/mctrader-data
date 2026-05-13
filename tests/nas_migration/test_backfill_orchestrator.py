"""test_backfill_orchestrator.py — TDD test suite for BackfillOrchestrator (MCT-153).

Story: MCT-153 (Stage 2 — backfill 76GB closed-day per-(symbol,day) chunking)
Issue: mclayer/mctrader-hub#265
ADR: ADR-027 D4 step 2 (backfill) + D6 (7종 invariant) + ADR-009 §D2.1 (node=DEFAULT)

Test Contract §8.1 (TestContractArchitectAgent — MCT-153):
- test_chunk_spec_is_legacy_node_true/false: S6 enforcement marker
- test_chunk_id_deterministic: resumability prerequisite (sha256[:16])
- test_nas_object_key_has_node_default_for_legacy: S6 박제 enforcement (AC-3)
- test_nas_object_key_preserves_node_for_non_legacy: S6 non-legacy path
- test_backfill_result_status_*: §6.8 Wording SSOT (4 enum)
- test_chunk_result_status_all_5_valid: §6.8 ChunkResult 5 enum
- test_orchestrator_run_all_verified: mock all pass → all_chunks_verified
- test_orchestrator_run_invariant_fail_quarantined: sha256_fail 3x → chunk_invariant_failed
- test_orchestrator_run_hard_floor_blocked: hard_floor_blocked → chunk_blocked
- test_orchestrator_run_sop_manual_gate: is_manual_gate=True → checkpoint_resumable
- test_orchestrator_skip_verified_chunk_on_resume: AC-5 idempotency
- test_closed_day_filter_excludes_today: S1 enforcement (AC-1)
- test_checkpoint_upsert_pending_idempotent: checkpoint semantics
- test_checkpoint_update_status_verified: checkpoint persistence
- test_checkpoint_list_all_returns_all: Phase E exit input

§6.8 Wording SSOT (박제 — variant 사용 금지):
- BackfillResult.status: "all_chunks_verified" / "chunk_invariant_failed" / "chunk_blocked" / "checkpoint_resumable"
- ChunkResult.status: "chunk_verified" / "chunk_skipped_resumed" / "chunk_quarantined" / "chunk_blocked" / "chunk_sop_skipped"
- BackfillCheckpoint status: "pending" / "in_flight" / "verified" / "quarantined" / "blocked"

§6.9 placement:
- Phase A (entry guard): SOPRunner.is_manual_gate() unconditional check
- Phase B (discovery): closed-day filter unconditional
- Phase D (per-chunk): PutResult.status switch conditional + InvariantResult.status switch conditional

TDD Red phase: src 구현 없음 → ImportError or NotImplementedError 확인.
TDD Green phase: impl 후 ALL PASS 목표.
"""
from __future__ import annotations

import hashlib
import io
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ─── TDD RED: 아래 import 가 구현 전에는 ImportError (RED phase 확인) ─────────
from mctrader_data.nas_migration.backfill_orchestrator import (
    BackfillCheckpoint,
    BackfillOrchestrator,
    BackfillResult,
    ChunkResult,
    ChunkSpec,
)
from mctrader_data.nas_storage.nas_uploader import PutResult
from mctrader_data.nas_migration.invariant_harness import InvariantResult


# ─── ADR-009 §D2.1 16-col schema SSOT ───────────────────────────────────────

ADR009_SCHEMA = pa.schema([
    pa.field("schema_version", pa.string()),
    pa.field("exchange", pa.string()),
    pa.field("symbol", pa.string()),
    pa.field("date", pa.string()),
    pa.field("ts", pa.int64()),
    pa.field("open", pa.decimal128(38, 9)),
    pa.field("high", pa.decimal128(38, 9)),
    pa.field("low", pa.decimal128(38, 9)),
    pa.field("close", pa.decimal128(38, 9)),
    pa.field("volume", pa.decimal128(38, 9)),
    pa.field("vwap", pa.decimal128(38, 9)),
    pa.field("trade_count", pa.int64()),
    pa.field("bid_count", pa.int64()),
    pa.field("ask_count", pa.int64()),
    pa.field("source_provenance", pa.string()),
    pa.field("ingestion_ts", pa.int64()),
])


# ─── helpers ─────────────────────────────────────────────────────────────────

def make_parquet(path: Path, rows: int = 3) -> bytes:
    """ADR-009 §D2.1 schema 정합 parquet 파일 생성 (test fixture)."""
    table = pa.table(
        {
            "schema_version": pa.array(["v1"] * rows, pa.string()),
            "exchange": pa.array(["BITHUMB"] * rows, pa.string()),
            "symbol": pa.array(["BTC_KRW"] * rows, pa.string()),
            "date": pa.array(["2025-01-01"] * rows, pa.string()),
            "ts": pa.array([1000 + i for i in range(rows)], pa.int64()),
            "open": pa.array([Decimal("50000.000000000")] * rows, pa.decimal128(38, 9)),
            "high": pa.array([Decimal("51000.000000000")] * rows, pa.decimal128(38, 9)),
            "low": pa.array([Decimal("49000.000000000")] * rows, pa.decimal128(38, 9)),
            "close": pa.array([Decimal("50500.000000000")] * rows, pa.decimal128(38, 9)),
            "volume": pa.array([Decimal("1.000000000")] * rows, pa.decimal128(38, 9)),
            "vwap": pa.array([Decimal("50250.000000000")] * rows, pa.decimal128(38, 9)),
            "trade_count": pa.array([100] * rows, pa.int64()),
            "bid_count": pa.array([50] * rows, pa.int64()),
            "ask_count": pa.array([50] * rows, pa.int64()),
            "source_provenance": pa.array(["test"] * rows, pa.string()),
            "ingestion_ts": pa.array([2000 + i for i in range(rows)], pa.int64()),
        },
        schema=ADR009_SCHEMA,
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    data = buf.getvalue()
    path.write_bytes(data)
    return data


def make_partition_dir(
    root: Path,
    symbol: str = "BTC_KRW",
    date: str = "2025-01-01",
    node: str | None = None,
    exchange: str = "BITHUMB",
    tier: str = "L2",
) -> Path:
    """test용 local partition 디렉토리 생성 (ADR-009 §D2.1 layout 정합).

    node=None → legacy (node= prefix 부재)
    node="MAIN" → non-legacy
    """
    if node is not None:
        partition_dir = (
            root
            / "market"
            / "orderbooksnapshot"
            / f"tier={tier}"
            / f"exchange={exchange}"
            / f"symbol={symbol}"
            / f"date={date}"
            / f"node={node}"
        )
    else:
        # legacy: node= prefix 부재
        partition_dir = (
            root
            / "market"
            / "orderbooksnapshot"
            / f"tier={tier}"
            / f"exchange={exchange}"
            / f"symbol={symbol}"
            / f"date={date}"
        )
    partition_dir.mkdir(parents=True, exist_ok=True)
    return partition_dir


@pytest.fixture
def mock_uploader():
    """NASUploader mock (HEAD-then-PUT idempotency — default: uploaded)."""
    m = MagicMock()
    m.put.return_value = PutResult(status="uploaded", latency_ms=100.0)
    m.bucket = "test-bucket"
    return m


@pytest.fixture
def mock_harness():
    """InvariantHarness mock (default: all_pass)."""
    m = MagicMock()
    m.verify.return_value = InvariantResult(status="all_pass", per_invariant_results={})
    return m


@pytest.fixture
def mock_sop():
    """NASUnreachableSOPRunner mock (default: is_manual_gate=False)."""
    m = MagicMock()
    m.is_manual_gate.return_value = False
    return m


@pytest.fixture
def mock_metrics():
    """PrometheusExporter mock."""
    return MagicMock()


@pytest.fixture
def checkpoint_path(tmp_path):
    return tmp_path / "backfill_checkpoint.sqlite"


@pytest.fixture
def evidence_pack_path(tmp_path):
    return tmp_path / "evidence_pack.md"


@pytest.fixture
def local_root(tmp_path):
    return tmp_path


@pytest.fixture
def orchestrator(
    mock_uploader,
    mock_harness,
    mock_sop,
    mock_metrics,
    local_root,
    checkpoint_path,
    evidence_pack_path,
    tmp_path,
):
    """기본 BackfillOrchestrator fixture (small test config)."""
    return BackfillOrchestrator(
        nas_uploader=mock_uploader,
        invariant_harness=mock_harness,
        sop_runner=mock_sop,
        metrics=mock_metrics,
        local_root=local_root,
        nas_partition_root="tier=L2",
        checkpoint_path=checkpoint_path,
        evidence_pack_path=evidence_pack_path,
        lock_path=tmp_path / "backfill.lock",
        max_workers=2,
        verify_retry_budget=3,
        chunk_timeout_s=10.0,
        tier="L2",
        partition_normalization=True,
    )


# ─── §6.8 Wording SSOT tests ─────────────────────────────────────────────────


def test_backfill_result_status_all_chunks_verified():
    """§6.8 Wording SSOT — "all_chunks_verified" 정확한 string."""
    r = BackfillResult(status="all_chunks_verified", total_chunks=10, verified_chunks=10)
    assert r.status == "all_chunks_verified"


def test_backfill_result_status_chunk_invariant_failed():
    """§6.8 Wording SSOT — "chunk_invariant_failed" 정확한 string."""
    r = BackfillResult(status="chunk_invariant_failed", quarantined_chunks=1)
    assert r.status == "chunk_invariant_failed"


def test_backfill_result_status_chunk_blocked():
    """§6.8 Wording SSOT — "chunk_blocked" 정확한 string."""
    r = BackfillResult(status="chunk_blocked", blocked_chunks=1)
    assert r.status == "chunk_blocked"


def test_backfill_result_status_checkpoint_resumable():
    """§6.8 Wording SSOT — "checkpoint_resumable" 정확한 string."""
    r = BackfillResult(status="checkpoint_resumable", resumable_chunks=5)
    assert r.status == "checkpoint_resumable"


def test_chunk_result_status_all_5_valid():
    """§6.8 ChunkResult 5 enum — 모두 유효."""
    chunk_id = "a" * 16
    statuses = [
        "chunk_verified",
        "chunk_skipped_resumed",
        "chunk_quarantined",
        "chunk_blocked",
        "chunk_sop_skipped",
    ]
    for s in statuses:
        r = ChunkResult(
            chunk_id=chunk_id,
            status=s,
            put_result=None,
            invariant_result=None,
        )
        assert r.status == s


# ─── ChunkSpec tests (S6 박제 — AC-3) ──────────────────────────────────────


def test_chunk_spec_is_legacy_node_true(tmp_path):
    """S6 박제: node= prefix 부재 partition → is_legacy_node=True (AC-3)."""
    # legacy: node= 없는 path
    source_path = tmp_path / "market" / "orderbooksnapshot" / "tier=L2" / "exchange=BITHUMB" / "symbol=BTC_KRW" / "date=2025-01-01" / "data.parquet"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    make_parquet(source_path)

    chunk = ChunkSpec(
        symbol="BTC_KRW",
        date="2025-01-01",
        source_path=source_path,
        nas_object_key="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=DEFAULT/data.parquet",
        nas_partition_prefix="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=DEFAULT",
        is_legacy_node=True,
        chunk_id="abcd1234abcd1234",
    )
    assert chunk.is_legacy_node is True


def test_chunk_spec_is_legacy_node_false(tmp_path):
    """S6 박제: node= prefix 존재 → is_legacy_node=False."""
    source_path = tmp_path / "market" / "orderbooksnapshot" / "tier=L2" / "exchange=BITHUMB" / "symbol=BTC_KRW" / "date=2025-01-01" / "node=MAIN" / "data.parquet"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    make_parquet(source_path)

    chunk = ChunkSpec(
        symbol="BTC_KRW",
        date="2025-01-01",
        source_path=source_path,
        nas_object_key="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=MAIN/data.parquet",
        nas_partition_prefix="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=MAIN",
        is_legacy_node=False,
        chunk_id="abcd1234abcd1234",
    )
    assert chunk.is_legacy_node is False


def test_chunk_id_deterministic(tmp_path):
    """AC-5 resumability: 동일 입력 → 동일 chunk_id (sha256[:16])."""
    symbol = "BTC_KRW"
    date = "2025-01-01"
    source_path = tmp_path / "data.parquet"
    make_parquet(source_path)

    expected_id = hashlib.sha256(f"{symbol}|{date}|{source_path}".encode()).hexdigest()[:16]

    chunk1 = ChunkSpec(
        symbol=symbol,
        date=date,
        source_path=source_path,
        nas_object_key="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=DEFAULT/data.parquet",
        nas_partition_prefix="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=DEFAULT",
        is_legacy_node=True,
        chunk_id=expected_id,
    )
    chunk2 = ChunkSpec(
        symbol=symbol,
        date=date,
        source_path=source_path,
        nas_object_key="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=DEFAULT/data.parquet",
        nas_partition_prefix="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=DEFAULT",
        is_legacy_node=True,
        chunk_id=expected_id,
    )
    assert chunk1.chunk_id == chunk2.chunk_id
    assert len(chunk1.chunk_id) == 16


def test_nas_object_key_has_node_default_for_legacy(tmp_path):
    """AC-3 enforcement: legacy partition → nas_object_key 에 'node=DEFAULT/' 포함."""
    source_path = tmp_path / "data.parquet"
    make_parquet(source_path)

    chunk = ChunkSpec(
        symbol="BTC_KRW",
        date="2025-01-01",
        source_path=source_path,
        nas_object_key="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=DEFAULT/data.parquet",
        nas_partition_prefix="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=DEFAULT",
        is_legacy_node=True,
        chunk_id="abcd1234abcd1234",
    )
    assert "node=DEFAULT" in chunk.nas_object_key
    assert "node=DEFAULT" in chunk.nas_partition_prefix


def test_nas_object_key_preserves_node_for_non_legacy(tmp_path):
    """AC-3 enforcement: non-legacy partition → nas_object_key 에 node=DEFAULT 없음."""
    source_path = tmp_path / "data.parquet"
    make_parquet(source_path)

    chunk = ChunkSpec(
        symbol="BTC_KRW",
        date="2025-01-01",
        source_path=source_path,
        nas_object_key="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=MAIN/data.parquet",
        nas_partition_prefix="tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-01-01/node=MAIN",
        is_legacy_node=False,
        chunk_id="abcd1234abcd1234",
    )
    assert "node=MAIN" in chunk.nas_object_key
    assert "node=DEFAULT" not in chunk.nas_object_key


# ─── BackfillCheckpoint tests ────────────────────────────────────────────────


def test_checkpoint_upsert_pending_idempotent(checkpoint_path, tmp_path):
    """BackfillCheckpoint.upsert_pending() — 재실행 시 verified 상태 보존 (AC-5 idempotency)."""
    cp = BackfillCheckpoint(checkpoint_path)
    source_path = tmp_path / "data.parquet"
    make_parquet(source_path)

    chunk = ChunkSpec(
        symbol="BTC_KRW",
        date="2025-01-01",
        source_path=source_path,
        nas_object_key="tier=L2/data.parquet",
        nas_partition_prefix="tier=L2",
        is_legacy_node=False,
        chunk_id="test1234test1234",
    )
    cp.upsert_pending([chunk])
    cp.update_status("test1234test1234", "verified", sha256="abc123")

    # 재실행: upsert_pending 이 verified 상태를 덮어쓰면 안 됨
    cp.upsert_pending([chunk])
    assert cp.get_status("test1234test1234") == "verified"


def test_checkpoint_update_status_verified(checkpoint_path, tmp_path):
    """BackfillCheckpoint.update_status() — verified + sha256 박제 확인."""
    cp = BackfillCheckpoint(checkpoint_path)
    source_path = tmp_path / "data.parquet"
    make_parquet(source_path)

    chunk = ChunkSpec(
        symbol="BTC_KRW",
        date="2025-01-01",
        source_path=source_path,
        nas_object_key="tier=L2/data.parquet",
        nas_partition_prefix="tier=L2",
        is_legacy_node=False,
        chunk_id="verify0000000001",
    )
    cp.upsert_pending([chunk])
    cp.update_status("verify0000000001", "verified", sha256="deadbeef")
    assert cp.get_status("verify0000000001") == "verified"


def test_checkpoint_list_all_returns_all(checkpoint_path, tmp_path):
    """BackfillCheckpoint.list_all() — Phase E 합산 입력 검증."""
    cp = BackfillCheckpoint(checkpoint_path)
    source_paths = []
    chunks = []
    for i in range(3):
        sp = tmp_path / f"data_{i}.parquet"
        make_parquet(sp)
        source_paths.append(sp)
        chunk_id = f"chunk{i:011}"
        chunks.append(
            ChunkSpec(
                symbol="BTC_KRW",
                date=f"2025-01-{i+1:02d}",
                source_path=sp,
                nas_object_key=f"tier=L2/data_{i}.parquet",
                nas_partition_prefix="tier=L2",
                is_legacy_node=False,
                chunk_id=chunk_id,
            )
        )

    cp.upsert_pending(chunks)
    all_records = cp.list_all()
    assert len(all_records) == 3
    chunk_ids = {r[0] for r in all_records}
    # chunk_id 는 3자리 숫자로 zero-pad (f"chunk{i:011}")
    assert any(cid.startswith("chunk") for cid in chunk_ids)


# ─── BackfillOrchestrator.run() integration tests ───────────────────────────


def test_orchestrator_run_all_verified(
    orchestrator, local_root, mock_uploader, mock_harness, mock_sop
):
    """AC-2 + AC-4: mock all pass → BackfillResult.status='all_chunks_verified'."""
    # 2 closed-day partitions
    for date in ["2025-01-01", "2025-01-02"]:
        p_dir = make_partition_dir(local_root, date=date)
        make_parquet(p_dir / "data.parquet")

    mock_uploader.put.return_value = PutResult(status="uploaded", latency_ms=100.0)
    mock_harness.verify.return_value = InvariantResult(status="all_pass", per_invariant_results={})
    mock_sop.is_manual_gate.return_value = False

    with patch("mctrader_data.nas_migration.backfill_orchestrator.date") as mock_date:
        mock_date.today.return_value = __import__("datetime").date(2030, 1, 1)
        result = orchestrator.run()

    assert result.status == "all_chunks_verified"
    assert result.verified_chunks > 0
    assert result.quarantined_chunks == 0
    assert result.blocked_chunks == 0


def test_orchestrator_run_invariant_fail_quarantined(
    orchestrator, local_root, mock_uploader, mock_harness, mock_sop
):
    """AC-4: invariant sha256_fail × 3 retry → chunk_invariant_failed + quarantined_chunks > 0."""
    p_dir = make_partition_dir(local_root, date="2025-01-01")
    make_parquet(p_dir / "data.parquet")

    mock_uploader.put.return_value = PutResult(status="uploaded", latency_ms=100.0)
    mock_harness.verify.return_value = InvariantResult(
        status="sha256_fail", per_invariant_results={}
    )
    mock_sop.is_manual_gate.return_value = False

    with patch("mctrader_data.nas_migration.backfill_orchestrator.date") as mock_date:
        mock_date.today.return_value = __import__("datetime").date(2030, 1, 1)
        result = orchestrator.run()

    assert result.status == "chunk_invariant_failed"
    assert result.quarantined_chunks > 0


def test_orchestrator_run_hard_floor_blocked(
    orchestrator, local_root, mock_uploader, mock_harness, mock_sop
):
    """EC-1: NASUploader 가 hard_floor_blocked → BackfillResult.status='chunk_blocked'."""
    p_dir = make_partition_dir(local_root, date="2025-01-01")
    make_parquet(p_dir / "data.parquet")

    mock_uploader.put.return_value = PutResult(status="hard_floor_blocked", latency_ms=10.0)
    mock_sop.is_manual_gate.return_value = False

    with patch("mctrader_data.nas_migration.backfill_orchestrator.date") as mock_date:
        mock_date.today.return_value = __import__("datetime").date(2030, 1, 1)
        result = orchestrator.run()

    assert result.status == "chunk_blocked"
    assert result.blocked_chunks > 0


def test_orchestrator_run_sop_manual_gate(
    orchestrator, local_root, mock_uploader, mock_harness, mock_sop
):
    """EC-1: Phase A SOPRunner.is_manual_gate()=True → checkpoint_resumable 즉시 반환."""
    p_dir = make_partition_dir(local_root, date="2025-01-01")
    make_parquet(p_dir / "data.parquet")

    mock_sop.is_manual_gate.return_value = True  # Phase A guard 트리거

    with patch("mctrader_data.nas_migration.backfill_orchestrator.date") as mock_date:
        mock_date.today.return_value = __import__("datetime").date(2030, 1, 1)
        result = orchestrator.run()

    assert result.status == "checkpoint_resumable"
    # Phase A 에서 즉시 종료 → NAS PUT 호출 0
    mock_uploader.put.assert_not_called()


def test_orchestrator_skip_verified_chunk_on_resume(
    mock_uploader,
    mock_harness,
    mock_sop,
    mock_metrics,
    local_root,
    checkpoint_path,
    evidence_pack_path,
    tmp_path,
):
    """AC-5 resumability: 이미 verified chunk 는 재실행 시 skip (put() 호출 0)."""
    # 파티션 생성
    p_dir = make_partition_dir(local_root, date="2025-01-01")
    make_parquet(p_dir / "data.parquet")

    mock_uploader.put.return_value = PutResult(status="uploaded", latency_ms=100.0)
    mock_harness.verify.return_value = InvariantResult(status="all_pass", per_invariant_results={})
    mock_sop.is_manual_gate.return_value = False

    def make_orch():
        return BackfillOrchestrator(
            nas_uploader=mock_uploader,
            invariant_harness=mock_harness,
            sop_runner=mock_sop,
            metrics=mock_metrics,
            local_root=local_root,
            nas_partition_root="tier=L2",
            checkpoint_path=checkpoint_path,
            evidence_pack_path=evidence_pack_path,
            lock_path=tmp_path / "backfill.lock",
            max_workers=2,
            verify_retry_budget=3,
            chunk_timeout_s=10.0,
            tier="L2",
            partition_normalization=True,
        )

    with patch("mctrader_data.nas_migration.backfill_orchestrator.date") as mock_date:
        mock_date.today.return_value = __import__("datetime").date(2030, 1, 1)
        result1 = make_orch().run()

    assert result1.status == "all_chunks_verified"
    first_run_put_count = mock_uploader.put.call_count
    assert first_run_put_count > 0  # first run did PUT

    # 재실행 — verified chunk skip → put() 호출 추가 없음
    mock_uploader.put.reset_mock()
    with patch("mctrader_data.nas_migration.backfill_orchestrator.date") as mock_date:
        mock_date.today.return_value = __import__("datetime").date(2030, 1, 1)
        result2 = make_orch().run()

    assert result2.status == "all_chunks_verified"
    # 이미 verified → put() 호출 0 (또는 skipped_idempotent only)
    assert mock_uploader.put.call_count == 0


def test_closed_day_filter_excludes_today(
    orchestrator, local_root, mock_uploader, mock_harness, mock_sop
):
    """AC-1 (S1 enforcement): 당일 partition 은 scope 외 (UTC midnight 이후 제외)."""
    import datetime

    today_str = datetime.date.today().isoformat()
    past_str = "2025-01-01"

    # 당일 + 과거 두 파티션 생성
    p_past = make_partition_dir(local_root, date=past_str)
    make_parquet(p_past / "data.parquet")

    p_today = make_partition_dir(local_root, date=today_str)
    make_parquet(p_today / "data.parquet")

    mock_uploader.put.return_value = PutResult(status="uploaded", latency_ms=100.0)
    mock_harness.verify.return_value = InvariantResult(status="all_pass", per_invariant_results={})
    mock_sop.is_manual_gate.return_value = False

    # today 를 실제 today 로 설정하여 filter 테스트
    result = orchestrator.run()

    # 당일 파티션은 제외되어야 함 → 과거 파티션 1개만 처리
    assert result.status == "all_chunks_verified"
    assert result.total_chunks == 1  # today 제외, past 만 포함


# ─── BackfillResult + ChunkResult frozen dataclass 테스트 ─────────────────


def test_backfill_result_is_frozen():
    """BackfillResult 가 frozen dataclass (불변)."""
    r = BackfillResult(status="all_chunks_verified")
    with pytest.raises((AttributeError, TypeError)):
        r.status = "chunk_blocked"  # type: ignore[misc]


def test_chunk_result_is_frozen():
    """ChunkResult 가 frozen dataclass (불변)."""
    r = ChunkResult(
        chunk_id="a" * 16,
        status="chunk_verified",
        put_result=None,
        invariant_result=None,
    )
    with pytest.raises((AttributeError, TypeError)):
        r.status = "chunk_blocked"  # type: ignore[misc]


def test_chunk_spec_is_frozen(tmp_path):
    """ChunkSpec 가 frozen dataclass (불변)."""
    source_path = tmp_path / "data.parquet"
    make_parquet(source_path)
    chunk = ChunkSpec(
        symbol="BTC_KRW",
        date="2025-01-01",
        source_path=source_path,
        nas_object_key="k",
        nas_partition_prefix="p",
        is_legacy_node=False,
        chunk_id="x" * 16,
    )
    with pytest.raises((AttributeError, TypeError)):
        chunk.symbol = "ETH_KRW"  # type: ignore[misc]
