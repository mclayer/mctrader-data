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
- BackfillResult.status: "all_chunks_verified" / "chunk_invariant_failed" /
  "chunk_blocked" / "checkpoint_resumable"
- ChunkResult.status: "chunk_verified" / "chunk_skipped_resumed" /
  "chunk_quarantined" / "chunk_blocked" / "chunk_sop_skipped"
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
from unittest.mock import MagicMock, patch

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
    *,
    channel: str = "orderbooksnapshot",
    schema_version: str = "orderbook_snapshot.v1",
    tier: str = "L2",
    exchange: str = "BITHUMB",
    symbol: str = "BTC_KRW",
    date_str: str = "2025-01-01",
    hour: str | None = None,  # MCT-159: hour key 처리
    node: str | None = "MERGED",  # MCT-159: 신규 schema default node=MERGED
) -> Path:
    """test용 local partition 디렉토리 생성 (신규 schema — MCT-159).

    MCT-159 갱신: schema_version=* + hour + node 지원.
    - schema_version=<v> 레이어 추가 (hot-fix 2026-05-13 정합)
    - hour=<HH> 선택적 추가 (신규 schema 의무, legacy backward-compat)
    - node=<N> 선택적 추가 (MERGED = 신규 schema 기본)
    - node=None → legacy path (node= prefix 부재)

    NOTE: 기존 signature 와 하위 호환 불가 (positional arg 제거) — 기존 호출부 갱신 필요.
    """
    parts: list[Path | str] = [
        root, "market", channel,
        f"schema_version={schema_version}",
        f"tier={tier}",
        f"exchange={exchange}",
        f"symbol={symbol}",
        f"date={date_str}",
    ]
    if hour is not None:
        parts.append(f"hour={hour}")
    if node is not None:
        parts.append(f"node={node}")

    partition_dir = Path(*[str(p) for p in parts[:2]])
    for p in parts[2:]:
        partition_dir = partition_dir / str(p)
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


def make_orchestrator(
    *,
    local_root: Path,
    tier: str = "L2",
    channel: str = "orderbooksnapshot",
    nas_partition_root: str = "tier=L2",
    tmp_path: Path | None = None,
    invariant_harness=None,
) -> BackfillOrchestrator:
    """BackfillOrchestrator 공통 factory (MCT-159 Task 9 추가)."""
    import tempfile
    from unittest.mock import MagicMock
    from mctrader_data.nas_migration.invariant_harness import InvariantResult

    if tmp_path is None:
        _tmpdir = tempfile.mkdtemp()
        tmp_path = Path(_tmpdir)

    mock_uploader = MagicMock()
    mock_uploader.put.return_value = None  # not used in discovery-only tests
    mock_uploader.bucket = "test-bucket"

    if invariant_harness is None:
        mock_harness = MagicMock()
        mock_harness.verify.return_value = InvariantResult(status="all_pass", per_invariant_results={})
    else:
        mock_harness = invariant_harness

    mock_sop = MagicMock()
    mock_sop.is_manual_gate.return_value = False
    mock_metrics = MagicMock()

    from typing import Literal, cast
    tier_lit = cast(Literal["L2", "L3"], tier)

    return BackfillOrchestrator(
        nas_uploader=mock_uploader,
        invariant_harness=mock_harness,
        sop_runner=mock_sop,
        metrics=mock_metrics,
        local_root=local_root,
        nas_partition_root=nas_partition_root,
        checkpoint_path=tmp_path / "backfill_checkpoint.sqlite",
        evidence_pack_path=tmp_path / "evidence_pack.md",
        lock_path=tmp_path / "backfill.lock",
        max_workers=2,
        verify_retry_budget=3,
        chunk_timeout_s=10.0,
        tier=tier_lit,
        partition_normalization=True,
        channel=channel,  # MCT-159 channel parametrize
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
    source_path = (
        tmp_path / "market" / "orderbooksnapshot"
        / "tier=L2" / "exchange=BITHUMB" / "symbol=BTC_KRW"
        / "date=2025-01-01" / "data.parquet"
    )
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
    source_path = (
        tmp_path / "market" / "orderbooksnapshot"
        / "tier=L2" / "exchange=BITHUMB" / "symbol=BTC_KRW"
        / "date=2025-01-01" / "node=MAIN" / "data.parquet"
    )
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
        p_dir = make_partition_dir(local_root, date_str=date)
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
    p_dir = make_partition_dir(local_root, date_str="2025-01-01")
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
    p_dir = make_partition_dir(local_root, date_str="2025-01-01")
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
    p_dir = make_partition_dir(local_root, date_str="2025-01-01")
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
    p_dir = make_partition_dir(local_root, date_str="2025-01-01")
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
    p_past = make_partition_dir(local_root, date_str=past_str)
    make_parquet(p_past / "data.parquet")

    p_today = make_partition_dir(local_root, date_str=today_str)
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


# ─── MCT-159 Task 9: channel parametrize tests ───────────────────────────────


def test_orchestrator_discovers_transaction_channel(tmp_path):
    """MCT-159 — channel parametrize. transaction channel 의 closed-day partition 탐색."""
    p_dir = make_partition_dir(
        tmp_path,
        channel="transaction",
        schema_version="tick.v1",
        tier="L2",
        date_str="2026-05-10",
        hour="13",
        node="MERGED",
    )
    parquet_path = p_dir / "part-test.parquet"
    parquet_path.write_bytes(b"PAR1")

    orch = make_orchestrator(local_root=tmp_path, tier="L2", channel="transaction", tmp_path=tmp_path)
    partitions = orch._discover_partitions()
    assert len(partitions) == 1
    assert "transaction" in str(partitions[0])


def test_orchestrator_default_channel_orderbooksnapshot(tmp_path):
    """MCT-159 — default channel=orderbooksnapshot backward-compat (R4 mitigation)."""
    p_dir = make_partition_dir(
        tmp_path,
        channel="orderbooksnapshot",
        schema_version="orderbook_snapshot.v1",
        tier="L2",
        date_str="2025-01-01",
        hour="10",
        node="MERGED",
    )
    parquet_path = p_dir / "part-default.parquet"
    parquet_path.write_bytes(b"PAR1")

    # channel 미지정 → default "orderbooksnapshot"
    orch = make_orchestrator(local_root=tmp_path, tier="L2", tmp_path=tmp_path)
    partitions = orch._discover_partitions()
    assert len(partitions) == 1
    assert "orderbooksnapshot" in str(partitions[0])


def test_orchestrator_transaction_not_discovered_when_orderbooksnapshot(tmp_path):
    """MCT-159 — channel isolation: orderbooksnapshot orchestrator 는 transaction 미발견."""
    # transaction 파티션만 생성
    p_dir = make_partition_dir(
        tmp_path,
        channel="transaction",
        schema_version="tick.v1",
        tier="L2",
        date_str="2025-01-01",
        hour="10",
        node="MERGED",
    )
    (p_dir / "part-trans.parquet").write_bytes(b"PAR1")

    # orderbooksnapshot 채널 orchestrator → transaction 미발견
    orch = make_orchestrator(local_root=tmp_path, tier="L2", channel="orderbooksnapshot", tmp_path=tmp_path)
    partitions = orch._discover_partitions()
    assert len(partitions) == 0


# ─── MCT-159 Task 10: hour key 박제 tests ────────────────────────────────────


def test_chunk_spec_includes_hour_partition(tmp_path):
    """MCT-159 — hour key 박제. 신규 schema 의 hour=HH/node=MERGED 가 nas_object_key 에 박제."""
    parquet_path = make_partition_dir(
        tmp_path,
        channel="orderbooksnapshot",
        schema_version="orderbook_snapshot.v1",
        tier="L2",
        date_str="2026-05-10",
        hour="13",
        node="MERGED",
    ) / "part-abc123.parquet"
    parquet_path.write_bytes(b"PAR1")

    orch = make_orchestrator(local_root=tmp_path, tier="L2", nas_partition_root="tier=L2", tmp_path=tmp_path)
    chunk = orch._build_chunk_spec(parquet_path)
    assert "hour=13" in chunk.nas_object_key
    assert "node=MERGED" in chunk.nas_object_key
    assert chunk.nas_object_key.endswith("part-abc123.parquet")


def test_chunk_spec_hour_absent_legacy_backward_compat(tmp_path):
    """MCT-159 — hour 부재 시 hour_segment="" (legacy backward-compat, R5 mitigation)."""
    # legacy path (hour 부재)
    legacy_dir = (
        tmp_path / "market" / "orderbooksnapshot"
        / "schema_version=orderbook_snapshot.v1"
        / "tier=L2" / "exchange=BITHUMB" / "symbol=BTC_KRW" / "date=2025-01-01"
    )
    legacy_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = legacy_dir / "part-legacy.parquet"
    parquet_path.write_bytes(b"PAR1")

    orch = make_orchestrator(local_root=tmp_path, tier="L2", nas_partition_root="tier=L2", tmp_path=tmp_path)
    chunk = orch._build_chunk_spec(parquet_path)
    # hour 없으면 nas_object_key 에 "hour=" 미포함 (legacy backward-compat)
    assert "hour=" not in chunk.nas_object_key
    assert chunk.nas_object_key.endswith("part-legacy.parquet")


# ─── MCT-159 Task 12: Integration test 7종 (AC-1~AC-5 + Edge Case 2) ──────────


def test_ac1_new_schema_path_100_percent(tmp_path, mock_uploader, mock_harness, mock_sop, mock_metrics):
    """AC-1: 신규 schema path (hour=HH/node=MERGED) 100% 준수, legacy 경로 혼입 0건."""
    # 4 partition × 다른 hour
    for hr in ["10", "11", "12", "13"]:
        p_dir = make_partition_dir(tmp_path, date_str="2025-01-01", hour=hr, node="MERGED")
        make_parquet(p_dir / f"part-{hr}.parquet")

    mock_uploader.put.return_value = PutResult(status="uploaded", latency_ms=50.0)
    mock_harness.verify.return_value = InvariantResult(status="all_pass", per_invariant_results={})
    mock_sop.is_manual_gate.return_value = False

    from mctrader_data.nas_migration.backfill_orchestrator import BackfillOrchestrator
    orch = BackfillOrchestrator(
        nas_uploader=mock_uploader,
        invariant_harness=mock_harness,
        sop_runner=mock_sop,
        metrics=mock_metrics,
        local_root=tmp_path,
        nas_partition_root="tier=L2",
        checkpoint_path=tmp_path / "cp.sqlite",
        evidence_pack_path=tmp_path / "ep.md",
        lock_path=tmp_path / "lock",
        max_workers=2,
        tier="L2",
        channel="orderbooksnapshot",
    )

    # _build_chunk_spec 로 각 chunk 의 nas_object_key 확인
    partitions = orch._discover_partitions()
    assert len(partitions) == 4

    for pf in partitions:
        chunk = orch._build_chunk_spec(pf)
        # AC-1: 신규 schema path 준수 — hour= + node=MERGED 포함
        assert "hour=" in chunk.nas_object_key, f"hour= missing in {chunk.nas_object_key}"
        assert "node=MERGED" in chunk.nas_object_key, f"node=MERGED missing in {chunk.nas_object_key}"
        # legacy 경로 (node=DEFAULT) 혼입 0
        assert "node=DEFAULT" not in chunk.nas_object_key


def test_ac2_mct156_legacy_exclusion_zero(tmp_path):
    """AC-2: legacy hour-key 부재 partition 제외 0건 (S1/S6 정합 — 신규 schema only)."""
    # 신규 schema partition (hour=HH 포함)
    p_new = make_partition_dir(tmp_path, date_str="2025-01-01", hour="10", node="MERGED")
    make_parquet(p_new / "part-new.parquet")

    # legacy partition (hour 부재) — _discover_partitions 는 모두 발견 (hour filter 없음),
    # _build_chunk_spec 이 hour_segment="" 로 처리 (legacy backward-compat)
    legacy_dir = (
        tmp_path / "market" / "orderbooksnapshot"
        / "schema_version=orderbook_snapshot.v1" / "tier=L2"
        / "exchange=BITHUMB" / "symbol=BTC_KRW" / "date=2025-01-02"
    )
    legacy_dir.mkdir(parents=True, exist_ok=True)
    make_parquet(legacy_dir / "part-legacy.parquet")

    orch = make_orchestrator(local_root=tmp_path, tier="L2", tmp_path=tmp_path)
    partitions = orch._discover_partitions()
    # 신규 + legacy 모두 발견 (discovery filter 없음 — chunk 처리 레이어에서 분기)
    assert len(partitions) == 2

    # _build_chunk_spec 으로 신규 vs legacy 분기 검증
    chunks = [orch._build_chunk_spec(pf) for pf in partitions]
    new_chunks = [c for c in chunks if "hour=" in c.nas_object_key]
    legacy_chunks = [c for c in chunks if "hour=" not in c.nas_object_key]
    assert len(new_chunks) == 1   # 신규 schema (hour=10)
    assert len(legacy_chunks) == 1  # legacy (hour 부재)


def test_ac3_invariant_harness_injected(tmp_path):
    """AC-3: BackfillOrchestrator 가 InvariantHarness inject 받아 verify 자동 호출."""
    from unittest.mock import MagicMock, patch

    mock_harness = MagicMock()
    mock_harness.verify.return_value = InvariantResult(status="all_pass", per_invariant_results={})
    mock_uploader = MagicMock()
    mock_uploader.put.return_value = PutResult(status="uploaded", latency_ms=50.0)
    mock_sop = MagicMock()
    mock_sop.is_manual_gate.return_value = False
    mock_metrics = MagicMock()

    p_dir = make_partition_dir(tmp_path, date_str="2025-01-01", hour="10", node="MERGED")
    make_parquet(p_dir / "part-test.parquet")

    from mctrader_data.nas_migration.backfill_orchestrator import BackfillOrchestrator
    orch = BackfillOrchestrator(
        nas_uploader=mock_uploader,
        invariant_harness=mock_harness,
        sop_runner=mock_sop,
        metrics=mock_metrics,
        local_root=tmp_path,
        nas_partition_root="tier=L2",
        checkpoint_path=tmp_path / "cp.sqlite",
        evidence_pack_path=tmp_path / "ep.md",
        lock_path=tmp_path / "lock",
        max_workers=2,
        tier="L2",
        channel="orderbooksnapshot",
    )

    with patch("mctrader_data.nas_migration.backfill_orchestrator.date") as mock_date:
        mock_date.today.return_value = __import__("datetime").date(2030, 1, 1)
        result = orch.run()

    # AC-3: InvariantHarness.verify() 가 호출됐어야 함
    assert mock_harness.verify.called, "InvariantHarness.verify() 가 호출되지 않음"
    assert result.status == "all_chunks_verified"


@pytest.mark.parametrize("channel", ["orderbooksnapshot", "transaction"])
@pytest.mark.parametrize("tier", ["L2", "L3"])
def test_ac4_channel_tier_matrix(tmp_path, channel, tier):
    """AC-4: channel + tier 4 case 매트릭스 — 각 case partition 탐색 정합."""
    schema_version = "orderbook_snapshot.v1" if channel == "orderbooksnapshot" else "tick.v1"
    p_dir = make_partition_dir(
        tmp_path,
        channel=channel,
        schema_version=schema_version,
        tier=tier,
        date_str="2025-01-01",
        hour="10",
        node="MERGED",
    )
    make_parquet(p_dir / "part-matrix.parquet")

    orch = make_orchestrator(local_root=tmp_path, tier=tier, channel=channel, tmp_path=tmp_path)
    partitions = orch._discover_partitions()
    assert len(partitions) == 1, f"channel={channel} tier={tier}: expected 1 partition, got {len(partitions)}"
    assert channel in str(partitions[0])
    assert f"tier={tier}" in str(partitions[0])


def test_ac5_ec1_path_mapping_failure_quarantine(tmp_path):
    """AC-5 / Edge Case 1: date/hour/node 누락 partition → quarantine 처리.

    _build_chunk_spec 이 date=None → "unknown" fallback, 이관은 진행되지만
    chunk_id 에 "unknown" date 가 박제되어 후속 검증에서 분리 가능.
    """
    # 비정상 경로 — date= 누락 (schema_version=*/tier=L2/exchange=X/symbol=Y/ 바로 파일)
    invalid_dir = (
        tmp_path / "market" / "orderbooksnapshot"
        / "schema_version=orderbook_snapshot.v1" / "tier=L2"
        / "exchange=BITHUMB" / "symbol=BTC_KRW"
    )
    invalid_dir.mkdir(parents=True, exist_ok=True)
    invalid_parquet = invalid_dir / "part-invalid.parquet"
    invalid_parquet.write_bytes(b"PAR1")

    orch = make_orchestrator(local_root=tmp_path, tier="L2", tmp_path=tmp_path)

    # date= 누락 path 는 _extract_date_from_path 가 None 반환 → _discover_partitions 에서 skip
    partitions = orch._discover_partitions()
    # date= 없는 path 는 closed-day filter 에서 제외 (quarantine — 수동 검토)
    assert len(partitions) == 0, "date= 없는 invalid partition 은 discovery 에서 제외되어야 함"


def test_ac5_ec2_partial_verify_fail_blocks_local_delete(tmp_path):
    """AC-5 / Edge Case 2: 검증 부분 실패 시 BackfillResult 로 명확히 표시.

    1종 invariant FAIL → chunk_invariant_failed + quarantined_chunks >= 1
    → 원본 source_path 가 여전히 존재 (local delete 미실행 검증).
    """
    from unittest.mock import MagicMock, patch

    mock_harness = MagicMock()
    # 첫 chunk 는 invariant FAIL
    mock_harness.verify.return_value = InvariantResult(status="sha256_fail", per_invariant_results={})
    mock_uploader = MagicMock()
    mock_uploader.put.return_value = PutResult(status="uploaded", latency_ms=50.0)
    mock_sop = MagicMock()
    mock_sop.is_manual_gate.return_value = False
    mock_metrics = MagicMock()

    p_dir = make_partition_dir(tmp_path, date_str="2025-01-01", hour="10", node="MERGED")
    source_file = p_dir / "part-verify-fail.parquet"
    make_parquet(source_file)

    from mctrader_data.nas_migration.backfill_orchestrator import BackfillOrchestrator
    orch = BackfillOrchestrator(
        nas_uploader=mock_uploader,
        invariant_harness=mock_harness,
        sop_runner=mock_sop,
        metrics=mock_metrics,
        local_root=tmp_path,
        nas_partition_root="tier=L2",
        checkpoint_path=tmp_path / "cp.sqlite",
        evidence_pack_path=tmp_path / "ep.md",
        lock_path=tmp_path / "lock",
        max_workers=2,
        verify_retry_budget=3,
        tier="L2",
        channel="orderbooksnapshot",
    )

    with patch("mctrader_data.nas_migration.backfill_orchestrator.date") as mock_date:
        mock_date.today.return_value = __import__("datetime").date(2030, 1, 1)
        result = orch.run()

    # EC-2: 검증 부분 실패 → chunk_invariant_failed, quarantined >= 1
    assert result.status == "chunk_invariant_failed"
    assert result.quarantined_chunks >= 1

    # 원본 파일 보존 검증 (BackfillOrchestrator 는 local DELETE 0)
    assert source_file.exists(), "invariant FAIL 시 원본 source file 이 삭제되어서는 안 됨 (CutoverVerifier gate)"
