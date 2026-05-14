# tests/integration/test_dual_writer_streaming.py
"""
Integration tests for DualWriter Path streaming (MCT-151 land signature).

Story: MCT-160 Phase 2 (QADeveloperAgent lane — R-EXTRA + L1 ordering verify)
Contract: Story §8 Test Contract (Test-5 + Test-6 expansion)

MCT-160 FIX iter 1 (F1+F2+F9):
  F1: pa.table(rows, schema=...) → pa.Table.from_pylist(rows, schema=...) fix
  F2: DualWriter constructor + signature MISMATCH fix (실 sig 답습)
  F9: Test-2 memory profile assertion 복구 (mock_read_bytes.call_count <= 1)

Test-1: test_dual_writer_accepts_path
  - DualWriter.write(data=Path) signature verify (MCT-151 실 signature 답습)
  - Path object accepted without conversion error
  - DualWriteResult.status ∈ {"committed", "local_only", "hard_floor_blocked"}

Test-2: test_dual_writer_memory_profile_read_bytes_caller_count
  - F9 fix: caller path 의 read_bytes() 호출 count verify (mock spy)
  - caller: streaming sha256 (open+chunk 방식, read_bytes 0)
  - DualWriter 내부: data=Path → read_bytes() 1회 (F3 surface — follow-up Story)
  - assert spy.call_count <= 1 (DualWriter 내부 1회, caller 0회)

ADR-027 D6 amendment (R-EXTRA):
  - caller sha256 산출 (runner.py _dispatch_dual_write)
  - DualWriter.write(data=parquet_path, sha256=<hex>) 호출
  - read_bytes() 호출 2회 (sha256 + dispatch) → 1회 (caller sha256만)
  - DualWriter 변경 0 (이미 data: Path accept)
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.l1 import _ORDERBOOKDEPTH_SCHEMA
from mctrader_data.nas_storage.dual_writer import DualWriter, DualWriteResult
from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def tmp_data_root(tmp_path: Path) -> Path:
    """Temporary data root."""
    root = tmp_path / "data"
    root.mkdir()
    (root / "market").mkdir()
    return root


@pytest.fixture
def sample_l2_parquet(tmp_data_root: Path) -> Path:
    """Create sample L2 parquet file using pa.Table.from_pylist (F1 fix)."""
    output_dir = (
        tmp_data_root / "market" / "orderbookdepth"
        / "schema_version=orderbook_depth.v1" / "tier=L2"
        / "exchange=bithumb" / "symbol=KRW-BTC"
        / "date=2026-05-10" / "hour=17" / "node=MERGED"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc)
    rows = [
        {
            "ts_utc": now_utc + timedelta(seconds=i),
            "received_at": now_utc - timedelta(milliseconds=50),
            "exchange": "bithumb",
            "symbol": "KRW-BTC",
            "side": "ask" if i % 2 == 0 else "bid",
            "price": Decimal("50000.0") + Decimal(i) * Decimal("10"),
            "quantity": Decimal("1.5") + Decimal(i) * Decimal("0.001"),
            "raw_json": '{"level": ' + str(i) + ', "data": "' + ("x" * 500) + '"}',
            "node_id": "node-1",
            "collector_run_id": "run-1",
            "ingest_seq": i,
        }
        for i in range(100)
    ]

    # F1 fix: pa.Table.from_pylist (not pa.table with list-of-row-dicts)
    table = pa.Table.from_pylist(rows, schema=_ORDERBOOKDEPTH_SCHEMA)
    parquet_path = output_dir / "part-test-001.parquet"
    pq.write_table(table, str(parquet_path))

    return parquet_path


@pytest.fixture
def mock_nas_uploader(tmp_data_root: Path) -> NASUploader:
    """Mock NASUploader that returns 'uploaded' status (unittest.mock 방식, F2 fix).

    MCT-163 F3: put_streaming() mock 추가 (DualWriter Path path → put_streaming 호출).
    실 NASUploader 인스턴스를 mock.MagicMock()으로 교체.
    NASUploader(endpoint=...) constructor 대신 mock 직접 주입.
    """
    uploader = mock.MagicMock(spec=NASUploader)
    uploader.put.return_value = PutResult(
        status="uploaded",
        object_etag="mock-etag-abc123",
        latency_ms=5.0,
    )
    # MCT-163 F3: DualWriter Path path uses put_streaming (backward compat extension)
    uploader.put_streaming.return_value = PutResult(
        status="uploaded",
        object_etag="mock-etag-abc123",
        latency_ms=5.0,
    )
    return uploader


@pytest.fixture
def dual_writer(tmp_data_root: Path, mock_nas_uploader: NASUploader) -> DualWriter:
    """DualWriter with mock NASUploader (실 signature 답습, F2 fix).

    실 signature: DualWriter(nas_uploader=..., local_root=..., metrics=None)
    """
    return DualWriter(nas_uploader=mock_nas_uploader, local_root=tmp_data_root)


# ============================================================================
# Test-1: DualWriter accepts Path (MCT-151 signature, F2 fix)
# ============================================================================

def test_dual_writer_accepts_path(
    dual_writer: DualWriter,
    sample_l2_parquet: Path,
) -> None:
    """
    MCT-151 signature: DualWriter.write(data=Path) 정상 동작 verify.
    F2 fix: 실 signature DualWriter(nas_uploader, local_root) 답습.

    Assertion:
      - DualWriteResult returned without exception
      - result.status ∈ {"committed", "local_only", "hard_floor_blocked"}
    """
    # Calculate sha256 (caller 책임 — D6 R-EXTRA pattern)
    sha256_hex = hashlib.sha256(sample_l2_parquet.read_bytes()).hexdigest()

    local_dest = sample_l2_parquet.parent / "part-dest-001.parquet"

    result = dual_writer.write(
        local_path=local_dest,
        nas_key="test/key/part.parquet",
        data=sample_l2_parquet,  # Path (not bytes)
        sha256=sha256_hex,
    )

    assert isinstance(result, DualWriteResult)
    assert result.status in ("committed", "local_only", "hard_floor_blocked"), (
        f"Unexpected DualWriteResult.status: {result.status!r}"
    )


# ============================================================================
# Test-2: DualWriter memory profile — caller read_bytes count (F9 fix)
# ============================================================================

def test_dual_writer_memory_profile_read_bytes_caller_count(
    dual_writer: DualWriter,
    sample_l2_parquet: Path,
) -> None:
    """
    F9 fix: caller path 의 read_bytes() 호출 count spy verify.

    R-EXTRA D6 pattern (MCT-160 Phase 2):
      - caller sha256 산출: streaming chunk read (read_bytes 0, open+iter 방식)
      - DualWriter.write(data=Path, sha256=hex) 호출
      - DualWriter 내부에서 data=Path → read_bytes() 1회 (F3 surface, follow-up MCT-163)
      - 합계: caller 0 + DualWriter 1 = 최대 1회

    Assertion:
      - spy.call_count <= 1  (DualWriter 내부 1회 이내, caller 0회)
    """
    local_dest = sample_l2_parquet.parent / "part-dest-002.parquet"

    # Caller: streaming sha256 (D6 pattern — read_bytes 0, open+iter 방식)
    sha = hashlib.sha256()
    with sample_l2_parquet.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    sha256_hex = sha.hexdigest()

    # spy: Path.read_bytes 클래스 레벨 패치 (WindowsPath 인스턴스 attribute read-only 우회)
    # original_fn = Path.read_bytes (unbound) — wrapper 에서 self 직접 전달
    read_bytes_call_count: list[int] = [0]
    _original_read_bytes = Path.read_bytes

    def spy_read_bytes(self_path: Path) -> bytes:
        read_bytes_call_count[0] += 1
        return _original_read_bytes(self_path)

    with mock.patch.object(Path, "read_bytes", spy_read_bytes):
        dual_writer.write(
            local_path=local_dest,
            nas_key="test/key/part-002.parquet",
            data=sample_l2_parquet,
            sha256=sha256_hex,
        )

    # F9 fix: caller 자체 read_bytes() 0 (streaming chunk read만)
    # DualWriter 내부 1회 (data=Path → payload = data.read_bytes(), F3 surface — follow-up MCT-163)
    # 합계: ≤ 1
    assert read_bytes_call_count[0] <= 1, (
        f"read_bytes() call count should be <= 1 (caller=0 + DualWriter internal=1). "
        f"Actual: {read_bytes_call_count[0]}. "
        f"F3 surface: DualWriter 내부 streaming은 MCT-163 후속 Story 대상."
    )
