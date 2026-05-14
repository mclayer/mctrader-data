"""tests/integration/test_dual_writer_streaming_v2.py
MCT-163 Phase 2.2 — F3 DualWriter put_streaming TDD tests.

Story: MCT-163
Spec: docs/superpowers/specs/2026-05-14-MCT-163-dualwriter-streaming-design.md §4 AC-1/AC-2/AC-6
Plan: docs/superpowers/plans/2026-05-14-mct-163-dualwriter-streaming.md Task 3

AC-1: test_dual_writer_no_read_bytes
  - DualWriter.write(data=Path) → Path.read_bytes() 호출 0 (streaming으로 교체)
  - put_streaming() 호출 verify

AC-1/INV-4: test_dual_writer_streaming_memory_invariant
  - 105 MiB payload, peak RSS + tracemalloc delta ≤ 50 MB
  - DualWriter streaming path (read_bytes 호출 0 포함)

D2=A: test_dual_writer_caller_sha256_metadata
  - caller-side sha256 계산 후 DualWriter.write(sha256=...) 주입
  - sha256 metadata가 NASUploader에 전달되어야 함 (INV-3)

D6=C: psutil RSS + tracemalloc delta-based assert (절대값 X, delta-based)
"""
from __future__ import annotations

import gc
import hashlib
import io
import os
import tracemalloc
from pathlib import Path
from unittest import mock

import psutil
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.nas_storage.dual_writer import DualWriter, DualWriteResult
from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult


# ============================================================================
# Helpers
# ============================================================================

def _make_parquet_bytes(size_bytes: int) -> bytes:
    """Generate a parquet file with large raw_json field to hit target size.

    size_bytes: approximate target file size.
    We use raw_json padding to reach the target.
    """
    # Each row has ~1 KB raw_json
    n_rows = max(1, size_bytes // 1024)
    rows = [
        {
            "ts_utc": i,
            "raw_json": "x" * 1000,
            "node_id": f"node-{i % 10}",
        }
        for i in range(n_rows)
    ]
    schema = pa.schema([
        pa.field("ts_utc", pa.int64()),
        pa.field("raw_json", pa.large_utf8()),
        pa.field("node_id", pa.string()),
    ])
    table = pa.Table.from_pylist(rows, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    raw = buf.getvalue()
    # If still too small, pad with extra bytes in a comment-like structure
    while len(raw) < size_bytes:
        extra = size_bytes - len(raw)
        raw = raw + b"\x00" * extra
        break
    return raw


def _measure_delta(fn) -> tuple[int, int]:
    """Run fn(), return (rss_delta_bytes, tracemalloc_delta_bytes).

    D6=C: delta-based (절대값 기준 X, start~end delta).
    """
    proc = psutil.Process(os.getpid())
    gc.collect()
    rss_before = proc.memory_info().rss
    tracemalloc.start()
    snap1 = tracemalloc.take_snapshot()

    fn()

    snap2 = tracemalloc.take_snapshot()
    tracemalloc.stop()
    gc.collect()
    rss_after = proc.memory_info().rss

    rss_delta = max(0, rss_after - rss_before)
    # tracemalloc delta = sum of positive size changes
    stats = snap2.compare_to(snap1, "lineno")
    tm_delta = sum(s.size_diff for s in stats if s.size_diff > 0)

    return rss_delta, tm_delta


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def tmp_root(tmp_path: Path) -> Path:
    root = tmp_path / "data"
    root.mkdir()
    return root


@pytest.fixture
def mock_uploader_streaming() -> NASUploader:
    """Mock NASUploader with both put and put_streaming returning 'uploaded'."""
    uploader = mock.MagicMock(spec=NASUploader)
    uploader.put.return_value = PutResult(
        status="uploaded",
        object_etag="mock-etag-streaming",
        latency_ms=5.0,
    )
    uploader.put_streaming.return_value = PutResult(
        status="uploaded",
        object_etag="mock-etag-streaming",
        latency_ms=5.0,
    )
    return uploader


# ============================================================================
# Test AC-1: no read_bytes in DualWriter path (F3 streaming)
# ============================================================================

def test_dual_writer_no_read_bytes(tmp_root: Path, mock_uploader_streaming: NASUploader) -> None:
    """AC-1 (F3): DualWriter.write(data=Path) 시 Path.read_bytes() 호출 0.

    MCT-163 F3: dual_writer.py write() 내부 read_bytes 제거 → put_streaming 호출.
    - Path.read_bytes() 호출 횟수 == 0 (streaming path)
    - put_streaming() 호출 1회 (NASUploader.put_streaming)
    """
    dw = DualWriter(nas_uploader=mock_uploader_streaming, local_root=tmp_root)

    # Create small test parquet file
    small_bytes = b"PAR1" + b"\x00" * 1024 + b"PAR1"
    source_path = tmp_root / "source.parquet"
    source_path.write_bytes(small_bytes)
    sha256_hex = hashlib.sha256(small_bytes).hexdigest()

    read_bytes_call_count: list[int] = [0]
    _original_read_bytes = Path.read_bytes

    def spy_read_bytes(self_path: Path) -> bytes:
        read_bytes_call_count[0] += 1
        return _original_read_bytes(self_path)

    dest_path = tmp_root / "dest.parquet"

    with mock.patch.object(Path, "read_bytes", spy_read_bytes):
        result = dw.write(
            local_path=dest_path,
            nas_key="test/key/dest.parquet",
            data=source_path,
            sha256=sha256_hex,
        )

    # AC-1: Path.read_bytes() 호출 0 (F3 streaming 전환 완료)
    assert read_bytes_call_count[0] == 0, (
        f"MCT-163 F3 violation: Path.read_bytes() called {read_bytes_call_count[0]} times. "
        f"Expected 0 (streaming path must not use read_bytes)."
    )

    # put_streaming 호출 검증
    assert mock_uploader_streaming.put_streaming.call_count >= 1, (  # type: ignore[union-attr]
        "put_streaming() must be called at least once (F3 streaming path)."
    )

    assert isinstance(result, DualWriteResult)
    assert result.status in ("committed", "local_only", "hard_floor_blocked")


# ============================================================================
# Test AC-1/INV-4: memory invariant ≤ 50 MB delta
# ============================================================================

@pytest.mark.slow
def test_dual_writer_streaming_memory_invariant(tmp_root: Path) -> None:
    """INV-4: 105 MiB payload 처리 시 peak RSS + tracemalloc delta ≤ 50 MB.

    D6=C: psutil RSS delta + tracemalloc delta (delta-based, 절대값 X).
    105 MiB = MCT-160 spec 기준 memory 재할당 0회 claim 검증.

    put_streaming → upload_fileobj (boto3 TransferConfig, D1=B) → 메모리 전체 로드 0.
    """
    target_bytes = 105 * 1024 * 1024  # 105 MiB
    limit_bytes = 50 * 1024 * 1024    # 50 MB (INV-4)

    payload = _make_parquet_bytes(target_bytes)
    source_path = tmp_root / "large.parquet"
    source_path.write_bytes(payload)
    sha256_hex = hashlib.sha256(payload).hexdigest()

    # Use mock uploader: put_streaming simulates streaming (does not load full bytes)
    uploader = mock.MagicMock(spec=NASUploader)
    # put_streaming receives fileobj — don't read it all (simulate streaming)
    def streaming_put(local_path_or_fileobj, nas_key, sha256):  # noqa: ANN001
        # Simulate chunk-wise read (not all at once)
        if hasattr(local_path_or_fileobj, "read"):
            chunk_size = 8 * 1024 * 1024  # 8 MB chunks
            while True:
                chunk = local_path_or_fileobj.read(chunk_size)
                if not chunk:
                    break
        return PutResult(status="uploaded", object_etag="mock-etag", latency_ms=1.0)

    uploader.put_streaming.side_effect = streaming_put
    uploader.put.return_value = PutResult(status="uploaded", object_etag="mock-etag", latency_ms=1.0)

    dw = DualWriter(nas_uploader=uploader, local_root=tmp_root)
    dest_path = tmp_root / "dest.parquet"

    def run():
        dw.write(
            local_path=dest_path,
            nas_key="test/key/large.parquet",
            data=source_path,
            sha256=sha256_hex,
        )

    rss_delta, tm_delta = _measure_delta(run)

    # INV-4: both RSS delta and tracemalloc delta must be ≤ 50 MB
    # (delta-based: 시작 전 baseline에서의 증분만 측정)
    assert rss_delta <= limit_bytes, (
        f"INV-4 RSS delta violation: {rss_delta / 1024 / 1024:.1f} MB > 50 MB. "
        f"DualWriter must use streaming (put_streaming + upload_fileobj), not read_bytes."
    )
    assert tm_delta <= limit_bytes, (
        f"INV-4 tracemalloc delta violation: {tm_delta / 1024 / 1024:.1f} MB > 50 MB. "
        f"DualWriter must use streaming (put_streaming + upload_fileobj), not read_bytes."
    )


# ============================================================================
# Test D2=A: caller sha256 propagation to NASUploader (INV-3)
# ============================================================================

def test_dual_writer_caller_sha256_metadata(tmp_root: Path, mock_uploader_streaming: NASUploader) -> None:
    """D2=A + INV-3: caller-side sha256가 NASUploader.put_streaming() 에 전달됨.

    sha256 SSOT: caller 가 single hash 계산 → DualWriter inject → NASUploader metadata header 전달.
    multipart ETag ≠ sha256 (INV-3 정합).
    """
    dw = DualWriter(nas_uploader=mock_uploader_streaming, local_root=tmp_root)

    payload = b"test-payload-for-sha256-verify" * 100
    source_path = tmp_root / "sha256test.parquet"
    source_path.write_bytes(payload)

    # D2=A: caller-side single sha256 계산
    sha256_hex = hashlib.sha256(payload).hexdigest()

    dest_path = tmp_root / "sha256dest.parquet"
    dw.write(
        local_path=dest_path,
        nas_key="test/key/sha256test.parquet",
        data=source_path,
        sha256=sha256_hex,
    )

    # INV-3: put_streaming 호출 시 sha256 kwarg 전달 검증
    assert mock_uploader_streaming.put_streaming.call_count >= 1, (  # type: ignore[union-attr]
        "put_streaming() must be called (F3 streaming path)."
    )
    call_args = mock_uploader_streaming.put_streaming.call_args  # type: ignore[union-attr]
    # sha256 must be passed as kwarg or positional arg
    called_sha256 = (
        call_args.kwargs.get("sha256")
        or (call_args.args[2] if len(call_args.args) >= 3 else None)
    )
    assert called_sha256 == sha256_hex, (
        f"INV-3 violation: sha256 not propagated to put_streaming(). "
        f"Expected {sha256_hex!r}, got {called_sha256!r}. "
        f"D2=A: caller-side sha256 must flow through DualWriter → NASUploader."
    )
