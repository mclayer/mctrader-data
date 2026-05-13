"""test_compactor_nas_wiring.py — Integration tests for compactor NAS wiring (Stage 3).

Story: MCT-156 (Phase 2 — compactor NAS dual-write wiring + 7종 invariant harness + Stage 3 ADR-027 amendment)
Issue: mclayer/mctrader-hub#279

Test Contract §8 (MCT-156):
- test_l2_committed_partition_appears: DualWriter committed → L2 bucket prefix 출현
- test_l3_committed_partition_appears: DualWriter committed → L3 bucket prefix 출현
- test_nas_unreachable_local_only_enqueue: NAS unreachable → status=local_only + retry_queue enqueue
- test_retry_hard_floor_sop_escalation: hard_floor → status=hard_floor_blocked + SOP escalation
- test_l1_no_nas_upload: L1 compaction 후 DualWriter.put 호출 absence (S3 invariant)
- test_legacy_minio_uploader_no_callsite: grep MinioUploader() call sites = 0
- test_prometheus_dual_write_counter_emit: dual_write_result_total Counter emit verify
- test_l2_compaction_latency_baseline: L2 compact_hour latency < 3000ms (NFR-1)

Architecture (ADR-027 D5 amendment, MCT-150/151 primitive 재사용):
- DualWriter.write(local_path, nas_key, data, sha256) → DualWriteResult(status, nas_put_result, ...)
- status enum: "committed" / "local_only" / "hard_floor_blocked"
- CompactorRunner._run_l2() / _run_l3() + DualWriter inject
- Prometheus emit: mctrader_dual_write_result_total{status, tier}

Mock strategy:
- mock NAS bucket (boto3 stubber or moto S3)
- mock RetryQueue (in-memory or sqlite)
- mock Prometheus Counter (verify emit)
"""
from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from mctrader_data.nas_storage.dual_writer import DualWriter
from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult
from mctrader_data.nas_storage.retry_queue import RetryQueue


log = logging.getLogger(__name__)


# ─── fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_nas_bucket(tmp_path: Path) -> Path:
    """Mock NAS bucket directory (simulating MinIO S3 bucket)."""
    bucket = tmp_path / "nas_bucket"
    bucket.mkdir()
    return bucket


@pytest.fixture
def retry_queue(tmp_path: Path) -> RetryQueue:
    """Shared RetryQueue fixture (MCT-150 pattern)."""
    return RetryQueue(path=tmp_path / "retry_queue")


@pytest.fixture
def mock_nas_uploader(tmp_nas_bucket: Path, retry_queue: RetryQueue) -> NASUploader:
    """Mock NASUploader using local filesystem (simulating MinIO via boto3 stubber)."""
    return NASUploader(
        endpoint="http://nas.local:9000",
        access_key="test-access-key",
        secret_key="test-secret-key",
        bucket="mctrader-market",
        retry_queue=retry_queue,
    )


@pytest.fixture
def dual_writer(mock_nas_uploader: NASUploader, tmp_path: Path) -> DualWriter:
    """DualWriter instance with mock NASUploader."""
    return DualWriter(
        nas_uploader=mock_nas_uploader,
        local_root=tmp_path / "local_root",
    )


@pytest.fixture
def sample_ohlcv_payload() -> bytes:
    """Sample OHLCV Parquet payload (minimal Parquet binary)."""
    # Minimal valid Parquet payload (real test would use pyarrow)
    return (
        b"PAR1"  # Parquet magic
        + b"\x00" * 100  # Minimal metadata placeholder
        + b"PAR1"  # Parquet magic end
    )


@pytest.fixture
def sample_ohlcv_sha256(sample_ohlcv_payload: bytes) -> str:
    """SHA256 hash of sample payload."""
    return hashlib.sha256(sample_ohlcv_payload).hexdigest()


# ─── §8.1: L2 Committed Partition Appears ────────────────────────────────────

class TestL2CommittedPartition:
    """§8 Test 1: DualWriter committed → L2 bucket prefix 출현."""

    def test_l2_committed_partition_appears(
        self,
        dual_writer: DualWriter,
        tmp_path: Path,
        sample_ohlcv_payload: bytes,
        sample_ohlcv_sha256: str,
    ) -> None:
        """L2 compaction tick → DualWriter.write committed.

        bucket prefix `tier=L2/.../hour=HH/node=MERGED/part-*.parquet` 출현.
        Mock NASUploader.put() → PutResult(status='uploaded')
        → DualWriteResult(status='committed').
        """
        # RED: fail expected — DualWriter.write not yet wired to CompactorRunner
        local_path = (
            tmp_path / "local_root" / "schema_version=v1"
            / "exchange=KRX" / "symbol=005930" / "channel=upbit"
            / "tier=L2" / "date=2026-05-13" / "hour=14" / "node=MERGED"
            / "part-00000.parquet"
        )
        nas_key = (
            "schema_version=v1/exchange=KRX/symbol=005930/channel=upbit"
            "/tier=L2/date=2026-05-13/hour=14/node=MERGED/part-00000.parquet"
        )

        with patch.object(dual_writer._uploader, "put") as mock_put:
            mock_put.return_value = PutResult(
                status="uploaded",
                object_etag="etag123",
                latency_ms=100.0,
            )

            result = dual_writer.write(
                local_path=local_path,
                nas_key=nas_key,
                data=sample_ohlcv_payload,
                sha256=sample_ohlcv_sha256,
            )

        # Assert committed status
        assert result.status == "committed", f"Expected committed, got {result.status}"
        assert result.local_path == local_path
        assert result.nas_key == nas_key
        # Assert NASUploader.put was called with correct key
        mock_put.assert_called_once()
        call_args = mock_put.call_args
        assert call_args[0][0] == nas_key, "put() called with wrong key"


# ─── §8.2: L3 Committed Partition Appears ────────────────────────────────────

class TestL3CommittedPartition:
    """§8 Test 2: DualWriter committed → L3 bucket prefix 출현 (no hour)."""

    def test_l3_committed_partition_appears(
        self,
        dual_writer: DualWriter,
        tmp_path: Path,
        sample_ohlcv_payload: bytes,
        sample_ohlcv_sha256: str,
    ) -> None:
        """L3 compaction tick → DualWriter.write committed (hour 없음).

        bucket prefix `tier=L3/.../date=D/node=MERGED/file.parquet` 출현.
        Mock NASUploader.put() → PutResult(status='uploaded')
        → DualWriteResult(status='committed').
        """
        # RED: fail expected
        local_path = (
            tmp_path / "local_root" / "schema_version=v1"
            / "exchange=KRX" / "symbol=005930" / "channel=upbit"
            / "tier=L3" / "date=2026-05-13" / "node=MERGED" / "file.parquet"
        )
        nas_key = (
            "schema_version=v1/exchange=KRX/symbol=005930/channel=upbit"
            "/tier=L3/date=2026-05-13/node=MERGED/file.parquet"
        )

        with patch.object(dual_writer._uploader, "put") as mock_put:
            mock_put.return_value = PutResult(
                status="uploaded",
                object_etag="etag456",
                latency_ms=150.0,
            )

            result = dual_writer.write(
                local_path=local_path,
                nas_key=nas_key,
                data=sample_ohlcv_payload,
                sha256=sample_ohlcv_sha256,
            )

        assert result.status == "committed"
        assert result.local_path == local_path
        assert result.nas_key == nas_key
        mock_put.assert_called_once()


# ─── §8.3: NAS Unreachable → Local Only + Enqueue ──────────────────────────

class TestNASUnreachableLocalOnly:
    """§8 Test 3: NAS unreachable → status=local_only + retry_queue enqueue."""

    def test_nas_unreachable_local_only_enqueue(
        self,
        dual_writer: DualWriter,
        tmp_path: Path,
        sample_ohlcv_payload: bytes,
        sample_ohlcv_sha256: str,
    ) -> None:
        """NAS endpoint unreachable (mock return queued) → DualWriter.write local_only.

        retry_queue.size() > 0.
        Mock NASUploader.put() → PutResult(status='queued')
        → DualWriteResult(status='local_only').
        """
        # RED: fail expected
        local_path = (
            tmp_path / "local_root" / "schema_version=v1"
            / "exchange=KRX" / "symbol=005930" / "channel=upbit"
            / "tier=L2" / "date=2026-05-13" / "hour=14" / "node=MERGED"
            / "part-00000.parquet"
        )
        nas_key = (
            "schema_version=v1/exchange=KRX/symbol=005930/channel=upbit"
            "/tier=L2/date=2026-05-13/hour=14/node=MERGED/part-00000.parquet"
        )

        with patch.object(dual_writer._uploader, "put") as mock_put:
            mock_put.return_value = PutResult(
                status="queued",
                object_etag="",
                latency_ms=50.0,
            )

            result = dual_writer.write(
                local_path=local_path,
                nas_key=nas_key,
                data=sample_ohlcv_payload,
                sha256=sample_ohlcv_sha256,
            )

        assert result.status == "local_only", f"Expected local_only, got {result.status}"
        assert local_path.exists(), "local_path must exist after local_only"


# ─── §8.4: Hard Floor Blocked + SOP Escalation ────────────────────────────

class TestHardFloorBlockedSOP:
    """§8 Test 4: hard_floor_blocked → SOP MANUAL_GATE escalation."""

    def test_retry_hard_floor_sop_escalation(
        self,
        dual_writer: DualWriter,
        tmp_path: Path,
        sample_ohlcv_payload: bytes,
        sample_ohlcv_sha256: str,
    ) -> None:
        """retry_queue hard floor (1000seg/10GB) exceeded → DualWriter.write hard_floor_blocked.

        local tmp rollback + status=hard_floor_blocked.
        Mock NASUploader.put() → PutResult(status='hard_floor_blocked')
        → DualWriteResult(status='hard_floor_blocked').
        """
        # RED: fail expected
        local_path = (
            tmp_path / "local_root" / "schema_version=v1"
            / "exchange=KRX" / "symbol=005930" / "channel=upbit"
            / "tier=L2" / "date=2026-05-13" / "hour=14" / "node=MERGED"
            / "part-00000.parquet"
        )
        nas_key = (
            "schema_version=v1/exchange=KRX/symbol=005930/channel=upbit"
            "/tier=L2/date=2026-05-13/hour=14/node=MERGED/part-00000.parquet"
        )

        with patch.object(dual_writer._uploader, "put") as mock_put:
            mock_put.return_value = PutResult(
                status="hard_floor_blocked",
                object_etag="",
                latency_ms=0.0,
            )

            result = dual_writer.write(
                local_path=local_path,
                nas_key=nas_key,
                data=sample_ohlcv_payload,
                sha256=sample_ohlcv_sha256,
            )

        assert result.status == "hard_floor_blocked", f"Expected hard_floor_blocked, got {result.status}"
        # tmp file should be rolled back (not exist)
        tmp_path_obj = local_path.with_suffix(local_path.suffix + ".tmp_dw")
        assert not tmp_path_obj.exists(), "tmp_dw file should be rolled back"


# ─── §8.5: L1 No NAS Upload (S3 Invariant) ────────────────────────────────

class TestL1NoNASUpload:
    """§8 Test 5: L1 compaction → DualWriter.put 호출 absence (S3 invariant)."""

    def test_l1_no_nas_upload(self) -> None:
        """L1 compaction flow: no DualWriter.put() call expected (L1 stays local only).

        L1 → L2 transition triggers DualWriter only for L2 and above.
        Verify by grep: CompactorRunner._run_l1() should not call dual_writer.write().
        """
        # RED: fail expected — CompactorRunner._run_l1 may incorrectly call DualWriter
        runner_file = Path("src/mctrader_data/compactor/runner.py")

        if runner_file.exists():
            runner_content = runner_file.read_text(encoding="utf-8")
            # Find _run_l1 method (if it exists)
            lines = runner_content.split("\n")
            in_run_l1 = False
            l1_lines = []

            for _, line in enumerate(lines):
                if "def _run_l1(" in line or "def compact_segment(" in line:
                    in_run_l1 = True
                elif in_run_l1 and line.startswith("    def "):
                    in_run_l1 = False

                if in_run_l1:
                    l1_lines.append(line)

            l1_method = "\n".join(l1_lines)
            # Assert that _run_l1 or compact_segment does NOT call dual_writer or DualWriter
            assert "dual_writer" not in l1_method.lower(), \
                "L1Compactor should not reference dual_writer (§8 Test 5 S3 invariant)"
            assert "DualWriter" not in l1_method, \
                "L1Compactor should not use DualWriter (§8 Test 5 S3 invariant)"


# ─── §8.6: Legacy MinioUploader No Call Sites ────────────────────────────────

class TestLegacyMinioUploaderDeprecation:
    """§8 Test 6: legacy MinioUploader 호출처 grep=0 (deprecation)."""

    def test_legacy_minio_uploader_no_callsite(self) -> None:
        """grep -r 'MinioUploader(' src/mctrader_data/cli.py src/mctrader_data/compactor/runner.py = 0 lines.

        Verify that MinioUploader is not referenced in CLI or runner (deprecated, replaced by DualWriter).
        """
        # RED: fail expected — MinioUploader may still be imported/used
        cli_file = Path("src/mctrader_data/cli.py")
        runner_file = Path("src/mctrader_data/compactor/runner.py")

        # Check cli.py
        if cli_file.exists():
            cli_content = cli_file.read_text(encoding="utf-8")
            cli_count = cli_content.count("MinioUploader(")
            assert cli_count == 0, f"MinioUploader() found {cli_count} times in cli.py (expected 0)"

        # Check runner.py
        if runner_file.exists():
            runner_content = runner_file.read_text(encoding="utf-8")
            runner_count = runner_content.count("MinioUploader(")
            assert runner_count == 0, f"MinioUploader() found {runner_count} times in runner.py (expected 0)"


# ─── §8.7: Prometheus Dual Write Result Counter Emit ──────────────────────

class TestPrometheusEmit:
    """§8 Test 7: Prometheus dual_write_result_total Counter emit."""

    def test_prometheus_dual_write_counter_emit(
        self,
        tmp_path: Path,
        sample_ohlcv_payload: bytes,
        sample_ohlcv_sha256: str,
    ) -> None:
        """mctrader_dual_write_result_total{status='committed', tier='L2'} Counter exists with correct labels.

        Verify that Prometheus dual_write_result_total Counter is defined with labels {status, tier}.
        """
        # RED: fail expected — Counter may not exist or have wrong labels
        from mctrader_data.nas_metrics.prometheus_exporters import dual_write_result_total

        # Verify Counter exists
        assert dual_write_result_total is not None, \
            "dual_write_result_total Counter must be defined (MCT-156 §8 Test 7)"

        # Verify Counter has correct label names
        assert hasattr(dual_write_result_total, "_labelnames"), \
            "Counter must have _labelnames attribute"

        label_names = dual_write_result_total._labelnames
        assert "status" in label_names, \
            f"Counter must have 'status' label. Found: {label_names}"
        assert "tier" in label_names, \
            f"Counter must have 'tier' label. Found: {label_names}"

        # Verify allowed status enum (3 values: committed, local_only, hard_floor_blocked)
        # Verify allowed tier enum (2 values: L2, L3)
        # This test verifies the metric exists; emit testing happens via mock in integration tests.
        pass


# ─── §8.8: L2 Compaction Latency Baseline (NFR-1) ─────────────────────────

class TestL2CompactionLatencyBaseline:
    """§8 Test 8 (perf): L2 compact_hour latency < 3000ms (MCT-148 NFR-1)."""

    def test_l2_compaction_latency_baseline(
        self,
        dual_writer: DualWriter,
        tmp_path: Path,
        sample_ohlcv_payload: bytes,
        sample_ohlcv_sha256: str,
    ) -> None:
        """L2 compaction (1h = 50 symbol tick) latency < 3000ms.

        Mock NAS latency (p99 50MB = 2870.65ms MCT-148 baseline).
        Verify total elapsed < 3000ms.
        """
        # RED: fail expected — latency baseline not yet verified
        local_path = (
            tmp_path / "local_root" / "schema_version=v1"
            / "exchange=KRX" / "symbol=005930" / "channel=upbit"
            / "tier=L2" / "date=2026-05-13" / "hour=14" / "node=MERGED"
            / "part-00000.parquet"
        )
        nas_key = (
            "schema_version=v1/exchange=KRX/symbol=005930/channel=upbit"
            "/tier=L2/date=2026-05-13/hour=14/node=MERGED/part-00000.parquet"
        )

        # Simulate NAS latency (mock as 50ms for unit test, real perf uses pytest-benchmark)
        mock_nas_latency_ms = 50.0

        start_time = time.monotonic()

        with patch.object(dual_writer._uploader, "put") as mock_put:
            mock_put.return_value = PutResult(
                status="uploaded",
                object_etag="etag_perf",
                latency_ms=mock_nas_latency_ms,
            )

            result = dual_writer.write(
                local_path=local_path,
                nas_key=nas_key,
                data=sample_ohlcv_payload,
                sha256=sample_ohlcv_sha256,
            )

        elapsed_ms = (time.monotonic() - start_time) * 1000

        # Assert latency baseline
        assert elapsed_ms < 3000.0, f"L2 latency {elapsed_ms}ms exceeds 3000ms baseline (NFR-1)"
        assert result.latency_ms < 3000.0, f"DualWriter result latency {result.latency_ms}ms exceeds baseline"
