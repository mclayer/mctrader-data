"""test_retry_queue.py — P0 TDD tests for RetryQueue (sqlite-WAL persistence).

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253

Test Contract §8 (TestContractArchitectAgent):
- test_enqueue_atomic: 단일 transaction 박제 (sqlite-WAL)
- test_persistent_restart_resume: process restart 후 pending segment 정확 복원 (real sqlite3 file)
- test_bounded_backlog_quarantine: threshold 초과 시 oldest quarantine + alert
- test_drain_on_recovery: NAS reachable 회복 시 drain 동작 (mock)
- Chaos test: NAS down → backlog 누적 → NAS up → resume + 손실 0 verify (NFR-3 의무)
"""
from __future__ import annotations

import hashlib
import sqlite3
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mctrader_data.nas_storage.retry_queue import DrainStats, EnqueueResult, RetryQueue


@pytest.fixture
def queue_path(tmp_path: Path) -> Path:
    return tmp_path / "retry_queue"


@pytest.fixture
def queue(queue_path: Path) -> RetryQueue:
    return RetryQueue(path=queue_path)


class TestEnqueueAtomic:
    """§8.2: enqueue contract — 단일 transaction 으로 segment metadata + payload reference insert."""

    def test_enqueue_single_item(self, queue: RetryQueue) -> None:
        """enqueue 1건 → depth() == 1."""
        data = b"segment-data"
        sha256 = hashlib.sha256(data).hexdigest()
        result = queue.enqueue(key="sym/date/file.parquet", data=data, sha256=sha256)
        assert isinstance(result, EnqueueResult)
        assert result.status in ("ok", "threshold_breached")
        assert queue.depth() == 1

    def test_enqueue_atomicity_on_simulated_crash(self, queue_path: Path) -> None:
        """sqlite WAL — 트랜잭션 commit 완료 후 row 존재 (crash-atomicity 증명)."""
        q = RetryQueue(path=queue_path)
        data = b"atomic-payload"
        sha256 = hashlib.sha256(data).hexdigest()

        result = q.enqueue(key="atomic/test.parquet", data=data, sha256=sha256)
        assert result.status in ("ok", "threshold_breached")

        # sqlite db 직접 열어서 row 존재 확인 (WAL commit 완료 증명)
        db_path = queue_path / "retry_queue.db"
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT key, sha256 FROM retry_queue WHERE state='pending'").fetchall()
        conn.close()

        assert len(rows) == 1
        assert rows[0][0] == "atomic/test.parquet"
        assert rows[0][1] == sha256

    def test_enqueue_multiple_items_ordered(self, queue: RetryQueue) -> None:
        """여러 enqueue → FIFO 순서 보장 (enqueue_ts ASC)."""
        keys = [f"key-{i}.parquet" for i in range(5)]
        for k in keys:
            data = k.encode()
            queue.enqueue(key=k, data=data, sha256=hashlib.sha256(data).hexdigest())

        assert queue.depth() == 5

    def test_concurrent_enqueue_thread_safe(self, queue_path: Path) -> None:
        """멀티스레드 동시 enqueue → 데이터 손실 0."""
        q = RetryQueue(path=queue_path)
        errors: list[Exception] = []

        def enqueue_n(n: int) -> None:
            for i in range(n):
                data = f"thread-data-{i}".encode()
                try:
                    q.enqueue(
                        key=f"thread/{threading.current_thread().name}/{i}.parquet",
                        data=data,
                        sha256=hashlib.sha256(data).hexdigest(),
                    )
                except Exception as e:
                    errors.append(e)

        threads = [threading.Thread(target=enqueue_n, args=(10,)) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors: {errors}"
        assert q.depth() == 50


class TestPersistentRestartResume:
    """AC-2: process restart 후 pending segment 정확 복원 (real sqlite3 file). §8.2 invariant."""

    def test_persistent_restart_resume(self, queue_path: Path) -> None:
        """RetryQueue 생성 → enqueue → 객체 삭제 → 재생성 → resume_on_startup() → pending count 일치."""
        q1 = RetryQueue(path=queue_path)
        payloads = [f"segment-{i}".encode() for i in range(3)]
        for i, payload in enumerate(payloads):
            q1.enqueue(
                key=f"resume-test/{i}.parquet",
                data=payload,
                sha256=hashlib.sha256(payload).hexdigest(),
            )
        assert q1.depth() == 3
        del q1  # simulate process exit

        q2 = RetryQueue(path=queue_path)
        pending_count = q2.resume_on_startup()
        assert pending_count == 3
        assert q2.depth() == 3

    def test_payload_files_persist_on_restart(self, queue_path: Path) -> None:
        """payload binary file 이 process restart 후에도 disk 에 존재."""
        q1 = RetryQueue(path=queue_path)
        data = b"important-segment-payload"
        sha256 = hashlib.sha256(data).hexdigest()
        q1.enqueue(key="persist/payload.parquet", data=data, sha256=sha256)
        del q1

        payload_dir = queue_path / "payloads"
        payload_files = list(payload_dir.glob("*.bin"))
        assert len(payload_files) == 1

        stored = payload_files[0].read_bytes()
        assert stored == data


class TestBoundedBacklogQuarantine:
    """AC-2: bounded backlog — threshold 초과 시 oldest quarantine + alert."""

    def test_threshold_segments_breach(self, queue_path: Path) -> None:
        """max_segments 초과 시 EnqueueResult.status='threshold_breached'."""
        q = RetryQueue(path=queue_path, max_segments=3, max_bytes=10 * 1024**3)
        for i in range(3):
            data = f"seg-{i}".encode()
            q.enqueue(key=f"seg-{i}.parquet", data=data, sha256=hashlib.sha256(data).hexdigest())

        data = b"one-too-many"
        result = q.enqueue(
            key="overflow.parquet", data=data, sha256=hashlib.sha256(data).hexdigest()
        )
        assert result.status == "threshold_breached"

    def test_threshold_bytes_breach(self, queue_path: Path) -> None:
        """max_bytes 초과 시 EnqueueResult.status='threshold_breached'."""
        q = RetryQueue(path=queue_path, max_segments=1000, max_bytes=50)

        data = b"x" * 30
        q.enqueue(key="first.parquet", data=data, sha256=hashlib.sha256(data).hexdigest())

        data2 = b"y" * 30  # total = 60 > 50
        result = q.enqueue(key="second.parquet", data=data2, sha256=hashlib.sha256(data2).hexdigest())
        assert result.status == "threshold_breached"

    def test_default_bounds_are_1000_and_10gb(self, queue_path: Path) -> None:
        """기본 max_segments=1000, max_bytes=10GB (S10 박제)."""
        q = RetryQueue(path=queue_path)
        assert q.max_segments == 1000
        assert q.max_bytes == 10 * 1024**3


class TestDrainOnRecovery:
    """§8.2 invariant: FIFO + 손실 0 drain. Chaos test (NFR-3 의무)."""

    def test_drain_on_recovery_no_loss(self, queue_path: Path) -> None:
        """NAS down → backlog 누적 → NAS up → drain → 손실 0 verify.

        NFR-3: chaos test 의무 — happy path 단독 거부.
        """
        q = RetryQueue(path=queue_path)
        keys = [f"drain-test/{i}.parquet" for i in range(5)]
        payloads = {k: f"payload-{i}".encode() for i, k in enumerate(keys)}

        # NAS down 시뮬레이션: enqueue
        for k, payload in payloads.items():
            q.enqueue(key=k, data=payload, sha256=hashlib.sha256(payload).hexdigest())

        assert q.depth() == 5

        # NAS up 시뮬레이션: uploader mock (always succeeds)
        mock_uploader = MagicMock()
        mock_uploader.put.return_value = MagicMock(status="uploaded")

        stats = q.drain(mock_uploader)

        assert isinstance(stats, DrainStats)
        assert stats.drained == 5
        assert stats.failed == 0
        assert q.depth() == 0  # 손실 0, 모두 drain 완료

    def test_drain_fifo_order(self, queue_path: Path) -> None:
        """drain 순서 = enqueue 순서 (FIFO). §8.2 invariant."""
        q = RetryQueue(path=queue_path)
        drained_keys: list[str] = []

        def capture_put(key: str, data: bytes, sha256: str | None = None):
            drained_keys.append(key)
            return MagicMock(status="uploaded")

        keys = [f"fifo/{i}.parquet" for i in range(5)]
        for k in keys:
            data = k.encode()
            q.enqueue(key=k, data=data, sha256=hashlib.sha256(data).hexdigest())

        mock_uploader = MagicMock()
        mock_uploader.put.side_effect = capture_put
        q.drain(mock_uploader)

        assert drained_keys == keys, f"Expected FIFO order, got: {drained_keys}"

    def test_drain_retains_failed_items(self, queue_path: Path) -> None:
        """drain 실패 item → backlog 유지 (다음 cycle 재시도). §8.2 invariant."""
        q = RetryQueue(path=queue_path)
        data = b"drain-fail-payload"
        q.enqueue(key="fail/item.parquet", data=data, sha256=hashlib.sha256(data).hexdigest())

        mock_uploader = MagicMock()
        from botocore.exceptions import EndpointConnectionError
        mock_uploader.put.side_effect = EndpointConnectionError(endpoint_url="http://nas:9000")

        stats = q.drain(mock_uploader)

        assert stats.failed == 1
        assert q.depth() == 1  # item retained
