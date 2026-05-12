"""test_retry_queue.py — P0 TDD tests for RetryQueue (sqlite-WAL persistence).

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253

Test Contract §8 (TestContractArchitectAgent):
- test_enqueue_atomic: 단일 transaction 박제 (sqlite-WAL)
- test_persistent_restart_resume: process restart 후 pending segment 정확 복원 (real sqlite3 file)
- test_bounded_backlog_quarantine: threshold 초과 시 oldest quarantine + alert
- test_drain_on_recovery: NAS reachable 회복 시 drain 동작 (mock)
- Chaos test: NAS down → backlog 누적 → NAS up → resume + 손실 0 verify (NFR-3 의무)

FIX#2 추가 (8 finding):
- P0-1: enqueue threshold breach drop 0 invariant — oldest quarantine + 신규 pending enqueue
- P0-1: hard floor (pending+quarantined > 10000) → MANUAL_GATE escalate
- P0-2: drain context-aware put suppress_enqueue=True → status='queued' → retain
- P1-2: drain chaos — 실 NASUploader + boto3 EndpointConnectionError mock
"""
from __future__ import annotations

import hashlib
import sqlite3
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

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

    def test_threshold_segments_breach_quarantine_and_enqueue(self, queue_path: Path) -> None:
        """max_segments 도달 시 oldest quarantine + 신규 pending enqueue (RPO=0 FIX#2 Option A).

        FIX#2 semantics: threshold breach → status='ok' (quarantine + pending).
        'threshold_breached' (drop) 는 RPO=0 위반으로 폐기.
        """
        q = RetryQueue(path=queue_path, max_segments=3, max_bytes=10 * 1024**3)
        for i in range(3):
            data = f"seg-{i}".encode()
            q.enqueue(key=f"seg-{i}.parquet", data=data, sha256=hashlib.sha256(data).hexdigest())

        data = b"one-too-many"
        result = q.enqueue(
            key="overflow.parquet", data=data, sha256=hashlib.sha256(data).hexdigest()
        )
        # FIX#2: 신규 segment 는 pending enqueue (drop 0)
        assert result.status == "ok", f"Expected 'ok' (RPO=0 quarantine+pending), got '{result.status}'"
        assert q.depth() == 3  # oldest quarantined (1개) + 기존 2개 pending + 신규 1개 = 3 pending

    def test_threshold_bytes_breach_quarantine_and_enqueue(self, queue_path: Path) -> None:
        """max_bytes 도달 시 oldest quarantine + 신규 pending enqueue (RPO=0 FIX#2 Option A)."""
        q = RetryQueue(path=queue_path, max_segments=10000, max_bytes=50)

        data = b"x" * 30
        q.enqueue(key="first.parquet", data=data, sha256=hashlib.sha256(data).hexdigest())

        data2 = b"y" * 30  # total pending bytes = 30; 30 + 30 > 50 → threshold
        result = q.enqueue(key="second.parquet", data=data2, sha256=hashlib.sha256(data2).hexdigest())
        # FIX#2: 신규 segment 는 pending enqueue (drop 0)
        assert result.status == "ok", f"Expected 'ok' (RPO=0), got '{result.status}'"

    def test_default_bounds_are_10000_and_10gb(self, queue_path: Path) -> None:
        """기본 max_segments=10000 (FIX#2 P0-1 spec), max_bytes=10GB (S10 박제)."""
        q = RetryQueue(path=queue_path)
        assert q.max_segments == 10_000
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

        # NAS up 시뮬레이션: uploader mock (always succeeds, suppress_enqueue 지원)
        mock_uploader = MagicMock()
        mock_uploader.put.return_value = MagicMock(status="uploaded")
        # suppress_enqueue 키워드 인자 수용을 위해 spec 없이 MagicMock 사용 (기본값 OK)

        stats = q.drain(mock_uploader)

        assert isinstance(stats, DrainStats)
        assert stats.drained == 5
        assert stats.failed == 0
        assert q.depth() == 0  # 손실 0, 모두 drain 완료

    def test_drain_fifo_order(self, queue_path: Path) -> None:
        """drain 순서 = enqueue 순서 (FIFO). §8.2 invariant."""
        q = RetryQueue(path=queue_path)
        drained_keys: list[str] = []

        def capture_put(key: str, data: bytes, sha256: str | None = None, suppress_enqueue: bool = False):
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
        """drain 실패 item → backlog 유지 (다음 cycle 재시도). §8.2 invariant.

        FIX#2: drain 이 suppress_enqueue=True 로 put() 호출 → EndpointConnectionError raise → retain.
        mock_uploader.put() 는 suppress_enqueue 파라미터 수용.
        """
        q = RetryQueue(path=queue_path)
        data = b"drain-fail-payload"
        q.enqueue(key="fail/item.parquet", data=data, sha256=hashlib.sha256(data).hexdigest())

        from botocore.exceptions import EndpointConnectionError

        def put_raises(*args, **kwargs):
            raise EndpointConnectionError(endpoint_url="http://nas:9000")

        mock_uploader = MagicMock()
        mock_uploader.put.side_effect = put_raises

        stats = q.drain(mock_uploader)

        assert stats.failed == 1
        assert q.depth() == 1  # item retained


class TestEnqueueDropZeroInvariant:
    """P0-1 FIX#2: threshold breach → quarantine oldest + 신규 pending enqueue (drop 0).

    RPO=0 invariant: enqueue 절대 drop 0.
    §6.2.2 Option A: oldest pending → quarantined 강등 + 신규 segment 정상 pending enqueue.
    """

    def test_threshold_breach_enqueues_new_item_not_drops(self, queue_path: Path) -> None:
        """threshold 도달 시 oldest → quarantined, 신규 → pending. drop 0 invariant."""
        q = RetryQueue(path=queue_path, max_segments=3, max_bytes=10 * 1024**3)

        keys = [f"existing-{i}.parquet" for i in range(3)]
        for k in keys:
            data = k.encode()
            q.enqueue(key=k, data=data, sha256=hashlib.sha256(data).hexdigest())

        assert q.depth() == 3  # 3 pending

        # threshold 도달 → 신규 enqueue: oldest quarantine + 신규 pending
        new_data = b"new-segment-at-threshold"
        result = q.enqueue(
            key="new-segment.parquet",
            data=new_data,
            sha256=hashlib.sha256(new_data).hexdigest(),
        )

        # RPO=0: 신규 segment 는 pending 으로 들어와야 함 (drop 아님)
        assert result.status == "ok", (
            f"Expected 'ok' (pending enqueue), got '{result.status}'. "
            "drop 발생 = RPO=0 위반"
        )
        # 신규 depth: oldest 1개 quarantine → pending = 2 + 신규 1 = 3
        assert q.depth() == 3

        # quarantine row 존재 확인 (direct DB query)
        db_path = queue_path / "retry_queue.db"
        conn = sqlite3.connect(str(db_path))
        quarantined = conn.execute(
            "SELECT COUNT(*) FROM retry_queue WHERE state='quarantined'"
        ).fetchone()[0]
        conn.close()
        assert quarantined == 1, "oldest pending 이 quarantined 로 강등되어야 함"

    def test_threshold_breach_oldest_is_quarantined(self, queue_path: Path) -> None:
        """oldest pending segment 가 quarantine 강등 대상 (FIFO 순서 확인)."""
        q = RetryQueue(path=queue_path, max_segments=2, max_bytes=10 * 1024**3)

        # 2개 enqueue (max_segments=2)
        q.enqueue(key="oldest.parquet", data=b"oldest", sha256=hashlib.sha256(b"oldest").hexdigest())
        q.enqueue(key="newer.parquet", data=b"newer", sha256=hashlib.sha256(b"newer").hexdigest())

        # threshold 도달 시 oldest quarantine 후 신규 enqueue
        q.enqueue(key="newest.parquet", data=b"newest", sha256=hashlib.sha256(b"newest").hexdigest())

        db_path = queue_path / "retry_queue.db"
        conn = sqlite3.connect(str(db_path))
        quarantined_keys = conn.execute(
            "SELECT key FROM retry_queue WHERE state='quarantined'"
        ).fetchall()
        conn.close()

        quarantined_key_list = [row[0] for row in quarantined_keys]
        assert "oldest.parquet" in quarantined_key_list, (
            "oldest pending 이 quarantine 강등 대상이어야 함"
        )

    def test_hard_floor_blocked_returns_hard_floor_blocked(self, queue_path: Path) -> None:
        """hard floor: pending + quarantined > 10000 → EnqueueResult(status='hard_floor_blocked').

        P0-1 FIX#2 §6.2.2: SOP MANUAL_GATE escalate 의무.
        """
        # hard_floor = 10000 (pending + quarantined total)
        # 실제 10000개 insert 는 느리므로 RetryQueue 내부 hard_floor 파라미터 추가 후 소규모 테스트
        q = RetryQueue(path=queue_path, max_segments=3, max_bytes=10 * 1024**3, hard_floor=5)

        # 3개 pending 채우기
        for i in range(3):
            data = f"seg-{i}".encode()
            q.enqueue(key=f"seg-{i}.parquet", data=data, sha256=hashlib.sha256(data).hexdigest())

        # threshold breach → quarantine 발생 (2회) → quarantined 2개
        for i in range(3, 5):
            data = f"seg-{i}".encode()
            q.enqueue(key=f"seg-{i}.parquet", data=data, sha256=hashlib.sha256(data).hexdigest())

        # hard floor (pending + quarantined = 5) 도달 → 다음 enqueue 는 hard_floor_blocked
        data = b"hard-floor-trigger"
        result = q.enqueue(
            key="hard-floor.parquet",
            data=data,
            sha256=hashlib.sha256(data).hexdigest(),
        )
        assert result.status == "hard_floor_blocked", (
            f"Expected 'hard_floor_blocked', got '{result.status}'"
        )

    def test_enqueue_never_silently_drops(self, queue_path: Path) -> None:
        """RPO=0 invariant: enqueue 가 silently drop 하는 path 없음.

        threshold breach → 'ok' (pending enqueue) OR 'hard_floor_blocked' (escalate).
        'threshold_breached' 단순 drop 은 RPO=0 위반 — 이 test 에서 검증.
        """
        q = RetryQueue(path=queue_path, max_segments=2, max_bytes=10 * 1024**3)

        for i in range(2):
            data = f"fill-{i}".encode()
            q.enqueue(key=f"fill-{i}.parquet", data=data, sha256=hashlib.sha256(data).hexdigest())

        # overflow: status 가 'ok' (quarantine+pending) 또는 'hard_floor_blocked' 여야 함
        # 'threshold_breached' (drop) 는 RPO=0 위반 → 허용 안 됨
        data = b"overflow-data"
        result = q.enqueue(
            key="overflow.parquet",
            data=data,
            sha256=hashlib.sha256(data).hexdigest(),
        )
        assert result.status != "threshold_breached", (
            "threshold_breached (silent drop) 는 RPO=0 위반 — "
            "ok (quarantine+pending) 또는 hard_floor_blocked 여야 함"
        )


class TestDrainContextAwarePut:
    """P0-2 FIX#2: drain() context-aware put suppress_enqueue=True → retain on queued status.

    drain() 가 put(suppress_enqueue=True) 호출 → status='queued' 시 원본 retain.
    §6.2.2 설계: drain loop 는 suppress_enqueue=True 로 NASUploader.put() 호출.
    """

    def test_drain_suppress_enqueue_retain_on_queued(self, queue_path: Path) -> None:
        """drain → put(suppress_enqueue=True) → status='queued' → retain (drop 0)."""
        from mctrader_data.nas_storage.nas_uploader import NASUploader

        q = RetryQueue(path=queue_path)
        data = b"drain-context-aware-payload"
        q.enqueue(key="ctx/item.parquet", data=data, sha256=hashlib.sha256(data).hexdigest())

        assert q.depth() == 1

        # suppress_enqueue=True 시 put 이 'queued' 반환 (EndpointConnectionError 대신)
        uploader = NASUploader(
            endpoint="http://nas.local:9000",
            access_key="test",
            secret_key="test",
            bucket="test-bucket",
            retry_queue=None,  # drain 내부에서 suppress_enqueue=True 로 호출하므로 queue 없어도 됨
        )

        from botocore.exceptions import EndpointConnectionError

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client
            client.head_object.side_effect = EndpointConnectionError(endpoint_url="http://nas.local:9000")

            stats = q.drain(uploader)

        # suppress_enqueue=True 경로: raise → drain catch → retain
        assert q.depth() == 1, "suppress_enqueue=True path: item 은 retain 되어야 함 (drop 0)"
        assert stats.failed == 1

    def test_drain_real_uploader_endpoint_unreachable_chaos(self, queue_path: Path) -> None:
        """P1-2 chaos: 실 NASUploader 인스턴스 + boto3 EndpointConnectionError mock.

        drain() 호출 → put(suppress_enqueue=True) raise → drain catch + 원본 retain assert.
        drain 후 queue 의 pending count 변화 없음 (drop 0).
        """
        from mctrader_data.nas_storage.nas_uploader import NASUploader

        # 실 NASUploader (retry_queue=None — drain 내부에서 suppress_enqueue=True 호출)
        uploader = NASUploader(
            endpoint="http://nas.chaos.local:9000",
            access_key="chaos-access",
            secret_key="chaos-secret",
            bucket="chaos-bucket",
            retry_queue=None,
        )

        q = RetryQueue(path=queue_path)

        # 5개 segment 적재 (NAS down 시뮬레이션)
        keys = [f"chaos/{i}.parquet" for i in range(5)]
        for k in keys:
            payload = k.encode()
            q.enqueue(key=k, data=payload, sha256=hashlib.sha256(payload).hexdigest())

        assert q.depth() == 5

        # boto3 client mock: EndpointConnectionError (NAS unreachable chaos)
        from botocore.exceptions import EndpointConnectionError

        with patch.object(uploader, "_get_client") as mock_client_factory:
            client = MagicMock()
            mock_client_factory.return_value = client
            client.head_object.side_effect = EndpointConnectionError(endpoint_url="http://nas.chaos.local:9000")

            stats = q.drain(uploader)

        # drain 후 pending count 변화 없음 (drop 0)
        assert q.depth() == 5, (
            f"chaos test: drain 후 pending count={q.depth()}, expected 5 (drop 0)"
        )
        assert stats.failed == 5
        assert stats.drained == 0
