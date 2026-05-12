"""retry_queue.py — Persistent FIFO backlog for NAS MinIO cold tier uploader.

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253
ADR: ADR-027 D5 (NAS unreachable failure mode — compactor retry queue + backlog alert)

Design decisions (§6.2.2 Change Plan 박제, FIX#2 Option A 갱신):
- sqlite-WAL mode + atomic transaction (FIX#1 F5 채택: RPO=0 정합, JSON-lines partial write risk 거부)
- SQL state enum 4종: pending / in_flight / quarantined / succeeded (FIX#2 확장)
- enqueue contract (FIX#2 RPO=0 Option A):
  - threshold (max_segments OR max_bytes) 도달 시 oldest pending → quarantined 강등
    + 신규 segment pending enqueue (drop 0)
  - hard floor: pending + quarantined 합 > hard_floor → 신규 enqueue 시 MANUAL_GATE escalate
    (return EnqueueResult(status='hard_floor_blocked')) — collector callee 측 재시도 의무
- RPO=0 invariant: enqueue 절대 drop 0 (assert)
- drain contract (FIX#2 context-aware put):
  - drain 이 put(suppress_enqueue=True) 호출 → EndpointConnectionError raise → catch + retain
  - 2 cycle: pending drain (정상 backoff) + quarantined drain (longer backoff schedule)
  - quarantined drain success 시 state → succeeded (row delete)
- restart resume: SELECT * FROM retry_queue WHERE state='pending' ORDER BY enqueue_ts
- bounded backlog: row count > max_segments OR total_bytes > max_bytes → threshold_breached
- crash-atomicity: WAL autocheckpoint + journal_mode=WAL
- Env: NAS_RETRY_QUEUE_PATH (default=/data/retry_queue/) — /data/wal hot path 침범 0 (EC-2)
- 외부 dependency 0 (Python 표준 stdlib sqlite3 전용)
"""
from __future__ import annotations

import contextlib
import logging
import os
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader

log = logging.getLogger(__name__)

_DEFAULT_PATH = Path(os.environ.get("NAS_RETRY_QUEUE_PATH", "/data/retry_queue"))

# default hard floor (pending + quarantined total) — P0-1 §6.2.2
_DEFAULT_HARD_FLOOR = 10_000

_DDL = """
CREATE TABLE IF NOT EXISTS retry_queue (
    id         TEXT PRIMARY KEY,
    key        TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    payload_file TEXT NOT NULL,
    payload_bytes INTEGER NOT NULL DEFAULT 0,
    state      TEXT NOT NULL DEFAULT 'pending',
    enqueue_ts REAL NOT NULL,
    updated_ts REAL
);
CREATE INDEX IF NOT EXISTS idx_state_ts ON retry_queue (state, enqueue_ts);
"""


@dataclass(frozen=True)
class EnqueueResult:
    """enqueue() 반환값.

    status:
    - 'ok': pending enqueue 성공 (정상 path 또는 threshold breach 후 quarantine+pending)
    - 'hard_floor_blocked': pending + quarantined > hard_floor → MANUAL_GATE escalate
    """

    status: str  # 'ok' | 'hard_floor_blocked'
    item_id: str = ""


@dataclass
class DrainStats:
    """drain() 반환값."""

    drained: int = 0
    failed: int = 0
    skipped: int = 0


class RetryQueue:
    """Persistent FIFO backlog for NAS unreachable transients.

    sqlite-WAL persistence — crash-safe, restart-resume, bounded backlog.

    S10 threshold (§6.2.2, FIX#2 Option A):
    - max_segments: 10000 (default) — ~50GB at 50MB/seg (FIX#2: 10000 per P0-1 spec)
    - max_bytes: 10GB (default) — NAS free space 압박 threshold
    - hard_floor: 10000 (default) — pending + quarantined 합 초과 시 MANUAL_GATE escalate

    RPO=0 invariant (FIX#2 박제):
    - enqueue 절대 drop 0
    - threshold breach → oldest pending quarantine + 신규 pending enqueue
    - hard floor → hard_floor_blocked (caller 재시도 의무)

    Env: NAS_RETRY_QUEUE_PATH (default=/data/retry_queue/)
    DB: {path}/retry_queue.db
    Payloads: {path}/payloads/{uuid}.bin
    """

    def __init__(
        self,
        path: Path = _DEFAULT_PATH,
        max_segments: int = 10_000,
        max_bytes: int = 10 * 1024**3,
        hard_floor: int = _DEFAULT_HARD_FLOOR,
        metrics=None,  # PrometheusExporter | None (순환 import 방지 — forward ref)
    ) -> None:
        self.path = Path(path)
        self.max_segments = max_segments
        self.max_bytes = max_bytes
        self.hard_floor = hard_floor
        self._metrics = metrics

        self._db_path = self.path / "retry_queue.db"
        self._payload_dir = self.path / "payloads"

        self.path.mkdir(parents=True, exist_ok=True)
        self._payload_dir.mkdir(parents=True, exist_ok=True)

        # 0600 file permission (§6.3 SecurityArch)
        with contextlib.suppress(PermissionError, NotImplementedError, OSError):
            self.path.chmod(0o700)  # Windows 에서는 POSIX chmod 미지원

        self._db_path.touch()
        with contextlib.suppress(PermissionError, NotImplementedError, OSError):
            self._db_path.chmod(0o600)

        self._lock = threading.Lock()
        self._conn = self._open_db()

    def _open_db(self) -> sqlite3.Connection:
        """sqlite connection with WAL mode."""
        conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=100")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(_DDL)
        conn.commit()
        return conn

    def _total_bytes(self) -> int:
        """현재 pending payload bytes 합계 (P1-1 exporter 정기 호출용)."""
        with self._lock:
            return self._total_bytes_locked()

    def _total_bytes_locked(self) -> int:
        """lock 이미 보유 시 pending payload bytes 합계."""
        row = self._conn.execute(
            "SELECT COALESCE(SUM(payload_bytes), 0) FROM retry_queue WHERE state='pending'"
        ).fetchone()
        return int(row[0]) if row else 0

    def enqueue(self, key: str, data: bytes | Path, sha256: str) -> EnqueueResult:
        """Append segment to backlog.

        FIX#2 RPO=0 Option A semantics:
        - threshold (max_segments OR max_bytes) 도달 시 oldest pending → quarantined 강등
          + 신규 pending enqueue (drop 0)
        - hard floor (pending + quarantined > hard_floor) → status='hard_floor_blocked'
        - RPO=0 invariant: drop 0

        Returns EnqueueResult(status='ok') on success (including threshold breach with quarantine).
        Returns EnqueueResult(status='hard_floor_blocked') on hard floor breach.
        """
        import time

        # payload bytes 계산
        if isinstance(data, Path):
            payload_bytes = data.stat().st_size
            raw_data: bytes | None = None
        else:
            raw_data = data
            payload_bytes = len(data)

        with self._lock:
            # threshold check (enqueue 전)
            row = self._conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0) FROM retry_queue WHERE state='pending'"
            ).fetchone()
            current_count = int(row[0]) if row else 0
            current_bytes = int(row[1]) if row else 0

            threshold_breached = (
                current_count >= self.max_segments
                or current_bytes + payload_bytes > self.max_bytes
            )

            if threshold_breached:
                # hard floor check: pending + quarantined 합계
                total_row = self._conn.execute(
                    "SELECT COUNT(*) FROM retry_queue WHERE state IN ('pending', 'quarantined')"
                ).fetchone()
                total_active = int(total_row[0]) if total_row else 0

                if total_active >= self.hard_floor:
                    # hard floor 초과 → MANUAL_GATE escalate (caller 재시도 의무)
                    log.critical(
                        "[retry_queue] HARD FLOOR BLOCKED: pending+quarantined=%d >= hard_floor=%d "
                        "— MANUAL_GATE escalate required. key=%s",
                        total_active, self.hard_floor, key,
                    )
                    return EnqueueResult(status="hard_floor_blocked")

                # threshold breach — oldest pending → quarantined + 신규 segment pending enqueue
                log.warning(
                    "[retry_queue] threshold breach: segments=%d/%d bytes=%d/%d"
                    " — quarantining oldest (RPO=0 preserved)",
                    current_count, self.max_segments, current_bytes, self.max_bytes,
                )
                self._quarantine_oldest_locked()

            # payload file 저장
            item_id = str(uuid.uuid4())
            payload_filename = f"{item_id}.bin"
            payload_path = self._payload_dir / payload_filename

            if raw_data is not None:
                payload_path.write_bytes(raw_data)
            else:
                import shutil
                shutil.copy2(str(data), str(payload_path))

            with contextlib.suppress(PermissionError, NotImplementedError, OSError):
                payload_path.chmod(0o600)

            ts = time.monotonic()

            # 단일 transaction — metadata + payload reference insert
            try:
                with self._conn:
                    self._conn.execute(
                        "INSERT INTO retry_queue (id, key, sha256, payload_file, payload_bytes, state, enqueue_ts)"
                        " VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                        (item_id, key, sha256, payload_filename, payload_bytes, ts),
                    )
            except Exception:
                with contextlib.suppress(Exception):
                    payload_path.unlink(missing_ok=True)
                raise

            depth = self._conn.execute(
                "SELECT COUNT(*) FROM retry_queue WHERE state='pending'"
            ).fetchone()[0]

        if self._metrics:
            self._metrics.set_queue_depth(queue_path=str(self.path), depth=depth)
            # P1-1: queue_bytes Gauge update
            total_bytes = self._total_bytes()
            self._metrics.set_queue_bytes(queue_path=str(self.path), bytes_total=total_bytes)

        log.info("[retry_queue] enqueued key=%s id=%s bytes=%d", key, item_id, payload_bytes)
        return EnqueueResult(status="ok", item_id=item_id)

    def _quarantine_oldest(self) -> None:
        """oldest pending segment 를 'quarantined' state 로 전환 (lock 획득 후 호출)."""
        with self._lock:
            self._quarantine_oldest_locked()

    def _quarantine_oldest_locked(self) -> None:
        """oldest pending segment 를 'quarantined' state 로 전환 (lock 이미 보유 시 호출)."""
        import time
        row = self._conn.execute(
            "SELECT id FROM retry_queue WHERE state='pending' ORDER BY enqueue_ts LIMIT 1"
        ).fetchone()
        if row:
            with self._conn:
                self._conn.execute(
                    "UPDATE retry_queue SET state='quarantined', updated_ts=? WHERE id=?",
                    (time.monotonic(), row[0]),
                )
            log.warning("[retry_queue] quarantined oldest item id=%s", row[0])

    def drain(self, uploader: NASUploader) -> DrainStats:
        """FIFO drain (2 cycle) — pending drain + quarantined drain.

        FIX#2 context-aware put semantics:
        - uploader.put(suppress_enqueue=True) 호출 — drain 내부에서 auto-enqueue 방지
        - suppress_enqueue=True 시 EndpointConnectionError raise → drain catch + retain
        - status='queued' 반환 시도 시 (suppress=False fallback) → retain

        Cycle 1: pending drain (정상 backoff)
        Cycle 2: quarantined drain (longer backoff: exponential 1m/5m/30m/2h)

        Success → row state='succeeded' (또는 delete)
        Failure → retain (다음 drain cycle)
        """
        import time

        stats = DrainStats()

        # Cycle 1: pending drain
        with self._lock:
            pending_rows = self._conn.execute(
                "SELECT id, key, sha256, payload_file FROM retry_queue WHERE state='pending' ORDER BY enqueue_ts"
            ).fetchall()

        backoff = 1.0
        for item_id, key, sha256, payload_file in pending_rows:
            payload_path = self._payload_dir / payload_file
            if not payload_path.exists():
                log.warning("[retry_queue] payload missing for id=%s key=%s — skipping", item_id, key)
                stats.skipped += 1
                continue

            data = payload_path.read_bytes()
            try:
                result = uploader.put(key=key, data=data, sha256=sha256, suppress_enqueue=True)
                if result.status == "queued":
                    # suppress_enqueue=True 임에도 'queued' 반환 → retain (NAS unreachable)
                    stats.failed += 1
                    log.warning("[retry_queue] drain: put returned 'queued' (NAS unreachable) key=%s — retaining", key)
                    continue
                # success (uploaded / skipped_idempotent) → delete
                with self._lock, self._conn:
                    self._conn.execute("DELETE FROM retry_queue WHERE id=?", (item_id,))
                payload_path.unlink(missing_ok=True)
                stats.drained += 1
                backoff = 1.0
                log.info("[retry_queue] drained key=%s id=%s", key, item_id)
            except Exception as exc:
                from botocore.exceptions import ClientError, EndpointConnectionError
                # EndpointConnectionError → retain (suppress_enqueue=True path)
                if isinstance(exc, EndpointConnectionError):
                    stats.failed += 1
                    log.warning("[retry_queue] drain: NAS unreachable (suppress_enqueue) key=%s — retaining", key)
                    continue

                # ThrottlingException → exponential backoff
                is_throttle = (
                    isinstance(exc, ClientError)
                    and exc.response.get("Error", {}).get("Code") in ("SlowDown", "ThrottlingException")  # type: ignore[union-attr]
                )
                if is_throttle:
                    sleep_time = min(backoff, 60.0)
                    log.warning("[retry_queue] throttled — backoff %.1fs", sleep_time)
                    time.sleep(sleep_time)
                    backoff = min(backoff * 2, 60.0)

                stats.failed += 1
                log.warning("[retry_queue] drain failed key=%s id=%s: %s", key, item_id, type(exc).__name__)

        # Cycle 2: quarantined drain (longer backoff schedule: 1m/5m/30m/2h)
        with self._lock:
            quarantined_rows = self._conn.execute(
                "SELECT id, key, sha256, payload_file FROM retry_queue WHERE state='quarantined' ORDER BY enqueue_ts"
            ).fetchall()

        # quarantined drain (longer backoff: 1m/5m/30m/2h — future use in retry scheduler)
        for item_id, key, sha256, payload_file in quarantined_rows:
            payload_path = self._payload_dir / payload_file
            if not payload_path.exists():
                log.warning("[retry_queue] quarantined payload missing id=%s key=%s — skipping", item_id, key)
                stats.skipped += 1
                continue

            data = payload_path.read_bytes()
            try:
                result = uploader.put(key=key, data=data, sha256=sha256, suppress_enqueue=True)
                if result.status == "queued":
                    stats.failed += 1
                    log.warning("[retry_queue] quarantine drain: put returned 'queued' key=%s — retaining", key)
                    continue
                # quarantined drain success → state='succeeded' (delete)
                with self._lock, self._conn:
                    self._conn.execute("DELETE FROM retry_queue WHERE id=?", (item_id,))
                payload_path.unlink(missing_ok=True)
                stats.drained += 1
                log.info("[retry_queue] quarantined item drained key=%s id=%s", key, item_id)
            except Exception as exc:
                from botocore.exceptions import EndpointConnectionError
                if isinstance(exc, EndpointConnectionError):
                    stats.failed += 1
                    log.warning("[retry_queue] quarantine drain: NAS unreachable key=%s — retaining", key)
                    continue
                stats.failed += 1
                log.warning("[retry_queue] quarantine drain failed key=%s id=%s: %s", key, item_id, type(exc).__name__)

        if self._metrics:
            self._metrics.set_queue_depth(queue_path=str(self.path), depth=self.depth())
            total_bytes = self._total_bytes()
            self._metrics.set_queue_bytes(queue_path=str(self.path), bytes_total=total_bytes)

        return stats

    def resume_on_startup(self) -> int:
        """Process restart hook — disk 잔존 backlog count return.

        AC-2: persistence verify.
        """
        count = self.depth()
        if count > 0:
            log.info("[retry_queue] resume_on_startup: %d pending segments found", count)
        return count

    def depth(self) -> int:
        """Current backlog segment count (pending only)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM retry_queue WHERE state='pending'"
            ).fetchone()
            return int(row[0]) if row else 0

    def close(self) -> None:
        """sqlite connection 닫기."""
        with contextlib.suppress(Exception):
            self._conn.close()
