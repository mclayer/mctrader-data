"""retry_queue.py — Persistent FIFO backlog for NAS MinIO cold tier uploader.

Story: MCT-150 (Stage 2 — uploader hardening)
Issue: mclayer/mctrader-hub#253
ADR: ADR-027 D5 (NAS unreachable failure mode — compactor retry queue + backlog alert)

Design decisions (§6.2.2 Change Plan 박제):
- sqlite-WAL mode + atomic transaction (FIX#1 F5 채택: RPO=0 정합, JSON-lines partial write risk 거부)
- enqueue contract: 단일 transaction 으로 segment metadata + payload reference insert
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
    """enqueue() 반환값."""

    status: str  # 'ok' | 'threshold_breached'
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

    S10 threshold (§6.2.2):
    - max_segments: 1000 (default) — ~50GB at 50MB/seg
    - max_bytes: 10GB (default) — NAS free space 압박 threshold

    Env: NAS_RETRY_QUEUE_PATH (default=/data/retry_queue/)
    DB: {path}/retry_queue.db
    Payloads: {path}/payloads/{uuid}.bin
    """

    def __init__(
        self,
        path: Path = _DEFAULT_PATH,
        max_segments: int = 1000,
        max_bytes: int = 10 * 1024**3,
        metrics=None,  # PrometheusExporter | None (순환 import 방지 — forward ref)
    ) -> None:
        self.path = Path(path)
        self.max_segments = max_segments
        self.max_bytes = max_bytes
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
        """현재 pending payload bytes 합계."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(payload_bytes), 0) FROM retry_queue WHERE state='pending'"
            ).fetchone()
            return int(row[0]) if row else 0

    def enqueue(self, key: str, data: bytes | Path, sha256: str) -> EnqueueResult:
        """Append segment to backlog.

        Threshold check 후 단일 transaction 으로 metadata + payload reference insert.
        Threshold breach → status='threshold_breached' (oldest quarantine 로직 포함).
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

            if current_count >= self.max_segments or current_bytes + payload_bytes > self.max_bytes:
                log.warning(
                    "[retry_queue] threshold breach: segments=%d/%d bytes=%d/%d — quarantining oldest",
                    current_count, self.max_segments, current_bytes, self.max_bytes,
                )
                self._quarantine_oldest_locked()
                if self._metrics:
                    depth = self._conn.execute(
                        "SELECT COUNT(*) FROM retry_queue WHERE state='pending'"
                    ).fetchone()[0]
                    self._metrics.set_queue_depth(queue_path=str(self.path), depth=depth)
                return EnqueueResult(status="threshold_breached")

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
        """FIFO drain — each pending item → uploader.put() retry.

        Success → remove from backlog; failure → retain (next drain cycle).
        Rate-limit: exponential backoff on ThrottlingException (initial=1s, max=60s, factor=2).
        """
        import time

        with self._lock:
            rows = self._conn.execute(
                "SELECT id, key, sha256, payload_file FROM retry_queue WHERE state='pending' ORDER BY enqueue_ts"
            ).fetchall()

        stats = DrainStats()
        backoff = 1.0

        for item_id, key, sha256, payload_file in rows:
            payload_path = self._payload_dir / payload_file
            if not payload_path.exists():
                log.warning("[retry_queue] payload missing for id=%s key=%s — skipping", item_id, key)
                stats.skipped += 1
                continue

            data = payload_path.read_bytes()
            try:
                uploader.put(key=key, data=data, sha256=sha256)
                # success → remove
                with self._lock, self._conn:
                    self._conn.execute("DELETE FROM retry_queue WHERE id=?", (item_id,))
                payload_path.unlink(missing_ok=True)
                stats.drained += 1
                backoff = 1.0  # reset on success
                log.info("[retry_queue] drained key=%s id=%s", key, item_id)
            except Exception as exc:
                from botocore.exceptions import ClientError
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

        if self._metrics:
            self._metrics.set_queue_depth(queue_path=str(self.path), depth=self.depth())

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
