# src/mctrader_data/wal/ingester.py
"""Per-symbol-channel WAL writer (append-only, O_APPEND + fsync)."""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from .ndjson_codec import encode_record
from .segment import active_segment_path, seal_path, segment_index


class WalIngester:
    """Writes one NDJSON line per event to a WAL file opened with O_APPEND.

    Segment boundary: every `segment_seconds` (default 300 = 5 min).
    On boundary cross, atomic rename .ndjson -> .ndjson.sealed.
    close() does final fsync + seal.
    """

    def __init__(
        self,
        *,
        root: Path,
        exchange: str,
        symbol: str,
        channel: str,
        node_id: str,
        fsync_batch: int = 1,
        segment_seconds: int = 300,
    ) -> None:
        self._root = root
        self._exchange = exchange
        self._symbol = symbol
        self._channel = channel
        self._node_id = node_id
        self._fsync_batch = fsync_batch
        self._segment_seconds = segment_seconds
        self._lock = threading.Lock()
        self._fd: int | None = None
        self._current_path: Path | None = None
        self._segment_start_idx: int = 0
        self._write_count: int = 0
        self._closed: bool = False
        self._open_new_segment()

    # ------------------------------------------------------------------ public

    def append(self, record: dict) -> None:
        # ADR-018 D5: O_APPEND+fsync 패턴 (tmp-rename 불필요한 이유):
        #   1) 각 NDJSON 라인은 완결된 레코드 — encode_record() 가 newline 보장.
        #   2) threading.Lock 이 단일 writer 직렬화 → write 후 fsync 가 충분.
        #   3) tmp-rename 은 세그먼트 봉인(seal) 시에만 사용 (_seal_current / close).
        if self._closed:
            raise RuntimeError("WalIngester is closed")
        with self._lock:
            self.maybe_seal()
            line = encode_record(record).encode("utf-8")
            assert self._fd is not None
            os.write(self._fd, line)
            self._write_count += 1
            if self._write_count % self._fsync_batch == 0:
                os.fsync(self._fd)

    def maybe_seal(self) -> Path | None:
        """Seal current segment if wall-clock has crossed segment boundary."""
        now_idx = segment_index(time.time(), self._segment_seconds)
        if now_idx > self._segment_start_idx:
            sealed = self._seal_current()
            self._open_new_segment(start_idx=now_idx)
            return sealed
        return None

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            if self._fd is not None:
                os.fsync(self._fd)
                os.close(self._fd)
                self._fd = None
            if self._current_path is not None and self._current_path.exists():
                sealed = seal_path(self._current_path)
                os.replace(str(self._current_path), str(sealed))
                self._current_path = None
            self._closed = True

    # ----------------------------------------------------------------- private

    def _open_new_segment(self, start_idx: int | None = None) -> None:
        if start_idx is None:
            start_idx = segment_index(time.time(), self._segment_seconds)
        self._segment_start_idx = start_idx
        start_ts = start_idx * self._segment_seconds
        dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        date_str = dt.strftime("%Y-%m-%d")
        path = active_segment_path(
            root=self._root,
            exchange=self._exchange,
            channel=self._channel,
            symbol=self._symbol,
            date=date_str,
            start_idx=start_idx,
            node_id=self._node_id,
            segment_seconds=self._segment_seconds,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        self._current_path = path
        self._fd = os.open(
            str(path),
            flags=os.O_WRONLY | os.O_APPEND | os.O_CREAT,
            mode=0o640,
        )
        self._write_count = 0

    def _seal_current(self) -> Path:
        assert self._fd is not None
        os.fsync(self._fd)
        os.close(self._fd)
        self._fd = None
        assert self._current_path is not None
        sealed = seal_path(self._current_path)
        os.replace(str(self._current_path), str(sealed))
        self._current_path = None
        return sealed
