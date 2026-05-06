"""Tier-별 logical key extractor + node priority + content mismatch detector.

Per MCT-92 Phase 3 (X3 of MCT-89). Transparent read-side dedup for active-active HA.
MCT-93 Phase 4 (X4): persist_quarantine_records helper for caller-side artifact write.

Architect 결정 freeze (plan §"Architect 결정 8항"):
- T1 hybrid late correction: received_at MAX + tie-break node priority alphabetical
- DEFAULT sentinel: zzz_DEFAULT (ASCII order 끝, post-HA partition 우선)
- Quarantine artifact: root manifest <root>/market/manifest/quarantine/...
- Multi-node mode 자동 감지 (distinct node= ≥ 2)
- Streaming dedup window: 200ms safety margin (ms-tolerance ±100ms × 2)
- dedup.py flat module 위치 (단일 책임 + test isolation)
- Quarantine backpressure: per-second 100 cap + batching (drop 방지)

Contract enforcement:
- ADR-009 §D5 T1 4-key + late correction
- ADR-009 §D10.7 T2 6-tuple fallback (Bithumb tx_id 부재)
- ADR-009 §D11.8 T3 8-tuple fallback best-effort
- heartbeat-schema.v1 metrics.dup_skip_count / quarantine_count emit hook (DedupCounterSink protocol)
- heartbeat-schema.v1 §Related Manifest Artifacts (X4 amendment) — quarantine artifact path/payload
"""
from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

# Architect 결정 #2 — DEFAULT sentinel (ASCII end, alphabetical priority 정합)
NODE_PRIORITY_DEFAULT_SENTINEL = "zzz_DEFAULT"

# Architect 결정 #5 — Streaming dedup window (200ms = ms-tolerance ±100ms × 2)
DEDUP_WINDOW_MS = 200

# Architect 결정 #8 — Quarantine backpressure (per-second 100 mismatch cap → batching)
QUARANTINE_RATE_LIMIT_PER_SEC = 100


# ----------------------------------------------------------------------
# Logical key extractors (ADR-009 §D5 / §D10.7 / §D11.8 enforcement)
# ----------------------------------------------------------------------

def candle_logical_key(row: Any) -> tuple[str, str, str, datetime]:
    """ADR-009 §D5 T1 — (exchange, symbol, timeframe, ts_utc) 4-key."""
    return (
        row.exchange,
        str(row.symbol),
        row.timeframe.value if hasattr(row.timeframe, "value") else str(row.timeframe),
        row.ts_utc,
    )


def tick_logical_key(
    row: Any,
) -> tuple[str, str, datetime, Decimal, Decimal, str]:
    """ADR-009 §D10.7 T2 — fallback 6-tuple (Bithumb tx_id 부재).

    `(exchange, symbol, ts_utc, price, quantity, side)`
    """
    return (
        row.exchange,
        str(row.symbol),
        row.ts_utc,
        row.price,
        row.quantity,
        row.side,
    )


def orderbook_logical_key(
    row: Any,
) -> tuple[str, str, datetime, str, str, int, Decimal, Decimal]:
    """ADR-009 §D11.8 T3 — fallback 8-tuple best-effort (Bithumb sequence_id 부재).

    `(exchange, symbol, ts_utc, event_type, side, level, price, quantity)`
    """
    return (
        row.exchange,
        str(row.symbol),
        row.ts_utc,
        row.event_type,
        row.side,
        row.level,
        row.price,
        row.quantity,
    )


# ----------------------------------------------------------------------
# Node priority (Architect 결정 #2 — alphabetical, zzz_DEFAULT for legacy)
# ----------------------------------------------------------------------

def node_priority(node_id: str | None) -> str:
    """ASCII alphabetical priority for tie-break.

    Lower string wins (NODE_A < NODE_B < ... < zzz_DEFAULT).
    None / empty → zzz_DEFAULT (legacy partition fallback per §D2.1).
    """
    if not node_id:
        return NODE_PRIORITY_DEFAULT_SENTINEL
    return node_id


# ----------------------------------------------------------------------
# Counter sink protocol (Codex F-2 fix — heartbeat metric wiring hook)
# ----------------------------------------------------------------------

class DedupCounterSink(Protocol):
    """Optional counter sink for heartbeat metric wiring.

    heartbeat-schema.v1 의 metrics.dup_skip_count / metrics.quarantine_count 와 1:1 매핑.
    X4 (status CLI) + X6 (web panel) 가 read 시 사용. dedup module 은 sink 가 None
    이면 자체 counter 만 증가 (read-only 컨텍스트에서 invasive 0).
    """

    def increment_dup_skip(self, n: int = 1) -> None: ...
    def increment_quarantine(self, n: int = 1) -> None: ...


# ----------------------------------------------------------------------
# Quarantine record + backpressure (Architect 결정 #8)
# ----------------------------------------------------------------------

@dataclass
class QuarantineRecord:
    """Per-mismatch audit record for active-active dedup.

    Architect 결정 #3 — written to root manifest (path = caller responsibility).
    """
    reason: str  # always "ACTIVE_ACTIVE_MISMATCH"
    tier: str  # "candle" / "tick" / "orderbook"
    logical_key: tuple
    rows: list  # the conflicting rows (양 node 의 row 모두)
    detected_at: datetime


class _BackpressureLimiter:
    """Per-second rate-limit + batching for quarantine artifact emission.

    Codex F-6 fix:
    - monotonic clock (time.monotonic) — wall clock skew 영향 0
    - per-scan single-thread assumption (Generator 호출 컨텍스트 single-threaded)
    - artifact count vs total quarantine_count 분리 (counter 는 모든 mismatch count)
    """

    def __init__(self, cap_per_sec: int = QUARANTINE_RATE_LIMIT_PER_SEC):
        self._cap = cap_per_sec
        self._window_start = time.monotonic()
        self._window_count = 0
        self._batch: list[QuarantineRecord] = []
        self._artifact_count = 0  # 별도 — counter 와 분리

    def admit(self, record: QuarantineRecord) -> tuple[bool, list[QuarantineRecord] | None]:
        """Admit a record. Returns (emit_now, batch_to_emit).

        Returns:
            (True, [record]) — under cap, emit immediately
            (False, None) — over cap, batched (still in buffer)
            (True, batch) — over cap and second tick reached, flush batch
        """
        now = time.monotonic()
        if now - self._window_start >= 1.0:
            # Window rollover — flush any pending batch
            self._window_start = now
            self._window_count = 0
            if self._batch:
                flushed = self._batch
                self._batch = []
                self._artifact_count += 1
                self._batch.append(record)
                return (True, flushed)

        self._window_count += 1
        if self._window_count <= self._cap:
            self._artifact_count += 1
            return (True, [record])
        # over cap — batch it
        self._batch.append(record)
        return (False, None)

    def flush(self) -> list[QuarantineRecord]:
        """Flush remaining batched records (call at scan end)."""
        if self._batch:
            flushed = self._batch
            self._batch = []
            self._artifact_count += 1
            return flushed
        return []

    @property
    def artifact_count(self) -> int:
        return self._artifact_count


# ----------------------------------------------------------------------
# Dedup result
# ----------------------------------------------------------------------

@dataclass
class DedupResult:
    emitted: list  # row 들 (deduplicated, sorted by ts_utc)
    dup_skip_count: int
    quarantine_count: int  # all mismatches (artifact count 와 분리)
    quarantine_records: list[QuarantineRecord] = field(default_factory=list)


# ----------------------------------------------------------------------
# T1 candle dedup (hybrid late correction — Architect 결정 #1)
# ----------------------------------------------------------------------

def deduplicate_candles(
    rows: Iterable[Any],
    *,
    multi_node: bool,
    sink: DedupCounterSink | None = None,
) -> DedupResult:
    """T1 candle dedup with hybrid late correction.

    multi_node=False (single-node 또는 legacy 단일) → no dedup, pass-through.
    multi_node=True → group by 4-key, choose row with:
      1. received_at MAX (late correction — 더 늦게 본 view 가 최신 데이터)
      2. tie-break: node_priority(node_id) alphabetical lower wins

    T1 mismatch 는 quarantine 안 함 (ADR-009 §D5 — late correction + append-only 정책).
    """
    if not multi_node:
        rows_list = list(rows)
        return DedupResult(
            emitted=sorted(rows_list, key=lambda r: r.ts_utc),
            dup_skip_count=0,
            quarantine_count=0,
        )

    by_key: dict[tuple, Any] = {}
    dup_skip = 0
    for row in rows:
        key = candle_logical_key(row)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = row
            continue
        # Hybrid resolution: received_at MAX → tie-break node priority
        new_ra = getattr(row, "received_at", None) or row.ts_utc
        old_ra = getattr(existing, "received_at", None) or existing.ts_utc
        if new_ra > old_ra:
            by_key[key] = row
        elif new_ra < old_ra:
            pass  # existing wins
        else:
            # Tie — alphabetical node priority
            new_pri = node_priority(getattr(row, "node_id", None))
            old_pri = node_priority(getattr(existing, "node_id", None))
            if new_pri < old_pri:
                by_key[key] = row
        dup_skip += 1

    if sink is not None and dup_skip > 0:
        sink.increment_dup_skip(dup_skip)

    return DedupResult(
        emitted=sorted(by_key.values(), key=lambda r: r.ts_utc),
        dup_skip_count=dup_skip,
        quarantine_count=0,
    )


# ----------------------------------------------------------------------
# T2 tick dedup (6-tuple, mismatch → quarantine)
# ----------------------------------------------------------------------

def deduplicate_ticks(
    rows: Iterable[Any],
    *,
    multi_node: bool,
    sink: DedupCounterSink | None = None,
) -> DedupResult:
    """T2 tick dedup using 6-tuple fallback logical key.

    multi_node=True → group by 6-tuple. Logical key 일치 = byte-identical
    (received_at + raw_json 외 모든 column 이 logical key). 따라서 동일 key 의
    second occurrence 는 idempotent skip (dup_skip_count 증가).

    Value mismatch (양 node 가 다른 logical key 6-tuple 가 아닌 동일 key 인데
    raw_json content 가 다른 경우) 는 byte-identical 위반 — quarantine.
    """
    rows_list = list(rows)
    if not multi_node:
        return DedupResult(
            emitted=sorted(rows_list, key=lambda r: r.ts_utc),
            dup_skip_count=0,
            quarantine_count=0,
        )

    by_key: dict[tuple, Any] = {}
    dup_skip = 0
    quarantine_count = 0
    quarantine_records: list[QuarantineRecord] = []
    limiter = _BackpressureLimiter()
    detected_at = datetime.now()

    for row in rows_list:
        key = tick_logical_key(row)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = row
            continue
        # Logical key 일치 — content mismatch 검증
        existing_raw = getattr(existing, "raw_json", None)
        new_raw = getattr(row, "raw_json", None)
        if existing_raw is not None and new_raw is not None and existing_raw != new_raw:
            # Quarantine
            qr = QuarantineRecord(
                reason="ACTIVE_ACTIVE_MISMATCH",
                tier="tick",
                logical_key=key,
                rows=[existing, row],
                detected_at=detected_at,
            )
            quarantine_count += 1
            emit_now, batch = limiter.admit(qr)
            if emit_now and batch:
                quarantine_records.extend(batch)
            # Tie-break with node priority
            new_pri = node_priority(getattr(row, "node_id", None))
            old_pri = node_priority(getattr(existing, "node_id", None))
            if new_pri < old_pri:
                by_key[key] = row
        else:
            dup_skip += 1

    # Final flush of batched quarantines
    final_batch = limiter.flush()
    if final_batch:
        quarantine_records.extend(final_batch)

    if sink is not None:
        if dup_skip > 0:
            sink.increment_dup_skip(dup_skip)
        if quarantine_count > 0:
            sink.increment_quarantine(quarantine_count)

    return DedupResult(
        emitted=sorted(by_key.values(), key=lambda r: r.ts_utc),
        dup_skip_count=dup_skip,
        quarantine_count=quarantine_count,
        quarantine_records=quarantine_records,
    )


# ----------------------------------------------------------------------
# T3 orderbook dedup (8-tuple, best-effort, mismatch → quarantine)
# ----------------------------------------------------------------------


def deduplicate_orderbook_events(
    rows: Iterable[Any],
    *,
    multi_node: bool,
    sink: DedupCounterSink | None = None,
) -> DedupResult:
    """T3 orderbook event dedup using 8-tuple fallback logical key (best-effort)."""
    rows_list = list(rows)
    if not multi_node:
        return DedupResult(
            emitted=sorted(rows_list, key=lambda r: r.ts_utc),
            dup_skip_count=0,
            quarantine_count=0,
        )

    by_key: dict[tuple, Any] = {}
    dup_skip = 0
    quarantine_count = 0
    quarantine_records: list[QuarantineRecord] = []
    limiter = _BackpressureLimiter()
    detected_at = datetime.now()

    for row in rows_list:
        key = orderbook_logical_key(row)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = row
            continue
        # Logical key 일치 — content mismatch 검증 (T3 best-effort)
        existing_raw = getattr(existing, "raw_json", None)
        new_raw = getattr(row, "raw_json", None)
        if existing_raw is not None and new_raw is not None and existing_raw != new_raw:
            qr = QuarantineRecord(
                reason="ACTIVE_ACTIVE_MISMATCH",
                tier="orderbook",
                logical_key=key,
                rows=[existing, row],
                detected_at=detected_at,
            )
            quarantine_count += 1
            emit_now, batch = limiter.admit(qr)
            if emit_now and batch:
                quarantine_records.extend(batch)
            new_pri = node_priority(getattr(row, "node_id", None))
            old_pri = node_priority(getattr(existing, "node_id", None))
            if new_pri < old_pri:
                by_key[key] = row
        else:
            dup_skip += 1

    final_batch = limiter.flush()
    if final_batch:
        quarantine_records.extend(final_batch)

    if sink is not None:
        if dup_skip > 0:
            sink.increment_dup_skip(dup_skip)
        if quarantine_count > 0:
            sink.increment_quarantine(quarantine_count)

    return DedupResult(
        emitted=sorted(by_key.values(), key=lambda r: r.ts_utc),
        dup_skip_count=dup_skip,
        quarantine_count=quarantine_count,
        quarantine_records=quarantine_records,
    )


# ----------------------------------------------------------------------
# Quarantine artifact persistence (MCT-93 X4)
# ----------------------------------------------------------------------

def _serialize_value(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    return str(v)


def persist_quarantine_records(
    root: Path | str,
    records: list[QuarantineRecord],
) -> list[Path]:
    """Atomic write quarantine artifacts under <root>/market/manifest/quarantine/.

    Path format: ``{tier}-{detected_at_iso}-{batch_seq:06d}.json``
    - tier: per-record (records grouped by tier defensively)
    - detected_at_iso: ISO compact UTC (``%Y%m%dT%H%M%SZ``)
    - batch_seq: 6-digit zero-padded, allocated atomically via O_EXCL

    Concurrency-safe (Codex F-5 PUSH-BACK fix):
    - Sequence reservation uses ``os.open(..., O_CREAT | O_EXCL)`` to prevent
      collision under shared-storage active-active concurrent calls.
    - Temp file name is unique per writer (pid + thread id + monotonic ns) so
      two writers reserving the same logical name cannot share a temp.
    - ``os.replace(temp, candidate)`` then overwrites the (empty) reserved
      file with payload content atomically.

    Append-only (existing artifacts 손실 방지). atomic write (temp → fsync → replace).

    Returns: written file paths (per tier batch).

    See heartbeat-schema.v1.md §Related Manifest Artifacts.
    """
    if not records:
        return []
    root_p = Path(root)
    out_dir = root_p / "market" / "manifest" / "quarantine"
    out_dir.mkdir(parents=True, exist_ok=True)

    by_tier: dict[str, list[QuarantineRecord]] = {}
    for r in records:
        by_tier.setdefault(r.tier, []).append(r)

    written: list[Path] = []
    import threading as _threading
    writer_id = f"{os.getpid()}-{_threading.get_ident()}-{time.monotonic_ns()}"
    for tier, batch in by_tier.items():
        detected_at = batch[0].detected_at
        iso_compact = detected_at.strftime("%Y%m%dT%H%M%SZ")
        # Atomic sequence reservation via O_EXCL
        seq = 0
        candidate: Path
        while True:
            candidate = out_dir / f"{tier}-{iso_compact}-{seq:06d}.json"
            try:
                fd = os.open(str(candidate), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                break
            except FileExistsError:
                seq += 1
        payload = {
            "tier": tier,
            "count": len(batch),
            "records": [
                {
                    "reason": r.reason,
                    "logical_key": [_serialize_value(x) for x in r.logical_key],
                    "rows": [str(row) for row in r.rows],
                    "detected_at": r.detected_at.isoformat(),
                }
                for r in batch
            ],
        }
        # Unique per-writer temp (avoid temp collision under concurrent shared storage)
        temp = candidate.with_suffix(f".{writer_id}.tmp")
        with temp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, default=str, indent=None)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, candidate)
        written.append(candidate)
    return written
