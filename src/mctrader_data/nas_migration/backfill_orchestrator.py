"""backfill_orchestrator.py — Historic 76GB cold L2 영구 이관 orchestrator (MCT-153).

Story: MCT-153 (Stage 2 — backfill 76GB closed-day per-(symbol,day) chunking)
Issue: mclayer/mctrader-hub#265
ADR: ADR-027 D4 step 2 (backfill) + D6 (7종 invariant) + ADR-009 §D2.1 (node=DEFAULT)

Design decisions (§6.1 Change Plan 박제):
- BackfillOrchestrator: one-shot orchestrator (cron 0, daemon 0)
  Phase A: entry guard (SOPRunner + checkpoint load + file lock)
  Phase B: partition discovery + closed-day filter (S1) + ChunkSpec 생성 (S6)
  Phase C: ThreadPoolExecutor(max_workers=10) parallel dispatch (S7)
  Phase D: per-chunk NAS PUT + InvariantHarness verify (AC-4) + quarantine/blocked handling
  Phase E: final BackfillResult 합성 + cleanup

- BackfillCheckpoint: sqlite-WAL persistent checkpoint (MCT-150 RetryQueue pattern 정합)
  schema: backfill_checkpoint(chunk_id PK, symbol, date, status, sha256, fail_invariant,
          retry_count, last_attempt_iso)
  5 status enum: pending / in_flight / verified / quarantined / blocked

- ChunkSpec: per-(symbol, day) tuple chunk plan (frozen dataclass, Phase B 박제)
- ChunkResult: per-chunk processing result (frozen dataclass, Phase D 박제)
- BackfillResult: final orchestrator result (frozen dataclass, Phase E 박제)

§6.8 Wording SSOT (박제 — variant 사용 금지):
- BackfillResult.status: "all_chunks_verified" / "chunk_invariant_failed" /
                          "chunk_blocked" / "checkpoint_resumable"
- ChunkResult.status: "chunk_verified" / "chunk_skipped_resumed" /
                       "chunk_quarantined" / "chunk_blocked" / "chunk_sop_skipped"
- BackfillCheckpoint status: "pending" / "in_flight" / "verified" / "quarantined" / "blocked"

§6.9 placement:
- Phase A (SOPRunner guard) + Phase B (closed-day filter) + Phase C (dispatch):
  unconditional sequential
- Phase D (_process_chunk): conditional on PutResult.status switch +
  InvariantResult.status switch
- legacy node= fallback (S6): conditional on is_legacy_node=True

§6.7 Cross-module contract:
- NASUploader.put() → PutResult.status 5 enum propagate (MCT-150 SSOT)
- InvariantHarness.verify() → InvariantResult.status 8 enum propagate (MCT-151 SSOT)
- NASUnreachableSOPRunner.is_manual_gate() → bool propagate (MCT-150 SSOT)
- BackfillOrchestrator.run() → BackfillResult.status 4 enum propagate (본 SSOT)

SecurityArch (§6.3):
- credential embed 0 (evidence pack + log 에 endpoint URL 제외)
- log 출력 시 chunk_id + partition prefix 만 (sha256 raw data embed 0)

ADR-017 hot path 무영향 invariant:
- one-shot invocation (cron 0, daemon 0) — collector tick 과 직교
- closed-day partition only (S1) — L1 in-flight race 0, CompactionBarrier inject 0
- collector WAL/L1 ParquetWriter 침범 0 (별 process / 별 file path)

ADR-009 forward-only invariant (§D12.2):
- NAS-side single-side append-only (기존 row 수정/삭제 0)
- quarantine 시 NAS object retain (DELETE 0)
"""
from __future__ import annotations

import contextlib
import hashlib
import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult
    from mctrader_data.nas_migration.invariant_harness import InvariantHarness, InvariantResult
    from mctrader_data.nas_metrics.prometheus_exporters import PrometheusExporter
    from mctrader_data.ops.nas_unreachable_sop import NASUnreachableSOPRunner

log = logging.getLogger(__name__)


# ─── Dataclasses ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ChunkSpec:
    """Per-(symbol, day) tuple chunk plan — partition discovery 결과 (Phase B 박제).

    fields (§6.8 Wording SSOT 박제 — single name, variant 금지):
    - symbol:               symbol name (예: "BTC_KRW") — partition prefix 구성 요소
    - date:                 YYYY-MM-DD ISO date string (UTC midnight 이전 closed-day, S1)
    - source_path:          local .parquet file (Path) — partition discovery 결과
    - nas_object_key:       NAS bucket 내 object key (legacy node= 부재 시 node=DEFAULT/ 명시 — S6)
    - nas_partition_prefix: NAS partition prefix (verify 시 InvariantHarness.verify nas_partition 인자)
    - is_legacy_node:       True if local partition 의 node= prefix 부재 (S6 enforcement marker)
    - chunk_id:             deterministic chunk identifier (sha256(symbol|date|source_path)[:16])
                            checkpoint primary key + resumability 정합 (재실행 시 동일 chunk_id)

    §6.9 placement: Phase B 에서 생성 (partition discovery 결과), immutable.
    """

    symbol: str
    date: str
    source_path: Path
    nas_object_key: str
    nas_partition_prefix: str
    is_legacy_node: bool
    chunk_id: str


@dataclass(frozen=True)
class ChunkResult:
    """_process_chunk() per-chunk processing 결과 — Phase D 박제.

    status enum 5종 (§6.8 Wording SSOT 박제 — single string, variant 금지):
    - "chunk_verified":         PUT 성공 (또는 skipped_idempotent) + 7종 invariant ALL PASS.
                                checkpoint UPDATE status='verified' + sha256 박제. caller 측 추가 액션 0.
    - "chunk_skipped_resumed":  checkpoint status='verified' 가 이미 박제 (재실행 시 skip) 또는
                                PutResult.status='queued' (NAS unreachable transient).
    - "chunk_quarantined":      PUT 성공 but 7종 invariant 1종+ FAIL (3 retry 후 quarantine).
                                NAS object retain + checkpoint UPDATE status='quarantined'.
    - "chunk_blocked":          PutResult.status='hard_floor_blocked' (retry queue hard floor 도달).
                                MANUAL_GATE escalation 의무.
    - "chunk_sop_skipped":      Phase D step 2 defensive SOPRunner MANUAL_GATE 진입 후 차단.

    fields:
    - chunk_id:        ChunkSpec.chunk_id propagate
    - status:          5 enum 위
    - put_result:      NASUploader.put() PutResult propagate (None if Phase D step 1 skip)
    - invariant_result: InvariantHarness.verify() InvariantResult propagate (None if PUT 단계 차단)
    - retry_count:     verify retry 횟수 (0~3, quarantined 시 3)
    - error_message:   raw exception message (debugging only — evidence pack embed 0)
    """

    chunk_id: str
    status: Literal[
        "chunk_verified",
        "chunk_skipped_resumed",
        "chunk_quarantined",
        "chunk_blocked",
        "chunk_sop_skipped",
    ]
    put_result: PutResult | None
    invariant_result: InvariantResult | None
    retry_count: int = 0
    error_message: str = ""


@dataclass(frozen=True)
class BackfillResult:
    """BackfillOrchestrator.run() 의 final result enum + caller contract.

    status enum 4종 (§6.8 Wording SSOT 박제 — single string, variant 금지):
    - "all_chunks_verified":    모든 chunk status='chunk_verified' (또는 chunk_skipped_resumed).
                                MCT-154 cutover 진입 prerequisite 충족.
    - "chunk_invariant_failed": 1+ chunk status='chunk_quarantined' (3 retry 후 invariant FAIL).
                                MCT-154 cutover 차단 의무 (caller 측).
    - "chunk_blocked":          1+ chunk status='chunk_blocked' (retry queue hard floor 도달).
                                MANUAL_GATE escalation.
    - "checkpoint_resumable":   진행 중단 (SOP MANUAL_GATE or pending 잔존).
                                재실행 시 checkpoint 부터 재개 (verified chunk skip + pending 진행).
                                AC-5 chaos test 의 직접 verify path.

    Caller contract (CLI scripts/migration/run_backfill.py):
    - status == "all_chunks_verified"    → exit code 0
    - status == "chunk_invariant_failed" → exit code 2
    - status == "chunk_blocked"          → exit code 3
    - status == "checkpoint_resumable"   → exit code 4
    """

    status: Literal[
        "all_chunks_verified",
        "chunk_invariant_failed",
        "chunk_blocked",
        "checkpoint_resumable",
    ]
    total_chunks: int = 0
    verified_chunks: int = 0
    quarantined_chunks: int = 0
    blocked_chunks: int = 0
    resumable_chunks: int = 0
    evidence_pack_path: Path | None = None
    run_duration_s: float = 0.0
    run_timestamp_iso: str = ""


# ─── BackfillCheckpoint ──────────────────────────────────────────────────────


class BackfillCheckpoint:
    """sqlite-WAL persistent checkpoint — per-chunk state 박제 (AC-5 resumability prerequisite).

    schema (sqlite single table):
        backfill_checkpoint (
          chunk_id TEXT PRIMARY KEY,
          symbol TEXT NOT NULL,
          date TEXT NOT NULL,
          status TEXT NOT NULL,  -- 5 enum: pending/in_flight/verified/quarantined/blocked
          sha256 TEXT,           -- verified 시 박제 (NULL otherwise)
          fail_invariant TEXT,   -- quarantined 시 박제
          retry_count INTEGER DEFAULT 0,
          last_attempt_iso TEXT NOT NULL
        )

    sqlite-WAL mode (MCT-150 RetryQueue pattern 정합):
    - PRAGMA journal_mode = WAL
    - PRAGMA synchronous = NORMAL
    - multi-thread safety (ThreadPoolExecutor 10 thread 동시 UPSERT 안전)
    """

    STATUS_PENDING: Literal["pending"] = "pending"
    STATUS_IN_FLIGHT: Literal["in_flight"] = "in_flight"
    STATUS_VERIFIED: Literal["verified"] = "verified"
    STATUS_QUARANTINED: Literal["quarantined"] = "quarantined"
    STATUS_BLOCKED: Literal["blocked"] = "blocked"

    def __init__(self, checkpoint_path: Path) -> None:
        self._path = checkpoint_path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self) -> None:
        """sqlite WAL mode 활성화 + table 생성 (idempotent CREATE IF NOT EXISTS)."""
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_checkpoint (
                    chunk_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    date TEXT NOT NULL,
                    status TEXT NOT NULL,
                    sha256 TEXT,
                    fail_invariant TEXT,
                    retry_count INTEGER DEFAULT 0,
                    last_attempt_iso TEXT NOT NULL
                )
                """
            )
            conn.commit()

    @contextlib.contextmanager
    def _connect(self):
        """Per-call sqlite connection (multi-thread safety — connection per thread)."""
        conn = sqlite3.connect(str(self._path), timeout=30.0, check_same_thread=False)
        try:
            yield conn
        finally:
            conn.close()

    def load(self) -> dict[str, str]:
        """모든 chunk 의 (chunk_id → status) mapping load.

        Returns:
            dict[chunk_id, status] — Phase A step 3 의 기존 상태 로드.
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT chunk_id, status FROM backfill_checkpoint"
            ).fetchall()
        return {row[0]: row[1] for row in rows}

    def upsert_pending(self, chunks: list[ChunkSpec]) -> None:
        """Phase B step 6 — 신규 chunk 모두 pending UPSERT.

        UPSERT semantics: 기존 chunk_id (재실행) 는 status 변경 0 (verified/quarantined 보존).
        신규 chunk_id 만 status='pending' INSERT.
        """
        import datetime
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._connect() as conn:
            for chunk in chunks:
                conn.execute(
                    """
                    INSERT INTO backfill_checkpoint
                        (chunk_id, symbol, date, status, sha256, fail_invariant, retry_count, last_attempt_iso)
                    VALUES (?, ?, ?, 'pending', NULL, NULL, 0, ?)
                    ON CONFLICT(chunk_id) DO NOTHING
                    """,
                    (chunk.chunk_id, chunk.symbol, chunk.date, now_iso),
                )
            conn.commit()

    def get_status(self, chunk_id: str) -> str:
        """Phase C step 7 — chunk_id 의 현재 status return."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM backfill_checkpoint WHERE chunk_id = ?",
                (chunk_id,),
            ).fetchone()
        return row[0] if row else self.STATUS_PENDING

    def update_status(
        self,
        chunk_id: str,
        new_status: Literal["in_flight", "verified", "quarantined", "blocked"],
        *,
        sha256: str | None = None,
        fail_invariant: str | None = None,
        retry_count: int = 0,
    ) -> None:
        """Per-chunk status 갱신 + last_attempt_iso UTC ISO8601 박제.

        verified 시: sha256 박제 의무.
        quarantined 시: fail_invariant + retry_count 박제 의무.
        """
        import datetime
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE backfill_checkpoint
                SET status = ?, sha256 = ?, fail_invariant = ?,
                    retry_count = ?, last_attempt_iso = ?
                WHERE chunk_id = ?
                """,
                (new_status, sha256, fail_invariant, retry_count, now_iso, chunk_id),
            )
            conn.commit()

    def list_all(self) -> list[tuple[str, str]]:
        """Phase E step 10 — 모든 chunk 의 (chunk_id, status) list."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT chunk_id, status FROM backfill_checkpoint"
            ).fetchall()
        return [(row[0], row[1]) for row in rows]


# ─── BackfillOrchestrator ────────────────────────────────────────────────────


class BackfillOrchestrator:
    """historic 76GB cold L2 영구 이관 orchestrator (one-shot, cron 0, daemon 0).

    Responsibilities:
    1. Partition discovery + closed-day filter (S1, AC-1)
    2. Legacy node= 부재 partition 의 node=DEFAULT 명시 enforcement (S6, AC-3)
    3. Per-(symbol, day) tuple chunk 분절 (single partition = 1 chunk, S7, AC-2)
    4. 10-symbol 병렬 dispatch via ThreadPoolExecutor (S7, AC-2)
    5. Per-chunk NASUploader.put() + InvariantHarness.verify() (AC-4)
    6. Quarantine on invariant FAIL (rollback DELETE 거부, §6.1 chief decision)
    7. Resumability checkpoint via sqlite-WAL (AC-5)
    8. SOPRunner.is_manual_gate() Phase A guard (EC-1)
    9. Evidence pack append (per-chunk verify result + per-run summary)

    §6.9 placement:
    - Phase A/B/C/E: unconditional sequential
    - Phase D (_process_chunk): conditional on PutResult.status switch + InvariantResult.status switch
    """

    def __init__(
        self,
        nas_uploader: NASUploader,
        invariant_harness: InvariantHarness,
        sop_runner: NASUnreachableSOPRunner,
        metrics: PrometheusExporter,
        *,
        local_root: Path,
        nas_partition_root: str,
        checkpoint_path: Path,
        evidence_pack_path: Path,
        lock_path: Path = Path("/data/backfill_orchestrator.lock"),
        max_workers: int = 10,
        verify_retry_budget: int = 3,
        chunk_timeout_s: float = 30.0,
        tier: Literal["L2", "L3"] = "L2",
        partition_normalization: bool = True,
        channel: Literal["orderbooksnapshot", "transaction"] = "orderbooksnapshot",  # MCT-159
    ) -> None:
        self._uploader = nas_uploader
        self._harness = invariant_harness
        self._sop_runner = sop_runner
        self._metrics = metrics
        self._local_root = local_root
        self._nas_partition_root = nas_partition_root
        self._checkpoint = BackfillCheckpoint(checkpoint_path)
        self._evidence_pack_path = evidence_pack_path
        self._lock_path = lock_path
        self._max_workers = max_workers
        self._verify_retry_budget = verify_retry_budget
        self._chunk_timeout_s = chunk_timeout_s
        self._tier = tier
        self._partition_normalization = partition_normalization
        self._channel = channel  # MCT-159: channel parametrize
        self._shutdown_requested = False

    def run(self) -> BackfillResult:
        """One-shot backfill orchestration — Phase A → B → C → D → E sequential.

        §6.9 unconditional phase structure:
        Phase A: SOPRunner guard + checkpoint load
        Phase B: partition discovery + closed-day filter + ChunkSpec 생성
        Phase C: ThreadPoolExecutor parallel dispatch + as_completed 수집
        Phase D: per-chunk _process_chunk (conditional, thread function)
        Phase E: BackfillResult 합성 + cleanup

        Returns:
            BackfillResult — status 4 enum (§6.8 SSOT)
        Raises:
            OSError — file lock / sqlite WAL corruption (CLI exit code 5)
        """
        t_start = time.monotonic()
        import datetime
        run_timestamp_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # ── Phase A: entry guard (unconditional, §6.9) ────────────────────────
        # Step 2: SOPRunner.is_manual_gate() check (EC-1)
        if self._sop_runner.is_manual_gate():
            log.warning(
                "[backfill] SOPRunner MANUAL_GATE active — skipping backfill. "
                "Resolve NAS unreachable state first."
            )
            return BackfillResult(
                status="checkpoint_resumable",
                total_chunks=0,
                resumable_chunks=0,
                run_duration_s=time.monotonic() - t_start,
                run_timestamp_iso=run_timestamp_iso,
            )

        # Step 3: checkpoint load (기존 상태 복원)
        existing_status = self._checkpoint.load()
        log.info(
            "[backfill] checkpoint loaded: %d existing records", len(existing_status)
        )

        # ── Phase B: partition discovery (unconditional, §6.9) ───────────────
        local_files = self._discover_partitions()
        log.info("[backfill] discovered %d closed-day partitions", len(local_files))

        chunks: list[ChunkSpec] = []
        for f in local_files:
            try:
                chunk = self._build_chunk_spec(f)
                chunks.append(chunk)
            except Exception as exc:
                log.error("[backfill] ChunkSpec build failed path=%s error=%s", f, exc)
                continue

        # Step 6: checkpoint UPSERT (신규 chunk 만, 기존 verified 보존)
        self._checkpoint.upsert_pending(chunks)
        total_chunks = len(chunks)

        # emit metric: total chunks
        with contextlib.suppress(Exception):  # metric emit 실패는 backfill 중단하지 않음
            self._metrics.emit_backfill_chunks_total(total_chunks)

        # ── Phase C: parallel dispatch (unconditional, §6.9) ─────────────────
        # Step 7: pending_chunks = verified 제외
        pending_chunks = [
            c for c in chunks
            if self._checkpoint.get_status(c.chunk_id) not in ("verified",)
        ]

        chunk_results: list[ChunkResult] = []

        if pending_chunks:
            with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
                futures = {
                    executor.submit(self._process_chunk, chunk): chunk
                    for chunk in pending_chunks
                }
                for future in as_completed(futures, timeout=None):
                    try:
                        chunk_result = future.result(timeout=self._chunk_timeout_s)
                    except Exception as exc:
                        chunk = futures[future]
                        log.error(
                            "[backfill] chunk future failed chunk_id=%s error=%s",
                            chunk.chunk_id, exc,
                        )
                        chunk_result = ChunkResult(
                            chunk_id=chunk.chunk_id,
                            status="chunk_skipped_resumed",
                            put_result=None,
                            invariant_result=None,
                            error_message=str(exc),
                        )
                    chunk_results.append(chunk_result)

                    # checkpoint 갱신은 _process_chunk 내부에서 처리
                    # metric emit
                    try:
                        self._metrics.emit_backfill_chunks_completed(chunk_result.status)
                        if chunk_result.put_result is not None:
                            self._metrics.emit_backfill_put_latency(
                                chunk_result.put_result.latency_ms / 1000.0
                            )
                    except Exception:
                        pass

                    # evidence pack append
                    self._append_evidence(chunk_result)

                    # SOP MANUAL_GATE defensive recheck (Phase C loop 내)
                    if self._sop_runner.is_manual_gate():
                        log.warning(
                            "[backfill] SOPRunner MANUAL_GATE detected mid-run — "
                            "cancelling remaining futures"
                        )
                        for fut in futures:
                            fut.cancel()
                        break

        # already-verified chunks → chunk_skipped_resumed 합산
        already_verified = [
            c for c in chunks if self._checkpoint.get_status(c.chunk_id) == "verified"
            and c.chunk_id not in {r.chunk_id for r in chunk_results}
        ]
        for c in already_verified:
            chunk_results.append(
                ChunkResult(
                    chunk_id=c.chunk_id,
                    status="chunk_skipped_resumed",
                    put_result=None,
                    invariant_result=None,
                )
            )

        # ── Phase E: 최종 BackfillResult 합성 (unconditional, §6.9) ──────────
        final_records = self._checkpoint.list_all()
        status_counts = _count_statuses(final_records)

        verified_chunks = status_counts.get("verified", 0)
        quarantined_chunks = status_counts.get("quarantined", 0)
        blocked_chunks = status_counts.get("blocked", 0)
        resumable_chunks = (
            status_counts.get("pending", 0)
            + status_counts.get("in_flight", 0)
        )

        # status 합성 (§6.7 cross-module contract — priority 순서)
        if blocked_chunks > 0:
            _final_status = "chunk_blocked"
        elif quarantined_chunks > 0:
            _final_status = "chunk_invariant_failed"
        elif resumable_chunks > 0:
            _final_status = "checkpoint_resumable"
        else:
            # all verified (또는 skipped_resumed)
            _final_status = "all_chunks_verified"
        final_status = cast(
            Literal[
                "all_chunks_verified",
                "chunk_invariant_failed",
                "chunk_blocked",
                "checkpoint_resumable",
            ],
            _final_status,
        )

        # resumable metric emit
        try:
            if resumable_chunks > 0:
                self._metrics.emit_backfill_resumable(resumable_chunks)
        except Exception:
            pass

        run_duration_s = time.monotonic() - t_start
        log.info(
            "[backfill] run complete status=%s total=%d verified=%d quarantined=%d "
            "blocked=%d resumable=%d duration=%.1fs",
            final_status, total_chunks, verified_chunks, quarantined_chunks,
            blocked_chunks, resumable_chunks, run_duration_s,
        )

        return BackfillResult(
            status=final_status,
            total_chunks=total_chunks,
            verified_chunks=verified_chunks,
            quarantined_chunks=quarantined_chunks,
            blocked_chunks=blocked_chunks,
            resumable_chunks=resumable_chunks,
            evidence_pack_path=self._evidence_pack_path,
            run_duration_s=run_duration_s,
            run_timestamp_iso=run_timestamp_iso,
        )

    def _discover_partitions(self) -> list[Path]:
        """Phase B step 4-5 — local file system traversal + closed-day filter (S1).

        Algorithm:
        1. glob: MCTRADER_DATA_ROOT/market/orderbooksnapshot/schema_version=*/tier=L2/**/*.parquet
           (ADR-027 D1 Hive layout: schema_version/exchange/node/tier/date)
        2. for each path:
           a. parse date= component — UTC midnight 이전 (date < today) verify
           b. closed-day pass → include (당일 partition 제외)
        3. return list[Path] sorted by (symbol, date)

        S1 박제: Collector flush barrier signal + closed-day scope
        (UTC midnight 이전 = backfill only, 당일 partition = dual-write 자연 수렴)

        EC-2 (mixed legacy/non-legacy partition): partition 별 별도 분석 (per-chunk 처리).
        """
        today = date.today()
        # Pre-flight hot-fix 2026-05-13: schema_version=* layer 추가 (ADR-027 D1 Hive layout 정합)
        # MCT-159 amendment: channel parametrize (orderbooksnapshot + transaction)
        channel_root = self._local_root / "market" / self._channel

        if not channel_root.exists():
            log.warning("[backfill] channel root not found: %s", channel_root)
            return []

        # Find all tier={tier} directories under any schema_version=*
        tier_dirs = list(channel_root.glob(f"schema_version=*/tier={self._tier}"))
        if not tier_dirs:
            log.warning(
                "[backfill] no tier=%s dirs found under %s/schema_version=*/",
                self._tier, channel_root,
            )
            return []

        parquet_files: list[Path] = []
        for td in tier_dirs:
            parquet_files.extend(td.rglob("*.parquet"))
        parquet_files.sort()
        closed_day_files: list[Path] = []

        for pf in parquet_files:
            # date= 컴포넌트 추출 (ADR-009 §D2.1 Hive layout 정합)
            date_str = _extract_date_from_path(pf)
            if date_str is None:
                log.debug("[backfill] date= not found in path: %s", pf)
                continue

            try:
                partition_date = _parse_date(date_str)
            except ValueError:
                log.warning("[backfill] invalid date= value: %s path=%s", date_str, pf)
                continue

            # S1 enforcement: date < today (UTC midnight 이전)
            if partition_date < today:
                closed_day_files.append(pf)
            else:
                log.debug(
                    "[backfill] skipping today/future partition date=%s path=%s",
                    date_str, pf,
                )

        log.info(
            "[backfill] discovered %d closed-day partitions (today=%s)",
            len(closed_day_files), today,
        )
        return closed_day_files

    def _build_chunk_spec(self, source_path: Path) -> ChunkSpec:
        """Phase B step 6-7 — ChunkSpec 생성 + legacy node= 검출 + nas_object_key 구성.

        Algorithm:
        1. parse symbol, date, exchange, node from source_path
        2. is_legacy_node = "node=" not in any part of source_path (S6 enforcement marker)
        3. nas_partition_prefix 구성:
           - non-legacy: uses existing node= value
           - legacy:     node=DEFAULT 명시 삽입 (ADR-009 §D2.1 read mapping 정합)
        4. nas_object_key = nas_partition_prefix + "/" + source_path.name
        5. chunk_id = sha256(f"{symbol}|{date}|{source_path}".encode())[:16]
        6. return ChunkSpec(...)

        S6 박제 enforcement: legacy node= 부재 시 nas_object_key 에 node=DEFAULT/ 명시
        EC-2 (mixed legacy/non-legacy): partition 별 is_legacy_node 개별 판단
        """
        parts = source_path.parts
        # ADR-009 §D2.1 Hive layout: .../tier=L2/exchange=X/symbol=S/date=D/[hour=HH/][node=N/]file.parquet
        exchange = _extract_hive_value(parts, "exchange")
        symbol = _extract_hive_value(parts, "symbol")
        date_str = _extract_hive_value(parts, "date")
        hour = _extract_hive_value(parts, "hour")  # MCT-159: hour 축 추가
        node = _extract_hive_value(parts, "node")

        if symbol is None:
            symbol = "UNKNOWN"
        if date_str is None:
            date_str = "unknown"

        is_legacy_node = node is None

        # MCT-159: hour=HH segment (있으면 박제, 없으면 "" — legacy backward-compat R5)
        hour_segment = f"/hour={hour}" if hour is not None else ""

        if is_legacy_node:
            # S6: legacy partition → node=DEFAULT 명시 삽입
            nas_partition_prefix = (
                f"{self._nas_partition_root}"
                f"/exchange={exchange or 'UNKNOWN'}"
                f"/symbol={symbol}"
                f"/date={date_str}"
                f"{hour_segment}"
                f"/node=DEFAULT"
            )
            if self._metrics is not None:
                with contextlib.suppress(Exception):
                    self._metrics.emit_backfill_legacy_node_default()
        else:
            nas_partition_prefix = (
                f"{self._nas_partition_root}"
                f"/exchange={exchange or 'UNKNOWN'}"
                f"/symbol={symbol}"
                f"/date={date_str}"
                f"{hour_segment}"
                f"/node={node}"
            )

        nas_object_key = f"{nas_partition_prefix}/{source_path.name}"
        chunk_id = hashlib.sha256(
            f"{symbol}|{date_str}|{source_path}".encode()
        ).hexdigest()[:16]

        return ChunkSpec(
            symbol=symbol,
            date=date_str,
            source_path=source_path,
            nas_object_key=nas_object_key,
            nas_partition_prefix=nas_partition_prefix,
            is_legacy_node=is_legacy_node,
            chunk_id=chunk_id,
        )

    def _process_chunk(self, chunk: ChunkSpec) -> ChunkResult:
        """Phase D — per-chunk thread function (ThreadPoolExecutor.submit target).

        §6.9 conditional placement:
        1. checkpoint pre-check (status='verified' → skip)
        2. SOPRunner.is_manual_gate() defensive check (EC-1 race defense)
        3. checkpoint UPDATE 'in_flight'
        4. NASUploader.put() + PutResult.status switch
        5. InvariantHarness.verify() + InvariantResult.status switch
        6. retry budget loop (3회, 재PUT 0 — idempotent surface)
        7. quarantine decision (NAS object retain, DELETE 0, §6.1 chief decision)

        Idempotency (AC-5 + EC-4):
        - HEAD-then-PUT idempotency (NASUploader, MCT-150) → 재PUT 시 skipped_idempotent
        - InvariantHarness.verify() = read-only → 다중 호출 동일 결과
        - quarantine retain (NAS object DELETE 0) → 재실행 시 동일 partition 재진입
        """
        # Step 1: checkpoint pre-check
        current_status = self._checkpoint.get_status(chunk.chunk_id)
        if current_status == BackfillCheckpoint.STATUS_VERIFIED:
            return ChunkResult(
                chunk_id=chunk.chunk_id,
                status="chunk_skipped_resumed",
                put_result=None,
                invariant_result=None,
            )

        # Step 2: SOPRunner defensive check (Phase A 통과 후 race 방어)
        if self._sop_runner.is_manual_gate():
            log.warning(
                "[backfill] SOP MANUAL_GATE detected in thread chunk_id=%s — skipping",
                chunk.chunk_id,
            )
            return ChunkResult(
                chunk_id=chunk.chunk_id,
                status="chunk_sop_skipped",
                put_result=None,
                invariant_result=None,
            )

        # Step 3: checkpoint UPDATE 'in_flight'
        self._checkpoint.update_status(chunk.chunk_id, "in_flight")

        # Step 4: NAS PUT
        try:
            local_data = chunk.source_path.read_bytes()
            local_sha = hashlib.sha256(local_data).hexdigest()
            put_result = self._uploader.put(
                key=chunk.nas_object_key,
                data=local_data,
                sha256=local_sha,
                suppress_enqueue=False,
            )
        except Exception as exc:
            log.error(
                "[backfill] PUT exception chunk_id=%s error=%s", chunk.chunk_id, exc
            )
            return ChunkResult(
                chunk_id=chunk.chunk_id,
                status="chunk_skipped_resumed",
                put_result=None,
                invariant_result=None,
                error_message=str(exc),
            )

        # Step 5: PutResult.status switch (§6.7 정합)
        if put_result.status == "hard_floor_blocked":
            self._checkpoint.update_status(chunk.chunk_id, "blocked")
            log.critical(
                "[backfill] hard_floor_blocked chunk_id=%s MANUAL_GATE escalation required",
                chunk.chunk_id,
            )
            return ChunkResult(
                chunk_id=chunk.chunk_id,
                status="chunk_blocked",
                put_result=put_result,
                invariant_result=None,
            )

        if put_result.status == "queued":
            # NAS unreachable transient → RetryQueue 가 처리, 재실행 시 재진입
            log.info(
                "[backfill] NAS unreachable queued chunk_id=%s — will retry on resume",
                chunk.chunk_id,
            )
            return ChunkResult(
                chunk_id=chunk.chunk_id,
                status="chunk_skipped_resumed",
                put_result=put_result,
                invariant_result=None,
            )

        # Step 6: InvariantHarness.verify() (uploaded / skipped_idempotent / skipped_etag_overwrite)
        # MCT-159 FIX Iter 2: per-file mode (ADR-027 §D6.1 chunk↔verify per-file contract)
        # chunk_spec = 1 file = 1 chunk_id (MCT-153 박제 보존), verify = per-file basis
        invariant_result = self._harness.verify(
            local_partition=chunk.source_path.parent,
            nas_partition=chunk.nas_partition_prefix,
            local_files=[chunk.source_path],
            nas_objects=[chunk.nas_object_key],
        )

        if invariant_result.status == "all_pass":
            self._checkpoint.update_status(
                chunk.chunk_id,
                "verified",
                sha256=local_sha,
            )
            log.debug("[backfill] chunk verified chunk_id=%s", chunk.chunk_id)
            return ChunkResult(
                chunk_id=chunk.chunk_id,
                status="chunk_verified",
                put_result=put_result,
                invariant_result=invariant_result,
            )

        # Step 7: retry budget loop (재PUT 0 — idempotent surface)
        for attempt in range(self._verify_retry_budget):
            log.warning(
                "[backfill] invariant FAIL chunk_id=%s fail=%s attempt=%d/%d",
                chunk.chunk_id, invariant_result.status, attempt + 1, self._verify_retry_budget,
            )
            invariant_result = self._harness.verify(
                local_partition=chunk.source_path.parent,
                nas_partition=chunk.nas_partition_prefix,
            )
            if invariant_result.status == "all_pass":
                self._checkpoint.update_status(
                    chunk.chunk_id, "verified", sha256=local_sha
                )
                return ChunkResult(
                    chunk_id=chunk.chunk_id,
                    status="chunk_verified",
                    put_result=put_result,
                    invariant_result=invariant_result,
                    retry_count=attempt + 1,
                )

        # quarantine: 3 retry 후 FAIL — NAS object retain (DELETE 0, §6.1 chief decision)
        self._checkpoint.update_status(
            chunk.chunk_id,
            "quarantined",
            fail_invariant=invariant_result.status,
            retry_count=self._verify_retry_budget,
        )
        with contextlib.suppress(Exception):
            self._metrics.emit_backfill_quarantine(invariant_result.status)
        log.error(
            "[backfill] chunk quarantined chunk_id=%s fail_invariant=%s retry_count=%d",
            chunk.chunk_id, invariant_result.status, self._verify_retry_budget,
        )
        return ChunkResult(
            chunk_id=chunk.chunk_id,
            status="chunk_quarantined",
            put_result=put_result,
            invariant_result=invariant_result,
            retry_count=self._verify_retry_budget,
        )

    def _append_evidence(self, chunk_result: ChunkResult) -> None:
        """Phase C as_completed() per-chunk evidence pack append.

        evidence pack 구조 (§6.6 + §11.4 박제):
        - per-chunk row: timestamp + chunk_id + status + put_status + invariant_status

        T4 secret masking: NAS endpoint URL embed 차단 (chunk_id + status 만).
        Append-only (§11.7 invariant).
        """
        import datetime
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        put_status = (
            chunk_result.put_result.status if chunk_result.put_result else "N/A"
        )
        inv_status = (
            chunk_result.invariant_result.status
            if chunk_result.invariant_result
            else "N/A"
        )
        line = (
            f"{ts}|{chunk_result.chunk_id}|{chunk_result.status}|"
            f"{put_status}|{inv_status}|{chunk_result.retry_count}\n"
        )
        try:
            with self._evidence_pack_path.open("a", encoding="utf-8") as f:
                f.write(line)
        except Exception as exc:
            log.debug("[backfill] evidence append failed: %s", exc)

    def shutdown(self) -> None:
        """Graceful shutdown — 재실행 안전.

        Caller 의무: SIGTERM/SIGINT 처리 시 호출 (CLI signal handler).
        Idempotency: 다중 호출 시 NO-OP.
        """
        self._shutdown_requested = True
        log.info("[backfill] shutdown requested (graceful)")


# ─── helpers ─────────────────────────────────────────────────────────────────


def _extract_hive_value(parts: tuple[str, ...], key: str) -> str | None:
    """Hive partition key=value 에서 value 추출.

    예: parts = (..., "symbol=BTC_KRW", ...) key="symbol" → "BTC_KRW"
    """
    prefix = f"{key}="
    for part in parts:
        if part.startswith(prefix):
            return part[len(prefix):]
    return None


def _extract_date_from_path(path: Path) -> str | None:
    """path parts 에서 date= 컴포넌트 추출."""
    return _extract_hive_value(tuple(path.parts), "date")


def _parse_date(date_str: str) -> date:
    """ISO date string → datetime.date (ValueError on invalid)."""
    from datetime import date as _date
    year, month, day = date_str.split("-")
    return _date(int(year), int(month), int(day))


def _count_statuses(records: list[tuple[str, str]]) -> dict[str, int]:
    """(chunk_id, status) list → status count dict."""
    counts: dict[str, int] = {}
    for _, status in records:
        counts[status] = counts.get(status, 0) + 1
    return counts
