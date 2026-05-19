---
story_key: MCT-204
title: compactor _tick stall 해소 + historical L1 회수 경로 신설
phase_structure: 1 Story = 2 PRs (Phase 1 Spec/Doc + Phase 2 Impl)
authored_by: codeforge-design:ArchitectAgent (chief author) — 6 deputy synthesis (CodebaseMapper / Refactor / SecurityArch / TestContractArch / DataMigrationArch / OperationalRiskArch)
created_at: 2026-05-19
status: Phase 1 design lane draft
live_touching: false
deputy_spawn: 6 permanent + LiveOps OFF + LiveOrdering OFF (read-only forward fix + reclaim, ordering side effect 0)
adr_amendments:
  - id: ADR-027 §D5 INCIDENT-2026-05-19 amendment
    repo: mctrader-hub
    status: Proposed (sibling PR 같은 phase)
  - id: ADR-027 §D7 amendment (forward path partition discovery boundary)
    repo: mctrader-hub
    status: Proposed (sibling PR 같은 phase)
  - id: ADR-029 D1=B amendment (historical L1 reclaim verify-after pattern)
    repo: mctrader-hub
    status: Proposed (sibling PR 같은 phase)
---

# MCT-204 Change Plan — compactor _tick stall 해소 + historical L1 회수 경로 신설

> Story file SSOT: [docs/stories/MCT-204.md](../stories/MCT-204.md) (§1-§11)
> Spec: [docs/superpowers/specs/2026-05-19-compactor-tick-stall-historical-l1-reclaim.md](../superpowers/specs/2026-05-19-compactor-tick-stall-historical-l1-reclaim.md)
> Phase 1 PR: [mclayer/mctrader-data#185](https://github.com/mclayer/mctrader-data/pull/185)

## §1 목적 + 사용자 호소

> [Story §1 verbatim](../stories/MCT-204.md#1-사용자-요구사항):
> 1. "이전에 compact 완료된 데이터는 삭제하라 했는데 여전히 해결되지 않아 계속 로컬 디스크가 낭비되고 있다."
> 2. "함께 L3로 데이터도 잘 쌓이고 있지 않다."

두 호소가 **단일 origin** (Story §2 RC 확정): forward `_run_l2` worker thread 가 NAS GET 안에서 stall → `_tick` 의 후속 step (`_run_l3`, `_cycle_count++`, `scan_and_cleanup_legacy`) 영구 미진입.

## §2 As-is — 현재 구조 (CodebaseMapper 변호)

### §2.1 sequential 차단 chain — [runner.py:86-149](../../src/mctrader_data/compactor/runner.py#L86-L149)

| step | 위치 | 동작 |
|---|---|---|
| 1 | `runner.py:107-112` | `for sealed in sealed_list: self._l1.compact_segment(sealed)` (정상) |
| 2 | `runner.py:114-116` | `await run_in_executor(None, self._run_l2)` (blocking await) |
| 3 | `runner.py:118-120` | `await run_in_executor(None, self._run_l3)` (2 차단 시 도달 0) |
| 4 | `runner.py:131` | `self._cycle_count += 1` (2 차단 시 영원히 0) |
| 5 | `runner.py:132-145` | `if cycle_count % LEGACY_CLEANUP_EVERY_N_CYCLES == 0: scan_and_cleanup_legacy(...)` (호출 0) |

### §2.2 rglob outer iter 폭주 — [runner.py:158](../../src/mctrader_data/compactor/runner.py#L158)

- `(self._root / "market").rglob("*/tier=L1/**/part-*.parquet")` = **historical 16,918 file iter** (5/13~17 130GB 포함)
- 400 unique `(exchange, symbol, channel)` tuple × `[today, yesterday]` × 24 hour seen 중복 제거 = **19,200 worker NAS GET dispatch**
- 평균 6 sec / dispatch × 19,200 ≈ 32h / 1 cycle — main thread 영구 stall

### §2.3 historical L1 회수 경로 부재 — [runner.py:471-475](../../src/mctrader_data/compactor/runner.py#L471-L475)

- `run_historical_promotion` 의 `_historical_dual_write(..., source_to_delete=None)` (`runner.py:471-475` 주석) — MCT-202 D-3 sequential local-only flow 충돌 회피
- promote-historical = L1 → L2 변환 + L2 NAS PUT **만** (L1 unlink 0)
- `scan_and_cleanup_legacy` 회수 gate = L1 NAS HEAD 4-tuple verify — **L1 NAS 객체 부재** → preserved (one-shot 검증: 31 iter / cleaned 2,359 / preserved 13,141)

### §2.4 보존 영역 (Refactor 변호 + Mapper 유지)

다음 함수/path 는 **본 Story scope 외** — 그대로 보존 (regression 차단 invariant):

- `L1Compactor.compact_segment()` — sealed → L1 parquet + DualWriter.put_l1() (ADR-029 D1=B 정합)
- `_dispatch_dual_write` 의 `source_to_delete=parquet_path` ([runner.py:288](../../src/mctrader_data/compactor/runner.py#L288)) — MCT-202 forward eager cascade
- `scan_and_cleanup_legacy` 본체 ([runner.py:322-418](../../src/mctrader_data/compactor/runner.py#L322-L418)) — 12-cycle cadence + 4-HEAD verify + INV-4 안전망
- `_discover_partitions_in_range` helper ([runner.py:421-457](../../src/mctrader_data/compactor/runner.py#L421-L457)) — 본 Story 가 신규 caller 추가 (signature 무변경)
- `_historical_dual_write` + `run_historical_promotion` 본체 — Layer 3 = batch 종료 hook 추가 (본체 무변경)

## §3 To-be — 3 Layer 통합 (Refactor 옹호 + chief author 채택)

### Layer 1 — forward `_run_l2` / `_run_l3` rglob 축소

**변경**: rglob outer iter → `_discover_partitions_in_range(channel=*, start_date=today-1, end_date=today)` 호출.

**Before** (`runner.py:158-173`):
```python
for parquet in (self._root / "market").rglob("*/tier=L1/**/part-*.parquet"):
    exchange = _extract_partition(parquet, "exchange")
    symbol = _extract_partition(parquet, "symbol")
    channel = parquet.parts[list(parquet.parts).index("market") + 1]
    for date_utc in [today, yesterday]:
        for hour in range(24):
            ...
            self._run_l2_for_parquet(...)
```

**After**:
```python
today = datetime.now(timezone.utc).date()
yesterday = today - timedelta(days=1)
for channel in _CHANNELS_FOR_L2:  # ("transaction", "orderbooksnapshot", "orderbookdepth")
    for ex, sym, d in _discover_partitions_in_range(
        self._root, channel=channel, start_date=yesterday, end_date=today,
    ):
        for hour in range(24):
            self._run_l2_for_parquet(
                exchange=ex, symbol=sym, channel=channel,
                date_utc=d, hour_utc=hour,
            )
```

**`_run_l3` 동형** — `_discover_partitions_in_range` 의 tier 인자 추가 OR 신규 L2 helper (`_discover_l2_partitions_in_range`) — chief 채택: 단일 helper 에 `tier: str = "L1"` parameter 추가 (signature backward-compat, 기본값 L1).

**Sub-deputy 정합** (CodebaseMapper 변호): helper 본문은 `tier=L1` 하드코딩 (`runner.py:439`) → tier parameter 화. 기존 callers 0 (helper 가 `run_historical_promotion` 단일 caller) → backward-compat 안전.

### Layer 2 — `_tick` step 격리 (asyncio task 분리)

**변경**: `_tick` 가 매 SCAN_INTERVAL_SECONDS 마다 5 step 순차 실행 → 별 long-lived task 3종 (L1 dispatcher / L2-L3-cleanup orchestrator / GC).

**chief 결정 (TestContractArch + OpRiskArch 통합)**: full asyncio 분리 = task lifecycle 복잡도 증가 + cycle_count drift 위험. **간소 형태 채택** — `_tick` 내 sequential 보존 + 각 step `asyncio.wait_for(timeout=MCTRADER_COMPACTOR_STEP_TIMEOUT_SECONDS)` wrap. timeout 시 `TimeoutError` catch → log warning + 다음 step 진행 (drop, 다음 tick 자연 재시도). cleanup 도 cycle_count gate 보존하되 timeout 적용.

**(FIX 1/3, P0 #2) ThreadPoolExecutor slot 고갈 완화 — 3중 lock 박제**:

기존 chief 결정 = `asyncio.wait_for` timeout drop **만** (main thread unblock). 그러나 DesignReviewPL P0 #2 = worker thread 가 ThreadPoolExecutor 안 stall 시 thread cancel 불가 (Python 제약) → 다음 cycle 마다 새 worker spawn + stall → default ThreadPoolExecutor `min(32, cpu+4)` slot 영구 점유 + 다른 step starvation.

**완화 3중 lock (derived default, chief 통합)**:

1. **boto3 `read_timeout=120s` + `connect_timeout=30s` (root cause fix)** — `NASUploader._s3` client config 박제. NAS GET stall 의 진정 원인 = boto3 default timeout = ∞. 120s timeout 적용 시 worker thread 자연 release. ADR-027 §D5 INCIDENT-2026-05-19 amendment 의 "silent stall 차단" 의 base layer.

```python
# src/mctrader_data/nas_storage/nas_uploader.py (Phase 2 modify)
from botocore.config import Config

_boto_config = Config(
    read_timeout=120,        # NAS GET hang 차단 (default = ∞)
    connect_timeout=30,
    retries={"max_attempts": 3, "mode": "standard"},
)
self._s3 = boto3.client("s3", endpoint_url=endpoint, config=_boto_config, ...)
```

2. **dedicated `ThreadPoolExecutor` per step (slot 격리)** — L2/L3/cleanup/historical 4 step 각 별 instance, max_workers=2 each, 총 8 thread cap. default executor 와 분리 → 한 step stall slot exhaustion 이 다른 step 으로 propagate 0.

```python
# src/mctrader_data/compactor/runner.py (Phase 2 modify, Layer 2 확장)
from concurrent.futures import ThreadPoolExecutor

class CompactorRunner:
    def __init__(self, ...):
        self._executors = {
            "l2": ThreadPoolExecutor(max_workers=2, thread_name_prefix="compactor-l2"),
            "l3": ThreadPoolExecutor(max_workers=2, thread_name_prefix="compactor-l3"),
            "cleanup": ThreadPoolExecutor(max_workers=2, thread_name_prefix="compactor-cleanup"),
            "historical": ThreadPoolExecutor(max_workers=2, thread_name_prefix="compactor-hist"),
        }

    async def _run_step_with_timeout(self, name: str, fn) -> None:
        ...
        await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(self._executors[name], fn),
            timeout=self._step_timeout,
        )
        ...

    async def stop(self) -> None:
        for ex in self._executors.values():
            ex.shutdown(wait=False, cancel_futures=True)
```

3. **asyncio.wait_for=600s outer (main thread unblock)** — 기존 layer. 두 root layer (boto3 + dedicated executor) 가 진정 mitigation, 본 layer 는 worker stall 의 last-resort safety net.

**3중 lock 진정 mitigation 효과**:
- worker thread stall 시 boto3 timeout=120s → worker 자연 release (다음 cycle 새 worker spawn 안 필요)
- 만일 boto3 timeout 미작동 (예: TCP keepalive 이슈) → dedicated executor 의 max_workers=2 가 본 step 만 영향 (다른 3 step 계속 작동)
- 두 layer 모두 fail → asyncio.wait_for=600s 가 main thread unblock + log warning

**Phase 2 후속 carry-over (P2 #1)**: full asyncio task 분리 (별 long-lived task per step + 별 cadence) = 별 Story KEY (TBD, MCT-204 RETRO 단계에서 PMOAgent 가 KEY 발행).

**근거 (Refactor 옹호 → chief 반박 일부 채택)**:
- spec §3 Layer 2 가 "asyncio.create_task 별 cadence" 제안. 그러나 task lifecycle (cancel / restart / exception propagation) 가 새 invariant 추가 → 단일 Story scope 폭증.
- 핵심 invariant = "한 step stall 이 다른 step 진입 차단 안 함" (AC-1). `asyncio.wait_for` timeout drop 이 본 invariant 충족 + 변경 최소.
- spec §3 Layer 2 "별 task" 형태로 향후 확장 가능 (Phase 2 후속, 본 Story 외).

**Before** (`runner.py:114-120`):
```python
if now - self._last_l2 >= L2_INTERVAL_SECONDS:
    self._last_l2 = now
    await asyncio.get_running_loop().run_in_executor(None, self._run_l2)
if now - self._last_l3 >= L3_INTERVAL_SECONDS:
    self._last_l3 = now
    await asyncio.get_running_loop().run_in_executor(None, self._run_l3)
```

**After**:
```python
_step_timeout = float(os.environ.get(
    "MCTRADER_COMPACTOR_STEP_TIMEOUT_SECONDS", "600"
))

async def _run_step_with_timeout(self, name: str, fn) -> None:
    start = time.time()
    try:
        await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(None, fn),
            timeout=self._step_timeout,
        )
    except asyncio.TimeoutError:
        elapsed = time.time() - start
        log.warning(
            "[compactor] step timeout name=%s elapsed=%.1fs limit=%.1fs",
            name, elapsed, self._step_timeout,
        )
        compactor_step_stall_seconds.labels(step=name).set(elapsed)
        return  # 다음 step 진입

if now - self._last_l2 >= L2_INTERVAL_SECONDS:
    self._last_l2 = now
    await self._run_step_with_timeout("l2", self._run_l2)
if now - self._last_l3 >= L3_INTERVAL_SECONDS:
    self._last_l3 = now
    await self._run_step_with_timeout("l3", self._run_l3)
```

cleanup 도 동일 `_run_step_with_timeout("cleanup", ...)` 처리.

**`mctrader_compactor_cleanup_cycle_delay_seconds` Gauge emit** (AC-1 target) = `_tick` 진입 시점에 `(now - self._last_cleanup_complete)` 계산 후 set. cleanup 완료 시점에 `self._last_cleanup_complete = now` update. timeout 으로 cleanup 진입 못해도 Gauge 가 정상 증가 → alert trigger.

### Layer 3 — historical L1 회수 경로 신설

**신규 file**: `src/mctrader_data/compactor/historical_reclaim.py`.

**모듈 책임**:
1. partition `(exchange, symbol, channel, date)` 의 L2 NAS HEAD 4-tuple verify (incremental_l1_reclaim.py GATE-2 동형, `promote_l1` 재사용 차원에서는 다름 — L2 객체 존재 + sha256 + size + ContentLength 검증)
2. verify pass 시 partition 의 L1 local `date_dir.rglob("part-*.parquet")` unlink
3. sentinel `.l1-promoted` zero-byte marker write (재실행 skip)
4. metric emit (`mctrader_historical_l1_reclaim_total{outcome}`)

**API** (FIX 1/3, P0 #3 + P1 #8 outcome enum 6 enum 정합):
```python
def reclaim_partition_l1_local(
    *,
    root: Path,
    nas_uploader: NASUploader,
    exchange: str,
    symbol: str,
    channel: str,
    date_utc: date,
    now_snapshot: date,  # caller monotonic snapshot (FIX 1/3 P0 #3)
) -> ReclaimOutcome:
    """L2 NAS HEAD verify → L1 unlink → sentinel write. 멱등 (sentinel 존재 시 skip)."""

@dataclass
class ReclaimOutcome:
    outcome: Literal[
        "ok",
        "skip_sentinel",
        "skip_today_window",
        "skip_forward_in_flight",   # FIX 1/3 P0 #3 (`.forward-processing` 존재)
        "skip_nas_missing",
        "fail_verify",
    ]
    files_unlinked: int
    bytes_freed: int
```

**caller** (FIX 1/3, P1 #4 insertion point 명시 — **per-partition** 위치, all-partitions 종료 시점 아님):

`run_historical_promotion` ([runner.py:524-623](../../src/mctrader_data/compactor/runner.py#L524-L623)) 의 partition loop 안 **각 `(ex, sym, d)` partition 별로**:

1. partition 의 24-hour L2 dual-write 완료 (`_historical_dual_write tier=L2` × 24 hour) +
2. partition 의 1 L3 dual-write 완료 (`_historical_dual_write tier=L3` × 1 day) **모두 commit 완료** 후,
3. 다음 partition `(ex2, sym2, d2)` 진입 **직전** (즉 partition loop body 마지막 statement) 에 reclaim 호출.

all-partitions 종료 후 일괄 reclaim **아님** (memory + race 회피).

```python
# src/mctrader_data/compactor/runner.py::run_historical_promotion partition loop body
# (per-partition, AFTER 24-hour L2 + 1 L3 dual_write commit, BEFORE next partition entry)
now_snapshot = ...  # caller cycle-entry snapshot (FIX 1/3 P0 #3)
for (ex, sym, d) in partitions:
    # ... existing 24-hour L2 dual_write loop
    # ... existing 1 L3 dual_write
    # === FIX 1/3 P1 #4 insertion point (per-partition) ===
    from mctrader_data.compactor.historical_reclaim import reclaim_partition_l1_local
    reclaim_outcome = reclaim_partition_l1_local(
        root=root,
        nas_uploader=dual_writer._uploader,
        exchange=ex,
        symbol=sym,
        channel=channel,
        date_utc=d,
        now_snapshot=now_snapshot,
    )
    counts[f"l1_reclaim_{reclaim_outcome.outcome}"] = counts.get(f"l1_reclaim_{reclaim_outcome.outcome}", 0) + 1
    counts["l1_reclaim_bytes_freed"] = counts.get("l1_reclaim_bytes_freed", 0) + reclaim_outcome.bytes_freed
    # === / FIX 1/3 P1 #4 insertion point ===

    # (FIX 1/3, P1 #10 Codex 3) partial completion abort/log policy:
    # _historical_dual_write 가 NASOperationalAlert re-raise 시 partition loop abort.
    # partition-level atomic 보장 (24-hour L2 partial PUT 케이스 reclaim 진입 0).
    # log message format: "[historical] partition abort exchange=%s symbol=%s channel=%s date=%s
    # error=%s reclaim_skipped=true" — operator 가 surface 인지.
```

**4-HEAD verify pattern (incremental_l1_reclaim.py GATE-2 차용 + ADR-029 §D5 정합)**:

```python
# 1. NAS L2 partition prefix list — KeyCount > 0
prefix = f"market/{channel}/schema_version={schema}/tier=L2/exchange={ex}/symbol={sym}/date={d.isoformat()}/"
resp = nas_uploader._s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
if resp.get("KeyCount", 0) == 0:
    return ReclaimOutcome(outcome="skip_nas_missing", ...)
# 2/3/4: 각 L2 object 의 HEAD 4-tuple (ETag + VersionId + sha256 Metadata + ContentLength) 가 local L2 와 match — promote_l1 패턴 동형 (선택 단순화: KeyCount > 0 + L2 local 존재 = 4-HEAD 박제, full per-file HEAD 는 Layer 3 단순성을 위해 skip — GATE-2 동형)
# (chief 결정: per-file HEAD = 비용 폭증 [partition 24-hour × N symbol]. GATE-2 KeyCount > 0 + local L2 존재 합치 = MCT-202 D-3 cascade 가 L2 NAS PUT 보장 후 cascading delete 의 동형 안전망. INV-C 안전망 보존 — verify fail 시 L1 unlink 0)
```

**sentinel write atomic (DataMigrationArch INV-F)**:

```python
sentinel = date_dir / ".l1-promoted"
tmp = sentinel.with_suffix(".tmp")
tmp.write_bytes(b"")
os.replace(tmp, sentinel)  # POSIX atomic
```

**race 격리 invariant (INV-B 박제)**:

```python
# (FIX 1/3, P0 #3) monotonic snapshot — caller 가 cycle 진입 시점 single date snapshot 박제 후 helper 에 전달
def reclaim_partition_l1_local(
    *,
    root: Path,
    nas_uploader: NASUploader,
    exchange: str,
    symbol: str,
    channel: str,
    date_utc: date,
    now_snapshot: date,  # caller monotonic snapshot (FIX 1/3 boundary race mitigation)
) -> ReclaimOutcome:
    # sentinel pre-check
    sentinel = date_dir / ".l1-promoted"
    if sentinel.exists():
        return ReclaimOutcome(outcome="skip_sentinel", ...)

    # forward window boundary check (monotonic snapshot 기반, sliding window race 차단)
    if date_utc >= now_snapshot - timedelta(days=1):
        return ReclaimOutcome(outcome="skip_today_window", ...)

    # (FIX 1/3, P0 #3) forward in-flight sentinel check — forward _run_l2_for_parquet 가
    # partition 진입 시 write, 완료 후 unlink. cross-cycle race 멱등 차단.
    forward_sentinel = date_dir / ".forward-processing"
    if forward_sentinel.exists():
        return ReclaimOutcome(outcome="skip_forward_in_flight", ...)

    # ... L2 NAS HEAD verify + L1 unlink + sentinel write
```

**(FIX 1/3, P0 #3) day boundary race mitigation — 2 layer 통합**:

기존 chief 결정 = `if date_utc >= today_utc - timedelta(days=1)` early-return 만. 그러나 DesignReviewPL P0 #3 = UTC 00:00:00 시점 sliding window boundary race — forward `_run_l2_for_parquet` in-flight 시 (now=N+1 직전 / window=[N-1, N]) historical reclaim 진입 (now=N+1 직후 / 이전 partition = date N-1 이 boundary 밖 진입 가능) = partition tuple `(ex, sym, channel, N-1)` 동시 access race.

**완화 2 layer (derived default, chief 통합)**:

1. **monotonic snapshot per cycle** — `_tick` / `run_historical_promotion` 진입 시점에 `now_snapshot = datetime.now(timezone.utc).date()` 단일 박제 후 caller chain 전체 전달. cycle 안 모든 boundary 비교가 single snapshot 기준 (sliding window 내부 race 차단).

```python
# src/mctrader_data/compactor/runner.py::_tick
async def _tick(self) -> None:
    self._cycle_count += 1
    now_snapshot = datetime.now(timezone.utc).date()  # monotonic per cycle (FIX 1/3 P0 #3)

    # forward _run_l2 / _run_l3 caller 가 now_snapshot 전달 (Layer 1 helper signature)
    await self._run_step_with_timeout("l2", lambda: self._run_l2(now_snapshot=now_snapshot))
    await self._run_step_with_timeout("l3", lambda: self._run_l3(now_snapshot=now_snapshot))
    ...

# src/mctrader_data/compactor/runner.py::run_historical_promotion
def run_historical_promotion(*, root, exchange, channel, start_date, end_date, ...) -> dict:
    now_snapshot = datetime.now(timezone.utc).date()  # monotonic per CLI invocation (FIX 1/3 P0 #3)
    if end_date >= now_snapshot - timedelta(days=1):
        raise ValueError(f"promote-historical end_date={end_date} must be < {now_snapshot - timedelta(days=1)} (today-1)")
    # ... partition loop 가 now_snapshot 전달
```

2. **partition-level `.forward-processing` sentinel** — forward `_run_l2_for_parquet` 가 partition 진입 시 `<date_dir>/.forward-processing` write, 완료 후 unlink (try/finally 박제). historical `reclaim_partition_l1_local` 가 sentinel 존재 시 `skip_forward_in_flight` outcome return → 다음 cycle 자연 재시도. 멱등 (sentinel race = 동시 진입 시 historical skip + 다음 cycle 정상).

```python
# src/mctrader_data/compactor/runner.py::_run_l2_for_parquet
def _run_l2_for_parquet(*, exchange, symbol, channel, date_utc, hour_utc, ...) -> None:
    date_dir = root / "market" / channel / f"schema_version={schema}" / f"tier=L1" / \
               f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_utc.isoformat()}"
    forward_sentinel = date_dir / ".forward-processing"
    forward_sentinel.touch()  # signal forward in-flight
    try:
        # ... existing L2 compact + dual_write
    finally:
        forward_sentinel.unlink(missing_ok=True)  # release signal
```

**효과**:
- monotonic snapshot per cycle → cycle 내부 boundary 비교 sliding 차단
- `.forward-processing` sentinel → cycle 사이 race 멱등 차단 (historical 가 skip + 다음 cycle 정상)
- INV-H wording 약화 (strict claim 폐기): "partition tuple ∅ overlap **best-effort (single cycle snapshot 기준), `.forward-processing` sentinel 가 cross-cycle race 멱등 차단**"

forward path = `[now_snapshot-1, now_snapshot]` only (Layer 1), historical reclaim = `date < now_snapshot - timedelta(days=1)` only — partition tuple intersection = **best-effort ∅ (monotonic snapshot 기준) + `.forward-processing` sentinel 멱등 보강** (FIX 1/3 박제).

### §3.4 신규 metric (Prometheus)

| metric | 종류 | label | source line |
|---|---|---|---|
| `mctrader_compactor_cleanup_cycle_delay_seconds` | Gauge | (none) | `runner.py::_tick` 진입 시점 |
| `mctrader_compactor_step_stall_seconds` | Gauge | step (l2/l3/cleanup/historical) | `runner.py::_run_step_with_timeout` timeout catch |
| `mctrader_historical_l1_reclaim_total` | Counter | exchange/channel/outcome (6 enum, FIX 1/3) | `historical_reclaim.py::reclaim_partition_l1_local` |
| `mctrader_l3_pending_partitions` | Gauge | exchange/channel | `runner.py::_run_l3` 진입 시점 (discovered partition 수) |

**emit module 결정 (CodebaseMapper 일치)**: `src/mctrader_data/metrics.py` (compactor_tier_pending_segments 정합). `nas_metrics/prometheus_exporters.py` = NAS-specific path (제외).

### §3.5 empirical-source annotation (FIX 1/3, P1 #9 — ADR-068 I-5)

본 Story 의 3 parameter empirical-source 명시 박제:

| parameter | 값 | empirical source |
|---|---|---|
| `MCTRADER_COMPACTOR_STEP_TIMEOUT_SECONDS` | default 600s | (1) **production NAS GET measurement**: py-spy Thread 133 stall 박제 (idle 2h+) — Story §2.2 `runner.py:158` 6 sec/dispatch × 19,200 dispatch ≈ 32h/cycle. 600s = single dispatch worst-case (95p) × 50 = single step 의 sub-batch 진행 후 next step 진입 가능한 cap. (2) **systemd-style default**: 10 min = OS-level "long" timeout convention (서비스 health check 표준). (3) **boto3 read_timeout 정합**: boto3 client `read_timeout=120s` × `max_attempts=3` ≈ 360s upper bound + 240s margin. |
| `cleanup_cycle_delay ≤ 300s` (AC-1 target) | 300s | **(1) `SCAN_INTERVAL_SECONDS=30s` × `LEGACY_CLEANUP_EVERY_N_CYCLES=12` = 360s base cadence**. (2) **container restart edge case** (P1 #11 Codex 4 결정): restart 직후 첫 cleanup 도달 시점 = 360s > 300s → AC-1 target wording 갱신 = "**steady state ≤ 300s, container restart 직후 첫 1 cycle 면제 (≤ 360s)**". 두 임계 명시 박제. (3) **operator perception**: 5분 = "사용자가 dashboard 새로고침 빈도" — 호소 #1 재발 차단 visibility target. |
| pidfile `flock` grace | 60s | (1) **`flock(LOCK_EX|LOCK_NB)` retry interval default** (Linux util-linux flock(1) man page, `-w 60`). (2) **MCT-173 BackfillManifest 패턴 답습** (production verified). (3) **operator manual recovery window**: 60s = "operator 가 SIGTERM 후 cleanup 확인 + 재실행 결정" minimum. |
| boto3 `read_timeout` | 120s | (1) **NAS GET 50MB worst-case latency** (MCT-148 PoC verified): p99 = 2870.65ms × 10 burst margin = 28.7s. 120s = 4x safety margin. (2) **MinIO server default `idle_timeout` = 90s** — client < server timeout 권고 (TCP keepalive 정합). (3) **botocore default `read_timeout = 60s`** 2x — production 측정 안전 margin. |
| dedicated executor `max_workers=2` per step | 2 | (1) **production single-symbol partition concurrency**: per-step 동시 진행 partition = 1 (sequential), max_workers=2 = current + queued single buffer slot. (2) **memory budget**: per-thread NAS GET 50MB peak (MCT-203 size-gated cache verified) × 4 step × 2 worker = 400MB cap — production container 2GB memory limit 안전. (3) **CPU bound 0**: NAS GET = network bound, max_workers > 2 = CPU/memory waste. |

**FIX 1/3 P1 #11 (Codex 4) AC-1 wording 정합 갱신 — Story §6 동시 갱신 의무 (별 commit)**.

## §4 영향 분석 + 회귀 방지 (Continuity)

Story §4.3 Continuity 표 cross-ref. 핵심 회귀 invariant:

| 의존 Story/ADR | 회귀 검증 | 본 Story 보존 path |
|---|---|---|
| MCT-202 D-1 forward eager cascade | `_dispatch_dual_write` `source_to_delete=parquet_path` 보존 | `runner.py:288` 무변경 |
| MCT-202 D-3 historical sequential local-only flow | `_historical_dual_write` `source_to_delete=None` 보존 | `runner.py:471-475` 무변경 (Layer 3 = post-hook) |
| MCT-189 D-3 C `scan_and_cleanup_legacy` 12-cycle cadence | cycle_count gate + 4-HEAD verify 본체 보존 | `runner.py:131-145` (timeout wrap 만 추가) / `runner.py:322-418` 본체 무변경 |
| MCT-160 D2 forward window | `[today-1, today]` boundary 보존 | Layer 1 helper start_date=today-1 / end_date=today |
| MCT-203 NAS GET size-gated cache | NAS GET 호출 수 감소 → cache hit ratio 개선 (synergy) | 회귀 0 |
| MCT-173 backfill | `iter_frozen_segments` + `BackfillManifest` 무변경 | `run_backfill` 본체 무변경 |
| ADR-029 D1=B L1 NAS DualWriter | `L1Compactor.put_l1()` 무변경 | 본 Story 가 caller 측 변경 0 |
| ADR-034 nas_key SSOT helper | `build_nas_key()` 재사용 (`historical_reclaim.py` 가 신규 caller) | helper signature 보존 |
| MCT-159 #48 orderbookdepth `NotImplementedError` | Layer 1+2+3 모두 `channel != orderbookdepth` 의무 X — `_CHANNELS_FOR_L2` 가 orderbookdepth 포함 (기존 동형) | `L2Compactor.compact_hour` 가 channel별 분기 (현행 정합) |

## §5 INV (8종) — Architect lane §5

| ID | Invariant | grep gate / test |
|---|---|---|
| INV-A | forward `_run_l2` / `_run_l3` 가 historical partition 미접근 (date < today-1 가 0-read) | `tests/integration/test_compactor_forward_rglob_scope.py` (historical partition fixture + open file count = 0 박제) + grep gate `runner.py::_run_l2` 안 `(self._root / "market").rglob` 패턴 0 |
| INV-B | promote-historical 가 today/yesterday partition 미접근 (date >= today-1 = abort + 0-unlink) | `historical_reclaim.py` 의 `skip_today_window` early-return + CLI `promote-historical --start <today>` abort + `tests/integration/test_promote_historical_today_abort.py` |
| INV-C | L2 NAS HEAD verify fail 시 L1 unlink 0 (안전망) | `historical_reclaim.py` 의 `skip_nas_missing` early-return + integration test |
| INV-D | sentinel `.l1-promoted` 멱등 (재실행 same partition = 0 호출) | `historical_reclaim.py` 의 sentinel.exists() early-return + integration test 2nd-run skip 박제 |
| INV-E | per-step stall timeout 후 다음 step 진입 (starvation 차단) | `runner.py::_run_step_with_timeout` `asyncio.TimeoutError` catch + log + `_run_l3` continue + integration test stall sim |
| INV-F | sentinel write atomic (tempfile + os.replace, partial sentinel 차단) | `historical_reclaim.py` 의 `os.replace(tmp, sentinel)` + integration test (mock-interrupt sim) |
| INV-G | pidfile lock — promote-historical 동시 실행 시 second 인스턴스 abort | CLI 진입 시 `<root>/audit/historical-reclaim.pid` flock exclusive (MCT-173 BackfillManifest pid 패턴 동형) — 2nd run = exit 2 |
| INV-H | partition tuple `(exchange, symbol, channel, date)` 격리 — forward ∩ historical = ∅ | INV-A + INV-B 합집합 + `tests/integration/test_compactor_tick_isolation.py` |

## §6 위험 + 완화 (OperationalRiskArch 변호)

| 위험 | 발생 시나리오 | 완화 |
|---|---|---|
| Layer 2 `asyncio.wait_for` timeout 이 cycle_count drift 유발 | timeout 으로 cleanup step skip → cycle_count 증가 못함 → 다음 12-cycle gate 미트리거 | cycle_count 가 `_tick` 진입 시점 (timeout 무관) 증가하도록 보장 — `runner.py:131` 위치 보존. cleanup 자체는 cycle_count % 12 == 0 시점에 시도, timeout 시 drop, 다음 12-cycle = 다음 6-min 자연 재시도 |
| L2 NAS HEAD verify 단순화 (KeyCount > 0) 가 L2 partial PUT 케이스 unlink 유발 | NAS L2 partition 가 일부 hour 만 PUT 완료 + L1 unlink → 데이터 손실 | **(FIX 1/3 P1 #1)** MCT-202 D-3 sequential local-only flow + `_historical_dual_write` re-raise (4xx fail-fast, ADR-027 §D5 INCIDENT-2026-05-17 정합) 가 partition-level atomic 보장 — partition 의 24-hour L2 모두 PUT committed 후 L1 reclaim 진입. partial completion 시 partition loop abort + reclaim 진입 0 (P1 #10 Codex 3 결정). INV-C 안전망 보조 (KeyCount=0 시 unlink 0). 추가 layer **본 Story scope 안 박제** (Phase 2 결정 모호 표현 제거): partition-level L2 file count match (local L2 hour file 수 == NAS L2 KeyCount) 검증 = GATE-2 단순 KeyCount > 0 + local L2 root 존재 합치 (full per-file HEAD 는 비용 폭증 회피, ADR-029 D3 amendment box 정합) |
| promote-historical 동시 실행 (operator 실수 2회 시작) | 동시 unlink + sentinel race | INV-G pidfile flock (MCT-173 BackfillManifest 패턴 동형) — second 인스턴스 exit 2. **(FIX 1/3 P2 #6 wording)** pidfile grace 60s = `flock(LOCK_EX|LOCK_NB)` release 의존 (PID content 의존 X) — atexit handler + SIGTERM trap 이 flock release 직접 호출, 60s = stale lock detection timeout (`flock` retry interval) |
| forward eager cascade 회귀 (MCT-202 AC 위반) | Layer 1+2 변경이 `_dispatch_dual_write source_to_delete` 의도 위반 | runner.py:288 (forward `_dispatch_dual_write source_to_delete=parquet_path`) + runner.py:494 (historical `_historical_dual_write source_to_delete=source_to_delete` caller-controlled) 양쪽 line touch 0 — grep gate `tests/integration/test_eager_cascade_regression.py` (MCT-202 회귀 박제) re-use |
| ADR amendment sibling sync 충돌 (mctrader-data Phase 1 PR ↔ mctrader-hub PR) | 두 PR merge order 가 ADR amendment 의 Proposed → Accepted 전환 시점에 영향 | derived default: mctrader-data Phase 1 = "Proposed" status 박제 + mctrader-hub sibling PR 가 같은 phase. mctrader-data Phase 2 (impl) merge 후 mctrader-hub PR merge → Proposed → Accepted 전환. ADR-020 Amendment 1 §결정 9 (joint-phase narrow form) 정합 |
| **(FIX 1/3, P0 #2)** Layer 2 `asyncio.wait_for` timeout drop 이 main thread 만 unblock — worker thread ThreadPoolExecutor slot 영구 점유 + 다른 step starvation | NAS GET stall 시 worker thread cancel 불가 (Python 제약) → 다음 cycle 마다 새 worker spawn + stall → default ThreadPoolExecutor `min(32, cpu+4)` slot 영구 점유 + 다른 step (cleanup, l3) starvation | **3중 lock 박제** (Layer 2 본문 cross-ref): (1) boto3 `read_timeout=120s` + `connect_timeout=30s` (root cause fix, ADR-027 §D5 INCIDENT-2026-05-19 amendment "silent stall 차단" base layer); (2) dedicated `ThreadPoolExecutor` per step (L2/L3/cleanup/historical 4 instance × max_workers=2, 총 8 thread cap, default executor 격리); (3) `asyncio.wait_for=600s` outer (main thread unblock last-resort). Phase 2 후속 full asyncio task 분리 별 Story carry-over (P2 #1) |
| **(FIX 1/3, P0 #3)** day boundary race — UTC 00:00:00 sliding window 시점 partition tuple race | forward `_run_l2_for_parquet` in-flight (now=N+1 직전, window=[N-1, N]) + historical reclaim 진입 (now=N+1 직후, 이전 partition date=N-1 boundary 밖) → `(ex, sym, channel, N-1)` partition tuple 동시 access race. INV-H "0 overlap strict" claim 위반 가능 | **2 layer mitigation 박제** (Layer 3 본문 cross-ref): (1) monotonic `now_snapshot per cycle` — `_tick` / `run_historical_promotion` 진입 시점 single date 박제 후 caller chain 전체 전달; (2) partition-level `.forward-processing` sentinel — forward `_run_l2_for_parquet` 진입 시 write/완료 unlink, historical reclaim 가 sentinel 존재 시 `skip_forward_in_flight` outcome return. INV-H wording 약화: "best-effort ∅ overlap (single cycle snapshot 기준) + `.forward-processing` sentinel 멱등 cross-cycle race 차단" |

## §7A 보안 설계 (FIX 1/3 P0 #1 — SecurityArch + OperationalRiskArch deputy 통합)

Story §7A verbatim mirror. 본 § = Change Plan 측 박제 (DesignReviewPL P0 #1 lane checklist "§7 보안 설계 누락 → P0" 차단 해소).

### §7A.1 trust boundary

- **내부 compactor 경로 only** — forward `_tick` / `_run_l2` / `_run_l3` / `historical_reclaim.py` / `cli.py::promote-historical` 모두 mctrader-data 컨테이너 내부
- **외부 input 0** — CLI arg = `--start/--end YYYY-MM-DD` + `--exchange/--channel` enum (`allowlist.py::validate_channel_exchange` 재사용)
- **NAS endpoint** = 기존 DualWriter creds (ADR-027 §D5 NAS PUT 4xx fail-fast wired) 재사용

### §7A.2 threat model

- **N/A — 외부 attack surface 0**. compactor = internal background worker
- 사유 (10자+): "내부 compactor 경로, 외부 input 0, 신규 attack surface 0"

### §7A.3 auth/authz

- **N/A — 기존 DualWriter creds 재사용**. `historical_reclaim.py` 가 `nas_uploader._s3` 동일 boto3 client 사용. IAM 권한 변경 = 0
- 사유 (10자+): "기존 NAS DualWriter IAM 재사용, 신규 권한 0"

### §7A.4 운영 리스크 5 sub-items (OperationalRiskArch deputy 본 §)

| sub-item | 상태 | 박제 |
|---|---|---|
| **DR (disconnect recovery)** | 본문 | NAS endpoint disconnect 시 `boto3 EndpointConnectionError` raise → `ReclaimOutcome(outcome="fail_verify")` early-return + L1 unlink 0. 다음 6-min cycle 자연 재시도 |
| **disconnect handling** | 본문 | NAS GET stall = 본 Story 진정 mitigation. Layer 2 3중 lock (boto3 read_timeout=120s + dedicated executor + asyncio.wait_for=600s) |
| **clock drift / boundary** | 본문 | P0 #3 mitigation — monotonic snapshot per cycle + `.forward-processing` sentinel 멱등 |
| **rate-limit** | N/A | partition tuple 별 1 LIST + 1 HEAD, production 4,608 partition × 2 / 6-min = 25.6 req/sec — well below MinIO default |
| **env-isolation** | 본문 | `MCTRADER_COMPACTOR_STEP_TIMEOUT_SECONDS` + boto3 client config production-scope only |

### §7A.5 민감 데이터

- **N/A — 로그 메시지 = enum + 수치 only**. PII / API key / wallet address 0
- 사유 (10자+): "log 메시지 enum + 수치 only, PII/secret 0"

### §7A.6 위협↔완화 매트릭스

- **N/A — 외부 위협 0**. 내부 race+stall = §7A.4 박제
- 사유 (10자+): "외부 위협 0, 내부 race+stall = §7A.4 박제"

### §7A.7 검증 의무

- §7A.1 본문 ✅ / §7A.2 N/A 30자 ✅ / §7A.3 N/A 26자 ✅ / §7A.4 5 sub-items 박제 ✅ / §7A.5 N/A 30자 ✅ / §7A.6 N/A 27자 ✅

## §7 의존성 (Story §7 cross-ref)

[Story §7 verbatim](../stories/MCT-204.md#7-의존성--architect-5-inv--6-위험) 의 의존 Story / ADR 목록 (§4.3 Continuity 표) 그대로 인용. 추가 prerequisite 0.

- **prereq**: MCT-202 (forward eager cascade, merged 2026-05-18) + MCT-189 (scan_and_cleanup_legacy, merged) + MCT-160 D2 (forward window, merged) + MCT-203 (NAS GET cache, merged 2026-05-18) + MCT-173 (backfill, merged) + ADR-029 D1=B (L1 NAS DualWriter, merged 2026-05-14) + ADR-034 (nas_key SSOT, merged)
- **downstream**: Phase 2 후속 operator script `_reclaim/incremental_l1_reclaim.py` deprecate (separate Story)
- **cross-repo isolation**: mctrader-hub ADR amendment 3종 (ADR-027 §D5 + §D7 + ADR-029 D1=B) — sibling PR same phase

## §8 Test Contract (TestContractArchitectAgent deputy owner)

### §8.1 Unit test (Phase 2)

| file | 책임 | LoC 예상 |
|---|---|---|
| `tests/unit/compactor/test_discovery_helper.py` | `_discover_partitions_in_range` boundary (today/yesterday/historical mix, tier=L1/L2, empty channel_root) | +60 |
| `tests/unit/compactor/test_step_timeout_wrap.py` | `_run_step_with_timeout` timeout / success / exception 3 branch | +40 |
| `tests/unit/compactor/test_historical_reclaim_unit.py` | `reclaim_partition_l1_local` 5 outcome branch (ok/skip_sentinel/skip_today_window/skip_nas_missing/fail_verify) | +120 |

### §8.2 Integration test (Phase 2)

| file | 책임 | AC/INV 박제 | LoC 예상 |
|---|---|---|---|
| `tests/integration/test_compactor_tick_isolation.py` | asyncio step 격리 — L2 stall sim → L3/cleanup 진입 박제 | AC-1, INV-E, INV-H | +250 |
| `tests/integration/test_compactor_forward_rglob_scope.py` | historical fixture 생성 → `_run_l2` 호출 → historical file open count = 0 박제 | AC-2, INV-A | +120 |
| `tests/integration/test_historical_l1_reclaim.py` | partition fixture + L2 NAS HEAD mock → L1 unlink + sentinel write + 2nd-run idempotent | AC-3, INV-C, INV-D, INV-F | +180 |
| `tests/integration/test_promote_historical_today_abort.py` | CLI date >= today-1 입력 → exit 2 + log error | INV-B | +50 |
| `tests/integration/test_promote_historical_pidfile_lock.py` | 동시 실행 시 second 인스턴스 exit 2 | INV-G | +40 |
| `tests/integration/test_l3_dispatch_normal.py` | L2 task 완료 무관 L3 진행 (cadence-only) | AC-4 | +60 |

### §8.3 Performance baseline — FIX 1/3 P1 #5 protocol 박제

**measurement environment**: Phase 2 integration test fixture (`tests/integration/test_compactor_forward_rglob_scope.py`) + production parity dev container (`docker compose --profile dev`).

**fixture 생성 절차**:
1. **synthetic historical L1 fixture** — `tmp_path / "market" / "orderbooksnapshot" / "schema_version=v1" / "tier=L1"` 안 5 date × 5 symbol × 24 hour = 600 part-*.parquet 파일 (각 1KB stub) 생성. date range = `[today - 7, today - 2]` (forward window 밖).
2. **synthetic forward L1 fixture** — `tier=L1` 안 today + yesterday × 5 symbol × 24 hour = 240 part-*.parquet (forward window).
3. **monitoring**: `pyfakefs` 또는 `unittest.mock.patch` 로 `Path.rglob` 호출 capture. file open count = `len(list(_run_l2(...)))` 측정.

**measurement protocol**:
- **iteration**: 30 reps per AC (MCT-148 NFR-2 협약 답습). mean / p95 / p99 보고.
- **mean 10% 회귀 baseline**: PR-time 측정 vs main branch baseline mean — 10% deviation = `pytest --rolling-baseline` (MCT-148 baseline rolling pattern 답습) flag warning.
- **CI signal**: `tests/integration/test_compactor_*` 통합 PR-time 실행 (≤ 5 min, MCT-148 dev container 정합).

**AC-2 측정**:
- Before (현재 production): `_run_l2` 1 invocation file open count = 16,918 (production 5/13~17 실측 박제, py-spy 확인)
- After (목표): forward partition file count × 1.2 — production fixture 기준 forward partition file count ≈ 50 (today=0 + yesterday minimal) → 목표 ≤ 60. fixture (24h × 5 sym × 2 day) = 240 base → 목표 ≤ 288.
- **assertion (Phase 2 integration test)**: `assert _run_l2_file_open_count <= forward_partition_file_count * 1.2`

**AC-1 측정**:
- Before: cleanup_cycle_delay_seconds = ∞ (영원히 미진입)
- After (**FIX 1/3 P1 #11 Codex 4**): **steady state ≤ 300s** (5분 = `SCAN_INTERVAL_SECONDS 30s × 10 cycle margin`) **+ container restart 직후 첫 1 cycle 면제 (≤ 360s, restart edge case)**. Prometheus alert rule = `(cleanup_cycle_delay > 300) and (up{job="compactor"} == 1 for 1m)` (restart 직후 1분 면제).

**AC-3 측정**:
- production 5/13~17 fixture: ok+skip 합계 ≈ 20,754 (Story §6 AC-3 예상치). fail = 0 박제.
- **assertion (Phase 2 integration test)**: `assert reclaim_outcome_counter[("ok", "skip_sentinel", "skip_nas_missing", "skip_forward_in_flight")].total > 0 and reclaim_outcome_counter[("fail_verify",)].total == 0`

### §8.4 §8.5 Stateful / restart invariant (TestContractArch — PL 결정 §8.5_active=true)

| 항목 | 박제 의무 |
|---|---|
| asyncio task lifecycle | timeout drop + log warning + 다음 step 진입 — task cancel propagation 안전 |
| sentinel restart-aware | container restart 시 sentinel 존재 partition = skip (2nd-run idempotency 박제) |
| cycle_count drift | `_tick` 진입 시점 (timeout 무관) 증가 → process restart 시 0 로 reset 정상 (best-effort cadence, 임의 partition 손실 0) |
| pidfile orphan cleanup | promote-historical pidfile = SIGTERM trap + atexit handler — stale pidfile 자동 cleanup (60s grace) |

### §8.5 verify gate script

`scripts/verify_historical_l1_reclaim.py` — 별 verify:
- partition 별 L1 local file count vs sentinel 존재성 cross-check
- NAS L2 KeyCount > 0 인 partition 의 L1 local part-*.parquet count == 0 박제
- 4 metric Prometheus query (cleanup_cycle_delay / step_stall / historical_reclaim / l3_pending) — 정상 emit 박제

## §9 ADR 판단

본 Story 는 신규 ADR 생성 없음. 기존 ADR amendment 3종:

| ADR amendment | 위치 (mctrader-hub) | 박제 내용 | 본 Story merge 후 status |
|---|---|---|---|
| ADR-027 §D5 INCIDENT-2026-05-19 amendment | `c:/workspace/mclayer/mctrader-hub/docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md` | _tick stall pattern (cooperative scheduling head-of-line blocking) — silent stall 차단 (per-step timeout + Counter emit) | Proposed (Phase 1) → Accepted (Phase 2 merge 후 mctrader-hub sibling PR merge) |
| ADR-027 §D7 amendment | 동일 file | forward path partition discovery boundary — `_discover_partitions_in_range` start_date=today-1 / end_date=today 강제 (rglob outer iter 차단) | 동상 |
| ADR-029 D1=B amendment | `c:/workspace/mclayer/mctrader-hub/docs/adr/ADR-029-tier-promotion-single-source.md` | historical L1 reclaim verify-after pattern (L2 NAS HEAD verify pass + sentinel `.l1-promoted` 멱등 박제) | 동상 |

**derived default 결정 (chief)**: mctrader-data Phase 1 PR = Change Plan + Story file 박제 + ADR amendment **"Proposed"** status 본문 박제 (in-line draft). mctrader-hub sibling PR open same phase (별 branch). mctrader-data Phase 2 (impl) merge 후 mctrader-hub PR merge → Proposed → Accepted 전환. (ADR-020 Amendment 1 §결정 9 joint-phase narrow form 정합.)

## §10 Phase 구조 (Story §9 mirror)

### Phase 1 (Spec/Doc-only PR, 본 PR #185)

- `docs/stories/MCT-204.md` §1-§11 완성 (요구사항 lane + 본 lane)
- `docs/change-plans/mct-204-compactor-tick-stall-historical-l1-reclaim.md` (본 file)
- `docs/superpowers/specs/2026-05-19-compactor-tick-stall-historical-l1-reclaim.md` (이미 박제)
- ADR amendment 3종 sibling PR (mctrader-hub) — same phase open

### Phase 2 (Impl PR)

- `src/mctrader_data/compactor/runner.py` Layer 1+2 수정 (+350/-120 LoC)
- `src/mctrader_data/compactor/historical_reclaim.py` Layer 3 신설 (+180 LoC)
- `src/mctrader_data/cli.py` promote-historical INV-B abort guard + INV-G pidfile lock (+20/-5 LoC)
- `src/mctrader_data/metrics.py` 신규 4 metric (+60 LoC)
- tests 9 file 신설 (§8.1 + §8.2) (+820 LoC)
- `scripts/verify_historical_l1_reclaim.py` (+150 LoC)
- `CLAUDE.md` 박제 (compactor _tick stall isolation + historical L1 reclaim path 섹션 신설)

## §11 데이터 마이그레이션 (DataMigrationArchitectAgent deputy owner)

### §11.1 Schema 영향

- Parquet schema 변경 = **0** (L1 / L2 / L3 col layout 보존)
- NAS object key 변경 = **0** (ADR-034 flat namespace 보존)
- NAS bucket policy / IAM = **0** (기존 DualWriter creds 재사용)

### §11.2 Migration 전략

- **forward-only** (schema migration 0, NAS re-key 0)
- 신규 sentinel `.l1-promoted` = partition-level zero-byte marker (`.compacted` namespace 정합)
- 기존 partition 의 sentinel 부재 → 첫 reclaim 사이클 = 4-HEAD verify 수행 → 정상 처리 (backward-compat)

### §11.3 Rollback

- sentinel `.l1-promoted` 단순 rm → 다음 reclaim 사이클 재실행 (4-HEAD verify 재확인 = idempotent)
- Layer 1+2 rollback = runner.py revert → 기존 sequential 동작 복원 (state 손실 0)
- Layer 3 rollback = historical_reclaim.py 모듈 unimport + runner.py caller 제거 → L1 회수 중단 (data loss 0, NAS L2 = SoT 유지)

### §11.4 Data integrity invariant (§5 cross-ref)

- INV-C: L2 NAS HEAD verify fail 시 L1 unlink 0 (안전망)
- INV-D: sentinel 멱등 (re-entry 안전)
- INV-F: sentinel write atomic (tempfile + os.replace, partial sentinel 차단)

### §11.5 forward / historical race 격리 (DataMigration + OpRiskArch 통합) — FIX 1/3 갱신

- partition tuple `(exchange, symbol, channel, date)` 격리 (**FIX 1/3 P0 #3 wording 약화**):
  - forward = `[now_snapshot - 1, now_snapshot]` only (**monotonic snapshot per cycle 기반**, sliding window race 차단)
  - historical = `date < now_snapshot - timedelta(days=1)` only (caller monotonic snapshot 전달)
- intersection = **best-effort ∅ (single cycle snapshot 기준)** — strict claim 폐기
- cross-cycle race 멱등 차단 (FIX 1/3 P0 #3 2 layer mitigation):
  1. **monotonic `now_snapshot` per cycle** — `_tick` / `run_historical_promotion` 진입 시점 단일 박제, caller chain 전체 전달
  2. **`.forward-processing` sentinel** — forward `_run_l2_for_parquet` 진입 시 write, finally unlink. historical `reclaim_partition_l1_local` 가 sentinel 존재 시 `skip_forward_in_flight` outcome return → 다음 cycle 자연 재시도 (멱등)
- 추가 안전망 (INV-G): pidfile flock — promote-historical 동시 실행 시 second exit 2

### §11.5.1 Backfill (FIX 1/3, P1 #6 sub-section 신설)

production 5/13~17 historical L1 130 GB 회수 = **operator one-shot CLI invocation** (`promote-historical`) per (exchange, channel, date range). batch size / throttle 정책:

- **batch_size**: 1 partition `(exchange, symbol, channel, date)` 가 base unit. CLI invocation 당 partition 수 = `len(symbols) × len(date_range)`. 5/13~17 upbit orderbooksnapshot ≈ 192 symbol × 5 day = 960 partition.
- **throttle**: per-partition sequential (parallelism = 1). 각 partition reclaim = 24-hour L2 dual_write + 1 L3 dual_write + 1 reclaim hook 호출 → 약 30s/partition × 960 = 8h 1회 invocation. operator 가 야간 invocation 권고 (peak hour 회피).
- **interruption 안전**: SIGTERM trap → flock release + 현재 partition 의 forward_sentinel cleanup. 재실행 시 sentinel 존재 partition skip (멱등 보장).
- **rollback**: sentinel `.l1-promoted` rm → 다음 invocation 가 4-HEAD verify 재실행 + L1 unlink 0 (이미 unlink 됨, idempotent).

### §11.5.2 §4 API 분류 (FIX 1/3, P1 #7)

본 Story 의 외부-facing 변경:

| 변경 | 분류 | 영향 |
|---|---|---|
| CLI `promote-historical --start <today>` abort guard 추가 | **breaking** (operator) — 기존 `--start <today>` 입력 시 정상 작동 → 본 Story 후 abort + exit 2 + log error | operator runbook 갱신 의무 (CLAUDE.md Phase 2 박제). 영향 = operator manual invocation only (production 자동 호출 0). |
| CLI `promote-historical --channel <ch>` enum 검증 (allowlist.py 재사용) | additive | 기존 정합, 신규 영향 0 |
| `_discover_partitions_in_range(tier="L1")` default parameter 추가 | additive (internal) | 기존 caller (`run_historical_promotion`) 동작 동일 (default 값 보존) |
| `ReclaimOutcome` 신규 dataclass | additive (internal) | 신규 module, 기존 caller 0 |
| `DualWriter._uploader` 노출 (historical_reclaim caller 가 boto3 client 접근) | **internal-only** | mctrader-data 패키지 내부 사용, public API 변경 0 |
| 4 신규 Prometheus metric | additive | Grafana dashboard 신규 패널 추가 (별 작업) |

본 Story 의 외부 API breaking 변경 = **1건** (CLI abort guard). production 자동 호출 무관 (operator manual 영역). dev/staging operator 가 새 invariant 인지 의무 (Phase 2 LAND 시 commit message + CLAUDE.md 박제).

### §11.6 Idempotency (DataMigration primary + OperationalRiskArch consult) — FIX 1/3 갱신

- sentinel-based: partition `date_dir / ".l1-promoted"` existence check (POSIX atomic, `os.replace(tmp, sentinel)`)
- **forward in-flight sentinel (FIX 1/3 P0 #3 신설)**: `date_dir / ".forward-processing"` — forward `_run_l2_for_parquet` 진입 시 `touch()`, finally `unlink(missing_ok=True)`. cross-cycle race 멱등 차단 (historical reclaim 가 sentinel 존재 시 `skip_forward_in_flight` outcome return → 다음 cycle 자연 재시도)
- restart safety: container restart 후 sentinel 존재 partition = skip (`.l1-promoted` 박제 partition). `.forward-processing` 가 restart 시 잔존 시 (process kill 도중) → 다음 cycle forward `_run_l2_for_parquet` 진입 시 새 `touch()` 가 idempotent 덮어쓰기, finally unlink 정상 진행. historical reclaim 가 sentinel 존재 시 skip → forward 가 finally unlink 후 다음 reclaim cycle 정상 진행
- pidfile cleanup: SIGTERM trap + atexit handler — flock release **직접 호출** (PID content 의존 X). 60s = `flock(LOCK_EX|LOCK_NB)` retry interval (second 인스턴스 가 stale lock 감지하는 timeout), stale pidfile auto-cleanup 의 의미
- forward path 무관: forward `_dispatch_dual_write` 의 `source_to_delete` MCT-202 D-1 cascade 가 별도 idempotency 보장 (HEAD-then-PUT sha256 + atomic unlink)
- monotonic snapshot: `_tick` / `run_historical_promotion` 진입 시점 single date 박제 → 동일 cycle 내 모든 boundary 비교 single snapshot 기준. restart 후 새 cycle = 새 snapshot (정상 idempotent)

### §11.7 Cross-repo isolation (ADR-034 §결정 5 정합)

- engine (`mctrader-engine`) = candles namespace only — market data L1 namespace 미참조 (verified-via engine `historical.py:42,65,87`)
- 본 Story 가 market data L1 namespace 변경 = **0** (sentinel = local-only marker, NAS object 영향 0)
- cross-repo impact = **ZERO**

---

## Appendix A — 6 deputy synthesis 채택 / 반박 trace

| Deputy 제안 | Chief 채택 / 반박 | 근거 |
|---|---|---|
| CodebaseMapper: `_discover_partitions_in_range` re-use | **채택** | helper 본문 박제 (rglob c169720 fix 정합) + signature backward-compat |
| Refactor: full asyncio task 분리 (별 long-lived task) | **부분 반박** — `asyncio.wait_for` timeout wrap 으로 단순화 | task lifecycle 복잡도 + cycle_count drift 위험. AC-1 invariant 는 timeout drop 으로 충족 |
| SecurityArch: IAM 권한 변경 0 (기존 DualWriter creds 재사용) | **채택** | attack surface = forward/historical date 격리 invariant 가 차단 |
| TestContractArch: §8.5 stateful invariant active=true | **채택** (PL 결정 정합) | asyncio task lifecycle + sentinel restart-aware + cycle_count drift = §8.5 trigger 4 조건 중 2 충족 (background worker + restart-aware) |
| DataMigrationArch: sentinel = zero-byte marker + os.replace atomic | **채택** | `.compacted` 패턴 동형 + content-free 단순성 |
| OperationalRiskArch: pidfile flock + per-step stall timeout | **채택** | INV-G + INV-E 박제 |

## Appendix B — sibling PR ADR amendment 본문 (mctrader-hub)

별 `docs/adr-amendments/mct-204-proposed-amendments.md` 본문 미박제 — Phase 1 commit 에 ADR amendment 본문은 **mctrader-hub sibling PR 별 worktree (`c:/workspace/mclayer/mctrader-hub/.claude/worktrees/mct-204-adr` 신규)** 에서 직접 ADR file edit. mctrader-data Phase 1 PR comment 에 sibling PR URL cross-ref.
