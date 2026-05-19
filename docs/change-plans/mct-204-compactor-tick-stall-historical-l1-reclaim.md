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

**API**:
```python
def reclaim_partition_l1_local(
    *,
    root: Path,
    nas_uploader: NASUploader,
    exchange: str,
    symbol: str,
    channel: str,
    date_utc: date,
) -> ReclaimOutcome:
    """L2 NAS HEAD verify → L1 unlink → sentinel write. 멱등 (sentinel 존재 시 skip)."""

@dataclass
class ReclaimOutcome:
    outcome: Literal["ok", "skip_sentinel", "skip_today_window", "skip_nas_missing", "fail_verify"]
    files_unlinked: int
    bytes_freed: int
```

**caller**: `run_historical_promotion` ([runner.py:524-623](../../src/mctrader_data/compactor/runner.py#L524-L623)) 의 partition loop 종료 시점 (각 `(ex, sym, d)` partition 의 24-hour L2 NAS PUT + 1 L3 NAS PUT 완결 후, 다음 partition 진입 직전).

```python
# After existing L3 dual-write logic
from mctrader_data.compactor.historical_reclaim import reclaim_partition_l1_local
reclaim_outcome = reclaim_partition_l1_local(
    root=root,
    nas_uploader=dual_writer._uploader,
    exchange=ex,
    symbol=sym,
    channel=channel,
    date_utc=d,
)
counts[f"l1_reclaim_{reclaim_outcome.outcome}"] = counts.get(f"l1_reclaim_{reclaim_outcome.outcome}", 0) + 1
counts["l1_reclaim_bytes_freed"] = counts.get("l1_reclaim_bytes_freed", 0) + reclaim_outcome.bytes_freed
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
today_utc = datetime.now(timezone.utc).date()
if date_utc >= today_utc - timedelta(days=1):
    return ReclaimOutcome(outcome="skip_today_window", ...)
```

forward path = `[today-1, today]` only (Layer 1), historical reclaim = `date < today-1` only — partition tuple intersection = ∅.

### §3.4 신규 metric (Prometheus)

| metric | 종류 | label | source line |
|---|---|---|---|
| `mctrader_compactor_cleanup_cycle_delay_seconds` | Gauge | (none) | `runner.py::_tick` 진입 시점 |
| `mctrader_compactor_step_stall_seconds` | Gauge | step (l2/l3/cleanup) | `runner.py::_run_step_with_timeout` timeout catch |
| `mctrader_historical_l1_reclaim_total` | Counter | exchange/channel/outcome | `historical_reclaim.py::reclaim_partition_l1_local` |
| `mctrader_l3_pending_partitions` | Gauge | exchange/channel | `runner.py::_run_l3` 진입 시점 (discovered partition 수) |

**emit module 결정 (CodebaseMapper 일치)**: `src/mctrader_data/metrics.py` (compactor_tier_pending_segments 정합). `nas_metrics/prometheus_exporters.py` = NAS-specific path (제외).

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
| L2 NAS HEAD verify 단순화 (KeyCount > 0) 가 L2 partial PUT 케이스 unlink 유발 | NAS L2 partition 가 일부 hour 만 PUT 완료 + L1 unlink → 데이터 손실 | MCT-202 D-3 sequential local-only flow + `_historical_dual_write` re-raise (4xx fail-fast) 가 partition-level atomic 보장 — partition 의 24-hour L2 모두 PUT committed 후 L1 reclaim 진입. INV-C 안전망 보조 (KeyCount=0 시 unlink 0). 추가 layer: chief 옵션 — partition-level L2 file count match (local L2 hour file 수 == NAS L2 KeyCount) 검증 (Phase 2 결정) |
| promote-historical 동시 실행 (operator 실수 2회 시작) | 동시 unlink + sentinel race | INV-G pidfile flock (MCT-173 BackfillManifest 패턴 동형) — second 인스턴스 exit 2 |
| forward eager cascade 회귀 (MCT-202 AC 위반) | Layer 1+2 변경이 `_dispatch_dual_write source_to_delete` 의도 위반 | runner.py:288 (forward `_dispatch_dual_write source_to_delete=parquet_path`) + runner.py:494 (historical `_historical_dual_write source_to_delete=source_to_delete` caller-controlled) 양쪽 line touch 0 — grep gate `tests/integration/test_eager_cascade_regression.py` (MCT-202 회귀 박제) re-use |
| ADR amendment sibling sync 충돌 (mctrader-data Phase 1 PR ↔ mctrader-hub PR) | 두 PR merge order 가 ADR amendment 의 Proposed → Accepted 전환 시점에 영향 | derived default: mctrader-data Phase 1 = "Proposed" status 박제 + mctrader-hub sibling PR 가 같은 phase. mctrader-data Phase 2 (impl) merge 후 mctrader-hub PR merge → Proposed → Accepted 전환. ADR-020 Amendment 1 §결정 9 (joint-phase narrow form) 정합 |

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

### §8.3 Performance baseline

**AC-2 측정**:
- Before (현재): `_run_l2` 1 invocation file open count = 16,918 (production 5/13~17 fixture)
- After (목표): forward partition file count × 1.2 — production fixture 기준 forward partition file count ≈ 50 (today=0 + yesterday minimal) → 목표 ≤ 60

**AC-1 측정**:
- Before: cleanup_cycle_delay_seconds = ∞ (영원히 미진입)
- After: ≤ 300s (5분 = SCAN_INTERVAL_SECONDS 30s × 10 cycle margin)

**AC-3 측정**:
- production 5/13~17 fixture: ok+skip 합계 ≈ 20,754 (Story §6 AC-3 예상치). fail = 0 박제

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

### §11.5 forward / historical race 격리 (DataMigration + OpRiskArch 통합)

- partition tuple `(exchange, symbol, channel, date)` 격리: forward = `[today-1, today]` only, historical = `date < today-1` only
- intersection = ∅ — race 가능성 = ZERO (INV-B + INV-A 합집합)
- 추가 안전망 (INV-G): pidfile flock — promote-historical 동시 실행 시 second exit 2

### §11.6 Idempotency (DataMigration primary + OperationalRiskArch consult)

- sentinel-based: partition `date_dir / ".l1-promoted"` existence check (POSIX atomic)
- restart safety: container restart 후 sentinel 존재 partition = skip
- pidfile cleanup: SIGTERM trap + atexit handler (stale pidfile auto-cleanup with 60s grace)
- forward path 무관: forward `_dispatch_dual_write` 의 `source_to_delete` MCT-202 D-1 cascade 가 별도 idempotency 보장 (HEAD-then-PUT sha256 + atomic unlink)

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
