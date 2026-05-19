---
title: compactor _tick stall 해소 + historical L1 회수 경로 신설
date: 2026-05-19
status: brainstorm-spec
story_key: MCT-204 (예약)
phase_structure: 1 Story = 2 PRs (Phase 1 Spec/Doc + Phase 2 Impl)
estimated_loc: 600-900
authored_by: Orchestrator (codeforge:brainstorm Phase 1 + PMOAgent 2nd pass)
---

# compactor _tick stall 해소 + historical L1 회수 경로 신설

## §0 사용자 호소 + evidence

**사용자 호소 (2건)**:
1. "이전에 compact 완료된 데이터는 삭제하라 했는데 여전히 해결되지 않아 계속 로컬 디스크가 낭비되고 있다."
2. "함께 L3로 데이터도 잘 쌓이고 있지 않다."

두 호소가 **단일 origin**.

### evidence

| 항목 | 측정값 | 의미 |
|---|---|---|
| `dual_write_result_total{tier=L1}` | 1914 | forward L1 dual-write 정상 |
| `dual_write_result_total{tier=L2}` | 2048 | forward L2 dual-write 정상 |
| `dual_write_result_total{tier=L3}` | **(없음 → 0)** | **forward L3 dispatch 0회** |
| `nas_key_helper_call_total{caller=runner_cleanup}` | **(없음 → 0)** | **scan_and_cleanup_legacy 호출 0회** |
| `compactor_tier_pending_segments{L1}` | 1914 (stuck) | sealed iter 누적 |
| ERROR / Traceback / "tick error" | 0 hits | exception 미발생 (= raise 아님) |

### py-spy live dump (pid 1, 2h up)

```
Thread 1 (idle): MainThread → asyncio event loop select (다음 tick 대기)
Thread 133 (idle): asyncio_0
    get_streaming (nas_storage/get_streaming.py:76)
    _compact_hour_nas (compactor/l2.py:231)
    compact_hour (compactor/l2.py:66)
    _run_l2_for_parquet (compactor/runner.py:190)
    _run_l2 (compactor/runner.py:167)
    run (concurrent/futures/thread.py:59)
```

→ worker thread 가 `_run_l2` NAS GET 안에서 영원히 진행 중. main thread 의 `await run_in_executor(_run_l2)` 가 대기 → _tick 의 step 4 (_run_l3) 와 step 6 (`_cycle_count++` + `scan_and_cleanup_legacy`) **영원히 진입 못함**.

### L1 historical 분포 (5/13~17)

| date | files | size |
|---|---|---|
| 2026-05-13 | 772 | 6.8 GB |
| 2026-05-14 | 4,608 | 38.7 GB |
| 2026-05-15 | 4,608 | 38.8 GB |
| 2026-05-16 | 4,607 | 30.9 GB |
| 2026-05-17 | 2,323 | 15.5 GB |
| 2026-05-18 | 0 | 0 |
| 2026-05-19 (today) | 0 | 0 |
| **합계** | **16,918** | **130.7 GB** |

→ forward eager cascade (MCT-202) 가 today/yesterday L1 정상 정리. **5/13~17 historical 130GB 가 forward `_run_l2` rglob outer iter 를 부풀려 stall 유발**.

### one-shot scan_and_cleanup_legacy 실행 결과 (operator 검증)

reclaim-helper 안 `python /tmp/one_shot_legacy_cleanup.py --loop --batch 500` 실행:

| 대상 | before → after | 회수 |
|---|---|---|
| L2 orderbookdepth | 11 G → 202 M | **10.8 GB** ✓ |
| L2 orderbooksnapshot | 787 M → 615 M | 172 MB ✓ |
| L1 orderbookdepth | 6.1 G → 6.1 G | 0 ✗ |
| L1 orderbooksnapshot | 128 G → 128 G | 0 ✗ |
| **사용 총량** | 225 G → 216 G | **9 GB** |

31 iter / 2,359 cleaned + 13,141 preserved (iter 10 이후 매 batch `cleaned=0, preserved=500`).

→ **`scan_and_cleanup_legacy` 는 L1 historical 130GB 회수에 본질적으로 부적합**. promote-historical 가 L1 → L2 변환 + L2 NAS PUT 만 함 (`source_to_delete=None`, [runner.py:471-473](src/mctrader_data/compactor/runner.py#L471-L473) 주석). L1 NAS 객체 부재 → `promote_l1` 4-HEAD verify fail = preserved.

## §1 핵심 진단 (single origin)

```
[_tick (sequential)]
   ├─ sealed scan + L1 compact loop
   ├─ if elapsed: _run_l2  ← rglob("*/tier=L1/**/part-*.parquet") outer iter
   │              ├─ historical 16,918 file iter (windows out, 부담만)
   │              └─ 400 unique (e,s,c) tuple × 48 dispatch = 19,200 worker-thread NAS GET
   │              └─ → 평균 6 sec / dispatch ≈ 32 시간 / 1 cycle
   │   ← worker thread stall 무한 await
   ├─ if elapsed: _run_l3  ❌ 영원히 진입 못함 → L3 dispatch 0 (사용자 호소 #2)
   ├─ _cycle_count++       ❌ 영원히 진행 못함
   ├─ scan_and_cleanup_legacy (12 cycles 마다)  ❌ 영원히 호출 0 (사용자 호소 #1)
   └─ run_gc
```

## §2 WHY (RequirementsAnalystAgent 분석)

- 명시 호소 = "디스크 정리·L3 쌓기"
- 실제 필요 = **sequential 차단 해소 + rglob 낭비 제거 + historical L1 회수 경로 신설**
- 단순 cleanup 트리거 버튼 추가가 아니라 forward 파이프라인 격리 + 스캔 범위 축소가 정답.

## §3 design (3 Layer, 1 Story 통합)

### Layer 1 — forward `_run_l2` / `_run_l3` rglob 축소

- **현행**: [runner.py:156](src/mctrader_data/compactor/runner.py#L156) `(self._root / "market").rglob("*/tier=L1/**/part-*.parquet")` outer iter 가 historical 16,918 file 까지 모두 iter.
- **fix**: 기존 `_discover_partitions_in_range(root, channel=..., start_date=today-1, end_date=today)` helper ([runner.py:419-455](src/mctrader_data/compactor/runner.py#L419-L455)) 재사용. forward outer iteration = `(exchange, symbol, date)` tuple discover (today/yesterday only).
- `_run_l3` 동형 (현재 `rglob("*/tier=L2/**/part-*.parquet")`).

### Layer 2 — `_tick` step 격리 (asyncio task 분리)

- **현행**: `_run_l2` / `_run_l3` / `scan_and_cleanup_legacy` 가 같은 _tick 안 sequential. 한 step worker stall 시 후속 진입 차단.
- **fix**: 각 step 을 별 `asyncio.create_task` 로 spawn. 각자 자기 cadence interval (L2=300s, L3=3600s, cleanup=별 cadence). 한 step 의 stall 이 다른 step 진입을 막지 않음.
- **per-step stall timeout**: env `MCTRADER_COMPACTOR_STEP_TIMEOUT_SECONDS` (default 600s) — timeout 시 log warning + 다음 step 진행. (Researcher Unknown #2 starvation 영구화 차단.)

### Layer 3 — historical L1 회수 경로 신설

- **현행**: promote-historical 가 L1 → L2 + L2 NAS PUT 만 (`source_to_delete=None`). L1 local 영구 보존.
- **fix**: `run_historical_promotion` (또는 `_historical_dual_write` batch 종료 hook) 가 `(exchange, symbol, channel, date)` partition batch 종료 시:
  1. 해당 partition 의 **L2 NAS HEAD verify** pass (incremental_l1_reclaim.py GATE-2 동형 pattern)
  2. pass 시 partition 의 L1 local `date_dir.rglob("part-*.parquet")` unlink
  3. sentinel `.l1-promoted` 멱등 마커 write (재실행 시 skip)
- **forward 와 race 격리**: forward path = today/yesterday only, historical = past dates (< today-1) only — partition tuple 격리 invariant 박제.

### 신규 metric (Prometheus)

- `mctrader_historical_l1_reclaim_total{exchange,channel,outcome}` Counter — AC-5
- `mctrader_l3_pending_partitions{exchange,channel}` Gauge — AC-4
- `mctrader_compactor_step_stall_seconds{step}` Gauge — AC-1 보조
- `mctrader_compactor_cleanup_cycle_delay_seconds` Gauge — AC-1 target

## §4 AC

- **AC-1**: `mctrader_compactor_cleanup_cycle_delay_seconds` ≤ 300s (5분). `_run_l2` worker stall 이 cleanup 진입을 차단 안 함.
- **AC-2**: forward `_run_l2` 1 invocation 당 rglob 호출 file 수 ≤ forward partition 수 × 1.2 (historical iter 0).
- **AC-3**: historical L1 회수가 forward path 대기 시간 0 (별 task / cadence).
- **AC-4**: `mctrader_l3_pending_partitions{exchange,channel}` Gauge 정상 emit + L3 dispatch 진행 (counter > 0).
- **AC-5**: historical L1 reclaim Counter `mctrader_historical_l1_reclaim_total{outcome=ok|skip|fail}` emit. production 5/13~17 130GB partition 모두 cleaned 또는 skip (preserved 0).

## §5 INV

- **INV-A**: forward path 가 historical partition 미접근. 즉 `_run_l2` / `_run_l3` 가 `date < today-1` partition 0-read. grep gate: `_run_l2` 본문에 `_discover_partitions_in_range` import 의무, rglob outer iter 0.
- **INV-B**: promote-historical 가 today/yesterday partition 미접근. CLI 진입 시 `date >= today-1` abort 게이트 + reclaim hook 가 today/yesterday partition 0-unlink.
- **INV-C**: L2 NAS HEAD verify fail 시 L1 unlink 0. 보존 fallback (안전망).
- **INV-D**: sentinel `.l1-promoted` 멱등. 재실행 시 같은 partition 0 호출 (incremental_l1_reclaim.py race noop 동형).

## §6 위험 + 완화

| 위험 | 완화 |
|---|---|
| Layer 2 asyncio 분리 가 기존 cycle counter / sentinel timing 의존성 회귀 | `tests/integration/test_compactor_tick_isolation.py` 박제 의무 — INV-A/B 박제 + stall 시뮬레이션 |
| Layer 3 reclaim INV-C 가 MCT-189 WS-B sweep 와 partition tuple race | partition tuple 격리 invariant grep gate + integration test 양쪽 박제 |
| forward _run_l2 가 historical backdated 진입 시 역할 경계 붕괴 (Researcher Unknown #1) | `_discover_partitions_in_range(start=today-1, end=today)` 강제 — backdate 가 들어와도 today/yesterday 만 |
| ADR amendment 2종 동시 file (ADR-027 §D5/§D7) | Phase 1 doc PR 단일 commit 처리 (sibling sync 충돌 회피) |

## §7 의존성

- **MCT-202** (eager post-compaction cleanup) — forward eager cascade `source_to_delete=Path` 보존 (이미 main, AC 회귀 검증)
- **MCT-189** (`scan_and_cleanup_legacy` WS-B sweep) — partition tuple 격리 invariant 신설 (already main, 본 Story 에서 INV-A 보강)
- **MCT-160 D2** (today+yesterday window) — forward window 정의 재사용
- **incremental_l1_reclaim.py** (operator script in `_reclaim/`) — Layer 3 가 동형 GATE-2 pattern 흡수 후 operator script deprecate 가능 (Phase 2 후속)

## §8 Phase 구조

- **Phase 1 (Spec/Doc-only PR)**: `docs/stories/MCT-204-*.md` Story 본문 (§1-11) + AC/INV 박제 + ADR-027 §D5/§D7 amendment + ADR-029 D1=B amendment (mctrader-hub cross-repo)
- **Phase 2 (Impl PR)**: runner.py 3 layer 동시 수정 + 신규 helper `compactor/historical_reclaim.py` + metric 3종 + tests 3 file 신설 + scripts/verify_historical_l1_reclaim.py + CLAUDE.md 박제

## §9 RETRO prereq (Phase 2 merge 후)

- `mctrader_compactor_cleanup_cycle_delay_seconds` Gauge 24h 관측 ≤ 300s
- `mctrader_historical_l1_reclaim_total{outcome=ok}` 증가 (production 5/13~17 partition 회수 진행)
- `mctrader_l3_pending_partitions` Gauge emit + L3 dispatch counter > 0
- `scripts/verify_historical_l1_reclaim.py` PASS (별 cross-check)

---

## scope_manifest (PMOAgent Phase 2 산출)

```yaml
story_key: MCT-204
story_slug: compactor-tick-stall-historical-l1-reclaim
phase_structure: 2-PR (Phase 1 Spec + Phase 2 Impl)
estimated_loc: 600-900

planned_adrs:
  count: 0
  rationale: 기존 ADR amendment 흡수 (ADR-027 §D5/§D7 + ADR-029 D1=B)
  amendments_planned:
    - adr_id: ADR-027
      section: §D5 INCIDENT amendment — _tick stall pattern (cooperative scheduling)
      target_repo: mctrader-hub
    - adr_id: ADR-027
      section: §D7 amendment — forward path partition discovery boundary
      target_repo: mctrader-hub
    - adr_id: ADR-029
      section: D1=B amendment — historical L1 reclaim verify-after pattern
      target_repo: mctrader-hub

planned_files:
  src_modifications:
    - src/mctrader_data/compactor/runner.py            # Layer 1+2+3 (+350/-120)
    - src/mctrader_data/compactor/historical_reclaim.py # Layer 3 신규 (+180)
    - src/mctrader_data/cli.py                          # promote-historical INV-B 게이트 (+20/-5)
  metrics:
    - src/mctrader_data/observability/metrics.py        # 신규 3 metric (+60)
  tests:
    - tests/integration/test_compactor_tick_isolation.py            # INV-A/B + stall sim (+250)
    - tests/integration/test_historical_l1_reclaim.py               # INV-C/D + AC-5 (+180)
    - tests/integration/test_compactor_forward_rglob_scope.py       # AC-2 grep gate (+120)
    - tests/unit/compactor/test_discovery_helper.py                 # boundary unit (+60)
  scripts:
    - scripts/verify_historical_l1_reclaim.py            # 별 verify (+150)
  docs:
    - docs/stories/MCT-204-compactor-tick-stall-historical-l1-reclaim.md  # Phase 1 본문 (+800)
    - docs/audit/MCT-204-tick-stall-evidence.md          # Phase 1 선결 evidence (+200)
    - docs/retros/2026-05-19-cfp-204-tick-stall-reclaim.md # Phase 2 merge 후 PMOAgent self-write (+180)
  cross_repo:
    - mctrader-hub:docs/adr/ADR-027-*.md   # §D5 + §D7 amendment
    - mctrader-hub:docs/adr/ADR-029-*.md   # D1=B amendment

planned_claude_md_sections:
  - section: compactor _tick stall isolation (MCT-204)
    type: NEW
  - section: historical L1 reclaim path (MCT-204)
    type: NEW
  - section: forward path partition discovery boundary
    type: NEW
  - section: Prometheus metrics (MCT-204 신설 3종)
    type: NEW
  - section: 관련 ADR (MCT-204 amendment 추가)
    type: APPEND

parallelism_judgment: sequential_single_story
  rationale:
    - Layer 1/2/3 가 runner.py 단일 파일 집중 (규칙 3 shared util)
    - INV-A/B/C/D 가 3 layer cross-invariant (vertical slice)
    - AC-1~5 단일 dashboard gate
    - 600-900 LoC = single Story 적정 (Epic 임계 1500+ 미달)
```

## brainstorming Phase 0 컨텍스트 패킷 (박제)

```
[DomainAgent]
- 3-tier cascade SSOT: grace-0-local-delete.md (verified-via: 직접 Read)
- cascade 미작동 시 117GB 누적 + disk-full 박제 (MCT-202 trigger 배경)
- historical 회수 경로 = _historical_dual_write (WS-A promote-historical)
- eager unlink ↔ sweep race amendment: ADR-027 §D5 + §D7
- NAS PUT sequential: put_streaming max_concurrency=1 (cold-path-memory-invariant.md:67)

[Researcher]
- 핵심: asyncio cooperative scheduling head-of-line blocking
- 핵심: ThreadPoolExecutor default min(32, cpu+4) FIFO
- 핵심: today/yesterday window + historical accumulation 결합 시 dispatch 폭주
- Unknown #1: forward _run_l2 rglob 이 historical backdated 흡수
- Unknown #2: cleanup task starvation 영구화 (timeout/circuit breaker 0)

[Analyst]
- WHY: sequential 차단 + rglob 낭비 (단순 cleanup 버튼 부족)
- 명시-실제 불일치 가능성 있음 (본질 = 병렬화 + 스캔 축소)
- AC-1~5 정의
- Edge: concurrent promotion + forward L2 race, retry_queue 멀티 contention

[PMO]
- 예상 Story 3개 → 사용자 confirm 으로 1 Story 통합
- 의존 epic: MCT-202 / MCT-189 / MCT-160
- 위험: _run_l2 의 .compacted sentinel 이 scan_and_cleanup_legacy 회수 gate
```
