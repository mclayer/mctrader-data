---
story_key: MCT-204
story_issue: mclayer/mctrader-data#184
parent_epic: none (단독 Story Mode A — Epic 부재, milestone N/A)
phase: Phase 2 (impl PR LAND — compactor _tick stall 해소 + historical L1 회수 경로 신설)
land_pr_phase1: mclayer/mctrader-data#185 (squash sha 95ac00f — design/spec PR)
land_pr_phase2: mclayer/mctrader-data#186 (squash sha 4c06a11 — impl PR)
sibling_pr: mclayer/mctrader-hub#411 (ADR-027 §D5/§D7 + ADR-029 D1=B Accepted, sha 1f2ec47)
adr: ADR-027 (mctrader-hub — §D5 cooperative scheduling head-of-line blocking amendment + §D7 forward partition boundary) + ADR-029 D1=B (L1 NAS reclaim verify-after pattern)
retro_author: PMOAgent
retro_date: 2026-05-19
adr_045_compliance: D-1 auto-trigger (Phase 2 PR #186 merge +5min grace) + D-4 partial-write retry policy + D-5 4-field schema (Story §11 self-write) + D-9 cross-Story pattern threshold (Pattern S — silent-failure mode 운영자 무인지 → ADR-027 §D5/§D6 fail-fast 계열 amendment 반복 N=3 REACHED → mandatory ADR trigger emitted; Pattern T — test-theatre via spec-less MagicMock auto-stub N=2 REACHED → mandatory ADR trigger emitted; Pattern U — FIX-introduced regression N=1 carrier)
---

# Retro — MCT-204 (compactor `_tick` stall 해소 + historical L1 회수 경로 신설)

## 0. Summary

사용자 호소 2건 — (1) "compact 완료 데이터 삭제 안 됨, 로컬 디스크 낭비" (2) "L3 데이터 안 쌓임" — 의 **단일 origin** 을 py-spy live dump 로 확정: forward `_run_l2` worker thread 가 NAS GET 안에서 영구 stall → 같은 `_tick` 내 sequential 후속 step (`_run_l3` / `_cycle_count++` / `scan_and_cleanup_legacy`) 영원히 미진입. stall 원인 = forward `_run_l2` 의 `rglob("*/tier=L1/**/part-*.parquet")` 가 today/yesterday window 와 무관한 historical 16,918 file (130GB) 까지 iter → 19,200 worker NAS GET ≈ 32h/cycle.

3 Layer 통합 fix LAND (Phase 2 PR #186 squash `4c06a11`):
- **Layer 1** — `_run_l2`/`_run_l3` 가 `_discover_partitions_in_range(start=yesterday, end=today)` 호출 (full-tree rglob → date-range 축소, historical 16,918 file iter 제거)
- **Layer 2** — `_tick` 각 step `_run_step_with_timeout` 독립 cadence + `asyncio.TimeoutError` 격리 (L2 stall 시 step drop 후 L3 진입 보장) + boto3 3중 lock (connect_timeout 30s + dedicated executor + asyncio.wait_for 600s)
- **Layer 3** — 신규 `historical_reclaim.py` — L2 NAS HEAD verify-after + L1 unlink + `.l1-promoted` sentinel 멱등 (130GB historical L1 회수 경로 신설)

5 quality lane 全 PASS, FIX budget 2/3 사용 (구현-리뷰 Iter1→3) + CI-gate FIX 2 iteration (∞ budget). 사용자 호소 2건 fidelity SATISFIED (§9.8 구현리뷰PL 최종 verdict). 즉시 조치로 operator one-shot `scan_and_cleanup_legacy` L2 ~11GB 회수 (L1 130GB 는 본질적 부적합 확인 → Layer 3 신설로 영구 해결).

**핵심**: 사용자 명시 표현("cleanup 안 됨" / "L3 안 쌓임")과 실제 origin(sequential head-of-line blocking + rglob 폭주)이 불일치 — 단순 cleanup 버튼 추가는 본질 해소 0. RequirementsAnalyst WHY-first 분석(§5.4)이 단일 origin 을 확정해 3 Layer 통합 scope 를 사용자 confirm 으로 도출.

## 1. Quality gate retrospect

| Lane | FIX iter | Verdict | Resolution method | Findings |
|---|---|---|---|---|
| Design (ArchitectPL + 6 deputy) | - | PASS (after iter 1) | chief author + SecurityArch/OpRiskArch deputy 통합 | §7A 보안 설계 신설 |
| DesignReview | 1 | FIX | ArchitectPL 회귀 (Change Plan §3/§6/§7A + ADR amend) | **P0 ×3**: (a) §7 보안 설계 섹션 부재 (lane checklist Severity rule 강제) (b) Layer 2 `asyncio.wait_for` timeout drop 이 main thread 만 unblock — worker ThreadPoolExecutor slot 영구 점유 + step starvation (c) Layer 3 `date < today-1` UTC day boundary partition tuple race window — INV-H "0 overlap strict" 위반 가능 + P1 ×10 + P2 ×7 |
| DesignReview re-verify | 2 | PASS | Change Plan §3 Layer 2 3중 lock + §6 위험 row 6 + Layer 3 monotonic snapshot + `.forward-processing` sentinel + INV-I 신설 | P0 ×3 + P1 ×10 ALL CLEAR, Iter2 새 발견 P2 advisory 3건 (block 안 함). 설계 lane 종료 (§9.3) |
| 구현 (DeveloperPL) | - | PASS | DeveloperPL | Layer 1+2+3 + historical_reclaim.py + metrics.py 4 metric + cli.py INV-B/G |
| CodeReview | 1 | FIX | DeveloperPL 재구현 (commit ddb46a0) | **P0 ×1**: `historical_reclaim.py:163` 가 존재않는 `nas_uploader._s3.list_objects_v2(...)` 접근 — production real `NASUploader` 주입 시 `AttributeError` → `:169` broad `except Exception:` 흡수 → 모든 partition `fail_verify` → **L1 reclaim 0 (사용자 호소 #1 130GB 미해소 + AC-3 미충족)**. P1 ×2: (a) test-quality — 신규 9 test 전부 `MagicMock()` uploader → real `NASUploader` 계약 0 검증 (P0 가 test contract 통과한 근본 원인) (b) impl-manifest wording 불일치 (boto3 timeout delta) |
| CodeReview re-verify | 2 | FIX | DeveloperPL 재구현 (FIX-introduced regression catch) | P0 #1 CLEAR (`NASUploader.list_prefix_count` public helper + `MagicMock(spec=NASUploader)` 재발 gate). **신규 P1 ×1 (FIX-introduced regression)**: Iter1 의 narrowed `except (ClientError, EndpointConnectionError)` 가 botocore `ConnectTimeoutError`/`ReadTimeoutError` (BotoCoreError 서브트리, caught type subclass 아님) 누락 → MCT-204 가 유도하는 지배적 timeout 실패가 `fail_verify` metric + `l1_reclaim_skipped` counter 우회 (AC-7 telemetry degrade) + P2 ×1 (§7A.3 stale `_s3` doc ref) |
| CodeReview re-verify | 3 | PASS | DeveloperPL `except (ClientError, BotoCoreError)` + botocore 1.43.6 issubclass 실증 | P1 #1 ALL CLEAR (Connect/ReadTimeoutError catch + AttributeError/TypeError propagate = Iter1 P0 의도 보존). 누적 regression 0. 사용자 호소 2건 fidelity SATISFIED. escalation 미발생 (3 trigger 0). 구현 리뷰 lane 종료 (§9.8) |
| CI-gate | 1 | CLEAR | DeveloperPL `ruff check --fix` + 수동 F841 (mechanical, fast-path) | ci-matrix ubuntu-latest Lint fail 36 errors (F401/F841, 신규 9 test file 중심). F841 `exs` = 제거 대신 executor shutdown assert 추가 = 검증 강화. windows-latest = PASS (lint step 차이) |
| CI-gate | 2 | PASS | DeveloperPL — test 기대값 갱신 (Case A 의도된 거동 변경) | `test_rerun_is_idempotent` `assert r2["l2_compacted"] == 24` → 실제 0. 1차 진단: MCT-204 Layer 3 가 1st run 에서 L1 unlink(INV-D) → 2nd run partition discovery 0 = **Case A 의도된 거동 변경** (production 결함 아님). Case B (production 수정) 배제 근거 = INV-D 약화 시 호소 #1 회귀. test docstring + 기대값 갱신, INV-C/D 보존 확인 (§9.10) |

**Max FIX 카운터 = 3 (구현-리뷰 budget)**. 실제 사용 2/3 (구현-리뷰 Iter1 P0 data-loss + Iter2 FIX-introduced regression — Iter3 PASS, 1 unused). 설계-리뷰 1 iteration (P0 ×3). CI-gate FIX = ∞ budget (2 iteration: lint mechanical + test Case A). ESCALATE 0, design re-write 0 (Change Plan 유지).

## 2. Pattern analysis (PMO mandate)

ADR-045 Amend5 §D-9 threshold = **defect-class 또는 process-mechanism recurring pattern N≥2** (positive process signal / structural carrier 는 threshold 비대상 — Google SRE "same *issue* twice" defect semantics). prior retro carrier (MCT-203 §0 Pattern L/M/O/P/Q + U3-MIGRATE §2 Pattern H/G/J/K + disk-pressure incident §3.2/§7) 대비 cross-Story 누적 매칭 + MCT-204 신규 후보 평가.

### 2.1 Pattern S — silent-failure mode (운영자/사용자 무인지) → ADR-027 §D5/§D6 fail-fast 계열 amendment **반복 — N=3 REACHED (mandatory ADR trigger)**

mctrader-data compactor/NAS pipeline 에서 **silent failure (raise 없음 / 운영자·사용자 인지 0 / 무진전)** 가 ADR-027 §D5(NAS PUT 4xx fail-fast) / §D6(silent-skip 차단) 계열 amendment 를 반복 유발:

| # | Story / Incident | silent-failure 양상 | 결과 | ADR amendment |
|---|---|---|---|---|
| 1 | disk-pressure incident 2026-05-17 §3.2 이슈 A | `_dispatch_dual_write` NAS 403 **silent fallback** (retry_queue 0 흡수, 운영자 인지 0, 117GB 미회수) | Action Item 1 (HIGH) — ADR-027 §D6 silent-skip 차단 cross-ref 강화 발의 | ADR-027 §D6 amendment 후보 |
| 2 | MCT-200 post-mortem (#130) | MinIO IAM 403 **silent failure** (NAS PUT 영구 실패 무인지) | ADR-027 §D5 NAS PUT 4xx fail-fast amendment **Accepted + LAND** (`_FAIL_FAST_CODE_TO_REASON` matrix + `NASOperationalAlert` raise) | ADR-027 §D5 amendment (Accepted) |
| 3 | **MCT-204** §2.4 (본 Story) | forward `_run_l2` worker thread **silent stall** (raise 없음, retry_queue 0 흡수, 사용자 2회 호소까지 무인지, py-spy 없이 진단 불가) | ADR-027 §D5 **cooperative scheduling head-of-line blocking** amendment + §D7 forward partition boundary (sibling hub#411 Accepted, sha 1f2ec47) | ADR-027 §D5/§D7 amendment (Accepted) |

**N=3 도달** (detection_channel = root_cause_class fallback hybrid — anchor_id 不一 (`silent_fallback` / `silent_failure` / `silent_stall`) 이나 동일 root_cause_class = "silent failure mode → 운영자 무인지 → ADR-027 fail-fast 계열 SSOT 미완"). §D-9 defect-class recurrence 정의 충족 (silent-skip / silent-fallback / silent-stall 은 동일 결함 클래스의 변종 — Google SRE "observability of failure" 동일 issue). 3 amendment 가 모두 §D5/§D6/§D7 에 산발 박제되었으나 **silent-failure mode 의 detection/surface/escalation 통합 SSOT 부재** — 매 incident 마다 ad-hoc amendment 재발.

**판정**: `escalation_action: adr_draft_emitted` (§3.1). carrier pre-registered: disk-pressure incident §7 Action Item 1 (ADR-027 §D6 강화) + MCT-200 §D5 amendment + MCT-204 §2.4 — 3 sample 의 공통 mechanism = "silent failure 의 통합 detection contract 부재" → mandatory ADR trigger (forcing function intact).

### 2.2 Pattern T — test-theatre via spec-less MagicMock auto-stub → false-green **N=2 REACHED (mandatory ADR trigger)**

`MagicMock()` (spec 없음) 이 존재하지 않는 attribute 를 auto-stub 하여 production-fatal bug 가 test contract 를 통과(false-green):

| Story | 양상 | catch lane | resolution |
|---|---|---|---|
| MCT-203 Task 3/4 (retro §1) | mock `return_value` → `side_effect` 미설정 (multi-key test 부정확) + prefix `l2/market/...` actual shape 不一 | per-task code-quality reviewer (review caught, NEEDS_FIXES) | `side_effect` fix + actual return shape fix |
| **MCT-204** §9.4 P1 #1 + §9.5 | 신규 9 test 전부 `MagicMock()` uploader → `nas_uploader._s3.list_objects_v2` (존재않는 attr) auto-stub → P0 (production `AttributeError` → L1 reclaim 0) 가 test contract **통과** (false-green) | CodeReview Iter1 (production-faithfulness 검증) | `MagicMock(spec=NASUploader)` 재발 gate test 의무화 + `NASUploader.list_prefix_count` public helper |

**N=2 도달** (detection_channel = root_cause_class fallback — anchor_id 不一 (`mock_return_value_drift` / `magicmock_no_spec_autostub`) 이나 동일 root_cause_class = "mock contract 가 production interface 와 unbound → false-green"). §D-9 defect-class recurrence 충족 (test-theatre 는 review lane 이 catch 한 동일 결함 클래스 — review 가 잡았으나 **test authoring guideline (spec= 의무화 / production-faithful boundary mock) SSOT 부재**가 반복 root). MCT-204 의 P0 가 test 를 통과한 근본 원인이 §9.4 P1 #1 으로 명시 박제됨 — review-dependent catch 는 안전망이나 결함 유입 차단 SSOT 아님.

**판정**: `escalation_action: adr_draft_emitted` (§3.2). 두 sample 모두 review 가 catch 했으나(positive review signal) **결함 유입 자체가 반복** = defect recurrence — review 성공 sample 이 아니라 "test authoring contract 부재로 인한 false-green 결함의 review-dependent late catch" recurrence. mandatory ADR trigger.

### 2.3 Pattern U — FIX-introduced regression (FIX 가 새 결함 도입) — N=1 carrier

MCT-204 CodeReview Iter1 의 P0 fix (broad `except Exception` → narrowed `except (ClientError, EndpointConnectionError)`) 가 botocore `ConnectTimeoutError`/`ReadTimeoutError` (BotoCoreError 서브트리, caught type subclass 아님) 누락 → Iter2 새 P1 발견 (FIX-introduced regression). iteration 재검(Iter2 → Iter3)의 가치 직접 실증 — narrowing fix 가 의도(P0 silent-skip 차단)는 달성하나 인접 timeout 실패 모드를 의도치 않게 우회.

- **본 Story 1건 sample (N=1)**. U3-MIGRATE/MCT-203 에 동형 FIX-introduced regression 명시 sample 부재 → carrier 박제 (재발 시 활성). `emit_condition: "FIX iteration 의 fix 가 인접 결함을 도입하는 패턴 재발 시 즉시 발의"`. defect semantics = "narrowing/broadening fix 의 인접 case coverage 검증 절차 부재".

### 2.4 Pattern V — single-origin multi-symptom (사용자 다중 호소 ↔ 단일 root cause) — N=2 평가

MCT-204: 사용자 호소 2건(디스크 미정리 + L3 미축적)이 단일 origin(sequential head-of-line blocking). disk-pressure incident §3.2: 단일 인시던트(디스크 압박)가 multi-defect(NAS 403 + sort drift) 동시 노출 — **방향 반대** (1 symptom → N defect vs N symptom → 1 origin). 동일 mechanism (symptom-defect cardinality mismatch) 의 대조 쌍.

- **non-trigger 판정**: 본 패턴 = RequirementsAnalyst WHY-first 분석의 **positive process signal** (single-origin 확정으로 3 Layer 통합 scope 정합 도출 = 정상·바람직한 분석 결과). §D-9 "design-guidance absence" semantics 미충족 — 결함 recurrence 아니라 분석 규율의 successful application. carrier 박제만 (N=1 mechanism, 방향 대조).

### 2.5 Pattern W — Case A/B disambiguation (의도된 거동 변경 vs 무손실 regression) — N=1 carrier

MCT-204 CI-gate Iter2: `test_rerun_is_idempotent` fail. DeveloperPL 1차 진단 = Case A(MCT-204 Layer 3 INV-D 가 1st run L1 unlink → 2nd run discovery 0 = 의도된 거동) vs Case B(무손실 regression). Case A 채택 + Case B 배제 근거(INV-D 약화 시 호소 #1 회귀) 명시 박제. test 기대값 갱신 + INV-C/D 보존 확인.

- **본 Story 1건 sample (N=1)**. "기존 test FAIL 시 의도된 거동 변경 vs production regression 분기 판정 절차" carrier 박제. U3-MIGRATE/MCT-203 동형 sample 부재. `emit_condition: "Case A/B disambiguation 패턴 재발 시 발의"`.

### 2.6 Pattern matrix 종합 (cross-Story 누적)

| Pattern | carrier source | MCT-204 match | 누적 N | §D-9 defect/process recurrence | Trigger 판정 |
|---|---|---|---|---|---|
| **S — silent-failure mode → ADR-027 fail-fast amendment 반복** | disk-pressure §7 AI-1 + MCT-200 §D5 | §2.4 silent stall | **N=3** | YES (silent-failure 통합 detection SSOT 부재, defect-class recurrence) | **TRIGGER (adr_draft_emitted)** |
| **T — test-theatre spec-less MagicMock false-green** | MCT-203 §1 Task 3/4 | §9.4 P1 #1 _s3 autostub | **N=2** | YES (test authoring contract 부재 → false-green 결함 review-dependent late catch 반복) | **TRIGGER (adr_draft_emitted)** |
| U — FIX-introduced regression | (신규) | CodeReview Iter2 narrowed except | N=1 | — | carrier (재발 시 활성) |
| V — single-origin multi-symptom | disk-pressure §3.2 (방향 대조) | §5.4 single origin | N=2 (mechanism, 방향 반대) | NO (positive process signal — WHY-first successful application) | non-trigger |
| W — Case A/B disambiguation | (신규) | CI-gate Iter2 | N=1 | — | carrier (재발 시 활성) |
| L — doc-only fast-path wording drift | U3-MIGRATE §2.6 | DesignReview Iter1 P2 doc | N≥3 | NO (확립 절차 successful application) | non-trigger (MCT-203 §0 정합) |
| H — mechanical fast-path FIX iter 비소비 | U3-MIGRATE §3.1 (ADR emitted) | CI-gate Iter1 ruff lint-autofix | N≥3 | (ADR already emitted U3 §3.1 — 중복 발의 회피) | non-trigger (carrier ADR 진행 중) |

**결론**: §D-9 N≥2 defect/process recurrence threshold 충족 = **Pattern S (N=3) + Pattern T (N=2)** 2건 → mandatory ADR trigger 2건 emit (§3). Pattern H 는 U3-MIGRATE §3.1 에서 이미 ADR draft emitted (carrier ADR ArchitectAgent 진행 중) → 중복 발의 회피 (non-trigger, ADR 진행 cross-ref 만). 나머지(V/L)는 positive signal / 확립 절차 successful application 으로 §D-9 semantics 미충족 → non-trigger. U/W = N=1 carrier 박제.

## 3. ADR 후보 발의 (PMO proposer only — Mandatory, threshold REACHED ×2)

ADR-045 Amend5 §D-9 mandatory framing 충족 (PMOAgent self-decide 영역 제거 — forcing function). `cross_story_pattern_adr_trigger` field mandatory 채움 + `escalation_action: adr_draft_emitted` ×2. Orchestrator 회부 → codeforge-design ArchitectAgent spawn → 신규 ADR Proposed status 직접 author 의무 (ArchitectAgent = verdict 권한 Accepted|Rejected 최종 결정, PMOAgent = proposer only — ADR-035 Sonnet decider Deprecated 정합).

### 3.1 ADR candidate (Pattern S — N=3 reached)

```yaml
adr_candidate:
  title: "ADR-NNN silent-failure mode 통합 detection/surface/escalation contract — compactor/NAS pipeline 의 silent-skip · silent-fallback · silent-stall 단일 SSOT"
  category: "Architecture / Operational Risk"
  trigger: >
    Cross-Story N=3 reach (CFP-665 / ADR-045 Amend5 §D-9 정량 임계값).
    (1) disk-pressure incident 2026-05-17 §3.2 이슈 A — _dispatch_dual_write NAS 403
        silent fallback (retry_queue 0 흡수, 운영자 인지 0, 117GB 미회수, AI-1 HIGH).
    (2) MCT-200 post-mortem #130 — MinIO IAM 403 silent failure (ADR-027 §D5 NAS PUT
        4xx fail-fast amendment Accepted+LAND, _FAIL_FAST_CODE_TO_REASON matrix).
    (3) MCT-204 §2.4 — forward _run_l2 worker thread silent stall (raise 없음,
        retry_queue 0 흡수, 사용자 2회 호소까지 무인지, py-spy 없이 진단 불가;
        ADR-027 §D5 cooperative scheduling head-of-line blocking amendment Accepted).
    3 sample 모두 ADR-027 §D5/§D6/§D7 에 산발 ad-hoc amendment 박제 → silent-failure
    mode 의 detection/surface/escalation 통합 SSOT 부재로 매 incident 마다 amendment 재발.
  proposer: PMOAgent
  author_pending: ArchitectAgent (chief author — codeforge-design plugin)
  status: Proposed (ArchitectAgent verdict 권한 — Accepted | Rejected 최종 결정)
  detection_channel: root_cause_class (fallback hybrid — silent_fallback/silent_failure/silent_stall anchor_id 不一이나 동일 root_cause_class)
  carrier_source: "disk-pressure incident §7 AI-1 + MCT-200 §D5 amendment (pre-registered)"
  references:
    - "mctrader-hub ADR-027 §D5 (NAS PUT 4xx fail-fast, MCT-200 Accepted)"
    - "mctrader-hub ADR-027 §D6 (silent-skip 차단, disk-pressure AI-1)"
    - "mctrader-hub ADR-027 §D5/§D7 MCT-204 cooperative scheduling amendment (hub#411, sha 1f2ec47)"
    - "MCT-204 §2.4 / §9.8 / docs/retros/2026-05-17-disk-pressure-incident.md §3.2 §7"
  proposed_decision_outline: |
    1. silent-failure taxonomy SSOT: silent-skip (조건 미충족 무경고 skip) /
       silent-fallback (4xx→retry_queue 0 흡수) / silent-stall (cooperative
       scheduling head-of-line blocking, raise 0) 3 변종 통합 정의.
    2. 통합 detection contract: 각 변종이 반드시 (a) Counter/Gauge emit
       (l1_reclaim_skipped / nas_put_operational_alert_total / step_stall_seconds)
       (b) raise or alert (NASOperationalAlert / OperationalRiskAlert) 중 하나
       의무 — "무진전 + telemetry 0 + raise 0" 동시 성립 금지 invariant.
    3. escalation 경로: silent-failure detection 시 operator-visible alarm SSOT
       (Prometheus alert rule) — 사용자 호소 의존 진단 차단.
    4. ADR-027 §D5/§D6/§D7 산발 amendment 를 본 ADR 로 cross-ref 통합 (ad-hoc
       amendment 재발 방지 — 신규 silent-failure 변종 발견 시 본 ADR taxonomy 확장).
```

### 3.2 ADR candidate (Pattern T — N=2 reached)

```yaml
adr_candidate:
  title: "ADR-NNN test-theatre 차단 — production-faithful boundary mock contract (MagicMock spec= 의무화 + mock-vs-production interface binding gate)"
  category: "Process / Test Strategy"
  trigger: >
    Cross-Story N=2 reach (CFP-665 / ADR-045 Amend5 §D-9 정량 임계값).
    (1) MCT-203 retro §1 Task 3/4 — mock return_value→side_effect 미설정 +
        prefix actual shape 不一 (per-task code-quality reviewer NEEDS_FIXES catch).
    (2) MCT-204 §9.4 P1 #1 — 신규 9 test 전부 MagicMock() (spec 없음) uploader →
        nas_uploader._s3 존재않는 attr auto-stub → P0 (production AttributeError →
        L1 reclaim 0 = 사용자 호소 #1 130GB 미해소) 가 test contract false-green
        통과 (CodeReview Iter1 production-faithfulness 검증으로 late catch).
    2 sample 모두 review 가 catch 했으나 결함 유입 자체가 반복 — test authoring
    contract (spec= 의무 / production-faithful boundary mock) SSOT 부재.
  proposer: PMOAgent
  author_pending: ArchitectAgent (chief author — codeforge-design plugin)
  status: Proposed (ArchitectAgent verdict 권한 — Accepted | Rejected 최종 결정)
  detection_channel: root_cause_class (fallback hybrid — mock_return_value_drift/magicmock_no_spec_autostub anchor_id 不一이나 동일 root_cause_class)
  carrier_source: "MCT-203 retro §1 Task 3/4 (review-caught mock contract drift)"
  references:
    - "MCT-204 §9.4 P1 #1 + §9.5 (MagicMock(spec=NASUploader) 재발 gate 의무화)"
    - "MCT-203 retro §1 Task 3/4 (side_effect / actual return shape fix)"
  proposed_decision_outline: |
    1. boundary mock 의무: external interface (NASUploader / boto3 client 등) mock
       시 MagicMock(spec=<RealClass>) 또는 autospec 의무 — bare MagicMock() 금지.
    2. mock-vs-production interface binding gate: production attr 변경 시 spec mock
       이 즉시 AttributeError 로 fail (auto-stub false-green 차단).
    3. production-faithfulness 검증 = code-review lane checklist 항목 (review-dependent
       late catch → authoring-time 차단으로 shift-left).
    4. 적용 경계: unit test boundary mock 한정 (integration testcontainers 실 I/O 는
       대상 외 — 이미 production-faithful).
```

### 3.3 deferred carrier 유지 (N=1, 재발 시 활성)

```yaml
deferred_carriers:
  - pattern: U (FIX-introduced regression)
    state: 1/2 (MCT-204 CodeReview Iter2 narrowed except only)
    emit_condition: "FIX iteration 의 fix 가 인접 결함을 도입하는 패턴 재발 시 즉시 발의"
  - pattern: W (Case A/B disambiguation — 의도된 거동 변경 vs regression)
    state: 1/2 (MCT-204 CI-gate Iter2 test_rerun_is_idempotent only)
    emit_condition: "기존 test FAIL 시 Case A/B 분기 판정 패턴 재발 시 발의"
  - pattern: V (single-origin multi-symptom — mechanism 방향 대조)
    state: 1/2 (disk-pressure §3.2 N defect↔1 symptom vs MCT-204 N symptom↔1 origin)
    emit_condition: "symptom-defect cardinality mismatch 3rd sample 시 발의 (단 positive signal — defect recurrence 시에만)"
```

## 4. Gate 준수 audit

| Audit 항목 | 결과 | Evidence |
|---|---|---|
| Preflight 누락 | PASS | #184 / PR #185 #186 comment trail (design-review FIX→re-verify PASS→code-review FIX iter1→2→3 PASS→CI-gate FIX→LAND), 각 lane 진입 Preflight trail 존재 |
| §8 Test Contract ↔ 실제 테스트 매핑 | PASS | 9 test files (§9.8 박제: 464 passed/36 skipped/4 xfailed, MCT-204 신규 60 passed/3 skipped/0 fail). P0 재발 gate `MagicMock(spec=NASUploader)` + timeout 2 test (`test_connect_timeout/read_timeout_returns_fail_verify`) + `test_l3_independent_of_l2_stall` 신규 박제 |
| §8.5 Impl Manifest ↔ git diff | PASS | PR #186 4c06a11 stat: runner.py +409 / historical_reclaim.py +272 (신규) / nas_uploader.py / metrics.py +27 / cli.py +97 / CLAUDE.md +75 / 9 test files — §8.5 Impl Manifest 기록과 git diff 일치 |
| FIX 원인 판정 evidence pack | PASS | §10 row 1-5 + §9.5/§9.7/§9.10: P0 #1 `historical_reclaim.py:163` line-level + Change Plan §3 Layer 3 인용 / Iter2 FIX-introduced regression botocore 1.43.6 issubclass 실증 / CI-gate Case A/B 배제 근거 (INV-D 약화 시 호소 #1 회귀) verbatim |
| 토큰 예산 초과 | N/A (본 retro 입력에 lane별 token telemetry 미포함 — §8.3 session retro 시 synthesize) | - |
| Epic milestone 갱신 | **N/A** (단독 Story Mode A — Epic 부재. Issue #184 milestone=null 실측 확인. retro mandate "Epic milestone 갱신" = 해당 없음 명시) | gh issue view 184 milestone:null |
| **AUDIT GAP — Issue #184 phase label drift** | **FAIL (forcing function gap)** | #184 = CLOSED 이나 label = `phase:구현-테스트` (stale, `phase:완료` 미전환) + `gate:retro-complete` 미부착 (본 retro step 5 가 부착 — forcing function 핵심 단계) |
| **AUDIT GAP — 로컬 main ↔ origin/main drift** | **NOTE (non-blocking, cross-session)** | 로컬 main worktree HEAD = `907695d` (origin/main `4c06a11` 보다 2 commit 뒤) + `.github/` 다수 미커밋 변경 (다른 세션 작업). PMOAgent 격리 worktree (`chore/mct-204-retro`, base origin/main) 사용으로 간섭 회피 (memory `동시 세션 git 간섭` 정합). 로컬 main sync = Orchestrator/operator 영역 — 회부 |

**Audit GAP 처리**:
- #184 label drift: 본 retro step 5 에서 `gate:retro-complete` add (forcing function 핵심). `phase:구현-테스트` → `phase:완료` 전환은 Orchestrator 영역 (label drift 박제, 회부). U3-MIGRATE retro §4 동형 gap (#89 `phase:구현` stale) 와 누적 — **§9 feedback_back_to_codeforge 에 N=2 process-gap carrier 승격** (U3 §9 가 N=1 carrier 박제, MCT-204 가 2nd sample).
- 로컬 main drift: non-blocking. 격리 worktree 로 작업 격리 완료 — 로컬 main sync 는 PMOAgent write 권한 범위 외 (회부).

## 5. ESCALATE trend

| Story | Lane | ESCALATE 횟수 | FIX budget 사용 | fast-path (비소비) | design re-write |
|---|---|---|---|---|---|
| U2-HELPER | All | 0 | 2/3 | 1 (Option A) | 0 |
| U3-MIGRATE | All | 0 | 2/3 | 1 (pyright) | 0 |
| MCT-203 | All | 0 | 0 (per-task NEEDS_FIXES 2/5, lane FIX 0) | - | 0 |
| **MCT-204** | All | **0** | **2/3** (구현-리뷰 Iter1 P0 data-loss + Iter2 FIX-introduced regression) | 1 (CI-gate Iter1 ruff lint-autofix) | 0 |
| **누적 trend** | - | **0 (4 Story 연속 baseline 유지)** | 평균 1.5/3 (budget 여유) | 누적 3 (Pattern H ADR carrier 진행 중) | **0 (4 Story 연속)** |

본 Story = critical blocker 0 (P0 ×3 설계-리뷰 + P0 ×1 구현-리뷰 모두 FIX 루프 내 resolve), FIX budget 초과 0 (2/3, Iter3 PASS), design re-write 0, ESCALATE 0. Cross-Story ESCALATE trend = **0 유지** (U2/U3/MCT-203/MCT-204 = 4 Story 연속 0). 양호.

단 본 Story 의 P0 ×1 (구현-리뷰 `_s3` AttributeError → L1 reclaim 0 = **사용자 호소 #1 130GB 미해소**) 가 test contract 를 false-green 통과한 점은 ESCALATE 는 아니나 **data-impact high-severity finding 이 review-dependent late catch** 된 사례 — §2.2 Pattern T (test-theatre) carrier 의 직접 근거. review lane 의 production-faithfulness 검증이 정상 안전망으로 작동(positive)했으나 결함 유입 차단 SSOT 부재가 Pattern T trigger 의 본질.

## 6. 사용자 호소 fidelity 최종 박제 (§9.8 구현리뷰PL verdict 인용)

```yaml
user_appeal_fidelity:
  appeal_1:
    원문: "이전에 compact 완료된 데이터는 삭제하라 했는데 여전히 해결되지 않아 계속 로컬 디스크가 낭비되고 있다"
    실제 origin: "sequential head-of-line blocking + rglob 폭주 (단순 cleanup 트리거 아님 — §5.4 Analyst WHY)"
    해소: "Layer 1 (rglob 축소) + Layer 2 (step 격리) + Layer 3 (historical L1 130GB 회수 경로 신설). run_historical_promotion line 725 real production NASUploader 주입 → list_prefix_count() (P0 #1 fix 보존) → L1 reclaim happy-path. timeout 시 fail_verify Counter emit (P1 #1 fix 복원)"
    verdict: SATISFIED (§9.8 구현리뷰PL)
  appeal_2:
    원문: "함께 L3로 데이터도 잘 쌓이고 있지 않다"
    실제 origin: "appeal_1 과 동일 single origin (sequential 차단으로 _run_l3 영원히 미진입)"
    해소: "Layer 1 = _run_l2/_run_l3 date-range 축소 (historical 16,918 file iter 제거). Layer 2 = _tick 독립 step cadence + asyncio.TimeoutError 격리 → L2 stall 시 step drop 후 _run_l3 진입 보장. test_l3_dispatch_normal.py::test_l3_independent_of_l2_stall 가 real runner._tick() 로 박제 (4 test green)"
    verdict: SATISFIED (§9.8 구현리뷰PL)
  observability_derived (Analyst §5.4 암묵 호소):
    "다시 호소 안 하게" → metric 4종 신설 (cleanup_cycle_delay_seconds / step_stall_seconds / historical_l1_reclaim_total / l3_pending_partitions) — 사용자 직접 dashboard 확인 가능, 호소 재발 차단
```

## 7. Cross-Story dependency 박제

```yaml
mct_204_delivery_boundary:
  delivered: "3 Layer fix (Phase 2 PR #186 LAND sha 4c06a11) — Layer 1 rglob 축소 + Layer 2 step 격리 + Layer 3 historical L1 reclaim 경로 신설 (코드)"
  operator_action_pending: >
    Layer 3 reclaim 경로는 코드 LAND 완료. 130GB historical L1 의 실제 점진 회수는
    forward _tick cycle (Layer 1+2 적용 후 정상 cadence 복구) + scan_and_cleanup_legacy
    sweep 자연 가동으로 진행 — 신규 image build → compactor 재기동 후 관측 (operator).
  related_stories:
    - "MCT-202 (eager post-compaction cleanup, merged 2026-05-18): forward eager cascade source_to_delete=Path today/yesterday — Layer 1+2 가 forward window invariant 보존, MCT-202 AC 회귀 0 (§3.3 보존 영역)"
    - "MCT-203 (NAS GET size-gated cache, merged): Layer 1 dispatch 수 감소 → MCT-203 cache 효율 동시 개선 (synergy, §4.3 Continuity)"
    - "MCT-173 (backfill manifest → frozen WAL L1 historical, merged): historical L1 130GB 생성 origin — 본 Story 가 그 130GB reclaim 경로 신설 (complementary, 생성/회수 paired)"
    - "MCT-200 (MinIO IAM 복원 + silent-skip ADR draft, merged #101): ADR-027 §D5 NAS PUT 4xx fail-fast — 본 Story Pattern S N=3 의 2nd sample (silent-failure 계열)"
    - "MCT-189 (scan_and_cleanup_legacy WS-B sweep, merged): Layer 2 가 sweep 별 cadence 분리, partition tuple race 차단 (INV-A 보강)"
    - "MCT-159 #48 (orderbookdepth L1 NotImplementedError, OPEN): Layer 1+2+3 모두 channel != orderbookdepth 만 적용 (INV-D channel 한정 invariant 정합)"
  adr_carrier:
    - "mctrader-hub ADR-027 §D5/§D7 (cooperative scheduling head-of-line blocking + forward partition boundary) — sibling PR #411 sha 1f2ec47 Accepted"
    - "mctrader-hub ADR-029 D1=B (L1 NAS reclaim verify-after pattern) — PR #411 Accepted"
  forward_carry:
    - "Pattern S ADR (silent-failure 통합 detection SSOT) → Orchestrator → ArchitectAgent (codeforge-design) Proposed author 의무"
    - "Pattern T ADR (test-theatre boundary mock contract) → 동일 경로"
    - "Pattern H ADR (mechanical fast-path, U3-MIGRATE §3.1 carrier) — ArchitectAgent 진행 중 (중복 발의 회피, CI-gate Iter1 ruff = 추가 sample cross-ref)"
```

## 8. Learnings count

```yaml
learnings_count: 9
itemized:
  - "py-spy live dump 가 cooperative-scheduler silent-stall 진단의 결정적 도구 — metric/log 만으로는 '왜 안 도는지' 불명, stack trace (Thread 133 = get_streaming→_compact_hour_nas idle 2h+) 가 single origin 확정. 사용자 2회 호소까지 무인지였던 silent failure 의 진단 unblock"
  - "Pattern S (silent-failure mode → ADR-027 fail-fast 계열 amendment 반복) N=3 reach (disk-pressure §3.2 silent-fallback + MCT-200 silent-failure + MCT-204 silent-stall) → mandatory ADR trigger (통합 detection/surface/escalation SSOT 부재 — ad-hoc amendment 재발 차단 발의)"
  - "Pattern T (test-theatre via spec-less MagicMock auto-stub false-green) N=2 reach (MCT-203 §1 + MCT-204 §9.4 P1 #1) → mandatory ADR trigger. P0 (production AttributeError → L1 reclaim 0 = 사용자 호소 #1 130GB 미해소) 가 bare MagicMock() auto-stub 로 test contract 통과 — MagicMock(spec=) 의무화 + production-faithfulness shift-left 발의"
  - "FIX-introduced regression (Pattern U, N=1 carrier) — CodeReview Iter1 P0 fix (broad except → narrowed) 가 botocore Connect/ReadTimeoutError 누락 → Iter2 새 P1. narrowing fix 의 인접 case coverage 검증의 가치 (iteration 재검이 catch)"
  - "사용자 명시 표현(cleanup 안 됨/L3 안 쌓임) ↔ 실제 origin(sequential head-of-line blocking) 불일치 — RequirementsAnalyst WHY-first 가 single origin 확정 → 단순 cleanup 버튼이 아닌 3 Layer 통합 scope 도출 (Pattern V positive signal)"
  - "Case A/B disambiguation (Pattern W, N=1 carrier) — 기존 test FAIL 시 의도된 거동 변경(Layer 3 INV-D 멱등) vs 무손실 regression 분기. Case B 배제 근거(INV-D 약화 시 호소 #1 회귀) 명시 박제가 production 수정 오판 차단"
  - "design-review P0 ×3 (보안 §7 부재 / ThreadPoolExecutor slot 고갈 / UTC day boundary race) 가 설계 lane 에서 catch — Layer 2 가 main thread 만 unblock 하면 worker slot 영구 점유라는 진정 mitigation 영역을 설계 단계 검출 (구현 진입 전 차단의 가치)"
  - "AUDIT GAP — Issue #184 phase label drift (phase:구현-테스트 stale → phase:완료 미전환) — U3-MIGRATE #89 동형 gap 와 누적 N=2 process-gap carrier (forcing function 관측 gap, Orchestrator 회부)"
  - "PMOAgent 격리 worktree (chore/mct-204-retro, base origin/main) 사용으로 로컬 main 의 cross-session 미커밋 .github/ 변경 간섭 회피 (memory 동시 세션 git 간섭 정합) — 로컬 main↔origin drift NOTE 박제"
```

## 9. Feedback back to codeforge

```yaml
feedback_back_to_codeforge:
  - title: "Story Issue close 시 phase label drift (phase:구현/구현-테스트 stale → phase:완료 미전환) — N=2 process-gap"
    detail: >
      MCT-204 #184 CLOSED 이나 label = phase:구현-테스트 (stale). U3-MIGRATE #89 도 동형
      (phase:구현 stale, U3 retro §4 N=1 carrier 박제). 2 Story 누적 N=2 — Story close
      시 phase label 전환 절차 부재 또는 미실행. gate:retro-complete 는 본 retro step 5
      부착 (forcing function 정상). phase label 전환 = Orchestrator 영역이나 close 시
      미전환 = forcing function 관측 gap. plugin-codeforge Orchestrator playbook Story
      close 절차 / phase-label-invariant workflow 검토 후보.
    severity: process-gap (non-blocking, 본 Story verdict 영향 0)
    classification: "carrier 승격 N=2 (U3 §9 N=1 → MCT-204 2nd sample). cross-Story 재발 시 ADR 후보"
  - title: "retro 자동 trigger 의 로컬 main↔origin drift 가정"
    detail: >
      retro-mandatory.yml D-1 auto-trigger 시 PMOAgent 작업 환경의 로컬 main 이
      origin/main 보다 뒤처지고 (본 Story: 907695d vs 4c06a11) cross-session 미커밋
      변경(.github/ 다수) 잔존 가능. PMOAgent 가 격리 worktree (base origin/main) 로
      회피했으나, ADR-045 D-1 trigger spec 에 'PMOAgent 격리 worktree 의무 + base
      origin/main' 명시 권장 (memory 동시 세션 git 간섭 정합 — 절차 SSOT 화).
    severity: process-improvement (non-blocking)
    classification: "carrier 박제 (N=1, retro-automation 환경 가정 gap)"
reason: >
  본 Story 범위 내 plugin-codeforge 정책/skill/agent contract 핵심 결함 0건.
  GitHub Issue lifecycle (phase label 전환) gap = U3 §9 와 누적 N=2 process-gap
  carrier 승격. retro-automation 환경 가정 (로컬 main drift) = N=1 carrier 박제.
  모두 Orchestrator/governance 회부 (PMOAgent write 권한 범위 외).
```

## 10. 산출물 인용

- **Story Issue**: [mclayer/mctrader-data#184](https://github.com/mclayer/mctrader-data/issues/184) (CLOSED, label phase:구현-테스트 stale + gate:retro-complete = 본 retro 부착)
- **Story file (SSOT)**: `docs/stories/MCT-204.md` (PR #186 squash 로 §1-§11 + §9.x + §10 FIX Ledger 전부 origin/main landed)
- **PR Phase 1 (design/spec)**: [mclayer/mctrader-data#185](https://github.com/mclayer/mctrader-data/pull/185) (squash-merged sha 95ac00f)
- **PR Phase 2 (impl)**: [mclayer/mctrader-data#186](https://github.com/mclayer/mctrader-data/pull/186) (squash-merged sha 4c06a11)
- **PR sibling (ADR carrier)**: [mclayer/mctrader-hub#411](https://github.com/mclayer/mctrader-hub/pull/411) (ADR-027 §D5/§D7 + ADR-029 D1=B Accepted, sha 1f2ec47)
- **ADR**: `mctrader-hub:docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md` (§D5 cooperative scheduling head-of-line blocking amendment + §D7 forward partition boundary) + `ADR-029` D1=B (L1 NAS reclaim verify-after)
- **Epic**: none (단독 Story Mode A — milestone N/A)
- **Prior retro (cross-Story carrier source)**:
  - `docs/retros/2026-05-17-disk-pressure-incident.md` §3.2 §7 (Pattern S carrier — silent-fallback)
  - `docs/retros/MCT-203-nas-get-size-gated-cache-retro-2026-05-19.md` §1 Task 3/4 (Pattern T carrier — mock contract drift)
  - `docs/retros/U3-MIGRATE-retro-2026-05-18.md` §3.1 §9 (Pattern H ADR carrier 진행 중 + #89 phase label drift N=1)
- **MCT-200 post-mortem (Pattern S 2nd sample)**: mctrader-data commit `57204dc` docs(MCT-200) post-mortem MinIO IAM 403 silent failure (#130)

## 11. pmo_output v1.2 (CFP-665 / ADR-045 Amend5 §D-9)

```yaml
pmo_output:
  schema_version: pmo-output-v1 v1.2
  story_key: MCT-204
  retro_file: docs/retros/2026-05-19-mct-204-tick-stall-historical-l1-reclaim.md
  cross_story_pattern_adr_trigger:
    triggered: true
    threshold_reached: true
    patterns:
      - pattern_id: "Pattern S — silent-failure mode (운영자 무인지) → ADR-027 fail-fast 계열 amendment 반복"
        cumulative_n: 3
        detection_channel: "root_cause_class (fallback hybrid — silent_fallback/silent_failure/silent_stall anchor_id 不一이나 동일 root_cause_class; carrier pre-registered disk-pressure §7 AI-1 + MCT-200 §D5)"
        escalation_action: adr_draft_emitted
        adr_proposal_ref: "§3.1 (ArchitectAgent spawn 의무 — Orchestrator 회부)"
      - pattern_id: "Pattern T — test-theatre via spec-less MagicMock auto-stub false-green"
        cumulative_n: 2
        detection_channel: "root_cause_class (fallback hybrid — mock_return_value_drift/magicmock_no_spec_autostub anchor_id 不一이나 동일 root_cause_class; carrier MCT-203 §1 Task 3/4)"
        escalation_action: adr_draft_emitted
        adr_proposal_ref: "§3.2 (ArchitectAgent spawn 의무 — Orchestrator 회부)"
  adr_proposal:
    - title: "ADR-NNN silent-failure mode 통합 detection/surface/escalation contract"
      category: "Architecture / Operational Risk"
      status: Proposed
      proposer: PMOAgent
      author_pending: ArchitectAgent
    - title: "ADR-NNN test-theatre 차단 — production-faithful boundary mock contract (MagicMock spec= 의무화)"
      category: "Process / Test Strategy"
      status: Proposed
      proposer: PMOAgent
      author_pending: ArchitectAgent
  non_triggered_patterns:
    - "V (single-origin multi-symptom) — N=2 mechanism 방향 대조이나 positive process signal (WHY-first successful application), §D-9 design-guidance absence 미충족"
    - "L (doc-only fast-path wording drift) — N≥3이나 확립 절차 successful application (MCT-203 §0 정합)"
    - "H (mechanical fast-path FIX iter 비소비) — N≥3이나 U3-MIGRATE §3.1 에서 이미 ADR draft emitted (carrier ADR ArchitectAgent 진행 중, 중복 발의 회피; CI-gate Iter1 ruff = 추가 sample cross-ref만)"
  deferred_carriers: "U (1/2 FIX-introduced regression) / W (1/2 Case A/B disambiguation) / V (1/2 mechanism 방향 대조)"
  escalate_count: 0
  fix_budget_used: "2/3 (구현-리뷰 Iter1 P0 data-loss + Iter2 FIX-introduced regression — Iter3 PASS, 1 unused) + 설계-리뷰 1 iter (P0 ×3) + CI-gate 2 iter (∞ budget)"
  fast_path_count: 1
  audit_gaps:
    - "Issue #184 phase label drift (phase:구현-테스트 stale → phase:완료 미전환) — U3 #89 와 누적 N=2 process-gap carrier, Orchestrator 회부"
    - "로컬 main↔origin/main drift (907695d vs 4c06a11) + cross-session 미커밋 .github/ 변경 — NOTE non-blocking, 격리 worktree 회피, Orchestrator/operator 회부"
  epic_milestone: "N/A (단독 Story Mode A — Epic 부재, Issue #184 milestone=null 실측)"
  user_appeal_fidelity: { appeal_1: SATISFIED, appeal_2: SATISFIED }
  forcing_function_status: "intact — Pattern S N=3 + Pattern T N=2 reach → mandatory ADR trigger ×2 emitted (PMOAgent self-decide 영역 제거 준수)"
  feedback_back_to_codeforge: 2 (phase label drift N=2 process-gap + retro-automation 환경 가정 N=1)
```

[PMOAgent retro authored — ADR-045 Amend1-5 mandate 정합 / CFP-138 D-1 auto-trigger (PR #186 merge +5min grace) / CFP-138 D-5 4-field schema (Story §11 self-write) / CFP-665 D-9 cross-Story threshold — Pattern S N=3 REACHED + Pattern T N=2 REACHED → mandatory ADR trigger ×2 emit / 단독 Story Mode A — Epic milestone N/A]
