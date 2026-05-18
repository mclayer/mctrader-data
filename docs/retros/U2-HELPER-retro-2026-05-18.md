---
story_key: U2-HELPER
story_issue: mclayer/mctrader-data#88
parent_epic: EPIC-nas-key-unification (mctrader-data#86)
phase: Phase 2 (P2-2 forward-fix)
land_pr: mclayer/mctrader-data#95 (squash-merged sha 4aa5483a)
sibling_pr: mclayer/mctrader-hub#395 (ADR-034 Amendment 1-4 sync, sha 4c973849)
adr: ADR-034 (mctrader-hub, Accepted)
retro_author: PMOAgent
retro_date: 2026-05-18
adr_045_compliance: D-1 auto-trigger + D-4 partial-write retry policy + D-5 4-field schema + D-9 cross-Story pattern threshold
---

# Retro — U2-HELPER (nas_key SSOT single helper, Phase 2 forward-fix)

## 0. Summary

mctrader-data 의 nas_key 산출이 **4 개 분산점** 사실 spec 에서 출발했으나 ArchitectPL Phase 0 deputy 산출물 통합 결과 **6 개** (SSOT-5 Orchestrator 발견 + SSOT-6 RefactorAgent 일반화) 으로 확장 식별. 단일 helper `src/mctrader_data/nas_storage/nas_key.py` (5 public + 1 private API) 도입으로 6 분산점 helper 경유 단일 SSOT 통합 완료. Phase 2 cutover 5-step 의 step 2 (forward-fix) 완료 — 신규 PUT = 평면, L2 GET dual-list union (§11.2-A Option A) 활성, INV-9 cutover-stable run_id 박제. ADR-045 Amend5 §D-9 mandate 충족 (MCT-168/169/189/190 nas_key 반복 touch ≥ 2 → ADR-034 정식 발의 Accepted).

PR #95 = 2073 +/-71 LOC (16 file), 3 commits, 60 new/updated tests. CI = 1243 tests PASS (ubuntu-latest), 0 net-new failures, §13.C dual-list perf gate 0.010ms / 18-call sweep (target < 100ms — 4 orders 우월).

## 1. Quality gate retrospect

| Lane | FIX iter | Verdict | Resolution method | Findings |
|---|---|---|---|---|
| Design Phase 1 | - | 6 deputy 만장일치 + chief author Verify-via | - | SSOT-5 + SSOT-6 discovery, §11.2-A L2 GET dual-prefix Option A 채택 |
| Design Phase 1.5 PL audit | - | PASS | deputy re-spawn 0 | - |
| Design Phase 2 chief author | - | Codex consult `no_findings` | debate-protocol-v1 v1.2 Round 0 미발동 | convergence_quality_invariant trigger 부재 (4 deputy 만장일치 + Codex no_findings) |
| Design Phase 3 PL verdict | - | PASS | 4 boolean self-check | - |
| DesignReview dual-track | 1 | FIX | doc-only fast-path | 14 findings (Claude 7 + Codex 8, 1 P1 수렴 cardinality drift) |
| DesignReview re-verify | 1 (lighter) | PASS | chief author inline re-spawn | 14/14 RESOLVED + 1 PROVISIONAL gate (§13.C TBD) |
| SecurityTest | 1 | PASS | GitHub native + Claude+Codex peer dedup | 0 finding |
| CodeReview dual-track | 2 | FIX | DeveloperPL re-spawn + mechanical fast-path Option A | 2 P1 runtime-bugs (peer convergence at l2.py:180 + l2.py:203) + 3 P2 + 1 NIT |
| CodeReview re-verify | 2 | FIX (new P1) | Orchestrator direct commit (mechanical fast-path Option A) | __all__ extension by DeveloperPL → test_public_surface CI fail → direct commit + verify |
| CodeReview fast-path verify | 2 (post-fast-path) | PASS | empirical line-level grep audit | All findings RESOLVED + new P1 fixed |

**Max FIX 카운터 = 3 (budget 정합)**. 실제 사용 2/3 = design-review iter 1 + code-review iter 2. Mechanical fast-path Option A (Orchestrator direct commit, code-review iter 2 내부 verify, §10 row append 0) 가 별 FIX iteration 으로 escalate 되지 않은 점 박제 — CFP-19 R11 정합.

## 2. Pattern analysis (PMO mandate)

### 2.1 Pattern A — 반복 패치 루프 종결 효과

| Story | 영역 | nas_key touch | 결과 |
|---|---|---|---|
| MCT-168 | dual_writer.py:371 | tactical patch | l1/ prefix 도입 (split scheme) |
| MCT-169 | l2.py:158 (SSOT-4) | tactical patch | f-string hardcode |
| MCT-189 | runner.py:350-351 | tactical patch (#75 post-merge FIX) | promote_l1 4중 HEAD verify |
| MCT-190 | runner.py:265 | tactical patch | _dispatch_dual_write 평면 |
| **U2-HELPER** | **6 분산점 통합** | **structural fix** | **single helper SSOT + 신규 PUT 평면 자동 + dual-list union + INV-9 박제** |

**효과 가설**: 향후 nas_key 영역 patch 시 단일 helper (`build_nas_key` / `build_l1_prefix` / `build_legacy_nas_key` / `build_nas_prefix` / `_extract_tier`) 만 touch. 분산 산출 0 (AC-1 grep guard 박제). Cross-Story pattern threshold (CFP-665 / ADR-045 Amend5 §D-9 N=2) 통과 → ADR-034 발의 → Accepted → 실제 구현 완료 — **forcing function chain 정상 동작 검증**.

### 2.2 Pattern B — PMO spec Ground Truth audit gap

본 Story Issue #88 body §2 Ground Truth = 4 SSOT (PMO 2nd pass 가 brief 의 3곳 → 4곳 정정 박제). ArchitectPL Phase 0 deputy 산출물 통합 결과:

- **SSOT-5 발견**: Orchestrator 가 `runner.py:439-475` `_historical_dual_write()` line 448 `str(parquet_path.relative_to(root)).replace("\\", "/")` 평면 직접 산출 (WS-A historical promotion path, `_dispatch_dual_write` 와 byte-동형 박제) 발견
- **SSOT-6 발견**: RefactorAgent §5 일반화 산출 — `l3.py:153-156` 가 L2 input prefix 직접 산출 (`build_nas_prefix(tier="L2")` 도입 의무 도출)

**50% 누락 (4 → 6 = +50%)**. PMO Ground Truth 정의 keyword grep 범위 제약 = 잠재 audit gap. Pattern threshold N=2 미충족 (single Story sample), 하지만 향후 Story 에서 동일 패턴 재발 시 ADR 후보로 escalate 의무.

**Carrier**: 본 Story 1건 + 향후 Story 1건 (재발 시) ≥ 2 → ADR 후보 "PMO Story Ground Truth 정의 - 광범위 keyword 변형 grep 의무" 발의 예약.

### 2.3 Pattern C — debate-protocol-v1 v1.2 미발동

본 Story 의 Design Phase 2 (chief author 통합 후 Codex consult) = `no_findings`. convergence_quality_invariant 3-tuple trigger (4 deputy 만장일치 + Codex non-empty findings + chief author 1차 산출물 mismatch) 미발동. debate sample = 0건. 향후 Story 대조군 retrospect data point 박제. Pattern threshold N=2 미충족.

### 2.4 Pattern D — Dual-track peer review same-line P1 convergence

CodeReview lane iter 2 = Claude (Opus 4.7 inline review) + Codex (adversarial-review + convergent review) **independently** flagged **SAME 2 P1 runtime-bugs at SAME lines** (`l2.py:180` alias-overlap row duplication + `l2.py:203` legacy-only → flat-only transition orphan). **Highest possible peer-review confidence**. ADR-001 dual-track SSOT 정합.

본 Story 1건 sample. Pattern threshold N=2 미충족. Cross-Story 누적 시 ADR 후보 "Dual-track peer convergence at SAME lines = 최고 confidence FIX signal — Sonnet decider 불필요 patten" 발의 예약.

### 2.5 Pattern E — PROVISIONAL gate 정량 escalation

DesignReview re-verify 시 §13.C dual-list NAS LIST round-trip overhead = `PROVISIONAL` + `[empirical-source: TBD]` annotation + §8.3 escalation gate explicit (≥ 100ms / 18-call sweep 발견 시 chief author 재spawn FIX trigger). DeveloperPL 실측 0.010ms / 18-call sweep (target < 100ms, 4 orders better) → genuinely PASS 전환. ADR-068 Amendment 1 Mitigation 2 정합.

**효과 박제**: PROVISIONAL annotation 패턴 = design-review lane 통과 차단 0 + impl-lane gate 명시 escalation 경로 보존 → 본 Story 1건 successful application sample. Pattern threshold N=2 미충족 (향후 동일 패턴 재발 시 ADR 후보 발의 예약).

### 2.6 Pattern F — Cross-Story dependency carrier 명시 박제

본 Story = Phase 2 5-step cutover 의 step 2 → step 3 (U3-MIGRATE delete) + step 5 (U5-VERIFY 회수) 카리어 명시 박제:

```yaml
to_u3_migrate:
  scripts/rekey_l1_migration.py:
    location: F-codex-6 scope note 반영 - U3 §9 Impl Manifest 의무
    timing: U2 LAND 후
    invariants: [INV-7 마이그레이션 완료, 4-HEAD verify, BackfillManifest YAML]
to_u5_verify:
  build_legacy_nas_key: 회수 (Phase 1 transitional dead code)
  build_legacy_l1_prefix: 회수
  _legacy_key_to_canonical: 회수
  _resolve_legacy_nas_key: 회수
  l2.py dual-list code path: 단순화 (single list flat-only)
  minio_uploader.py:23: 회수 (deprecated module)
  INV-2 + INV-6: forward-only invariant grep gate 박제
  Counter mode=legacy_dual_read: value=0 invariant + Prometheus alert rule
```

본 Story 1건 successful application sample. Pattern threshold N=2 미충족.

## 3. ADR 후보 발의 (PMO proposer only)

### 3.1 Candidate 1 — PMO Story Ground Truth 정의 의무 확장

```yaml
adr_candidate:
  title: "ADR-NNN PMO Story Ground Truth 정의 — 광범위 keyword 변형 grep 의무"
  category: "Process"
  trigger: "U2-HELPER 사례 - 4 SSOT spec → 6 SSOT 실측 (+50% 누락)"
  proposer: PMOAgent
  author_pending: ArchitectAgent (chief author)
  status: deferred (cross-Story threshold N≥2 미충족)
  emit_condition: "동일 PMO Ground Truth audit gap 패턴 재발 시 즉시 발의"
```

본 Story 1건 sample (N=1) → emit 보류, ArchitectAgent spawn 의무 미발동. Threshold reach (N=2) 시 본 retro 가 carrier 박제 source.

### 3.2 Candidate 2 — Mechanical fast-path Option A 적용 조건 박제

```yaml
adr_candidate:
  title: "ADR-NNN Mechanical fast-path Option A — Orchestrator direct commit 권한 + 검증 절차"
  category: "Process"
  trigger: "U2-HELPER CodeReview iter 2 - DeveloperPL __all__ extension 새 P1 → Orchestrator direct commit (DeveloperPL 재spawn 없이)"
  proposer: PMOAgent
  author_pending: ArchitectAgent (chief author)
  status: deferred (cross-Story threshold N≥2 미충족)
  emit_condition: "동일 mechanical_category fast-path 패턴 재발 시 즉시 발의"
  references: CFP-19 R11 (same-iteration internal verify, §10 row append 0)
```

본 Story 1건 sample (N=1) → emit 보류. Threshold reach (N=2) 시 carrier 박제.

## 4. ESCALATE trend

| Story | Lane | ESCALATE 횟수 |
|---|---|---|
| U2-HELPER | All lanes | **0** |

본 Story = critical blocker 0, FIX budget 초과 0 (2/3 used), design re-write 0. ESCALATE trend 양호. Cross-Story 누적 baseline 박제.

## 5. Cross-Story pattern threshold check (CFP-665 / ADR-045 Amend5 §D-9)

```yaml
pmo_output_v1.2:
  cross_story_pattern_adr_trigger: null
  reason: "all 5 sub-patterns (B/C/D/E/F) at N=1, threshold N≥2 미충족"
  carriers_emitted:
    - "Pattern B (PMO Ground Truth audit gap) - 1/2"
    - "Pattern C (debate-protocol-v1 미발동 sample) - 1/2"
    - "Pattern D (dual-track same-line P1 convergence) - 1/2"
    - "Pattern E (PROVISIONAL gate 정량 escalation) - 1/2"
    - "Pattern F (Cross-Story dependency carrier 박제) - 1/2"
  forcing_function_status: "intact - re-evaluate at next Story retro write"
```

ArchitectAgent spawn 의무 미발동 (anchor_id ≥ 2 strict primary 채널 미충족 + root_cause_class fallback hybrid 채널도 N=1).

## 6. Cross-Story carrier baseline 박제

본 retro 가 다음 carrier baseline source:

```yaml
carrier_baselines:
  to_u3_migrate_story89:
    inherited: [scripts/rekey_l1_migration.py spec, INV-7 마이그레이션 완료, 4-HEAD verify, BackfillManifest YAML pattern]
    timing: post-U2 LAND
  to_u5_verify_story91:
    inherited:
      - build_legacy_* helper 회수 (4종)
      - _resolve_legacy_nas_key Phase 1 helper 회수
      - l2.py dual-list code path 단순화
      - minio_uploader.py:23 회수
      - INV-2 + INV-6 forward-only grep gate
      - Counter mode=legacy_dual_read value=0 invariant
    timing: post-U3 LAND
  to_future_stories:
    inherited:
      - Pattern B/C/D/E/F sub-pattern N=1 박제 (재발 시 N=2 → ADR 후보 활성)
      - PMOAgent retro template 적용 (본 retro 가 standard reference)
      - INV-9 cutover-stable run_id 패턴 (canonical_keys sha256 박제)
```

## 7. 산출물 인용

- **Story file**: `docs/stories/U2-HELPER.md` (§1-§13, §10 FIX Ledger iter 1+2 박제, §11.5 본 retro pointer)
- **Change Plan**: `docs/change-plans/U2-HELPER.md` (chief author authored)
- **ADR**: `mctrader-hub:docs/adr/ADR-034-nas-key-unification.md` (Accepted + Amendment 1-4)
- **PR (LAND)**: [mclayer/mctrader-data#95](https://github.com/mclayer/mctrader-data/pull/95) (squash-merged sha 4aa5483a)
- **PR (sibling)**: [mclayer/mctrader-hub#395](https://github.com/mclayer/mctrader-hub/pull/395) (Amendment box sync, sha 4c973849)
- **Epic**: [mclayer/mctrader-data#86](https://github.com/mclayer/mctrader-data/issues/86)
- **Comments (Story #88)**:
  - DesignReview FIX iter 1 발행: #88 comment 4471224824 (2026-05-17)
  - DesignReview PASS re-verify: #88 comment 4471271962 (2026-05-17)
  - CodeReview FIX iter 2: #88 comment 4471405441 (2026-05-17)
  - Orchestrator LAND: #88 comment 4471571768 (2026-05-17)

## 8. Learnings count

```yaml
learnings_count: 6
itemized:
  - "Pattern A 반복 패치 루프 종결 효과 검증 - forcing function chain 정상 동작"
  - "Pattern B PMO spec Ground Truth audit gap (4 → 6 SSOT, 50% 누락)"
  - "Pattern C debate-protocol-v1 v1.2 미발동 sample 박제"
  - "Pattern D Dual-track peer review same-line P1 convergence 최고 confidence signal"
  - "Pattern E PROVISIONAL gate 정량 escalation 패턴 successful application"
  - "Pattern F Cross-Story dependency carrier 명시 박제 패턴"
```

## 9. Feedback back to codeforge

```yaml
feedback_back_to_codeforge: []
reason: "본 Story 범위 내 plugin-codeforge 정책/skill/agent contract 결함 0건 발견. Pattern B/C/D/E/F 모두 N=1 - 향후 Story 재발 시 ADR 후보 carrier 활성 예정."
```

[PMOAgent retro authored — ADR-045 Amend1-5 mandate 정합 / CFP-138 D-5 4-field schema / CFP-665 D-9 cross-Story threshold check]
