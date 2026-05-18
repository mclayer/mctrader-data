---
story_key: parse-node-id-suffix-strip
story_issue: none (단일 세션 internal Story — compactor-sort-key Story PR #96 retro §6 follow-up #3, formal MCT-NNN/codeforge Issue 미할당, spec = canonical artifact)
parent_epic: none (single Story — parse_node_id_from_segment latent landmine DRY refactor)
phase: standalone (single Story, 단일 PR atomic landmine-removal — spec §9 1-PR 압축 권장, AC-1 regression-0 production 샘플 fail 0 → 2-PR 회귀 미발동)
land_pr: mclayer/mctrader-data#127 (squash-merged sha d8912ad, single commit, 617 +/- 15 LOC, 4 file)
sibling_pr: none (단일 PR, sibling chore 미발생 — CI 1발 PASS)
adr: none (신규/변경 0 — 기존 ADR-017 Amendment 3 / ADR-009 §D2.8 mctrader-hub#398 bba73f4 longest-first suffix-strip 규약 준수)
retro_author: PMOAgent
retro_date: 2026-05-18
adr_045_compliance: D-1 auto-trigger + D-4 partial-write retry policy + D-5 4-field schema (spec §13 등가 박제 — Story file 부재) + D-9 cross-Story pattern threshold (Pattern K N=2 evaluated → non-trigger, semantics 판정)
---

# Retro — parse-node-id-suffix-strip (parse_node_id_from_segment latent landmine DRY refactor)

## 0. Summary

compactor-sort-key Story (PR #96 LAND `adfddf4`) Task 2 code review 가 발견한 sibling `parse_node_id_from_segment` latent bug → 해당 retro §6 follow-up #3 carrier 박제 → 본 Story 가 closure. `src/mctrader_data/wal/segment.py:67-73` 의 chained `stem.replace(".ndjson.sealed","").replace(".ndjson","")` 가 `.compacted` 파일 (`segment-<ts>-<node>.ndjson.sealed.compacted`) 적용 시 `parts[2]` = `<node>.compacted` 오염 (substring 부분소비). 단일 production caller `l1.py:227` (`_parse_segment_meta` ← `compact_segment`) = `scan_sealed` 필터 `.ndjson.sealed` 전용 경로 → **dormant landmine**. forward-only 도메인 (ADR-009 §D12.2) + node= partition leaf MANDATORY (§D2.1) → `parts[2]` 오염 = L1 Hive partition + multi-node dedup 8-tuple 파손 데이터 무결성 결함 클래스. dormancy 영구 아님 (U3-MIGRATE `skipped_not_compacted` status 축 = future `.compacted` caller 활성 시나리오) → detective(사전 차단)가 유일 방어.

`_strip_segment_suffixes(name)` longest-first private helper (`.ndjson.sealed.compacted` → `.ndjson.sealed` → `.ndjson` tuple) 신설 + `parse_node_id_from_segment` + `parse_ts_from_segment` 양쪽 흡수 (suffix-strip 단일 책임만 — error contract 비대칭 `"DEFAULT"` lenient / `ValueError` strict 의도적 보존, Researcher U1 zero-regression mandate). DRY 완전 달성 (segment.py `.replace(".ndjson` chain 0). PR #127 = `d8912ad` = single commit 617 +/-15 LOC, 4 file (spec 154 + plan 373 + segment.py 39 + test 66), 121 passed / 4 xfail (pre-existing 무관). 단일 세션 internal Story (formal Issue 미할당) — spec = canonical artifact, ADR-045 §D-5 4-field schema = spec §13 등가 박제 (Story file 부재).

ADR-045 Amend5 §D-9 mandate 충족 — cross-Story pattern threshold check carrier 평가 (§5). **Pattern K (branch protection matrix-name 미스매치 → admin merge) = N=2 reach** (compactor-sort-key retro §2.5 N=1 carrier + 본 Story 재발) — 단 §D-9 "plugin-codeforge design-guidance absence" semantics 미충족 (mctrader-data infra governance 영역, plugin-codeforge 비대상) → **non-trigger** (U3-MIGRATE retro §2.7 확립 판정 원칙 정합). ADR 후보 0건 (proposer only — threshold semantics 미충족).

## 1. Quality gate retrospect

subagent-driven-development 5 TDD bite-sized task. 각 task = implementer + spec compliance reviewer + code quality reviewer (combined). 최종 entire-branch reviewer = APPROVED FOR MERGE.

| Task | 영역 | spec verdict | code quality verdict | Resolution method | Findings |
|---|---|---|---|---|---|
| 1 | spec + plan git stage (Phase 1 doc commit) | - (doc-only) | - (doc-only) | 직접 검증 | doc stage only, code 변경 0 |
| 2 | `_strip_segment_suffixes` helper (AC-6) | PASS | APPROVED | non-fix | Minor: mid-file import + comment precision (non-blocking, deferred) |
| 3 | `parse_node_id_from_segment` 흡수 (AC-1/2/4) | PASS | APPROVED | non-fix | AC-1 byte-identical regression guard **진성 검증** (non-tautological — old chained `.replace` inline oracle vs new helper, `.compacted` RED→GREEN 입증) |
| 4 | `parse_ts_from_segment` 흡수 (AC-3/5) | PASS | APPROVED | non-fix | DRY 완전 달성 (segment.py `.replace(".ndjson` chain 0). 43/43 pass. ValueError strict contract 불변 |
| 5 | 전체 회귀 + lint + PR open | - | - | ruff E402 fix commit (test-only, mid-file import) | 121 passed / 4 xfail (pre-existing 무관). git diff origin/main l1.py = empty (caller 무변경) |
| Final | entire-branch reviewer | APPROVED FOR MERGE | - | non-fix | "textbook narrow landmine-removal refactor". Minor 2 deferrable (regression loop `.compacted` sample 미포함 — 별 test AC-2 가 cover / test 파일명 정확성) |

**NEEDS_FIXES ratio = 0/5 task** (Task 2/3/4 combined APPROVED, Task 1/5 doc·verification only). 유일 fix commit = Task 5 ruff E402 (mid-file import, **test-only** — production behavior 무영향). Max FIX 카운터 = 0 (lane-level FIX 루프 미발동, ESCALATE 0). subagent-driven-development per-task review FIX = same-session same-task internal verify (별 FIX iteration escalate 아님, CFP-19 R11 정합 — §10 row append 0).

**대조 (compactor-sort-key retro Pattern G)**: compactor-sort-key = 10 task 중 7 NEEDS_FIXES (70%, code quality hygiene defect 절대다수). 본 Story = 5 task 중 **0 NEEDS_FIXES** (combined APPROVED 3 + doc·verify 2). 대조 원인 분석 → §2.1 Pattern M.

## 2. Pattern analysis (PMO mandate)

compactor-sort-key retro 의 Pattern G-L (전부 N=1 carrier 박제) 대비 cross-Story 누적 매칭 + 본 Story 신규/연장 패턴 평가. ADR-045 Amend5 §D-9 threshold = **defect-class 또는 process-mechanism recurring pattern N≥2** AND **plugin-codeforge design-guidance absence semantics 충족** (positive process signal / structural carrier / 확립 절차 successful application / plugin-codeforge 비대상 infra governance 는 threshold 비대상 — U3-MIGRATE retro §2.7 확립 판정 원칙 정합).

### 2.1 Pattern M — scope 극소 + 사전 ResearcherAgent fix 설계 → per-task NEEDS_FIXES 0 (Pattern G 대조군)

본 Story = 5 task 중 0 NEEDS_FIXES. compactor-sort-key Pattern G = 10 task 중 7 NEEDS_FIXES (70%). 대조 원인:

| 요인 | compactor-sort-key (Pattern G 70%) | 본 Story (0%) |
|---|---|---|
| scope | 26 file / 3113 LOC / 11 task / 신규 sort_key.py + verify script | 1 prod file / ~15 LOC prod / 5 task / segment.py helper 1 |
| 사전 설계 정밀도 | Phase 0 burst 후 11 task 분해 (구현 hygiene 미사전반영) | Phase 0 burst + Researcher behavior-change 판정이 설계 anchor (helper 시그니처 + error contract 비대칭 보존 사전 확정) |
| spec AC 정밀도 | AC sort key 정합 (구현 자유도 큼) | AC-1 byte-identical regression-0 BLOCKING (old chained `.replace` inline oracle = 구현 자유도 0) |
| 결과 | code quality reviewer hygiene defect 다발 (broad except / in-loop import / mkdir parents / type alias 무효) | hygiene defect 0 (helper 11 LOC, oracle-pinned implementation) |

**해석**: per-task NEEDS_FIXES 율은 implementer hygiene 자체보다 **scope 크기 + 사전 설계의 구현 자유도 제약 정도**에 강하게 종속. 본 Story = scope 극소 + Researcher fix 설계 (error contract 비대칭 보존 = 명시 anchor) + AC-1 inline oracle (byte-identical 강제) → implementer 가 hygiene defect 주입 표면적 자체가 최소. compactor-sort-key Pattern G ("implementer prompt hygiene pre-checklist 주입" ADR 후보 carrier) 의 **대조군 sample** — Pattern G 의 root class 가 "implementer hygiene 미반영" 보다 "scope·자유도" 종속일 가능성 박제. Pattern threshold: Pattern G(compactor-sort-key, hygiene 고비율) 와 Pattern M(본 Story, 대조 0%) 은 **root_cause_class 동일 mechanism 의 양극 sample** — recurrence N=2 아님 (대조 쌍, defect recurrence 정의 미충족). carrier 박제 (cross-Story 3rd sample 시 scope-vs-hygiene 상관 ADR 후보 평가).

### 2.2 Pattern H 연장 — out-of-scope finding → follow-up Story → closure 전체 lifecycle 1 사이클 완주 (N=2 mechanism, non-defect)

compactor-sort-key Task 2 code review 가 `parse_node_id_from_segment` latent bug 발견 → 당시 out-of-scope 판정 (pre-existing dormant + behavior change risk) → compactor-sort-key retro §6 follow-up #3 carrier 박제 → **본 Story 가 closure** (spec §1 발견 경위 + §12 cross-ref forward-reference). out-of-scope discovery → follow-up 박제 → 별 Story closure 전체 lifecycle 1 사이클 완주 sample.

| 단계 | Story | 산출 |
|---|---|---|
| discovery | compactor-sort-key Task 2 (PR #96 `adfddf4`) | code quality reviewer 가 신규 `parse_ts_from_segment` longest-first chain 과 sibling 비교 시 발견 |
| out-of-scope 판정 | compactor-sort-key | pre-existing dormant + behavior change risk (pre-existing caller 검증 의무) → 미수행, scope creep 차단 |
| carrier 박제 | compactor-sort-key retro §6 follow-up #3 | `to_followup_story_parse_node_id_dry` (behavior change risk + DRY refactor + sibling fix) |
| **closure** | **본 Story** | `_strip_segment_suffixes` longest-first helper + 양쪽 흡수, AC-1 byte-identical regression-0 BLOCKING gate 로 behavior change risk 해소 |

**해석**: superpowers `test-driven-development` / `systematic-debugging` "Don't refactor beyond task" 원칙 → out-of-scope 박제 → 별 Story closure 의 **표준 절차가 cross-Story 로 정상 동작 1 사이클 완주 검증**. compactor-sort-key Pattern H (out-of-scope finding 처리) carrier + 본 Story closure = N=2 mechanism. 단 §D-9 판정: 이 mechanism = **확립된 scope-discipline 절차의 successful application** (defect recurrence 아님, "design-guidance absence" semantics 미충족 — U3-MIGRATE retro §2.7 Pattern L/I 분류 원칙 정합) → **non-trigger** (positive process signal). lifecycle 완주 자체가 절차 건전성 검증. carrier 종결 (closure 달성 — 더 이상 deferred 아님).

### 2.3 Pattern K 재발 — branch protection matrix-name 미스매치 → admin merge **(N=2 REACHED — semantics 판정)**

compactor-sort-key retro §2.5 Pattern K = `required_status_checks.contexts:["ci"]` 가 matrix job 명("ci (ubuntu-latest)" / "ci (windows-latest)")과 미스매치 → required check "ci" 영원히 미보고 → perma-BLOCKED → `enforce_admins:false` admin merge (N=1 carrier "1/2" 박제, §9 feedback "mctrader-data infra governance, plugin-codeforge 비대상" 명시).

본 Story = 동일 패턴 재발: CI 전부 PASS 1발 (ubuntu + windows + check-gate + CodeQL) 이나 branch protection `required_status_checks.contexts:["ci"]` matrix-job-name 미스매치 perma-BLOCKED → phase-gate-mergeable governance (labels `phase:구현`+`gate:design-review-pass` + `[설계-리뷰] PASS` comment + PR body Lane evidence → SUCCESS) → `enforce_admins:false` admin merge.

**누적 N 판정 = N=2 REACHED** (root_cause_class fallback channel — branch protection contexts ↔ actions matrix job 명 SSOT drift, 동일 mechanism 2 Story 연속).

**§D-9 semantics 판정 = non-trigger** (U3-MIGRATE retro §2.7 확립 판정 원칙 정합):

| 판정 축 | 평가 |
|---|---|
| N≥2 정량 임계 | **충족** (compactor-sort-key N=1 + 본 Story = N=2) |
| defect/process-mechanism recurrence | 충족 (infra governance drift 동일 mechanism 2회) |
| **§D-9 "design-guidance absence" semantics** | **미충족** — 본 drift = mctrader-data 의 GitHub branch protection `required_status_checks.contexts` ↔ actions workflow matrix job 명 SSOT drift (인프라 거버넌스 결함, 코드 결함 아님). plugin-codeforge 정책/skill/agent contract 영역 **비대상** (CFP-665 / ADR-045 §D-9 = plugin-codeforge cross-Story pattern, consumer repo infra governance ≠ codeforge design-guidance absence) |
| escape hatch 정당성 | admin merge (`enforce_admins:false`) = governance gap 의 의도된 owner 권한 사용 (ESCALATE 아님, U3-MIGRATE/compactor-sort-key 동일 패턴) |

**결론**: Pattern K = N=2 정량 도달하나 **plugin-codeforge design-guidance absence semantics 미충족** → mandatory ADR trigger **non-emit** (PMOAgent forcing function 영역 = plugin-codeforge cross-Story pattern 한정, consumer infra governance 비대상). 단 mctrader-data **infra governance 영역의 별 chore Story 후보**로 §9 feedback 명시 + §6 carrier 박제 (N=2 → mctrader-data 측 fix 의무 escalate — branch protection `contexts` 를 matrix job 명과 정합 OR `ci` aggregate job 추가). codeforge §D-9 forcing function 은 intact (semantics gate 정상 동작 — 정량 N=2 도달해도 scope 외 패턴 차단).

### 2.4 Pattern L 연장 — ADR-073 verify-via dormancy 사실 확정이 설계 anchor (N=2 mechanism, non-defect)

compactor-sort-key Pattern L = 외부 세션 발의 Story 사실 정정 (Orchestrator 직접 git show origin/main verify-via 가 외부 초안 2 부정확 가설 정정). 본 Story = ADR-073 verify-via 가 **dormancy 사실 확정을 설계 anchor 로** 격상한 sample:

- Orchestrator 직접 `git show origin/main` verify: 단일 production caller `l1.py:227` = `.ndjson.sealed` 전용 (`scan_sealed` 필터) → **dormancy 확정** (spec §pre_lookup_evidence 9-line verify-via 박제)
- dormancy 사실 = "현 시점 production 무영향" → fix 정당성 = forward-only 도메인 + future `.compacted` caller 활성 시나리오 (U3-MIGRATE `skipped_not_compacted` status 축) = **detective(사전 차단) 정당화의 설계 anchor**
- AC-1 byte-identical regression-0 BLOCKING = dormancy 사실 ("단일 caller `.ndjson.sealed` 전용") 의 직접 귀결 (해당 caller path old/new byte-identical assert-equal)

**해석**: compactor-sort-key Pattern L (verify-via 가 외부 초안 정정) + 본 Story (verify-via 가 dormancy 사실 확정 → 설계 anchor 격상) = N=2 mechanism. 단 §D-9 판정: ADR-073 verify-via = **확립된 source-of-truth 의무의 successful application** (defect recurrence 아님, positive process signal) → **non-trigger** (U3-MIGRATE retro §2.7 Pattern C/D 분류 원칙 정합). carrier 박제 (verify-via 효과 누적 baseline).

### 2.5 Pattern N — CFP-637 derived default minimal-interaction (Analyst 4 질문 → dialog 0)

Phase 1: CFP-637/ADR-064 §결정 10 — Analyst 4 확인질문 (입력 scope + invalid 정책 등) 전부 ResearcherAgent 기술분석 + YAGNI/zero-regression 으로 해소 → dialog 진입 차단, 6 derived default declare. 핵심 derived default = error contract 비대칭 의도적 보존 (Researcher U1: `"DEFAULT"` = silent-corruption sentinel, strict 통일 시 신규 production raise regression).

compactor-sort-key Pattern (Phase 1 dialog 1-question + 6 sub-결정 derived default) 대비 본 Story = **dialog 0-question** (Analyst 4 질문 전부 Researcher 기술분석 해소). minimal-interaction 효과 강화 sample — scope 극소 + Researcher behavior-change 판정 정밀도가 dialog 진입 자체 차단.

**해석**: CFP-637/ADR-064 §결정 10 derived default declare 메커니즘이 본 Story 에서 dialog 0 까지 압축 (compactor-sort-key 1-question 대비). 단일 세션 internal Story (외부 follow-up 발의, formal Issue 없음) + Researcher fix 설계 정밀도 효과. positive process signal (defect 아님) → carrier 박제 (minimal-interaction 효과 누적 baseline, threshold 비대상).

### 2.6 Pattern matrix 종합 (cross-Story 누적)

| Pattern | 이전 carrier | 본 Story match | 누적 N | §D-9 defect/proc-mech recurrence | plugin-codeforge design-guidance absence semantics | Trigger 판정 |
|---|---|---|---|---|---|---|
| **K — branch protection matrix-name → admin merge** | compactor-sort-key §2.5 (N=1) | 동일 admin merge 재발 | **N=2** | 충족 (infra governance drift 동일 mechanism) | **미충족** (mctrader-data infra governance, plugin-codeforge 비대상) | **non-trigger** (semantics gate — §2.3) |
| H — out-of-scope → follow-up → closure lifecycle | compactor-sort-key §2.2 (N=1) | 본 Story closure | N=2 | 미충족 (확립 scope-discipline 절차 successful application) | — | non-trigger (positive signal, carrier 종결) |
| L — ADR-073 verify-via 효과 | compactor-sort-key §2.6 (N=1) | dormancy 확정 = 설계 anchor | N=2 | 미충족 (확립 source-of-truth 의무 successful application) | — | non-trigger (positive signal) |
| G ↔ M — per-task NEEDS_FIXES scope 종속 | compactor-sort-key §2.1 G (70%) | 본 Story M (0% 대조) | N=1 (양극 대조 쌍, recurrence 아님) | — | — | carrier (3rd sample 시 scope-vs-hygiene 상관 평가) |
| N — CFP-637 derived default minimal-interaction | compactor-sort-key (1-question) | 본 Story (0-question) | N=2 | 미충족 (positive process signal) | — | non-trigger |
| I — merge-during-PR conflict | compactor-sort-key §2.3 (N=1) | **no match** (CI 1발 PASS, merge conflict 0, sibling chore 0) | N=1 유지 | — | — | carrier 유지 (1/2) |
| J — CI unblock saga (tech debt vs 신규 결함 분류) | compactor-sort-key §2.4 (N=1) | **no match** (CI 1발 PASS, unblock saga 0 — PR #98 sibling 이미 main 흡수) | N=1 유지 | — | — | carrier 유지 (1/2) |

**결론**: defect/process-mechanism recurrence threshold (§D-9 N≥2 정량) 도달 = Pattern K (N=2) + H/L/N (N=2). 그러나 **plugin-codeforge design-guidance absence semantics 충족 = 0건**:
- Pattern K = N=2 정량 도달하나 **mctrader-data infra governance 영역 (plugin-codeforge 비대상)** → non-trigger (§2.3 semantics gate 정상 동작)
- Pattern H/L/N = N=2 도달하나 확립 절차 successful application / positive process signal → non-trigger (U3-MIGRATE retro §2.7 판정 원칙 정합)
- Pattern G↔M = 양극 대조 쌍 (defect recurrence 아님), I/J = no match (carrier 1/2 유지)

mandatory ADR trigger **non-emit** — `cross_story_pattern_adr_trigger` = null (threshold semantics 미충족). codeforge §D-9 forcing function = intact (정량 N=2 도달해도 semantics gate 가 scope 외/positive-signal 패턴 차단 정상 동작 — U3-MIGRATE retro §2.7 와 동형 판정 원칙).

## 3. ADR 후보 발의 (PMO proposer only)

**ADR 후보 = 0건**. threshold semantics 미충족 (§2.6) → `escalation_action` 미설정 (mandatory fill 조건 자체 미충족 — anchor_id ≥ 2 strict primary 채널 미충족: 본 Story lane FIX 루프 0 = review-verdict-v4 anchor_id 미생성 + root_cause_class fallback hybrid 채널 N=2 도달하나 plugin-codeforge design-guidance absence semantics 미충족).

신규/변경 ADR = 0 (spec §3.4 — 기존 ADR-017 Amendment 3 + ADR-009 §D2.8 mctrader-hub#398 `bba73f4` longest-first suffix-strip 규약 준수, `parse_ts_from_segment` 가 이미 체현, 본 refactor 가 `parse_node_id_from_segment` 로 확장 + DRY 통합).

### 3.1 deferred carrier 상태 (이전 retro carrier 갱신)

```yaml
deferred_carriers:
  - pattern: K (branch protection matrix-name 미스매치 → admin merge)
    state: N=2 REACHED (compactor-sort-key §2.5 + 본 Story) — codeforge §D-9 non-trigger (semantics 미충족, mctrader-data infra governance)
    escalation_target: "mctrader-data infra governance — 별 chore Story 후보 (branch protection contexts ↔ matrix job 명 정합 OR ci aggregate job 추가). plugin-codeforge 비대상 (§9 feedback empty 정합)"
    codeforge_carrier: closed (plugin-codeforge scope 외 확정 — semantics gate 정상 차단)
  - pattern: H (out-of-scope → follow-up → closure lifecycle)
    state: closure 달성 (본 Story 가 compactor-sort-key follow-up #3 종결) — carrier 종결
    emit_condition: N/A (lifecycle 1 사이클 완주, 확립 절차 검증 완료)
  - pattern: L (ADR-073 verify-via 효과)
    state: N=2 (compactor-sort-key §2.6 + 본 Story dormancy anchor) — positive signal, non-trigger
    emit_condition: "verify-via 효과 = 누적 baseline (확립 의무 successful application, ADR 후보 아님)"
  - pattern: "G↔M (per-task NEEDS_FIXES scope/자유도 종속)"
    state: N=1 (compactor-sort-key G 70% + 본 Story M 0% = 양극 대조 쌍)
    emit_condition: "3rd Story sample 시 scope-vs-hygiene 상관 정량 평가 → ADR 후보 (implementer hygiene pre-checklist vs scope-constraint 분기)"
  - pattern: I (merge-during-PR conflict)
    state: 1/2 (compactor-sort-key §2.3 only, 본 Story no match — CI 1발 PASS)
    emit_condition: "동일 merge-during-PR file-overlap 패턴 재발 시 발의"
  - pattern: J (CI unblock saga tech debt vs 신규 결함 분류)
    state: 1/2 (compactor-sort-key §2.4 only, 본 Story no match)
    emit_condition: "동일 CI unblock saga 분류 패턴 재발 시 발의"
```

### 3.2 Deferred (non-ADR follow-up — spec §4 OUT 박제 완료, 정보 박제만)

ADR 후보 아닌 follow-up Story 후보 (spec §4 OUT, ADR proposer 영역 아님):

1. error contract 통일 (DEFAULT → ValueError) — Researcher U1 zero-regression mandate 위배, **명시 제외** (재발의 금지 — helper docstring + caller 인접 주석 + R2 scope-creep guard 박제)
2. gc.py / gc_daemon.py string-slicing `.compacted` 경로 통합 (Researcher U2) — 별 Story 후보
3. WAL segment filename grammar domain-knowledge 페이지 신설 (DomainAgent 권고) — 별 doc Story 후보
4. branch protection contexts ↔ matrix job 명 정합 (Pattern K N=2) — **mctrader-data infra governance 별 chore Story 후보** (plugin-codeforge 비대상)

## 4. ESCALATE trend

| Story | Lane | ESCALATE 횟수 | FIX budget 사용 | per-task NEEDS_FIXES | design re-write |
|---|---|---|---|---|---|
| compactor-sort-key-l1-naming | All | 0 | 0 (lane FIX 미발동, subagent per-task 7/10) | 7/10 (Pattern G) | 0 |
| **parse-node-id-suffix-strip** | All | **0** | **0** (lane FIX 미발동) | **0/5** (Pattern M 대조) | **0** |
| **누적 trend** | - | **0** (U2/U3/compactor-sort-key/본 Story = 4 Story 연속 baseline) | 0 (lane FIX 루프 미발동 — single-session internal Story 특성) | - | 0 |

본 Story = critical blocker 0, lane-level FIX 루프 미발동 (Max FIX 카운터 0), design re-write 0, 사용자 ESCALATE 0. CI 1발 PASS (ubuntu + windows + check-gate + CodeQL — windows 정상 = PR #98 compactor-sort-key sibling chore 가 이미 main 흡수 효과, testcontainers Windows skip 가드 기 적용) → compactor-sort-key Pattern J (CI unblock saga 5 이슈) 와 대조적 no-saga. admin merge (`enforce_admins:false`) = governance escape hatch 정당 사용 (ESCALATE 아님, Pattern K §2.3). ESCALATE trend = **0 유지** (U2-HELPER/U3-MIGRATE/compactor-sort-key + 본 Story = 4 Story 연속 0). 양호.

## 5. Cross-Story pattern threshold check (CFP-665 / ADR-045 Amend5 §D-9)

```yaml
pmo_output_v1.2:
  cross_story_pattern_adr_trigger: null
  detection_channel_evaluation:
    primary_strict_anchor_id_ge_2: not_met
      # 본 Story = no formal review-verdict-v4 anchor_id (단일 세션 internal Story,
      # lane FIX 루프 0, formal Issue 미할당 — review-verdict-v4 anchor_id 미생성)
    secondary_fallback_root_cause_class_ge_2: met_but_semantics_filtered
      # Pattern K = N=2 REACHED (compactor-sort-key §2.5 N=1 + 본 Story 재발,
      # branch protection matrix-name SSOT drift 동일 mechanism)
      # BUT §D-9 "plugin-codeforge design-guidance absence" semantics 미충족
      # (mctrader-data infra governance 영역, plugin-codeforge 정책/skill/agent
      # contract 비대상) → non-trigger (U3-MIGRATE retro §2.7 확립 판정 원칙 정합)
  pattern_k_n2_verdict:
    quantitative_threshold: REACHED (N=2 — compactor-sort-key N=1 carrier + 본 Story)
    mechanism: "branch protection required_status_checks.contexts ↔ actions matrix job 명 SSOT drift → enforce_admins:false admin merge"
    semantics_gate: NON_TRIGGER
    reason: >
      Pattern K = mctrader-data 의 GitHub branch protection ↔ workflow matrix job
      명 SSOT drift (consumer repo infra governance 결함, 코드 결함 아님).
      CFP-665 / ADR-045 §D-9 cross-Story pattern = plugin-codeforge 정책/skill/
      agent contract design-guidance absence 한정. consumer repo infra governance
      ≠ codeforge design-guidance absence → forcing function scope 외.
      정량 N=2 도달하나 semantics gate 가 정상 차단 (U3-MIGRATE retro §2.7
      Pattern I/L "표준 git hygiene / 확립 절차" non-trigger 분류와 동형 —
      defect recurrence 정의 충족하나 design-guidance absence semantics 미충족).
    escalation_target: "mctrader-data infra governance 별 chore Story 후보 (§3.2 #4, §9 feedback empty 정합 — plugin-codeforge 비대상)"
  reason: >
    Pattern K = N=2 정량 도달 / semantics 미충족 (plugin-codeforge 비대상) →
    non-trigger. Pattern H/L/N = N=2 도달하나 확립 절차 successful application /
    positive process signal → non-trigger (U3-MIGRATE §2.7 판정 원칙 정합).
    Pattern G↔M = 양극 대조 쌍 (defect recurrence 아님). Pattern I/J = no match
    (carrier 1/2 유지). threshold semantics 충족 0건 → mandatory ADR trigger
    non-emit, escalation_action 미설정 (mandatory fill 조건 미충족).
  carriers_status:
    - "Pattern K (branch protection matrix-name → admin merge) - N=2 REACHED, codeforge non-trigger (semantics), mctrader-data infra Story 후보로 escalate, codeforge carrier closed"
    - "Pattern H (out-of-scope → follow-up → closure lifecycle) - closure 달성, carrier 종결"
    - "Pattern L (ADR-073 verify-via 효과) - N=2, positive signal non-trigger, baseline 누적"
    - "Pattern G↔M (per-task NEEDS_FIXES scope 종속) - N=1 양극 대조 쌍, 3rd sample 시 평가"
    - "Pattern I (merge-during-PR conflict) - 1/2 유지 (본 Story no match)"
    - "Pattern J (CI unblock saga 분류) - 1/2 유지 (본 Story no match)"
    - "Pattern N (CFP-637 derived default minimal-interaction) - N=2 positive signal, baseline 누적"
  forcing_function_status: "intact — Pattern K 정량 N=2 도달했으나 semantics gate (plugin-codeforge design-guidance absence) 정상 차단. PMOAgent self-decide 영역 제거 준수 (semantics 판정은 self-decide 아닌 §D-9 정의 적용 — U3-MIGRATE §2.7 확립 원칙). re-evaluate at next Story retro write"
```

ArchitectAgent spawn 의무 **미발동** — `escalation_action` 미설정 (threshold semantics 미충족, mandatory fill 조건 자체 미충족). anchor_id ≥ 2 strict primary 채널 미충족 (lane FIX 루프 0) + root_cause_class fallback hybrid 채널은 Pattern K N=2 정량 도달하나 **plugin-codeforge design-guidance absence semantics 미충족** (mctrader-data infra governance 비대상). U3-MIGRATE retro §2.7 확립 판정 원칙 ("N=2 도달해도 §D-9 design-guidance absence semantics 충족해야 trigger") 정합 — 정량 임계와 semantics gate 의 2-stage 판정 일관 적용.

## 6. Cross-Story carrier baseline 박제

본 retro 가 다음 carrier baseline source:

```yaml
carrier_baselines:
  pattern_k_escalation:
    inherited: [branch protection required_status_checks.contexts ↔ actions matrix job 명 SSOT drift, enforce_admins:false admin merge governance escape hatch]
    state: N=2 REACHED (compactor-sort-key §2.5 + 본 Story)
    codeforge_disposition: non-trigger (semantics 미충족, plugin-codeforge 비대상 — carrier closed)
    mctrader_data_disposition: "별 chore Story 후보 — branch protection contexts 를 matrix job 명과 정합 OR ci aggregate job 추가. infra governance 영역 (plugin-codeforge 비대상)"
    timing: independent (mctrader-data infra governance backlog)
  pattern_h_closure:
    inherited: [compactor-sort-key retro §6 follow-up #3 — parse_node_id_from_segment latent bug]
    state: CLOSURE 달성 (본 Story 가 _strip_segment_suffixes longest-first helper + 양쪽 흡수 + AC-1 byte-identical regression-0 BLOCKING 으로 종결)
    timing: 완주 (out-of-scope discovery → follow-up 박제 → 별 Story closure lifecycle 1 사이클)
  to_followup_story_gc_compacted_consolidation:
    inherited: ["gc.py / gc_daemon.py string-slicing .compacted 경로 통합 (Researcher U2)", "parse_node_id_from_segment SSOT 정합 — 현재 string-slicing 우회 → helper 경유 통합 후보"]
    timing: independent (spec §4 OUT — 별 Story 후보)
  to_followup_doc_story_wal_filename_grammar:
    inherited: ["WAL segment filename grammar domain-knowledge 페이지 신설 (DomainAgent 권고)", "_strip_segment_suffixes longest-first SSOT 규약 박제 source"]
    timing: independent (doc Story 후보)
  to_future_stories:
    inherited:
      - "Pattern G↔M scope-vs-hygiene 대조 쌍 N=1 박제 (3rd sample 시 scope-constraint vs implementer hygiene pre-checklist 분기 ADR 후보 평가)"
      - "Pattern I (merge-during-PR conflict) / J (CI unblock saga 분류) carrier 1/2 유지 (compactor-sort-key only, 본 Story no match)"
      - "ADR-073 verify-via dormancy-as-design-anchor 패턴 (verify-via 가 dormancy 사실 확정 → detective fix 정당화 anchor 격상)"
      - "CFP-637/ADR-064 §결정 10 derived default dialog-0 압축 패턴 (Analyst 4 질문 → Researcher 기술분석 전부 해소, scope 극소 + Researcher behavior-change 판정 정밀도 효과)"
      - "단일 세션 internal Story (외부 follow-up 발의, formal Issue 없음) 단일 PR atomic landmine-removal 패턴 (spec §9 1-PR 압축 — AC-1 regression-0 production 샘플 fail 0 → 2-PR 회귀 미발동)"
      - "AC byte-identical regression-0 inline oracle 패턴 (old chained .replace inline = 구현 자유도 0 → implementer hygiene defect 표면적 최소, Pattern M 핵심)"
      - "PMOAgent retro template 적용 (U2-HELPER/U3-MIGRATE/compactor-sort-key retro = standard reference, 본 retro 동형 + semantics-gate 2-stage 판정 박제)"
```

## 7. 산출물 인용

- **Spec file**: `docs/superpowers/specs/2026-05-18-parse-node-id-suffix-strip.md` (§1-§12 + §11 scope_manifest + §pre_lookup_evidence 9-line verify-via 박제, compactor-sort-key retro §6 follow-up #3 closure)
- **Plan file**: `docs/superpowers/plans/2026-05-18-parse-node-id-suffix-strip.md` (5 TDD bite-sized task — Self-Review spec coverage 완전)
- **ADR**: 신규/변경 0 (spec §3.4 — 기존 ADR-017 Amendment 3 / ADR-009 §D2.8 mctrader-hub#398 `bba73f4` longest-first suffix-strip 규약 준수)
- **PR (LAND)**: [mclayer/mctrader-data#127](https://github.com/mclayer/mctrader-data/pull/127) (squash-merged sha `d8912ad`, single commit, 617 +/- 15 LOC, 4 file, 2026-05-18T02:13:23Z, labels `phase:구현`+`gate:design-review-pass`)
- **PR (sibling)**: none (단일 PR, sibling chore 미발생 — CI 1발 PASS, PR #98 testcontainers Windows skip 가드 이미 main 흡수)
- **Source files (PR #127)**:
  - `src/mctrader_data/wal/segment.py` (+39/-? — `_strip_segment_suffixes` longest-first private helper 신설 + `parse_node_id_from_segment`/`parse_ts_from_segment` 흡수, error contract 비대칭 `"DEFAULT"`/`ValueError` 각 보존)
  - `tests/wal/test_segment_parse_ts.py` (+66/-? — AC-1 regression-0 byte-identical + AC-2 `.compacted` correctness + AC-3 parse_ts 불변 + AC-4 DEFAULT 보존 + AC-5 ValueError 보존 + AC-6 helper 단위)
  - `docs/superpowers/specs/2026-05-18-parse-node-id-suffix-strip.md` (신규 154 LOC)
  - `docs/superpowers/plans/2026-05-18-parse-node-id-suffix-strip.md` (신규 373 LOC)
- **Caller invariant evidence**: `git diff origin/main -- src/mctrader_data/compactor/l1.py` = **empty** (단일 caller `l1.py:227` `_parse_segment_meta` ← `compact_segment` 시그니처 불변, AC-1 byte-identical 보존 — plan Task 5 Step 2)
- **CI**: ubuntu + windows + check-gate + CodeQL 전부 PASS 1발 (windows 정상 = PR #98 sibling chore 이미 main 흡수). Task 5 ruff E402 fix commit (test-only mid-file import, production 무영향). phase-gate-mergeable governance (labels `phase:구현`+`gate:design-review-pass` + `[설계-리뷰] PASS` comment + PR body Lane evidence) → admin merge (`enforce_admins:false`, Pattern K §2.3)
- **Origin (carrier source)**: `docs/retros/compactor-sort-key-l1-naming-retro-2026-05-18.md` §6 follow-up #3 (`to_followup_story_parse_node_id_dry`) — 본 Story = closure carrier (merged retro immutable, forward-reference only)
- **Cross-Story threshold judgment reference**: `docs/retros/U3-MIGRATE-retro-2026-05-18.md` §2.7 (N=2 도달해도 §D-9 design-guidance absence semantics 충족 판정 원칙 — 본 retro Pattern K semantics gate 정합 source)

## 8. Learnings count

```yaml
learnings_count: 7
itemized:
  - "Pattern K (branch protection matrix-name 미스매치 → admin merge) N=2 REACHED — 정량 임계 도달하나 §D-9 plugin-codeforge design-guidance absence semantics 미충족 (mctrader-data infra governance 비대상) → non-trigger. semantics gate 가 scope 외 패턴 차단 정상 동작 검증 (U3-MIGRATE §2.7 판정 원칙 정합)"
  - "Pattern H (out-of-scope → follow-up → closure) lifecycle 1 사이클 완주 — compactor-sort-key Task 2 discovery → retro §6 carrier → 본 Story closure, scope-discipline 표준 절차 cross-Story 정상 동작 검증, carrier 종결"
  - "Pattern M (scope 극소 + Researcher fix 설계 → per-task NEEDS_FIXES 0%) — compactor-sort-key Pattern G 70% 대조군, NEEDS_FIXES 율이 implementer hygiene 자체보다 scope·구현자유도 종속 가능성 박제"
  - "Pattern L 연장 — ADR-073 verify-via 가 dormancy 사실 확정 → detective fix 정당화 설계 anchor 격상 (외부 follow-up 발의 Story, single production caller .ndjson.sealed 전용 verify)"
  - "Pattern N — CFP-637/ADR-064 §결정 10 derived default dialog-0 압축 (Analyst 4 질문 전부 Researcher 기술분석 해소, compactor-sort-key 1-question 대비 minimal-interaction 강화)"
  - "AC-1 byte-identical regression-0 inline oracle 패턴 — old chained .replace inline = 구현 자유도 0 → implementer hygiene defect 표면적 최소 (non-tautological 진성 regression guard, Pattern M 핵심 메커니즘)"
  - "단일 세션 internal Story (외부 follow-up 발의, formal Issue 없음) 단일 PR atomic landmine-removal — spec §9 1-PR 압축 (AC-1 regression-0 production 샘플 fail 0 → 2-PR 회귀 미발동), ADR-045 §D-5 4-field schema = spec §13 등가 박제 (Story file 부재)"
```

## 9. Feedback back to codeforge

```yaml
feedback_back_to_codeforge: []
reason: >
  본 Story 범위 내 plugin-codeforge 정책/skill/agent contract 결함 0건.
  Pattern K (branch protection matrix-name 미스매치 → admin merge) = cross-Story
  N=2 REACHED 이나 mctrader-data infra governance 영역 (GitHub branch protection
  required_status_checks.contexts ↔ actions workflow matrix job 명 SSOT drift —
  코드 결함 아님, 인프라 거버넌스 결함). CFP-665 / ADR-045 §D-9 cross-Story
  pattern = plugin-codeforge 정책/skill/agent contract design-guidance absence
  한정 → consumer repo infra governance 비대상. semantics gate 가 정량 N=2
  도달에도 scope 외 패턴 차단 정상 동작 (U3-MIGRATE retro §2.7 확립 판정 원칙
  정합) → plugin-codeforge feedback empty. mctrader-data 측 별 chore Story
  후보로 §3.2 #4 + §6 carrier 박제 (branch protection contexts ↔ matrix job
  명 정합 OR ci aggregate job 추가).
  Pattern G↔M (per-task NEEDS_FIXES scope/자유도 종속) = compactor-sort-key
  retro §3.1 ADR 후보 1 (implementer hygiene pre-checklist) 의 대조군 sample —
  N=1 양극 대조 쌍, 3rd Story sample 시 scope-constraint vs hygiene pre-checklist
  분기 ADR 후보 평가 (현 시점 confirmed plugin-codeforge 결함 아님 — carrier 박제).
  현 시점 confirmed plugin-codeforge 결함 0 → empty list.
```

[PMOAgent retro authored — ADR-045 Amend1-5 mandate 정합 / CFP-138 D-5 4-field schema (spec §13 등가 박제, Story file 부재) / CFP-665 D-9 cross-Story threshold check (Pattern K N=2 REACHED → semantics gate non-trigger, plugin-codeforge 비대상) / ADR-073 verify-via source-of-truth 준수 / U3-MIGRATE §2.7 2-stage 판정 원칙 (정량 임계 + semantics gate) 정합]
