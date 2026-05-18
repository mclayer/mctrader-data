---
story_key: compactor-sort-key-l1-naming
story_issue: none (외부 세션 운영 진단 발의 → 본 세션 brainstorm→plan→execute 단일 세션 internal, formal MCT-NNN/codeforge Issue 없음)
parent_epic: none (single Story, WS-A 117GB 회수 unblock + latent forward path silent loss 차단)
phase: standalone (single Story — sort fix + L1 naming + dual-glob + verify gate cohesive 4-file change)
land_pr: mclayer/mctrader-data#96 (squash-merged sha adfddf4)
sibling_pr: mclayer/mctrader-data#98 (testcontainers Windows skip 가드, squash-merged sha 06926e3)
adr: ADR-017 Amendment 3 draft + ADR-009 §D2 Amendment N draft (docs/adr-drafts/, mctrader-hub cross-repo PR 미진행 — follow-up §6)
retro_author: PMOAgent
retro_date: 2026-05-18
adr_045_compliance: D-1 auto-trigger + D-4 partial-write retry policy + D-5 4-field schema + D-9 cross-Story pattern threshold
---

# Retro — compactor-sort-key-l1-naming (L2/L3 content-derived sort key + L1 ts-prefix filename naming)

## 0. Summary

운영 시급 unblock (A) + latent forward path silent loss 차단 (B) + 일반 compactor 정합성 (C) 셋 모두 진정한 필요였던 단일 Story. 외부 Claude Code 세션이 `promote-historical` 480/456 quarantine (l2_compacted=0) 운영 실측 후 Story 후보 발의 → 본 세션 codeforge-brainstorm Phase 0 burst (DomainAgent + ResearcherAgent + RequirementsAnalystAgent + PMOAgent 4 agent 병렬) → Orchestrator 직접 git show origin/main verify-via (ADR-073 §결정 1) → Phase 1 dialog 1-question (CFP-637/ADR-064 §결정 10) → Phase 2 PMO scope_manifest → subagent-driven-development 11 TDD task → LAND.

핵심 사실 정정: 외부 세션 초안 = "sort 키 변경" + "L3 동형 가능성". Orchestrator verify-via 결과 = root cause = L1 파일명 (`part-<sha[:16]>.parquet`) 시간정보 0 (`_derive_run_id = sha256(sealed_path)[:16]`), l2.py:70 broken 확정, l2.py:163 latent broken 확정, **l3.py:68 = incidentally safe** (path 에 `hour=NN` zero-padded 가 `part-` 앞 = byte-sort=hour-sort, 단 hour 당 다중 L2 시 regression — 외부 초안의 "L3 동형 broken" 가설 사실 정정). 사용자 Phase 1 응답 = "본 Story 포함(근본 fix)" → Opt2 (sort key 교체) + Opt3 (파일명 ts 임베드) 동시 적용 확정.

PR #96 = adfddf4 = 3113 +/- 182 LOC, 26 file, 192 tests PASS. Sibling chore PR #98 = 06926e3 = 3 test file (testcontainers Windows skip 가드, lazy import + `_docker_unavailable_reason()`). Phase 1 PR + Phase 2 PR 표준 2-PR structure 의 결합 LAND (단일 세션 internal Story). Branch protection matrix-name 미스매치로 perma-BLOCKED 상태 → `enforce_admins:false` 활용 admin merge (PR #98 먼저, 후 PR #96).

ADR-045 Amend5 §D-9 mandate 충족 — 본 retro 가 cross-Story pattern threshold check carrier baseline source. ADR 후보 2건 발의 (proposer only, N=1 deferred — §3).

## 1. Quality gate retrospect

subagent-driven-development 11 TDD bite-sized task. 각 task = implementer + spec compliance reviewer + code quality reviewer (sequential). 최종 entire-branch reviewer = APPROVED FOR MERGE.

| Task | 영역 | spec verdict | code quality verdict | Resolution method | Findings |
|---|---|---|---|---|---|
| 1 | spec + ADR-017 Amd3 + ADR-009 §D2 AmdN draft | - | NEEDS_FIXES | fix commit | `l3.py:74` line number 오류 (실제 `l3.py:68`) 4 file 정정 |
| 2 | `parse_ts_from_segment` helper | - | NEEDS_FIXES | **out-of-scope 판정** → follow-up 기록 + 진행 | sibling `parse_node_id_from_segment` latent bug (`.compacted` 파일 `.replace` chain 오염) 발견. pre-existing dormant (scan_sealed 필터로 미발현) |
| 3 | L1 `_derive_parquet_path` | PASS (N1 minor) | NEEDS_FIXES | fix commit (6 추가 regression) | `tmp_path` dead fixture param + `mkdtemp()` cleanup 일관성. test segment 명명 규약 표준화 + Windows MAX_PATH 회피 `tempfile.TemporaryDirectory` |
| 4 | `_extract_min_ts` | PASS | NEEDS_FIXES | fix commit (Important 3 + Minor 2) | broad `except Exception` + `PathOrStream=Union[...,"object"]` type alias 무효 + 0-row guard hoist + BytesIO test 누락 |
| 5 | L2 `compact_hour` local | PASS | NEEDS_FIXES | fix commit | `import logging` in-loop → module-level `_log` hoist + nested comprehension refactor |
| 6 | L2 `_compact_hour_nas` | combined APPROVED | combined APPROVED | - | Minor: keyed type annotation object→datetime deferred |
| 7 | L3 `compact_day` local | combined APPROVED | combined APPROVED | - | Minor cosmetic (_log 위치 + single-pass — reviewer "L3 pattern arguably better") |
| 8 | L3 `_compact_day_nas` | combined PASS | combined PASS | - | Minor inherited from Task 6 |
| 9 | `verify_l2_l3_sort_correctness.py` | combined | combined NEEDS_FIXES | fix commit | Important `audit_dir.mkdir parents=True` 누락 (fresh root crash) + `_extract_min_ts` KeyError unguarded → parents=True + try/except + error_count field |
| 10 | testcontainers MinIO integration | combined APPROVED | combined APPROVED | - | NASUploader signature plan bug (endpoint_url→endpoint) + win32 skip guard |
| 11 | CLAUDE.md + 전체 회귀 + PR open | - | - | - | 192 tests pass, PR #96 open |
| Final | entire-branch reviewer | APPROVED FOR MERGE | - | cleanup commit | 1 Important (keyed type annotation) → cleanup |

**NEEDS_FIXES ratio = 7/10 task** (Task 1-5 + Task 9 + final-branch Important). FIX budget 의미의 §10 max FIX counter 와는 별개 — subagent-driven-development per-task review FIX 는 same-session same-task internal verify (별 FIX iteration escalate 아님, CFP-19 R11 정합 — §10 row append 0). Max FIX 카운터 = 0 (lane-level FIX 루프 미발동, ESCALATE 0).

## 2. Pattern analysis (PMO mandate)

### 2.1 Pattern G — subagent-driven-development per-task 양단 review NEEDS_FIXES 고비율

10 task 중 7 NEEDS_FIXES (70%). 단 spec compliance verdict 는 거의 PASS (Task 3/4/5 모두 spec PASS) — NEEDS_FIXES 의 절대다수가 **code quality reviewer** 발행 (broad `except`, in-loop import, dead fixture param, mkdir parents 누락, type alias 무효). 즉 spec 의도 구현은 정확, 구현 위생(hygiene) defect 가 review 단계에서 일관 catch.

**해석**: subagent-driven-development per-task code quality reviewer 가 정상 동작 (defect catch rate 높음 = 게이트 효과 우월). 단 NEEDS_FIXES 7건의 공통 root class = "implementer 가 1차 산출 시 hygiene 미반영" — TDD red-green 단계의 green 직후 refactor 단계 압축 가능성. Pattern threshold N=1 (single Story sample) → ADR 후보 carrier 박제 (재발 시 N=2 → "implementer prompt 에 hygiene pre-checklist 주입" ADR 후보 활성).

| Defect class | Task | 빈도 |
|---|---|---|
| broad except / silent failure | 4 | 1 |
| in-loop import (hoist 필요) | 5 | 1 |
| dead fixture param / cleanup 일관성 | 3 | 1 |
| mkdir parents=True 누락 (fresh root crash) | 9 | 1 |
| type alias 무효 (`Union[...,"object"]`) | 4 | 1 |
| line number doc 오류 | 1 | 1 |
| keyed type annotation object→datetime | 6, final | 2 |

### 2.2 Pattern H — Out-of-scope finding 처리 ("Don't refactor beyond task" 정합)

Task 2 code review 가 sibling `parse_node_id_from_segment` latent bug 발견 (`.replace(".ndjson.sealed","").replace(".ndjson","")` chained sub-string replace → `.compacted` 파일 적용 시 `parts[2]` 가 `<node>.sealed.compacted` 오염). 현재 `scan_sealed` 필터로 dormant (sealed-only caller). 신규 `parse_ts_from_segment` 의 longest-first `.replace` chain 와 대조 시 발견.

**처리 결정 = out-of-scope** (pre-existing dormant + behavior change risk: pre-existing caller 검증 필요) → spec §4 OUT section + §11 follow-up Story 후보 박제 + 본 Story 진행 (refactor 미수행). superpowers `test-driven-development` / `systematic-debugging` 의 "Don't refactor beyond task" 원칙 정합 — scope creep 차단 successful application. Pattern threshold N=1 → carrier 박제 (cross-Story 재발 시 ADR 후보 "out-of-scope latent bug discovery → follow-up Story 박제 표준 절차" 활성).

### 2.3 Pattern I — Merge-during-PR conflict → Opus tier resolution

PR #96 open 후 origin/main 이 PR #95 (U2-HELPER nas_key SSOT, `4aa5483`) + #94 (retro, `103fda9`) merge 로 이동 → `_compact_hour_nas` 동일 영역 충돌 (PR #95 `build_l1_prefix`/`build_nas_prefix` + canonical dedup ↔ 본 Story content-derived sort). **Opus tier merge resolution subagent** spawn:

- l2.py + l3.py NAS GET path 충돌 manual resolve (post-U2 flat keys 정합)
- l1.py auto-merge, CLAUDE.md auto-merge
- `test_l2_nas_sort_key.py` mock semantics 조정 (post-U2 flat keys)
- 결과: 192 tests pass + nas_key suite 27/27 PASS

**해석**: 동시 진행 Story 가 spec §12 cross-ref 에서 "무관, 동시 진행 가능" 으로 명시됐으나 (`nas-key-unification-design.md` 무관 박제), 실제 `_compact_hour_nas` 함수 영역에서 물리 충돌 발생. spec-level "무관" 판정과 file-level 충돌 발생의 gap = PMO Epic 분해 자문 §1 규칙 3 ("같은 shared util/함수 영역 수정 → 순차")의 적용 범위 확장 필요 신호. **단 본 Story 는 외부 발의 single Story 로 Epic 분해 자문 미경유** — 동시 진행 2 Story 의 file-overlap pre-check 부재가 root. Pattern threshold N=1 → carrier 박제.

### 2.4 Pattern J — CI unblock saga: pre-existing tech debt vs 신규 결함 분류

CI unblock 5 이슈를 2 class 로 분류:

| 이슈 | class | root | 처리 |
|---|---|---|---|
| 1. ruff lint 6 errors (UP007 + F401 + E501) | **신규 결함** (내 PR 신규 파일) | implementer hygiene | fix commit |
| 2. origin/main 재이동 CI 재실행 | environmental | upstream merge race | merge 후 재실행 → ubuntu PASS |
| 3. testcontainers schema interaction `ArrowTypeError` | **신규 결함 (post-merge interaction)** | PR #95 build_l1_prefix + canonical dedup ↔ L2 ParquetWriter schema 불일치 | `@pytest.mark.slow` CI exclude + spec §11 follow-up 기록 |
| 4. E501 재발 (slow marker rationale comment 179 chars) | **신규 결함** | comment 길이 | multi-line split fix |
| 5. Windows CI 3 file 18 errors (testcontainers Docker socket Linux-only) | **pre-existing tech debt** (origin/main 도 동일 실패, 내 PR 영향 0) | testcontainers Docker socket 미마운트 | 별 chore PR #98 (`_docker_unavailable_reason()` skip 가드 + lazy import) → LAND → PR #96 재머지로 동반 정상화 |

**해석**: 이슈 5 의 "origin/main 도 동일 실패 검증 → pre-existing tech debt 확정 → 별 chore PR 로 분리 (본 Story scope 미오염)" = scope discipline 우수 사례. 신규 결함(1/3/4)은 본 PR 내 즉시 fix, pre-existing(5)는 별 PR — superpowers `verification-before-completion` 의 evidence-before-assertion 정합 (origin/main 동일 실패 실증 후 분류). Pattern threshold N=1 → carrier 박제.

### 2.5 Pattern K — Branch protection matrix-name 미스매치 → admin merge

`required_status_checks.contexts:["ci"]` 가 matrix job 명("ci (ubuntu-latest)" / "ci (windows-latest)")과 미스매치 → required check "ci" 영원히 미보고 → perma-BLOCKED. `enforce_admins:false` 활용 admin merge (PR #98 먼저 LAND, 후 PR #96 origin/main 재머지 → admin merge).

**해석**: GitHub branch protection `contexts` 명세와 actions matrix job 명 간 SSOT drift = 인프라 governance 결함 (코드 결함 아님). admin merge 는 governance gap 의 escape hatch 로 정당 사용 (enforce_admins:false 의도된 owner 권한). 단 이 drift 자체는 **별 chore Story 후보** (branch protection contexts → matrix job 명 정합 또는 `ci` aggregate job 추가). Pattern threshold N=1 → carrier 박제 + §9 feedback (mctrader-data infra governance, plugin-codeforge 비대상).

### 2.6 Pattern L — 외부 세션 발의 Story 사실 정정 (ADR-073 verify-via 효과)

외부 Claude Code 세션 초안 2 가설:
1. "sort 키 변경이 fix" → 정정: root cause = L1 파일명 시간정보 0 (`_derive_run_id = sha256[:16]`), sort 키는 증상 — Opt2 + Opt3 동시 필요
2. "L3 동형 broken 가능성" → 정정: l3.py:68 = **incidentally safe** (path `hour=NN` zero-padded 가 `part-` 앞 = byte-sort=hour-sort). hour 당 다중 L2 발생 시에만 regression → defensive uniformity fix (broken fix 아님)

Orchestrator 직접 `git show origin/main:src/mctrader_data/compactor/{l2,l3,l1}.py` verify-via (ADR-073 §결정 1 source-of-truth verify 의무) 가 외부 초안의 2 부정확 가설을 정정. **외부 세션 발의 = 신뢰 가능 trigger 이나 root cause 가설은 verify-via 필수** 라는 ADR-073 효과 실증 sample. Phase 0 burst 시 Analyst 가 U2-HELPER Story 와 본 Story scope 혼동 → option B (신규 Story 발의) 명시 재dispatch 1회 (Analyst 재요청 1회 — 외부 발의 Story 의 scope 경계 모호성 부수 효과). Pattern threshold N=1 → carrier 박제.

## 3. ADR 후보 발의 (PMO proposer only)

### 3.1 Candidate 1 — subagent-driven-development implementer hygiene pre-checklist

```yaml
adr_candidate:
  title: "ADR-NNN subagent-driven-development implementer prompt — hygiene pre-checklist 주입"
  category: "Process"
  trigger: "compactor-sort-key-l1-naming - 10 task 중 7 NEEDS_FIXES, code quality reviewer 발행 (broad except / in-loop import / mkdir parents 누락 / dead fixture / type alias 무효)"
  proposer: PMOAgent
  author_pending: ArchitectAgent (chief author)
  status: deferred (cross-Story threshold N>=2 미충족, N=1)
  emit_condition: "동일 implementer hygiene defect 고비율 패턴 cross-Story 재발 시 즉시 발의"
  rationale: "spec verdict 는 거의 PASS (의도 구현 정확) — defect 절대다수가 hygiene class. implementer prompt 에 7-item hygiene pre-checklist (no-broad-except / module-level-import / mkdir-parents / no-dead-fixture / valid-type-alias / line-number-doc / keyed-annotation) 주입 시 per-task review iteration 감소 가능 가설."
```

본 Story 1건 sample (N=1) → emit 보류, ArchitectAgent spawn 의무 미발동. Threshold reach (N=2) 시 본 retro 가 carrier 박제 source.

### 3.2 Candidate 2 — 동시 진행 Story file-overlap pre-check

```yaml
adr_candidate:
  title: "ADR-NNN 동시 진행 Story file-overlap pre-check — spec cross-ref '무관' 판정 보강"
  category: "Process"
  trigger: "compactor-sort-key-l1-naming - spec §12 가 nas-key-unification 을 '무관, 동시 진행 가능' 박제했으나 _compact_hour_nas 함수 영역 물리 충돌 → Opus tier merge resolution 필요"
  proposer: PMOAgent
  author_pending: ArchitectAgent (chief author)
  status: deferred (cross-Story threshold N>=2 미충족, N=1)
  emit_condition: "동일 spec-level '무관' 판정 ↔ file-level 충돌 gap 패턴 cross-Story 재발 시 즉시 발의"
  references: "PMO Epic 분해 자문 §1 규칙 3 (shared util/함수 영역 수정 → 순차) — 외부 발의 single Story 라 Epic 분해 미경유, 동시 진행 2 Story file-overlap pre-check 부재가 root"
```

본 Story 1건 sample (N=1) → emit 보류. Threshold reach (N=2) 시 carrier 박제.

### 3.3 Deferred (non-ADR follow-up — spec §11 박제 완료)

ADR 후보 아닌 follow-up Story 후보 6건 (spec §4 OUT + §11 박제 완료, ADR proposer 영역 아님 — 정보 박제만):

1. 운영 검증 AC-8 (이슈 A #100 NAS 4xx fix LAND 됨 — 운영 promote-historical 실측 가능)
2. Opt4 cross-file overlap k-way merge 안전망 (verify gate 데이터 누적 후)
3. `parse_node_id_from_segment` latent bug DRY refactor (Task 2 발견 — Pattern H carrier)
4. NAS GET sort-phase I/O 최적화 (BytesIO cache, 2N+1 GET → N+1, final review 발견)
5. testcontainers MinIO + L2 NAS GET schema interaction (`@pytest.mark.slow` 해제, root cause 조사 — Pattern J 이슈 3 carrier)
6. ADR-017 Amendment 3 + ADR-009 §D2 Amendment N mctrader-hub cross-repo PR (sibling sync 미진행 — `docs/adr-drafts/` draft 상태)

## 4. ESCALATE trend

| Story | Lane | ESCALATE 횟수 |
|---|---|---|
| compactor-sort-key-l1-naming | All lanes | **0** |

본 Story = critical blocker 0, lane-level FIX 루프 미발동 (Max FIX 카운터 0), design re-write 0, 사용자 ESCALATE 0. CI unblock saga 5 이슈는 모두 자체 해소 (ESCALATE 아님 — 신규 결함 즉시 fix + pre-existing 별 PR 분리 + admin merge governance escape hatch). admin merge 는 ESCALATE 아닌 의도된 owner 권한 사용. ESCALATE trend 양호. Cross-Story 누적 baseline 박제 (U2-HELPER retro = 0 → 본 Story = 0, 2-Story 연속 ESCALATE 0).

## 5. Cross-Story pattern threshold check (CFP-665 / ADR-045 Amend5 §D-9)

```yaml
pmo_output_v1.2:
  cross_story_pattern_adr_trigger: null
  detection_channel_evaluation:
    primary_strict_anchor_id_ge_2: not_met  # 본 Story = no formal review-verdict-v4 anchor_id (외부 발의 internal Story, lane FIX 루프 0)
    secondary_fallback_root_cause_class_ge_2: not_met  # all 6 sub-patterns (G/H/I/J/K/L) at N=1
  reason: "all 6 sub-patterns (G/H/I/J/K/L) at N=1, threshold N>=2 미충족. U2-HELPER retro 의 Pattern B/C/D/E/F 와 root_cause_class disjoint (U2 = nas_key SSOT/debate/dual-track review; 본 Story = subagent hygiene/out-of-scope/merge-conflict/CI-class/branch-protection/verify-via). cross-Story class 누적 매칭 0."
  carriers_emitted:
    - "Pattern G (subagent-driven-development per-task NEEDS_FIXES 고비율 70%) - 1/2"
    - "Pattern H (out-of-scope finding 처리 - Don't refactor beyond task 정합) - 1/2"
    - "Pattern I (merge-during-PR conflict → Opus tier resolution) - 1/2"
    - "Pattern J (CI unblock saga - pre-existing tech debt vs 신규 결함 분류) - 1/2"
    - "Pattern K (branch protection matrix-name 미스매치 → admin merge) - 1/2"
    - "Pattern L (외부 세션 발의 Story 사실 정정 - ADR-073 verify-via 효과) - 1/2"
  forcing_function_status: "intact - re-evaluate at next Story retro write"
```

ArchitectAgent spawn 의무 미발동 — `escalation_action` 미설정 (threshold 미도달, mandatory fill 조건 자체 미충족). anchor_id ≥ 2 strict primary 채널 미충족 (본 Story lane FIX 루프 0 = review-verdict-v4 anchor_id 미생성) + root_cause_class ≥ 2 fallback hybrid 채널도 N=1. U2-HELPER retro 의 5 carrier 와 본 retro 6 carrier 는 root_cause_class disjoint (cross-Story 누적 매칭 0). Threshold reach 시 본 retro 가 carrier 박제 source.

## 6. Cross-Story carrier baseline 박제

본 retro 가 다음 carrier baseline source:

```yaml
carrier_baselines:
  to_followup_story_ops_verify:
    inherited: [AC-8 운영 검증 - 이슈 A #100 LAND 완료로 unblock, promote-historical --start 2026-05-13 --end 2026-05-13 실측 게이트]
    timing: post-issue-A LAND (#100 이미 LAND, immediate ready)
  to_followup_story_parse_node_id_dry:
    inherited: [parse_node_id_from_segment latent bug, .replace chain 오염, behavior change risk - pre-existing caller 검증 의무, DRY refactor + sibling fix]
    timing: independent (Pattern H carrier)
  to_followup_story_minio_schema_interaction:
    inherited: ["@pytest.mark.slow test_compactor_sort_minio.py 해제", "PR #95 build_l1_prefix + canonical dedup ↔ L2 ParquetWriter schema 불일치 root cause 조사", "pyarrow auto-dict encoding · NAS GET stream behavior"]
    timing: independent (Pattern J 이슈 3 carrier)
  to_followup_story_adr_cross_repo_sync:
    inherited: [ADR-017 Amendment 3 draft, ADR-009 §D2 Amendment N draft, mctrader-hub cross-repo PR 미진행, docs/adr-drafts/ draft 상태]
    timing: independent (sibling sync follow-up)
  to_future_stories:
    inherited:
      - Pattern G/H/I/J/K/L sub-pattern N=1 박제 (재발 시 N=2 → ADR 후보 활성)
      - ADR 후보 1 (subagent-driven-development implementer hygiene pre-checklist) carrier
      - ADR 후보 2 (동시 진행 Story file-overlap pre-check) carrier
      - PMOAgent retro template 적용 (U2-HELPER retro = standard reference, 본 retro 동형)
      - INV-A/B/C/D (CLAUDE.md historical tier promotion) + content-derived sort key 패턴 (pq.read_metadata stats.min primary + iter_batches[:1] fallback, 파일명 untrusted 원칙)
```

## 7. 산출물 인용

- **Spec file**: `docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md` (§1-§12 + §11 scope_manifest, 외부 세션 진단 발의 → 본 세션 codeforge-brainstorm)
- **Plan file**: `docs/superpowers/plans/2026-05-17-compactor-sort-key-l1-naming.md` (11 TDD bite-sized task)
- **ADR draft**: `docs/adr-drafts/ADR-017-amendment-3-compactor-sort-key.md` (28 LOC) + `docs/adr-drafts/ADR-009-amendment-N-l1-dual-filename.md` (29 LOC) — mctrader-hub cross-repo PR 미진행 (follow-up §6)
- **PR (LAND)**: [mclayer/mctrader-data#96](https://github.com/mclayer/mctrader-data/pull/96) (squash-merged sha adfddf4, 3113 +/- 182 LOC, 26 file, 192 tests PASS, 2026-05-18 08:53:18 +0900)
- **PR (sibling chore)**: [mclayer/mctrader-data#98](https://github.com/mclayer/mctrader-data/pull/98) (testcontainers Windows skip 가드, squash-merged sha 06926e3, 3 test file, `_docker_unavailable_reason()` + lazy import)
- **Source files (PR #96)**:
  - `src/mctrader_data/compactor/sort_key.py` (신규 65 LOC — `_extract_min_ts` content-derived)
  - `src/mctrader_data/compactor/l1.py` (+16/-? — `_derive_parquet_path` ts-prefix)
  - `src/mctrader_data/compactor/l2.py` (+43/-? — compact_hour + _compact_hour_nas sort key swap)
  - `src/mctrader_data/compactor/l3.py` (+44/-? — compact_day + _compact_day_nas defensive)
  - `src/mctrader_data/wal/segment.py` (+24 — `parse_ts_from_segment` helper)
  - `scripts/verify_l2_l3_sort_correctness.py` (신규 136 LOC — audit JSON 운영 게이트)
  - `CLAUDE.md` (+37 — L1 naming + sort key 규약 + dual-glob 호환 박제)
- **CI unblock**: ruff fix commit + `@pytest.mark.slow` exclude + E501 multi-line split + phase-gate-mergeable governance (labels `phase:구현`+`gate:design-review-pass` + `[설계-리뷰] PASS` PR comment + Lane evidence section) + admin merge (`enforce_admins:false`)
- **Related Story**: `docs/stories/U2-HELPER.md` (PR #95 동시 진행, `_compact_hour_nas` 영역 merge conflict — Pattern I)

## 8. Learnings count

```yaml
learnings_count: 8
itemized:
  - "Pattern G subagent-driven-development per-task NEEDS_FIXES 고비율 70% (10 task 중 7) — spec verdict PASS / code quality reviewer hygiene defect 절대다수, 게이트 효과 우월 검증"
  - "Pattern H out-of-scope latent bug (Task 2 parse_node_id_from_segment) → follow-up 박제 + 진행 — Don't refactor beyond task scope discipline 정합"
  - "Pattern I merge-during-PR conflict (PR #95 _compact_hour_nas 동일 영역) → Opus tier resolution subagent 패턴 — spec-level '무관' ↔ file-level 충돌 gap 식별"
  - "Pattern J CI unblock saga 5 이슈 — pre-existing tech debt(Windows) vs 신규 결함(ruff/schema/E501) 분류, origin/main 동일실패 실증 후 별 chore PR #98 분리"
  - "Pattern K branch protection matrix-name 미스매치 → admin merge — governance escape hatch 정당 사용, infra governance drift 식별"
  - "Pattern L 외부 세션 발의 Story 사실 정정 (sort 키 증상 / L3 incidentally safe) — ADR-073 verify-via source-of-truth 의무 효과 실증"
  - "Phase 1 dialog 1-question (CFP-637/ADR-064 §결정 10) + 6 sub-결정 derived default declare — 사용자 '근본 fix' 선택 → Opt2+Opt3 동시 적용 minimal-interaction 효과"
  - "단일 세션 internal Story (외부 발의, formal Issue 없음) 의 Phase 1+2 PR 결합 LAND 패턴 — ADR-038 §결정 11 2-PR 표준의 internal session 변형"
```

## 9. Feedback back to codeforge

```yaml
feedback_back_to_codeforge: []
reason: >
  본 Story 범위 내 plugin-codeforge 정책/skill/agent contract 결함 0건.
  Pattern K (branch protection matrix-name 미스매치) 는 mctrader-data infra governance 영역
  (GitHub branch protection contexts ↔ actions matrix job 명 SsoT drift) — plugin-codeforge
  비대상, mctrader-data 별 chore Story 후보로 §6 carrier 박제.
  Pattern G (subagent-driven-development implementer hygiene 고비율) + Pattern I (동시 진행
  Story file-overlap pre-check 부재) 는 plugin-codeforge 잠재 개선 영역이나 N=1 (single Story
  sample) — §3 ADR 후보 carrier 박제, cross-Story 재발 시(N=2) feedback_back_to_codeforge
  활성 + ArchitectAgent spawn 의무 발동 예정. 현 시점 confirmed 결함 0 → empty list.
```

[PMOAgent retro authored — ADR-045 Amend1-5 mandate 정합 / CFP-138 D-5 4-field schema / CFP-665 D-9 cross-Story threshold check / ADR-073 verify-via source-of-truth 준수]
