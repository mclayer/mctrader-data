---
story_key: U3-MIGRATE
story_issue: mclayer/mctrader-data#89
parent_epic: EPIC-nas-key-unification (mctrader-data#86)
phase: Phase 2 (P2-3 cutover step 4 — migration tool delivery)
land_pr: mclayer/mctrader-data#102 (squash-merged sha 37d6037e)
sibling_pr: mclayer/mctrader-hub#396 (ADR-034 Amendment 5 LAND, sha 0864fb10)
adr: ADR-034 (mctrader-hub, Accepted + Amendment 1-5)
retro_author: PMOAgent
retro_date: 2026-05-18
adr_045_compliance: D-1 auto-trigger + D-4 partial-write retry policy + D-5 4-field schema + D-9 cross-Story pattern threshold (N=2 REACHED — mandatory ADR trigger emitted)
---

# Retro — U3-MIGRATE (NAS `l1/` → 평면 1회성 멱등 re-key 마이그레이션 도구, Phase 2 cutover step 4)

## 0. Summary

U3-MIGRATE = EPIC-nas-key-unification Phase 2 cutover 5-step 의 step 4 (migration **도구** 인도). 전 exchange/channel `l1/` prefix 객체 → 평면 key 1회성 멱등 re-key 마이그레이션 tool 인도 완료. copy → 4중 HEAD verify (ETag+VersionId+sha256 Metadata+ContentLength) → old key delete, `.compacted` sentinel 완료 객체만, 재실행 safe. PR #102 squash-merged sha `37d6037e` (2026-05-18), sibling hub#396 (ADR-034 Amendment 5, `rekey-l1-manifest-` wording) sha `0864fb10`.

**대상 fact 정정 박제**: Story body §3 기준 23,981 로컬 L1 snapshot 누적 중 NAS `l1/` 실측 = **4,608 objects** (~117GB). PL 결정 #1 verbatim — 차분 19,373 = WS-A historical promotion scope (별 경로, U3 비대상). U3 마이그레이션 본체 = NAS `l1/` 4,608 객체.

5 quality lane 全 PASS, FIX budget 2/3 사용 (1 unused) + pyright fast-path 1 (FIX iteration 비소비). CI ubuntu-latest + windows-latest PASS, pytest 49 passed / 2 skipped (fcntl-Windows POSIX) / 0 failed, GitHub native 6 tools clean.

**핵심**: U3 = 마이그레이션 *도구* 인도지 마이그레이션 *실행* 아님. 실제 4,608 객체 117GB re-key = **운영자 트리거** (`docker compose --profile migration run --rm rekey-migration ... --execute --i-understand-this-is-irreversible`, 의도된 operator gate). U5-VERIFY full LAND 은 operator 마이그레이션 실행 + 30일 cool-down 후 가능 (§7 cross-Story 의존성 박제).

## 1. Quality gate retrospect

| Lane | FIX iter | Verdict | Resolution method | Findings |
|---|---|---|---|---|
| Design Phase 1 (6 deputy) | - | PASS | 4-deputy convergence + chief author Verify-via | NASUploader Option X (copy/delete primitive) 4-deputy 수렴 + §11.2-A wording drift |
| Design Phase 1.5 PL audit | - | PASS | deputy re-spawn 0 | - |
| Design Phase 2 chief author | - | Codex consult `no_findings` | debate-protocol-v1 v1.2 미발동 (convergence_quality_invariant trigger 부재) | 15 PL 결정 통합 박제 |
| Design Phase 3 PL verdict | - | PASS | 4 boolean self-check | - |
| DesignReview dual-track | 1 | FIX | doc-only fast-path (chief author inline re-spawn) | 1 P1 (Manifest status enum 9-state vs 11-state wording drift, 6+ 위치) + 2 P2 (metrics inventory drift / failed_total reason enum) |
| DesignReview re-verify | 1 (lighter) | PASS | 18 위치 일괄 정정 (Change Plan 13 + Story-mirror 5) | 3/3 RESOLVED, 11-state enum 단일화 + §9.4.4 axis disambiguation SSOT 신설, 2 remaining 미사용 |
| 구현 (DeveloperPL) | - | PASS | DeveloperPL | CopyResult 4-state impl-discovered (`dst_conflict` — design 3-state 대비 strengthening) |
| SecurityTest dual-layer | 2 | FIX | DeveloperPL 재구현 | SEC-P1-1 (`_verify_4head` HEAD-3 sha256 absent-ONE-SIDE soft-pass without all_pass=False — trust-boundary, Both Claude+Codex 독립 동일 P1) |
| CodeReview dual-track | 2 | FIX | DeveloperPL 재구현 + CodeReviewPL severity reconciliation | **P0-1** (`source_not_found` branch `both_head_404` guard 부재 → permanent silent data loss, Codex P1 → CodeReviewPL **P0 elevate**) + P1-1 (mid-flight crash state 방치) + P1-2 (INV-G test sentinel-skip only, mid-state 미주입) + 6 P2 |
| SecurityTest + CodeReview re-verify | 2 (lighter) | PASS | dual-lane 통합 line-level audit RESOLVED | 4 blocking + P2-doc 모두 RESOLVED, pytest 49 passed 0 failed |
| fast-path (pyright) | - | FIX iteration **비소비** | Orchestrator/DeveloperPL mechanical fix | moto 5.x mock_aws + importlib.util + spec None guard (CFP-19 R11, pyright 19→0, §10 row append 0) |

**Max FIX 카운터 = 3 (budget 정합)**. 실제 사용 2/3 = design-review iter 1 (doc-only) + security-test/code-review iter 2 (dual-lane 통합, P0 data-loss). 1 iteration unused. pyright fast-path 는 별 FIX iteration 으로 escalate 되지 않음 (CFP-19 R11 mechanical, U2-HELPER Option A 패턴 재현 — §2.2 N=2 분석 대상).

## 2. Pattern analysis (PMO mandate)

U2-HELPER retro 의 sub-pattern B/C/D/E/F (전부 N=1 carrier 박제) 대비 cross-Story 누적 매칭 + U3 신규 후보 6종 평가. ADR-045 Amend5 §D-9 threshold = **defect-class 또는 process-mechanism recurring pattern N≥2** (positive process signal / structural carrier 는 threshold 비대상 — Google SRE "same *issue* twice" defect semantics).

### 2.1 Pattern G — data-safety P0 elevation (CodeReviewPL severity_override authority)

U3 CodeReview iter 2: Codex 가 `source_not_found` branch `both_head_404` guard 부재를 **P1** flag → CodeReviewPL 이 **P0 elevate** (severity_override, 근거: source가 copy 전 삭제된 partition = permanent silent data loss + zero test coverage, 데이터 손실 = P0 정의 충족). PL adjudication authority 박제 — verdict reconciliation 패턴 (ADR-035 Sonnet decider Deprecated 정합, PL = adjudicator).

- **본 Story 1건 sample (N=1)**. Cross-Story 누적 시 ADR 후보 "Dual-track verdict reconciliation — PL severity_override 권한 + data-safety P0 elevation 기준" 발의 예약.
- U2-HELPER 에는 severity *downgrade* (Codex P1 → PL P2, ETag-advisory design-sanctioned) 1건 존재 — *방향 반대* (upgrade vs downgrade). 동일 mechanism (PL severity reconciliation authority) 의 대조 쌍이나 patternthreshold 는 **mechanism N=2** 로 본다 → §2.7 종합 평가 참조.

### 2.2 Pattern H — Mechanical fast-path (FIX iteration 비소비) **— N=2 REACHED**

| Story | fast-path 사례 | mechanical_category | FIX iteration 소비 | Resolution |
|---|---|---|---|---|
| U2-HELPER | CodeReview iter 2 — DeveloperPL `__all__` extension 새 P1 → test_public_surface CI fail | minor (symbol export) | 비소비 (Orchestrator direct commit, code-review iter 2 내부 verify) | Option A direct commit + verify |
| **U3-MIGRATE** | pyright type errors (moto 5.x mock_aws + importlib.util + spec None guard) | minor (type-only) | **비소비** (CFP-19 R11 mechanical, §10 row append 0) | mechanical fix + CI re-verify |

**N=2 도달** (root_cause_class fallback channel — `mechanical_category` fast-path, FIX iteration 비소비 경계). U2-HELPER retro §3.2 Candidate 2 가 `emit_condition: "동일 mechanical_category fast-path 패턴 재발 시 즉시 발의"` 로 carrier 사전 박제 → **threshold reach → mandatory ADR trigger (§3 발의, forcing function intact)**.

**defect semantics 판정**: 이 패턴은 단순 git hygiene 가 아니라 "FIX iteration counter 소비 경계 + Orchestrator direct-commit 권한 + same-iteration internal verify 절차" 의 **process-mechanism 부재** 신호 — CFP-19 R11 이 inline 언급되나 정식 ADR-level 절차/권한 SSOT 부재. ADR-045 Amend5 §D-9 defect/process-mechanism recurrence 정의 충족 → `escalation_action: adr_draft_emitted`.

### 2.3 Pattern I — merge-during-PR conflict → rebase resolution

U3 PR #102 base 이동 (103fda9 → main, 5 PR 누적: #96 / #98 / #99 / #100 / #103) → `fix/u3-migrate-rekey` rebase onto origin/main, CLAUDE.md disjoint 신규 섹션 양쪽 보존, force-with-lease. U2-HELPER 에도 동형 사례 존재 (retro 미명시였으나 U3 trigger note 참조 — U2 PR #95 base 이동).

- **누적 N=2 가능성 평가**: 단 본 패턴 = **표준 git hygiene** (다중 동시 PR land 환경에서 정상·예상 workflow). 설계 지침 부재 신호 아님 (rebase + disjoint section 양쪽 보존 = 이미 확립된 절차). ADR-045 Amend5 §D-9 "design-guidance absence" 정의 미충족 → **non-trigger** (trivial, `escalate_user` 조차 불요).

### 2.4 Pattern J — impl-discovered design-faithful strengthening = ACCEPT (not escalation)

U3: design Change Plan §11.6 = CopyResult 3-state. impl 에서 DeveloperPL 이 `dst_conflict` 4번째 state 도입 (sha256 mismatch overwrite 차단 realization). CodeReviewPL adjudication = **ACCEPT** (design-faithful — Change Plan §11.6:1007 이미 "sha256 mismatch overwrite 차단" mandate, 4-state = 정확 realization, ArchitectPL escalation 불필요, residual = stale docstring P2-doc inline 정정).

- **본 Story 1건 sample (N=1)**. "impl-discovered design-faithful strengthening = ACCEPT (escalation 아님)" 패턴 carrier 박제. Cross-Story 재발 시 ADR 후보 "impl-discovered strengthening — design-faithful 판정 기준 (ACCEPT vs ArchitectPL escalation 분기)" 발의 예약.

### 2.5 Pattern K — cross-lane FIX 통합 (SecurityTest + CodeReview 단일 iteration)

U3 FIX iter 2 = SecurityTest (SEC-P1-1) + CodeReview (P0-1 + P1-1 + P1-2) 를 **단일 FIX iteration 으로 통합** 처리 (DeveloperPL 재구현 1회 → dual-lane re-verify). FIX budget 효율 (2-lane finding 1 iteration 소비). U2-HELPER 는 SecurityTest 0 finding (PASS) 으로 통합 미발생 → 대조군.

- **본 Story 1건 sample (N=1)**. "동일 PR 내 multi-lane blocking finding = 단일 FIX iteration 통합 처리 (budget 효율)" 패턴 carrier 박제. Cross-Story 재발 시 ADR 후보 발의 예약.

### 2.6 Pattern L — doc-only fast-path (design-review iter 1 wording drift)

U3 design-review iter 1 = Manifest status enum 9-state vs 11-state wording drift (6+ → 18 위치). `mechanical_category: minor-naming`, src/scripts 변경 0 → doc-only fast-path (chief author inline re-spawn, DeveloperPL 진단 단계 없음). U2-HELPER design-review iter 1 도 동형 doc-only fast-path (14 findings, cardinality drift).

- **누적 N=2 가능성**: design-review lane 의 wording-drift doc-only fast-path 는 이미 codeforge playbook 에 확립된 절차 (chief author inline re-spawn, FIX iteration 정상 소비). 설계 지침 부재 아님 — **non-trigger** (확립된 fast-path 가 정상 동작한 successful application sample, defect recurrence 아님). N=2 reach 하나 §D-9 "design-guidance absence" 미충족.

### 2.7 Pattern matrix 종합 (cross-Story 누적)

| Pattern | U2 carrier | U3 match | 누적 N | §D-9 defect/process-mechanism recurrence | Trigger 판정 |
|---|---|---|---|---|---|
| **H — Mechanical fast-path (FIX iter 비소비)** | §3.2 Candidate 2 carrier | pyright fast-path | **N=2** | YES (process-mechanism SSOT 부재) | **TRIGGER (adr_draft_emitted)** |
| C — debate-protocol-v1 미발동 | Pattern C carrier | Codex no_findings | N=2 | NO (trigger 부재 = 정상, defect 아님) | non-trigger |
| D — dual-track same-line P1 convergence | Pattern D carrier | SEC-P1-1 both 독립 동일 | N=2 | NO (positive review signal, defect 아님) | non-trigger |
| F — Cross-Story dependency carrier | Pattern F carrier | U5 carrier 박제 | N=2 | NO (structural carrier, defect 아님) | non-trigger |
| I — merge-during-PR rebase | (U3 trigger note) | PR #102 rebase | N=2 | NO (표준 git hygiene) | non-trigger (trivial) |
| L — doc-only fast-path wording drift | design-review iter 1 | design-review iter 1 | N=2 | NO (확립 절차 successful application) | non-trigger |
| G — PL severity reconciliation | downgrade 1건 | P0 upgrade 1건 | N=1 (mechanism, 방향 대조) | — | carrier (N=1, 재발 시 활성) |
| B — PMO Ground Truth audit gap | Pattern B carrier | (U3 무관 — impl-discovered strengthening) | N=1 (no U3 match) | — | carrier 유지 (1/2) |
| E — PROVISIONAL gate escalation | Pattern E carrier | §13.C 영향 0 (logic-only) | N=1 (no new application) | — | carrier 유지 (1/2) |
| J — impl-discovered strengthening ACCEPT | (신규) | CopyResult 4-state | N=1 | — | carrier (1/2) |
| K — cross-lane FIX 통합 | (신규) | SecurityTest+CodeReview iter 2 | N=1 | — | carrier (1/2) |

**결론**: defect/process-mechanism recurrence threshold (§D-9 N≥2) 충족 = **Pattern H 1건** (mechanical fast-path FIX iteration 비소비 — U2-HELPER §3.2 Candidate 2 carrier pre-registered, U3 pyright 으로 N=2 reach). 나머지 N=2 도달 패턴 (C/D/F/I/L) 은 positive signal / structural carrier / 확립 절차 successful application 으로 §D-9 "design-guidance absence" semantics 미충족 → non-trigger. Pattern H 만 mandatory ADR trigger emit (§3).

## 3. ADR 후보 발의 (PMO proposer only — Mandatory, threshold N=2 REACHED)

ADR-045 Amend5 §D-9 mandatory framing 충족 (PMOAgent self-decide 영역 제거 — forcing function). `cross_story_pattern_adr_trigger` field mandatory 채움 + `escalation_action: adr_draft_emitted`. Orchestrator 회부 → codeforge-design ArchitectAgent spawn → 신규 ADR Proposed status 직접 author 의무.

### 3.1 ADR candidate (Pattern H — N=2 reached, U2-HELPER §3.2 Candidate 2 carrier 활성)

```yaml
adr_candidate:
  title: "ADR-NNN Mechanical fast-path — Orchestrator direct-commit 권한 + FIX iteration 비소비 경계 + same-iteration internal verify 절차"
  category: "Process"
  trigger: >
    Cross-Story N=2 reach (CFP-665 / ADR-045 Amend5 §D-9 정량 임계값).
    U2-HELPER CodeReview iter 2 (DeveloperPL __all__ extension 새 P1 → Orchestrator
    direct commit, DeveloperPL 재spawn 없이, code-review iter 2 내부 verify, §10 row
    append 0) + U3-MIGRATE pyright type errors fast-path (moto 5.x mock_aws +
    importlib.util + spec None guard, FIX iteration 비소비, §10 row append 0).
    2 sample → mechanical_category fast-path 의 적용 조건/Orchestrator 권한/FIX
    counter 소비 경계가 CFP-19 R11 inline 언급에 머물고 정식 ADR-level 절차 SSOT 부재.
  proposer: PMOAgent
  author_pending: ArchitectAgent (chief author — codeforge-design plugin)
  status: Proposed (ArchitectAgent verdict 권한 — Accepted | Rejected 최종 결정)
  detection_channel: root_cause_class (fallback hybrid — mechanical_category fast-path)
  carrier_source: U2-HELPER retro §3.2 Candidate 2 (emit_condition pre-registered)
  references:
    - CFP-19 R11 (same-iteration internal verify, §10 FIX row append 0)
    - U2-HELPER retro §1 line 37 (Mechanical fast-path Option A 박제)
    - U3-MIGRATE Orchestrator LAND comment (#89 — fast_path: pyright FIX 비소비)
  proposed_decision_outline: |
    1. mechanical_category enum 정의 (minor-naming / type-only / symbol-export / ...)
       중 fast-path 적격 subset SSOT
    2. fast-path FIX iteration counter 비소비 조건 (logic/data-safety = 비적격,
       no behavioral change = 적격)
    3. Orchestrator direct-commit 권한 경계 (DeveloperPL 재spawn 회피 조건) +
       same-iteration internal verify 절차 (CI/pytest re-run + line-level grep audit)
    4. §10 FIX Ledger append 정책 (fast-path = row append 0, audit trail 별도 표기)
```

### 3.2 deferred carrier 유지 (N=1, 재발 시 활성)

```yaml
deferred_carriers:
  - pattern: B (PMO Story Ground Truth audit gap)
    state: 1/2 (U2 sample only, U3 무 match)
    emit_condition: "동일 PMO Ground Truth audit gap 패턴 재발 시 즉시 발의"
  - pattern: E (PROVISIONAL gate 정량 escalation)
    state: 1/2 (U2 successful application only, U3 §13.C 영향 0)
    emit_condition: "동일 PROVISIONAL gate 패턴 재발 시 발의"
  - pattern: G (PL severity reconciliation authority)
    state: 1/2 (U2 downgrade + U3 upgrade = mechanism 대조 쌍, 방향 반대로 N=1 mechanism)
    emit_condition: "동일 PL severity_override mechanism 3rd sample 시 발의"
  - pattern: J (impl-discovered design-faithful strengthening ACCEPT)
    state: 1/2 (U3 CopyResult 4-state only)
    emit_condition: "impl-discovered strengthening ACCEPT/escalation 분기 재발 시 발의"
  - pattern: K (cross-lane FIX 단일 iteration 통합)
    state: 1/2 (U3 SecurityTest+CodeReview iter 2 only)
    emit_condition: "multi-lane blocking finding 단일 iteration 통합 재발 시 발의"
```

## 4. Gate 준수 audit

| Audit 항목 | 결과 | Evidence |
|---|---|---|
| Preflight 누락 | PASS (각 lane 진입 Preflight 코멘트 trail 존재) | #89 comment trail (design-review FIX → re-verify PASS → security-test/code-review FIX → re-verify PASS → LAND) |
| §8 Test Contract ↔ 실제 테스트 매핑 | PASS | 5 test files (49 passed/2 skipped fcntl-Windows/0 failed), P0-1 `test_both_head_404_yields_failed_not_done` + P1-2 `test_invg_midstate_copied_partition_resumes` 신규 추가 |
| §8.5 Impl Manifest ↔ git diff | PASS | rekey.py + nas_uploader.py + rekey_l1_migration.py + prometheus_exporters.py + compose.yml + 5 test files, Orchestrator LAND comment 핵심 산출물 목록 git 일치 |
| FIX 원인 판정 evidence pack | PASS | P0-1 Change Plan §11.6:986-1003 decision matrix verbatim 인용 + SEC-P1-1 SecurityArch §7.6 M-2/§7.2 T-T2 인용 + pytest 로그 코멘트 포함 |
| 토큰 예산 초과 | N/A (본 retro 입력에 lane별 token telemetry 미포함 — §8 session retro 시 synthesize) | - |
| **AUDIT GAP — orphan sub-issues** | **FAIL (gate-compliance gap)** | `[U3-MIGRATE] impl: *` sub-issue **21건 OPEN 잔존** (#104-#124, decomposition tracking issues). #89 LAND (PR #102 merged) 후에도 미close — Story close 시 sub-issue cascade close 미실행 |
| **AUDIT GAP — Story #89 label drift** | **FAIL (forcing function gap)** | #89 = CLOSED 이나 label = `phase:구현` (stale, `phase:완료` 미전환) + `gate:retro-complete` 미부착 (본 retro 가 부착 — forcing function 핵심 단계) |
| Epic tracking 메커니즘 | NOTE | EPIC #86 = milestone **부재** (GitHub milestone 0건). Epic tracking = Issue #86 body. retro mandate "Epic milestone 갱신" → **Epic #86 body comment 갱신**으로 대체 (U2-HELPER retro 동일 방식 — body 갱신) |

**Audit GAP 처리**:
- orphan sub-issue 21건: gate-compliance gap 박제. Orchestrator 회부 — Story LAND 시 sub-issue cascade close 누락 패턴 (cross-Story 재발 시 ADR 후보 — 단 본 retro N=1, U2 #88 sub-issue 상태 별도 확인 권고). PMOAgent write 권한 범위 외 (sub-issue close = Orchestrator 영역) — 회부.
- #89 label drift: 본 retro step 3 에서 `gate:retro-complete` add (forcing function 핵심). `phase:구현` → `phase:완료` 전환은 Orchestrator 영역 (label drift 박제, 회부).

## 5. ESCALATE trend

| Story | Lane | ESCALATE 횟수 | FIX budget 사용 | fast-path (비소비) | design re-write |
|---|---|---|---|---|---|
| U2-HELPER | All | 0 | 2/3 | 1 (Option A) | 0 |
| **U3-MIGRATE** | All | **0** | **2/3** (design-review 1 + security/code-review 통합 1) | **1** (pyright) | **0** |
| **누적 trend** | - | **0 (2 Story baseline 유지)** | 평균 2/3 (budget 여유 1) | 2 (N=2 → Pattern H ADR trigger) | 0 |

본 Story = critical blocker 0, FIX budget 초과 0 (2/3, 1 unused), design re-write 0, ESCALATE 0. Cross-Story ESCALATE trend = **0 유지** (U2 baseline + U3 = 2 Story 연속 0). 양호. 단 P0-1 (data-loss) 가 code-review lane 에서 catch 된 점은 ESCALATE 는 아니나 **data-safety high-severity finding** 으로 §2.1 Pattern G carrier 박제 (PL P0 elevate authority 정상 동작).

## 6. SEC-P2-1 hardening backlog 박제 (SecurityTest 권고, non-blocking)

```yaml
backlog_item:
  id: SEC-P2-1
  title: "NASUploader copy/delete key-masking T-I2 consistency"
  source: "U3-MIGRATE SecurityTest dual-layer re-verify (#89 comment, residual P2)"
  severity: P2 (non-blocking)
  classification: "NOT a leak — SecurityArch Hive path = Public-internal 분류"
  detail: >
    NASUploader.copy_object / delete_object 의 log key masking 이 기존
    put / put_streaming 패턴과 inconsistent. content leak 아님 (Public-internal
    분류) 이나 log hygiene consistency hardening 권고.
  disposition: "별 hardening Story 후보 (non-blocking, U3 verdict 영향 0)"
  carrier_to: "Epic #86 maintenance backlog (30일 cool-down 종료 후 script 회수 + bucket versioning ILM rule 와 동반 처리 후보)"
  status: 박제됨 (본 retro = SSOT carrier)
```

## 7. Cross-Story dependency 박제 (U5-VERIFY #91 + operator gate)

```yaml
u3_delivery_boundary:
  delivered: "마이그레이션 도구 (PR #102 LAND sha 37d6037e)"
  NOT_delivered: "실제 4,608 객체 117GB re-key 실행 (= 운영자 트리거, 의도된 operator gate)"
  operator_trigger: >
    docker compose --profile migration run --rm rekey-migration ...
    --execute --i-understand-this-is-irreversible
  sequence: "dry-run 우선 → execute → 30일 cool-down → U5-VERIFY 검증"

to_u5_verify_story91:
  blocking_gate: "U3 delete 단계 완료 = cutover step 5 선행 게이트"
  blocking_reality: >
    U3 = 도구 인도만 완료. U5-VERIFY full LAND 은 operator 마이그레이션 실행
    (execute) + 30일 cool-down 종료 후에만 가능. U3 LAND ≠ cutover step 4 실제 완료.
  inherited_invariants:
    - "INV-7: l1/ 잔존 객체 0 — production NAS 실측 grep gate (operator 실행 후)"
    - "rekey-l1-manifest- wording grep gate (ADR-034 Amendment 5 carrier, hub#396)"
    - "Phase 1 helper 회수: _resolve_legacy_nas_key + build_legacy_nas_key + build_legacy_l1_prefix + _legacy_key_to_canonical + dual-read fallback code path"
    - "Counter mode=legacy_dual_read value=0 invariant + Prometheus alert (SecurityArch gate #3)"
    - "30일 cool-down 종료 후 script 자체 회수 + bucket versioning lifecycle ILM rule (별 maintenance Story 후보)"
    - "SEC-P2-1 hardening (§6, maintenance backlog)"

epic_86_phase2_status:
  u1_adr_87: "OPEN (label=phase:reservation) — ADR-034 Accepted (hub) 이나 Story Issue 미close. Orchestrator 회부 (ADR Accepted = U1 deliverable 완료, issue close 후보)"
  u2_helper_88: "CLOSED (PR #95 LAND sha 4aa5483a)"
  u3_migrate_89: "CLOSED (PR #102 LAND sha 37d6037e) — 단 = 도구 인도, operator 실행 미완"
  u4_xrepo_90: "CLOSED not_planned (§7.1 RESOLVED — engine=candles only, cross-repo 본체 0)"
  u5_verify_91: "OPEN (operator-gated — U3 execute + 30일 cool-down 후 진입 가능)"
  phase2_completion: "3/5 issue CLOSED (U2/U3/U4) + U1 ADR Accepted (issue open) + U5 operator-gated. Epic NOT complete — operator 마이그레이션 실행 + cool-down 잔존"
```

## 8. Learnings count

```yaml
learnings_count: 8
itemized:
  - "Pattern H (Mechanical fast-path FIX iteration 비소비) — U2+U3 N=2 reach → mandatory ADR trigger emit (forcing function chain 정상 동작 2회차 검증)"
  - "Pattern G (CodeReviewPL severity_override — Codex P1 → P0 data-loss elevate) PL adjudication authority 박제"
  - "Pattern J (impl-discovered design-faithful strengthening = ACCEPT, not escalation — CopyResult 4-state, Change Plan §11.6:1007 mandate 정합)"
  - "Pattern K (cross-lane FIX 단일 iteration 통합 — SecurityTest+CodeReview budget 효율)"
  - "Pattern L/I/C/D/F N=2 도달하나 §D-9 design-guidance absence semantics 미충족 → non-trigger (positive signal/structural/확립절차 분류 의무)"
  - "U3 = 도구 인도 ≠ 마이그레이션 실행 — operator gate 박제, U5-VERIFY full LAND = operator execute + 30일 cool-down 후 (cross-Story 의존성 명시 의무)"
  - "AUDIT GAP — orphan sub-issue 21건 + #89 label drift (phase:구현 stale, gate:retro-complete 미부착) — forcing function/gate-compliance gap 박제, Orchestrator 회부"
  - "SEC-P2-1 hardening backlog 박제 (NASUploader copy/delete key-masking T-I2 consistency, non-blocking, maintenance Story 후보)"
```

## 9. Feedback back to codeforge

```yaml
feedback_back_to_codeforge:
  - title: "Story LAND 시 sub-issue cascade close 누락 — gate-compliance gap"
    detail: >
      U3-MIGRATE #89 CLOSED (PR #102 merged) 후에도 [U3-MIGRATE] impl: * decomposition
      sub-issue 21건 (#104-#124) OPEN 잔존. Story close → 하위 tracking issue cascade
      close 절차 부재 또는 미실행. cross-Story 재발 여부 = U2 #88 sub-issue 상태 확인 권고
      (재발 시 ADR 후보). plugin-codeforge Orchestrator playbook §14 / Story close 절차
      검토 후보.
    severity: process-gap (non-blocking, 본 Story verdict 영향 0)
    classification: "carrier 박제 (N=1, U2 cross-check 후 N=2 가능성)"
  - title: "Story Issue close 시 phase label drift (phase:구현 stale → phase:완료 미전환)"
    detail: >
      #89 CLOSED 이나 label = phase:구현 (stale). gate:retro-complete = 본 retro
      step 3 부착 (forcing function 정상). phase label 전환은 Orchestrator 영역이나
      close 시 미전환 = forcing function 관측 gap.
    severity: process-gap (non-blocking)
    classification: "carrier 박제 (N=1)"
reason: >
  본 Story 범위 내 plugin-codeforge 정책/skill/agent contract 핵심 결함 0건.
  단 GitHub Issue lifecycle (sub-issue cascade close + phase label 전환) gap 2건
  관측 — process-gap carrier 박제, Orchestrator 회부.
```

## 10. 산출물 인용

- **Story Issue**: [mclayer/mctrader-data#89](https://github.com/mclayer/mctrader-data/issues/89) (Story file 부재 — Issue body 가 Story SSOT, U2-HELPER 와 달리 Story file 미생성)
- **PR (LAND)**: [mclayer/mctrader-data#102](https://github.com/mclayer/mctrader-data/pull/102) (squash-merged sha 37d6037e)
- **PR (sibling)**: [mclayer/mctrader-hub#396](https://github.com/mclayer/mctrader-hub/pull/396) (ADR-034 Amendment 5, sha 0864fb10)
- **ADR**: `mctrader-hub:docs/adr/ADR-034-nas-key-unification.md` (Accepted + Amendment 1-5)
- **Epic**: [mclayer/mctrader-data#86](https://github.com/mclayer/mctrader-data/issues/86)
- **Prior retro (cross-Story carrier source)**: `docs/retros/U2-HELPER-retro-2026-05-18.md`
- **Comments (Story #89)**:
  - DesignReview FIX iter 1: #89 comment 4472915476 (2026-05-17)
  - DesignReview PASS re-verify: #89 comment (2026-05-18, 11-state 18 위치)
  - SecurityTest+CodeReview FIX iter 2: #89 comment (2026-05-18T00:58:40Z, P0-1 data-loss)
  - dual-lane PASS re-verify: #89 comment (2026-05-18T01:18:55Z)
  - Orchestrator LAND: #89 comment (2026-05-18T01:28:30Z)

## 11. pmo_output v1.2 (CFP-665 / ADR-045 Amend5 §D-9)

```yaml
pmo_output:
  schema_version: pmo-output-v1 v1.2
  story_key: U3-MIGRATE
  retro_file: docs/retros/U3-MIGRATE-retro-2026-05-18.md
  cross_story_pattern_adr_trigger:
    triggered: true
    threshold_reached: true
    pattern_id: "Pattern H — Mechanical fast-path (FIX iteration 비소비)"
    cumulative_n: 2
    detection_channel: "root_cause_class (fallback hybrid — mechanical_category fast-path; anchor_id strict primary 미충족이나 carrier pre-registered U2-HELPER §3.2 Candidate 2)"
    escalation_action: adr_draft_emitted
    adr_proposal_ref: "§3.1 (ArchitectAgent spawn 의무 — Orchestrator 회부)"
  adr_proposal:
    title: "ADR-NNN Mechanical fast-path — Orchestrator direct-commit 권한 + FIX iteration 비소비 경계 + same-iteration internal verify 절차"
    category: "Process"
    status: Proposed
    proposer: PMOAgent
    author_pending: ArchitectAgent
  non_triggered_n2_patterns:
    - "C (debate 미발동) / D (dual-track same-line convergence) / F (dependency carrier) / I (merge-rebase) / L (doc-only fast-path) — N=2 reach 하나 §D-9 design-guidance absence semantics 미충족 (positive signal/structural/확립절차)"
  deferred_carriers: "B(1/2) / E(1/2) / G(1/2 mechanism) / J(1/2) / K(1/2)"
  escalate_count: 0
  fix_budget_used: "2/3 (1 unused)"
  fast_path_count: 1
  audit_gaps:
    - "orphan sub-issue 21건 OPEN (#104-124) — gate-compliance gap, Orchestrator 회부"
    - "#89 label drift (phase:구현 stale) — forcing function 관측 gap"
  forcing_function_status: "intact — N=2 reach → mandatory ADR trigger emitted (PMOAgent self-decide 영역 제거 준수)"
  feedback_back_to_codeforge: 2 (process-gap carrier 박제)
```

[PMOAgent retro authored — ADR-045 Amend1-5 mandate 정합 / CFP-138 D-5 4-field schema / CFP-665 D-9 cross-Story threshold N=2 REACHED → mandatory ADR trigger emit / Story file 부재로 §11.5 = Issue #89 body comment 대체]
