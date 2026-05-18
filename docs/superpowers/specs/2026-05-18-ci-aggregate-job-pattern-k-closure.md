---
spec: ci-aggregate-job-pattern-k-closure
date: 2026-05-18
origin: parse-node-id-suffix-strip Story retro §2 Pattern K N=2 reach escalate (mctrader-data infra governance, codeforge §D-9 비대상 confirmed) → 본 Story
status: brainstorm-complete → writing-plans 대기
stories: 1 (단일 Story — .github/workflows/ci.yml 1 production file, 단일 PR atomic)
key: MCT-201 (planned, Issue 시점 확정 — gh issue list next 가용 verified by PMO; 단일 세션 internal Story 경우 formal Issue 미할당)
pre_lookup_evidence:
  - "branch protection main: required_status_checks.contexts:['ci'], strict:true, enforce_admins:false — verified-via: gh api repos/mclayer/mctrader-data/branches/main/protection/required_status_checks"
  - ".github/workflows/ci.yml jobs.ci + matrix os:[ubuntu-latest,windows-latest] → check 명 = 'ci (ubuntu-latest)' / 'ci (windows-latest)' (matrix dimension append) — verified-via: Read .github/workflows/ci.yml + 본 세션 6 PR CI checks 실측"
  - "branch protection 의 'ci' context 영구 미해결 (bare ci job 미생성, matrix expansion 만 보고) → 모든 PR mergeStateStatus:BLOCKED → enforce_admins:false admin override 강제 — verified-via: gh pr view 96/98/103/126/127/128 본 세션"
  - "6 PR admin override 박제: #96 adfddf4 / #98 06926e3 / #103 6b4afae / #126 a215e07 / #127 d8912ad / #128 c288599 — verified-via: gh pr view + git log origin/main"
  - "ADR-011 (mctrader-hub) D1 enforce_admins=false = intentional escape hatch / D2 명목 5 required checks 실제 4/6 repo 단일 ci aggregate 운영 drift (amendment 0) / D11 bot 미도입 trigger 'CI gate 빈자리 보완' 전제 — verified-via: DomainAgent Phase 0 Read mctrader-hub/docs/adr/ADR-011-branch-protection-ci.md"
  - "sibling engine/market repo = matrix 미사용 → contexts:['ci'] drift 무 (미래 matrix 도입 시 동일 trap) — verified-via: DomainAgent Phase 0 cat .github/workflows/ci.yml engine/market"
  - "GitHub 표준 패턴 (aggregate job needs:[matrix] + if:always() + contains(needs.*.result,'failure'|'cancelled') exit 1) — verified-via: ResearcherAgent Phase 0 (GitHub community discussion #26822, devopsdirective 2025-08)"
  - "Critical gotcha: aggregate `if: always()` 누락 시 matrix fail → aggregate skip → required check 영구 pending (현 stuck 메커니즘 재현) — verified-via: ResearcherAgent Phase 0"
  - "7-day visibility window: required check 후보 = 최근 7일 실행 context (settings UI 검색). 본 fix 의 aggregate 'ci' job 명 = 기존 contexts literal 재사용이라 UI 신규 등록 무관 — verified-via: ResearcherAgent Phase 0 docs.github.com/troubleshooting"
  - "최근 main: 4ad0171 #101 MCT-200 / c288599 #128 retro / d8912ad #127 parse_node_id / a215e07 #126 chore — verified-via: git log origin/main"
  - "open phase:설계 epic = 없음 — verified-via: PMO Phase 0"
  - "next MCT KEY: MCT-201 (MCT-200 직후) — verified-via: PMO Phase 0 gh issue list"
---

# branch protection ci context aggregate job — 설계 (brainstorm 산출, Pattern K closure)

## §1 동기 (WHY — Analyst 추출, 4-agent 만장일치)

**A 운영마찰 제거 (primary) + B governance 복원 + Pattern K threshold closure** 삼중 동기.

mctrader-data main branch protection 의 `required_status_checks.contexts:["ci"]` 가 실제 GitHub Actions matrix job 명 (`ci (ubuntu-latest)` / `ci (windows-latest)`, matrix dimension append 후 bare `ci` 미생성) 과 SSOT drift. 결과: 모든 PR `mergeStateStatus:BLOCKED` 영구, `enforce_admins:false` (ADR-011 D1 intentional escape hatch) 활용 admin override 강제 — 6 PR 연속 (#96/98/103/126/127/128).

- **A 운영마찰**: admin merge 의존이 LAND 절차 차단 (admin 부재 시 perma-block, 6 PR 누적 마찰 임계).
- **B governance 복원**: branch protection 사실상 비기능 (admin 우회만 작동) = false sense of safety. CI 게이팅 실효성 회복 — ADR-011 D11 "bot 미도입 trigger = CI gate 빈자리 보완" 전제 회복.
- **Pattern K closure**: parse-node-id retro §2 Pattern K N=2 reach 후 escalate carrier → 본 Story 가 closure (Pattern H 패턴 정합 — out-of-scope→follow-up→Story closure 1 사이클).

**불일치 해소**: A3 (branch protection contexts 만 변경) 후보가 표면상 minimal 이나 workflow 무변경으로 `contexts:["ci"]` 매칭 영구 불가 — Analyst 부적절 판정 (확정).

## §2 근본 원인 (사실 검증 완료)

| RC | 내용 | 증거 / 검증 |
|----|------|------|
| RC-1 | branch protection `contexts:["ci"]` 영구 미해결 — matrix expansion 으로 `ci` literal check 미생성, `ci (<dim>)` 만 보고 | gh api branch protection + Actions check name 규약 |
| RC-2 | mctrader-data 단독 drift — sibling engine/market 는 matrix 미사용 → 동일 contexts:["ci"] 정상 작동. 미래 matrix 도입 시 동일 trap | DomainAgent Phase 0 |
| RC-3 | ADR-011 D2 "5 required checks" 명목 표 ↔ 4/6 repo 단일 ci aggregate 운영 drift (amendment 0) — 본 Story 가 mctrader-data 측 정상화 (ADR amendment = 별 doc-only Story 후보) | DomainAgent Phase 0 |
| 안전 | `enforce_admins:false` = ADR-011 D1 intentional (governance violation 아님) — 본 Story 변경 OUT | DomainAgent Phase 0 |

**Researcher 표준 패턴 (4-agent anchor)**: aggregate job `needs:[matrix]` + `if: always()` + `contains(needs.*.result, 'failure'|'cancelled') exit 1`. **Critical gotcha**: `if: always()` 누락 = matrix fail → aggregate skip → required check 영구 pending (현 stuck 메커니즘 재현).

**PMO 핵심 통찰**: aggregate job 명 = **`ci` 재사용** → branch protection `contexts:["ci"]` SSOT **무변경** (shared infra mutation 0). 기존 matrix job 을 `ci-matrix` rename + 신규 aggregate `ci` 가 contexts 매칭.

## §3 설계 (확정 — derived default, 4 agent 만장일치 A1 anchor)

### §3.1 `.github/workflows/ci.yml` 변경

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  ci-matrix:            # ← rename: 기존 'ci' → 'ci-matrix' (matrix expansion: 'ci-matrix (ubuntu-latest)' / 'ci-matrix (windows-latest)')
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, windows-latest]
    runs-on: ${{ matrix.os }}
    steps:
      # ... 기존 steps 전부 동일 (checkout / setup-uv / Python / git auth / Install / Lint / Type check / Test / Coverage) ...

  ci:                   # ← 신규 aggregate, branch protection contexts:["ci"] 매칭 SSOT 보존
    needs: [ci-matrix]
    if: always()
    runs-on: ubuntu-latest
    steps:
      - name: Verify all ci-matrix jobs passed
        if: contains(needs.ci-matrix.result, 'failure') || contains(needs.ci-matrix.result, 'cancelled')
        run: exit 1
```

### §3.2 branch protection — 무변경

- `required_status_checks.contexts:["ci"]` **그대로** (SSOT 보존, gh api PATCH 0).
- `enforce_admins:false` **그대로** (ADR-011 D1 intentional escape hatch).
- `strict:true` **그대로**.
- shared infra mutation 0 → 사용자 명시 authorization 별 단계 불요 → 본 Story self-contained reviewable PR.

### §3.3 검증 패턴 (R3 mitigation)

- README.md + docs/**/*.md + .github/**/*.{md,yml,yaml} 에서 `ci (ubuntu-latest)` / `ci/ci` literal grep — hit 시 update scope 확장, 0 hit 시 noop evidence 박제.
- badge URL = workflow 단위 (`workflows/ci.yml/badge.svg`) 면 job 명 무관 — break 0. job-specific badge 면 update.

### §3.4 chicken-and-egg + Pattern K count (R1, H mitigation)

- 본 Story PR 자체가 broken state 하 merge 의무 → **7th admin override** (마지막 1회).
- PR body 명시: "FINAL admin override, Pattern K closure carrier".
- retro §1 = Pattern K count N=2 → **N=3** 갱신 (본 PR 자체 = 7th override sample).
- post-merge 첫 정상 PR = override 0 입증 (regression-0 실측 AC).
- codeforge §D-9 semantics gate 재평가: parse-node-id retro §2 의 "infra governance 비대상" 판정 confirm 또는 정정.

### §3.5 ADR 영향 = 0 (본 Story 한정)

- 본 Story 자체 신규/변경 ADR 0.
- **별 doc-only Story 후보** (mctrader-hub cross-repo PR, follow-up):
  - ADR-011 D2 amendment: "4/6 repo 단일 `ci` aggregate 운영 drift 명시화" (현 명목 표 5-check vs 실제 single ci aggregate).
  - ADR-011 D11 amendment: bot account 도입 trigger 재평가 (본 fix 가 D11 mitigation 전제 회복하므로 trigger 미충족 — 단 명시 박제 가치).

## §4 범위 경계

### IN
- `.github/workflows/ci.yml` matrix rename + aggregate job
- README/docs grep — badge/dashboard hardcoded 영향 점검 (변경 없으면 noop)
- 본 spec §cross-ref — parse-node-id retro §2 Pattern K closure forward-reference
- retro §1 Pattern K count N=3 갱신 + closure marker

### OUT (별 Story 후보)
- branch protection mutation (A2/A3) — shared infra/permissions, 사용자 명시 authorization 의무
- `enforce_admins=true` 강화 (ADR-011 D1 의도 변경, 별 governance Story)
- ADR-011 D2/D11 amendment (mctrader-hub cross-repo PR, doc-only fast-path 가능)
- sibling repo (engine/market) matrix 도입 시 동일 fix 선행 — 별 sibling sync Story
- bot account 도입 (ADR-011 D11 trigger 별 도달 시)

## §5 Acceptance Criteria

- **AC-1 (workflow 변경 정확성)**: Given ci.yml, When matrix job 명 `ci-matrix` + aggregate `ci` (needs+if:always+result check), Then `if: always()` literal 존재 + contains(needs.ci-matrix.result, 'failure'|'cancelled') 표현 정합.
- **AC-2 (Phase 1 Preflight)**: Given branch protection 현 상태, When `gh api .../protection/required_status_checks --jq .contexts` 실측, Then `["ci"]` 확인 (다른 literal 발견 시 ESCALATE).
- **AC-3 (R3 grep evidence)**: Given README + docs + .github, When `ci\s*\(ubuntu-latest\)|ci/ci` grep, Then 0 hit (변경 noop) OR hit 시 본 PR scope 에 update 포함.
- **AC-4 (chicken-and-egg 명시)**: Given 본 PR body, When 작성, Then "FINAL admin override (7th), Pattern K closure carrier" 명시.
- **AC-5 (BLOCKING regression-0 실측)**: Given 본 Story LAND 후, When **next 정상 PR** (본 Story 후 첫 PR) 생성 + CI 통과, Then mergeStateStatus = CLEAN (admin override 불필요) + 정상 squash merge 가능 — retro evidence 박제 (가능하다면 본 retro 작성 전 첫 정상 PR 실측, 미가용 시 다음 normal Story PR 의 실측 기다리며 retro 박제 reserve).
- **AC-6 (matrix fail 시 aggregate fail 검증)**: Given 의도적 matrix slot 1개 fail 주입 (예: temp PR with broken test), When CI 실행, Then aggregate `ci` job 결과 = failure (skip 아님) — `if: always()` 정합 입증.

(AC-6 는 의도적 PR 주입이 운영 부담 → unit verify 로 대체 가능: CodeReview lane 가 yaml syntax 검증 + Researcher gotcha 문서 1줄 점검으로 격감.)

## §6 Edge cases

1. **matrix 추가/제거 시 aggregate 자동 추적**: aggregate `needs:[ci-matrix]` 가 matrix slot 명시 아닌 job-level 의존이라 matrix item 추가/제거 시 자동 추적 (각 slot result 가 `ci-matrix.result` 에 집계).
2. **badge URL hardcoded `ci` job 명**: README badge 가 `workflows/ci.yml/badge.svg` 면 job 명 무관, 정상. job-specific badge 면 update (Phase 1 grep evidence).
3. **신규 `ci` aggregate job 의 7-day visibility window**: branch protection contexts:["ci"] 이미 등록 상태 (현재 stuck 의 그것) → 신규 ci aggregate job 명 = 동일 `ci` → settings UI 신규 등록 무관, register 즉시 매칭. visibility window 우려 무.
4. **post-merge ongoing PR (#129 등 진행 중) 영향**: 본 PR merge 후 ongoing PR 가 rebase/sync 하면 신규 `ci` aggregate job 가 자동 적용 (workflow file path 동일).

## §7 위험 평가

| ID | 등급 | 내용 | Mitigation |
|----|------|------|-----------|
| R1 | MED | chicken-and-egg: 본 PR 자체 7th admin override (Pattern K count N=2 → N=3) | PR body 명시 "FINAL override" + retro N=3 갱신 + post-merge 첫 정상 PR override 0 실측 evidence |
| R2 | HIGH | `if: always()` 누락 → matrix fail 시 aggregate skip → 현 stuck 메커니즘 재현 (R2 가 정확히 본 stuck 의 원인 — re-introduce 시 even worse stuck) | spec §3.1 yaml verbatim + CodeReview lane 의무 verify (`if: always()` literal 존재) + AC-1 정합 |
| R3 | LOW | badge / external dashboard literal break (rename 시) | AC-3 grep evidence 의무 |
| R4 | LOW | sibling repo (engine/market) matrix 도입 시 동일 trap 재발 (Pattern K N=4 잠재) | retro §6 follow-up Story 후보 명시 (sibling sync Story) |
| R5 | LOW | branch protection contexts 가 실제로 `["ci"]` 단일 literal 인지 verify 부재 시 mismatch | AC-2 Phase 1 Preflight 의무 (`gh api PATH --jq .contexts` 실측, 다른 literal 발견 시 ESCALATE) |

## §8 의존

- parse-node-id-suffix-strip Story (PR #127 d8912ad / retro #128 c288599) — Pattern K escalate carrier (cross-ref only, code dep 0)
- compactor-sort-key-l1-naming Story (PR #96 adfddf4 / retro #103 6b4afae) — Pattern K 초기 N=1 carrier
- open phase:설계 epic = 없음
- ongoing PR (#129 등) 미파악 — 본 Story merge 시 자동 적용 (workflow 동일 경로)
- 신규/변경 ADR 0 (mctrader-hub ADR-011 amendment = 별 follow-up Story)

## §9 PR 분할 (단일 PR, commit 3 분리)

- 단일 PR (scope 극소, workflow 1 file + Story spec + grep evidence atomic).
- commit 분리 (PMO 권장 — revert granularity + CodeReview 격리):
  1. `docs(MCT-201): branch protection ci aggregate Story spec` (Phase 1 — spec + plan)
  2. `ci(MCT-201): ci → ci-matrix rename + ci aggregate job (Pattern K closure, A1 workflow-only)` (Phase 2 core — workflow 1 file)
  3. `docs(MCT-201): badge/docs grep evidence (R3 mitigation, 0 hit noop)` (Phase 2 evidence)

## §10 brainstorm 컨텍스트 패킷 (Phase 0 burst 산출)

- **DomainAgent**: ADR-011 D1/D2/D11 박제 — D1 enforce_admins=false intentional, D2 명목/실제 drift 4/6 repo, D11 bot 미도입 trigger "CI gate 빈자리 보완" 전제. 본 결함 = mctrader-data 단독 drift (sibling engine/market matrix 미사용). 지식 공백: ADR-011 D2 amendment 의무 미박제 (별 doc-only Story 후보).
- **ResearcherAgent**: 표준 패턴 anchor — aggregate `needs+if:always()+result check`. Critical gotcha = `if: always()` 누락 시 동일 stuck 재현. 7-day visibility window (본 fix 의 aggregate 명 `ci` 재사용으로 무관). U3 숨은 이점: 기존 leaf check `ci (ubuntu-latest)` 는 protection 에 미등록 (rename 후 무영향). **A1 권고**.
- **Analyst**: WHY = A 운영마찰(primary) + B governance + C ADR-024 정합. A3 부적절 (workflow 미변경 contexts 매칭 불가). 확장 요구 5건, AC 6건. Edge: badge/`if:always()` 누락/contexts 순서.
- **PMO**: 단일 Story 단일 PR commit 3 분리. KEY=MCT-201. R1 chicken-and-egg + R2 if:always() + R3 badge + R4 sibling + R5 contexts literal. branch_protection_mutation:false, pattern_k_closure:true.

## §11 scope_manifest (writing-plans 이관)

```yaml
planned_adrs: []
planned_files:
  - path: .github/workflows/ci.yml
    change: matrix job 'ci' → 'ci-matrix' rename + aggregate job 'ci' 신설 (needs:[ci-matrix] + if:always() + contains result check)
  - path: docs/superpowers/specs/2026-05-18-ci-aggregate-job-pattern-k-closure.md
    change: 본 spec (신규)
planned_claude_md_sections: []
planned_grep_evidence:
  - target: README.md + docs/**/*.md + .github/**/*.{md,yml,yaml}
    pattern: "ci\\s*\\(ubuntu-latest\\)|workflow.*ci\\.yml"
    purpose: badge/dashboard hardcoded matrix job 명 grep (R3 mitigation)
branch_protection_mutation: false
pattern_k_closure: true   # post-merge retro Pattern K N=3 갱신 + codeforge §D-9 semantics gate 재평가
admin_override_count_at_merge: 7   # 본 PR 자체 7th, post-merge 0 expected
```

## §12 cross-ref

- `docs/retros/parse-node-id-suffix-strip-retro-2026-05-18.md` §2 — Pattern K N=2 reach escalate source (본 Story closure carrier)
- `docs/retros/compactor-sort-key-l1-naming-retro-2026-05-18.md` — Pattern K N=1 초기 carrier
- mctrader-hub `docs/adr/ADR-011-branch-protection-ci.md` — D1/D2/D11 SSOT (본 Story 가 D2/D11 amendment 별 doc-only Story 후보 escalate)
- mctrader-hub `docs/adr/ADR-024-story-scoped-branch-policy.md` — branch governance (codeforge plugin 측, consumer overlay applicability)
- 6 PR admin override 박제: #96 adfddf4 / #98 06926e3 / #103 6b4afae / #126 a215e07 / #127 d8912ad / #128 c288599
- GitHub 표준 패턴 reference: github.com/orgs/community/discussions/26822 (Researcher Phase 0)
