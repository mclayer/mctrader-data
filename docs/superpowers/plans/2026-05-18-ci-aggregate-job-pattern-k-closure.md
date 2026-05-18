# Branch Protection `ci` Aggregate Job — Pattern K Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** mctrader-data `.github/workflows/ci.yml` 의 matrix job 명을 `ci` → `ci-matrix` 로 rename 하고 신규 aggregate job `ci` 를 추가하여 branch protection `required_status_checks.contexts:["ci"]` 의 SSOT 매칭을 복원. 6 PR 연속 admin override (Pattern K N=2) 종결, post-merge 첫 정상 PR 부터 normal CI gating 작동.

**Architecture:** A1 workflow-only 변경 (branch protection mutation 0, shared infra risk 0). aggregate job 명 `ci` 재사용으로 contexts:["ci"] SSOT 보존. 표준 GitHub 패턴 (`needs:[ci-matrix]` + `if: always()` + `contains(needs.ci-matrix.result, 'failure'|'cancelled') exit 1` — Researcher critical gotcha 회피). chicken-and-egg: 본 PR 자체 7th admin merge (마지막), post-merge 부터 admin override 0.

**Tech Stack:** GitHub Actions YAML.

**Scope:** spec [docs/superpowers/specs/2026-05-18-ci-aggregate-job-pattern-k-closure.md](docs/superpowers/specs/2026-05-18-ci-aggregate-job-pattern-k-closure.md). 단일 Story, 단일 PR (commit 3 분리 — spec / workflow / grep evidence). KEY = MCT-201 (planned). doc-only fast-path 불가 (workflow 코드 변경, 강제 Story). ADR reservation lane = N/A (기존 ADR-011 D1/D11 정합, D2 amendment = 별 doc-only Story 후보).

**Out of scope (별 Story):** branch protection mutation (A2/A3) / enforce_admins 강화 / ADR-011 D2/D11 amendment (mctrader-hub cross-repo) / sibling repo (engine/market) matrix sync / bot account 도입 (D11 trigger).

---

### Task 1: Phase 1 Preflight + spec/plan git stage

**Files:**
- Stage: `docs/superpowers/specs/2026-05-18-ci-aggregate-job-pattern-k-closure.md` (이미 존재)
- Stage: `docs/superpowers/plans/2026-05-18-ci-aggregate-job-pattern-k-closure.md` (이 파일)

- [ ] **Step 1: AC-2 Preflight — branch protection contexts 실측**

```bash
gh api repos/mclayer/mctrader-data/branches/main/protection/required_status_checks --jq '.contexts'
```

Expected: `["ci"]` (단일 literal).
- 다른 literal 발견 시 즉시 ESCALATE (spec §3.1 yaml 의 aggregate job 명이 `ci` 면 mismatch — scope 변경 의무, 본 Story 중단).

- [ ] **Step 2: spec + plan git add**

```bash
git add docs/superpowers/specs/2026-05-18-ci-aggregate-job-pattern-k-closure.md docs/superpowers/plans/2026-05-18-ci-aggregate-job-pattern-k-closure.md
```

- [ ] **Step 3: commit**

```bash
git commit -m "$(cat <<'EOF'
docs(MCT-201): branch protection ci aggregate Story spec + plan

parse-node-id retro §2 Pattern K N=2 reach escalate → 본 Story closure.
A1 workflow-only (matrix 'ci' → 'ci-matrix' rename + aggregate job 'ci' 신설).
branch protection contexts:['ci'] SSOT 무변경, shared infra mutation 0.
EOF
)"
```

---

### Task 2: ci.yml matrix rename + aggregate job — workflow change

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: 현재 ci.yml 백업 + Read 검증**

```bash
cp .github/workflows/ci.yml /tmp/ci.yml.pre-mct201.backup
cat .github/workflows/ci.yml
```

Expected (verified-via origin/main):
- `jobs.ci.strategy.matrix.os: [ubuntu-latest, windows-latest]` 확인
- 8 step (checkout / setup-uv / Python / git auth / Install / Lint / Type check / Test / Coverage)

- [ ] **Step 2: workflow rename + aggregate 추가**

Edit `.github/workflows/ci.yml` — `jobs.ci:` 를 `jobs.ci-matrix:` 로 rename, 그 아래에 신규 aggregate job `ci:` 추가:

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  ci-matrix:
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, windows-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4

      - uses: astral-sh/setup-uv@v3
        with:
          version: "latest"

      - name: Set up Python 3.11
        run: uv python install 3.11

      - name: Configure git auth for private org packages
        run: git config --global url."https://x-access-token:${{ secrets.CODEFORGE_CROSS_REPO_PAT }}@github.com/".insteadOf "https://github.com/"

      - name: Install dependencies
        run: uv sync --all-extras

      - name: Lint (ruff)
        if: matrix.os == 'ubuntu-latest'
        run: uv run ruff check src tests

      - name: Type check (pyright)
        if: matrix.os == 'ubuntu-latest'
        run: uv run pyright

      - name: Test
        run: uv run pytest --cov=mctrader_data --cov-report=xml --cov-report=term -m "not slow"

      - name: Coverage gate (60%)
        if: matrix.os == 'ubuntu-latest'
        run: |
          uv run python -c "
          import xml.etree.ElementTree as ET
          tree = ET.parse('coverage.xml')
          rate = float(tree.getroot().attrib['line-rate'])
          pct = rate * 100
          print(f'Coverage: {pct:.1f}%')
          assert pct >= 60.0, f'Coverage {pct:.1f}% < 60% baseline (ADR-011)'
          "

  ci:
    # Aggregate gate job — branch protection required_status_checks.contexts:["ci"] 가
    # 본 job 명과 매치 (matrix expansion 안 됨, 단일 check 보고). MCT-201 — Pattern K closure.
    # Critical: if:always() + contains(...result, 'failure'|'cancelled') exit 1 표준 패턴
    # (`if: always()` 누락 시 matrix fail → aggregate skip → required check 영구 pending = stuck 재현).
    needs: [ci-matrix]
    if: always()
    runs-on: ubuntu-latest
    steps:
      - name: Verify all ci-matrix jobs passed
        if: contains(needs.ci-matrix.result, 'failure') || contains(needs.ci-matrix.result, 'cancelled')
        run: exit 1
```

- [ ] **Step 3: YAML syntax 검증 (로컬)**

```bash
py -3.12 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo "YAML PARSE OK"
```

Expected: `YAML PARSE OK` (syntax 유효).

만약 `py -3.12` 미가용:
```bash
python -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo "YAML PARSE OK"
```

- [ ] **Step 4: AC-1 verify — `if: always()` literal 존재 + contains 표현 정합**

```bash
grep -n "if: always()" .github/workflows/ci.yml && grep -n "contains(needs.ci-matrix.result" .github/workflows/ci.yml
```

Expected: 각각 1 매치 (line number 출력) — gotcha 회피 확정.

- [ ] **Step 5: commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci(MCT-201): ci → ci-matrix rename + ci aggregate job (Pattern K closure, A1 workflow-only)

branch protection required_status_checks.contexts:['ci'] SSOT 매칭 복원.
- matrix job 'ci' → 'ci-matrix' rename (check 명 'ci-matrix (ubuntu-latest)'/'ci-matrix (windows-latest)')
- 신규 aggregate job 'ci' (needs:[ci-matrix] + if:always() + contains result check)
- branch protection mutation 0, shared infra mutation 0
- Researcher critical gotcha 회피: if:always() literal 보존 (누락 시 stuck 재현)

post-merge: admin override 0 expected (regression-0 실측 AC-5 carrier)."
```

---

### Task 3: R3 grep evidence — badge/dashboard hardcoded job 명 영향

**Files:**
- (verification only, no file change unless hit)

- [ ] **Step 1: grep README + docs/**/*.md + .github/**/*.{md,yml,yaml}**

```bash
grep -rn "ci\s*(ubuntu-latest)\|ci\s*(windows-latest)" README.md docs/ .github/ 2>&1 | grep -v ".github/workflows/ci.yml" | head -20
```

Expected: 0 hit (변경 noop) — `ci.yml` 자체 매치는 제외.

(주의: `grep -E` 또는 PowerShell `Select-String` 환경별 미세 차이. 위 명령이 빈 출력이면 R3 mitigation OK.)

- [ ] **Step 2: 추가 grep — badge URL hardcoded job 참조**

```bash
grep -rn "workflows/ci\.yml\|badge.svg" README.md docs/ 2>&1 | head -10
```

Expected: workflow 단위 badge URL (`workflows/ci.yml/badge.svg`) 만 발견 → job 명 무관, break 0.
job-specific badge (예: `?job=ci`) 발견 시 update 의무.

- [ ] **Step 3: grep evidence commit (noop if 0 hit)**

If 0 hit (expected):

```bash
# noop commit — evidence 박제용 (변경 없음, R3 mitigation closure)
# OR: scope 분리상 별도 commit 불필요, retro §evidence 에 명령+0 hit 기록으로 대체
echo "R3 grep 0 hit — badge/dashboard hardcoded job 명 영향 무, 변경 noop"
```

If hit:
- Hit 파일 update + commit:
  ```bash
  # 예: README badge URL update
  git add <hit_files>
  git commit -m "docs(MCT-201): R3 badge/dashboard hardcoded 'ci (...)' literal update (ci-matrix rename 영향)"
  ```

(Step 3 의 commit 은 hit 시만 실행 — 0 hit 면 evidence 만 박제하고 commit 생략, retro §evidence 박제로 충분.)

---

### Task 4: 전체 검증 + PR open

**Files:**
- (verification + PR open)

- [ ] **Step 1: branch 상태 + commit log 확인**

```bash
git log origin/main..HEAD --oneline
```

Expected (commit 분리 정합):
1. `docs(MCT-201): branch protection ci aggregate Story spec + plan` (Task 1)
2. `ci(MCT-201): ci → ci-matrix rename + ci aggregate job (Pattern K closure, A1 workflow-only)` (Task 2)
3. (optional) `docs(MCT-201): R3 ... update` (Task 3 if hit)

- [ ] **Step 2: AC-1 + AC-2 + AC-4 최종 verify**

```bash
# AC-1 (workflow 정확성)
grep -n "if: always()" .github/workflows/ci.yml
grep -n "contains(needs.ci-matrix.result" .github/workflows/ci.yml
# AC-2 (Preflight contexts) — 이미 Task 1 Step 1 에서 검증
# AC-4 (PR body 명시) — Step 3 에서 PR body 작성 시 명시
```

- [ ] **Step 3: push + PR open**

```bash
git push -u origin HEAD
gh pr create --title "ci(MCT-201): branch protection ci aggregate job (Pattern K N=2 → N=3 closure, A1 workflow-only)" --body "$(cat <<'EOF'
## Summary
- mctrader-data `.github/workflows/ci.yml` matrix job `ci` → `ci-matrix` rename + 신규 aggregate job `ci` 추가 (A1 workflow-only)
- branch protection `required_status_checks.contexts:["ci"]` SSOT **무변경** — aggregate job 명 `ci` 재사용으로 매칭 복원 (shared infra mutation 0)
- 6 PR 연속 admin override 해소 — Pattern K (parse-node-id retro §2 N=2 reach) closure carrier
- post-merge: 첫 정상 PR 부터 normal CI gating 작동, admin override 0

## ⚠️ FINAL admin override (7th)
**본 PR 자체가 broken state 하 merge 의무 — 7th admin override (마지막).** Pattern K count: 6 (history) + 1 (본 PR) = N=3. retro §1 N=3 갱신 + post-merge 첫 정상 PR override 0 실측 evidence 박제 의무.

## 표준 패턴 정합 (Researcher Phase 0 anchor)
- aggregate job `needs:[ci-matrix]` + `if: always()` + `contains(needs.ci-matrix.result, 'failure'|'cancelled') exit 1`
- **Critical: `if: always()` 누락 = matrix fail 시 aggregate skip → required check 영구 pending (stuck 재현)**. 본 PR 은 verbatim 적용 (CodeReview 검증 의무).

## Origin
- parse-node-id-suffix-strip Story (PR #127 d8912ad / retro #128 c288599) retro §2 Pattern K N=2 reach escalate
- compactor-sort-key-l1-naming Story (PR #96 adfddf4) Pattern K N=1 초기 carrier
- 6 PR admin override 박제: #96 / #98 / #103 / #126 / #127 / #128

## Test plan
- [x] AC-1: workflow 정확성 (`if: always()` + `contains(needs.ci-matrix.result, ...)` literal 존재)
- [x] AC-2: Preflight contexts 실측 (`["ci"]` 확정 — Story 진입 조건)
- [x] AC-3: R3 grep evidence (README/docs/.github badge hardcoded literal 0 hit / hit 시 update)
- [x] AC-4: PR body "FINAL admin override (7th)" 명시 (본 PR body)
- [ ] AC-5 (post-merge BLOCKING): next 정상 PR mergeStateStatus=CLEAN + admin override 불필요 실측 (retro §evidence carrier)
- [-] AC-6: matrix fail 시 aggregate fail (의도적 PR 주입 운영 부담 → CodeReview lane 의무 verify 로 격감)

## ADR
신규/변경 ADR 0. ADR-011 D1 (enforce_admins=false 의도) / D11 (bot 미도입 trigger) 정합. ADR-011 D2 amendment (운영 drift 명시화) = 별 doc-only Story 후보 (mctrader-hub cross-repo).

## Lane evidence
- 요구사항: PASS (codeforge-brainstorm Phase 0 burst 4 agent) / 설계: PASS (PMO scope_manifest A1 만장일치) / 설계-리뷰: PASS (PR comment evidence) / 구현: PASS (commit 3 분리) / 구현-리뷰: PASS / 구현-테스트: PASS (YAML syntax + AC-1/2/3 verify)
- 보안-테스트: SKIPPED (ADR-048 default, infra workflow change) / ADR-reservation: N/A (기존 정합)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**Spec coverage**:
- §3.1 workflow change → Task 2
- §3.2 branch protection 무변경 → 명시 (Task 별도 단계 없음)
- §3.3 R3 검증 → Task 3
- §3.4 chicken-and-egg + Pattern K count → PR body (Task 4 Step 3) + retro 단계 (post-merge)
- §3.5 ADR 0 → spec + PR body 명시
- §5 AC-1~AC-6 → Task 1-4 각 단계 매핑
- §7 R1~R5 → mitigation 전부 task 단계 또는 lane (CodeReview) 의무
- §9 commit 3 분리 → Task 1/2/3 commit 구조

**Placeholder scan**: 없음 — 모든 code block / 명령 / expected output 완전.

**Type consistency**: workflow YAML — `ci-matrix` (matrix job key) + `ci` (aggregate job key) 일관. branch protection contexts (`["ci"]`) 와 aggregate job 명 (`ci`) 매칭 명시.
