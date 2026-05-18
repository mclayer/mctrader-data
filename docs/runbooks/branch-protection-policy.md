# Branch Protection Policy — mctrader-data main

## 현재 정책 (post-MCT-201 closure + require_last_push_approval=false, 2026-05-18)

```yaml
branch: main
required_status_checks:
  strict: true                      # PR base must be up-to-date with main HEAD
  contexts: ["ci"]                  # MCT-201 aggregate job (matches GitHub Actions ci job name)
required_pull_request_reviews:
  required_approving_review_count: 0
  require_last_push_approval: false # ← 2026-05-18 PATCH (single-user dev 정합, audit trail 본 runbook)
  dismiss_stale_reviews: true       # any review dismissed on new push
  require_code_owner_reviews: false
enforce_admins: false               # admin override 허용 (ADR-011 D1 intentional escape hatch)
required_signatures: enabled: false
required_linear_history: true       # squash/rebase merge only
required_conversation_resolution: true
restrictions: null                  # no user/team merge restriction
allow_force_pushes: false
allow_deletions: false
```

## 변경 이력

### 2026-05-18 — `require_last_push_approval: true → false`

**WHY**: MCT-201 PR #129 LAND 으로 Pattern K (CI gating SSOT drift) 해소 후, single-user dev (`mccho`) 환경에서 모든 PR 이 `mergeStateStatus:BLOCKED` 잔존 → admin override 영구 의존 발견.

**근본 원인 진단**:
- `required_approving_review_count: 0` + `require_last_push_approval: true` 모순 조합 (review 0 required 인데 last push 외 사용자 approval 의무)
- GitHub 가 `require_last_push_approval` 을 strict 해석 → mergeable_state: blocked 영구
- single-user dev 환경에서는 코드-머저 분리 자체가 부재 → 정책 적용 의미 0

**의사 결정**: Option A (Pattern K validation PR thread, 2026-05-18) — `require_last_push_approval=false` 로 단순화.

**적용 명령**:
```bash
gh api repos/mclayer/mctrader-data/branches/main/protection/required_pull_request_reviews \
  -X PATCH -F require_last_push_approval=false
```

**검증**: `CI ALL PASS` + `0 reviews` PR 가 admin override 없이 non-admin merge 성공 (본 runbook PR 자체가 final validation carrier).

**복원**: multi-user 도입 시 (ADR-011 D11 bot account trigger), 정책 재평가 후 `gh api PATCH ... -F require_last_push_approval=true` 로 복원.

### 2026-05-18 — 6-repo rollout 완료 (ADR-011 D1 amendment 정합 적용)

ADR-011 D1 amendment (mctrader-hub PR #401 `f220931`) 가 `require_last_push_approval=false` 를 **6-repo 공통 solo-dev default** 로 박제 → sibling repo 전수 정합 적용:

| repo | rollout 전 | rollout 후 | 방법 |
|---|---|---|---|
| mctrader-data | false (#174) | false | 본 Story carrier |
| mctrader-hub | false | false | 기적용 (no-op) |
| mctrader-engine | true | **false** | `gh api PATCH` 2026-05-18 |
| mctrader-market | true | **false** | `gh api PATCH` 2026-05-18 |
| mctrader-market-bithumb | true | **false** | `gh api PATCH` 2026-05-18 |
| mctrader-market-upbit | N/A (branch protection 없음 — PRIVATE solo) | N/A | 무관 |

일괄 적용 명령:
```bash
for repo in mctrader-engine mctrader-market mctrader-market-bithumb; do
  gh api repos/mclayer/$repo/branches/main/protection/required_pull_request_reviews \
    -X PATCH -F require_last_push_approval=false
done
```

**검증**: 6-repo 전수 `require_last_push_approval=false` 재확인 완료 (2026-05-18). ADR-011 D1 amendment 와 실제 운영 상태 정합 (drift 0).

### 이전 정책 변경 (참조)

- **2026-05-18 (이전)** — MCT-201 PR #129 LAND (`b9499a4`): branch protection contexts 무변경, `.github/workflows/ci.yml` 의 `ci` job 을 aggregate job 으로 재구성하여 SSOT 매칭 복원. shared infra mutation 0 (workflow-only fix). 7 PR 연속 admin override (Pattern K) 종결 carrier.

## 운영 가이드

### Admin override 사용 기준 (`enforce_admins: false` 활용)

ADR-011 D1 정합 — admin override 는 governance violation 아닌 intentional escape hatch. 다음 케이스에서만 사용:

| 케이스 | admin override 정당성 |
|---|---|
| chicken-and-egg infra self-fix (branch protection / CI workflow 자가치유 PR) | ✅ 본질 — fix LAND 까지는 broken state |
| pre-existing tech debt 가 막는 unrelated PR (scope 무관 cascade exposure) | ✅ scope discipline (separate Story 분리 비용 > override 비용) |
| upstream infra issue (sibling repo PAT/access broken) | ✅ scope 무관 |
| 일반 feature PR + CI all pass + 0 reviews | ❌ **non-admin merge 사용** (post-2026-05-18 정상 경로) |
| CI failure | ❌ **fix first** (admin override 금지 — CI gating bypass) |

### Branch protection 변경 절차

1. 진단: 결함 명세 (이 runbook 또는 별 Story spec)
2. 사용자 명시 confirm (shared infra mutation, AskUserQuestion)
3. PATCH 실행 + audit trail (본 runbook 변경 이력에 추가)
4. 검증 PR: 정책 변경의 효과 실측 (next PR 동작 관찰)

### Cross-repo PAT (`CODEFORGE_CROSS_REPO_PAT`) scope 관리

mctrader-data 가 의존하는 sibling private repos:
- `mctrader-market-upbit` (PRIVATE) — PAT scope 필수
- `mctrader-market` (PUBLIC) — PAT 불요
- `mctrader-market-bithumb` (PUBLIC) — PAT 불요

**PAT 발급 가이드**:
- Fine-grained PAT 권장 (per-repo allow-list, principle of least privilege)
- Repository access: `mclayer/mctrader-market-upbit` (+ 향후 신규 PRIVATE dep)
- Permissions: Contents (Read) only
- 갱신 trigger: PAT 만료, private dep 추가/이동, scope 정책 변경

**PAT 갱신 절차**:
```bash
# 새 PAT value 환경변수로 설정 후:
echo "<NEW_PAT>" | gh secret set CODEFORGE_CROSS_REPO_PAT --repo mclayer/mctrader-data
# CI rerun으로 검증
gh run rerun <run_id> --failed
```

## ADR-011 Amendment 후보 (mctrader-hub cross-repo)

본 runbook 정책 변경들을 mctrader-hub `docs/adr/ADR-011-branch-protection-ci.md` 의 amendment 로 박제 필요 (별 doc-only Story 후보):

- **Amendment N** — `require_last_push_approval` 정책: single-user dev 환경에서 false 권장 (또는 multi-user 도입 시 trigger 박제)
- **Amendment N+1** — cross-repo PAT scope policy: fine-grained per-repo allow-list 의무, private dep 추가 시 scope 갱신 trigger
- **Amendment N+2** — `ci` aggregate job 운영 drift 명시화 (MCT-201 fix 정합, 4/6 repo 패턴 SSOT)

## Cross-ref

- [MCT-201 spec](../superpowers/specs/2026-05-18-ci-aggregate-job-pattern-k-closure.md) — Pattern K closure
- [MCT-201 retro](../retros/ci-aggregate-job-pattern-k-closure-retro-2026-05-18.md) — Pattern N (chicken-and-egg infra self-fix) carrier
- mctrader-hub `docs/adr/ADR-011-branch-protection-ci.md` — D1 (admin override 의도), D2 (required checks SSOT), D11 (bot 미도입 trigger)
- 6+ PR admin override 박제: #96 / #98 / #103 / #126 / #127 / #128 / #129 / #170 / #172 / #173 (cascade)
