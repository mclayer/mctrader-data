## Project

`mctrader` 6-repo sister 의 한 부분. mctrader-hub 가 governance / Story / ADR / Epic SSOT.

## codeforge 의무 사용 (CFP-96 Phase 6b, ADR-027)

본 repo = mctrader 6-repo 의 5 sister 중 하나. CFP-111 Phase 6b adoption 시점부터 codeforge protocol 의무.

### 의존 plugin (9개)

`/plugins install` 으로 9 plugin 등록 의무 (codeforge wrapper + 6 lane + 4 dep).

### 3-trigger enforcement

1. **SessionStart** — `regen-agents.sh` (overlay merge) + `check-bootstrap.sh` (drift 검증)
2. **UserPromptSubmit** — `userprompt-reminder.sh` (변경 prompt regex 검출 → reminder)
3. **Story phase** — `phase-gate-mergeable.yml` (CFP-106 #143 fast-pass 적용) + `phase-label-invariant.yml`

### Cross-repo Story (hub MCT-N)

본 repo 의 변경도 mctrader-hub 의 MCT-N Story 로 추적 (Mode B cross-repo, ADR-020). 본 repo 별도 KEY prefix 미사용.

### Bypass

`HOTFIX_BYPASS_CODEFORGE=1` + `HOTFIX_BYPASS_REASON='<incident-id>'` 양 env 의무.

### Adoption 범위 (CFP-111)

- `.claude/settings.json` — schema-correct nested 3-level hook 등록
- `.claude/_overlay/project.yaml` — repo SSOT
- `.claude/_overlay/CLAUDE.md` — 본 파일
- `.github/workflows/` — 7 workflows (phase-gate-mergeable / phase-label-invariant / story-init / story-section-1-immutable / subissue-from-impl-manifest / fix-ledger-sync / story-section-schema)
- `.github/ISSUE_TEMPLATE/` — 3 forms (audit / bug / story)

신규 도메인 specialization agent 는 first iteration 에 hub-shared (DomainAgent / DataEngineerAgent) 만 reuse. repo-specific agent 는 후속 iteration.
