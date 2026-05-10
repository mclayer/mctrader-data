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

### plugin 버전 메모 (MCT-129, 2026-05-11)

codeforge 4종 최신 버전 (MCT-127/MCT-128 반영):

```
codeforge-design@mclayer        # 0.5.0 — CFP-319: ArchitectAgent WS stream push_interval 실증 의무
codeforge-develop@mclayer       # 0.4.0 — presets/docker-compose.test.yml (IntegrationTestAgent §8.6)
codeforge-test@mclayer          # REVIVED (ADR-055/CFP-367) — IntegrationTestAgent(Sonnet) active; TestAgent/StatefulTestAgent deprecated (spawn 불가); test-verdict-v2
codeforge-review@mclayer        # 1.2.0 — CFP-318: 3 ReviewPL bootstrap-labels.sh preflight; review-verdict v4
```

### Story workflow phase (MCT-129, 2026-05-11)

요구사항 → 설계 → 설계-리뷰 → 구현 → 구현-리뷰 → CI 테스트 (ADR-048) → **통합테스트 (IntegrationTestAgent, ADR-055, §8.6, test-verdict-v2)** → 보안-테스트 → 완료 → PMO 회고 (의무)

### Agent model tier (ADR-042 Amendment 2, 2026-05-11)

InfraEngineerAgent·QADeveloperAgent·DataEngineerAgent = `claude-haiku-4-5` (기계적 패턴 실행 카테고리).
나머지 모든 agent = Sonnet 이상.

### Plugin 업그레이드 체크리스트

`mctrader-hub/.claude/_overlay/CLAUDE.md` §"codeforge 업그레이드 프로세스" (step 1~6) 참조.
