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

codeforge 5종 최신 버전 (MCT-127/MCT-128 + 2026-05-11 업그레이드 반영):

```
codeforge@mclayer               # 5.10.0 — ADR-014 Amendment 2: deputy mandate 소유권 annotation 갱신
codeforge-design@mclayer        # 0.6.0 — ArchitectAgent Phase 3.5 self-lint 추가; §8.5_active spawn param; LiveOps/LiveOrdering 경계 (ADR-014 Amendment 2)
codeforge-develop@mclayer       # 0.5.0 — maintenance scripts 추가; consumer-breaking 없음
codeforge-test@mclayer          # 1.1.1 (REVIVED — ADR-055/CFP-367 + Amendment 2/CFP-371) — test-verdict-v2.1 (Epic-level); IntegrationTestAgent(Sonnet) active; TestAgent/StatefulTestAgent deprecated (spawn 불가)
codeforge-review@mclayer        # 1.2.1 — review-verdict v4 canonical in plugin (CFP-137 sibling sync); v3 Archived; 4-step Orchestrator algorithm
```

### Story workflow phase (MCT-129, 2026-05-11)

요구사항 → 설계 → 설계-리뷰 → 구현 → 구현-리뷰 → CI 테스트 (ADR-048) → **통합테스트 (IntegrationTestAgent, ADR-055, §8.6, test-verdict-v2.1, Epic-level CFP-371)** → 보안-테스트 → 완료 → PMO 회고 (의무)

### Agent model tier (ADR-042 Amendment 2, 2026-05-11)

InfraEngineerAgent·QADeveloperAgent·DataEngineerAgent = `claude-haiku-4-5` (기계적 패턴 실행 카테고리).
나머지 모든 agent = Sonnet 이상.

### Plugin 업그레이드 체크리스트

`mctrader-hub/.claude/_overlay/CLAUDE.md` §"codeforge 업그레이드 프로세스" (step 1~6) 참조.
