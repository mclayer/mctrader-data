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

### plugin 버전 메모 (2026-05-11 업그레이드 반영)

codeforge plugin 최신 버전 (hub `mctrader-hub/.claude/_overlay/CLAUDE.md` mirror — 자세한 carrier 링크는 hub 참조):

```
codeforge@mclayer               # 5.14.0 — CFP-411 Requirements lane multi-round debate / CFP-389 evidence-enforceable framework + hotfix-bypass:* label / CFP-391 debate-protocol-v1 + auto_on_divergence / CFP-387 ADR-058 is_transitional frontmatter
codeforge-requirements@mclayer  # 0.5.0 — codex-proactive-check worker (dispatch_mode: auto_on_divergence) + semantic divergence 3 criteria
codeforge-design@mclayer        # 0.7.0 — templates/adr.md is_transitional frontmatter + ## 해소 기준 섹션 schema
codeforge-develop@mclayer       # 0.5.0 — maintenance scripts; consumer-breaking 없음
codeforge-test@mclayer          # 1.1.1 (REVIVED) — test-verdict-v2.1 (Epic-level); IntegrationTestAgent active; TestAgent/StatefulTestAgent deprecated
codeforge-review@mclayer        # 1.3.0 — review-pl-base §3.0~§3.3 debate-protocol-v1 dispatch SOP + review-verdict v4.1 (findings[].anchor_id)
codeforge-pmo@mclayer           # 0.1.0
```

### Adversarial Debate auto-trigger (CFP-391/411)

DesignReview / Requirements lane 진입 시 divergence 감지되면 자동 multi-round debate (min 3 / max 5). divergence 미검출 시 기존 single-shot 유지 (backward-compat). Anchor 재발 시 즉시 사용자 escalation. 자세한 sequence 는 hub CLAUDE.md 참조.

### 도메인 ADR 작성 schema (CFP-387/ADR-058)

본 repo 또는 hub 의 `docs/adr/` 신규 작성 시 frontmatter `is_transitional` + body `## 해소 기준` 섹션 의무 (미선언 default = `true`). 측정성 3-tuple (metric / who / how) 정량 명시 의무, 모달 어휘 금지. 자세한 schema 는 hub CLAUDE.md 참조.

### Story workflow phase (MCT-129, 2026-05-11)

요구사항 → 설계 → 설계-리뷰 → 구현 → 구현-리뷰 → CI 테스트 (ADR-048) → **통합테스트 (IntegrationTestAgent, ADR-055, §8.6, test-verdict-v2.1, Epic-level CFP-371)** → 보안-테스트 → 완료 → PMO 회고 (의무)

### Agent model tier (ADR-042 Amendment 2, 2026-05-11)

InfraEngineerAgent·QADeveloperAgent·DataEngineerAgent = `claude-haiku-4-5` (기계적 패턴 실행 카테고리).
나머지 모든 agent = Sonnet 이상.

### Plugin 업그레이드 체크리스트

`mctrader-hub/.claude/_overlay/CLAUDE.md` §"codeforge 업그레이드 프로세스" (step 1~6) 참조.
