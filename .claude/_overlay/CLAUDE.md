## Project

`mctrader` 6-repo sister 의 한 부분. mctrader-hub 가 governance / Story / ADR / Epic SSOT.

## codeforge 의무 사용 (CFP-96 Phase 6b, ADR-027)

본 repo = mctrader 6-repo 의 5 sister 중 하나. CFP-111 Phase 6b adoption 시점부터 codeforge protocol 의무.

### 의존 plugin (13개)

`/plugins install` 으로 13 plugin 등록 의무 (codeforge wrapper + 6 lane + **2 deploy** + 4 dep). codeforge 6.0.0 MAJOR 로 family 7→9 (codeforge-deploy + codeforge-deploy-review 신설, 2026-05-22).

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

### plugin 버전 메모 (2026-05-22 업그레이드 반영)

codeforge plugin 최신 버전 (hub `mctrader-hub/.claude/_overlay/CLAUDE.md` mirror — 자세한 carrier 링크 + 반영 로그는 hub 참조). **codeforge 6.0.0 MAJOR — lane 6→8 (배포/배포-리뷰 신설) + family 7→9** (5.75.0 → 6.0.5):

```
codeforge@mclayer               # 6.0.5 — CFP-1059/ADR-087~090 lane 6→8 (배포+배포-리뷰) MAJOR + deploy.* 스키마; CFP-1086/1126 ADR-042 Amd8~10 design roster 6+3+1; ADR-085 multi-session active_sessions[]; 5.86.0 cross-repo-gh-safety hook (gh write --repo 의무)
codeforge-requirements@mclayer  # 0.6.0 — (변경 없음) CFP-510 divergence 4 영역 + PL fact marker 5종
codeforge-design@mclayer        # 0.21.1 — ADR-091 DDD wave: 14 agent bounded_context+ddd_pattern + change-plan §3.A/§3.D DDD 블록; ADR-042 Amd8~10 roster 6+3+1 (AggregateArch→ModuleArch 통합, APIContractArch 신설)
codeforge-develop@mclayer       # 0.7.0 — (변경 없음) CFP-507 DeveloperPL PR body convention / CFP-609 병렬 결정 tree
codeforge-test@mclayer          # 1.3.0 — IntegrationTestAgent §7.4 operational-risk 3-axis measurement contract + evidence path; review-verdict v4.8 DDD sync
codeforge-review@mclayer        # 1.9.0 — review-verdict-v4 v4.8 +3 DDD finding type(bc_violation/aggregate_violation/ubiquitous_language_drift); §8.6 audit-gate IntegrationTest pointer 검증
codeforge-pmo@mclayer           # 0.2.0 — DialogFidelityAgent 신설 (agent 2→3, ADR-071 Amd1 external read-only dialog auditor)
codeforge-deploy@mclayer        # 1.0.0 (신설 seed) — 배포 lane (ADR-087 blue-green). Phase 1 governance scaffolding
codeforge-deploy-review@mclayer # 1.0.0 (신설 seed) — 배포-리뷰 lane (ADR-088 + ProductionEvidenceDeputy 이관)
```

> **deploy.* 스키마 정합**: deploy lane = opt-in (project.yaml `deploy.*` block 부재 = trigger skip). mctrader-data 실 배포(로컬빌드 `mctrader-data:pilot` + compose + NAS Container Manager + .env)는 deploy.* 스키마 전제(Docker Hub + Traefik + 1Password + SSH-pull blue-green)와 패러다임 mismatch → hub §deploy.* escalate 결과 따름 (consumer workaround 금지).

### Adversarial Debate auto-trigger (CFP-391/411)

DesignReview / Requirements lane 진입 시 divergence 감지되면 자동 multi-round debate (min 3 / max 5). divergence 미검출 시 기존 single-shot 유지 (backward-compat). Anchor 재발 시 즉시 사용자 escalation. 자세한 sequence 는 hub CLAUDE.md 참조.

### 도메인 ADR 작성 schema (CFP-387/ADR-058)

본 repo 또는 hub 의 `docs/adr/` 신규 작성 시 frontmatter `is_transitional` + body `## 해소 기준` 섹션 의무 (미선언 default = `true`). 측정성 3-tuple (metric / who / how) 정량 명시 의무, 모달 어휘 금지. 자세한 schema 는 hub CLAUDE.md 참조.

### Story workflow phase (MCT-129 → codeforge 6.0.0 MAJOR, 2026-05-22)

요구사항 → 설계 → 설계-리뷰 → 구현 → 구현-리뷰 → CI 테스트 (ADR-048) → **통합테스트 (IntegrationTestAgent, ADR-055, §8.6, test-verdict-v2.2, Epic-level CFP-371/CFP-373)** → 보안-테스트 → **배포 (ADR-087)** → **배포-리뷰 (ADR-088)** → 완료 → PMO 회고 (의무)

> 배포/배포-리뷰 = opt-in (project.yaml `deploy.*` 부재 = trigger skip). mctrader-data 는 현재 미작성 → 8-phase 까지만 실행 (위 §plugin 버전 메모 deploy.* 정합 참조).

### Agent model tier (ADR-042 Amendments 2/5/8/9/10 — 2026-05-10 ~ 05-21)

InfraEngineerAgent·QADeveloperAgent·DataEngineerAgent = `claude-haiku-4-5` (기계적 패턴 실행 카테고리).
CodebaseMapperAgent·RefactorAgent·ChangeImpactAgent·DeveloperPLAgent = `claude-sonnet-4-6` selective rollback (ADR-057 Amendment 3 / ADR-042 Amendment 5, CFP-448 — 4 agent 실측 정합).
FeasibilityAgent·ContinuityAgent = `claude-opus-4-7` 유지.
나머지 모든 agent = Sonnet 이상.
**Design roster 6+3+1 (ADR-042 Amd8~10 / ADR-091 DDD wave)**: CodeArchitect→ModuleArchitect (AggregateArch 흡수), APIContractArchitect 신설, DataArchitect OLAP 축소, ProductionEvidenceDeputy→deploy-review 이관. 14 design agent `bounded_context`+`ddd_pattern` frontmatter. DeployPL=Sonnet / DeployReviewPL=Opus.

### Plugin 업그레이드 체크리스트

`mctrader-hub/.claude/_overlay/CLAUDE.md` §"codeforge 업그레이드 프로세스" (step 1~6) 참조.

### BackfillOrchestrator channel parametrize + hour key amend (MCT-159, EPIC-cold-tier-stage-3-wiring sibling)

MCT-156 Phase 2 LAND (`mctrader-data#47` dff8aa5) 후 hot pipeline NAS PUT 정상화. 그러나 LAND 이전 로컬 누적 L2/L3 backlog (~8.85 GiB / 7118 file, 신규 schema `tier=L{2,3}/.../date=D/hour=HH/node=MERGED/`) 강제 이관 필요. MCT-153 (transaction-only path) `BackfillOrchestrator` 의 2 amendment 후 재호출.

**amendment 2종**:

1. **channel parametrize**: `BackfillOrchestrator.__init__` 에 `channel: Literal["orderbooksnapshot", "transaction"] = "orderbooksnapshot"` 추가 + `_discover_partitions` 의 `"orderbooksnapshot"` 하드코딩 (line 596) → `self._channel` parametrize. `run_backfill.py` 에 `--channel` flag 추가.
2. **hour key 처리**: `_build_chunk_spec` (line 645-709) 에 `hour = _extract_hive_value(parts, "hour")` 추출 + `nas_partition_prefix` 에 `/hour={hour}/` 박제 (있을 시). hour 부재 시 backward-compat (legacy ADR-009 §D2.1 layout 정합).

**🔴 CRITICAL — test fixture 갱신 의무**: 사용자 unstaged `_discover_partitions()` hot-fix 가 `make_partition_dir()` fixture (`tests/nas_migration/test_backfill_orchestrator.py`) 와 mismatch → 8+ integration test silent 통과 (total_chunks=0 오통과) 위험. **Phase 2 Task 8 first step = fixture 갱신 (TDD red phase 강제)**.

**회귀 보호**:
- `tests/nas_migration/test_backfill_orchestrator.py` + `test_backfill_resumability_chaos.py` 양 channel 매트릭스 확장
- default `channel="orderbooksnapshot"` backward-compat 유지 (MCT-153 transaction-only 회귀 0)
- hour 부재 case (legacy layout) backward-compat 보존

**invariant 보존**:
- forward-only (ADR-009 §D12.2) — row 변경/삭제 0
- 7종 invariant ALL PASS (MCT-151 InvariantHarness inject 자동)
- local GC 7d grace 답습 (MCT-155 gc_runner 재사용, MCT-153 손실 교훈 정합)

**scope 한계**:
- L2/L3 backlog 8.85 GiB only (~4.8% of 전체 183 GiB)
- L1 sealed backlog (76,200 file / ~115 GiB) + WAL (59 GiB) = MCT-160 책임 (sequential 의무)
- bucket versioning 활성화 = MCT-161 책임

### Data Health Framework (MCT-165, 2026-05-14)

`src/mctrader_data/health/` 신규 모듈 — 4-layer data accumulation health verification.

**INV-1 read-only**: fs walk만 (Path.glob, Path.rglob, Path.stat). write/수정/소급보정 절대 금지.
**INV-2 cut-in**: `start_date` default = 2026-05-09 (50-sym universe 전환 시점, MCT-103).
**INV-3 4 layer freeze**: volume / gap / file_count / lag (parity/schema/presence 후속 ADR).
**INV-4 exit code**: 0=ALL PASS, 1=any FAIL, 2=tool error (NotImplementedError 포함).

**실제 storage layout** (reconciled 2026-05-14):
```
<MCTRADER_DATA_ROOT>/market/orderbookdepth/schema_version=orderbook_depth.v1/
  tier={L1|L2|L3}/exchange={exchange}/symbol={symbol}/
  date={YYYY-MM-DD}/[hour={H}/][node={node}/]part-*.parquet
WAL: <MCTRADER_DATA_ROOT>/wal/{exchange}/orderbookdepth/{symbol}/{YYYY-MM-DD}/segment-*.ndjson
```

**CLI 사용**:
```bash
mctrader-data health-check --target collector --window 5d --start-date 2026-05-09 --output markdown
mctrader-data health-check --baseline rolling  # NotImplementedError (exit 2) — ADR-028 reserved
```

**테스트**: `tests/unit/health/` (6 unit) + `tests/integration/health/` (1 integration).
**Cross-ref**: MCT-165 Story / ADR-028 Reserved / ADR-009 §D12.
