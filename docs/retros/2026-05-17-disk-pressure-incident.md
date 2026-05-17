---
incident: 디스크 압박 117GB 미해소 (mctrader-compactor production)
date: 2026-05-17
duration_hours: ~8 (사용자 보고 ~13:00 → 세션 종료 ~21:30)
status: PARTIAL — 코드 측 fix 박제 완료 (WS-B / WS-A / MCT-182), 운영 측 117GB 자연 회수는 2건의 사전 회귀 (이슈 A NAS 403 + 이슈 B l2.py file sort byte-order) LAND 후 점진 가동.
related_prs:
  - mctrader-data#83 (WS-B `4dc11dc` — scan_and_cleanup_legacy tier-aware nas_key)
  - mctrader-data#85 (WS-A `f2e2bc9` — date-bounded historical tier promotion)
  - mctrader-data#92 (U1-ADR `ecfe150` — spec git stage + §7.1 RESOLVED + CLAUDE.md plan section, prior 세션)
  - mctrader-data#93 (MCT-182 + trace `be5bd50` — vendor wheel refresh + WS-A/B brainstorm 산출물 박제)
related_adrs:
  - ADR-034 (NAS Object Key Unification, mctrader-hub Accepted)
  - ADR-027 §D6 (silent-skip 차단 — 이슈 A NAS 403 동반 silent failure 가 amendment 후보)
  - ADR-009 §D12 (forward-only invariant)
follow_up_stories:
  - 이슈 A: NAS bucket auth/policy 복원 (ops/infra 도메인, 별 Story)
  - 이슈 B: l2.py + l3.py local fallback file sort ts_utc 기반 + 단위 회귀 (production-faithful fixture)
  - EPIC #86 nas-key-unification Phase 2 (U2-HELPER #88 / U3-MIGRATE #89 / U5-VERIFY #91 — prior 세션 진행)
authors:
  - mccho8865@gmail.com (operator + reporter)
  - Claude (Opus 4.7, codeforge orchestrator session)
---

# 디스크 압박 117GB 미해소 — 인시던트 retrospective (2026-05-17)

> 본 문서는 mctrader-data 측 1차 retro SSOT. mctrader-hub 측 cross-cutting PMO audit
> 미러: `mctrader-hub/docs/retros/PMO-AUDIT-INCIDENT-2026-05-17-disk-pressure.md`
> (process pattern 박제 — codeforge governance 입력). 본 문서는 코드/도메인 측 사실
> + 디버깅 진행 + lessons learned 박제에 집중.

## §1 증상 + 사용자 보고

세션 초기 사용자 운영 보고 원문 (~13:00 KST):

> "로컬 디스크의 디스크 압박이 여전히 해소되지 않았는데?"

초기 가시 신호:
- `mctrader-compactor` 컨테이너의 `/var/lib/mctrader/data` 점유율 임계 근접 유지.
- `market/orderbooksnapshot/.../tier=L1/` namespace 의 누적 parquet 23,981 files
  (특히 2026-05-13 ~ 2026-05-15 윈도우 16,946 files ≈ 117GB).
- 직전 세션의 MCT-189 (forward grace-0 wiring) LAND 후에도 회수 trace 부재 —
  `docker logs mctrader-compactor | grep "legacy cleanup batch"` 가 `cleaned=0` 만 출력.

operator 의 직접 가설은 부재 — "왜 안 줄어드는가" 만 제기. 디버깅 진입.

## §2 진단 — 6회 가설 정정 (체계적 디버깅 효과 정량)

진단은 매 가설마다 production 실측 (HEAD/list_objects) 또는 코드 직접 읽기로 정정.
6회 모두 잘못된 fix 가 main 에 LAND 되기 전 차단됨.

| # | 가설 | 정정 원인 (사실 근거) | 비용/효과 |
|---|------|--------------------|-----------|
| H1 | glob 구분자 `market\**\*.parquet` (Windows backslash) → POSIX 비매칭 | Read 도구의 `/` → `\` 렌더링 아티팩트. 원시 바이트는 정상 `/` (codeforge:claude-md 인용 verbatim). | systematic-debugging "Verify before continuing" 가 prevent — 실제 fix 였다면 동일 결과 + spurious change (true positive: 0, false positive: 1) |
| H3 | `scan_and_cleanup_legacy` 의 단순 `l1/` prefix 누락 | 실제로는 tier 별로 키 스킴이 다름 (L1 = `l1/market/<rel>`, L2/L3 = 평면 `market/<rel>`). live `head_object` 두 패턴 모두 hit 으로 정정. | 단순 prefix 추가했다면 L2/L3 회귀 발생 (L2/L3 키도 `l1/` 받음 → 100% 404). `_resolve_legacy_nas_key` helper 가 root-relative parts 기반 tier 추출로 정정 (PR #83 review FIX `3688ee8`) |
| manifest-bounded | MCT-173 backfill manifest 의 date range 한정으로 회수 범위 도출 | manifest 가 76 parquet 만 박제 (전체 23,981 의 0.3%). 본체는 manifest 와 무관한 forward ingestion 자연 누적. | manifest-bounded 채택했다면 117GB 본체 (16,946 files / 05-13~15) 무영향, 0.3% 만 회수 |
| hour-filter | `compact_hour(date, hour)` 가 ts_utc 로 hour 필터링 가정 | `src/mctrader_data/compactor/l2.py` 직접 읽기로 hour 파라미터 = output 파일명용, L1 read 는 24 hour 모두 동일 input 확정. | 잘못된 test 가정으로 통합 테스트 invalidate 가능 — 24 hour 별 input 분리 가정한 fixture 가 production 동작 미반영 |
| rglob | `_discover_partitions_in_range` 의 `date_dir.glob("part-*.parquet")` 비재귀 | production L1 layout = `date=<d>/node=<node_id>/part-*.parquet`. 비재귀 glob → 0 match → WS-A 전체 no-op. 테스트 fixture 가 평면화 (`date=*/part-*.parquet`) 한 결과 마스킹. | rglob (commit `c169720` CRITICAL fix) 미반영 시 production 적용 시점에 WS-A 전체 no-op. fixture-vs-production layout drift 추가 단위 회귀 박제 (`tests/compactor/test_historical_partition_discovery.py` "production node= layout" 케이스) |
| 운영 실행 단일 원인 | promote-historical 첫 실행의 0 회수 (`l2_compacted=0, skipped_no_l1=456, errors=25`) 가 단일 원인 | 2개 독립 사전 회귀 동반 노출 — (A) NAS bucket auth/policy 회귀 (`forbidden 403`), (B) `l2.py` local fallback file sort byte-order monotonic_violation. 둘 다 별 Story 분리. | 단일 가정 (e.g. "WS-A 의 channel filter 추가 fix") 진행했다면 부분 fix 후 여전히 회수 0. 멀티 root cause 확정으로 fix triage 정합 |

**효과 (정성)**:
매 가설을 추측으로 통과시켰다면 (a) 잘못된 fix 가 main 에 LAND → 추가 patch 사이클,
(b) production 측 silent failure (NAS 403 / sort drift) 잔존, (c) 본 세션의 trust capital
소진. 디버깅 규율 (`Iron Law: NO FIXES WITHOUT ROOT CAUSE INVESTIGATION` —
`superpowers:systematic-debugging`) 가 prevent. 6 가설 × 평균 5~10분 verify 비용 ≪
잘못된 fix 1건의 LAND + 재롤백 + 운영 트래픽 영향 비용.

## §3 근본 원인 (다층)

### §3.1 디스크 압박 직접 원인 (3-layer)

**RC-1 — forward `_run_l2`/`_run_l3` `[today, yesterday]` 고정 윈도우 (설계 가정 결함)**:
forward compaction loop 가 L1 의 실제 date 파티션을 무시하고 어제·오늘만 승급. 어제 너머
historical-dated L1 (e.g. 컨테이너 다운타임 회복 후 backfill, 또는 단순 forward
ingestion 의 지연 sealed segment) 은 영구 미승급 → NAS 미적재 → WS-B sweep 도 안전망
(`promote_l1` 4중 HEAD 404 → `preserved`) 으로 인해 보존. 데이터 안전하나 디스크 회수 0.

**RC-2 — `scan_and_cleanup_legacy` 의 nas_key tier-비인지 (WS-B fix, PR #83)**:
`scan_and_cleanup_legacy` 가 `nas_key` 를 전 tier 평면 (`str(parquet.relative_to(root)).replace("\\","/")`)
으로 산출. 그러나 실제 NAS object 키 스킴은 tier 별로 다름:
  - `tier=L1` → `l1/market/<rel>` (`DualWriter.put_l1`: `"l1/"+rel.as_posix()`)
  - `tier=L2|L3` → 평면 `market/<rel>` (`_dispatch_dual_write`: `relative_to(root)`)

→ `tier=L1` 객체는 항상 HEAD 404 → `PromotionVerifyError` → `preserved` →
**117GB L1 영구 미회수** (디스크 미해소의 직접 원인 중 하나).

**RC-3 — backfill `while true` 루프 가동 (재팽창, 사용자 결정으로 유지)**:
MCT-173 backfill 컨테이너가 sealed WAL 을 지속 materialize 함. 회수 속도가 ingestion +
backfill 속도보다 낮으면 net delta 양. 사용자 결정으로 backfill 유지 (도메인 가치
우선) — 회수 도구 (WS-A) 의 throughput 으로 흡수 가능한지가 후속 관측 대상.

### §3.2 사전 회귀 2건 (이번 진단 중 동반 노출)

**이슈 A — NAS bucket auth/policy 회귀 (ops/infra 도메인)**:
WS-A `promote-historical --start 2026-05-13 --end 2026-05-13 --exchange upbit`
첫 실행에서 NAS PUT 가 403 `forbidden` 으로 다수 실패. WS-A 본 코드는 정상 (HEAD-then-PUT
sha256 idempotency 보존). bucket policy / IAM 회귀로 추정. forward 경로 (`_dispatch_dual_write`)
도 동일 403 silent fallback 으로 영향 받음 — `ADR-027 §D6` "silent-skip 차단" 와의
정합 후속 amendment 후보 (Action Item 1).

**이슈 B — `l2.py` local fallback file sort byte-order monotonic_violation (설계 결함, latent)**:
WS-A 실행 errors 25건 분석 중, L2 compaction local fallback 경로에서 input parquet
파일을 byte-order (filename string sort) 로 sort 함을 확인. node_id 가 hostname-prefix
변경되거나 sealed segment timestamp 가 0-padded 폭이 다르면 monotonic_violation 발생.
forward 경로는 NAS 정렬 path 사용으로 무사 — production 발견 시점까지 latent.
별 Story 분리, 후속 brainstorm spec untracked
(`docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md`).

## §4 Remediation (이번 세션 박제)

### §4.1 PR 체인 (main commit log)

```text
4dc11dc fix(MCT-189): post-merge — scan_and_cleanup_legacy tier-aware nas_key (WS-B) (#83)
f2e2bc9 feat(WS-A): date-bounded historical tier promotion (117GB L1 무손실 회수 도구) (#85)
ecfe150 docs(U1-ADR): spec git stage + §7.1 RESOLVED + CLAUDE.md plan section (#92)
be5bd50 chore(MCT-182): vendor wheel refresh + WS-A/B brainstorm 산출물 사후 박제 (#93)
```

### §4.2 PR 별 박제 요약

| PR | Scope | 핵심 invariant |
|---|---|---|
| #83 WS-B | `_resolve_legacy_nas_key` helper (tier-aware) + 1줄 caller 교체 (`scan_and_cleanup_legacy`) | `promote_l1` 4-tuple HEAD verify + pre-delete guard byte-unchanged. 키가 틀려도 안전하게 `preserved` (삭제 경로 미도입). |
| #85 WS-A | `_discover_partitions_in_range` + `_historical_dual_write` + `run_historical_promotion` + `promote-historical` Click CLI | INV-A: forward `_run_l2`/`_run_l3` 윈도우 byte-unchanged. INV-B: 무손실 — WS-A 경로 `promote_l1`/`unlink` 호출 0 (WS-B sweep 이 회수 게이트). INV-C: 재실행 안전 — deterministic `run_id` + HEAD-then-PUT sha256. INV-D: channel 한정 (`orderbooksnapshot`). |
| #92 U1-ADR | spec git stage + §7.1 RESOLVED + CLAUDE.md plan section (`## nas_key SSOT 규약`) + sibling mctrader-hub#393 (ADR-034 publish) cross-ref | EPIC #86 nas-key-unification Phase 2 진입 게이트. resolver internal to mctrader-data, engine = candles only, market data L1 cross-repo impact = none → U4-XREPO #90 close 후보. |
| #93 MCT-182 + trace | `vendor/mctrader_market-0.1.0-py3-none-any.whl` 12,129 → 27,950 bytes (paper_lineage 포함) + spec/plan git stage | 운영 실 적용 시점에서야 module 누락 surface — vendor wheel staleness 가 deploy unblock 차단 (lesson §6.4). |

### §4.3 운영 적용 시퀀스

1. PR #83/#85 머지 (admin override squash, windows-latest CI pre-existing testcontainers infra 실패 비차단).
2. 신규 image build → 컴팩터 재기동 → `ModuleNotFoundError mctrader_market.paper_lineage` restart loop (vendor wheel staleness).
3. MCT-182 vendor wheel 갱신 (mctrader-market@4902b53 main 소스 빌드) → 컴팩터 healthy.
4. operator 실행: `docker exec mctrader-compactor python -m mctrader_data.cli promote-historical --root /var/lib/mctrader/data --start 2026-05-13 --end 2026-05-13 --exchange upbit --channel orderbooksnapshot`
5. 결과: `l2_compacted=0, skipped_no_l1=456, errors=25` (회수 0 — 이슈 A + 이슈 B 동반 노출).
6. rollback 의사결정 무관 (forward / sweep 정상 — 이미지 healthy 유지). 117GB 잔존, 이슈 A/B LAND 대기.
7. PR #92 (prior 세션 carry) + #93 (vendor + trace) 머지 → main HEAD `be5bd50`.

## §5 잔존 + 후속

### §5.1 별 Story 분리

| 분류 | 후속 Story | 소유 도메인 |
|---|---|---|
| 이슈 A (NAS 403) | NAS bucket auth/policy 복원 + ADR-027 §D6 silent-skip 차단 cross-ref 강화 | ops/infra (별 세션) |
| 이슈 B (sort drift) | l2.py + l3.py file sort ts_utc 기반 + production-faithful fixture 단위 회귀 | data lane (별 세션 — spec `2026-05-17-compactor-sort-key-l1-naming.md` 박제 됨) |
| EPIC #86 Phase 2 | U2-HELPER (#88) / U3-MIGRATE (#89) / U5-VERIFY (#91) — prior 세션 진행 | data lane + cross-repo (mctrader-hub ADR carrier) |

### §5.2 자연 회수 시나리오 (이슈 A/B LAND 후)

이슈 A + 이슈 B LAND 후 자동 가동 흐름:
1. forward `_dispatch_dual_write` 403 회복 → 신규 L1 정상 NAS 적재.
2. operator 가 `promote-historical --start 2026-05-13 --end 2026-05-15` 재실행
   → WS-A 의 L1→L2→NAS PUT (+ L3→NAS PUT) 정상 가동.
3. 다음 6분 cycle 마다 WS-B `scan_and_cleanup_legacy` 가 batch_limit=500 으로
   점진 회수. 16,946 files / 500 = ~34 cycle × 6min ≈ **3.4시간** (이론치). 실제는
   I/O 경합 + NAS 응답 latency 로 52h 점진 (PR #85 cross-ref).

## §6 Lessons Learned

### §6.1 시스템 디버깅 규율 효과 정량화

6 가설 / 0 잘못된 fix LAND. 매 가설마다 production 실측 또는 코드 직접 읽기로 정정.
`superpowers:systematic-debugging` 의 "Verify before continuing" 가 6/6 prevent.
정성 효과: 잘못된 fix 1건 LAND 시 추가 patch 사이클 + 운영 트래픽 영향이 verify 비용
6× 누적의 수십 배. **규율의 ROI 가 단일 인시던트 내에서 회수됨**.

### §6.2 다층 결함 동시 노출 패턴

단일 인시던트 (디스크 압박) 진단 중 사전 회귀 2건 (NAS 403 + sort drift) 추가 노출.
공통 특성:
- 두 회귀 모두 단위/통합 테스트는 부재 (production-faithful fixture 미박제).
- 운영 적용 단계 (WS-A 첫 실행) 에서만 surface.
- forward 경로 silent fallback (NAS 403 동반 silent failure / sort fallback 의
  monotonic_violation skip) 으로 일상 운영 중에는 가시화 안 됨.

→ **인시던트 1건이 multi-defect surfacing 의 자연 trigger 역할**. 인시던트 진단 시
"하나의 root cause" 가정 금지 — `superpowers:systematic-debugging` 의 "Pattern
recognition: parallel defects often surface together" 패턴.

### §6.3 테스트 fixture 가 production 결함 마스킹

`rglob` 사례가 대표적. fixture 가 `date=*/part-*.parquet` 평면 layout 으로
seed → 비재귀 glob 통과 → production `date=*/node=*/part-*.parquet` 에서 0 match
재현. PR #85 commit `c169720` "CRITICAL fix" 가 회규 단위 (`production node= layout`)
+ 통합 5 시나리오로 박제. **lesson**: production layout SSOT 를 단위/통합 fixture
가 정확히 반영하도록 강제 (codeforge governance 후보 — Action Item 3).

### §6.4 vendor wheel staleness 가 deploy unblock 차단

MCT-182 vendor wheel (mctrader_market 0.1.0) 가 paper_lineage / aggregation 누락 상태로
12.1KB 박제. WS-A/B 코드는 paper_lineage 미참조라 단위/통합 테스트 통과, 그러나 신규
이미지 빌드 (mctrader-data:pilot) 시 entrypoint 의 다른 모듈이 paper_lineage import →
runtime `ModuleNotFoundError`. CI 또는 staging deploy gate 에서 import smoke test
부재로 LAND 후 운영 적용 시점에야 surface. **lesson**: vendor wheel staleness CI gate
(Dockerfile build dry-run + entrypoint import smoke) 권장 (Action Item 4).

### §6.5 codeforge precedence rule (ADR-064)

세션 중 "추측 멈춤 + 사용자에게 derived default 발화" 가 반복 적용. 예: WS-B 의 nas_key
tier 분기 가설 → "L1 만 `l1/` prefix" 정정 시점에 사용자 confirm 없이 default 추정
하지 않고 production HEAD 두 패턴 모두 확인 후 derived default 제시. 결과: dialog
트래픽 절약 + 진행 속도 양립. **lesson**: ADR-064 (codeforge precedence rule) 이 단일
세션 내 다회 invocation 으로 효과 검증.

### §6.6 cross-repo retrospective SSOT 분리 (본 retro 의 메타)

본 retro 는 mctrader-data 측 사실/디버깅/lesson 박제. cross-cutting process pattern
(codeforge governance, ADR 후보, PMO cross-Story trend) 은 mctrader-hub
`PMO-AUDIT-INCIDENT-2026-05-17-disk-pressure.md` 가 SSOT. **lesson**: cross-repo
영향 인시던트는 retro 도 도메인 분리 박제 — repo 별 reader 가 자기 SSOT 만 보아도
의사결정 가능하도록 (memory feedback_lane_self_write_boundary 정합).

## §7 Action Items (cross-Story)

| # | Action | 소유 | 우선순위 |
|---|---|---|---|
| 1 | 이슈 A: NAS auth/policy 복원 + ADR-027 §D6 silent-skip 차단 cross-ref 강화 (forward `_dispatch_dual_write` 403 silent fallback 차단) | ops/infra + data ArchitectPL | HIGH (운영 가동 게이트) |
| 2 | 이슈 B: `l2.py` + `l3.py` file sort 를 ts_utc 기반으로 변경 + production-faithful fixture 단위 회귀 박제 | data lane (spec `2026-05-17-compactor-sort-key-l1-naming.md` 진행) | HIGH (운영 가동 게이트) |
| 3 | L2 quarantine 시 silent return `None` → 명시 `OperationalRiskAlert` (ADR-027 §D6 정합) | data lane | MEDIUM (관측성) |
| 4 | vendor wheel staleness CI gate (Dockerfile build dry-run + entrypoint import smoke) | infra lane (mctrader-data CI) | MEDIUM (deploy unblock 보호) |
| 5 | retrospective 패턴 박제 (codeforge:retro-template 별 도구화 — 본 retro 의 frontmatter / §1-§7 구조를 template 화) | codeforge governance (hub-side PMO 발의) | LOW |
| 6 | EPIC #86 nas-key-unification Phase 2 진행 (U2-HELPER #88 / U3-MIGRATE #89 / U5-VERIFY #91) | data lane (prior 세션 진행 중) | MEDIUM (별 Epic) |

## §8 Cross-ref

| 항목 | 위치 |
|---|---|
| 본 세션 transcript | 대화 자체 (Claude Opus 4.7 codeforge orchestrator) |
| main commit log | `git log 4dc11dc..be5bd50` (4 PR) |
| WS-B PR (RC-2 fix) | mctrader-data#83 |
| WS-A PR (RC-1 회수 도구) | mctrader-data#85 |
| U1-ADR PR (Phase 2 gate) | mctrader-data#92 |
| MCT-182 + trace PR | mctrader-data#93 |
| ADR-034 carrier | mctrader-hub#393 (`docs/adr/ADR-034-nas-key-unification.md`) |
| WS-A/B brainstorm spec | `docs/superpowers/specs/2026-05-17-disk-pressure-remediation-design.md` |
| WS-B implementation plan | `docs/superpowers/plans/2026-05-17-ws-b-tier-aware-nas-key.md` |
| Issue B 후속 spec (untracked, 별 Story) | `docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md` |
| Hub PMO audit mirror | `mctrader-hub/docs/retros/PMO-AUDIT-INCIDENT-2026-05-17-disk-pressure.md` |
| EPIC nas-key-unification | mctrader-data#86 (U1-ADR #87 / U2-HELPER #88 / U3-MIGRATE #89 / U4-XREPO #90 close / U5-VERIFY #91) |
| 후속 Story (이슈 A) | 별 세션 발의 대기 |
| 후속 Story (이슈 B) | 별 세션 발의 대기 (spec 박제 됨) |
