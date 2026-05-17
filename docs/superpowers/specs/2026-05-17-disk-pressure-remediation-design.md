---
spec: disk-pressure-remediation
date: 2026-05-17
origin: 운영 인시던트 — 로컬 디스크 압박 미해소 (사용자 보고)
status: brainstorm-complete → writing-plans 대기
stories: 2 (WS-B post-merge FIX → WS-A historical tier promotion, 순차)
pre_lookup_evidence:
  - "runner.py:148-170 _run_l2 [today,yesterday] 고정 윈도우 — verified-via: Read"
  - "runner.py:265 _dispatch_dual_write nas_key 평면 — verified-via: Bash sed"
  - "runner.py:351 scan_and_cleanup_legacy nas_key 평면(tier-비인지) — verified-via: git show HEAD cat -A"
  - "dual_writer.py:371 put_l1 nas_key='l1/'+rel — verified-via: git show HEAD cat -A"
  - "프로덕션 HEAD 실측: L1=l1/market/... FOUND / L2,L3=market/... FOUND / market/<L1rel>=404 — verified-via: docker exec boto3 head_object"
  - "볼륨 189GB, market/orderbooksnapshot tier=L1=117.4GB/23,981 files, NAS l1/ tier=L1=4,608 — verified-via: docker exec du + list_objects_v2"
  - "#48 MCT-159 open phase:reservation — verified-via: gh issue list"
---

# 디스크 압박 미해소 — Remediation 설계 (brainstorm 산출)

## §1 동기 (WHY — Analyst 추출)

단건 디스크 청소가 아니라, backfill `while-true` 재팽창을 허용하는 운영 조건에서도
**forward-only invariant + audit trail 을 유지하며 historical backfill 산출물이
승급·NAS 이관·회수되는 닫힌 루프**를 복구하는 것. 사용자 결정: 무손실 / backfill 유지 /
MCT-189 #75 post-merge FIX.

## §2 근본 원인 (전수 프로덕션 검증 완료)

| RC | 내용 | 증거 |
|----|------|------|
| RC-1 | `_run_l2`/`_run_l3` 가 L1 경로의 실제 date 파티션을 무시하고 `[today,yesterday]` 만 승급 → MCT-173 backfill historical-dated L1 영구 미승급·로컬 고착 | runner.py:148-170; L1 23,981 vs L2 9,169 vs NAS 4,608 |
| RC-2 | `scan_and_cleanup_legacy` nas_key 가 tier-비인지(평면 `market/<rel>`) → L1(NAS=`l1/` prefix) 전부 HEAD 404 → `preserved`(미회수, **삭제 아님 = 데이터 안전**) | runner.py:351; HEAD 실측 |
| RC-3 | backfill `while-true` 루프 가동 → 능동적 재팽창 | 컨테이너 Up, 168.9→189GB. **사용자 결정: 유지(수용)** |
| 정상 | MCT-189 forward grace-0 wiring (DualWriter.write committed self-delete) | `[promotion] promoted ... local deleted` 로그 |

**이중/삼중 SSOT (PMO 지적, 핵심 구조 결함)**: nas_key 가 3곳에서 분산 산출 —
`DualWriter.put_l1`=`"l1/"+rel` (L1), `_dispatch_dual_write`(runner.py:265)=평면 rel (L2/L3),
`scan_and_cleanup_legacy`(runner.py:351)=평면 rel (전 tier — RC-2 버그). 회복 키와 PUT 키
불일치 시 무손실 게이트 통과해도 orphan 위험.

## §3 설계 (확정 — derived default + 사용자 confirm)

### Story 1 = WS-B (최우선) — `scan_and_cleanup_legacy` tier-aware nas_key (MCT-189 #75 post-merge FIX)

- 단일 helper `_resolve_legacy_nas_key(parquet, root)` 신설: parquet 경로의 `tier=L*`
  파싱 → **tier=L1 → `"l1/"+rel.as_posix()`**, **tier=L2|L3 → 평면 `rel.as_posix()`**.
  (PUT 측 `DualWriter.put_l1` `l1/` + `_dispatch_dual_write` 평면 과 정합)
- TDD: 실패테스트 먼저 — `test_runner_retroactive_cleanup.py` 를 **실 프로덕션 스킴**으로 시드
  (L1 객체는 `l1/market/...`, L2/L3 는 `market/...`). 현재 코드에서 L1 케이스 fail 입증 →
  helper 도입 → green.
- 무손실 안전망 = 기존 `promote_l1` 4중 HEAD verify + pre-delete guard (변경 없음).
- §10 FIX Ledger 기록 (MCT-189 #75 post-merge, closed PR in-loop 아님 → 신규 PR).
- 효과: NAS 존재분(L1 ~19% + L2/L3 전량) 회수 시작. 117GB 본체는 WS-A 가 NAS 적재 후 회수.

### Story 2 = WS-A (WS-B merge 후) — manifest-bounded historical tier promotion

- `audit/backfill-manifest-*.yaml` 의 date range 로 **범위 한정 일회성** historical-promote
  경로 신설. forward `_run_l2`/`_run_l3` `[today,yesterday]` 윈도우는 **불변**
  (Edge-RC1: 전역 완화 시 실시간 과확장+throughput 급증 회피).
- date-bounded L1→L2(→L3) 를 기존 `compact_hour`/`compact_day` + DualWriter NAS PUT 로
  승급. idempotent: `.compacted` sentinel + INV-1 XOR + INV-6 already_promoted no-op
  (재실행/`while-true` 재생성과 안전 공존).
- 무손실 게이트: WS-B 수정본 `scan_and_cleanup_legacy` 가 L1→L2 NAS PUT 성공
  (promote_l1 4중 HEAD verify) 확인 **후에만** L1 reclaim.
- 약한 선행의존: **#48 MCT-159 Issue1** (orderbookdepth `NotImplementedError` L1 loop
  차단) — Story §3 guard/우회 확인 게이트 명시 의무 (blocking 아님).
- verify 스크립트 + CLAUDE.md backfill 섹션 cross-ref.

### 부가 (PL 판단)

- domain-knowledge 2 페이지 신규 (mctrader-hub/docs/domain-knowledge/domain/data-health/):
  `tier-aware-nas-key-scheme.md`, `l1-promotion-window.md` — 현재 코드 de-facto SSOT, 미박제.
- ADR 후보 (Mandatory, PMO 발의 / ArchitectAgent author): "NAS nas_key tier-aware prefix
  scheme SSOT" — nas_key 3곳 분산, MCT-168/169/189/190 반복 touch ≥2 (ADR-045 Amend5 §D-9).

## §4 Acceptance Criteria (Analyst AC 정규화)

- AC-1: backfill manifest date range(05-11~16) L1 이 forward 윈도우 밖이어도 manifest-bounded
  경로로 L2 승급 대상 포함 (WS-A).
- AC-2: `scan_and_cleanup_legacy` 가 L1=`l1/` / L2,L3=평면 tier-aware 해석 — 오삭제/회수누락 0 (WS-B).
- AC-3: L1 reclaim 은 L1→L2→NAS PUT 검증(promote_l1 4중 HEAD) 통과 후에만 — 선행 미검증 삭제 0.
- AC-4: verify gate 통과 시에만 remediation 완료 판정, silent-skip 0 (실패는 명시 노출).
- AC-5: 대상 범위별 무손실 회수 audit trail (완료/미완료 범위 구분 가능).

## §5 Edge / Risk

- Edge-RC1: historical 윈도우 전역완화 금지 — manifest/범위 한정 결합 (확정: manifest-bounded).
- Edge-RC2: tier 판별 실패 시 회수누락보다 교차삭제가 더 위험 → tier 보수적 확정 시에만 cleanup.
- R: unlink↔backfill write race → NAS ETag vs local sha256 divergence → 영구 preserved 누적.
  완화 = `.compacted` sentinel + INV-1 XOR (Researcher unknown, 설계 lane 정밀화).
- R: drain throughput ≥ backfill 재생성 rate 수렴 미검증 (backfill 유지 결정의 trade-off,
  WS-A verify 스크립트가 측정).

## §6 scope_manifest (PMO 2nd pass)

```yaml
scope_manifest:
  epic: "mctrader-data L1 117GB 고착 해소 — historical tier promotion + tier-aware legacy cleanup"
  parallelism: "순차 (Phase1 WS-B → Phase2 WS-A) — 동일 runner.py merge 충돌 회피"
  story_keys: "신규 2개 예약 (Orchestrator 가 hub tracker 확정; MCT-189 후속 ~MCT-19x)"
  planned_adrs:
    count: 1
    candidate: "NAS nas_key tier-aware prefix scheme SSOT (Mandatory — nas_key 3곳 분산, 누적 ≥2)"
  planned_files:
    WS-B:
      - "src/mctrader_data/compactor/runner.py"   # _resolve_legacy_nas_key helper + scan_and_cleanup_legacy
      - "tests/integration/compactor/test_runner_retroactive_cleanup.py"  # 실 l1/ 스킴 시드 정정
    WS-A:
      - "src/mctrader_data/compactor/runner.py"   # 신규 historical-promote method (forward 윈도우 불변)
      - "src/mctrader_data/compactor/<manifest_reader>.py"  # 위치 ArchitectAgent 확정
      - "scripts/verify_historical_promotion.py"
      - "tests/integration/compactor/test_historical_promotion.py"
      - "CLAUDE.md"
  planned_claude_md_sections:
    - "Compactor source 분기 규약 (ADR-017 Amendment 2)"
    - "신규: NAS nas_key tier prefix 규약 (L1=l1/, L2/L3=평면)"
    - "신규: L1 historical promotion window 규약 (forward vs manifest-bounded)"
    - "backfill mode (MCT-173) — historical promotion cross-ref"
  domain_knowledge_pages:
    repo: "mctrader-hub/docs/domain-knowledge/domain/data-health/"
    pages: ["tier-aware-nas-key-scheme.md", "l1-promotion-window.md"]
```
