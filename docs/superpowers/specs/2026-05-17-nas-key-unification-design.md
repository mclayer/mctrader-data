---
spec: nas-key-unification
date: 2026-05-17
origin: 사용자 보고 — NAS 적재 구조 불일치 (L1 분리 적재, L2/L3 미통합). "세 번 더 작업하게 하지 말고 이번에 제대로 완결"
status: brainstorm-complete → U1-ADR Accepted (ADR-034 publish 박제, 2026-05-17)
supersedes_partial: docs/superpowers/specs/2026-05-17-disk-pressure-remediation-design.md (WS-B = 본 spec Phase 1로 흡수; WS-A의 deferred ADR = 본 spec Phase 2로 승격)
stories: 5 (Phase2 unification 5; story_keys = MCT-194~198 채번 — U4-XREPO 는 §7.1 RESOLVED 결과로 close 후보)
adr_carrier: mctrader-hub:docs/adr/ADR-034-nas-key-unification.md (U1-ADR LAND, 2026-05-17 — §결정 1~6 publish)
github_issues:
  epic: mctrader-data#86
  u1_adr: mctrader-data#87
  u2_helper: mctrader-data#88
  u3_migrate: mctrader-data#89
  u4_xrepo: mctrader-data#90 (close 후보 — §7.1 RESOLVED 박제)
  u5_verify: mctrader-data#91
pre_lookup_evidence:
  - "L1 NAS key = 'l1/'+rel — verified-via: Read src/mctrader_data/nas_storage/dual_writer.py:371"
  - "L2/L3 NAS key = relative_to(root) 평면 — verified-via: Read src/mctrader_data/compactor/runner.py:265"
  - "scan_and_cleanup_legacy 전 tier 평면 조회 (RC-2 버그) — verified-via: Read runner.py:350-351 (disk-pressure spec)"
  - "L2 compactor L1 GET prefix = 'l1/market/...' 하드코딩 (4번째 SSOT 분산점) — verified-via: Read src/mctrader_data/compactor/l2.py:150-160"
  - "mctrader-engine = NAS key 간접 의존 (data REST partition_path hint 경유, 직접 read 아님) — verified-via: PMO 2nd pass + historical.py:42,65,87"
  - "MCT-192 = git log 최고 사용 키 (commit 58d99ad) → MCT-191..196 충돌, 채번 Story 생성 시 — verified-via: git log --grep MCT"
  - "MCT-159 (#48) open phase:reservation — verified-via: gh issue list"
  - "bucket versioning=Enabled (MCT-161) = re-key rollback 안전망 — verified-via: disk-pressure spec pre_lookup_evidence"
---

# NAS Object Key 통합 — Unification 설계 (brainstorm 산출)

## §1 동기 (WHY — Analyst 추출)

단순 디렉터리 정리가 아니라, **tier별 NAS key 스킴 분산(현 4곳)으로 인한 반복 패치 루프를
구조적으로 종결**하는 것. 사용자 원문 뉘앙스("세 번 더 작업하게 하지 말고 이번에 제대로"):
MCT-168/169/189/190 이 nas_key 를 반복 touch 했으나 매번 전술 패치 → 분산 SSOT 잔존 →
다음 작업이 또 같은 곳을 건드림. 사용자의 실제 필요 = **단일 SSOT + 기존 데이터 전량 정리
+ 신규 수집 자동 통합 적재(forward-fix) + 부분 성공 상태 잔존 0**. 핵심 가치는 이동이 아니라
**재작업 영구 차단과 완결성 보증**.

사용자 confirm (3 결정, 2-phase 시퀀싱으로 reconcile):
- Q1 "WS-B 전술" + Q2 "전 tier flat (l1/ 제거)" + Q3 "reader까지 같은 Epic" 은
  either/or 로는 모순 → **Phase 1(urgent tactical) → Phase 2(structural permanent)** 시퀀싱
  으로만 동시 충족. Phase 1 = stepping stone, Phase 2 가 supersede.

## §2 근본 원인 / Ground Truth (전수 코드 검증 완료)

| 항목 | 사실 | verified-via |
|---|---|---|
| SSOT-1 (PUT L1) | `put_l1()` nas_key = `"l1/" + rel.as_posix()` → `l1/market/…/tier=L1/…` | Read dual_writer.py:371 |
| SSOT-2 (PUT L2/L3) | `_dispatch_dual_write` nas_key = `relative_to(root)` 평면 → `market/…/tier=L{2,3}/…` | Read runner.py:265 |
| SSOT-3 (cleanup) | `scan_and_cleanup_legacy` 전 tier 평면 조회 → L1(=`l1/`) HEAD 404 → preserved (RC-2, 117GB 미회수) | disk-pressure spec §2 |
| **SSOT-4 (GET L1)** | **`l2.py:158` L2 compactor가 L1 입력을 `f"l1/market/{channel}/…/tier=L1/…"` prefix 로 GET (MCT-169 D3=C)** | Read l2.py:150-160 |

**핵심 구조 결함**: nas_key 가 **4곳** 분산 산출 (PMO 2nd pass 가 brief의 "3곳" 을
실측 4곳으로 정정 — SSOT-4 추가). 회복 키/PUT 키/GET 키 불일치 시 무손실 게이트
통과해도 orphan·split-brain 위험. MCT-168/169/189/190 반복 touch ≥2 →
ADR-045 Amend5 §D-9 기준 Mandatory ADR.

**cross-repo 경계 (PMO 정정)**: mctrader-engine 은 NAS key 를 **직접 read 하지 않음** —
`historical.py:42,65,87` 이 `tier=L1/exchange=…` partition_path **hint string** 을 생성,
`data` REST API 가 그 hint → 실제 NAS key 로 resolve. 따라서 cross-repo 작업의 본체 =
**data REST `partition_path → NAS key` resolver 정합** (resolver 위치 미확인 — 설계 lane
Architect 의 cross-repo 탐색이 prerequisite). engine `tier=L1/` hint 자체 변경 여부는
resolver 계약에 종속.

## §3 설계 (확정 — 사용자 confirm + derived default)

### Phase 1 — WS-B (즉시 ship, urgent / 신규 작업 아님)

기존 `docs/superpowers/plans/2026-05-17-ws-b-tier-aware-nas-key.md` **그대로 실행**.
`scan_and_cleanup_legacy` 에 tier-aware helper `_resolve_legacy_nas_key` 도입 (L1→`l1/`+rel,
L2/L3→평면). 현 split 스킴 하 117GB 회수 → 라이브 189GB volume 디스크 압박 즉시 완화.
**stepping stone — Phase 2 가 helper 를 단순화/제거하며 supersede.** (이미 brainstorm-complete
+ plan 실행 준비 완료. 본 spec 은 Phase 1 을 재설계하지 않고 cross-ref 만.)

### Phase 2 — EPIC-nas-key-unification (영구 구조 fix, 사용자 실제 요구)

**목표 스킴**: 전 tier 단일 평면 SSOT —
`market/<channel>/schema_version=*/tier=L{1,2,3}/exchange=*/symbol=*/date=*/[hour=*/][node=*/]part-*.parquet`
(`l1/` prefix 제거. 로컬 Hive 경로의 `tier=L1/L2/L3` 파티션이 이미 tier 구분 → prefix 불요.
L1 이 L2/L3 와 "완전히 동일한 형식" — 사용자 Q2 confirm.)

| Story 슬러그 | 제목 | repo | 1줄 |
|---|---|---|---|
| **U1-ADR** | NAS nas_key 단일 평면 SSOT 통합 ADR | mctrader-data | deferred candidate → *unification* ADR (split 승인 아님, 4-way → 1 collapse). dual-read 전환 윈도우 + cutover sequence 정의. Phase 2 전체 설계 SSOT (선행). Architect = chief author. |
| **U2-HELPER** | nas_key SSOT 단일 helper 통합 (forward-fix) | mctrader-data | 4 분산점(dual_writer.py:371 / runner.py:265 / runner.py:350-351 / l2.py:158) → 단일 helper 1곳. 신규 수집 자동 평면 적재 (forward-fix 보증). Phase 1 helper 단순화. |
| **U3-MIGRATE** | 기존 NAS `l1/` 객체 1회성 멱등 re-key 마이그레이션 | mctrader-data | 전 exchange/channel(bithumb: tx/obs/obd, upbit: tx/obs) `l1/` → 평면. copy → 4중 HEAD verify → old key delete. `.compacted` 완료 객체만. 멱등 (재실행 safe). |
| **U4-XREPO** | data REST partition_path→NAS key resolver + engine hint 정합 | mctrader-engine + data REST | resolver 위치 식별(설계 lane 탐색 prerequisite) → 평면 정합 → engine `tier=L1/` hint 회귀. repo 경계 = 별 Story 의무. |
| **U5-VERIFY** | 통합 검증 + cutover gate + Phase 1 helper 회수 | mctrader-data (+ engine 회귀) | 신규 평면 적재 e2e + 회수율 + cross-repo 정합 회귀 + dual-read fallback 제거 + Phase 1 tier-aware helper dead-code 회수 + forward-only invariant 박제. |

**부가 (PL 판단)**:
- ADR (Mandatory, PMO 발의 / Architect author): "NAS nas_key 단일 평면 SSOT 통합" —
  카테고리 Data & Storage, Architect 채번 (ADR-031 다음 가용).
- domain-knowledge (mctrader-hub): `data-health/nas-key-layout-ssot.md` (신규, tier별 layout
  SSOT 박제), `runbooks/nas-l1-rekey-migration-runbook.md` (신규, U3 cutover 절차).

## §4 Acceptance Criteria

- **AC-1** (U2): nas_key 가 단일 helper 1곳에서만 산출 — dual_writer/runner/l2 4 분산점 모두 helper 경유, 직접 문자열 조합 0 (grep 가드 테스트).
- **AC-2** (U2): 신규 수집 L1 PUT = 평면 `market/…/tier=L1/…` (l1/ prefix 0). L2 GET 이 평면 L1 입력 발견. forward-fix — 수동 개입 없이 통합 적재.
- **AC-3** (U3): 기존 `l1/` 객체 전량(전 exchange/channel) 평면 key 로 re-key. old `l1/` key 잔존 0 (마이그레이션 완료 마커 검증).
- **AC-4** (U3): re-key 는 copy → 4중 HEAD verify(ETag+VersionId+sha256 Metadata+ContentLength) 통과 **후에만** old key delete. 선행 미검증 delete 0. 재실행 시 중복/유실 0 (멱등).
- **AC-5** (U4): mctrader-engine 백테스트 historical fetch 가 평면 정합 후 200 (404 0). data REST resolver 평면 매핑 회귀 green.
- **AC-6** (U5): cutover 후 dual-read fallback 제거 + Phase 1 tier-aware helper dead-code 0. forward-only invariant(ADR-009 §D12) 박제 테스트 green.
- **AC-7** (전 Story): 실패는 명시 노출 (silent-skip 0). 완료/미완료 범위 audit trail 구분 가능.

## §5 Edge / Risk + 안전 게이트

| # | 위험 | 안전 게이트 |
|---|---|---|
| R1 | forward-only 위반 — U2 평면 cutover 시 마이그레이션 미완 객체 `l1/` 잔존 → reader split-brain | U1 ADR 에 **dual-read 전환 윈도우** 명시 의무: reader 가 평면+`l1/` 양쪽 fallback 조회 가능 상태에서만 U2 land. U5 가 fallback 제거 + forward-only 박제. |
| R2 | 마이그레이션 중 in-flight compaction race — U3 copy/delete 중 active compactor 가 동일 key PUT | U3 대상 = `.compacted` sentinel 완료 객체만 (active 제외, MCT-173 INV-1/2 패턴 재사용). U2 선행 보장 시 신규 PUT = 평면 → old `l1/` 와 충돌 없음. source immutable until 4-HEAD pass. |
| R3 | cross-repo cutover ordering — data REST resolver 위치/계약 미확인 → engine fetch 404 | U4 진입 전 설계 lane Architect 의 resolver 위치 탐색 = prerequisite. cutover: resolver 평면 정합 → engine 회귀 green → **그 후에만** U3 delete 단계. |
| R4 | 117GB 대량 delete 비용/오류 | U3 batch self-pacing(`runner.py:347-348` 패턴) + dry-run 우선 + per-batch 4-HEAD gate. bucket versioning=Enabled(MCT-161) 박제 후 진입 = rollback 안전망. delete 는 copy+4-HEAD 전수 통과 후 별 단계. |
| R5 | MCT-159 Issue 1/2 회귀 은닉 — compactor touch 시 orderbookdepth/pyarrow 경로 동시 변경 | U2 scope = nas_key helper 로 격리 (schema_version/pyarrow concat 경로 변경 금지, Impl Manifest §8.5 경계 명시). MCT-159 Issue 1(`l1.py:57`)·2(`l2.py:44`) line ≠ U2 대상(`l2.py:158`) → 격리 가능. |

**Cutover ordering (U1 ADR 명시 의무)**:
```
1. U1 ADR Accepted (dual-read 윈도우 + sequence 정의)
2. U2 코드 forward-fix land (신규=평면 / reader=dual-read fallback)
3. U4 data REST resolver 평면 정합 + engine 회귀 green  ┐ 병렬
   U3 dry-run + copy + 4-HEAD verify (delete 보류)       ┘
4. cross-repo 회귀 전수 green → U3 old l1/ key delete
5. U5 통합 검증 + fallback 제거 + Phase1 helper 회수 + forward-only 박제
```

## §6 scope_manifest (PMO 2nd pass)

```yaml
scope_manifest:
  epic: EPIC-nas-key-unification
  phase_1_ws_b:
    new_epic: false
    reuse:
      spec: docs/superpowers/specs/2026-05-17-disk-pressure-remediation-design.md
      plan: docs/superpowers/plans/2026-05-17-ws-b-tier-aware-nas-key.md
    story_slug: WS-B
  planned_adrs:
    count: 1
    candidate:
      title: "NAS Object Key Unification — 4-way split SSOT → single flat layout collapse"
      category: "data"   # hub ADR-027 frontmatter category 정합 (도메인 ADR SSOT = mctrader-hub)
      source: "disk-pressure spec §3 deferred candidate → unification ADR 재정의"
      owner_story: U1-ADR
      author: Architect    # PMO = proposer only (ADR-035)
      number: ADR-034   # U1-ADR LAND 박제 (hub 채번, 2026-05-17)
      carrier_repo: mctrader-hub   # 데이터 도메인 ADR SSOT = mctrader-hub/docs/adr/ (ADR-031 Layer 2 정합)
      carrier_path: "mctrader-hub:docs/adr/ADR-034-nas-key-unification.md"
  planned_files:
    phase_1:
      - "mctrader-data:src/mctrader_data/compactor/runner.py  # WS-B plan 참조 (PR #84, _resolve_legacy_nas_key 임시 helper)"
    phase_2:
      # path 정정 (U1-ADR LAND 박제): ADR 본문 = mctrader-hub SSOT (도메인 ADR), mctrader-data 측 = code + spec + CLAUDE.md만
      mctrader-hub:
        - "docs/adr/ADR-034-nas-key-unification.md         # U1-ADR LAND (Architect, 2026-05-17)"
        - "docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md # §D1 amendment box (U1-ADR sibling sync, ADR-034 cross-ref)"
        - "docs/adr/ADR-029-tier-promotion-single-source.md # §D9 amendment box (U1-ADR sibling sync, ADR-034 cross-ref)"
      mctrader-data:
        - "docs/superpowers/specs/2026-05-17-nas-key-unification-design.md # U1-ADR LAND (spec git stage, §7.1 RESOLVED + §6 정정)"
        - "src/mctrader_data/nas_storage/nas_key.py            # U2 단일 SSOT helper (신규, ADR-034 §결정 2 계약 정합)"
        - "src/mctrader_data/nas_storage/dual_writer.py:371      # U2 put_l1 l1/ prefix 제거"
        - "src/mctrader_data/compactor/runner.py:265,350-351     # U2 _dispatch_dual_write + cleanup → helper"
        - "src/mctrader_data/compactor/l2.py:157-160             # U2 L1 GET prefix → helper (SSOT-4)"
        - "src/mctrader_data/compactor/promotion.py              # U2 nas_key 계약 docstring 정합"
        - "scripts/rekey_l1_migration.py                         # U3 1회성 멱등 마이그레이션 (신규, ADR-034 §결정 4 4-HEAD verify)"
        - "tests/integration/test_dual_writer_l1.py              # U2 l1/ prefix assertion 갱신"
        - "tests/integration/compactor/test_promotion.py         # U2 nas_key fixture 갱신"
        - "tests/integration/test_nas_key_ssot.py                # U2 INV-1 single helper grep guard (신규)"
        - "tests/integration/test_forward_only_nas_key.py        # U5 INV-2/INV-6 forward-only invariant 박제 (신규)"
        - "CLAUDE.md                                             # nas_key SSOT 단일 helper 규약 신설 (Phase 2 U2-HELPER PR 가 land, U1-ADR PR 가 plan section 추가)"
      # U4-XREPO scope = close 후보 (§7.1 RESOLVED 박제): engine = candles only, market data L1 cross-repo impact = none
      # mctrader-engine + data_rest_api 항목 = ADR-034 §결정 5 흡수 후 무효 (U4 Story close 시 본 section 제거)
  planned_claude_md_sections:
    - "신규: ## nas_key SSOT 규약 (EPIC-nas-key-unification) — 전 tier 평면, 단일 helper"
    - "WAL freeze flags 표 인접: l1/ legacy prefix 마이그레이션 완료 마커"
    - "Compactor source 분기 규약 (ADR-017 Amendment 2) — nas_key 평면 cross-ref"
  domain_knowledge_pages:
    repo: mctrader-hub
    pages:
      - "docs/domain-knowledge/domain/data-health/nas-key-layout-ssot.md"
      - "docs/runbooks/nas-l1-rekey-migration-runbook.md"
  parallelism:
    phase_1: "WS-B 단독, 즉시 ship, Phase 2 전체 선행"
    phase_2:
      - "P2-1 순차 선행: U1-ADR (전체 설계 SSOT, Accepted 전 후속 차단)"
      - "P2-2 순차: U2-HELPER (후속 기준 layout)"
      - "P2-3 병렬: U3-MIGRATE ∥ U4-XREPO (파일경로 disjoint + repo 경계; 단 U2 선행 의존)"
      - "P2-4 순차 통합: U5-VERIFY (U3 ∧ U4 완료 게이트)"
  story_keys:
    note: "MCT-192 = 최고 사용 키 (commit 58d99ad). 채번은 Story 생성 시 Orchestrator 가 hub tracker 에서 확정 (MCT-192 이후 가용). 본 spec 은 슬러그(WS-B/U1~U5) 사용 — disk-pressure spec 동일 패턴."
  mct_159_relation:
    verdict: "partial_absorb_plus_precede (NOT supersede)"
    absorbed: "MCT-159 Issue 3 재발방지 → U3/U5 멱등 마이그레이션+verify gate"
    retained: "Issue 1 (orderbookdepth), Issue 2 (pyarrow overflow), Issue 3 손실분 backfill 재실행"
    action: "MCT-159 body 에 [PMO] cross-link 코멘트 ; phase:reservation 30일 타이머 Issue 1/2 기준 재평가"
```

## §7 Open prerequisite

### §7.1 RESOLVED — data REST partition_path → NAS key resolver 위치 식별 (2026-05-17 U1-ADR LAND)

**Verdict**: **resolver internal to mctrader-data, engine = candles only, market data L1 cross-repo impact = none**.

#### Explore §7.1 ground truth (전수 코드 verified, verified-via 인용)

| 항목 | 사실 | verified-via |
|---|---|---|
| **REST endpoint** | `partition_path: str = Query(...)` 수신 → validation → `tier_reader.read(partition_path)` 위임. REST 측 nas_key 변환 없음 | Read `mctrader-data/src/mctrader_data/api/routes_v1.py:63-91` |
| **tier_reader facade** | partition_path → `_extract_tier` → L1 reader (candles) or cold_reader. tier_reader 자체는 nas_key 산출 없음 | Read `mctrader-data/src/mctrader_data/io/tier_reader.py:147-223` |
| **L1 reader `_build_key`** | `tier=L1/exchange=DEFAULT/symbol={SYM}/date={date}/hour={HH}/{SYM}_{date}_{HH}.parquet` — **candles namespace** (`l1/` 0, `market/` 0, `schema_version=` 0). market data L1 namespace 와 disjoint | Read `mctrader-data/src/mctrader_data/io/l1_reader.py:75-84` |
| **cold reader `_build_nas_object_key`** | partition_path-as-is (또는 `node=DEFAULT/` 주입) — 평면 정합 | Read `mctrader-data/src/mctrader_data/io/cold_reader.py:195-238` |
| **dual_writer.py:329-371 `put_l1()`** | `nas_key = "l1/" + rel.as_posix()` (line 371) — write-side SSOT-1 (mctrader-data 내부) | Read `mctrader-data/src/mctrader_data/nas_storage/dual_writer.py:329-371` |
| **l2.py:140-170 `_l1_nas_source`** | L2 compactor 가 L1 입력을 `f"l1/market/{channel}/schema_version={ver}/tier=L1/exchange={exchange}/symbol={symbol}/date={date_str}/"` prefix 로 GET — read-side SSOT-4 (mctrader-data 내부) | Read `mctrader-data/src/mctrader_data/compactor/l2.py:140-170` |
| **engine `historical.py:42,65,87`** | `partition_path = f"tier=L1/exchange={exchange}/symbol={symbol_str}/timeframe={timeframe_str}/date={date_str}/part-00.parquet"` — **candles namespace (`timeframe=` 포함), market data L1 namespace (`market/<channel>/schema_version=*/`) 미참조** | Read `mctrader-engine/src/mctrader_engine/data_client/historical.py:35-87` |

#### Scope 변화 (§7.1 RESOLVED → 본 Epic 의 정정)

1. **U4-XREPO scope 재조정**: 원 spec 은 engine reader 정합을 가정했으나, engine = candles only, market data L1 layout 미사용 → **U4 close 후보**. ADR-034 §결정 5 가 cross-repo isolation invariant carrier 박제.
2. **Epic "cross-repo Epic" 프레이밍 축소**: market data L1 unification = mctrader-data 내부 구조 fix. cross-repo Story U4 = close 또는 axiom-of-symmetry 정합 / candles vs market data 영역 격리 명시 (코드 변경 없음, 문서 commit only — case B). Architect 판정 (ADR-034 §결정 5): **case A 채택 (close 권고)**.
3. **U2-HELPER 의 SSOT 통합 대상 4 곳 정합 유지**: put_l1 (`dual_writer.py:371`) / `_dispatch_dual_write` (`runner.py:265`) / `scan_and_cleanup_legacy` (`runner.py:350-351`) / L2 GET prefix (`l2.py:157-160`). Phase 1 WS-B (PR #84) 가 cleanup 임시 tier-aware helper 도입 → U5-VERIFY 가 dead-code 회수.

### §7.2 Story key 채번 (U1-ADR LAND 박제)

- EPIC `mctrader-data#86`
- U1-ADR `mctrader-data#87`
- U2-HELPER `mctrader-data#88`
- U3-MIGRATE `mctrader-data#89`
- U4-XREPO `mctrader-data#90` (close 후보)
- U5-VERIFY `mctrader-data#91`

PMO `gh issue list` 박제 완료. 본 Story 진입 시 추가 채번 불요.
